import asyncio
import hashlib
import os
import random
import signal
import sys
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, AsyncGenerator

import httpx
import shutil
import structlog
from cloakbrowser import launch_persistent_context_async
from dotenv import load_dotenv
from scrapling import Selector

# Attempt to import browserforge for human-like fingerprinting
try:
    from browserforge.fingerprints import UserAgent
except ImportError:
    UserAgent = None

# Load environment variables from .env
load_dotenv()

# ---------------------------------------------------------------------------
# Configuration (2026 Strict Typing)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ScrapingConfig:
    """Optimized, immutable configuration with resilient timeout parameters."""
    target_url: str = os.getenv("TARGET_URL", "https://books.toscrape.com/")
    proxy_list_url: str = os.getenv(
        "PROXY_URL",
        "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks5/data.txt",
    )
    proxy_file: Path = Path(os.getenv("PROXY_FILE", "socks5.txt"))
    profile_root: Path = Path(os.getenv("PROFILE_ROOT", Path.home() / ".cloakbrowser_profiles"))
    
    max_total_attempts: int = int(os.getenv("MAX_TOTAL_ATTEMPTS", "20"))
    browser_launch_timeout: float = float(os.getenv("BROWSER_LAUNCH_TIMEOUT", "15.0"))
    page_load_timeout: float = float(os.getenv("PAGE_LOAD_TIMEOUT", "25000"))  # ms
    request_timeout: float = float(os.getenv("REQUEST_TIMEOUT", "30.0"))
    proxy_validation_timeout: float = float(os.getenv("PROXY_VALIDATION_TIMEOUT", "5.0"))
    max_proxy_validation_concurrency: int = int(os.getenv("MAX_PROXY_VALIDATION_CONCURRENCY", "60"))
    proxy_cache_ttl: int = int(os.getenv("PROXY_CACHE_TTL", "1800"))
    
    delay_between_attempts_min: float = float(os.getenv("DELAY_BETWEEN_ATTEMPTS_MIN", "0.5"))
    delay_between_attempts_max: float = float(os.getenv("DELAY_BETWEEN_ATTEMPTS_MAX", "1.5"))
    max_retries_per_proxy: int = int(os.getenv("MAX_RETRIES_PER_PROXY", "2"))
    retry_backoff_base: float = float(os.getenv("RETRY_BACKOFF_BASE", "1.0"))
    
    circuit_breaker_failure_threshold: int = int(os.getenv("CIRCUIT_BREAKER_THRESHOLD", "5"))
    circuit_breaker_reset_timeout: float = float(os.getenv("CIRCUIT_BREAKER_RESET_TIMEOUT", "30.0"))
    
    headless: bool = os.getenv("HEADLESS", "true").lower() == "true"
    max_profiles_to_keep: int = int(os.getenv("MAX_PROFILES_TO_KEEP", "10"))
    
    viewport: dict[str, int] = field(default_factory=lambda: {"width": 1920, "height": 947})
    user_agents: list[str] = field(
        default_factory=lambda: os.getenv(
            "USER_AGENTS",
            (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36,"
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
        ).split(","),
    )

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class ScraperError(Exception): """Base exception class."""
class ProxyDownloadError(ScraperError): """Proxy download failure."""
class BrowserLaunchError(ScraperError): """Chromium initialization failure."""
class PageLoadError(ScraperError): """Target page load timeout."""
class AntibotBlockedError(ScraperError): """Anti-bot protection detected."""
class DataExtractionError(ScraperError): """DOM extraction failure."""
class CircuitBreakerOpenError(ScraperError): """Circuit breaker open state."""

# ---------------------------------------------------------------------------
# Structured Logging Setup
# ---------------------------------------------------------------------------

def setup_logging() -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

# ---------------------------------------------------------------------------
# Utility Helpers
# ---------------------------------------------------------------------------

def get_stable_proxy_hash(proxy: str) -> str:
    """Generates a cryptographically stable hash for disk caching."""
    return hashlib.sha256(proxy.encode("utf-8")).hexdigest()[:10]

def random_port(start: int = 15000, end: int = 25000) -> int:
    return random.randint(start, end)

def format_socks5_proxy(proxy: str) -> str:
    clean_proxy = proxy.replace("socks5h://", "").replace("socks5://", "")
    return f"socks5://{clean_proxy}"

async def validate_socks5_handshake(host: str, port: int, timeout: float) -> bool:
    """Low-level SOCKS5 fast TCP reject handshake."""
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
        try:
            writer.write(b"\x05\x01\x00")
            await writer.drain()
            response = await asyncio.wait_for(reader.readexactly(2), timeout=timeout)
            return response == b"\x05\x00"
        finally:
            writer.close()
            await writer.wait_closed()
    except Exception:
        return False

# ---------------------------------------------------------------------------
# Async Profile Storage Guard
# ---------------------------------------------------------------------------

class ProfileStorageGuard:
    def __init__(self, config: ScrapingConfig) -> None:
        self.config = config
        self.logger = structlog.get_logger(__name__).bind(component="StorageGuard")

    async def prune_old_profiles(self) -> None:
        """Executes non-blocking FS deletion via ThreadPool."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._prune_sync)

    def _prune_sync(self) -> None:
        try:
            root = self.config.profile_root
            if not root.exists():
                return
            
            now = time.time()
            profiles = [p for p in root.iterdir() if p.is_dir() and (now - p.stat().st_mtime) > 60]
            
            if len(profiles) <= self.config.max_profiles_to_keep:
                return

            profiles.sort(key=lambda p: p.stat().st_mtime)
            to_delete = profiles[: len(profiles) - self.config.max_profiles_to_keep]
            
            for p in to_delete:
                self.logger.debug("Pruning transient profile", path=str(p))
                shutil.rmtree(p, ignore_errors=True)
        except Exception as exc:
            self.logger.warning("Pruning failed", error=str(exc))

# ---------------------------------------------------------------------------
# Proxy Manager
# ---------------------------------------------------------------------------

class ProxyManager:
    def __init__(self, client: httpx.AsyncClient, config: ScrapingConfig) -> None:
        self._client = client
        self.config = config
        self._cache: dict[str, Any] = {}
        self.logger = structlog.get_logger(__name__).bind(component="ProxyManager")

    async def get_proxies(self, force_refresh: bool = False) -> list[str]:
        now = time.monotonic()
        if not force_refresh and self._cache and (now - self._cache["timestamp"]) < self.config.proxy_cache_ttl:
            return self._cache["data"]

        try:
            response = await self._client.get(self.config.proxy_list_url)
            response.raise_for_status()
            proxies = [line.strip() for line in response.text.splitlines() if line.strip()]
            
            self.config.proxy_file.parent.mkdir(parents=True, exist_ok=True)
            self.config.proxy_file.write_text(response.text, encoding="utf-8")
            
            self._cache = {"data": proxies, "timestamp": now}
            return proxies
        except Exception as exc:
            self.logger.warning("Download failed, using local cache", error=str(exc))
            if self.config.proxy_file.exists():
                content = self.config.proxy_file.read_text(encoding="utf-8")
                return [line.strip() for line in content.splitlines() if line.strip()]
            return []

    async def filter_proxies(self, proxies: list[str]) -> list[str]:
        """Dual-layer proxy validation (TCP & WAN)."""
        semaphore = asyncio.Semaphore(self.config.max_proxy_validation_concurrency)

        async def check_one(proxy: str) -> dict[str, Any]:
            async with semaphore:
                try:
                    parts = proxy.replace("socks5h://", "").replace("socks5://", "").split(":")
                    host, port = parts[0], int(parts[1]) if len(parts) > 1 else 1080
                except (IndexError, ValueError):
                    return {"proxy": proxy, "tcp_ok": False, "wan_ok": False}

                if not await validate_socks5_handshake(host, port, self.config.proxy_validation_timeout):
                    return {"proxy": proxy, "tcp_ok": False, "wan_ok": False}

                try:
                    formatted = format_socks5_proxy(proxy)
                    async with httpx.AsyncClient(proxies=formatted, timeout=self.config.proxy_validation_timeout) as c:
                        res = await c.get("http://checkip.amazonaws.com", follow_redirects=False)
                        return {"proxy": proxy, "tcp_ok": True, "wan_ok": res.status_code == 200}
                except Exception:
                    return {"proxy": proxy, "tcp_ok": True, "wan_ok": False}

        tasks = [asyncio.create_task(check_one(p)) for p in proxies]
        results = await asyncio.gather(*tasks)

        best = [r["proxy"] for r in results if r["wan_ok"]]
        if best:
            self.logger.info("WAN validated proxies ready", count=len(best))
            return best

        fallback = [r["proxy"] for r in results if r["tcp_ok"]]
        self.logger.warning("Falling back to TCP-only validated proxies", count=len(fallback))
        return fallback

# ---------------------------------------------------------------------------
# Browser Context Engine
# ---------------------------------------------------------------------------

class BrowserManager:
    def __init__(self, config: ScrapingConfig, ua_rotator: Any) -> None:
        self.config = config
        self.ua_rotator = ua_rotator
        self.logger = structlog.get_logger(__name__).bind(component="BrowserManager")

    @asynccontextmanager
    async def context_scope(self, proxy: str, profile_dir: Path) -> AsyncGenerator[Any, None]:
        """Safe asynchronous context manager for Chromium lifecycle."""
        context = None
        formatted_proxy = format_socks5_proxy(proxy)
        try:
            context = await asyncio.wait_for(
                launch_persistent_context_async(
                    user_data_dir=str(profile_dir),
                    headless=self.config.headless,
                    proxy=formatted_proxy,
                    user_agent=self.ua_rotator.random_agent(),
                    geoip=True,
                    humanize=True,
                    human_preset="careful",
                    viewport=self.config.viewport,
                    args=[
                        f"--remote-debugging-port={random_port()}",
                        "--remote-debugging-address=127.0.0.1",
                        "--disable-dev-shm-usage",
                        "--no-zygote",
                        "--no-sandbox",
                        "--ignore-certificate-errors"
                    ]
                ),
                timeout=self.config.browser_launch_timeout
            )
            yield context
        except Exception as exc:
            self.logger.error("Browser launch anomaly", error=str(exc))
            raise BrowserLaunchError(f"Engine init failed: {exc}") from exc
        finally:
            if context:
                try:
                    await context.close()
                except Exception:
                    pass

class UserAgentRotator:
    def __init__(self, fallback_agents: list[str]) -> None:
        self.fallback_agents = fallback_agents
        self.ua_generator = UserAgent() if UserAgent else None

    def random_agent(self) -> str:
        try:
            if self.ua_generator: return self.ua_generator.generate()
        except Exception:
            pass
        return random.choice(self.fallback_agents)

# ---------------------------------------------------------------------------
# Resilience: Circuit Breaker
# ---------------------------------------------------------------------------

class CircuitBreaker:
    def __init__(self, threshold: int, timeout: float) -> None:
        self.threshold = threshold
        self.timeout = timeout
        self.failures = 0
        self.last_failure_time = 0.0
        self.state = "closed"
        self.logger = structlog.get_logger(__name__).bind(component="CircuitBreaker")

    def record_failure(self) -> None:
        self.failures += 1
        self.last_failure_time = time.monotonic()
        if self.failures >= self.threshold and self.state != "open":
            self.state = "open"
            self.logger.error("Tripped OPEN", failures=self.failures)

    def record_success(self) -> None:
        if self.state != "closed" or self.failures > 0:
            self.logger.info("Recovered to CLOSED state")
        self.failures = 0
        self.state = "closed"

    async def before_call(self) -> None:
        if self.state == "open":
            elapsed = time.monotonic() - self.last_failure_time
            if elapsed < self.timeout:
                raise CircuitBreakerOpenError("Circuit isolated due to cascading failures.")
            self.state = "half_open"
            self.logger.info("Testing HALF-OPEN state")

# ---------------------------------------------------------------------------
# Scraper Orchestrator
# ---------------------------------------------------------------------------

class Scraper:
    def __init__(self, config: ScrapingConfig, proxy_mgr: ProxyManager, browser_mgr: BrowserManager, storage_guard: ProfileStorageGuard) -> None:
        self.config = config
        self.proxy_mgr = proxy_mgr
        self.browser_mgr = browser_mgr
        self.storage_guard = storage_guard
        self.circuit_breaker = CircuitBreaker(config.circuit_breaker_failure_threshold, config.circuit_breaker_reset_timeout)
        self.logger = structlog.get_logger(__name__).bind(component="Scraper")

    async def _load_page(self, context: Any) -> Any:
        page = context.pages[0] if context.pages else await context.new_page()
        await page.goto(self.config.target_url, wait_until="domcontentloaded", timeout=self.config.page_load_timeout)
        return page

    def _parse_html_sync(self, html: str) -> dict[str, Any]:
        """Synchronous CPU-bound parsing block."""
        lowercased = html.lower()
        if any(i in lowercased for i in ["just a moment...", "cloudflare", "attention required", "access denied"]):
            raise AntibotBlockedError("Target actively refused connection via anti-bot.")

        scrapling_page = Selector(html)  
        books = [
            {"title": pod.css("h3 a::attr(title)").get(), "price": pod.css("p.price_color::text").get()}
            for pod in scrapling_page.css("article.product_pod")
            if pod.css("h3 a::attr(title)").get() and pod.css("p.price_color::text").get()
        ]
        
        return {
            "page_title": scrapling_page.css("title::text").get(),
            "total_books_found": len(books),
            "books": books
        }

    async def _extract_data(self, html: str) -> dict[str, Any]:
        """Offloads DOM extraction to prevent Event Loop Starvation."""
        return await asyncio.to_thread(self._parse_html_sync, html)

    async def scrape(self, shutdown_event: asyncio.Event) -> dict[str, Any]:
        force_refresh = False
        total_attempts = 0

        while True:
            if shutdown_event.is_set() or total_attempts >= self.config.max_total_attempts:
                break

            await self.storage_guard.prune_old_profiles()

            try:
                raw_proxies = await self.proxy_mgr.get_proxies(force_refresh=force_refresh)
                valid_proxies = await self.proxy_mgr.filter_proxies(raw_proxies)
            except Exception as e:
                self.logger.error("Proxy tier collapse", error=str(e))
                await asyncio.sleep(2)
                force_refresh = True
                continue

            random.shuffle(valid_proxies)

            for proxy in valid_proxies:
                if shutdown_event.is_set() or total_attempts >= self.config.max_total_attempts:
                    break

                try:
                    await self.circuit_breaker.before_call()
                except CircuitBreakerOpenError:
                    sleep_time = self.config.circuit_breaker_reset_timeout - (time.monotonic() - self.circuit_breaker.last_failure_time)
                    if sleep_time > 0:
                        self.logger.warning("Circuit breaker OPEN. Suspending thread logic.", sleep_sec=round(sleep_time, 2))
                        await asyncio.sleep(sleep_time)
                    continue

                profile_dir = self.config.profile_root / f"profile_{get_stable_proxy_hash(proxy)}"
                profile_dir.mkdir(parents=True, exist_ok=True)

                for attempt in range(1, self.config.max_retries_per_proxy + 1):
                    if shutdown_event.is_set() or total_attempts >= self.config.max_total_attempts:
                        break

                    total_attempts += 1
                    self.logger.info("Executing precise extraction cycle", attempt=total_attempts, proxy=proxy)

                    try:
                        async with self.browser_mgr.context_scope(proxy, profile_dir) as context:
                            page = await self._load_page(context)
                            await asyncio.sleep(random.uniform(2.5, 4.5))
                            html = await page.content()
                            data = await self._extract_data(html)
                            
                            self.circuit_breaker.record_success()
                            return data

                    except AntibotBlockedError as exc:
                        self.logger.warning("WAF block triggered. Purging proxy.", proxy=proxy)
                        self.circuit_breaker.record_failure()
                        break 
                        
                    except Exception as exc:
                        self.logger.warning("Transient drop in context layer", error=str(exc))
                        self.circuit_breaker.record_failure()
                        if attempt < self.config.max_retries_per_proxy:
                            await asyncio.sleep(self.config.retry_backoff_base * (2 ** (attempt - 1)))
                
                await asyncio.sleep(random.uniform(self.config.delay_between_attempts_min, self.config.delay_between_attempts_max))
            force_refresh = True

        raise ScraperError(f"Pipeline terminated. Maximum bounded attempts ({self.config.max_total_attempts}) exhausted.")

# ---------------------------------------------------------------------------
# Signal Handling
# ---------------------------------------------------------------------------

def setup_signal_handlers(loop: asyncio.AbstractEventLoop, shutdown_event: asyncio.Event) -> None:
    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, shutdown_event.set)
            except NotImplementedError:
                pass

# ---------------------------------------------------------------------------
# Execution Entry Point
# ---------------------------------------------------------------------------

async def main() -> None:
    setup_logging()
    logger = structlog.get_logger(__name__)
    
    config = ScrapingConfig()
    shutdown_event = asyncio.Event()
    setup_signal_handlers(asyncio.get_running_loop(), shutdown_event)

    limits = httpx.Limits(max_keepalive_connections=20, max_connections=50)
    timeout = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0)
    
    async with httpx.AsyncClient(limits=limits, timeout=timeout) as http_client:
        scraper = Scraper(
            config,
            ProxyManager(http_client, config),
            BrowserManager(config, UserAgentRotator(config.user_agents)),
            ProfileStorageGuard(config)
        )

        scrape_task = asyncio.create_task(scraper.scrape(shutdown_event))
        shutdown_watcher = asyncio.create_task(shutdown_event.wait())

        done, pending = await asyncio.wait(
            [scrape_task, shutdown_watcher],
            return_when=asyncio.FIRST_COMPLETED,
        )

        if shutdown_watcher in done:
            logger.warning("OS Interrupt received. Commencing safe shutdown protocol.")
            scrape_task.cancel()
        else:
            try:
                payload = scrape_task.result()
                logger.info("Extraction Target Achieved")
                print(f"\n[SUCCESS] Payload Captured: {payload}")
            except Exception as e:
                logger.critical("Fatal extraction trajectory collapse", error=str(e))
                sys.exit(1)

        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass