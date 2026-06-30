import asyncio
import os
import random
import signal
import sys
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import httpx
import shutil
import structlog
from cloakbrowser import launch_persistent_context_async
from dotenv import load_dotenv
from scrapling import Selector

# browserforge import preserved for integrated support
try:
    from browserforge.fingerprints import UserAgent
except ImportError:
    UserAgent = None

# Load environment variables
load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class ScrapingConfig:
    """Configuration with extended parameters for maximum resilience."""
    target_url: str = os.getenv("TARGET_URL", "https://books.toscrape.com/")
    proxy_list_url: str = os.getenv(
        "PROXY_URL",
        "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks5/data.txt",
    )
    proxy_file: Path = Path(os.getenv("PROXY_FILE", "socks5.txt"))
    profile_root: Path = Path(
        os.getenv("PROFILE_ROOT", Path.home() / ".cloakbrowser_profiles")
    )
    max_total_attempts: int = int(os.getenv("MAX_TOTAL_ATTEMPTS", "20"))
    browser_launch_timeout: float = float(os.getenv("BROWSER_LAUNCH_TIMEOUT", "12.0"))
    page_load_timeout: float = float(os.getenv("PAGE_LOAD_TIMEOUT", "25000"))  # ms
    request_timeout: float = float(os.getenv("REQUEST_TIMEOUT", "30.0"))
    proxy_validation_timeout: float = float(os.getenv("PROXY_VALIDATION_TIMEOUT", "3.0"))  # seconds
    max_proxy_validation_concurrency: int = int(os.getenv("MAX_PROXY_VALIDATION_CONCURRENCY", "80"))
    proxy_cache_ttl: int = int(os.getenv("PROXY_CACHE_TTL", "1800"))  # 30 minutes
    delay_between_attempts_min: float = float(os.getenv("DELAY_BETWEEN_ATTEMPTS_MIN", "0.5"))
    delay_between_attempts_max: float = float(os.getenv("DELAY_BETWEEN_ATTEMPTS_MAX", "1.5"))
    max_retries_per_proxy: int = int(os.getenv("MAX_RETRIES_PER_PROXY", "2"))
    retry_backoff_base: float = float(os.getenv("RETRY_BACKOFF_BASE", "1.0"))
    circuit_breaker_failure_threshold: int = int(os.getenv("CIRCUIT_BREAKER_THRESHOLD", "8"))
    circuit_breaker_reset_timeout: float = float(os.getenv("CIRCUIT_BREAKER_RESET_TIMEOUT", "15.0"))
    headless: bool = os.getenv("HEADLESS", "true").lower() == "true"
    max_profiles_to_keep: int = int(os.getenv("MAX_PROFILES_TO_KEEP", "10"))
    viewport: dict[str, int] = field(
        default_factory=lambda: {"width": 1920, "height": 947}
    )
    user_agents: list[str] = field(
        default_factory=lambda: os.getenv(
            "USER_AGENTS",
            (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36,"
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36,"
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
        ).split(","),
    )

# ---------------------------------------------------------------------------
# Custom Exceptions
# ---------------------------------------------------------------------------

class ScraperError(Exception):
    """Base exception class."""

class ProxyDownloadError(ScraperError):
    """Failed to download proxy list."""

class ProxyValidationError(ScraperError):
    """Proxy validation failed."""

class BrowserLaunchError(ScraperError):
    """Failed to launch browser."""

class PageLoadError(ScraperError):
    """Failed to load page."""

class AntibotBlockedError(ScraperError):
    """Page blocked by anti-bot protection."""

class DataExtractionError(ScraperError):
    """Failed to extract data."""

class CircuitBreakerOpenError(ScraperError):
    """Circuit breaker is open due to error threshold exceeded."""

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

def random_port(start: int = 15000, end: int = 25000) -> int:
    return random.randint(start, end)

def format_socks5_proxy(proxy: str) -> str:
    """
    Ensures correct socks5:// protocol usage for Chromium compatibility.
    """
    clean_proxy = proxy.replace("socks5h://", "").replace("socks5://", "")
    return f"socks5://{clean_proxy}"

# ---------------------------------------------------------------------------
# Dynamic SOCKS5 Handshake Validator (Protocol-level verification)
# ---------------------------------------------------------------------------

async def validate_socks5_handshake(host: str, port: int, timeout: float) -> bool:
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=timeout,
        )
        try:
            writer.write(b"\x05\x01\x00")
            await writer.drain()
            
            response = await asyncio.wait_for(reader.readexactly(2), timeout=timeout)
            
            if response == b"\x05\x00":
                return True
        finally:
            try:
                # Python 3.13 support: Protecting against possible BrokenPipeError from wait_closed()
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
    except Exception:
        pass
    return False

# ---------------------------------------------------------------------------
# Profile Storage Guard (Disk memory manager)
# ---------------------------------------------------------------------------

class ProfileStorageGuard:
    def __init__(self, config: ScrapingConfig) -> None:
        self.config = config
        self.logger = structlog.get_logger(__name__).bind(component="StorageGuard")

    def prune_old_profiles(self) -> None:
        try:
            root = self.config.profile_root
            if not root.exists():
                return
            
            # Windows OS compatibility: Only delete profiles not modified in the last 60 seconds
            now = time.time()
            profiles = [p for p in root.iterdir() if p.is_dir() and (now - p.stat().st_mtime) > 60]
            
            if len(profiles) <= self.config.max_profiles_to_keep:
                return

            profiles.sort(key=lambda p: p.stat().st_mtime)
            
            to_delete = profiles[: len(profiles) - self.config.max_profiles_to_keep]
            for p in to_delete:
                self.logger.info("Pruning old profile directory to free disk space", path=str(p))
                shutil.rmtree(p, ignore_errors=True)
        except Exception as exc:
            self.logger.warning("Failed to prune old profiles", error=str(exc))

# ---------------------------------------------------------------------------
# Proxy Manager
# ---------------------------------------------------------------------------

class ProxyManager:
    def __init__(self, client: httpx.AsyncClient, config: ScrapingConfig) -> None:
        self._client = client
        self.config = config
        self._cache: dict[str, Any] = {}
        self.logger = structlog.get_logger(__name__).bind(component="ProxyManager")

    async def download_proxies(self) -> list[str]:
        url = self.config.proxy_list_url
        self.logger.info("Downloading proxy list", url=url)
        try:
            response = await self._client.get(url)
            response.raise_for_status()
            raw_text = response.text
            self.config.proxy_file.parent.mkdir(parents=True, exist_ok=True)
            self.config.proxy_file.write_text(raw_text, encoding="utf-8")
            return [line.strip() for line in raw_text.splitlines() if line.strip()]
        except httpx.HTTPError as exc:
            self.logger.error("HTTP error downloading proxies", error=str(exc))
            raise ProxyDownloadError(f"Failed to download proxy list: {exc}") from exc

    async def load_local_proxies(self) -> list[str]:
        if not self.config.proxy_file.exists():
            raise ProxyDownloadError("Local proxy file not found")
        content = self.config.proxy_file.read_text(encoding="utf-8")
        return [line.strip() for line in content.splitlines() if line.strip()]

    async def get_proxies(self, force_refresh: bool = False) -> list[str]:
        now = time.monotonic()
        if not force_refresh and self._cache and (now - self._cache["timestamp"]) < self.config.proxy_cache_ttl:
            return self._cache["data"]

        try:
            proxies = await self.download_proxies()
            self._cache = {"data": proxies, "timestamp": now}
            return proxies
        except ProxyDownloadError:
            self.logger.warning("Download failed, falling back to local proxies")
            proxies = await self.load_local_proxies()
            self._cache = {"data": proxies, "timestamp": now}
            return proxies

    async def validate_proxy(self, proxy: str) -> bool:
        try:
            parts = proxy.replace("socks5h://", "").replace("socks5://", "").split(":")
            host = parts[0]
            port = int(parts[1]) if len(parts) > 1 else 1080
        except (IndexError, ValueError):
            return False

        return await validate_socks5_handshake(host, port, self.config.proxy_validation_timeout)

    async def filter_proxies(self, proxies: list[str]) -> list[str]:
        semaphore = asyncio.Semaphore(self.config.max_proxy_validation_concurrency)

        async def check_one(proxy: str) -> Optional[str]:
            async with semaphore:
                valid = await self.validate_proxy(proxy)
                return proxy if valid else None

        tasks = [asyncio.create_task(check_one(p)) for p in proxies]
        results = await asyncio.gather(*tasks)
        valid_proxies = [p for p in results if p is not None]
        self.logger.info("Proxy filtering complete", total=len(proxies), valid=len(valid_proxies))
        return valid_proxies

# ---------------------------------------------------------------------------
# Browser Manager
# ---------------------------------------------------------------------------

class BrowserManager:
    def __init__(self, config: ScrapingConfig, user_agent_rotator: Any) -> None:
        self.config = config
        self.user_agent_rotator = user_agent_rotator
        self.logger = structlog.get_logger(__name__).bind(component="BrowserManager")

    async def create_context(self, proxy: str, profile_dir: Path) -> Any:
        formatted_proxy = format_socks5_proxy(proxy)
        debugging_port = random_port()
        user_agent = self.user_agent_rotator.random_agent()

        self.logger.info(
            "Launching CloakBrowser context", 
            proxy=formatted_proxy, 
            port=debugging_port, 
            profile=str(profile_dir)
        )
        try:
            context = await asyncio.wait_for(
                launch_persistent_context_async(
                    user_data_dir=str(profile_dir),
                    headless=self.config.headless,
                    proxy=formatted_proxy,
                    user_agent=user_agent,
                    geoip=True,
                    humanize=True,
                    human_preset="careful",
                    viewport=self.config.viewport,
                    args=[
                        f"--remote-debugging-port={debugging_port}",
                        "--remote-debugging-address=127.0.0.1",
                        "--disable-dev-shm-usage",
                        "--no-zygote",
                        "--no-sandbox",
                        "--ignore-certificate-errors"
                    ]
                ),
                timeout=self.config.browser_launch_timeout
            )
            context._debugging_port = debugging_port
            return context
        except Exception as exc:
            self.logger.error("Browser launch failed, raising exception", proxy=formatted_proxy, error=str(exc))
            raise BrowserLaunchError(f"Failed to launch browser with proxy {formatted_proxy}: {exc}") from exc

    @staticmethod
    async def close_context(context: Any) -> None:
        if context is not None:
            try:
                await context.close()
            except Exception:
                pass

async def launch_fallback_context(self: Any, proxy: str, port: int, ua: str, p_dir: Path) -> Any:
    return await asyncio.wait_for(
        launch_persistent_context_async(
            user_data_dir=str(p_dir),
            headless=self.config.headless,
            proxy=proxy,
            user_agent=ua,
            geoip=True,
            humanize=True,
            human_preset="careful",
            viewport=self.config.viewport,
            args=[
                f"--remote-debugging-port={port}",
                "--remote-debugging-address=127.0.0.1",
                "--disable-dev-shm-usage",
                "--no-zygote",
                "--no-sandbox",
                "--ignore-certificate-errors"
            ]
        ),
        timeout=self.config.browser_launch_timeout
    )

# ---------------------------------------------------------------------------
# User-Agent Rotator (with browserforge support)
# ---------------------------------------------------------------------------

class UserAgentRotator:
    """Statistically accurate real browser user-agent generator."""
    def __init__(self, fallback_agents: list[str]) -> None:
        self.fallback_agents = fallback_agents
        try:
            if UserAgent is not None:
                self.ua_generator = UserAgent()
                structlog.get_logger(__name__).info("BrowserForge UserAgent engine initialized successfully")
            else:
                self.ua_generator = None
        except Exception:
            self.ua_generator = None

    def random_agent(self) -> str:
        if self.ua_generator is not None:
            try:
                return self.ua_generator.generate()
            except Exception:
                pass
        return random.choice(self.fallback_agents)

# ---------------------------------------------------------------------------
# Circuit Breaker
# ---------------------------------------------------------------------------

class CircuitBreaker:
    def __init__(self, failure_threshold: int, reset_timeout: float) -> None:
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.state = "closed"
        self.logger = structlog.get_logger(__name__).bind(component="CircuitBreaker")

    def record_failure(self) -> None:
        self.failure_count += 1
        self.last_failure_time = time.monotonic()
        self.logger.warning("Circuit breaker failure recorded", count=self.failure_count)
        if self.failure_count >= self.failure_threshold:
            self.state = "open"
            self.logger.error("Circuit breaker state set to OPEN")

    def record_success(self) -> None:
        self.failure_count = 0
        self.state = "closed"

    async def before_call(self) -> None:
        if self.state == "open":
            elapsed = time.monotonic() - self.last_failure_time
            if elapsed < self.reset_timeout:
                raise CircuitBreakerOpenError("Circuit breaker is currently open due to high failure rate")
            self.state = "half_open"
            self.logger.info("Circuit breaker entering HALF-OPEN state")

    async def __aenter__(self) -> "CircuitBreaker":
        await self.before_call()
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> bool:
        if exc_type is not None:
            self.record_failure()
        else:
            self.record_success()
        return False

# ---------------------------------------------------------------------------
# Scraper (Main class)
# ---------------------------------------------------------------------------

class Scraper:
    def __init__(self, config: ScrapingConfig, proxy_manager: ProxyManager, browser_manager: BrowserManager, storage_guard: ProfileStorageGuard) -> None:
        self.config = config
        self.proxy_manager = proxy_manager
        self.browser_manager = browser_manager
        self.storage_guard = storage_guard
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=config.circuit_breaker_failure_threshold,
            reset_timeout=config.circuit_breaker_reset_timeout
        )
        self.logger = structlog.get_logger(__name__).bind(component="Scraper")

    async def _load_page(self, context: Any) -> Any:
        page = context.pages[0] if context.pages else await context.new_page()
        await page.goto(
            self.config.target_url,
            wait_until="domcontentloaded",
            timeout=self.config.page_load_timeout,
        )
        return page

    def _check_antibot_block(self, html: str) -> None:
        """Analyzes page content for anti-bot blocking indicators."""
        lowercased = html.lower()
        block_indicators = [
            "just a moment...",
            "cloudflare",
            "checking your browser",
            "ddos protection",
            "enable javascript",
            "attention required",
            "captcha-delivery",
            "challenge-platform",
            "access denied"
        ]
        if any(ind in lowercased for ind in block_indicators):
            raise AntibotBlockedError("Antibot challenge or access block detected on the target page")

    async def _extract_data(self, page_content: str) -> dict[str, Optional[str]]:
        try:
            self._check_antibot_block(page_content)
            # Using Scrapling's native, fast Selector class
            scrapling_page = Selector(page_content)  
            
            # Page title
            title = scrapling_page.css("title::text").get()
            
            # Book list parsing according to your HTML structure
            books = []
            pods = scrapling_page.css("article.product_pod")
            for pod in pods:
                book_title = pod.css("h3 a::attr(title)").get()
                price = pod.css("p.price_color::text").get()
                if book_title and price:
                    books.append({"title": book_title, "price": price})
            
            return {
                "page_title": title,
                "total_books_found": len(books),
                "books": books
            }
        except AntibotBlockedError:
            raise
        except Exception as exc:
            raise DataExtractionError(f"Failed to parse or extract DOM structure: {exc}") from exc

    async def scrape_with_proxy(self, proxy: str, profile_dir: Path) -> dict[str, Optional[str]]:
        context = None
        try:
            context = await self.browser_manager.create_context(proxy, profile_dir)
            page = await self._load_page(context)
            await asyncio.sleep(random.uniform(2.5, 4.5))
            html = await page.content()
            data = await self._extract_data(html)
            return data
        finally:
            if context:
                await BrowserManager.close_context(context)

    async def scrape(self, shutdown_event: asyncio.Event) -> dict[str, Optional[str]]:
        """
        Executes the main scraping loop.
        If all proxies are blocked, automatically refreshes the list dynamically.
        """
        force_refresh = False
        total_attempts = 0

        while total_attempts < self.config.max_total_attempts:
            if shutdown_event.is_set():
                raise ScraperError("Shutdown requested by system")

            self.storage_guard.prune_old_profiles()

            try:
                raw_proxies = await self.proxy_manager.get_proxies(force_refresh=force_refresh)
            except Exception as e:
                self.logger.error("Failed to load proxies, retrying refresh", error=str(e))
                await asyncio.sleep(2)
                force_refresh = True
                continue

            valid_proxies = await self.proxy_manager.filter_proxies(raw_proxies)
            if not valid_proxies:
                self.logger.warning("No valid SOCKS5 proxies found, forcing cache refresh")
                force_refresh = True
                await asyncio.sleep(3)
                continue

            random.shuffle(valid_proxies)

            for proxy in valid_proxies:
                if shutdown_event.is_set():
                    raise ScraperError("Shutdown requested by system")

                proxy_hash = str(abs(hash(proxy)))[:10]
                profile_dir = self.config.profile_root / f"profile_{proxy_hash}"
                profile_dir.mkdir(parents=True, exist_ok=True)

                for attempt in range(1, self.config.max_retries_per_proxy + 1):
                    if shutdown_event.is_set():
                        raise ScraperError("Shutdown requested by system")

                    total_attempts += 1
                    self.logger.info("Attempting scrape cycle", proxy=proxy, attempt=attempt, total=total_attempts)

                    async with self.circuit_breaker:
                        try:
                            data = await self.scrape_with_proxy(proxy, profile_dir)
                            return data
                        except AntibotBlockedError as exc:
                            self.logger.warning("Proxy was flagged/blocked by antibot, discarding", proxy=proxy, error=str(exc))
                            break
                        except (asyncio.TimeoutError, BrowserLaunchError, PageLoadError) as exc:
                            self.logger.warning("Transient network failure on proxy", proxy=proxy, error=str(exc))
                            if attempt < self.config.max_retries_per_proxy:
                                delay = self.config.retry_backoff_base * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                                await asyncio.sleep(delay)
                        except Exception as exc:
                            self.logger.error("Unexpected error, shifting proxy", error=str(exc))
                            break

                await asyncio.sleep(random.uniform(self.config.delay_between_attempts_min, self.config.delay_between_attempts_max))

            force_refresh = True

        raise ScraperError("Max scraping attempts reached without successful data extraction")

# ---------------------------------------------------------------------------
# Graceful Shutdown & Zombie Process Reap
# ---------------------------------------------------------------------------

def setup_signal_handlers(loop: asyncio.AbstractEventLoop, shutdown_event: asyncio.Event) -> None:
    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, shutdown_event.set)
            except ValueError:
                pass

# ---------------------------------------------------------------------------
# Main Entry
# ---------------------------------------------------------------------------

async def main() -> None:
    setup_logging()
    logger = structlog.get_logger(__name__)
    logger.info("Unstoppable CloakBrowser-Scrapling Scraper Initialized")

    config = ScrapingConfig()
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    setup_signal_handlers(loop, shutdown_event)

    limits = httpx.Limits(max_keepalive_connections=20, max_connections=50)
    timeout = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0)
    
    pending = set()
    
    async with httpx.AsyncClient(limits=limits, timeout=timeout) as http_client:
        proxy_manager = ProxyManager(http_client, config)
        user_agent_rotator = UserAgentRotator(config.user_agents)
        browser_manager = BrowserManager(config, user_agent_rotator)
        storage_guard = ProfileStorageGuard(config)
        scraper = Scraper(config, proxy_manager, browser_manager, storage_guard)

        try:
            scrape_task = asyncio.create_task(scraper.scrape(shutdown_event))
            shutdown_watcher = asyncio.create_task(shutdown_event.wait())

            done, pending = await asyncio.wait(
                [scrape_task, shutdown_watcher],
                return_when=asyncio.FIRST_COMPLETED,
            )

            if shutdown_watcher in done:
                logger.warning("Graceful shutdown signal triggered. Halting tasks safely...")
                scrape_task.cancel()
                try:
                    await scrape_task
                except asyncio.CancelledError:
                    pass
            else:
                data = scrape_task.result()
                logger.info("Unstoppable Scraper Executed Successfully")
                print(f"\n[SUCCESS] Highly Resilient Extracted Data: {data}")

        except CircuitBreakerOpenError as exc:
            logger.critical("Fatal: Circuit Breaker Open, Scraper Aborted", error=str(exc))
            sys.exit(1)
        except ScraperError as exc:
            logger.critical("Fatal Scraper Process Exception", error=str(exc))
            sys.exit(1)
        finally:
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            if not shutdown_event.is_set():
                shutdown_event.set()

    logger.info("Graceful execution complete. Goodbye.")

if __name__ == "__main__":
    asyncio.run(main())