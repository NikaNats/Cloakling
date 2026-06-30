# Cloakling: Resilient CloakBrowser & Scrapling Crawler

An asynchronous, highly resilient web scraper engineered to handle proxy instability, network latency, and basic anti-bot defenses. It integrates **CloakBrowser** (Chromium with offline GeoIP databases) for stealth automation, **Scrapling** for high-speed DOM parsing, and **browserforge** for realistic client fingerprints.

## Architecture & Resilience Features

*   **Dynamic SOCKS5 Handshake Validation**: Verifies proxy viability at the TCP/protocol level before initiating browser processes, avoiding unnecessary browser launch overhead.
*   **Circuit Breaker Pattern**: Automatically halts operations if consecutive failures cross the configured threshold, preventing resource exhaustion and target IP bans.
*   **Profile Storage Management**: Periodically prunes transient Chromium user data directories to prevent local disk space exhaustion.
*   **Structured JSON Logging**: Out-of-the-box support for centralized logging platforms (ELK, Datadog) via `structlog`.
*   **Graceful Shutdown**: Intercepts `SIGINT` (Ctrl+C) and `SIGTERM` signals to cleanly terminate active browser contexts and pending background tasks without leaving zombie processes.

## Tech Stack

*   **Runtime Engine**: Python 3.10+ (Asynchronous asyncio loop)
*   **Browser Driver**: `cloakbrowser` & `patchright` (Stealth Playwright fork)
*   **DOM Parsing**: `scrapling` & `lxml` (High-performance CSS/XPath selectors)
*   **Network Client**: `httpx` with SOCKS5 capability
*   **Fingerprint Generation**: `browserforge`

## Project Structure

```text
├── .env.example          # Sample environment configurations
├── .gitignore            # Excludes local environments, credentials, and cache
├── LICENSE               # MIT License
├── Scrap.py              # Main crawler engine & execution entry point
├── requirements.txt      # Production-pinned dependency list
└── README.md             # Project documentation
```

## Installation

### 1. Clone the Repository
```bash
git clone https://github.com/NikaNats/Cloakling.git
cd Cloakling
```

### 2. Set Up a Virtual Environment & Dependencies
```bash
# Create virtual environment
python -m venv venv

# Activate on Linux/macOS:
source venv/bin/activate  

# Activate on Windows (PowerShell):
.\venv\Scripts\Activate.ps1

# Upgrade pip and install requirements
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Initialize Browser Engines
Install the underlying browser binaries required by the automation engine:
```bash
playwright install chromium
```

## Configuration

The application is configured using a `.env` file at the root of the project directory.

1. Copy the template file:
   ```bash
   cp .env.example .env
   ```
2. Adjust variables as needed:

| Parameter | Default Value | Description |
| :--- | :--- | :--- |
| `TARGET_URL` | `https://books.toscrape.com/` | Target website URL to scrape |
| `PROXY_URL` | *(Public SOCKS5 API)* | Public proxy list URL (SOCKS5 format) |
| `MAX_TOTAL_ATTEMPTS` | `15` | Total failure limit before script termination |
| `HEADLESS` | `true` | Run Chromium without a graphical user interface |
| `MAX_PROFILES_TO_KEEP` | `5` | Maximum browser profile folders retained on disk |

## Usage

Run the main asynchronous controller:
```bash
python Scrap.py
```

### Log Output Structure
The application writes logs structured in JSON format to standard output, making it compatible with enterprise log aggregators:

```json
{"component": "Scraper", "event": "Attempting scrape cycle", "proxy": "socks5://192.168.1.1:1080", "attempt": 1, "total": 1, "level": "info", "timestamp": "2026-07-01T00:15:30.123456Z"}
```


## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.