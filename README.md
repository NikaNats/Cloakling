# Resilient CloakBrowser & Scrapling Crawler

An asynchronous, highly resilient web scraper engineered to handle proxy failure, network latency, and basic anti-bot defenses. It integrates **CloakBrowser** (Chromium with offline GeoIP databases) for stealth automation, **Scrapling** for high-speed DOM parsing, and **browserforge** for realistic client fingerprints.


## Architecture & Resilience Features

*   **Dynamic SOCKS5 Handshake Validation**: Verifies proxy viability at the TCP/protocol level before initiating browser processes, avoiding unnecessary browser launch overhead.
*   **Circuit Breaker Pattern**: Automatically halts operations if consecutive failures cross the configured threshold, preventing system degradation and IP bans.
*   **Profile Storage Management**: Periodically prunes transient Chromium profile directories to prevent disk exhaustion.
*   **Structured JSON Logging**: Out-of-the-box support for centralized logging platforms via `structlog`.
*   **Graceful Shutdown**: Intercepts `SIGINT`/`SIGTERM` to clean up active browser contexts and background tasks without leaving orphan zombie processes.


## Tech Stack

*   **Runtime Engine**: Python 3.10+ (Asynchronous asyncio loop)
*   **Browser Driver**: `cloakbrowser` & `patchright` (Stealth Playwright fork)
*   **DOM Parsing**: `scrapling` & `lxml` (High-performance CSS selectors)
*   **Network Client**: `httpx` with SOCKS5 capabilities
*   **Fingerprint Generation**: `browserforge`


## Installation

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/resilient-scraper.git
cd resilient-scraper
```

### 2. Set Up a Virtual Environment & Dependencies
```bash
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate

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
| `TARGET_URL` | `https://books.toscrape.com/` | Target URL to scrape |
| `PROXY_URL` | *(Public SOCKS5 API)* | Public proxy list URL (SOCKS5 format) |
| `MAX_TOTAL_ATTEMPTS` | `15` | Total failure limit before script termination |
| `HEADLESS` | `true` | Run Chromium without a graphical user interface |
| `MAX_PROFILES_TO_KEEP` | `5` | Maximum browser profile folders retained on disk |


## Usage

Run the main asynchronous controller:
```bash
python scraper.py
```

### Log Output Structure
The application writes logs structured in JSON format to standard output, making it compatible with ELK, Datadog, or localized JSON parsing tools.

```json
{"component": "Scraper", "event": "Attempting scrape cycle", "proxy": "socks5://192.168.1.1:1080", "attempt": 1, "total": 1, "level": "info", "timestamp": "2026-07-01T00:15:30.123456Z"}
```


## Project Structure

```text
├── .env.example          # Sample environment configurations
├── .gitignore            # Excludes logs, credentials, and local profiles
├── scraper.py            # Main crawler script (contains core engine & runner)
├── requirements.txt      # List of pinning dependencies
└── README.md             # Project documentation
```