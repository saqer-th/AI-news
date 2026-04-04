import contextlib
import feedparser
import html
import io
import random
import re
import time
import urllib.parse
import urllib3
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from urllib.parse import urlparse
import requests
from utils import setup_logger
try:
    from bs4 import BeautifulSoup as _BS4
    _BS4_AVAILABLE = True
except ImportError:
    _BS4_AVAILABLE = False
try:
    import undetected_chromedriver as uc  # selenium fallback only for Google News
    from selenium.webdriver.common.by import By as _SeleniumBy
except ImportError:
    uc = None
    _SeleniumBy = None

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = setup_logger(__name__)
_RESOLVE_CACHE: dict[str, str | None] = {}
_driver = None


def _matches_domain(domain: str, target: str) -> bool:
    """Return True if domain equals target or is a subdomain of target.

    Examples:
        _matches_domain("a6.alriyadh.com", "alriyadh.com")  -> True
        _matches_domain("www.alriyadh.com", "alriyadh.com") -> True  (www stripped)
        _matches_domain("alriyadh.com", "alriyadh.com")     -> True
        _matches_domain("sabq.org", "alriyadh.com")         -> False
    """
    return domain == target or domain.endswith("." + target)

ALLOWED_DOMAINS = [
    "alriyadh.com",
    "okaz.com.sa",
    "aawsat.com",
    "alyaum.com",
    "sabq.org",
    "argaam.com",
    "maaal.com",
    "saudiexchange.sa",
    "aleqt.com",
    "cnbcarabia.com",
    "alarabiya.net",
    "spa.gov.sa",
]

BASE_ARABIC_FINANCE_QUERY = (
    "(\u0627\u0642\u062a\u0635\u0627\u062f OR "
    "\u062a\u0627\u0633\u064a OR "
    "\u0623\u0639\u0645\u0627\u0644 OR "
    "\u0645\u0627\u0644\u064a\u0629)"
)


def build_rss_sources(days_back: int = 7) -> list[str]:
    recency_clause = f" when:{days_back}d" if days_back > 0 else ""
    return [
        (
            "https://news.google.com/rss/search?q="
            f"{urllib.parse.quote(BASE_ARABIC_FINANCE_QUERY + recency_clause + ' site:' + domain)}"
            "&hl=ar&gl=SA&ceid=SA:ar"
        )
        for domain in ALLOWED_DOMAINS
    ]

SIMAH_KEYWORDS = [
    "simah",
    "\u0633\u0645\u0629",
    "\u0627\u0644\u0634\u0631\u0643\u0629 \u0627\u0644\u0633\u0639\u0648\u062f\u064a\u0629 \u0644\u0644\u0645\u0639\u0644\u0648\u0645\u0627\u062a \u0627\u0644\u0627\u0626\u062a\u0645\u0627\u0646\u064a\u0629",
]
SAMA_KEYWORDS = [
    "sama",
    "\u0633\u0627\u0645\u0627",
    "\u0627\u0644\u0628\u0646\u0643 \u0627\u0644\u0645\u0631\u0643\u0632\u064a",
]
FINANCIAL_KEYWORDS = [
    "\u0645\u0627\u0644\u064a",
    "\u0627\u0642\u062a\u0635\u0627\u062f",
    "\u0628\u0646\u0648\u0643",
    "\u0628\u0646\u0643",
    "\u0623\u0633\u0647\u0645",
    "\u062a\u062f\u0627\u0648\u0644",
    "\u0627\u0633\u062a\u062b\u0645\u0627\u0631",
    "\u062a\u0645\u0648\u064a\u0644",
    "\u0623\u0631\u0628\u0627\u062d",
    "\u062e\u0633\u0627\u0626\u0631",
    "\u0633\u0648\u0642 \u0627\u0644\u0623\u0648\u0631\u0627\u0642 \u0627\u0644\u0645\u0627\u0644\u064a\u0629",
]
ECONOMY_KEYWORDS = [
    "\u0627\u0642\u062a\u0635\u0627\u062f \u0633\u0639\u0648\u062f\u064a",
    "\u0631\u0624\u064a\u0629 2030",
    "\u0627\u0644\u0627\u0642\u062a\u0635\u0627\u062f \u0627\u0644\u0633\u0639\u0648\u062f\u064a",
    "\u0627\u0644\u0645\u0645\u0644\u0643\u0629",
    "\u0627\u0644\u0646\u0627\u062a\u062c \u0627\u0644\u0645\u062d\u0644\u064a",
]

BUSINESS_PRIORITY = {
    "simah": 4,
    "sama": 3,
    "financial": 2,
    "economy": 1,
    "general": 0,
}

PROBLEMATIC_DATE_DOMAINS = {"sabq.org", "maaal.com", "spa.gov.sa"}


def clean_html(raw_html: str) -> str:
    if not raw_html:
        return ""
    clean_text = re.sub(r"<[^>]+>", "", raw_html)
    return html.unescape(clean_text).strip()


# --- Lightweight date parsing helpers (no Playwright) ---
ARABIC_MONTHS = {
    "يناير": 1,
    "فبراير": 2,
    "مارس": 3,
    "أبريل": 4,
    "ابريل": 4,
    "إبريل": 4,    # kasra form used by alriyadh.com
    "مايو": 5,
    "يونيو": 6,
    "يونيه": 6,
    "يوليو": 7,
    "يوليه": 7,
    "أغسطس": 8,
    "اغسطس": 8,
    "سبتمبر": 9,
    "أكتوبر": 10,
    "اكتوبر": 10,
    "نوفمبر": 11,
    "ديسمبر": 12,
}
ARABIC_DIGIT_TRANSLATION = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")



def normalize_domain(url: str) -> str:
    host = urlparse(url).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def get_driver():
    global _driver

    if _driver is None:
        if uc is None:
            raise RuntimeError("undetected_chromedriver is unavailable")

        options = uc.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-gpu")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            _driver = uc.Chrome(options=options)

    return _driver


def close_driver() -> None:
    global _driver
    if _driver:
        try:
            _driver.quit()
        finally:
            _driver = None


# ---------------------------------------------------------------------------
# Visible (non-headless) Chrome driver — used for sites that block headless
# ---------------------------------------------------------------------------
_visible_driver = None


def get_visible_driver():
    """Return a regular, VISIBLE Chrome driver (no headless).

    Some sites (e.g. maaal.com) actively detect and block headless browsers.
    Regular Selenium with a visible window bypasses this.
    """
    global _visible_driver
    if _visible_driver is None:
        try:
            from selenium import webdriver as _sw
            from selenium.webdriver.chrome.options import Options as _ChromeOpts
            opts = _ChromeOpts()
            # No --headless flag — deliberately visible
            opts.add_argument("--disable-blink-features=AutomationControlled")
            opts.add_experimental_option("excludeSwitches", ["enable-automation"])
            opts.add_experimental_option("useAutomationExtension", False)
            _visible_driver = _sw.Chrome(options=opts)
        except Exception as exc:
            raise RuntimeError(f"Could not start visible Chrome: {exc}") from exc
    return _visible_driver


def close_visible_driver() -> None:
    global _visible_driver
    if _visible_driver:
        with contextlib.suppress(Exception):
            _visible_driver.quit()
        _visible_driver = None


# ---------------------------------------------------------------------------
# Google News URL extraction from RSS entry
# ---------------------------------------------------------------------------
# Google News RSS <description> always embeds the real article URL as an
# <a href="REAL_URL"> link, so we can extract it at parse time — zero
# extra network calls, zero dependency on JS redirects.
# ---------------------------------------------------------------------------

# Paths that indicate a tag/category/infographic page rather than an article.
_NON_ARTICLE_PATH_RE = re.compile(
    r"(/tags?/|/topics?/|/category/|/categories/|/infographic/"
    r"|/search[/?]|[?&]page=\d+|[?&]_wrapper_format=)",
    re.IGNORECASE,
)


def _is_article_url(url: str) -> bool:
    """Return False for tag, category, infographic, and paginated pages."""
    return not _NON_ARTICLE_PATH_RE.search(url)


def _extract_real_url_from_rss_entry(entry) -> str | None:
    """Return the actual publisher article URL embedded in a Google News RSS entry."""
    # 1. feedparser sometimes puts the canonical URL in entry.links
    for link_obj in entry.get("links", []):
        href = link_obj.get("href", "")
        if href and href.startswith("http") and "news.google.com" not in href and _is_article_url(href):
            return href

    # 2. The <description> / summary HTML contains <a href="REAL_URL">Title</a>
    for field in ("summary", "description", "summary_detail"):
        raw = entry.get(field, "")
        if isinstance(raw, dict):
            raw = raw.get("value", "")
        if not raw:
            continue
        matches = re.findall(r'href=["\']([^"\']+)["\']', raw, re.IGNORECASE)
        for href in matches:
            if href.startswith("http") and "news.google.com" not in href and _is_article_url(href):
                return href

    # 3. content[] field in some feed formats
    for content in entry.get("content", []):
        value = content.get("value", "")
        matches = re.findall(r'href=["\']([^"\']+)["\']', value, re.IGNORECASE)
        for href in matches:
            if href.startswith("http") and "news.google.com" not in href and _is_article_url(href):
                return href

    return None


def resolve_final_url(url: str) -> str | None:
    """Resolve a Google News redirect URL to the real publisher URL.

    Resolution order:
      1. Cache hit — free.
      2. URL is already non-Google — return immediately.
      3. HTTP request with browser headers — handles 302 redirects & meta-refresh.
      4. Selenium — last resort, waits for JS navigation to complete.
    """
    if not url:
        return None

    if url in _RESOLVE_CACHE:
        return _RESOLVE_CACHE[url]

    parsed = urlparse(url)
    if "news.google.com" not in parsed.netloc.lower():
        _RESOLVE_CACHE[url] = url
        return url

    # --- Strategy 1: HTTP request (handles 302 redirects and meta-refresh) ---
    try:
        resp = requests.get(
            url,
            timeout=8,
            allow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "ar-SA,ar;q=0.9,en;q=0.8",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )
        resolved = resp.url
        if resolved and "news.google.com" not in urlparse(resolved).netloc.lower():
            logger.debug(f"Resolved via HTTP redirect: {resolved}")
            _RESOLVE_CACHE[url] = resolved
            return resolved

        # Google sometimes uses window.location or meta-refresh JS redirects
        location_match = re.search(
            r'(?:window\.location(?:\.href)?\s*=\s*|url=|content=["\']\d+;\s*url=)["\']?(https?://[^"\'\s;]+)',
            resp.text,
            re.IGNORECASE,
        )
        if location_match:
            candidate = location_match.group(1)
            if "news.google.com" not in urlparse(candidate).netloc.lower():
                logger.debug(f"Resolved via page redirect hint: {candidate}")
                _RESOLVE_CACHE[url] = candidate
                return candidate
    except Exception as exc:
        logger.warning(f"HTTP resolution failed for {url}: {exc}")

    # --- Strategy 2: Selenium with explicit wait for URL change ---
    if uc is None:
        _RESOLVE_CACHE[url] = None
        logger.warning(f"Failed to resolve (no Selenium): {url}")
        return None

    try:
        logger.debug("Using Selenium fallback for URL resolution")
        driver = get_driver()
        driver.get(url)

        # Wait up to 8 s for the URL to leave news.google.com
        deadline = time.time() + 8
        while time.time() < deadline:
            current = driver.current_url
            if current and "news.google.com" not in urlparse(current).netloc.lower():
                logger.debug(f"Resolved via Selenium: {current}")
                _RESOLVE_CACHE[url] = current
                return current
            time.sleep(0.4)
    except Exception as exc:
        logger.warning(f"Selenium resolution error: {exc}")
        close_driver()

    _RESOLVE_CACHE[url] = None
    logger.warning(f"Failed to resolve: {url}")
    return None



def normalize_arabic_digits(text: str) -> str:
    return (text or "").translate(ARABIC_DIGIT_TRANSLATION)


def parse_page_date_text(date_text: str) -> datetime | None:
    if not date_text:
        return None
    normalized = normalize_arabic_digits(clean_html(date_text))
    normalized = re.sub(r"\s+", " ", normalized).strip()
    normalized = normalized.replace("،", " ").replace(",", " ")
    normalized = re.sub(r"\s+في\s+\d{1,2}:\d{2}\s*\S*", "", normalized)

    def _to_gregorian_year(y: int) -> int:
        """Convert a Hijri year to approximate Gregorian year.

        Hijri years 1380-1480 correspond roughly to 1960-2058 CE.
        Formula: G ≈ H + 622 − H/33  (accurate to ±1 year)
        """
        if 1380 <= y <= 1480:
            return y + 622 - y // 33
        return y

    m = re.search(r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})", normalized)
    if m:
        y, mo, d = map(int, m.groups())
        y = _to_gregorian_year(y)
        try:
            return datetime(y, mo, d, tzinfo=timezone.utc)
        except ValueError:
            return None

    # Try every date-shaped substring — the first match may be a Hijri date
    # with an unrecognised month name (e.g. 'شوال'), so we must keep trying.
    for m in re.finditer(r"(\d{1,2})\s+([^\s\d]+?[\u0600-\u06FF][^\s\d]*)\s+(\d{4})", normalized):
        day = int(m.group(1))
        month_name = m.group(2).strip().lower()
        year = int(m.group(3))
        year = _to_gregorian_year(year)
        mo = ARABIC_MONTHS.get(month_name)
        if mo:
            try:
                return datetime(year, mo, day, tzinfo=timezone.utc)
            except ValueError:
                continue
    return None


def extract_page_date(url: str) -> datetime | None:
    domain = normalize_domain(url)

    # alriyadh.com works fine with plain requests (div.article-time time)
    # Only these sites genuinely need Selenium
    _SELENIUM_DOMAINS = {"sabq.org", "maaal.com", "spa.gov.sa"}
    if any(_matches_domain(domain, d) for d in _SELENIUM_DOMAINS):
        try:
            driver = get_driver()
            driver.get(url)
            driver.implicitly_wait(3)

            source = driver.page_source

            if _matches_domain(domain, "sabq.org"):
                m = re.search(r"\d{1,2}\s+\S+\s+\d{4}.*?في.*", source, re.DOTALL)
                if m:
                    parsed = parse_page_date_text(m.group(0))
                    if parsed:
                        return parsed

            if _matches_domain(domain, "maaal.com"):
                # maaal.com blocks headless browsers — use a visible Chrome window
                try:
                    vis = get_visible_driver()
                    vis.get(url)
                    time.sleep(5)  # wait for JS to render the date
                    if _SeleniumBy is not None:
                        el = vis.find_element(_SeleniumBy.CSS_SELECTOR, "span.text-fontColorSecondary")
                        date_text = el.text.strip()
                        logger.debug(f"maaal.com visible driver text: {date_text!r}")
                        parsed = parse_page_date_text(date_text)
                        if parsed:
                            return parsed
                    # Fallback: regex on rendered page source
                    m = re.search(r"\d{1,2}\s+[\u0600-\u06FF]+\s+\d{4}", vis.page_source)
                    if m:
                        parsed = parse_page_date_text(m.group(0))
                        if parsed:
                            return parsed
                except Exception as e:
                    logger.warning(f"maaal.com visible driver error for {url}: {e}")
                    close_visible_driver()

            if _matches_domain(domain, "spa.gov.sa"):
                # Accept date with OR without the Arabic م/هـ suffix
                m = re.search(r"\d{1,2}\s+[\u0600-\u06FF]+\s+\d{4}(?:\s*[مهـ])?", source)
                if m:
                    parsed = parse_page_date_text(m.group(0))
                    if parsed:
                        return parsed

        except Exception as e:
            logger.warning(f"Selenium date extraction error for {url}: {e}")

    return extract_page_date_requests_only(url)


def extract_page_date_requests_only(url: str) -> datetime | None:
    """Lightweight HTTP-only date extraction. Used as the fallback after Selenium."""
    domain = normalize_domain(url)

    # ------------------------------------------------------------------
    # alriyadh.com: handled FIRST (before the generic request below)
    # because a6.* subdomains need verify=False and a CSS-selector approach.
    # ------------------------------------------------------------------
    if _matches_domain(domain, "alriyadh.com"):
        try:
            resp_ar = requests.get(
                url,
                timeout=12,
                allow_redirects=True,
                verify=False,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/124.0.0.0"
                    ),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "ar-SA,ar;q=0.9,en;q=0.8",
                    "Referer": "https://www.alriyadh.com/",
                },
            )
            logger.info(
                f"alriyadh.com fetch: status={resp_ar.status_code} "
                f"len={len(resp_ar.text)} bs4={_BS4_AVAILABLE} url={url}"
            )
            resp_ar.encoding = "utf-8"
            html = resp_ar.text

            # --- Priority 1: <time datetime="YYYY-MM-DD..."> attribute (ISO, no Arabic parsing) ---
            m = re.search(
                r'<div[^>]*article-time[^>]*>.*?<time[^>]*datetime="([^"]+)"',
                html,
                re.IGNORECASE | re.DOTALL,
            )
            if m:
                parsed = parse_page_date_text(m.group(1))
                logger.info(f"alriyadh.com datetime attr={m.group(1)!r} parsed={parsed}")
                if parsed:
                    return parsed

            # --- Priority 2: BeautifulSoup text extraction ---
            if _BS4_AVAILABLE:
                soup = _BS4(html, "html.parser")
                time_tag = soup.select_one("div.article-time time")
                logger.info(f"alriyadh.com BS4 time_tag={time_tag!r}")
                if time_tag:
                    dt_attr = time_tag.get("datetime", "")
                    if dt_attr:
                        parsed = parse_page_date_text(dt_attr)
                        if parsed:
                            return parsed
                    parsed = parse_page_date_text(time_tag.get_text(strip=True))
                    logger.info(f"alriyadh.com BS4 text={time_tag.get_text(strip=True)!r} parsed={parsed}")
                    if parsed:
                        return parsed
            else:
                # Regex equivalent of soup.select_one("div.article-time time").get_text()
                # Matches the visible text inside <time> when there is no datetime attribute.
                m2 = re.search(
                    r'<div[^>]*article-time[^>]*>.*?<time[^>]*>([^<]+)</time>',
                    html,
                    re.IGNORECASE | re.DOTALL,
                )
                if m2:
                    text = m2.group(1).strip()
                    parsed = parse_page_date_text(text)
                    logger.info(f"alriyadh.com regex text={text!r} parsed={parsed}")
                    if parsed:
                        return parsed

            # --- Priority 3: any <time datetime="..."> in the full page ---
            for dt in re.findall(r'<time[^>]*datetime="([^"]+)"', html, re.IGNORECASE):
                parsed = parse_page_date_text(dt)
                if parsed:
                    logger.info(f"alriyadh.com fallback datetime={dt!r} parsed={parsed}")
                    return parsed


        except Exception as exc:
            logger.warning(f"alriyadh.com date extraction failed for {url}: {exc}")
        return None  # don't fall through to the generic request path

    try:
        resp = requests.get(
            url,
            timeout=10,
            allow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept-Language": "ar-SA,ar;q=0.9,en;q=0.8",
            },
        )
        resp.raise_for_status()
        # Force UTF-8 so we never get mojibake when matching Arabic text
        resp.encoding = resp.apparent_encoding or "utf-8"
        html_text = resp.text

        # --- Domain-specific fast paths ---
        if _matches_domain(domain, "sabq.org"):
            for span_text in re.findall(r"<span[^>]*>([^<]+)</span>", html_text, flags=re.IGNORECASE):
                cleaned = clean_html(span_text)
                if re.search(r"\d{1,2}\s+\S+\s+\d{4}", cleaned):
                    parsed = parse_page_date_text(cleaned)
                    if parsed:
                        return parsed
            m = re.search(r"(\d{1,2}\s+\S+\s+\d{4})\s*في", html_text)
            if m:
                parsed = parse_page_date_text(m.group(1))
                if parsed:
                    return parsed

        if _matches_domain(domain, "maaal.com"):
            for candidate in re.findall(r'"datePublished"\s*:\s*"([^"]+)"', html_text, flags=re.IGNORECASE):
                parsed = parse_page_date_text(candidate)
                if parsed:
                    return parsed
            m = re.search(r"\d{1,2}\s+[\u0600-\u06FF]+\s+\d{4}", html_text)
            if m:
                parsed = parse_page_date_text(m.group(0))
                if parsed:
                    return parsed

        if _matches_domain(domain, "spa.gov.sa"):
            m = re.search(r'meta name="description" content="([^"]+)"', html_text, flags=re.IGNORECASE)
            if m:
                date_match = re.search(r"\d{1,2}\s+[\u0600-\u06FF]+\s+\d{4}\s*[مهـ]", m.group(1))
                if date_match:
                    parsed = parse_page_date_text(date_match.group(0))
                    if parsed:
                        return parsed
            m = re.search(r"\d{1,2}\s+[\u0600-\u06FF]+\s+\d{4}\s*[مهـ]", html_text)
            if m:
                parsed = parse_page_date_text(m.group(0))
                if parsed:
                    return parsed

        # --- Universal patterns (work for most sites) ---
        patterns = [
            r'<time[^>]*datetime="([^"]+)"',
            r'meta property="article:published_time" content="([^"]+)"',
            r'meta property="og:updated_time" content="([^"]+)"',
            r'meta name="pubdate" content="([^"]+)"',
            r'meta name="publishdate" content="([^"]+)"',
            r'meta name="date" content="([^"]+)"',
            r'meta itemprop="datePublished" content="([^"]+)"',
            r'class="[^"]*(?:date-posted|entry-date|post-date|article-date|news-date)[^"]*"[^>]*>([^<]+)<',
        ]
        candidates: list[str] = []
        for pat in patterns:
            candidates.extend(re.findall(pat, html_text, flags=re.IGNORECASE | re.DOTALL))

        candidates.extend(re.findall(r'"datePublished"\s*:\s*"([^"]+)"', html_text, flags=re.IGNORECASE))
        candidates.extend(re.findall(r'\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}\b', html_text))
        candidates.extend(re.findall(r'\b\d{4}-\d{2}-\d{2}\b', html_text))
        candidates.extend(re.findall(r'\b\d{2}/\d{2}/\d{4}\b', html_text))

        for candidate in candidates:
            parsed = parse_page_date_text(candidate)
            if parsed:
                return parsed

    except Exception as exc:
        logger.debug(f"requests-only date extraction failed for {url}: {exc}")
        return None
    return None


def is_allowed_domain(url: str) -> bool:
    host = normalize_domain(url)
    return any(host == domain or host.endswith(f".{domain}") for domain in ALLOWED_DOMAINS)


def detect_business_bucket(title: str, description: str) -> str:
    text = f"{title} {description}".lower()
    if any(keyword in text for keyword in SIMAH_KEYWORDS):
        return "simah"
    if any(keyword in text for keyword in SAMA_KEYWORDS):
        return "sama"
    if any(keyword in text for keyword in FINANCIAL_KEYWORDS):
        return "financial"
    if any(keyword in text for keyword in ECONOMY_KEYWORDS):
        return "economy"
    return "general"


def score_news(title: str, description: str) -> int:
    text = f"{title} {description}".lower()
    score = 0

    if any(keyword in text for keyword in SIMAH_KEYWORDS):
        score += 100
    if any(keyword in text for keyword in SAMA_KEYWORDS):
        score += 80
    if any(keyword in text for keyword in FINANCIAL_KEYWORDS):
        score += 50
    if any(keyword in text for keyword in ECONOMY_KEYWORDS):
        score += 30

    return score


def normalize_title_for_dedupe(title: str) -> str:
    normalized = clean_html(title).lower()
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = re.sub(r"[^\w\u0600-\u06FF ]+", " ", normalized)
    return normalized.strip()


def is_near_duplicate(title: str, existing_titles: list[str]) -> bool:
    normalized = normalize_title_for_dedupe(title)
    if not normalized:
        return False

    normalized_tokens = set(normalized.split())
    for existing in existing_titles:
        if normalized == existing:
            return True

        if SequenceMatcher(None, normalized, existing).ratio() >= 0.9:
            return True

        existing_tokens = set(existing.split())
        if normalized_tokens and existing_tokens:
            overlap = len(normalized_tokens & existing_tokens) / max(len(normalized_tokens), len(existing_tokens))
            if overlap >= 0.8:
                return True

    return False


def parse_date(date_str: str) -> datetime:
    try:
        parsed = parsedate_to_datetime(date_str)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)


def is_recent_news(date_str: str, days_back: int = 7) -> bool:
    published_at = parse_date(date_str)
    if published_at == datetime.min.replace(tzinfo=timezone.utc):
        return False

    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    return published_at >= cutoff


def validate_article(item: dict, days_back: int = 7) -> tuple[bool, str]:
    link = item.get("link", "")
    final_url = resolve_final_url(link)

    if not final_url:
        logger.info(f"Rejected: unresolved URL {link}")
        return False, final_url or ""

    # Reject tag/category/infographic pages — they can never have an article date
    if not _is_article_url(final_url):
        logger.info(f"Rejected: non-article URL {final_url}")
        return False, final_url

    logger.debug(f"Validating article: {final_url}")
    page_date = extract_page_date(final_url)

    if page_date:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
        if page_date >= cutoff:
            return True, final_url
        logger.info(f"Rejected: old page date ({page_date.date()}) {final_url}")
        return False, final_url

    logger.info(f"Rejected: no page date {final_url}")
    return False, final_url



def fetch_valid_news(target: int = 25, max_batches: int = 6, days_back: int = 7) -> list:
    collected = []
    seen_titles = set()
    seen_links = set()
    try:
        for attempt in range(1, max_batches + 1):
            batch = fetch_rss_news(limit=50, days_back=days_back)
            # avoid same ordering each run
            random.shuffle(batch)
            if not batch:
                logger.warning(f"Batch {attempt} returned empty feed.")
                continue

            for item in batch:
                title = item.get("title", "")
                link = item.get("link", "")
                if title in seen_titles or link in seen_links:
                    continue

                is_valid, resolved_link = validate_article(item, days_back=days_back)
                if not is_valid:
                    continue

                item["link"] = resolved_link or link
                seen_titles.add(title)
                seen_links.add(item["link"])
                collected.append(item)

                if len(collected) >= target:
                    logger.info(f"Collected {len(collected)} recent articles (target {target}).")
                    return collected

        logger.info(f"Collected {len(collected)} articles after {max_batches} batches (target {target}).")
        return collected
    finally:
        close_driver()


def fetch_rss_news(limit: int = 25, days_back: int = 7) -> list:
    logger.info("Aggregating news from curated financial sources...")
    all_news = []
    rss_sources = build_rss_sources(days_back=days_back)

    for url in rss_sources:
        try:
            feed = feedparser.parse(url)
            if feed.bozo and feed.bozo_exception:
                logger.warning(f"Feed parser warning for {url}: {feed.bozo_exception}")

            for entry in feed.entries[:30]:
                title = clean_html(entry.get("title", "No Title"))

                # Prefer the real publisher URL embedded in the RSS entry.
                # Google News <description> always wraps the article with
                # <a href="REAL_URL">, so we extract it here instead of
                # storing the CBMi... redirect URL which is unresolvable.
                real_url = _extract_real_url_from_rss_entry(entry)
                link = real_url or entry.get("link", "")

                description = clean_html(entry.get("summary", "No Description"))
                published = entry.get("published", "")

                is_allowed = False
                source_href = entry.get("source", {}).get("href", "").lower()
                source_title = entry.get("source", {}).get("title", "").lower()
                title_lower = title.lower()
                link_lower = link.lower()
                source_name = source_title if source_title else "Unknown"

                for domain in ALLOWED_DOMAINS:
                    domain_clean = domain.split(".")[0]
                    if (
                        domain in source_href
                        or domain in source_title
                        or domain in link_lower
                        or domain_clean in source_title
                        or domain_clean in title_lower
                    ):
                        is_allowed = True
                        source_name = domain
                        break

                if not is_allowed:
                    continue

                if " / " in title:
                    parts = title.split(" / ", 1)
                    if len(parts[0].strip()) <= 25:
                        title = parts[1].strip()

                rss_score = score_news(title, description)
                business_bucket = detect_business_bucket(title, description)

                all_news.append(
                    {
                        "title": title,
                        "description": description,
                        "link": link,
                        "published": published,
                        "source": source_name,
                        "score": rss_score,
                        "rss_score": rss_score,
                        "business_bucket": business_bucket,
                        "business_priority": BUSINESS_PRIORITY[business_bucket],
                        "source_domain": normalize_domain(link),
                    }
                )
        except Exception as exc:
            logger.error(f"Error fetching RSS from {url}: {exc}")

    all_news.sort(
        key=lambda item: (item["business_priority"], item["rss_score"], parse_date(item["published"])),
        reverse=True,
    )

    seen_titles = []
    unique_news = []
    for item in all_news:
        if not is_near_duplicate(item["title"], seen_titles):
            seen_titles.append(normalize_title_for_dedupe(item["title"]))
            unique_news.append(item)

    final_selection = unique_news[:limit]
    logger.info(f"Successfully fetched {len(final_selection)} filtered and scored news items.")
    return final_selection
