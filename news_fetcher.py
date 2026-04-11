import contextlib
import feedparser
import glob
import html
import io
import os
import re
import threading
import time
import urllib.parse
import urllib3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import requests
from pipeline_utils import RSS_HEADERS, TTLMemoryCache, load_json_cache, request_with_retry, save_json_cache, stable_hash, utc_now_iso
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
_CACHE_DIR = Path(__file__).resolve().parent / ".cache"
_RESOLVE_CACHE_PATH = _CACHE_DIR / "resolved_urls.json"
_RESOLVE_DISK_CACHE = load_json_cache(_RESOLVE_CACHE_PATH)
_RESOLVE_CACHE = TTLMemoryCache(maxsize=4096, ttl_seconds=12 * 3600)
_RESOLVE_CACHE_DIRTY = False
_SEARCH_CACHE_PATH = _CACHE_DIR / "search_resolved_urls.json"
_SEARCH_DISK_CACHE = load_json_cache(_SEARCH_CACHE_PATH)
_SEARCH_CACHE = TTLMemoryCache(maxsize=2048, ttl_seconds=12 * 3600)
_SEARCH_CACHE_DIRTY = False
_driver_lock = threading.RLock()
_visible_driver_lock = threading.RLock()
_driver = None


def _emit_progress(progress_callback, *, stage: str, message: str, progress: float | None = None, **details) -> None:
    if progress_callback is None:
        return

    payload = {
        "stage": stage,
        "message": message,
    }
    if progress is not None:
        payload["progress"] = max(0.0, min(1.0, float(progress)))
    payload.update(details)

    try:
        progress_callback(payload)
    except Exception as exc:
        logger.debug(f"Progress callback failed during stage '{stage}': {exc}")


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

# Query targets Saudi-specific financial news by combining topic terms with
# mandatory Saudi geographic/institutional context anchors.
# The AND clause dramatically reduces off-topic international results.
BASE_ARABIC_FINANCE_QUERY = (
    "("
    "\u0627\u0642\u062a\u0635\u0627\u062f OR "     # اقتصاد
    "\u0645\u0627\u0644\u064a\u0629 OR "           # مالية
    "\u0623\u0639\u0645\u0627\u0644 OR "           # أعمال
    "\u0627\u0633\u062a\u062b\u0645\u0627\u0631 OR "# استثمار
    "\u0628\u0646\u0648\u0643 OR "                # بنوك
    "\u062a\u0627\u0633\u064a"                    # تاسي
    ") AND ("
    "\u0627\u0644\u0633\u0639\u0648\u062f\u064a\u0629 OR "  # السعودية
    "\u0633\u0639\u0648\u062f\u064a OR "                   # سعودي
    "\u0627\u0644\u0645\u0645\u0644\u0643\u0629"           # المملكة
    ")"
)


BASE_ENGLISH_FINANCE_QUERY = (
    "("
    "economy OR economic OR finance OR financial OR banking OR bank OR "
    "investment OR regulation OR regulatory OR inflation OR gdp OR budget OR "
    "\"capital market authority\" OR \"central bank\""
    ") AND ("
    "\"Saudi Arabia\" OR Saudi OR Riyadh OR kingdom OR Tadawul OR SAMA OR "
    "\"Vision 2030\" OR \"Ministry of Finance\""
    ")"
)
ENGLISH_SOURCE_TARGETS = {
    "aawsat.com": "english.aawsat.com",
    "alarabiya.net": "english.alarabiya.net",
    "argaam.com": "argaam.com/en",
    "spa.gov.sa": "spa.gov.sa/en",
}


def _build_domain_rss_source(
    query: str,
    domain: str,
    *,
    days_back: int,
    language: str,
) -> str:
    recency_clause = f" when:{days_back}d" if days_back > 0 else ""
    return (
        "https://news.google.com/rss/search?q="
        f"{urllib.parse.quote(query + recency_clause + ' site:' + domain)}"
        f"&hl={language}&gl=SA&ceid=SA:{language}"
    )


def build_rss_sources(days_back: int = 7) -> list[str]:
    sources: list[str] = []
    for domain in ALLOWED_DOMAINS:
        sources.append(
            _build_domain_rss_source(
                BASE_ARABIC_FINANCE_QUERY,
                domain,
                days_back=days_back,
                language="ar",
            )
        )
        english_target = ENGLISH_SOURCE_TARGETS.get(domain)
        if english_target:
            sources.append(
                _build_domain_rss_source(
                    BASE_ENGLISH_FINANCE_QUERY,
                    english_target,
                    days_back=days_back,
                    language="en",
                )
            )
    return sources

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
FINANCIAL_KEYWORDS.extend(
    [
        "finance",
        "financial",
        "banking",
        "bank",
        "stock",
        "stocks",
        "share",
        "shares",
        "tadawul",
        "investment",
        "fund",
        "capital",
        "ipo",
        "offering",
        "credit",
        "insurance",
        "asset",
        "market",
    ]
)
ECONOMY_KEYWORDS.extend(
    [
        "saudi economy",
        "economy",
        "economic",
        "economic growth",
        "vision 2030",
        "gdp",
        "inflation",
        "budget",
        "fiscal",
        "non-oil",
        "oil revenue",
    ]
)
ENGLISH_SAUDI_SCORE_KEYWORDS = [
    "saudi arabia",
    "saudi",
    "riyadh",
    "kingdom",
    "vision 2030",
    "ministry of finance",
]

REGULATORY_INTEL_KEYWORDS = [
    "regulation",
    "regulatory",
    "capital market authority",
    "cma",
    "license",
    "compliance",
    "governance",
    "ministry of finance",
    "insurance authority",
    "central bank",
]
BANKING_INTEL_KEYWORDS = [
    "banking",
    "bank",
    "credit",
    "loan",
    "deposit",
    "liquidity",
    "mortgage",
    "payment",
    "fintech",
    "open banking",
]
MACRO_INTEL_KEYWORDS = [
    "gdp",
    "inflation",
    "fiscal",
    "budget",
    "economic growth",
    "non-oil",
    "oil revenue",
    "trade balance",
    "current account",
    "vision 2030",
]
STOCK_DIVIDEND_NOISE_KEYWORDS = [
    "dividend",
    "cash dividend",
    "bonus shares",
    "earnings per share",
    "eps",
    "ex-dividend",
    "rights issue",
]

BUSINESS_PRIORITY = {
    "simah": 4,
    "sama": 3,
    "financial": 2,
    "economy": 1,
    "general": 0,
}

PROBLEMATIC_DATE_DOMAINS = {"sabq.org", "maaal.com", "spa.gov.sa"}
_ARABIC_DIACRITICS_RE = re.compile(r"[\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06ED]")
_ARABIC_CHAR_RE = re.compile(r"[\u0600-\u06FF]")
_ENGLISH_CHAR_RE = re.compile(r"[A-Za-z]")
_SOURCE_SEPARATOR_RE = re.compile(r"\s*(?:\||-|\u2013|\u2014)\s*")
_SEARCH_RESULT_LIMIT = 5
_MAX_ENGLISH_ARTICLES = 10
_SEARCH_PROVIDER_HOSTS = {
    "google.com",
    "www.google.com",
    "news.google.com",
    "duckduckgo.com",
    "www.duckduckgo.com",
    "bing.com",
    "www.bing.com",
}
_SOURCE_HINT_DOMAIN_MAP = {
    "argaam": "argaam.com",
    "أرقام": "argaam.com",
    "okaz": "okaz.com.sa",
    "عكاظ": "okaz.com.sa",
    "sabq": "sabq.org",
    "سبق": "sabq.org",
    "maaal": "maaal.com",
    "مال": "maaal.com",
    "aawsat": "aawsat.com",
    "الشرق الأوسط": "aawsat.com",
    "alyaum": "alyaum.com",
    "اليوم": "alyaum.com",
    "saudiexchange": "saudiexchange.sa",
    "تداول": "saudiexchange.sa",
    "تداول السعودية": "saudiexchange.sa",
    "aleqt": "aleqt.com",
    "الاقتصادية": "aleqt.com",
    "cnbc عربية": "cnbcarabia.com",
    "cnbc arabia": "cnbcarabia.com",
    "العربية": "alarabiya.net",
    "alarabiya": "alarabiya.net",
    "واس": "spa.gov.sa",
    "وكالة الأنباء السعودية": "spa.gov.sa",
    "spa": "spa.gov.sa",
    "alriyadh": "alriyadh.com",
    "الرياض": "alriyadh.com",
}
_COMMON_SOURCE_SUFFIXES = {
    "bloomberg",
    "reuters",
    "cnn",
    "bbc",
    "cnbc",
    "financial times",
    "the wall street journal",
}


def _normalize_text_for_match(text: str) -> str:
    normalized = clean_html(text or "")
    normalized = normalize_arabic_digits(normalized)
    normalized = normalized.replace("\u200f", " ").replace("\u200e", " ").replace("\ufeff", " ")
    normalized = _ARABIC_DIACRITICS_RE.sub("", normalized)
    normalized = normalized.replace("أ", "ا").replace("إ", "ا").replace("آ", "ا")
    normalized = normalized.replace("ٱ", "ا").replace("ى", "ي")
    normalized = re.sub(r"[^\w\s\u0600-\u06FF]", " ", normalized, flags=re.UNICODE)
    normalized = re.sub(r"\s+", " ", normalized).strip().lower()
    return normalized


def _looks_like_source_suffix(fragment: str) -> bool:
    normalized = _normalize_text_for_match(fragment)
    if not normalized:
        return False
    if normalized in _COMMON_SOURCE_SUFFIXES:
        return True
    if _normalize_source_hint(fragment):
        return True
    return False


def clean_title(title: str) -> str:
    cleaned = clean_html(title or "")
    cleaned = normalize_arabic_digits(cleaned)
    cleaned = cleaned.replace("\u200f", " ").replace("\u200e", " ").replace("\ufeff", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" \t\r\n-|")
    for _ in range(3):
        match = re.match(r"^(.*?)(?:\s*(?:\||-|\u2013|\u2014)\s*)([^|\u2013\u2014-]{1,80})$", cleaned)
        if not match:
            break
        candidate = match.group(2).strip()
        if not _looks_like_source_suffix(candidate):
            break
        cleaned = match.group(1).strip(" \t\r\n-|")
    return re.sub(r"\s+", " ", cleaned).strip()


def _normalize_source_hint(source_hint: str | None) -> str | None:
    raw_hint = clean_html(source_hint or "").strip()
    if not raw_hint:
        return None

    host_candidate = raw_hint
    if "://" in raw_hint:
        host_candidate = normalize_domain(raw_hint)
    elif "." in raw_hint and " " not in raw_hint and "/" not in raw_hint:
        host_candidate = normalize_domain(f"https://{raw_hint}")

    if host_candidate:
        for allowed in ALLOWED_DOMAINS:
            if _matches_domain(host_candidate, allowed):
                return allowed

    normalized = _normalize_text_for_match(raw_hint)
    if not normalized:
        return None

    for alias, domain in _SOURCE_HINT_DOMAIN_MAP.items():
        alias_normalized = _normalize_text_for_match(alias)
        if alias_normalized and (normalized == alias_normalized or alias_normalized in normalized):
            return domain

    for domain in ALLOWED_DOMAINS:
        domain_token = _normalize_text_for_match(domain.split(".")[0])
        if domain_token and domain_token in normalized:
            return domain

    return None


def _infer_source_domain(*hints: str | None) -> str:
    for hint in hints:
        domain = _normalize_source_hint(hint)
        if domain:
            return domain
    return ""


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


def _get_cached_resolve_value(url: str) -> str | None | object:
    cached = _RESOLVE_CACHE.get(url)
    if isinstance(cached, dict) and "value" in cached:
        return cached.get("value")

    disk_cached = _RESOLVE_DISK_CACHE.get(url)
    if isinstance(disk_cached, dict) and "value" in disk_cached:
        _RESOLVE_CACHE.set(url, disk_cached)
        return disk_cached.get("value")

    return ...


def _set_cached_resolve_value(url: str, value: str | None) -> None:
    global _RESOLVE_CACHE_DIRTY
    payload = {
        "cached_at": utc_now_iso(),
        "value": value,
    }
    _RESOLVE_CACHE.set(url, payload)
    _RESOLVE_DISK_CACHE[url] = payload
    _RESOLVE_CACHE_DIRTY = True


def _persist_resolve_cache() -> None:
    global _RESOLVE_CACHE_DIRTY
    if not _RESOLVE_CACHE_DIRTY:
        return
    try:
        save_json_cache(_RESOLVE_CACHE_PATH, _RESOLVE_DISK_CACHE, max_entries=4000, ttl_seconds=7 * 24 * 3600)
        _RESOLVE_CACHE_DIRTY = False
    except Exception as exc:
        logger.warning(f"Failed to save URL resolution cache: {exc}")


def _build_search_cache_key(title: str, expected_domain: str | None = None) -> str:
    return stable_hash("title-search", _normalize_text_for_match(clean_title(title)), expected_domain or "")


def _get_cached_search_value(cache_key: str) -> str | None | object:
    cached = _SEARCH_CACHE.get(cache_key)
    if isinstance(cached, dict) and "value" in cached:
        return cached.get("value")

    disk_cached = _SEARCH_DISK_CACHE.get(cache_key)
    if isinstance(disk_cached, dict) and "value" in disk_cached:
        _SEARCH_CACHE.set(cache_key, disk_cached)
        return disk_cached.get("value")

    return ...


def _set_cached_search_value(cache_key: str, value: str | None, *, title: str, expected_domain: str | None = None) -> None:
    global _SEARCH_CACHE_DIRTY
    payload = {
        "cached_at": utc_now_iso(),
        "value": value,
        "title": clean_title(title),
        "expected_domain": expected_domain or "",
    }
    _SEARCH_CACHE.set(cache_key, payload)
    _SEARCH_DISK_CACHE[cache_key] = payload
    _SEARCH_CACHE_DIRTY = True


def _persist_search_cache() -> None:
    global _SEARCH_CACHE_DIRTY
    if not _SEARCH_CACHE_DIRTY:
        return
    try:
        save_json_cache(_SEARCH_CACHE_PATH, _SEARCH_DISK_CACHE, max_entries=2000, ttl_seconds=7 * 24 * 3600)
        _SEARCH_CACHE_DIRTY = False
    except Exception as exc:
        logger.warning(f"Failed to save title-search cache: {exc}")


def _retry_cached_google_miss(
    url: str,
    cached_value: str | None,
    article_title: str | None = None,
    source_hint: str | None = None,
) -> str | None | object:
    if cached_value is not None:
        return cached_value

    parsed = urlparse(url or "")
    if "news.google.com" not in parsed.netloc.lower():
        return cached_value

    cleaned_title = clean_title(article_title or "")
    if not cleaned_title:
        return cached_value

    logger.info("Retrying cached Google miss via title-search fallback.")
    fallback_url = search_real_url(cleaned_title, expected_source=source_hint)
    if fallback_url:
        _set_cached_resolve_value(url, fallback_url)
        return fallback_url
    return cached_value


def _cleanup_stale_chromedriver(force: bool = False) -> None:
    """Delete stale chromedriver.exe files left by crashed sessions.

    On Windows, undetected_chromedriver renames the downloaded chromedriver.exe
    to a temp name before patching it.  If a previous session crashed mid-rename
    the file already exists and the next rename raises WinError 183.  We delete
    it proactively so uc can proceed cleanly.
    """
    uc_dir = os.path.join(os.path.expanduser("~"), "appdata", "roaming", "undetected_chromedriver")
    patterns = [
        os.path.join(uc_dir, "**", "chromedriver.exe"),
        os.path.join(uc_dir, "undetected_chromedriver.exe"),
    ]
    for pattern in patterns:
        for path in glob.glob(pattern, recursive=True):
            try:
                os.remove(path)
                logger.debug(f"Removed stale chromedriver file: {path}")
            except PermissionError:
                if force:
                    logger.warning(f"Could not remove locked chromedriver file: {path}")
            except FileNotFoundError:
                pass


def get_driver():
    global _driver

    if _driver is None:
        if uc is None:
            raise RuntimeError("undetected_chromedriver is unavailable")

        _cleanup_stale_chromedriver()

        options = uc.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-gpu")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            # Pin to the major version of the installed Chrome browser.
            # If Chrome updates, bump this number to match.
            try:
                _driver = uc.Chrome(options=options, version_main=146)
            except Exception as exc:
                # WinError 183 can still race on first try — clean up and retry once
                if "183" in str(exc) or "already exists" in str(exc).lower():
                    logger.warning(f"ChromeDriver init race error, retrying after cleanup: {exc}")
                    _cleanup_stale_chromedriver(force=True)
                    time.sleep(1)
                    _driver = uc.Chrome(options=options, version_main=146)
                else:
                    raise

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


def get_visible_driver(force_new: bool = False):
    """Return a regular, VISIBLE Chrome driver (no headless).

    Some sites (e.g. maaal.com) actively detect and block headless browsers.
    Regular Selenium with a visible window bypasses this.
    """
    global _visible_driver
    with _visible_driver_lock:
        if force_new:
            close_visible_driver()

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
    with _visible_driver_lock:
        driver = _visible_driver
        _visible_driver = None
    if driver:
        with contextlib.suppress(Exception):
            driver.quit()


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


def _extract_title_hint_from_url(url: str) -> str:
    parsed = urlparse(url or "")
    slug = unquote(parsed.path or "")
    slug = slug.replace("/", " ")
    slug = re.sub(r"[-_]+", " ", slug)
    return re.sub(r"\s+", " ", slug).strip()


def _token_overlap_ratio(left: str, right: str) -> float:
    left_tokens = {token for token in left.split() if len(token) >= 3}
    right_tokens = {token for token in right.split() if len(token) >= 3}
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / max(len(left_tokens), len(right_tokens))


def validate_match(
    title: str,
    url: str,
    candidate_title: str | None = None,
    expected_source: str | None = None,
) -> bool:
    if not url or not url.startswith("http"):
        return False
    if not is_allowed_domain(url) or not _is_article_url(url):
        return False

    expected_domain = _normalize_source_hint(expected_source)
    if expected_domain and not _matches_domain(normalize_domain(url), expected_domain):
        return False

    expected_title = _normalize_text_for_match(clean_title(title))
    observed_title = _normalize_text_for_match(clean_title(candidate_title or _extract_title_hint_from_url(url)))
    if not expected_title or not observed_title:
        return False

    if expected_title == observed_title or expected_title in observed_title or observed_title in expected_title:
        return True

    similarity = SequenceMatcher(None, expected_title, observed_title).ratio()
    overlap = _token_overlap_ratio(expected_title, observed_title)
    return similarity >= 0.72 or overlap >= 0.6


def _unwrap_search_result_url(href: str, base_url: str) -> str:
    if not href:
        return ""

    absolute = urljoin(base_url, href)
    parsed = urlparse(absolute)
    host = normalize_domain(absolute)

    if host.endswith("duckduckgo.com"):
        query = parse_qs(parsed.query)
        wrapped = query.get("uddg", []) or query.get("u", [])
        if wrapped:
            return unquote(wrapped[0])

    if host.endswith("google.com") or host.endswith("news.google.com"):
        return ""

    return absolute


def _extract_search_results(html_text: str, *, base_url: str, engine: str) -> list[dict[str, str]]:
    if not _BS4_AVAILABLE:
        return []

    soup = _BS4(html_text, "html.parser")
    selectors = {
        "duckduckgo": "a.result__a, a[data-testid='result-title-a']",
        "bing": "li.b_algo h2 a, .b_algo h2 a",
    }

    candidates: list[dict[str, str]] = []
    seen_urls: set[str] = set()
    for anchor in soup.select(selectors.get(engine, "a")):
        href = _unwrap_search_result_url(anchor.get("href", ""), base_url)
        title = clean_html(anchor.get_text(" ", strip=True))
        if not href or href in seen_urls:
            continue
        if not href.startswith("http"):
            continue
        host = normalize_domain(href)
        if host in _SEARCH_PROVIDER_HOSTS:
            continue
        seen_urls.add(href)
        candidates.append({"title": title, "url": href})
        if len(candidates) >= _SEARCH_RESULT_LIMIT:
            break
    return candidates


def _build_search_queries(title: str, expected_domain: str | None = None) -> list[str]:
    cleaned = clean_title(title)
    if not cleaned:
        return []

    quoted = f"\"{cleaned}\""
    queries: list[str] = []
    if expected_domain:
        queries.append(f"{quoted} site:{expected_domain}")
    else:
        domain_clause = " OR ".join(f"site:{domain}" for domain in ALLOWED_DOMAINS)
        queries.append(f"{quoted} ({domain_clause})")
    queries.append(quoted)
    return queries


def search_real_url(title: str, expected_source: str | None = None) -> str | None:
    cleaned = clean_title(title)
    if not cleaned:
        return None

    if not _BS4_AVAILABLE:
        logger.warning("BeautifulSoup is unavailable, skipping title-search URL fallback.")
        return None

    expected_domain = _normalize_source_hint(expected_source)
    cache_key = _build_search_cache_key(cleaned, expected_domain)
    cached_value = _get_cached_search_value(cache_key)
    if cached_value is not ...:
        return cached_value

    search_specs = [
        {
            "engine": "duckduckgo",
            "url": "https://html.duckduckgo.com/html/",
            "session": "search_duckduckgo",
            "params_builder": lambda query: {"q": query, "kl": "sa-ar"},
        },
        {
            "engine": "bing",
            "url": "https://www.bing.com/search",
            "session": "search_bing",
            "params_builder": lambda query: {"q": query, "setlang": "ar-SA"},
        },
    ]

    try:
        for query in _build_search_queries(cleaned, expected_domain):
            for spec in search_specs:
                try:
                    response = request_with_retry(
                        "GET",
                        spec["url"],
                        session_name=spec["session"],
                        params=spec["params_builder"](query),
                        timeout=6,
                        headers={"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
                    )
                    response.raise_for_status()
                    response.encoding = response.apparent_encoding or "utf-8"
                except Exception as exc:
                    logger.warning(f"{spec['engine']} title search failed for '{cleaned[:80]}': {exc}")
                    continue

                for candidate in _extract_search_results(response.text, base_url=response.url, engine=spec["engine"]):
                    if validate_match(cleaned, candidate["url"], candidate["title"], expected_domain):
                        logger.info(f"Resolved article URL via title search: {candidate['url']}")
                        _set_cached_search_value(cache_key, candidate["url"], title=cleaned, expected_domain=expected_domain)
                        return candidate["url"]
    except Exception as exc:
        logger.warning(f"Title-search URL fallback failed for '{cleaned[:80]}': {exc}")

    _set_cached_search_value(cache_key, None, title=cleaned, expected_domain=expected_domain)
    return None


def resolve_final_url(url: str, article_title: str | None = None, source_hint: str | None = None) -> str | None:
    """Resolve a Google News redirect URL to the real publisher URL.

    Resolution order:
      1. Cache hit — free.
      2. URL is already non-Google — return immediately.
      3. HTTP request with browser headers — handles 302 redirects & meta-refresh.
      4. Selenium — last resort, waits for JS navigation to complete.
      5. Search fallback — exact-title search against allowed publishers.
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
        logger.warning(f"Failed to resolve (no Selenium): {url}")
        fallback_url = search_real_url(article_title or "", expected_source=source_hint)
        _RESOLVE_CACHE[url] = fallback_url
        return fallback_url

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

    fallback_url = search_real_url(article_title or "", expected_source=source_hint)
    if fallback_url:
        logger.info(f"Resolved Google News URL via title-search fallback: {fallback_url}")
        _RESOLVE_CACHE[url] = fallback_url
        return fallback_url

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

    m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", normalized)
    if m:
        d, mo, y = map(int, m.groups())
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


def _extract_date_from_url_path(url: str) -> datetime | None:
    path = urlparse(url or "").path
    match = re.search(r"/(20\d{2})/(\d{1,2})/(\d{1,2})(?:/|$)", path)
    if not match:
        return None

    year, month, day = map(int, match.groups())
    try:
        return datetime(year, month, day, tzinfo=timezone.utc)
    except ValueError:
        return None


def _extract_maaal_date_from_html(html_text: str) -> datetime | None:
    if not html_text:
        return None

    for candidate in re.findall(r'"datePublished"\s*:\s*"([^"]+)"', html_text, flags=re.IGNORECASE):
        parsed = parse_page_date_text(candidate)
        if parsed:
            return parsed

    match = re.search(r"\d{1,2}\s+[\u0600-\u06FF]+\s+\d{4}", html_text)
    if match:
        parsed = parse_page_date_text(match.group(0))
        if parsed:
            return parsed

    return None


def _extract_spa_date_from_html(html_text: str) -> datetime | None:
    if not html_text:
        return None

    candidates: list[str] = []

    for description in re.findall(r'meta name="description" content="([^"]+)"', html_text, flags=re.IGNORECASE):
        candidates.extend(
            re.findall(r"\d{1,2}\s+[\u0600-\u06FF]+\s+\d{4}\s*[\u0645\u0647\u0640]?", description)
        )

    candidates.extend(re.findall(r"\d{1,2}\s+[\u0600-\u06FF]+\s+\d{4}\s*[\u0645\u0647\u0640]?", html_text))

    for candidate in candidates:
        parsed = parse_page_date_text(candidate)
        if parsed:
            return parsed

    return None


def extract_page_date(url: str) -> datetime | None:
    domain = normalize_domain(url)

    # alriyadh.com works fine with plain requests (div.article-time time)
    # Only these sites genuinely need Selenium
    _SELENIUM_DOMAINS = {"sabq.org", "maaal.com"}
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
                vis = None
                try:
                    vis = get_visible_driver(force_new=True)
                    vis.get(url)
                    time.sleep(5)  # wait for JS to render the date
                    if _SeleniumBy is not None:
                        candidate_selectors = [
                            "span.text-fontColorSecondary",
                            "[class*='text-fontColorSecondary']",
                        ]
                        for selector in candidate_selectors:
                            for el in vis.find_elements(_SeleniumBy.CSS_SELECTOR, selector):
                                date_text = (el.text or "").strip()
                                if not date_text:
                                    continue
                                logger.debug(f"maaal.com visible driver text: {date_text!r}")
                                parsed = parse_page_date_text(date_text)
                                if parsed:
                                    return parsed
                    parsed = _extract_maaal_date_from_html(vis.page_source)
                    if parsed:
                        return parsed
                except Exception as e:
                    logger.warning(f"maaal.com visible driver error for {url}: {e}")
                finally:
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


# Strong Saudi geographic/institutional indicators used by is_saudi_news()
# and for boosting the relevance score.
SAUDI_KEYWORDS = [
    "السعودية",
    "المملكة",
    "الرياض",
    "جدة",
    "ساما",
    "تاسي",
    "وزارة المالية",
    "البنك المركزي",
    "مكة",
    "الدمام",
    "سعودي",
    "سعودية",
    "صندوق الاستثمارات العامة",
    "رؤية 2030",
]


def is_saudi_news(title: str, description: str) -> bool:
    """Return True only if the article contains at least one strong Saudi indicator.

    This acts as a hard gate after domain filtering to ensure we never collect
    financial news that happens to come from an allowed domain but covers a
    non-Saudi subject (e.g. global markets, other countries).
    """
    text = f"{title} {description}".lower()
    return any(kw.lower() in text for kw in SAUDI_KEYWORDS)


SAUDI_KEYWORDS.extend(
    [
        "saudi",
        "saudi arabia",
        "kingdom of saudi arabia",
        "kingdom",
        "riyadh",
        "jeddah",
        "sama",
        "tadawul",
        "vision 2030",
        "ministry of finance",
        "public investment fund",
    ]
)


def detect_language(text: str) -> str:
    normalized = clean_html(text or "")
    if not normalized:
        return "ar"

    arabic_count = len(_ARABIC_CHAR_RE.findall(normalized))
    english_count = len(_ENGLISH_CHAR_RE.findall(normalized))

    if english_count == 0 and arabic_count == 0:
        return "ar"
    if arabic_count == 0 and english_count > 0:
        return "en"
    if english_count >= 12 and english_count > arabic_count * 1.2:
        return "en"
    if english_count > arabic_count and arabic_count < 8:
        return "en"
    return "ar"


def split_articles_by_language(articles: list[dict]) -> tuple[list[dict], list[dict]]:
    english_articles: list[dict] = []
    arabic_articles: list[dict] = []

    for item in articles:
        language = item.get("language")
        if language not in {"ar", "en"}:
            language = detect_language(f"{item.get('title', '')} {item.get('description', '')}")
            item["language"] = language

        if language == "en":
            english_articles.append(item)
        else:
            arabic_articles.append(item)

    return english_articles, arabic_articles


def _contains_any_keyword(text_lower: str, keywords: list[str]) -> bool:
    return any(keyword in text_lower for keyword in keywords)


def _is_stock_or_dividend_noise(title: str, description: str) -> bool:
    text_lower = f"{title} {description}".lower()
    return _contains_any_keyword(text_lower, STOCK_DIVIDEND_NOISE_KEYWORDS)


def detect_business_bucket(title: str, description: str) -> str:
    text = f"{title} {description}".lower()
    if _contains_any_keyword(text, SIMAH_KEYWORDS):
        return "simah"
    if _contains_any_keyword(text, SAMA_KEYWORDS):
        return "sama"
    if _contains_any_keyword(text, REGULATORY_INTEL_KEYWORDS) or _contains_any_keyword(text, BANKING_INTEL_KEYWORDS):
        return "financial"
    if _contains_any_keyword(text, MACRO_INTEL_KEYWORDS):
        return "economy"
    if _contains_any_keyword(text, FINANCIAL_KEYWORDS):
        return "financial"
    if _contains_any_keyword(text, ECONOMY_KEYWORDS):
        return "economy"
    return "general"


def score_news(title: str, description: str) -> int:
    """Score an article by topic relevance, with Saudi context weighted highest."""
    text = f"{title} {description}"
    text_lower = text.lower()
    score = 0
    has_english_saudi_context = _contains_any_keyword(text_lower, ENGLISH_SAUDI_SCORE_KEYWORDS)
    has_english_sama = "sama" in text_lower
    has_english_tadawul = "tadawul" in text_lower

    # --- Saudi geographic / institutional weighting (highest priority) ---
    if "السعودية" in text or "المملكة" in text:
        score += 60
    if "ساما" in text:
        score += 50
    if "تاسي" in text:
        score += 40

    if has_english_saudi_context:
        score += 60
    if has_english_sama:
        score += 50
    if has_english_tadawul:
        score += 40

    # --- Existing topic-based scoring ---
    if _contains_any_keyword(text_lower, SIMAH_KEYWORDS):
        score += 100
    if _contains_any_keyword(text_lower, SAMA_KEYWORDS):
        score += 80
    if _contains_any_keyword(text_lower, FINANCIAL_KEYWORDS):
        score += 50
    if _contains_any_keyword(text_lower, ECONOMY_KEYWORDS):
        score += 30
    if _contains_any_keyword(text_lower, REGULATORY_INTEL_KEYWORDS):
        score += 45
    if _contains_any_keyword(text_lower, BANKING_INTEL_KEYWORDS):
        score += 35
    if _contains_any_keyword(text_lower, MACRO_INTEL_KEYWORDS):
        score += 30

    if _is_stock_or_dividend_noise(title, description):
        score -= 40

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


def _article_sort_key(item: dict) -> tuple[int, int, datetime]:
    return (
        int(item.get("business_priority", 0)),
        int(item.get("rss_score", item.get("score", 0))),
        parse_date(item.get("published", "")),
    )


def _batch_signature(items: list[dict], sample_size: int = 40) -> str:
    signature_parts: list[str] = []
    for item in items[:sample_size]:
        signature_parts.append(item.get("title", ""))
        signature_parts.append(item.get("link", ""))
    return stable_hash("rss-batch", *signature_parts)


def _is_allowed_article_source(item: dict) -> bool:
    source_hint = item.get("source_domain") or item.get("source")
    if _normalize_source_hint(source_hint):
        return True

    link = item.get("link", "")
    return bool(link and is_allowed_domain(link))


def _select_language_balanced_articles(
    articles: list[dict],
    limit: int,
    max_english: int = _MAX_ENGLISH_ARTICLES,
) -> list[dict]:
    if limit <= 0:
        return []

    english_articles, arabic_articles = split_articles_by_language(articles)
    english_articles = [item for item in english_articles if _is_allowed_article_source(item)]

    english_articles.sort(key=_article_sort_key, reverse=True)
    arabic_articles.sort(key=_article_sort_key, reverse=True)

    english_limit = min(max_english, limit)
    selected_english = english_articles[:english_limit]
    remaining_slots = max(0, limit - len(selected_english))
    selected_arabic = arabic_articles[:remaining_slots]

    final_selection = selected_english + selected_arabic
    if len(final_selection) < limit:
        extra_slots = limit - len(final_selection)
        final_selection.extend(arabic_articles[len(selected_arabic): len(selected_arabic) + extra_slots])

    final_selection.sort(key=_article_sort_key, reverse=True)
    english_selected = sum(1 for item in final_selection if item.get("language") == "en")
    logger.info(
        "Language balancing applied: "
        f"en={english_selected}, ar={len(final_selection) - english_selected}, "
        f"cap={english_limit}, limit={limit}"
    )
    return final_selection[:limit]


def _has_required_language_mix(
    articles: list[dict],
    target: int,
    max_english: int = _MAX_ENGLISH_ARTICLES,
) -> bool:
    desired_english = min(max_english, target)
    required_arabic = max(0, target - desired_english)
    english_articles, arabic_articles = split_articles_by_language(articles)
    return len(english_articles) >= desired_english and len(arabic_articles) >= required_arabic


def _finalize_valid_news_selection(
    articles: list[dict],
    target: int,
    max_english: int = _MAX_ENGLISH_ARTICLES,
) -> list[dict]:
    final_selection = _select_language_balanced_articles(articles, limit=target, max_english=max_english)
    for rank, item in enumerate(final_selection, start=1):
        item["rank"] = rank
    return final_selection


def is_recent_news(date_str: str, days_back: int = 7) -> bool:
    published_at = parse_date(date_str)
    if published_at == datetime.min.replace(tzinfo=timezone.utc):
        return False

    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    return published_at >= cutoff


def validate_article(item: dict, days_back: int = 7) -> tuple[bool, str]:
    link = item.get("link", "")
    final_url = resolve_final_url(
        link,
        article_title=item.get("title", ""),
        source_hint=item.get("source_domain") or item.get("source", ""),
    )

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



def fetch_valid_news(target: int = 35, max_batches: int = 6, days_back: int = 7) -> list:
    collected = []
    seen_titles = set()
    seen_links = set()
    base_candidate_limit = max(70, target * 2)
    candidate_step = max(20, target // 2)
    previous_batch_signature = ""
    try:
        for attempt in range(1, max_batches + 1):
            current_limit = base_candidate_limit + ((attempt - 1) * candidate_step)
            batch = fetch_rss_news(limit=current_limit, days_back=days_back, enforce_language_balance=False)
            if not batch:
                logger.warning(f"Batch {attempt} returned empty feed.")
                continue

            batch_signature = _batch_signature(batch)
            if batch_signature == previous_batch_signature:
                logger.info(
                    f"Stopping after batch {attempt}: RSS candidate pool repeated "
                    f"(limit={current_limit}, size={len(batch)})."
                )
                break
            previous_batch_signature = batch_signature

            new_validated_in_batch = 0
            new_candidates_in_batch = 0
            for item in batch:
                title = item.get("title", "")
                link = item.get("link", "")
                if title in seen_titles or link in seen_links:
                    continue

                new_candidates_in_batch += 1
                is_valid, resolved_link = validate_article(item, days_back=days_back)
                if not is_valid:
                    continue

                item["link"] = resolved_link or link
                item["source_domain"] = normalize_domain(item["link"])
                seen_titles.add(title)
                seen_links.add(item["link"])
                collected.append(item)
                new_validated_in_batch += 1

                if len(collected) >= target and _has_required_language_mix(collected, target):
                    final_selection = _finalize_valid_news_selection(collected, target)
                    logger.info(
                        f"Collected {len(collected)} validated articles and finalized {len(final_selection)} "
                        f"items (target {target})."
                    )
                    return final_selection

            if new_candidates_in_batch == 0 or new_validated_in_batch == 0:
                logger.info(
                    f"Stopping after batch {attempt}: no meaningful progress "
                    f"(new_candidates={new_candidates_in_batch}, new_validated={new_validated_in_batch})."
                )
                break

        final_selection = _finalize_valid_news_selection(collected, target)
        logger.info(
            f"Collected {len(collected)} validated articles after {max_batches} batches "
            f"and finalized {len(final_selection)} items (target {target})."
        )
        return final_selection
    finally:
        _persist_resolve_cache()
        _persist_search_cache()
        close_driver()


def fetch_rss_news(limit: int = 35, days_back: int = 7, enforce_language_balance: bool = True) -> list:
    logger.info("Aggregating news from curated financial sources...")
    all_news = []
    rss_sources = build_rss_sources(days_back=days_back)

    _RSS_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/rss+xml,application/xml,text/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ar-SA,ar;q=0.9,en;q=0.8",
    }

    for url in rss_sources:
        try:
            # Pre-fetch with real browser headers so Google News returns valid XML
            # instead of an HTML error/captcha page.
            try:
                rss_resp = requests.get(url, headers=_RSS_HEADERS, timeout=15)
                feed = feedparser.parse(rss_resp.content)
            except Exception as fetch_exc:
                logger.warning(f"HTTP fetch failed for {url}: {fetch_exc} — falling back to feedparser direct")
                feed = feedparser.parse(url)

            if feed.bozo and feed.bozo_exception:
                if not feed.entries:
                    logger.warning(f"Skipping broken feed (no entries) for {url}: {feed.bozo_exception}")
                    continue
                logger.warning(f"Feed parser warning for {url}: {feed.bozo_exception}")

            for entry in feed.entries[:40]:
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

                source_domain = _infer_source_domain(source_href, source_title, source_name, link)
                if source_domain:
                    source_name = source_domain
                elif link:
                    source_domain = normalize_domain(link)

                # Hard Saudi-context gate: skip articles with no Saudi indicator
                # regardless of domain or financial keywords.
                if not is_saudi_news(title, description):
                    logger.debug(f"Skipped (no Saudi context): {title[:60]}")
                    continue

                if " / " in title:
                    parts = title.split(" / ", 1)
                    if len(parts[0].strip()) <= 25:
                        title = parts[1].strip()

                rss_score = score_news(title, description)
                business_bucket = detect_business_bucket(title, description)
                language = detect_language(f"{title} {description}")

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
                        "source_domain": source_domain,
                        "language": language,
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

    if enforce_language_balance:
        final_selection = _select_language_balanced_articles(unique_news, limit=limit)
    else:
        final_selection = unique_news[:limit]

    # Add a 1-based rank field ordered by score (highest score = rank 1).
    # The list is already sorted by (business_priority, rss_score, date) desc,
    # so rank simply reflects that ordering for display purposes.
    for rank, item in enumerate(final_selection, start=1):
        item["rank"] = rank

    logger.info(f"Successfully fetched {len(final_selection)} filtered and scored news items.")
    return final_selection


def _resolve_final_url_optimized(
    url: str,
    article_title: str | None = None,
    source_hint: str | None = None,
) -> str | None:
    if not url:
        return None

    cached_value = _get_cached_resolve_value(url)
    if cached_value is not ...:
        refreshed_cached_value = _retry_cached_google_miss(url, cached_value, article_title, source_hint)
        return None if refreshed_cached_value is ... else refreshed_cached_value

    parsed = urlparse(url)
    if "news.google.com" not in parsed.netloc.lower():
        _set_cached_resolve_value(url, url)
        return url

    try:
        resp = request_with_retry(
            "GET",
            url,
            session_name="url_resolver",
            timeout=8,
            allow_redirects=True,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )
        resolved = resp.url
        if resolved and "news.google.com" not in urlparse(resolved).netloc.lower():
            logger.debug(f"Resolved via HTTP redirect: {resolved}")
            _set_cached_resolve_value(url, resolved)
            return resolved

        location_match = re.search(
            r'(?:window\.location(?:\.href)?\s*=\s*|url=|content=["\']\d+;\s*url=)["\']?(https?://[^"\'\s;]+)',
            resp.text,
            re.IGNORECASE,
        )
        if location_match:
            candidate = location_match.group(1)
            if "news.google.com" not in urlparse(candidate).netloc.lower():
                logger.debug(f"Resolved via page redirect hint: {candidate}")
                _set_cached_resolve_value(url, candidate)
                return candidate
    except Exception as exc:
        logger.warning(f"HTTP resolution failed for {url}: {exc}")

    if uc is None:
        logger.warning(f"Failed to resolve (no Selenium): {url}")
        fallback_url = search_real_url(article_title or "", expected_source=source_hint)
        _set_cached_resolve_value(url, fallback_url)
        return fallback_url

    try:
        logger.debug("Using Selenium fallback for URL resolution")
        driver = get_driver()
        driver.get(url)
        deadline = time.time() + 8
        while time.time() < deadline:
            current = driver.current_url
            if current and "news.google.com" not in urlparse(current).netloc.lower():
                logger.debug(f"Resolved via Selenium: {current}")
                _set_cached_resolve_value(url, current)
                return current
            time.sleep(0.4)
    except Exception as exc:
        logger.warning(f"Selenium resolution error: {exc}")
        close_driver()

    fallback_url = search_real_url(article_title or "", expected_source=source_hint)
    if fallback_url:
        logger.info(f"Resolved Google News URL via title-search fallback: {fallback_url}")
        _set_cached_resolve_value(url, fallback_url)
        return fallback_url

    _set_cached_resolve_value(url, None)
    logger.warning(f"Failed to resolve: {url}")
    return None


def _fetch_rss_source_entries(url: str) -> list[dict]:
    try:
        try:
            rss_resp = request_with_retry("GET", url, session_name="rss", headers=RSS_HEADERS, timeout=15)
            feed = feedparser.parse(rss_resp.content)
        except Exception as fetch_exc:
            logger.warning(f"HTTP fetch failed for {url}: {fetch_exc} - falling back to feedparser direct")
            feed = feedparser.parse(url)

        if feed.bozo and feed.bozo_exception:
            if not feed.entries:
                logger.warning(f"Skipping broken feed (no entries) for {url}: {feed.bozo_exception}")
                return []
            logger.warning(f"Feed parser warning for {url}: {feed.bozo_exception}")

        collected: list[dict] = []
        for entry in feed.entries[:40]:
            title = clean_html(entry.get("title", "No Title"))
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

            source_domain = _infer_source_domain(source_href, source_title, source_name, link)
            if source_domain:
                source_name = source_domain
            elif link:
                source_domain = normalize_domain(link)

            if not is_saudi_news(title, description):
                logger.debug(f"Skipped (no Saudi context): {title[:60]}")
                continue

            if " / " in title:
                parts = title.split(" / ", 1)
                if len(parts[0].strip()) <= 25:
                    title = parts[1].strip()

            rss_score = score_news(title, description)
            business_bucket = detect_business_bucket(title, description)
            language = detect_language(f"{title} {description}")
            collected.append(
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
                    "source_domain": source_domain,
                    "language": language,
                }
            )

        return collected
    except Exception as exc:
        logger.error(f"Error fetching RSS from {url}: {exc}")
        return []


def _fetch_rss_news_optimized(
    limit: int = 35,
    days_back: int = 7,
    enforce_language_balance: bool = True,
    progress_callback=None,
    progress_base: float = 0.0,
    progress_span: float = 1.0,
) -> list:
    logger.info("Aggregating news from curated financial sources...")
    rss_sources = build_rss_sources(days_back=days_back)
    all_news: list[dict] = []
    source_count = max(1, len(rss_sources))

    _emit_progress(
        progress_callback,
        stage="rss_start",
        message=f"Reading {len(rss_sources)} approved RSS feeds...",
        progress=progress_base + (progress_span * 0.05),
        source_count=len(rss_sources),
        candidate_limit=limit,
    )

    max_workers = min(6, max(1, len(rss_sources)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for source_index, items in enumerate(executor.map(_fetch_rss_source_entries, rss_sources), start=1):
            all_news.extend(items)
            _emit_progress(
                progress_callback,
                stage="rss_collect",
                message=f"Fetched RSS source {source_index}/{len(rss_sources)}.",
                progress=progress_base + (progress_span * (0.05 + (0.45 * (source_index / source_count)))),
                source_index=source_index,
                source_count=len(rss_sources),
                raw_candidates=len(all_news),
                candidate_limit=limit,
            )

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

    _emit_progress(
        progress_callback,
        stage="rss_rank",
        message=f"Ranked {len(unique_news)} unique articles from {len(all_news)} feed hits.",
        progress=progress_base + (progress_span * 0.72),
        raw_candidates=len(all_news),
        unique_candidates=len(unique_news),
        candidate_limit=limit,
    )

    if enforce_language_balance:
        final_selection = _select_language_balanced_articles(unique_news, limit=limit)
    else:
        final_selection = unique_news[:limit]
    for rank, item in enumerate(final_selection, start=1):
        item["rank"] = rank

    logger.info(f"Successfully fetched {len(final_selection)} filtered and scored news items.")
    _emit_progress(
        progress_callback,
        stage="rss_done",
        message=f"Prepared {len(final_selection)} scored candidates for validation.",
        progress=progress_base + progress_span,
        raw_candidates=len(all_news),
        unique_candidates=len(unique_news),
        selected_candidates=len(final_selection),
        candidate_limit=limit,
    )
    return final_selection


def _fetch_valid_news_optimized(
    target: int = 35,
    max_batches: int = 6,
    days_back: int = 7,
    progress_callback=None,
) -> list:
    collected = []
    seen_titles = set()
    seen_links = set()
    base_candidate_limit = max(70, target * 2)
    candidate_step = max(20, target // 2)
    previous_batch_signature = ""
    validation_progress_start = 0.05
    validation_progress_span = 0.90

    _emit_progress(
        progress_callback,
        stage="start",
        message=f"Starting news aggregation for {target} target articles...",
        progress=0.01,
        target=target,
        max_batches=max_batches,
        days_back=days_back,
    )

    try:
        for attempt in range(1, max_batches + 1):
            current_limit = base_candidate_limit + ((attempt - 1) * candidate_step)
            batch_base = validation_progress_start + (((attempt - 1) / max_batches) * validation_progress_span)
            batch_span = validation_progress_span / max_batches
            rss_span = batch_span * 0.35
            validation_base = batch_base + rss_span
            validation_span = batch_span * 0.65

            _emit_progress(
                progress_callback,
                stage="batch_start",
                message=f"Batch {attempt}/{max_batches}: collecting RSS candidates...",
                progress=batch_base,
                attempt=attempt,
                max_batches=max_batches,
                candidate_limit=current_limit,
                validated_count=len(collected),
                target=target,
            )

            batch = fetch_rss_news(
                limit=current_limit,
                days_back=days_back,
                enforce_language_balance=False,
                progress_callback=progress_callback,
                progress_base=batch_base,
                progress_span=rss_span,
            )
            if not batch:
                logger.warning(f"Batch {attempt} returned empty feed.")
                _emit_progress(
                    progress_callback,
                    stage="batch_empty",
                    message=f"Batch {attempt}/{max_batches} returned no candidates.",
                    progress=batch_base + rss_span,
                    attempt=attempt,
                    max_batches=max_batches,
                    candidate_limit=current_limit,
                    validated_count=len(collected),
                    target=target,
                )
                continue

            batch_signature = _batch_signature(batch)
            if batch_signature == previous_batch_signature:
                logger.info(
                    f"Stopping after batch {attempt}: RSS candidate pool repeated "
                    f"(limit={current_limit}, size={len(batch)})."
                )
                _emit_progress(
                    progress_callback,
                    stage="stopped_repeated_pool",
                    message=f"Stopped at batch {attempt}: the RSS candidate pool repeated.",
                    progress=min(0.97, validation_base),
                    attempt=attempt,
                    max_batches=max_batches,
                    batch_size=len(batch),
                    candidate_limit=current_limit,
                    validated_count=len(collected),
                    target=target,
                )
                break
            previous_batch_signature = batch_signature

            batch_seen_titles = set()
            batch_seen_links = set()
            new_validated_in_batch = 0
            new_candidates_in_batch = 0
            processed_in_batch = 0
            candidate_count = max(1, len(batch))
            for item in batch:
                title = item.get("title", "")
                link = item.get("link", "")
                if (
                    title in seen_titles
                    or link in seen_links
                    or title in batch_seen_titles
                    or link in batch_seen_links
                ):
                    continue

                batch_seen_titles.add(title)
                batch_seen_links.add(link)
                new_candidates_in_batch += 1
                processed_in_batch += 1

                is_valid, resolved_link = validate_article(item, days_back=days_back)
                if not is_valid:
                    if processed_in_batch == 1 or processed_in_batch % 5 == 0:
                        _emit_progress(
                            progress_callback,
                            stage="validating",
                            message=f"Batch {attempt}/{max_batches}: validating article freshness and source URLs...",
                            progress=min(0.97, validation_base + (validation_span * (processed_in_batch / candidate_count))),
                            attempt=attempt,
                            max_batches=max_batches,
                            checked_in_batch=processed_in_batch,
                            batch_size=len(batch),
                            validated_count=len(collected),
                            target=target,
                            candidate_limit=current_limit,
                        )
                    continue

                item["link"] = resolved_link or link
                item["source_domain"] = normalize_domain(item["link"])
                seen_titles.add(title)
                seen_links.add(item["link"])
                collected.append(item)
                new_validated_in_batch += 1

                _emit_progress(
                    progress_callback,
                    stage="validated",
                    message=f"Validated {len(collected)} of {target} target articles.",
                    progress=min(0.97, validation_base + (validation_span * (processed_in_batch / candidate_count))),
                    attempt=attempt,
                    max_batches=max_batches,
                    checked_in_batch=processed_in_batch,
                    batch_size=len(batch),
                    validated_count=len(collected),
                    target=target,
                    candidate_limit=current_limit,
                )

                if len(collected) >= target and _has_required_language_mix(collected, target):
                    final_selection = _finalize_valid_news_selection(collected, target)
                    logger.info(
                        f"Collected {len(collected)} validated articles and finalized {len(final_selection)} "
                        f"items (target {target})."
                    )
                    _emit_progress(
                        progress_callback,
                        stage="complete",
                        message=f"Finished collecting {len(final_selection)} ready-to-use articles.",
                        progress=1.0,
                        attempt=attempt,
                        max_batches=max_batches,
                        validated_count=len(collected),
                        final_count=len(final_selection),
                        target=target,
                    )
                    return final_selection

            if new_candidates_in_batch == 0 or new_validated_in_batch == 0:
                logger.info(
                    f"Stopping after batch {attempt}: no meaningful progress "
                    f"(new_candidates={new_candidates_in_batch}, new_validated={new_validated_in_batch})."
                )
                _emit_progress(
                    progress_callback,
                    stage="stopped_no_progress",
                    message=f"Stopped at batch {attempt}: no meaningful new progress.",
                    progress=min(0.97, validation_base + validation_span),
                    attempt=attempt,
                    max_batches=max_batches,
                    new_candidates_in_batch=new_candidates_in_batch,
                    new_validated_in_batch=new_validated_in_batch,
                    validated_count=len(collected),
                    target=target,
                )
                break

        final_selection = _finalize_valid_news_selection(collected, target)
        logger.info(
            f"Collected {len(collected)} validated articles after {max_batches} batches "
            f"and finalized {len(final_selection)} items (target {target})."
        )
        _emit_progress(
            progress_callback,
            stage="complete",
            message=f"Finished collecting {len(final_selection)} ready-to-use articles.",
            progress=1.0,
            validated_count=len(collected),
            final_count=len(final_selection),
            target=target,
            max_batches=max_batches,
        )
        return final_selection
    finally:
        _persist_resolve_cache()
        _persist_search_cache()
        close_driver()
        close_visible_driver()


resolve_final_url = _resolve_final_url_optimized
fetch_rss_news = _fetch_rss_news_optimized
fetch_valid_news = _fetch_valid_news_optimized


def _extract_page_date_requests_only_optimized(url: str) -> datetime | None:
    domain = normalize_domain(url)

    url_path_date = _extract_date_from_url_path(url)

    if _matches_domain(domain, "alriyadh.com"):
        try:
            resp_ar = request_with_retry(
                "GET",
                url,
                session_name="page_date",
                timeout=12,
                allow_redirects=True,
                verify=False,
                headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Referer": "https://www.alriyadh.com/",
                },
            )
            resp_ar.encoding = "utf-8"
            page_html = resp_ar.text

            match = re.search(
                r'<div[^>]*article-time[^>]*>.*?<time[^>]*datetime="([^"]+)"',
                page_html,
                re.IGNORECASE | re.DOTALL,
            )
            if match:
                parsed = parse_page_date_text(match.group(1))
                if parsed:
                    return parsed

            if _BS4_AVAILABLE:
                soup = _BS4(page_html, "html.parser")
                time_tag = soup.select_one("div.article-time time")
                if time_tag:
                    dt_attr = time_tag.get("datetime", "")
                    if dt_attr:
                        parsed = parse_page_date_text(dt_attr)
                        if parsed:
                            return parsed
                    parsed = parse_page_date_text(time_tag.get_text(strip=True))
                    if parsed:
                        return parsed
            else:
                match = re.search(
                    r'<div[^>]*article-time[^>]*>.*?<time[^>]*>([^<]+)</time>',
                    page_html,
                    re.IGNORECASE | re.DOTALL,
                )
                if match:
                    parsed = parse_page_date_text(match.group(1).strip())
                    if parsed:
                        return parsed

            for dt_value in re.findall(r'<time[^>]*datetime="([^"]+)"', page_html, re.IGNORECASE):
                parsed = parse_page_date_text(dt_value)
                if parsed:
                    return parsed
        except Exception as exc:
            logger.warning(f"alriyadh.com date extraction failed for {url}: {exc}")
        return None

    try:
        resp = request_with_retry(
            "GET",
            url,
            session_name="page_date",
            timeout=10,
            allow_redirects=True,
        )
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or "utf-8"
        html_text = resp.text

        if _matches_domain(domain, "sabq.org"):
            for span_text in re.findall(r"<span[^>]*>([^<]+)</span>", html_text, flags=re.IGNORECASE):
                cleaned = clean_html(span_text)
                if re.search(r"\d{1,2}\s+\S+\s+\d{4}", cleaned):
                    parsed = parse_page_date_text(cleaned)
                    if parsed:
                        return parsed
            match = re.search(r"(\d{1,2}\s+\S+\s+\d{4})\s*ÙÙŠ", html_text)
            if match:
                parsed = parse_page_date_text(match.group(1))
                if parsed:
                    return parsed

        if _matches_domain(domain, "maaal.com"):
            parsed = _extract_maaal_date_from_html(html_text)
            if parsed:
                return parsed

        if _matches_domain(domain, "spa.gov.sa"):
            parsed = _extract_spa_date_from_html(html_text)
            if parsed:
                return parsed

        patterns = [
            r'<time[^>]*datetime="([^"]+)"',
            r'<time[^>]*>([^<]+)</time>',
            r'meta property="article:published_time" content="([^"]+)"',
            r'meta property="og:updated_time" content="([^"]+)"',
            r'meta name="pubdate" content="([^"]+)"',
            r'meta name="publishdate" content="([^"]+)"',
            r'meta name="date" content="([^"]+)"',
            r'meta itemprop="datePublished" content="([^"]+)"',
            r'class="[^"]*(?:date-posted|entry-date|post-date|article-date|news-date)[^"]*"[^>]*>([^<]+)<',
        ]
        candidates: list[str] = []
        for pattern in patterns:
            candidates.extend(re.findall(pattern, html_text, flags=re.IGNORECASE | re.DOTALL))

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
        return url_path_date

    if url_path_date:
        return url_path_date

    return None


extract_page_date_requests_only = _extract_page_date_requests_only_optimized
