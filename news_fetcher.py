import feedparser
import html
import re
import urllib.parse
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

from utils import setup_logger

logger = setup_logger(__name__)

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


def clean_html(raw_html: str) -> str:
    if not raw_html:
        return ""
    clean_text = re.sub(r"<[^>]+>", "", raw_html)
    return html.unescape(clean_text).strip()


def normalize_domain(url: str) -> str:
    host = urlparse(url).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host


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
                link = entry.get("link", "")
                description = clean_html(entry.get("summary", "No Description"))
                published = entry.get("published", "")

                if not is_recent_news(published, days_back=days_back):
                    continue

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
