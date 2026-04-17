import contextlib
import html
import io
import json
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests
import trafilatura
try:
    import undetected_chromedriver as uc
except ImportError:
    uc = None
from playwright.sync_api import sync_playwright
import time

from news_fetcher import (
    ALLOWED_DOMAINS,
    BUSINESS_PRIORITY,
    detect_language as detect_news_language,
    detect_business_bucket,
    extract_page_date_requests_only,
    is_allowed_domain,
    resolve_final_url as shared_resolve_final_url,
)
from pipeline_utils import load_json_cache, request_with_retry, save_json_cache, stable_hash, utc_now_iso
from utils import setup_logger

logger = setup_logger(__name__)
_url_cache: dict[str, str | None] = {}

OLLAMA_API_URL = os.getenv("OLLAMA_API_URL", "http://localhost:11434/api/generate")
MODEL_NAME = os.getenv("OLLAMA_MODEL", "llama3:8b")
CACHE_PATH = Path(__file__).resolve().parent / ".cache" / "processed_news.json"
ARTICLE_CACHE_PATH = Path(__file__).resolve().parent / ".cache" / "scraped_articles.json"
EXTRACTION_CACHE_VERSION = "2026-04-12-language-aware-v1"
PROCESSED_CACHE_TTL_SECONDS = 7 * 24 * 3600
PROCESSED_CACHE_MAX_ENTRIES = 1500
ARTICLE_CACHE_TTL_SECONDS = 36 * 3600
ARTICLE_CACHE_MAX_ENTRIES = 800
LLM_MAX_WORKERS = max(1, int(os.getenv("LLM_MAX_WORKERS", "2")))
OLLAMA_GLOBAL_MAX_CONCURRENT = max(1, int(os.getenv("OLLAMA_GLOBAL_MAX_CONCURRENT", str(LLM_MAX_WORKERS))))
OLLAMA_SLOT_TIMEOUT_SECONDS = max(1, int(os.getenv("OLLAMA_SLOT_TIMEOUT_SECONDS", "180")))
MIN_CONTENT_LENGTH = 300
ARTICLE_MAX_AGE_DAYS = 7
LLM_RETRY_LIMIT = 3
_OLLAMA_REQUEST_SEMAPHORE = threading.BoundedSemaphore(OLLAMA_GLOBAL_MAX_CONCURRENT)
ALLOWED_CATEGORIES = {
    "\u0627\u0642\u062a\u0635\u0627\u062f",
    "\u062a\u0642\u0646\u064a\u0629",
    "\u0633\u064a\u0627\u0633\u0629",
    "\u0623\u0639\u0645\u0627\u0644",
    "\u0639\u0627\u0645",
}
FALLBACK_CATEGORY = "\u0623\u0639\u0645\u0627\u0644"
SUMMARY_SPLIT_PATTERN = re.compile(r"[\n\r]+|[.!?\u061b\u060c\u061f]+")
NOISE_PATTERNS = [
    r"^\s*$",
    r"^(read more|related news|advertisement|subscribe|follow us)\b",
    r"^(\u0627\u0644\u0645\u0632\u064a\u062f|\u0623\u062e\u0628\u0627\u0631 \u0630\u0627\u062a \u0635\u0644\u0629|\u0627\u0634\u062a\u0631\u0643|\u0625\u0639\u0644\u0627\u0646|\u0645\u0634\u0627\u0631\u0643\u0629)\b",
]
ARGAAM_GARBAGE_PATTERNS = [
    r"\b\d{4}\s*-\s*",
    r"\u062a\u0631\u062a\u064a\u0628 \u0627\u0644\u0628\u0646\u0648\u0643",
    r"\u0645\u0624\u0634\u0631\u0627\u062a \u0627\u0644\u0628\u0646\u0648\u0643",
    r"\u0625\u062d\u0635\u0627\u0626\u064a\u0627\u062a \u0627\u0644\u0623\u0633\u0645\u0646\u062a",
    r"\u0634\u0631\u0643\u0627\u062a \u0627\u0644\u0623\u0633\u0645\u0646\u062a",
    r"\u0625\u0646\u0641\u0627\u0642 \u0627\u0644\u0645\u0633\u062a\u0647\u0644\u0643\u064a\u0646",
    r"\u0627\u0644\u0635\u0627\u062f\u0631\u0627\u062a \u0648\u0627\u0644\u0648\u0627\u0631\u062f\u0627\u062a",
    r"\u0627\u0644\u0633\u0644\u0639 \u0627\u0644\u063a\u0630\u0627\u0626\u064a\u0629",
    r"\u0627\u0644\u0633\u0644\u0639 \u063a\u064a\u0631 \u0627\u0644\u063a\u0630\u0627\u0626\u064a\u0629",
    r"\u0627\u0644\u0633\u0644\u0639 \u0627\u0644\u0627\u0646\u0634\u0627\u0626\u064a\u0629",
    r"\u062a\u0631\u062a\u064a\u0628 \u0627\u0644\u0628\u062a\u0631\u0648\u0643\u064a\u0645\u0627\u0648\u064a\u0627\u062a",
    r"\u0645\u0624\u0634\u0631\u0627\u062a \u0627\u0644\u0628\u062a\u0631\u0648\u0643\u064a\u0645\u0627\u0648\u064a\u0627\u062a",
    r"\u062a\u0631\u062a\u064a\u0628 \u0627\u0644\u062a\u062c\u0632\u0626\u0629",
    r"\u0645\u0624\u0634\u0631\u0627\u062a \u0627\u0644\u062a\u062c\u0632\u0626\u0629",
    r"\u0627\u0644\u0623\u0639\u0644\u0649 \u0646\u0645\u0648\u0627\u064b",
    r"\u0627\u0644\u062a\u0648\u0632\u064a\u0639\u0627\u062a \u0627\u0644\u0646\u0642\u062f\u064a\u0629 \u0627\u0644\u062a\u0627\u0631\u064a\u062e\u064a\u0629",
]

PROMPT_TEMPLATE = """STRICT JSON ONLY.
DO NOT WRITE ANY TEXT BEFORE OR AFTER JSON.

You MUST return EXACTLY this structure:
{{
  "title": "",
  "summary": ["", ""],
  "category": "",
  "importance": 1,
  "image_keyword": ""
}}

RULES:
- title and summary items must be in {output_language_name}.
- {language_guard}
- summary must be an array of exactly 2 concise items in {output_language_name}.
- category must be one of: \u0627\u0642\u062a\u0635\u0627\u062f, \u062a\u0642\u0646\u064a\u0629, \u0633\u064a\u0627\u0633\u0629, \u0623\u0639\u0645\u0627\u0644, \u0639\u0627\u0645.
- importance must be an integer from 1 to 10.
- image_keyword must be English only and specific to the subject.
- focus only on the main article.
- ignore menus, sidebars, ads, and related links.

NEWS TITLE:
{title}

NEWS CONTENT:
{content}
"""

SUMMARIZE_3_LINES_PROMPT_AR = """\u0623\u0646\u062a \u0645\u062d\u0631\u0631 \u0623\u062e\u0628\u0627\u0631 \u0645\u062d\u062a\u0631\u0641.

\u0627\u0642\u0631\u0623 \u0627\u0644\u062e\u0628\u0631 \u0627\u0644\u062a\u0627\u0644\u064a \u0628\u0639\u0646\u0627\u064a\u0629\u060c \u062b\u0645 \u0627\u0643\u062a\u0628 \u0645\u0644\u062e\u0635\u064b\u0627 \u0648\u0627\u0636\u062d\u064b\u0627 \u0648\u0645\u062e\u062a\u0635\u0631\u064b\u0627 \u0645\u0646 3 \u0633\u0637\u0648\u0631 \u0641\u0642\u0637.

\u0627\u0644\u0634\u0631\u0648\u0637:

* \u0627\u0643\u062a\u0628 \u0628\u0627\u0644\u0644\u063a\u0629 \u0627\u0644\u0639\u0631\u0628\u064a\u0629 \u0627\u0644\u0641\u0635\u062d\u0649 \u0641\u0642\u0637
* \u0644\u0627 \u062a\u0633\u062a\u062e\u062f\u0645 \u0646\u0642\u0627\u0637 \u0623\u0648 \u062a\u0639\u062f\u0627\u062f
* \u0643\u0644 \u0633\u0637\u0631 \u064a\u0643\u0648\u0646 \u062c\u0645\u0644\u0629 \u0648\u0627\u062d\u062f\u0629 \u0642\u0635\u064a\u0631\u0629
* \u0627\u0644\u0633\u0637\u0631 \u0627\u0644\u0623\u0648\u0644: \u0645\u0627\u0630\u0627 \u062d\u062f\u062b
* \u0627\u0644\u0633\u0637\u0631 \u0627\u0644\u062b\u0627\u0646\u064a: \u0623\u0647\u0645 \u0627\u0644\u062a\u0641\u0627\u0635\u064a\u0644
* \u0627\u0644\u0633\u0637\u0631 \u0627\u0644\u062b\u0627\u0644\u062b: \u0644\u0645\u0627\u0630\u0627 \u0647\u0630\u0627 \u0627\u0644\u062e\u0628\u0631 \u0645\u0647\u0645 \u0623\u0648 \u062a\u0623\u062b\u064a\u0631\u0647
* \u0644\u0627 \u062a\u0636\u0641 \u0623\u064a \u0646\u0635 \u062e\u0627\u0631\u062c \u0627\u0644\u0645\u0644\u062e\u0635

STRICT:
\u064a\u062c\u0628 \u0623\u0646 \u064a\u0643\u0648\u0646 \u0627\u0644\u0646\u0627\u062a\u062c 3 \u0633\u0637\u0648\u0631 \u0641\u0642\u0637.

\u0627\u0644\u062e\u0628\u0631:
{NEWS_TEXT}"""

SUMMARIZE_3_LINES_PROMPT_EN = """You are a professional news editor.

Read the following article carefully, then write a clear summary in exactly 3 short lines.

Rules:

* Write in English only
* Do not translate the summary into Arabic
* Do not use bullets or numbering
* Each line must be one short sentence
* Line 1: what happened
* Line 2: the key details
* Line 3: why this matters
* Do not add any text outside the summary

STRICT:
The output must be exactly 3 lines.

NEWS:
{NEWS_TEXT}"""

SUMMARY_PROMPT_CACHE_VERSION = "summary-v2"
STRUCTURED_PROMPT_CACHE_VERSION = "structured-v2"


def normalize_content_language(value: str | None) -> str:
    return "en" if str(value or "").strip().lower() == "en" else "ar"


def detect_processing_language(news_item: dict, content_to_use: str = "") -> str:
    explicit = str(news_item.get("language", "")).strip().lower()
    if explicit in {"ar", "en"}:
        return explicit

    candidate_text = " ".join(
        part
        for part in (
            str(news_item.get("title", "") or ""),
            str(news_item.get("description", "") or ""),
            str(content_to_use or "")[:1500],
        )
        if part
    )
    return normalize_content_language(detect_news_language(candidate_text))


def _language_name(language: str) -> str:
    return "English" if normalize_content_language(language) == "en" else "Arabic"


def build_structured_prompt(news_item: dict, content_to_use: str, language: str) -> str:
    normalized_language = normalize_content_language(language)
    language_guard = (
        "If the source article is in English, keep the title and summary in English and do not translate them into Arabic."
        if normalized_language == "en"
        else "If the source article is in Arabic, keep the title and summary in Arabic."
    )
    return PROMPT_TEMPLATE.format(
        title=news_item.get("title", ""),
        content=content_to_use,
        output_language_name=_language_name(normalized_language),
        language_guard=language_guard,
    )


def build_summary_prompt(news_text: str, language: str) -> str:
    template = SUMMARIZE_3_LINES_PROMPT_EN if normalize_content_language(language) == "en" else SUMMARIZE_3_LINES_PROMPT_AR
    return template.replace("{NEWS_TEXT}", news_text)


def fallback_summary_line(language: str) -> str:
    return (
        "Insufficient article details were available."
        if normalize_content_language(language) == "en"
        else "\u0644\u0645 \u062a\u062a\u0648\u0641\u0631 \u062a\u0641\u0627\u0635\u064a\u0644 \u0643\u0627\u0641\u064a\u0629."
    )


@contextlib.contextmanager
def _ollama_request_slot():
    acquired = _OLLAMA_REQUEST_SEMAPHORE.acquire(timeout=OLLAMA_SLOT_TIMEOUT_SECONDS)
    if not acquired:
        raise TimeoutError(
            f"Timed out waiting for an Ollama slot after {OLLAMA_SLOT_TIMEOUT_SECONDS} seconds."
        )
    try:
        yield
    finally:
        _OLLAMA_REQUEST_SEMAPHORE.release()

DOMAIN_SELECTORS = {
    "argaam.com": [
        "#articledetail .ck-editor.article.m-bottom",
        "#articledetail .ck-editor.article",
        ".article-detail-content .ck-editor.article.m-bottom",
        ".article-detail-content .ck-editor.article",
    ],
    "sabq.org": [
        '[data-testid="text-article-body"]',
        "main article",
        "article",
        ".prose",
        ".article-content",
    ],
    "maaal.com": [
        ".td-post-content",
        ".entry-content",
        ".post-content",
        ".article-content",
        "article",
    ],
    "spa.gov.sa": [
        ".news-details",
        ".article-content",
        ".field--name-body",
        "article",
    ],
    "alarabiya.net": [
        "main article",
        ".article-body",
        ".article-content",
        ".entry-content",
        "article",
    ],
    "aawsat.com": [
        "main article",
        ".article-body",
        ".field-name-body",
        ".article-content",
        "article",
    ],
}
GENERIC_SELECTORS = [
    "article",
    ".article-body",
    ".article-content",
    ".entry-content",
    ".post-content",
    ".td-post-content",
    "#story_body",
    "#news_content",
]

ARABIC_MONTHS = {
    "\u064a\u0646\u0627\u064a\u0631": 1,
    "\u0641\u0628\u0631\u0627\u064a\u0631": 2,
    "\u0645\u0627\u0631\u0633": 3,
    "\u0623\u0628\u0631\u064a\u0644": 4,
    "\u0627\u0628\u0631\u064a\u0644": 4,
    "\u0645\u0627\u064a\u0648": 5,
    "\u064a\u0648\u0646\u064a\u0648": 6,
    "\u064a\u0648\u0646\u064a\u0647": 6,
    "\u064a\u0648\u0644\u064a\u0648": 7,
    "\u064a\u0648\u0644\u064a\u0647": 7,
    "\u0623\u063a\u0633\u0637\u0633": 8,
    "\u0627\u063a\u0633\u0637\u0633": 8,
    "\u0633\u0628\u062a\u0645\u0628\u0631": 9,
    "\u0623\u0643\u062a\u0648\u0628\u0631": 10,
    "\u0627\u0643\u062a\u0648\u0628\u0631": 10,
    "\u0646\u0648\u0641\u0645\u0628\u0631": 11,
    "\u062f\u064a\u0633\u0645\u0628\u0631": 12,
}

ARABIC_DIGIT_TRANSLATION = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


def clean_json_response(response_text: str) -> str:
    response_text = response_text.strip()
    if response_text.startswith("```json"):
        response_text = response_text[7:]
    elif response_text.startswith("```"):
        response_text = response_text[3:]

    if response_text.endswith("```"):
        response_text = response_text[:-3]

    return response_text.strip()


def normalize_arabic_digits(text: str) -> str:
    return (text or "").translate(ARABIC_DIGIT_TRANSLATION)


def parse_article_date_text(date_text: str) -> datetime | None:
    if not date_text:
        return None

    normalized = normalize_arabic_digits(date_text)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    normalized = normalized.replace("،", " ").replace(",", " ")
    normalized = re.sub(r"\s+في\s+\d{1,2}:\d{2}\s*[^\s]+", "", normalized)
    normalized = re.sub(r"\|\s*", " ", normalized)

    def _to_gregorian_year(y: int) -> int:
        """Convert Hijri year (1380-1480) to approximate Gregorian year."""
        if 1380 <= y <= 1480:
            return y + 622 - y // 33
        return y

    iso_match = re.search(r"(\d{4})/(\d{1,2})/(\d{1,2})", normalized)
    if iso_match:
        year, month, day = map(int, iso_match.groups())
        year = _to_gregorian_year(year)
        try:
            return datetime(year, month, day, tzinfo=timezone.utc)
        except ValueError:
            return None

    iso_dash_match = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", normalized)
    if iso_dash_match:
        year, month, day = map(int, iso_dash_match.groups())
        year = _to_gregorian_year(year)
        try:
            return datetime(year, month, day, tzinfo=timezone.utc)
        except ValueError:
            return None

    # Try every date-shaped substring — the first may be a Hijri date with an
    # unrecognised month name (e.g. 'شوال'), so keep trying until one parses.
    for arabic_match in re.finditer(
        r"(\d{1,2})\s+([^\s\d]+?[\u0600-\u06FF][^\s\d]*)\s+(\d{4})",
        normalized,
    ):
        day = int(arabic_match.group(1))
        month_name = arabic_match.group(2).strip().lower()
        year = int(arabic_match.group(3))
        year = _to_gregorian_year(year)
        month = ARABIC_MONTHS.get(month_name)
        if month:
            try:
                return datetime(year, month, day, tzinfo=timezone.utc)
            except ValueError:
                continue

    return None


def is_recent_article_date(article_date: datetime | None, max_age_days: int = ARTICLE_MAX_AGE_DAYS) -> bool:
    if article_date is None:
        return False
    cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
    return article_date >= cutoff


def load_cache() -> dict:
    try:
        if CACHE_PATH.exists():
            return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning(f"Failed to load cache: {exc}")
    return {}


def save_cache(cache: dict) -> None:
    try:
        CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as exc:
        logger.warning(f"Failed to save cache: {exc}")


def build_cache_key(url: str) -> str:
    return (url or "").strip().lower()


def clamp_importance(value, fallback: int = 5) -> int:
    try:
        parsed = int(str(value).strip())
    except Exception:
        parsed = fallback
    return max(1, min(10, parsed))


def default_importance_from_rss(rss_score: int) -> int:
    if rss_score >= 100:
        return 9
    if rss_score >= 80:
        return 8
    if rss_score >= 50:
        return 7
    if rss_score >= 30:
        return 6
    return 5


def compute_final_score(rss_score: int, importance: int) -> float:
    return round((float(rss_score) * 0.7) + (float(importance) * 0.3), 2)


def normalize_summary(value, fallback_text: str) -> list[str]:
    items = []

    if isinstance(value, list):
        items = [str(item).strip(" -\u2022") for item in value if str(item).strip()]
    elif isinstance(value, str):
        items = [part.strip(" -\u2022") for part in SUMMARY_SPLIT_PATTERN.split(value) if part.strip()]

    if len(items) < 2:
        fallback_parts = [part.strip(" -\u2022") for part in SUMMARY_SPLIT_PATTERN.split(fallback_text) if part.strip()]
        for part in fallback_parts:
            if len(items) >= 2:
                break
            items.append(part)

    if len(items) < 2:
        items.extend(["", ""])

    normalized = []
    for item in items[:2]:
        cleaned = re.sub(r"\s+", " ", item).strip()
        normalized.append(cleaned[:220] if cleaned else fallback_text[:220])

    if not normalized[0]:
        normalized[0] = fallback_text[:220] or "\u0645\u062d\u062a\u0648\u0649 \u0627\u0644\u062e\u0628\u0631 \u063a\u064a\u0631 \u0645\u062a\u0627\u062d."
    if not normalized[1]:
        normalized[1] = normalized[0]

    return normalized


def clean_summary_lines(response_text: str, language: str = "ar") -> list[str]:
    cleaned = clean_json_response(response_text)
    normalized_language = normalize_content_language(language)
    lines = []
    for raw_line in cleaned.splitlines():
        line = re.sub(r"\s+", " ", raw_line).strip()
        line = re.sub(r"^[\-\u2022\u25cf\d\.\)\(]+", "", line).strip()
        if not line:
            continue
        if normalized_language == "ar" and re.search(r"[A-Za-z]", line):
            line = re.sub(r"[A-Za-z]", "", line).strip()
        if line:
            lines.append(line)
    return lines


def manual_fix_three_lines(lines: list[str], fallback_text: str, language: str = "ar") -> list[str]:
    normalized_language = normalize_content_language(language)
    merged = lines[:]
    if len(merged) < 3:
        fallback_parts = [part.strip(" -\u2022") for part in SUMMARY_SPLIT_PATTERN.split(fallback_text) if part.strip()]
        for part in fallback_parts:
            if len(merged) >= 3:
                break
            merged.append(re.sub(r"\s+", " ", part).strip())

    if len(merged) > 3:
        merged = merged[:2] + [" ".join(merged[2:]).strip()]

    while len(merged) < 3:
        merged.append(merged[-1] if merged else fallback_summary_line(normalized_language))

    fixed_lines = []
    for line in merged[:3]:
        cleaned = re.sub(r"[A-Za-z]", "", line) if normalized_language == "ar" else line
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        fixed_lines.append(cleaned[:220] if cleaned else fallback_summary_line(normalized_language))
    return fixed_lines


def summarize_3_lines(news_text: str, language: str | None = None) -> list[str]:
    normalized_language = normalize_content_language(language)
    fallback_text = re.sub(r"\s+", " ", (news_text or "")).strip()
    prompt = build_summary_prompt(news_text, normalized_language)
    payload = {
        "model": "mistral",
        "prompt": prompt,
        "stream": False,
        "options": {
            "num_ctx": 8192,
            "temperature": 0.2,
        },
    }

    last_lines = []
    for _ in range(2):
        try:
            response = requests.post("http://localhost:11434/api/generate", json=payload, timeout=90)
            response.raise_for_status()
            raw_output = response.json().get("response", "")
            lines = clean_summary_lines(raw_output, normalized_language)
            if len(lines) == 3:
                return lines
            last_lines = lines
        except Exception as exc:
            logger.warning(f"3-line summary generation failed: {exc}")

    return manual_fix_three_lines(last_lines, fallback_text, normalized_language)


def fallback_summary_text(news_item: dict, content: str) -> str:
    description = str(news_item.get("description", "")).strip()
    if description:
        return description
    return content[:440].replace("\n", " ").strip() or news_item.get("title", "")


def normalize_category(value: str) -> str:
    category = str(value or "").strip()
    if category not in ALLOWED_CATEGORIES:
        return FALLBACK_CATEGORY
    return category


def normalize_image_keyword(value: str, fallback_title: str) -> str:
    keyword = re.sub(r"\s+", " ", str(value or "").strip())
    if keyword:
        return keyword[:120]
    fallback = re.sub(r"[^\w\s-]", " ", fallback_title)
    fallback = re.sub(r"\s+", " ", fallback).strip()
    return fallback[:120] or "Saudi financial news"


def get_business_priority(title: str, description: str) -> tuple[str, int]:
    bucket = detect_business_bucket(title, description)
    return bucket, BUSINESS_PRIORITY[bucket]


def resolve_final_url(url: str) -> str | None:
    if not url:
        return None

    if url in _url_cache:
        return _url_cache[url]

    if "news.google.com" not in url.lower():
        _url_cache[url] = url
        return url

    try:
        response = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=5,
            allow_redirects=True,
        )
        resolved_url = response.url
        if resolved_url and "news.google.com" not in resolved_url.lower():
            _url_cache[url] = resolved_url
            logger.info(f"Resolved Google News URL via requests: {resolved_url}")
            return resolved_url
    except Exception as exc:
        logger.warning(f"Requests-based Google News resolution failed for {url}: {exc}")

    if uc is None:
        logger.warning(f"Google News resolution requires Selenium fallback but undetected_chromedriver is unavailable: {url}")
        _url_cache[url] = None
        return None

    driver = None
    try:
        options = uc.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-gpu")

        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            driver = uc.Chrome(options=options)
        driver.get(url)
        time.sleep(3)

        resolved_url = driver.current_url
        if resolved_url and "news.google.com" not in resolved_url.lower():
            _url_cache[url] = resolved_url
            logger.info(f"Resolved Google News URL via Selenium: {resolved_url}")
            return resolved_url
    except Exception as exc:
        logger.warning(f"Selenium Google News resolution failed for {url}: {exc}")
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass

    _url_cache[url] = None
    logger.warning(f"Failed to resolve Google News URL: {url}")
    return None


def get_domain_selectors(url: str) -> list[str]:
    host = urlparse(url).netloc.lower()
    if host.startswith("www."):
        host = host[4:]

    for domain, selectors in DOMAIN_SELECTORS.items():
        if host == domain or host.endswith(f".{domain}"):
            return selectors + GENERIC_SELECTORS
    return GENERIC_SELECTORS


def is_argaam_garbage_text(text: str) -> bool:
    if not text:
        return False

    ticker_hits = len(re.findall(ARGAAM_GARBAGE_PATTERNS[0], text))
    line_ticker_hits = len(re.findall(r"(?m)^\s*\d{4}\s*-\s*", text))
    keyword_hits = sum(1 for pattern in ARGAAM_GARBAGE_PATTERNS[1:] if re.search(pattern, text, re.IGNORECASE))

    if ticker_hits >= 8:
        return True
    if line_ticker_hits >= 4:
        return True
    if ticker_hits >= 4 and keyword_hits >= 2:
        return True
    if keyword_hits >= 4:
        return True
    return False


def postprocess_domain_text(url: str, text: str) -> str:
    if not text:
        return ""

    host = urlparse(url).netloc.lower()
    if host.startswith("www."):
        host = host[4:]

    if host == "argaam.com" or host.endswith(".argaam.com"):
        paragraphs = []
        for paragraph in re.split(r"\n{2,}", text):
            cleaned = re.sub(r"\s+", " ", paragraph).strip()
            if not cleaned:
                continue
            if re.match(r"^\d{4}\s*-\s*", cleaned):
                continue
            if is_argaam_garbage_text(cleaned):
                continue
            paragraphs.append(cleaned)

        text = "\n\n".join(paragraphs).strip()
        if is_argaam_garbage_text(text):
            return ""

    return text


def extract_argaam_article_text(page) -> str:
    try:
        extracted = page.evaluate(
            """() => {
                const articleRoot = document.querySelector('#articledetail');
                if (!articleRoot) return "";

                const container =
                    articleRoot.querySelector('.ck-editor.article.m-bottom') ||
                    articleRoot.querySelector('.ck-editor.article');

                if (!container) return "";

                const clone = container.cloneNode(true);
                clone.querySelectorAll('script, style, noscript, iframe, .file-attachment').forEach(node => node.remove());
                const paragraphs = Array.from(clone.querySelectorAll('p'))
                    .map(node => (node.innerText || "").replace(/\u00a0/g, ' ').trim())
                    .filter(text => text.length > 40);

                if (paragraphs.length > 0) {
                    return paragraphs.join('\\n\\n').trim();
                }

                return (clone.innerText || "").trim();
            }"""
        )
        cleaned = clean_extracted_text(extracted)
        return postprocess_domain_text("https://www.argaam.com", cleaned)
    except Exception:
        return ""


def clean_extracted_text(raw_text: str) -> str:
    if not raw_text:
        return ""

    lines = []
    seen = set()
    for line in re.split(r"[\r\n]+", raw_text):
        cleaned = re.sub(r"\s+", " ", line).strip()
        if not cleaned:
            continue
        if any(re.search(pattern, cleaned, re.IGNORECASE) for pattern in NOISE_PATTERNS):
            continue
        if len(cleaned) < 25 and len(cleaned.split()) < 5:
            continue
        if cleaned in seen:
            continue
        seen.add(cleaned)
        lines.append(cleaned)

    text = "\n\n".join(lines)
    text = re.sub(r"(\n\s*){3,}", "\n\n", text)
    return text.strip()


def extract_article_text(page, selectors: list[str]) -> str:
    try:
        page.evaluate(
            """() => {
                document.querySelectorAll(
                    'nav, header, footer, aside, .menu, .mega-menu, .sidebar, .related, .tags, '
                    + '.advertisement, .ad, .share-tools, .social-share, .recommended, .most-read, '
                    + '.share-buttons, .comments, .comments-section, .overlay-modal, .article-breif-content, '
                    + '.article-breif-contentPane, .file-attachment, .ad-space, script, style, noscript, iframe, '
                    + '[role="navigation"], [role="banner"]'
                ).forEach(node => node.remove());
            }"""
        )
    except Exception:
        pass

    for selector in selectors:
        try:
            extracted = page.evaluate(
                """(sel) => {
                    const nodes = Array.from(document.querySelectorAll(sel));
                    const candidates = nodes
                        .map(node => (node.innerText || "").trim())
                        .filter(text => text.length > 0)
                        .sort((a, b) => b.length - a.length);
                    return candidates[0] || "";
                }""",
                selector,
            )
        except Exception:
            extracted = ""

        cleaned = clean_extracted_text(extracted)
        if len(cleaned) >= MIN_CONTENT_LENGTH:
            return cleaned

    html_content = page.content()
    fallback = trafilatura.extract(
        html_content,
        include_comments=False,
        include_tables=False,
        favor_precision=True,
        favor_recall=False,
    )
    return clean_extracted_text(fallback or "")


def get_original_image_url(page) -> str:
    try:
        return page.evaluate(
            """() => {
                const candidates = [
                    document.querySelector('meta[property="og:image"]')?.content || "",
                    document.querySelector('meta[name="twitter:image"]')?.content || "",
                    document.querySelector('article img')?.src || "",
                    document.querySelector('img')?.src || ""
                ];
                return candidates.find(Boolean) || "";
            }"""
        )
    except Exception:
        return ""


def get_article_date_from_page(page) -> datetime | None:
    try:
        date_candidates = page.evaluate(
            """() => {
                return [
                    ...Array.from(document.querySelectorAll('.flex.items-center.gap-1')).map(node => node.innerText || ""),
                    document.querySelector('[data-testid="text-article-subtitle"]')?.innerText || "",
                    document.querySelector('[data-testid="text-article-date"]')?.innerText || "",
                    document.querySelector('.text-article-subtitle')?.innerText || "",
                    document.querySelector('.article-subtitle')?.innerText || "",
                    document.querySelector('.article-meta')?.innerText || "",
                    document.querySelector('.article-info')?.innerText || "",
                    document.querySelector('.entry-date')?.innerText || "",
                    document.querySelector('.post-date')?.innerText || "",
                    document.querySelector('.td-post-date')?.innerText || "",
                    document.querySelector('.single-post-meta')?.innerText || "",
                    document.querySelector('.news-date')?.innerText || "",
                    document.querySelector('.date')?.innerText || "",
                    document.querySelector('#articledetail .date-posted')?.innerText || "",
                    document.querySelector('.date-posted')?.innerText || "",
                    document.querySelector('time')?.getAttribute('datetime') || "",
                    document.querySelector('time[datetime]')?.innerText || "",
                    document.querySelector('time')?.innerText || "",
                    document.querySelector('meta[property="article:published_time"]')?.content || "",
                    document.querySelector('meta[property="article:modified_time"]')?.content || "",
                    document.querySelector('meta[name="publishdate"]')?.content || "",
                    document.querySelector('meta[name="date"]')?.content || "",
                    document.querySelector('meta[name="pubdate"]')?.content || "",
                    document.querySelector('meta[itemprop="datePublished"]')?.content || "",
                    document.querySelector('.published-date')?.innerText || "",
                    document.querySelector('.article-date')?.innerText || "",
                    document.querySelector('div.article-time time')?.innerText || "",
                ].filter(Boolean);
            }"""
        )
    except Exception:
        return None

    for candidate in date_candidates:
        parsed = parse_article_date_text(str(candidate))
        if parsed is not None:
            return parsed
    return None


def extract_sabq_date(page) -> datetime | None:
    try:
        date_candidates = page.evaluate(
            """() => {
                return Array.from(document.querySelectorAll('.flex.items-center.gap-1'))
                    .map(node => (node.innerText || '').trim())
                    .filter(Boolean);
            }"""
        )
    except Exception as exc:
        logger.warning(f"Sabq date extraction failed while reading page nodes: {exc}")
        return None

    for candidate in date_candidates:
        parsed = parse_article_date_text(str(candidate))
        if parsed is not None:
            return parsed

    logger.warning("Sabq date extraction failed: no parsable date found in '.flex.items-center.gap-1'.")
    return None


def scrape_full_article(
    page,
    url: str,
    article_title: str | None = None,
    source_hint: str | None = None,
) -> tuple[str, str, str, bool]:
    final_url = resolve_final_url(url, article_title=article_title, source_hint=source_hint)
    if not final_url or not is_allowed_domain(final_url):
        logger.warning(f"Rejected article after final URL resolution: {final_url or url}")
        return "", "", final_url, False

    try:
        page.goto(final_url, wait_until="domcontentloaded", timeout=20000)
        try:
            page.wait_for_load_state("networkidle", timeout=5000)
        except Exception:
            pass
    except Exception as exc:
        logger.warning(f"Failed to load resolved article URL {final_url}: {exc}")
        return "", "", final_url, False

    host = urlparse(final_url).netloc.lower()
    if host.startswith("www."):
        host = host[4:]

    # Extract date for metadata only — never reject based on page-extracted date.
    # Freshness filtering already happened at the RSS level in news_fetcher.py.
    if host == "sabq.org" or host.endswith(".sabq.org"):
        article_date = extract_sabq_date(page)
        if article_date is None:
            logger.info(f"Sabq date not found for {final_url} — proceeding anyway.")
    else:
        article_date = get_article_date_from_page(page)
        if article_date is None:
            logger.info(f"Page date not found for {final_url} — proceeding anyway.")

    selectors = get_domain_selectors(final_url)

    if host == "argaam.com" or host.endswith(".argaam.com"):
        text = extract_argaam_article_text(page)
        if not text:
            text = extract_article_text(page, selectors)
    else:
        text = extract_article_text(page, selectors)

    text = postprocess_domain_text(final_url, text)
    image_url = get_original_image_url(page)
    scrape_full_article.last_article_date = article_date
    # Always return stale=False — rejection by page date was removed intentionally.
    return text, image_url, final_url, False


def parse_llm_json(raw_output: str) -> dict:
    cleaned = clean_json_response(raw_output)
    return json.loads(cleaned)


def normalize_llm_output(
    parsed_data: dict,
    news_item: dict,
    content_to_use: str,
    verified: bool,
    language: str | None = None,
) -> dict:
    normalized_language = detect_processing_language(news_item, content_to_use) if language is None else normalize_content_language(language)
    fallback_summary = fallback_summary_text(news_item, content_to_use)
    fallback_importance = default_importance_from_rss(int(news_item.get("rss_score", 0)))
    title = str(parsed_data.get("title", "")).strip() or news_item.get("title", "")
    if normalized_language == "en" and re.search(r"[\u0600-\u06FF]", title) and re.search(r"[A-Za-z]", str(news_item.get("title", ""))):
        title = str(news_item.get("title", "")).strip()
    summary = normalize_summary(parsed_data.get("summary", []), fallback_summary)
    category = normalize_category(parsed_data.get("category", FALLBACK_CATEGORY))
    importance = clamp_importance(parsed_data.get("importance", fallback_importance), fallback_importance)
    image_keyword = normalize_image_keyword(parsed_data.get("image_keyword", ""), title)
    bucket, priority = get_business_priority(title, " ".join(summary))
    rss_score = int(news_item.get("rss_score", 0))

    normalized = {
        "title": title,
        "summary": summary,
        "category": category,
        "importance": importance,
        "image_keyword": image_keyword,
        "link": news_item.get("link", ""),
        "published": news_item.get("published", ""),
        "source": news_item.get("source", ""),
        "rss_score": rss_score,
        "score": rss_score,
        "business_bucket": bucket,
        "business_priority": priority,
        "language": normalized_language,
        "verification_status": "verified" if verified else "unverified",
    }
    normalized["final_score"] = compute_final_score(normalized["rss_score"], normalized["importance"])
    return normalized


def call_ollama_with_retries(news_item: dict, content_to_use: str, language: str | None = None) -> dict:
    normalized_language = detect_processing_language(news_item, content_to_use) if language is None else normalize_content_language(language)
    prompt = build_structured_prompt(news_item, content_to_use, normalized_language)
    payload = {
        "model": MODEL_NAME,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": {
            "num_ctx": 8192,
            "temperature": 0.2,
        },
    }

    last_error = None
    for attempt in range(1, LLM_RETRY_LIMIT + 1):
        try:
            response = requests.post(OLLAMA_API_URL, json=payload, timeout=90)
            response.raise_for_status()
            raw_output = response.json().get("response", "")
            parsed = parse_llm_json(raw_output)
            return normalize_llm_output(parsed, news_item, content_to_use, verified=True, language=normalized_language)
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            last_error = exc
            logger.warning(f"Invalid JSON from Ollama for '{news_item.get('title', '')[:60]}', retry {attempt}/{LLM_RETRY_LIMIT}")
        except requests.exceptions.RequestException as exc:
            last_error = exc
            logger.error(f"Failed to communicate with Ollama API: {exc}")
            break
        except Exception as exc:
            last_error = exc
            logger.warning(f"Unexpected Ollama parsing error: {exc}")

    logger.warning(f"Using safe fallback output for '{news_item.get('title', '')[:60]}': {last_error}")
    fallback_parsed = {
        "title": news_item.get("title", ""),
        "summary": normalize_summary([], fallback_summary_text(news_item, content_to_use)),
        "category": FALLBACK_CATEGORY,
        "importance": default_importance_from_rss(int(news_item.get("rss_score", 0))),
        "image_keyword": news_item.get("title", "Saudi financial news"),
    }
    return normalize_llm_output(fallback_parsed, news_item, content_to_use, verified=False, language=normalized_language)


def get_cached_result(cache: dict, url_candidates: list[str]) -> dict | None:
    for candidate in url_candidates:
        key = build_cache_key(candidate)
        if key and key in cache:
            payload = cache[key]
            if payload.get("extraction_cache_version") != EXTRACTION_CACHE_VERSION:
                continue
            return deepcopy(payload)
    return None


def update_cache(cache: dict, result: dict, original_url: str, final_url: str) -> None:
    payload = deepcopy(result)
    payload["canonical_url"] = final_url or original_url
    payload["extraction_cache_version"] = EXTRACTION_CACHE_VERSION
    for candidate in {build_cache_key(original_url), build_cache_key(final_url)}:
        if candidate:
            cache[candidate] = payload


def process_news_item(news_item: dict, page=None, cache: dict | None = None) -> dict | None:
    url = news_item.get("link", "")
    logger.info(f"Preparing to process: '{news_item.get('title', '')[:60]}'")
    host = urlparse(url).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    skip_cache_domains = {"argaam.com", "sabq.org", "maaal.com", "spa.gov.sa"}
    skip_cache = any(host == domain or host.endswith(f".{domain}") for domain in skip_cache_domains)

    cached = None if skip_cache else get_cached_result(cache or {}, [url])
    if cached:
        cached["rss_score"] = int(news_item.get("rss_score", cached.get("rss_score", 0)))
        cached["score"] = cached["rss_score"]
        cached["final_score"] = compute_final_score(cached["rss_score"], clamp_importance(cached.get("importance", 5)))
        return cached

    full_text = ""
    original_image_url = ""
    final_url = url

    if url and page:
        full_text, original_image_url, final_url, _ = scrape_full_article(
            page,
            url,
            article_title=news_item.get("title", ""),
            source_hint=news_item.get("source_domain") or news_item.get("source", ""),
        )
    elif url:
        final_url = resolve_final_url(
            url,
            article_title=news_item.get("title", ""),
            source_hint=news_item.get("source_domain") or news_item.get("source", ""),
        )

    if final_url and not is_allowed_domain(final_url):
        logger.warning(f"Skipping non-approved source after resolution: {final_url}")
        return None

    if not final_url:
        logger.warning(f"Skipping article because final URL resolution failed: {url}")
        return None

    content_to_use = full_text if len(full_text) >= MIN_CONTENT_LENGTH else ""
    if not content_to_use:
        logger.warning(f"Skipping article because extracted page content was insufficient: {final_url}")
        return None

    if not content_to_use:
        logger.warning(f"No usable content found for '{news_item.get('title', '')[:60]}'.")
        return None

    if host == "argaam.com" or host.endswith(".argaam.com"):
        preview = content_to_use[:300].replace("\n", " ")
        logger.info(f"Argaam extracted preview: {preview}")

    language = detect_processing_language(news_item, content_to_use)
    normalized = call_ollama_with_retries(news_item, content_to_use, language=language)
    summary_lines = summarize_3_lines(content_to_use, language=language)
    normalized["summary"] = summary_lines
    normalized["summary_3_lines"] = "\n".join(summary_lines)
    normalized["link"] = final_url or url
    normalized["canonical_url"] = final_url or url
    normalized["language"] = language
    normalized["original_text"] = content_to_use
    normalized["original_image_url"] = original_image_url
    normalized["use_original_image"] = bool(original_image_url)
    article_date = getattr(scrape_full_article, "last_article_date", None)
    normalized["article_date"] = article_date.isoformat() if article_date else ""

    cached = get_cached_result(cache or {}, [final_url])
    if cached:
        return cached

    if cache is not None:
        update_cache(cache, normalized, url, final_url)

    return normalized


def process_all_news(news_items: list) -> list:
    processed = []
    cache = load_cache()

    try:
        import asyncio
        import sys

        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                ],
            )
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
            )
            context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            page = context.new_page()

            for item in news_items:
                result = process_news_item(item, page=page, cache=cache)
                if result:
                    processed.append(result)

            browser.close()
    except Exception as exc:
        logger.error(f"Playwright browser initialization failed: {exc}")
        for item in news_items:
            result = process_news_item(item, page=None, cache=cache)
            if result:
                processed.append(result)

    save_cache(cache)
    return processed


def _load_cache_optimized() -> dict:
    return load_json_cache(CACHE_PATH)


def _save_cache_optimized(cache: dict) -> None:
    try:
        save_json_cache(
            CACHE_PATH,
            cache,
            max_entries=PROCESSED_CACHE_MAX_ENTRIES,
            ttl_seconds=PROCESSED_CACHE_TTL_SECONDS,
        )
    except Exception as exc:
        logger.warning(f"Failed to save cache: {exc}")


def _load_article_cache() -> dict:
    return load_json_cache(ARTICLE_CACHE_PATH)


def _save_article_cache(article_cache: dict) -> None:
    try:
        save_json_cache(
            ARTICLE_CACHE_PATH,
            article_cache,
            max_entries=ARTICLE_CACHE_MAX_ENTRIES,
            ttl_seconds=ARTICLE_CACHE_TTL_SECONDS,
        )
    except Exception as exc:
        logger.warning(f"Failed to save article cache: {exc}")


def _strip_internal_fields(payload: dict) -> dict:
    cleaned = deepcopy(payload)
    cleaned.pop("cache_kind", None)
    cleaned.pop("cached_at", None)
    cleaned.pop("extraction_cache_version", None)
    cleaned.pop("payload", None)
    return cleaned


def _parse_optional_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _structured_cache_key(content_hash: str) -> str:
    return f"llm::{content_hash}"


def _summary_cache_key(content_hash: str) -> str:
    return f"summary::{content_hash}"


def _build_structured_hash(news_item: dict, content_to_use: str, language: str) -> str:
    return stable_hash(
        "structured",
        EXTRACTION_CACHE_VERSION,
        STRUCTURED_PROMPT_CACHE_VERSION,
        MODEL_NAME,
        normalize_content_language(language),
        news_item.get("title", ""),
        content_to_use,
    )


def _build_summary_hash(content_to_use: str, language: str) -> str:
    return stable_hash(
        "summary-3-lines",
        EXTRACTION_CACHE_VERSION,
        SUMMARY_PROMPT_CACHE_VERSION,
        normalize_content_language(language),
        content_to_use,
    )


def _get_internal_cache(cache: dict, key: str, kind: str):
    payload = cache.get(key)
    if not isinstance(payload, dict):
        return None
    if payload.get("cache_kind") != kind:
        return None
    if payload.get("extraction_cache_version") != EXTRACTION_CACHE_VERSION:
        return None
    return deepcopy(payload.get("payload"))


def _set_internal_cache(cache: dict, key: str, kind: str, payload) -> None:
    cache[key] = {
        "cache_kind": kind,
        "cached_at": utc_now_iso(),
        "extraction_cache_version": EXTRACTION_CACHE_VERSION,
        "payload": deepcopy(payload),
    }


def _get_cached_result_optimized(cache: dict, url_candidates: list[str]) -> dict | None:
    for candidate in url_candidates:
        key = build_cache_key(candidate)
        if not key or key not in cache:
            continue
        payload = cache[key]
        if not isinstance(payload, dict):
            continue
        cache_kind = payload.get("cache_kind")
        if cache_kind not in {None, "processed_result"}:
            continue
        version = payload.get("extraction_cache_version")
        if version not in {None, EXTRACTION_CACHE_VERSION}:
            continue
        return _strip_internal_fields(payload)
    return None


def _update_cache_optimized(cache: dict, result: dict, original_url: str, final_url: str) -> None:
    payload = deepcopy(result)
    payload["canonical_url"] = final_url or original_url
    payload["extraction_cache_version"] = EXTRACTION_CACHE_VERSION
    payload["cached_at"] = utc_now_iso()
    payload["cache_kind"] = "processed_result"
    for candidate in {build_cache_key(original_url), build_cache_key(final_url)}:
        if candidate:
            cache[candidate] = payload


def _get_cached_article(article_cache: dict, url_candidates: list[str]) -> dict | None:
    for candidate in url_candidates:
        key = build_cache_key(candidate)
        if not key or key not in article_cache:
            continue
        payload = article_cache[key]
        if not isinstance(payload, dict):
            continue
        if payload.get("cache_kind") != "scraped_article":
            continue
        if payload.get("extraction_cache_version") != EXTRACTION_CACHE_VERSION:
            continue
        return _strip_internal_fields(payload)
    return None


def _update_article_cache(article_cache: dict, article_payload: dict, original_url: str, final_url: str) -> None:
    payload = deepcopy(article_payload)
    payload["canonical_url"] = final_url or original_url
    payload["extraction_cache_version"] = EXTRACTION_CACHE_VERSION
    payload["cached_at"] = utc_now_iso()
    payload["cache_kind"] = "scraped_article"
    for candidate in {build_cache_key(original_url), build_cache_key(final_url)}:
        if candidate:
            article_cache[candidate] = payload


def _summarize_3_lines_optimized(news_text: str, language: str | None = None) -> list[str]:
    normalized_language = normalize_content_language(language)
    fallback_text = re.sub(r"\s+", " ", (news_text or "")).strip()
    prompt = build_summary_prompt(news_text, normalized_language)
    payload = {
        "model": "mistral",
        "prompt": prompt,
        "stream": False,
        "options": {
            "num_ctx": 8192,
            "temperature": 0.2,
        },
    }

    last_lines = []
    for attempt in range(1, 3):
        try:
            with _ollama_request_slot():
                response = request_with_retry(
                    "POST",
                    "http://localhost:11434/api/generate",
                    session_name="ollama_summary",
                    json=payload,
                    timeout=90,
                    headers={"Content-Type": "application/json"},
                )
            response.raise_for_status()
            raw_output = response.json().get("response", "")
            lines = clean_summary_lines(raw_output, normalized_language)
            if len(lines) == 3:
                return lines
            last_lines = lines
        except Exception as exc:
            logger.warning(f"3-line summary generation failed: {exc}")
            time.sleep(min(2.0, 0.4 * attempt))

    return manual_fix_three_lines(last_lines, fallback_text, normalized_language)


def _call_ollama_with_retries_optimized(news_item: dict, content_to_use: str, language: str | None = None) -> dict:
    normalized_language = detect_processing_language(news_item, content_to_use) if language is None else normalize_content_language(language)
    prompt = build_structured_prompt(news_item, content_to_use, normalized_language)
    payload = {
        "model": MODEL_NAME,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": {
            "num_ctx": 8192,
            "temperature": 0.2,
        },
    }

    last_error = None
    for attempt in range(1, LLM_RETRY_LIMIT + 1):
        try:
            with _ollama_request_slot():
                response = request_with_retry(
                    "POST",
                    OLLAMA_API_URL,
                    session_name="ollama_structured",
                    json=payload,
                    timeout=90,
                    headers={"Content-Type": "application/json"},
                )
            response.raise_for_status()
            raw_output = response.json().get("response", "")
            parsed = parse_llm_json(raw_output)
            return normalize_llm_output(parsed, news_item, content_to_use, verified=True, language=normalized_language)
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            last_error = exc
            logger.warning(
                f"Invalid JSON from Ollama for '{news_item.get('title', '')[:60]}', retry {attempt}/{LLM_RETRY_LIMIT}"
            )
            time.sleep(min(2.0, 0.4 * attempt))
        except requests.exceptions.RequestException as exc:
            last_error = exc
            logger.error(f"Failed to communicate with Ollama API: {exc}")
            break
        except Exception as exc:
            last_error = exc
            logger.warning(f"Unexpected Ollama parsing error: {exc}")
            time.sleep(min(2.0, 0.4 * attempt))

    logger.warning(f"Using safe fallback output for '{news_item.get('title', '')[:60]}': {last_error}")
    fallback_parsed = {
        "title": news_item.get("title", ""),
        "summary": normalize_summary([], fallback_summary_text(news_item, content_to_use)),
        "category": FALLBACK_CATEGORY,
        "importance": default_importance_from_rss(int(news_item.get("rss_score", 0))),
        "image_keyword": news_item.get("title", "Saudi financial news"),
    }
    return normalize_llm_output(fallback_parsed, news_item, content_to_use, verified=False, language=normalized_language)


def _extract_image_url_from_html(html_text: str) -> str:
    patterns = [
        r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)["\']',
        r'<img[^>]+src=["\']([^"\']+)["\']',
    ]
    for pattern in patterns:
        match = re.search(pattern, html_text, flags=re.IGNORECASE)
        if match:
            return html.unescape(match.group(1)).strip()
    return ""


def _scrape_full_article_requests_only(
    url: str,
    article_title: str | None = None,
    source_hint: str | None = None,
) -> tuple[str, str, str, bool]:
    final_url = url
    if final_url and ("news.google.com" in final_url.lower() or not is_allowed_domain(final_url)):
        final_url = shared_resolve_final_url(
            url,
            article_title=article_title,
            source_hint=source_hint,
        )

    if not final_url or not is_allowed_domain(final_url):
        logger.warning(f"Rejected article after final URL resolution: {final_url or url}")
        return "", "", final_url, False

    try:
        response = request_with_retry(
            "GET",
            final_url,
            session_name="article_html",
            timeout=20,
            allow_redirects=True,
        )
        response.raise_for_status()
        response.encoding = response.apparent_encoding or "utf-8"
        html_text = response.text
    except Exception as exc:
        logger.warning(f"Requests-only article fetch failed for {final_url}: {exc}")
        return "", "", final_url, False

    extracted = trafilatura.extract(
        html_text,
        include_comments=False,
        include_tables=False,
        favor_precision=True,
        favor_recall=False,
    )
    text = postprocess_domain_text(final_url, clean_extracted_text(extracted or ""))
    image_url = _extract_image_url_from_html(html_text)
    article_date = extract_page_date_requests_only(final_url)
    _scrape_full_article_requests_only.last_article_date = article_date
    return text, image_url, final_url, False


def _refresh_cached_processed_result(cached: dict, news_item: dict) -> dict:
    updated = deepcopy(cached)
    rss_score = int(news_item.get("rss_score", updated.get("rss_score", 0)))
    updated["rss_score"] = rss_score
    updated["score"] = rss_score
    updated["published"] = news_item.get("published", updated.get("published", ""))
    updated["source"] = news_item.get("source", updated.get("source", ""))
    updated["link"] = updated.get("canonical_url", news_item.get("link", "")) or news_item.get("link", "")
    updated["language"] = detect_processing_language(news_item, updated.get("original_text", ""))
    updated["final_score"] = compute_final_score(rss_score, clamp_importance(updated.get("importance", 5)))
    return updated


def _prepare_news_item_for_processing(news_item: dict, page, cache: dict, article_cache: dict):
    url = news_item.get("link", "")
    logger.info(f"Preparing to process: '{news_item.get('title', '')[:60]}'")

    cached_result = _get_cached_result_optimized(cache, [url])
    if cached_result:
        return {"kind": "result", "result": _refresh_cached_processed_result(cached_result, news_item)}

    final_url = url
    if final_url and ("news.google.com" in final_url.lower() or not is_allowed_domain(final_url)):
        final_url = shared_resolve_final_url(
            url,
            article_title=news_item.get("title", ""),
            source_hint=news_item.get("source_domain") or news_item.get("source", ""),
        )

    if final_url:
        cached_result = _get_cached_result_optimized(cache, [final_url, url])
        if cached_result:
            return {"kind": "result", "result": _refresh_cached_processed_result(cached_result, news_item)}

    if not final_url:
        logger.warning(f"Skipping article because final URL resolution failed: {url}")
        return None

    if not is_allowed_domain(final_url):
        logger.warning(f"Skipping non-approved source after resolution: {final_url}")
        return None

    article_payload = _get_cached_article(article_cache, [final_url, url])
    if article_payload:
        content_to_use = article_payload.get("original_text", "")
        original_image_url = article_payload.get("original_image_url", "")
        article_date = _parse_optional_datetime(article_payload.get("article_date", ""))
    else:
        full_text = ""
        original_image_url = ""
        article_date = None

        if page is not None:
            full_text, original_image_url, final_url, _ = scrape_full_article(page, final_url)
            article_date = getattr(scrape_full_article, "last_article_date", None)
        if len(full_text) < MIN_CONTENT_LENGTH:
            full_text, original_image_url, final_url, _ = _scrape_full_article_requests_only(final_url)
            article_date = getattr(_scrape_full_article_requests_only, "last_article_date", article_date)

        content_to_use = full_text if len(full_text) >= MIN_CONTENT_LENGTH else ""
        if not content_to_use:
            logger.warning(f"Skipping article because extracted page content was insufficient: {final_url}")
            return None

        _update_article_cache(
            article_cache,
            {
                "original_text": content_to_use,
                "original_image_url": original_image_url,
                "article_date": article_date.isoformat() if article_date else "",
            },
            url,
            final_url,
        )

    if len(content_to_use) < MIN_CONTENT_LENGTH:
        logger.warning(f"No usable content found for '{news_item.get('title', '')[:60]}'.")
        return None

    host = urlparse(final_url).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    if host == "argaam.com" or host.endswith(".argaam.com"):
        preview = content_to_use[:300].replace("\n", " ")
        logger.info(f"Argaam extracted preview: {preview}")

    language = detect_processing_language(news_item, content_to_use)
    return {
        "kind": "prepared",
        "news_item": deepcopy(news_item),
        "original_url": url,
        "final_url": final_url,
        "content_to_use": content_to_use,
        "original_image_url": original_image_url,
        "article_date": article_date,
        "language": language,
        "structured_hash": _build_structured_hash(news_item, content_to_use, language),
        "summary_hash": _build_summary_hash(content_to_use, language),
    }


def _compose_processed_result(prepared: dict, structured: dict, summary_lines: list[str]) -> dict:
    result = deepcopy(structured)
    rss_score = int(prepared["news_item"].get("rss_score", result.get("rss_score", 0)))
    result["summary"] = summary_lines
    result["summary_3_lines"] = "\n".join(summary_lines)
    result["link"] = prepared["final_url"] or prepared["original_url"]
    result["canonical_url"] = prepared["final_url"] or prepared["original_url"]
    result["published"] = prepared["news_item"].get("published", result.get("published", ""))
    result["source"] = prepared["news_item"].get("source", result.get("source", ""))
    result["rss_score"] = rss_score
    result["score"] = rss_score
    result["original_text"] = prepared["content_to_use"]
    result["original_image_url"] = prepared["original_image_url"]
    result["use_original_image"] = bool(prepared["original_image_url"])
    result["article_date"] = prepared["article_date"].isoformat() if prepared["article_date"] else ""
    result["language"] = prepared.get("language", detect_processing_language(prepared["news_item"], prepared["content_to_use"]))
    result["final_score"] = compute_final_score(rss_score, clamp_importance(result.get("importance", 5)))
    return result


def _configure_playwright_event_loop() -> None:
    try:
        import asyncio
        import sys

        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    except Exception as exc:
        logger.debug(f"Failed to configure Playwright event loop policy: {exc}")


def _process_news_item_optimized(news_item: dict, page=None, cache: dict | None = None) -> dict | None:
    processed_cache = cache if cache is not None else load_cache()
    article_cache = _load_article_cache()

    prepared = _prepare_news_item_for_processing(news_item, page=page, cache=processed_cache, article_cache=article_cache)
    if not prepared:
        return None
    if prepared["kind"] == "result":
        return prepared["result"]

    structured = _get_internal_cache(processed_cache, _structured_cache_key(prepared["structured_hash"]), "structured_llm")
    if structured is None:
        structured = call_ollama_with_retries(
            prepared["news_item"],
            prepared["content_to_use"],
            language=prepared["language"],
        )
        _set_internal_cache(processed_cache, _structured_cache_key(prepared["structured_hash"]), "structured_llm", structured)

    summary_lines = _get_internal_cache(processed_cache, _summary_cache_key(prepared["summary_hash"]), "summary_3_lines")
    if summary_lines is None:
        summary_lines = summarize_3_lines(prepared["content_to_use"], language=prepared["language"])
        _set_internal_cache(processed_cache, _summary_cache_key(prepared["summary_hash"]), "summary_3_lines", summary_lines)

    result = _compose_processed_result(prepared, structured, summary_lines)
    _update_cache_optimized(processed_cache, result, prepared["original_url"], prepared["final_url"])

    if cache is None:
        save_cache(processed_cache)
        _save_article_cache(article_cache)
    return result


def _process_all_news_optimized(news_items: list) -> list:
    processed: list[dict] = []
    cache = load_cache()
    article_cache = _load_article_cache()
    prepared_items: list[dict] = []

    def _consume(prepared_item) -> None:
        if not prepared_item:
            return
        if prepared_item["kind"] == "result":
            processed.append(prepared_item["result"])
        else:
            prepared_items.append(prepared_item)

    browser = None
    context = None
    try:
        _configure_playwright_event_loop()
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                ],
            )
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
            )
            context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            context.route(
                "**/*",
                lambda route: route.abort()
                if route.request.resource_type in {"image", "media", "font"}
                else route.continue_(),
            )
            page = context.new_page()
            page.set_default_navigation_timeout(20000)
            page.set_default_timeout(10000)

            for item in news_items:
                _consume(_prepare_news_item_for_processing(item, page=page, cache=cache, article_cache=article_cache))
    except Exception as exc:
        logger.error(f"Playwright browser initialization failed: {type(exc).__name__}: {exc!r}")
        for item in news_items:
            _consume(_prepare_news_item_for_processing(item, page=None, cache=cache, article_cache=article_cache))
    finally:
        if context is not None:
            with contextlib.suppress(Exception):
                context.close()
        if browser is not None:
            with contextlib.suppress(Exception):
                browser.close()

    structured_results: dict[str, dict] = {}
    summary_results: dict[str, list[str]] = {}
    missing_tasks = {}

    for prepared in prepared_items:
        structured = _get_internal_cache(cache, _structured_cache_key(prepared["structured_hash"]), "structured_llm")
        if structured is not None:
            structured_results[prepared["structured_hash"]] = structured
        else:
            missing_tasks.setdefault(("structured", prepared["structured_hash"]), prepared)

        summary_lines = _get_internal_cache(cache, _summary_cache_key(prepared["summary_hash"]), "summary_3_lines")
        if summary_lines is not None:
            summary_results[prepared["summary_hash"]] = summary_lines
        else:
            missing_tasks.setdefault(("summary", prepared["summary_hash"]), prepared)

    if missing_tasks:
        max_workers = min(max(1, LLM_MAX_WORKERS), len(missing_tasks))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {}
            for task_key, prepared in missing_tasks.items():
                task_type, task_hash = task_key
                if task_type == "structured":
                    future = executor.submit(
                        call_ollama_with_retries,
                        prepared["news_item"],
                        prepared["content_to_use"],
                        prepared["language"],
                    )
                else:
                    future = executor.submit(summarize_3_lines, prepared["content_to_use"], prepared["language"])
                future_map[future] = (task_type, task_hash, prepared)

            for future in as_completed(future_map):
                task_type, task_hash, prepared = future_map[future]
                try:
                    value = future.result()
                except Exception as exc:
                    logger.warning(f"Parallel {task_type} generation failed: {exc}")
                    if task_type == "structured":
                        value = normalize_llm_output(
                            {
                                "title": prepared["news_item"].get("title", ""),
                                "summary": normalize_summary([], fallback_summary_text(prepared["news_item"], prepared["content_to_use"])),
                                "category": FALLBACK_CATEGORY,
                                "importance": default_importance_from_rss(int(prepared["news_item"].get("rss_score", 0))),
                                "image_keyword": prepared["news_item"].get("title", "Saudi financial news"),
                            },
                            prepared["news_item"],
                            prepared["content_to_use"],
                            verified=False,
                            language=prepared["language"],
                        )
                    else:
                        value = manual_fix_three_lines([], prepared["content_to_use"], prepared["language"])

                if task_type == "structured":
                    structured_results[task_hash] = value
                    _set_internal_cache(cache, _structured_cache_key(task_hash), "structured_llm", value)
                else:
                    summary_results[task_hash] = value
                    _set_internal_cache(cache, _summary_cache_key(task_hash), "summary_3_lines", value)

    for prepared in prepared_items:
        structured = structured_results.get(prepared["structured_hash"])
        if structured is None:
            structured = call_ollama_with_retries(
                prepared["news_item"],
                prepared["content_to_use"],
                language=prepared["language"],
            )
            _set_internal_cache(cache, _structured_cache_key(prepared["structured_hash"]), "structured_llm", structured)

        summary_lines = summary_results.get(prepared["summary_hash"])
        if summary_lines is None:
            summary_lines = summarize_3_lines(prepared["content_to_use"], language=prepared["language"])
            _set_internal_cache(cache, _summary_cache_key(prepared["summary_hash"]), "summary_3_lines", summary_lines)

        result = _compose_processed_result(prepared, structured, summary_lines)
        processed.append(result)
        _update_cache_optimized(cache, result, prepared["original_url"], prepared["final_url"])

    save_cache(cache)
    _save_article_cache(article_cache)
    return processed


resolve_final_url = shared_resolve_final_url
load_cache = _load_cache_optimized
save_cache = _save_cache_optimized
get_cached_result = _get_cached_result_optimized
update_cache = _update_cache_optimized
summarize_3_lines = _summarize_3_lines_optimized
call_ollama_with_retries = _call_ollama_with_retries_optimized
process_news_item = _process_news_item_optimized
process_all_news = _process_all_news_optimized
