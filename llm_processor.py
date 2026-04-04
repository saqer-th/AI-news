import json
import os
import re
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests
import trafilatura
from playwright.sync_api import sync_playwright

from news_fetcher import ALLOWED_DOMAINS, BUSINESS_PRIORITY, detect_business_bucket, is_allowed_domain
from utils import setup_logger

logger = setup_logger(__name__)

OLLAMA_API_URL = os.getenv("OLLAMA_API_URL", "http://localhost:11434/api/generate")
MODEL_NAME = os.getenv("OLLAMA_MODEL", "llama3:8b")
CACHE_PATH = Path(__file__).resolve().parent / ".cache" / "processed_news.json"
EXTRACTION_CACHE_VERSION = "2026-04-04-stale-page-date-v3"
MIN_CONTENT_LENGTH = 300
ARTICLE_MAX_AGE_DAYS = 7
LLM_RETRY_LIMIT = 3
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
- title, summary items, and category must be in Arabic.
- summary must be an array of exactly 2 concise items in Arabic.
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

SUMMARIZE_3_LINES_PROMPT = """\u0623\u0646\u062a \u0645\u062d\u0631\u0631 \u0623\u062e\u0628\u0627\u0631 \u0645\u062d\u062a\u0631\u0641.

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

DOMAIN_SELECTORS = {
    "argaam.com": [
        "#articledetail .ck-editor.article.m-bottom",
        "#articledetail .ck-editor.article",
        ".article-detail-content .ck-editor.article.m-bottom",
        ".article-detail-content .ck-editor.article",
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

    iso_match = re.search(r"(\d{4})/(\d{1,2})/(\d{1,2})", normalized)
    if iso_match:
        year, month, day = map(int, iso_match.groups())
        try:
            return datetime(year, month, day, tzinfo=timezone.utc)
        except ValueError:
            return None

    arabic_match = re.search(
        r"(\d{1,2})\s+([^\s]+)\s+(\d{4})",
        normalized,
    )
    if arabic_match:
        day = int(arabic_match.group(1))
        month_name = arabic_match.group(2).strip().lower()
        year = int(arabic_match.group(3))
        month = ARABIC_MONTHS.get(month_name)
        if month:
            try:
                return datetime(year, month, day, tzinfo=timezone.utc)
            except ValueError:
                return None

    return None


def is_recent_article_date(article_date: datetime | None, max_age_days: int = ARTICLE_MAX_AGE_DAYS) -> bool:
    if article_date is None:
        return True
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


def clean_summary_lines(response_text: str) -> list[str]:
    cleaned = clean_json_response(response_text)
    lines = []
    for raw_line in cleaned.splitlines():
        line = re.sub(r"\s+", " ", raw_line).strip()
        line = re.sub(r"^[\-\u2022\u25cf\d\.\)\(]+", "", line).strip()
        if not line:
            continue
        if re.search(r"[A-Za-z]", line):
            line = re.sub(r"[A-Za-z]", "", line).strip()
        if line:
            lines.append(line)
    return lines


def manual_fix_three_lines(lines: list[str], fallback_text: str) -> list[str]:
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
        merged.append(merged[-1] if merged else "\u0644\u0645 \u062a\u062a\u0648\u0641\u0631 \u062a\u0641\u0627\u0635\u064a\u0644 \u0643\u0627\u0641\u064a\u0629.")

    fixed_lines = []
    for line in merged[:3]:
        cleaned = re.sub(r"[A-Za-z]", "", line)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        fixed_lines.append(cleaned[:220] if cleaned else "\u0644\u0645 \u062a\u062a\u0648\u0641\u0631 \u062a\u0641\u0627\u0635\u064a\u0644 \u0643\u0627\u0641\u064a\u0629.")
    return fixed_lines


def summarize_3_lines(news_text: str) -> list[str]:
    fallback_text = re.sub(r"\s+", " ", (news_text or "")).strip()
    prompt = SUMMARIZE_3_LINES_PROMPT.replace("{NEWS_TEXT}", news_text)
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
            lines = clean_summary_lines(raw_output)
            if len(lines) == 3:
                return lines
            last_lines = lines
        except Exception as exc:
            logger.warning(f"3-line summary generation failed: {exc}")

    return manual_fix_three_lines(last_lines, fallback_text)


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


def resolve_final_url(page, url: str) -> str:
    if not url:
        return ""

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=20000)
        try:
            page.wait_for_url(lambda current: "google.com" not in current, timeout=8000)
        except Exception:
            pass

        try:
            page.wait_for_load_state("networkidle", timeout=5000)
        except Exception:
            pass

        candidates = page.evaluate(
            """() => {
                return [
                    window.location.href,
                    document.querySelector('link[rel="canonical"]')?.href || "",
                    document.querySelector('meta[property="og:url"]')?.content || "",
                    document.querySelector('meta[name="twitter:url"]')?.content || ""
                ].filter(Boolean);
            }"""
        )

        current_url = page.url
        for candidate in candidates:
            if candidate.startswith("http") and "google.com" not in candidate:
                if candidate != current_url:
                    page.goto(candidate, wait_until="domcontentloaded", timeout=20000)
                    try:
                        page.wait_for_load_state("networkidle", timeout=5000)
                    except Exception:
                        pass
                return page.url

        return current_url
    except Exception as exc:
        logger.warning(f"Failed to resolve final URL for {url}: {exc}")
        return url


def resolve_final_url_via_requests(url: str) -> str:
    if not url:
        return ""

    try:
        response = requests.get(
            url,
            timeout=15,
            allow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        response.raise_for_status()
        return response.url or url
    except Exception as exc:
        logger.warning(f"Requests-based URL resolution failed for {url}: {exc}")
        return url


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
                    document.querySelector('[data-testid="text-article-subtitle"]')?.innerText || "",
                    document.querySelector('[data-testid="text-article-date"]')?.innerText || "",
                    document.querySelector('#articledetail .date-posted')?.innerText || "",
                    document.querySelector('.date-posted')?.innerText || "",
                    document.querySelector('time')?.getAttribute('datetime') || "",
                    document.querySelector('time')?.innerText || "",
                    document.querySelector('meta[property="article:published_time"]')?.content || "",
                    document.querySelector('meta[name="publishdate"]')?.content || "",
                    document.querySelector('meta[name="date"]')?.content || "",
                    document.querySelector('.published-date')?.innerText || "",
                    document.querySelector('.article-date')?.innerText || ""
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


def scrape_full_article(page, url: str) -> tuple[str, str, str, bool]:
    final_url = resolve_final_url(page, url)
    if not final_url or not is_allowed_domain(final_url):
        logger.warning(f"Rejected article after final URL resolution: {final_url or url}")
        return "", "", final_url, False

    article_date = get_article_date_from_page(page)
    if article_date and not is_recent_article_date(article_date):
        logger.info(f"Skipping stale article based on page date {article_date.date()}: {final_url}")
        return "", "", final_url, True

    selectors = get_domain_selectors(final_url)
    host = urlparse(final_url).netloc.lower()
    if host.startswith("www."):
        host = host[4:]

    if host == "argaam.com" or host.endswith(".argaam.com"):
        text = extract_argaam_article_text(page)
        if not text:
            text = extract_article_text(page, selectors)
    else:
        text = extract_article_text(page, selectors)

    text = postprocess_domain_text(final_url, text)
    image_url = get_original_image_url(page)
    scrape_full_article.last_article_date = article_date
    return text, image_url, final_url, False


def parse_llm_json(raw_output: str) -> dict:
    cleaned = clean_json_response(raw_output)
    return json.loads(cleaned)


def normalize_llm_output(parsed_data: dict, news_item: dict, content_to_use: str, verified: bool) -> dict:
    fallback_summary = fallback_summary_text(news_item, content_to_use)
    fallback_importance = default_importance_from_rss(int(news_item.get("rss_score", 0)))
    title = str(parsed_data.get("title", "")).strip() or news_item.get("title", "")
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
        "verification_status": "verified" if verified else "unverified",
    }
    normalized["final_score"] = compute_final_score(normalized["rss_score"], normalized["importance"])
    return normalized


def call_ollama_with_retries(news_item: dict, content_to_use: str) -> dict:
    prompt = PROMPT_TEMPLATE.format(title=news_item.get("title", ""), content=content_to_use)
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
            return normalize_llm_output(parsed, news_item, content_to_use, verified=True)
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
    return normalize_llm_output(fallback_parsed, news_item, content_to_use, verified=False)


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
    is_argaam = host == "argaam.com" or host.endswith(".argaam.com")

    cached = None if is_argaam else get_cached_result(cache or {}, [url])
    if cached:
        cached["rss_score"] = int(news_item.get("rss_score", cached.get("rss_score", 0)))
        cached["score"] = cached["rss_score"]
        cached["final_score"] = compute_final_score(cached["rss_score"], clamp_importance(cached.get("importance", 5)))
        return cached

    full_text = ""
    original_image_url = ""
    final_url = url
    stale_article = False

    if url and page:
        full_text, original_image_url, final_url, stale_article = scrape_full_article(page, url)
    elif url:
        final_url = resolve_final_url_via_requests(url)

    if final_url and not is_allowed_domain(final_url):
        logger.warning(f"Skipping non-approved source after resolution: {final_url}")
        return None

    if stale_article:
        logger.info(f"Skipping stale article entirely: {final_url or url}")
        return None

    content_to_use = full_text if len(full_text) >= MIN_CONTENT_LENGTH else ""
    if not content_to_use:
        content_to_use = clean_extracted_text(news_item.get("description", ""))
        logger.warning("Full article extraction was low quality. Falling back to RSS summary.")

    if not content_to_use:
        logger.warning(f"No usable content found for '{news_item.get('title', '')[:60]}'.")
        return None

    if is_argaam:
        preview = content_to_use[:300].replace("\n", " ")
        logger.info(f"Argaam extracted preview: {preview}")

    normalized = call_ollama_with_retries(news_item, content_to_use)
    summary_lines = summarize_3_lines(content_to_use)
    normalized["summary"] = summary_lines
    normalized["summary_3_lines"] = "\n".join(summary_lines)
    normalized["link"] = final_url or url
    normalized["canonical_url"] = final_url or url
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
