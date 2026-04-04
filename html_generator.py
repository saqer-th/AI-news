import base64
import html as html_lib
import os
import re
import urllib.parse
from datetime import datetime
from urllib.parse import urlparse

import requests
from hijri_converter import Gregorian

from utils import setup_logger

logger = setup_logger(__name__)

FALLBACK_IMAGE_URL = "https://images.unsplash.com/photo-1542744173-8e7e53415bb0?q=80&w=1200&auto=format&fit=crop"
_IMAGE_DATA_CACHE = {}


def build_bing_image_url(keyword: str) -> str:
    search_query = re.sub(r"\s+", " ", (keyword or "Saudi financial news").replace("-", " ")).strip()
    encoded_query = urllib.parse.quote(search_query)
    return f"https://tse1.mm.bing.net/th?q={encoded_query}&w=800&h=600&c=7&rs=1&p=0"


def is_safe_url(url: str) -> bool:
    parsed = urlparse(url or "")
    return parsed.scheme in {"http", "https", "data"}


def safe_url(url: str, fallback: str = "#") -> str:
    candidate = (url or "").strip()
    if candidate and is_safe_url(candidate):
        return candidate
    return fallback


def get_news_image_url(news: dict, prefer_original: bool = True) -> str:
    original_image = safe_url(news.get("original_image_url", ""), "")
    if prefer_original and original_image:
        return original_image

    keyword = news.get("image_keyword") or news.get("title") or "Saudi financial news"
    image_url = build_bing_image_url(keyword)
    return safe_url(image_url, FALLBACK_IMAGE_URL)


def fetch_image_data_uri(image_url: str) -> str:
    cache_key = image_url.strip()
    if cache_key in _IMAGE_DATA_CACHE:
        return _IMAGE_DATA_CACHE[cache_key]

    try:
        response = requests.get(image_url, timeout=10)
        response.raise_for_status()
        content_type = response.headers.get("Content-Type", "image/jpeg").split(";")[0]
        encoded = base64.b64encode(response.content).decode("utf-8")
        result = f"data:{content_type};base64,{encoded}"
        _IMAGE_DATA_CACHE[cache_key] = result
        return result
    except Exception as exc:
        logger.warning(f"Failed to fetch image '{image_url}': {exc}")
        return FALLBACK_IMAGE_URL


def generate_base64_ai_image(news: dict) -> str:
    image_url = get_news_image_url(news, prefer_original=news.get("use_original_image", False))
    if image_url.startswith("data:"):
        return image_url
    return fetch_image_data_uri(image_url)


CSS_STYLE = """
<style>
    @import url('https://fonts.googleapis.com/css2?family=Tajawal:wght@400;700;800&family=Roboto:wght@400;700;900&display=swap');

    :root {
        --bg-color: #eaf1f7;
        --card-bg: #ffffff;
        --title-color: #0f5c7a;
        --text-color: #243746;
        --border-color: #d7e4ef;
        --accent-color: #0ea5a6;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }

    body {
        font-family: 'Roboto', 'Tajawal', sans-serif;
        background: linear-gradient(180deg, #eef4f8 0%, #e1edf7 100%);
        color: var(--text-color);
        padding: 2rem 1rem;
        line-height: 1.6;
    }

    .container {
        max-width: 980px;
        margin: 0 auto;
        border: 1px solid var(--border-color);
        border-radius: 24px;
        background-color: rgba(255, 255, 255, 0.8);
        box-shadow: 0 30px 80px rgba(28, 57, 84, 0.08);
        padding: 2rem;
    }

    .header-wrapper {
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        margin-bottom: 2rem;
        border-bottom: 1px solid var(--border-color);
        padding-bottom: 1rem;
    }

    .header-en { text-align: left; direction: ltr; }
    .header-ar { text-align: right; direction: rtl; }

    .header-en h1, .header-ar h1 {
        font-size: 2.4rem;
        color: var(--title-color);
        font-weight: 900;
        margin-bottom: 0.2rem;
    }

    .header-en p, .header-ar p {
        font-size: 0.9rem;
        font-weight: 700;
        color: var(--text-color);
    }

    .news-grid {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 1.5rem;
    }

    .card {
        background-color: var(--card-bg);
        border: 1px solid var(--border-color);
        border-radius: 20px;
        display: flex;
        flex-direction: column;
        overflow: hidden;
        min-height: 100%;
    }

    .full-width {
        grid-column: 1 / -1;
        display: grid;
        grid-template-columns: 1fr 1fr;
    }

    .card-content {
        padding: 1.5rem;
        display: flex;
        flex-direction: column;
        gap: 0.85rem;
    }

    .card-title {
        color: var(--title-color);
        font-size: 1.1rem;
        font-weight: 800;
        line-height: 1.5;
    }

    .card-meta {
        font-size: 0.82rem;
        color: #5c7388;
        font-weight: 700;
    }

    .card-summary {
        display: grid;
        gap: 0.5rem;
        color: var(--text-color);
        font-size: 0.95rem;
    }

    .card-summary-line {
        margin: 0;
        line-height: 1.7;
    }

    .card-img {
        width: 100%;
        height: 100%;
        min-height: 240px;
        object-fit: cover;
        background-color: #f5f7fa;
    }

    .read-more {
        font-size: 0.88rem;
        color: white;
        background-color: var(--accent-color);
        padding: 8px 14px;
        border-radius: 999px;
        text-decoration: none;
        display: inline-block;
        align-self: flex-start;
        margin-top: auto;
        font-weight: 700;
    }

    .footer-wrapper {
        margin-top: 2rem;
        border: 1px solid var(--border-color);
        border-radius: 16px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 1rem 1.25rem;
        background-color: #0f5c7a;
        color: white;
    }

    .footer-text {
        color: white;
        font-weight: 800;
        font-size: 1rem;
    }
</style>
"""


def get_arabic_day(weekday: int) -> str:
    return [
        "\u0627\u0644\u0627\u062b\u0646\u064a\u0646",
        "\u0627\u0644\u062b\u0644\u0627\u062b\u0627\u0621",
        "\u0627\u0644\u0623\u0631\u0628\u0639\u0627\u0621",
        "\u0627\u0644\u062e\u0645\u064a\u0633",
        "\u0627\u0644\u062c\u0645\u0639\u0629",
        "\u0627\u0644\u0633\u0628\u062a",
        "\u0627\u0644\u0623\u062d\u062f",
    ][weekday]


def render_summary(summary_raw) -> str:
    if isinstance(summary_raw, list):
        items = summary_raw
    else:
        items = [part.strip() for part in str(summary_raw).splitlines() if part.strip()]

    safe_items = []
    for item in items[:3]:
        clean_item = re.sub(r"\s+", " ", str(item)).strip()
        if clean_item:
            safe_items.append(f"<p class=\"card-summary-line\">{html_lib.escape(clean_item)}</p>")

    if not safe_items:
        safe_items.append("<p class=\"card-summary-line\"></p>")

    return f"<div class=\"card-summary\">{''.join(safe_items)}</div>"


def generate_card_html(news: dict, index: int) -> str:
    title = html_lib.escape(str(news.get("title", "")).strip())
    link = html_lib.escape(safe_url(news.get("link", "")))
    source = html_lib.escape(str(news.get("source", "")).strip())
    score = html_lib.escape(str(news.get("final_score", news.get("rss_score", ""))))
    img_src = html_lib.escape(generate_base64_ai_image(news))
    summary_html = render_summary(news.get("summary", []))

    is_arabic = any("\u0600" <= char <= "\u06FF" for char in str(news.get("title", "")))
    direction = "rtl" if is_arabic else "ltr"
    read_more_text = "\u0644\u0644\u0645\u0632\u064a\u062f" if is_arabic else "Read More"

    img_html = f'<img src="{img_src}" alt="" class="card-img" />'
    text_html = (
        f'<div class="card-content" dir="{direction}">'
        f'<div class="card-meta">{source} | Score {score}</div>'
        f'<h3 class="card-title">{title}</h3>'
        f"{summary_html}"
        f'<a href="{link}" target="_blank" rel="noopener noreferrer" class="read-more">{read_more_text}</a>'
        f"</div>"
    )

    mod = index % 5
    if mod == 0:
        return f'<article class="card full-width">{text_html}{img_html}</article>'
    if mod == 3:
        return f'<article class="card full-width">{img_html}{text_html}</article>'
    return f'<article class="card">{text_html}</article>'


def generate_report(
    news_items: list,
    output_filename: str = "weekly_news.html",
    issue_num: str = "1",
    custom_ar_date: str = None,
    custom_en_date: str = None,
):
    logger.info(f"Generating HTML report for {len(news_items)} news items...")

    cards_html = "".join(generate_card_html(news, idx) for idx, news in enumerate(news_items))

    now = datetime.now()
    en_date = custom_en_date if custom_en_date else now.strftime("%A, %b %d %Y")

    if custom_ar_date:
        ar_date = custom_ar_date
    else:
        hijri = Gregorian.fromdate(now.date()).to_hijri()
        hijri_months = [
            "\u0645\u062d\u0631\u0645",
            "\u0635\u0641\u0631",
            "\u0631\u0628\u064a\u0639 \u0627\u0644\u0623\u0648\u0644",
            "\u0631\u0628\u064a\u0639 \u0627\u0644\u0622\u062e\u0631",
            "\u062c\u0645\u0627\u062f\u0649 \u0627\u0644\u0623\u0648\u0644\u0649",
            "\u062c\u0645\u0627\u062f\u0649 \u0627\u0644\u0622\u062e\u0631\u0629",
            "\u0631\u062c\u0628",
            "\u0634\u0639\u0628\u0627\u0646",
            "\u0631\u0645\u0636\u0627\u0646",
            "\u0634\u0648\u0627\u0644",
            "\u0630\u0648 \u0627\u0644\u0642\u0639\u062f\u0629",
            "\u0630\u0648 \u0627\u0644\u062d\u062c\u0629",
        ]
        ar_date = f"{get_arabic_day(now.weekday())} {hijri.day} {hijri_months[hijri.month - 1]} {hijri.year} \u0647\u0640"

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Weekly News</title>
    {CSS_STYLE}
</head>
<body>
    <div class="container">
        <header class="header-wrapper">
            <div class="header-en">
                <h1>Weekly News</h1>
                <p>Issue {html_lib.escape(str(issue_num))} | {html_lib.escape(en_date)}</p>
            </div>
            <div class="header-ar">
                <h1>\u0623\u062e\u0628\u0627\u0631 \u0627\u0644\u0623\u0633\u0628\u0648\u0639</h1>
                <p>\u0627\u0644\u0639\u062f\u062f {html_lib.escape(str(issue_num))} | {html_lib.escape(ar_date)}</p>
            </div>
        </header>
        <main class="news-grid">{cards_html}</main>
        <div class="footer-wrapper">
            <span>AI Financial News</span>
            <span class="footer-text">\u062f\u0644\u064a\u0644\u0643 \u0644\u0642\u0631\u0627\u0631 \u0648\u0627\u062b\u0642</span>
        </div>
    </div>
</body>
</html>
"""

    with open(output_filename, "w", encoding="utf-8") as handle:
        handle.write(html_content)

    logger.info(f"Successfully generated HTML report: {os.path.abspath(output_filename)}")
    return os.path.abspath(output_filename)


def generate_email_report(news_items: list, output_filename: str = "weekly_news_email.html", issue_num: str = "1"):
    return generate_report(news_items, output_filename=output_filename, issue_num=issue_num)
