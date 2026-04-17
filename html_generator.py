import base64
import html as html_lib
import os
import re
import sys
import urllib.parse
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import requests
from hijri_converter import Gregorian

from pipeline_utils import request_with_retry
from utils import setup_logger

logger = setup_logger(__name__)

FALLBACK_IMAGE_URL = "https://images.unsplash.com/photo-1542744173-8e7e53415bb0?q=80&w=1200&auto=format&fit=crop"
_IMAGE_DATA_CACHE = {}
_MIN_BRAND_ASSET_BYTES = 64
_PROJECT_DIR = Path(__file__).resolve().parent
_DEFAULT_LOGO_CANDIDATES = [
    os.getenv("NEWS_AI_LOGO_PATH", ""),
    str(_PROJECT_DIR / "assets" / "logo.png"),
    str(_PROJECT_DIR / "assets" / "simah_logo.png"),
]
_DEFAULT_FOOTER_CANDIDATES = [
    os.getenv("NEWS_AI_FOOTER_PATH", ""),
    str(_PROJECT_DIR / "assets" / "footer.png"),
    str(_PROJECT_DIR / "assets" / "simah_footer.png"),
]


def _guess_mime_type(path: str) -> str:
    ext = Path(path).suffix.lower()
    if ext == ".png":
        return "image/png"
    if ext == ".svg":
        return "image/svg+xml"
    if ext == ".webp":
        return "image/webp"
    return "image/jpeg"


def _file_to_data_uri(path: str) -> str:
    with open(path, "rb") as handle:
        encoded = base64.b64encode(handle.read()).decode("utf-8")
    return f"data:{_guess_mime_type(path)};base64,{encoded}"


def _is_brand_asset_visually_valid(asset_path: Path) -> bool:
    suffix = asset_path.suffix.lower()
    if suffix == ".svg":
        return asset_path.stat().st_size >= 120

    try:
        from PIL import Image
    except Exception:
        return True

    try:
        with Image.open(asset_path) as image_obj:
            image = image_obj.convert("RGBA")
            image.thumbnail((128, 128))
            pixels = list(image.getdata())
    except Exception as exc:
        logger.warning(f"Failed to inspect brand asset '{asset_path}': {exc}")
        return False

    if not pixels:
        return False

    opaque_pixels = [pixel for pixel in pixels if pixel[3] > 8]
    if not opaque_pixels:
        return False

    total_pixels = len(pixels)
    opaque_count = len(opaque_pixels)
    opaque_coverage = opaque_count / total_pixels

    white_pixels = 0
    quantized_colors = set()
    for red, green, blue, _ in opaque_pixels:
        if red >= 248 and green >= 248 and blue >= 248:
            white_pixels += 1
        quantized_colors.add((red // 32, green // 32, blue // 32))

    white_ratio = white_pixels / opaque_count

    # Reject fully white placeholder images but still allow transparent white logos.
    if white_ratio >= 0.98 and opaque_coverage >= 0.92:
        return False

    # Reject flat single-color placeholders that fill the full frame.
    if len(quantized_colors) <= 1 and opaque_coverage >= 0.95:
        return False

    return True


def resolve_brand_asset_data_uri(
    explicit_path: str | None,
    candidate_paths: list[str],
    min_size_bytes: int = _MIN_BRAND_ASSET_BYTES,
) -> str:
    candidates = []
    if explicit_path:
        candidates.append(explicit_path)
    candidates.extend(candidate_paths)

    for candidate in candidates:
        candidate_path = str(candidate or "").strip()
        if not candidate_path:
            continue
        try:
            asset_path = Path(candidate_path).expanduser().resolve()
        except Exception:
            continue
        if not asset_path.is_file():
            continue
        if asset_path.stat().st_size < min_size_bytes:
            continue
        if not _is_brand_asset_visually_valid(asset_path):
            logger.warning(f"Skipped visually empty brand asset '{asset_path}'")
            continue
        try:
            return _file_to_data_uri(str(asset_path))
        except Exception as exc:
            logger.warning(f"Failed to encode brand asset '{asset_path}': {exc}")
    return ""


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
        response = request_with_retry(
            "GET",
            image_url,
            timeout=10,
            headers={
                "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
                "Referer": image_url,
            },
        )
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
    @import url('https://fonts.googleapis.com/css2?family=Source+Sans+3:wght@400;600;700;800&family=Tajawal:wght@400;500;700;800&display=swap');

    :root {
        --page-bg-top: #0a3458;
        --page-bg-bottom: #114a77;
        --panel-bg: #1d4f79;
        --card-bg: #1f5b88;
        --card-border: #2f6f99;
        --text-primary: #e9f3fb;
        --text-muted: #9bb7cd;
        --accent: #14d1de;
        --accent-soft: #6fe8ef;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }

    body {
        font-family: 'Source Sans 3', 'Tajawal', sans-serif;
        background: linear-gradient(180deg, var(--page-bg-top) 0%, var(--page-bg-bottom) 100%);
        color: var(--text-primary);
        padding: 1.1rem 0.65rem;
        line-height: 1.55;
    }

    .container {
        max-width: 980px;
        margin: 0 auto;
        border: 1px solid #2d6993;
        background-color: var(--panel-bg);
        padding: 1rem;
    }

    .masthead {
        margin-bottom: 1rem;
    }

    .brand-row {
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        gap: 1rem;
        margin-bottom: 0.95rem;
    }

    .brand-unit {
        line-height: 1.15;
        padding-top: 0.12rem;
    }

    .brand-unit-ar {
        font-family: 'Tajawal', sans-serif;
        font-size: 0.8rem;
        font-weight: 700;
        color: #f0f7ff;
        margin-bottom: 0.18rem;
    }

    .brand-unit-en {
        font-size: 0.64rem;
        color: #d3e2ef;
    }

    .brand-logo {
        min-width: 196px;
        display: flex;
        justify-content: flex-end;
        align-items: center;
    }

    .brand-logo-image {
        max-height: 64px;
        width: auto;
        object-fit: contain;
        display: block;
    }

    .brand-logo-fallback {
        display: flex;
        align-items: center;
        gap: 0.38rem;
        color: #ffffff;
        font-weight: 700;
        font-size: 1.15rem;
        letter-spacing: 0.02em;
    }

    .brand-logo-mark {
        font-size: 2rem;
        line-height: 1;
        transform: rotate(16deg);
        opacity: 0.9;
    }

    .titles-row {
        display: flex;
        justify-content: space-between;
        align-items: flex-end;
        gap: 0.8rem;
    }

    .header-en {
        text-align: left;
        direction: ltr;
    }

    .header-ar {
        text-align: right;
        direction: rtl;
        font-family: 'Tajawal', sans-serif;
    }

    .header-en h1,
    .header-ar h1 {
        font-size: 2rem;
        color: var(--accent);
        font-weight: 800;
        margin-bottom: 0.1rem;
        line-height: 1.08;
    }

    .header-en p,
    .header-ar p {
        font-size: 0.76rem;
        font-weight: 600;
        color: var(--text-muted);
    }

    .news-grid {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 0.95rem;
    }

    .card {
        background-color: var(--card-bg);
        border: 1px solid var(--card-border);
        display: flex;
        flex-direction: column;
        overflow: hidden;
        min-height: 100%;
    }

    .full-width {
        grid-column: 1 / -1;
        display: grid;
        grid-template-columns: 1.12fr 0.88fr;
    }

    .card-content {
        padding: 0.95rem 0.95rem 0.9rem 0.95rem;
        display: flex;
        flex-direction: column;
        gap: 0.5rem;
    }

    .card-title {
        color: var(--accent);
        font-size: 1.05rem;
        font-weight: 700;
        line-height: 1.45;
        font-family: 'Tajawal', 'Source Sans 3', sans-serif;
    }

    .card-source {
        font-size: 0.74rem;
        color: var(--text-muted);
        font-weight: 600;
    }

    .card-summary {
        display: grid;
        gap: 0.22rem;
        color: var(--text-primary);
    }

    .card-summary-line {
        margin: 0;
        line-height: 1.6;
        font-size: 0.92rem;
    }

    .card-img {
        width: 100%;
        height: 100%;
        min-height: 205px;
        object-fit: cover;
        border-left: 1px solid var(--card-border);
        background-color: #1a4d73;
    }

    .full-width > .card-img:first-child {
        border-left: 0;
        border-right: 1px solid var(--card-border);
    }

    .read-more {
        font-size: 0.82rem;
        color: var(--accent-soft);
        text-decoration: underline;
        display: inline-block;
        margin-top: auto;
        font-weight: 700;
    }

    .footer-wrapper {
        margin-top: 1rem;
        border: 1px dashed rgba(216, 230, 242, 0.78);
        border-radius: 15px;
        padding: 0.55rem 0.9rem;
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 0.9rem;
        min-height: 56px;
        background: rgba(4, 16, 29, 0.78);
        box-shadow: inset 0 0 0 1px rgba(120, 149, 173, 0.45);
    }

    .footer-icons {
        display: flex;
        align-items: center;
        gap: 0.42rem;
    }

    .footer-icon {
        width: 48px;
        height: 34px;
        border: 1px solid rgba(225, 237, 247, 0.88);
        border-radius: 4px;
        position: relative;
        display: inline-block;
        background-color: rgba(255, 255, 255, 0.01);
    }

    .footer-icon-slash-left::before,
    .footer-icon-slash-right::before {
        content: "";
        position: absolute;
        left: 8px;
        width: 27px;
        height: 1.8px;
        border-radius: 99px;
        background-color: rgba(229, 240, 248, 0.95);
    }

    .footer-icon-slash-left::before {
        top: 24px;
        transform: rotate(-34deg);
    }

    .footer-icon-slash-right::before {
        top: 23px;
        left: 9px;
        transform: rotate(-30deg);
    }

    .footer-icon-nodes {
        background:
            radial-gradient(circle at 12px 24px, rgba(229, 240, 248, 0.98) 0 3.5px, transparent 3.7px),
            radial-gradient(circle at 22px 17px, rgba(229, 240, 248, 0.98) 0 3.5px, transparent 3.7px),
            radial-gradient(circle at 31px 10px, rgba(229, 240, 248, 0.98) 0 3.5px, transparent 3.7px),
            linear-gradient(-33deg, transparent 42%, rgba(229, 240, 248, 0.96) 42% 46%, transparent 46% 100%);
        background-color: rgba(255, 255, 255, 0.01);
        background-repeat: no-repeat;
    }

    .footer-text {
        color: #f3f8fd;
        font-family: 'Tajawal', sans-serif;
        font-weight: 700;
        font-size: 1.34rem;
        line-height: 1;
        letter-spacing: 0.01em;
    }

    .footer-banner-image {
        display: block;
        width: 100%;
        height: auto;
        border-radius: 12px;
    }

    .footer-wrapper-image {
        padding: 0;
        border: 0;
        background: transparent;
        box-shadow: none;
        min-height: 0;
    }

    @media (max-width: 880px) {
        .container {
            padding: 0.85rem;
        }

        .brand-row {
            margin-bottom: 0.75rem;
        }

        .brand-logo {
            min-width: 120px;
        }

        .brand-logo-image {
            max-height: 50px;
        }

        .header-en h1,
        .header-ar h1 {
            font-size: 1.62rem;
        }

        .news-grid {
            grid-template-columns: 1fr;
        }

        .full-width {
            grid-template-columns: 1fr;
        }

        .card-img {
            min-height: 190px;
            border-left: 0;
            border-top: 1px solid var(--card-border);
        }

        .full-width > .card-img:first-child {
            border-right: 0;
            border-bottom: 1px solid var(--card-border);
            border-top: 0;
        }

        .footer-wrapper {
            min-height: 46px;
            border-radius: 12px;
            padding: 0.46rem 0.64rem;
        }

        .footer-icon {
            width: 38px;
            height: 27px;
        }

        .footer-text {
            font-size: 0.98rem;
        }

        .footer-banner-image {
            border-radius: 10px;
        }
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
    img_src = html_lib.escape(generate_base64_ai_image(news))
    summary_html = render_summary(news.get("summary", []))
    source_html = f'<div class="card-source">{source}</div>' if source else ""

    is_arabic = any("\u0600" <= char <= "\u06FF" for char in str(news.get("title", "")))
    direction = "rtl" if is_arabic else "ltr"
    read_more_text = "\u0644\u0644\u0645\u0632\u064a\u062f" if is_arabic else "Read More"

    img_html = f'<img src="{img_src}" alt="" class="card-img" />'
    text_html = (
        f'<div class="card-content" dir="{direction}">'
        f'<h3 class="card-title">{title}</h3>'
        f"{source_html}"
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
    logo_path: str | None = None,
    footer_path: str | None = None,
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

    logo_data_uri = html_lib.escape(resolve_brand_asset_data_uri(logo_path, _DEFAULT_LOGO_CANDIDATES))
    footer_data_uri = html_lib.escape(resolve_brand_asset_data_uri(footer_path, _DEFAULT_FOOTER_CANDIDATES))
    if logo_data_uri:
        logo_html = f'<img src="{logo_data_uri}" alt="SIMAH Logo" class="brand-logo-image" />'
    else:
        logo_html = (
            '<div class="brand-logo-fallback" dir="rtl">'
            '<span class="brand-logo-mark">*</span>'
            '<span>\u0633\u0645\u0629&nbsp;SIMAH</span>'
            "</div>"
        )

    footer_wrapper_class = "footer-wrapper"
    if footer_data_uri:
        footer_wrapper_class = "footer-wrapper footer-wrapper-image"
        footer_content = f'<img src="{footer_data_uri}" alt="" class="footer-banner-image" />'
    else:
        footer_content = (
            '<div class="footer-icons" aria-hidden="true">'
            '<span class="footer-icon footer-icon-slash-left"></span>'
            '<span class="footer-icon footer-icon-nodes"></span>'
            '<span class="footer-icon footer-icon-slash-right"></span>'
            "</div>"
            '<span class="footer-text">\u062f\u0644\u064a\u0644\u0643 \u0644\u0642\u0631\u0627\u0631 \u0648\u0627\u062b\u0642</span>'
        )

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
        <header class="masthead">
            <div class="brand-row">
                <div class="brand-unit">
                    <div class="brand-unit-ar">\u0627\u0644\u062a\u0648\u0627\u0635\u0644 \u0627\u0644\u0645\u0624\u0633\u0633\u064a</div>
                    <div class="brand-unit-en">Corporate Communications</div>
                </div>
                <div class="brand-logo">{logo_html}</div>
            </div>
            <div class="titles-row">
                <div class="header-en">
                    <h1>Weekly News</h1>
                    <p>Issue {html_lib.escape(str(issue_num))} | {html_lib.escape(en_date)}</p>
                </div>
                <div class="header-ar">
                    <h1>\u0623\u062e\u0628\u0627\u0631 \u0627\u0644\u0623\u0633\u0628\u0648\u0639</h1>
                    <p>\u0627\u0644\u0639\u062f\u062f {html_lib.escape(str(issue_num))} | {html_lib.escape(ar_date)}</p>
                </div>
            </div>
        </header>
        <main class="news-grid">{cards_html}</main>
        <footer class="{footer_wrapper_class}">{footer_content}</footer>
    </div>
</body>
</html>
"""

    with open(output_filename, "w", encoding="utf-8") as handle:
        handle.write(html_content)

    logger.info(f"Successfully generated HTML report: {os.path.abspath(output_filename)}")
    return os.path.abspath(output_filename)


def _configure_playwright_event_loop() -> None:
    try:
        import asyncio

        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    except Exception as exc:
        logger.debug(f"Failed to configure Playwright event loop policy for export: {exc}")


def export_report_assets(
    news_items: list,
    html_output: str = "weekly_news_interactive.html",
    pdf_output: str | None = "weekly_news_interactive.pdf",
    image_output: str | None = "weekly_news_interactive.jpg",
    issue_num: str = "1",
    custom_ar_date: str | None = None,
    custom_en_date: str | None = None,
    logo_path: str | None = None,
    footer_path: str | None = None,
) -> dict:
    html_path = os.path.abspath(html_output)
    pdf_path = os.path.abspath(pdf_output) if pdf_output else ""
    image_path = os.path.abspath(image_output) if image_output else ""

    generate_report(
        news_items,
        html_path,
        issue_num=issue_num,
        custom_ar_date=custom_ar_date,
        custom_en_date=custom_en_date,
        logo_path=logo_path,
        footer_path=footer_path,
    )

    result = {
        "html_path": html_path,
        "pdf_path": pdf_path,
        "image_path": image_path,
        "pdf_success": False,
        "image_success": False,
        "export_error": "",
    }

    if not pdf_path and not image_path:
        return result

    try:
        from playwright.sync_api import sync_playwright

        _configure_playwright_event_loop()
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1000, "height": 1200})
            page.goto(Path(html_path).resolve().as_uri(), wait_until="networkidle")
            page.emulate_media(media="screen")
            page.wait_for_timeout(600)
            content_height = int(page.evaluate("() => document.documentElement.scrollHeight || document.body.scrollHeight || 1200")) + 40

            if pdf_path:
                page.pdf(
                    path=pdf_path,
                    width="1000px",
                    height=f"{content_height}px",
                    print_background=True,
                    page_ranges="1",
                    margin={"top": "0", "right": "0", "bottom": "0", "left": "0"},
                )
                result["pdf_success"] = os.path.exists(pdf_path)

            if image_path:
                page.screenshot(path=image_path, full_page=True)
                result["image_success"] = os.path.exists(image_path)

            browser.close()
    except Exception as exc:
        logger.warning(f"Playwright asset export failed: {exc}")
        result["export_error"] = str(exc)

    return result


def generate_email_report(news_items: list, output_filename: str = "weekly_news_email.html", issue_num: str = "1"):
    return generate_report(news_items, output_filename=output_filename, issue_num=issue_num)
