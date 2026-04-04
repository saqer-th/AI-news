import html
import os
from datetime import datetime

import streamlit as st

from html_generator import get_news_image_url, generate_report
from llm_processor import process_all_news
from news_fetcher import fetch_rss_news

CARD_COLUMNS = 3
RAW_NEWS_LIMIT = 20
RAW_NEWS_DAYS_BACK = 7

st.set_page_config(page_title="AI Financial News", layout="wide", page_icon="\U0001f4f0")

st.markdown(
    """
    <style>
        .stApp {
            background: linear-gradient(180deg, #f4f8fb 0%, #edf5fb 100%);
        }
        .hero-shell {
            background: linear-gradient(135deg, #0f5c7a 0%, #159d9a 100%);
            color: white;
            padding: 1.4rem 1.6rem;
            border-radius: 24px;
            margin-bottom: 1.25rem;
            box-shadow: 0 18px 50px rgba(15, 92, 122, 0.18);
        }
        .hero-shell h1 {
            margin: 0 0 0.4rem 0;
            font-size: 2rem;
        }
        .hero-shell p {
            margin: 0;
            opacity: 0.92;
            font-size: 1rem;
        }
        .card-shell {
            background: #ffffff;
            border: 1px solid #d9e8f3;
            border-radius: 22px;
            overflow: hidden;
            box-shadow: 0 12px 28px rgba(23, 61, 89, 0.08);
            margin-bottom: 0.85rem;
        }
        .card-shell.high-score {
            border: 2px solid #0ea5a6;
            box-shadow: 0 18px 36px rgba(14, 165, 166, 0.16);
        }
        .card-shell img {
            width: 100%;
            height: 180px;
            object-fit: cover;
            display: block;
            background: #eef4f8;
        }
        .card-content {
            padding: 1rem 1rem 0.4rem 1rem;
        }
        .card-title {
            color: #12344d;
            font-size: 1rem;
            font-weight: 700;
            line-height: 1.5;
            margin-bottom: 0.6rem;
            min-height: 3rem;
        }
        .card-meta {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 0.5rem;
            color: #5c7388;
            font-size: 0.83rem;
            margin-bottom: 0.7rem;
        }
        .score-badge {
            display: inline-block;
            background: #e7f8f6;
            color: #0b7f80;
            padding: 0.25rem 0.65rem;
            border-radius: 999px;
            font-weight: 700;
        }
        .card-desc {
            color: #496173;
            font-size: 0.9rem;
            line-height: 1.55;
            min-height: 4.8rem;
            margin-bottom: 0.9rem;
        }
        .selection-bar {
            background: rgba(255, 255, 255, 0.92);
            border: 1px solid #d9e8f3;
            border-radius: 18px;
            padding: 0.9rem 1rem;
            margin-bottom: 1rem;
        }
        .section-shell {
            background: rgba(255, 255, 255, 0.92);
            border: 1px solid #d9e8f3;
            border-radius: 20px;
            padding: 1rem 1.1rem;
            margin-bottom: 1rem;
            box-shadow: 0 12px 24px rgba(23, 61, 89, 0.05);
        }
        .section-shell h3 {
            color: #12344d;
            font-size: 1.05rem;
            margin: 0 0 0.35rem 0;
        }
        .section-shell p {
            color: #5c7388;
            margin: 0;
            font-size: 0.92rem;
        }
        .card-link {
            display: inline-block;
            margin-top: 0.2rem;
            color: #0f5c7a;
            font-size: 0.9rem;
            font-weight: 700;
            text-decoration: none;
        }
        .card-link:hover {
            color: #0ea5a6;
            text-decoration: underline;
        }
        .review-head {
            background: linear-gradient(135deg, #ffffff 0%, #f6fbff 100%);
            border: 1px solid #d9e8f3;
            border-radius: 18px;
            padding: 0.95rem 1rem;
            margin-bottom: 0.85rem;
        }
        .review-head h4 {
            color: #12344d;
            font-size: 1rem;
            line-height: 1.6;
            margin: 0 0 0.4rem 0;
        }
        .review-head p {
            color: #5c7388;
            font-size: 0.84rem;
            margin: 0;
        }
        .review-link {
            display: inline-block;
            margin-top: 0.65rem;
            background: #eef7fb;
            color: #0f5c7a;
            border: 1px solid #d7e4ef;
            border-radius: 999px;
            padding: 0.45rem 0.85rem;
            text-decoration: none;
            font-size: 0.88rem;
            font-weight: 700;
        }
        .review-link:hover {
            background: #e3f4f2;
            color: #0b7f80;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="hero-shell">
        <h1>AI Financial News Hub</h1>
        <p>Review high-priority Saudi financial news, select the strongest items, and generate a polished report draft.</p>
    </div>
    """,
    unsafe_allow_html=True,
)


def load_news() -> list:
    with st.spinner("Fetching, filtering, and ranking news from approved domains..."):
        return fetch_rss_news(limit=RAW_NEWS_LIMIT, days_back=RAW_NEWS_DAYS_BACK)


def reset_selection_flags(total_items: int, value: bool) -> None:
    for idx in range(total_items):
        st.session_state[f"chk_{idx}"] = value


def format_card_date(value: str) -> str:
    if not value:
        return ""

    try:
        normalized = value.replace("Z", "+00:00")
        if "T" in normalized:
            dt = datetime.fromisoformat(normalized)
        else:
            dt = datetime.strptime(normalized[:25], "%a, %d %b %Y %H:%M:%S")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return str(value)[:20]


if "raw_news" not in st.session_state:
    st.session_state.raw_news = load_news()
    st.session_state.raw_news_days_back = RAW_NEWS_DAYS_BACK

if st.session_state.get("raw_news_days_back") != RAW_NEWS_DAYS_BACK:
    st.session_state.raw_news = load_news()
    st.session_state.raw_news_days_back = RAW_NEWS_DAYS_BACK

if "processed_news" not in st.session_state:
    st.session_state.processed_news = None

raw_news = st.session_state.raw_news
for idx in range(len(raw_news)):
    st.session_state.setdefault(f"chk_{idx}", False)

if st.session_state.processed_news is None:
    selected_count = sum(1 for idx in range(len(raw_news)) if st.session_state.get(f"chk_{idx}", False))

    control_a, control_b, control_c, control_d = st.columns([1, 1, 1, 1.2])
    with control_a:
        if st.button("Select All", use_container_width=True) and raw_news:
            reset_selection_flags(len(raw_news), True)
            st.rerun()
    with control_b:
        if st.button("Clear Selection", use_container_width=True) and raw_news:
            reset_selection_flags(len(raw_news), False)
            st.rerun()
    with control_c:
        if st.button("Refresh Feed", use_container_width=True):
            st.session_state.raw_news = load_news()
            st.session_state.processed_news = None
            st.rerun()
    with control_d:
        st.metric("Selected News", selected_count)

    st.markdown(
        """
        <div class="section-shell">
            <h3>مرحلة اختيار الأخبار</h3>
            <p>اختر الأخبار الأنسب من البطاقات التالية. يمكنك زيارة المصدر الأصلي مباشرة قبل التحديد.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if not raw_news:
        st.warning("No news retrieved. Check network connectivity or approved source coverage.")
    else:
        for start in range(0, len(raw_news), CARD_COLUMNS):
            columns = st.columns(CARD_COLUMNS)
            for offset in range(CARD_COLUMNS):
                idx = start + offset
                if idx >= len(raw_news):
                    continue

                item = raw_news[idx]
                title = html.escape(str(item.get("title", "")))
                source = html.escape(str(item.get("source", "")))
                description = html.escape(str(item.get("description", ""))[:180])
                score = item.get("rss_score", item.get("score", 0))
                published_date = format_card_date(item.get("published", ""))
                image_url = get_news_image_url(item, prefer_original=True)
                card_class = "card-shell high-score" if score >= 100 else "card-shell"

                with columns[offset]:
                    st.markdown(
                        f"""
                        <div class="{card_class}">
                            <img src="{html.escape(image_url)}" alt="" />
                            <div class="card-content">
                                <div class="card-meta">
                                    <span>{source}</span>
                                    <span class="score-badge">RSS {score}</span>
                                </div>
                                <div class="card-meta"><span>التاريخ: {html.escape(published_date)}</span></div>
                                <div class="card-title">{title}</div>
                                <div class="card-desc">{description}...</div>
                                <a class="card-link" href="{html.escape(item.get('link', '#'))}" target="_blank" rel="noopener noreferrer">زيارة الخبر الأصلي</a>
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
                    st.checkbox("Select", key=f"chk_{idx}")

    selected_news = [item for idx, item in enumerate(raw_news) if st.session_state.get(f"chk_{idx}", False)]

    if st.button("Generate Report", type="primary", use_container_width=True):
        if not selected_news:
            st.error("Select at least one article before generating the report.")
        else:
            with st.spinner("Resolving articles, extracting content, and generating structured summaries..."):
                processed_news = process_all_news(selected_news)
                processed_news.sort(key=lambda item: item.get("final_score", 0), reverse=True)
                if not processed_news:
                    st.error("Processing failed. Confirm Ollama is running and the sources are reachable.")
                else:
                    st.session_state.processed_news = processed_news
                    st.rerun()

else:
    st.markdown(
        """
        <div class="section-shell">
            <h3>مرحلة المراجعة والتحرير</h3>
            <p>راجع النصوص والصور والملخصات، ثم افتح الخبر الأصلي عند الحاجة قبل التصدير النهائي.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    top_controls = st.columns([1, 1, 1.5])
    with top_controls[0]:
        if st.button("Back to Selection", use_container_width=True):
            st.session_state.processed_news = None
            st.rerun()
    with top_controls[1]:
        st.metric("Selected Items", len(st.session_state.processed_news))
    with top_controls[2]:
        highest_score = max((item.get("final_score", 0) for item in st.session_state.processed_news), default=0)
        st.metric("Top Final Score", highest_score)

    st.markdown("حرر المحتوى قبل التصدير. الأخبار غير الموثقة تحتاج مراجعة بشرية قبل الاعتماد.")

    header_col_a, header_col_b, header_col_c = st.columns(3)
    with header_col_a:
        issue_num_input = st.text_input("Issue Number", value="1")
    with header_col_b:
        custom_ar_date = st.text_input("Custom Arabic Date (Optional)")
    with header_col_c:
        custom_en_date = st.text_input("Custom English Date (Optional)")

    edited_news_pool = []
    for start in range(0, len(st.session_state.processed_news), 2):
        columns = st.columns(2)
        for offset in range(2):
            idx = start + offset
            if idx >= len(st.session_state.processed_news):
                continue

            item = st.session_state.processed_news[idx]
            image_choice_default = 0 if item.get("original_image_url") else 1
            article_date = format_card_date(item.get("article_date") or item.get("published", ""))

            with columns[offset]:
                with st.container(border=True):
                    st.markdown(
                        f"""
                        <div class="review-head">
                            <h4>{html.escape(str(item.get("title", "")))}</h4>
                            <p>
                                المصدر: {html.escape(str(item.get("source", "Unknown")))}
                                | RSS: {item.get("rss_score", 0)}
                                | AI: {item.get("importance", 0)}
                                | النهائي: {item.get("final_score", 0)}
                                | التاريخ: {html.escape(article_date)}
                            </p>
                            <a class="review-link" href="{html.escape(item.get('link', '#'))}" target="_blank" rel="noopener noreferrer">فتح الخبر الأصلي</a>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
                    if item.get("verification_status") == "verified":
                        st.success("Structured JSON verified.")
                    else:
                        st.warning("Unverified fallback values were used. Review carefully.")

                    preview_image = get_news_image_url(item, prefer_original=True)
                    st.image(preview_image, use_container_width=True)

                    remove_toggle = st.checkbox(f"Remove News {idx + 1}", key=f"rm_{idx}")
                    title_val = st.text_input("Title", value=item.get("title", ""), key=f"title_{idx}")

                    summary_items = item.get("summary", [])
                    if not isinstance(summary_items, list):
                        summary_items = [str(summary_items)]
                    summary_first = summary_items[0] if len(summary_items) > 0 else ""
                    summary_second = summary_items[1] if len(summary_items) > 1 else ""
                    summary_third = summary_items[2] if len(summary_items) > 2 else ""

                    summary_one = st.text_area("Summary Point 1", value=summary_first, height=90, key=f"sum1_{idx}")
                    summary_two = st.text_area("Summary Point 2", value=summary_second, height=90, key=f"sum2_{idx}")
                    summary_three = st.text_area("Summary Point 3", value=summary_third, height=90, key=f"sum3_{idx}")

                    use_original = False
                    original_image_url = item.get("original_image_url", "")
                    image_keyword = item.get("image_keyword", "")

                    if original_image_url:
                        img_choice = st.radio(
                            "مصدر الصورة",
                            ["اعتمد الصورة الأصلية", "استخدم كلمة مفتاحية للصورة"],
                            index=image_choice_default,
                            key=f"imgsrc_{idx}",
                        )
                        use_original = img_choice == "اعتمد الصورة الأصلية"
                    else:
                        st.info("No original article image was found. A keyword-based image will be used.")

                    if not use_original:
                        image_keyword = st.text_input("Image Keyword", value=image_keyword, key=f"imgkw_{idx}")
                        st.image(get_news_image_url({"image_keyword": image_keyword, "title": title_val}), use_container_width=True)

                    with st.expander("View Extracted Article Text"):
                        st.text(item.get("original_text", "No extracted text available."))

                    if not remove_toggle:
                        updated_item = {
                            "title": title_val,
                            "summary": [summary_one.strip(), summary_two.strip(), summary_three.strip()],
                            "summary_3_lines": "\n".join(
                                [summary_one.strip(), summary_two.strip(), summary_three.strip()]
                            ).strip(),
                            "image_keyword": image_keyword,
                            "link": item.get("link", ""),
                            "source": item.get("source", ""),
                            "rss_score": item.get("rss_score", 0),
                            "importance": item.get("importance", 0),
                            "final_score": item.get("final_score", 0),
                            "original_image_url": original_image_url,
                            "use_original_image": use_original,
                            "selected_image_source": "original" if use_original else "keyword",
                        }
                        edited_news_pool.append(updated_item)

    st.markdown("---")

    if st.button("Finalize and Export", type="primary", use_container_width=True):
        if not edited_news_pool:
            st.error("Keep at least one article before export.")
        else:
            with st.status("Generating final outputs...", expanded=True) as status:
                base_dir = os.path.dirname(os.path.abspath(__file__))
                html_path = os.path.join(base_dir, "weekly_news_interactive.html")
                pdf_path = os.path.join(base_dir, "weekly_news_interactive.pdf")
                img_path = os.path.join(base_dir, "weekly_news_interactive.jpg")

                edited_news_pool.sort(key=lambda item: item.get("final_score", 0), reverse=True)
                generate_report(
                    edited_news_pool,
                    html_path,
                    issue_num=issue_num_input,
                    custom_ar_date=custom_ar_date or None,
                    custom_en_date=custom_en_date or None,
                )
                st.write("HTML report generated.")

                from playwright.sync_api import sync_playwright

                pdf_success = False
                img_success = False
                try:
                    with sync_playwright() as playwright:
                        browser = playwright.chromium.launch(headless=True)
                        page = browser.new_page()
                        page.set_viewport_size({"width": 1000, "height": 1200})
                        page.goto(f"file://{html_path}", wait_until="networkidle")
                        page.emulate_media(media="screen")
                        content_height = page.evaluate("() => document.documentElement.scrollHeight") + 40
                        page.pdf(
                            path=pdf_path,
                            width="1000px",
                            height=f"{content_height}px",
                            print_background=True,
                            page_ranges="1",
                            margin={"top": "0", "right": "0", "bottom": "0", "left": "0"},
                        )
                        pdf_success = True
                        page.screenshot(path=img_path, full_page=True)
                        img_success = True
                        browser.close()
                except Exception as exc:
                    st.warning(f"Playwright export failed: {exc}")

                status.update(label="Report generation complete.", state="complete")

            download_cols = st.columns(3)
            with download_cols[0]:
                with open(html_path, "rb") as handle:
                    st.download_button("Download HTML", handle, file_name="report.html", use_container_width=True)
            with download_cols[1]:
                if pdf_success and os.path.exists(pdf_path):
                    with open(pdf_path, "rb") as handle:
                        st.download_button("Download PDF", handle, file_name="report.pdf", use_container_width=True)
            with download_cols[2]:
                if img_success and os.path.exists(img_path):
                    with open(img_path, "rb") as handle:
                        st.download_button(
                            "Download Image",
                            handle,
                            file_name="report.jpg",
                            mime="image/jpeg",
                            use_container_width=True,
                        )
