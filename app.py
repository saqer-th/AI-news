import html
import json
import os
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import streamlit as st

from html_generator import export_report_assets, get_news_image_url
CARD_COLUMNS = 5
LATEST_CACHE_PATH = Path("cache") / "latest.json"

st.set_page_config(page_title="AI Financial News", layout="wide", page_icon="\U0001f4f0")

if "session_token" not in st.session_state:
    st.session_state.session_token = uuid4().hex[:10]

st.markdown(
    """
    <link href="https://fonts.googleapis.com/css2?family=Cairo:wght@400;600;700;900&family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        * { font-family: 'Cairo', 'Inter', sans-serif; }

        .stApp {
            background: #0b1220;
        }

        /* â”€â”€ Hero â”€â”€ */
        .hero-shell {
            position: relative;
            overflow: hidden;
            background: linear-gradient(135deg, #0d1f3c 0%, #0f3d5c 45%, #0a7a78 100%);
            color: white;
            padding: 2.2rem 2rem 2rem 2rem;
            border-radius: 28px;
            margin-bottom: 1.5rem;
            box-shadow: 0 24px 64px rgba(10, 122, 120, 0.35), 0 0 0 1px rgba(255,255,255,0.06);
        }
        .hero-shell::before {
            content: '';
            position: absolute;
            top: -60px; right: -80px;
            width: 320px; height: 320px;
            background: radial-gradient(circle, rgba(14,165,166,0.25) 0%, transparent 70%);
            animation: pulse-orb 4s ease-in-out infinite alternate;
        }
        .hero-shell::after {
            content: '';
            position: absolute;
            bottom: -40px; left: -60px;
            width: 240px; height: 240px;
            background: radial-gradient(circle, rgba(99,102,241,0.18) 0%, transparent 70%);
            animation: pulse-orb 5s ease-in-out infinite alternate-reverse;
        }
        @keyframes pulse-orb {
            from { transform: scale(1); opacity: 0.6; }
            to   { transform: scale(1.15); opacity: 1; }
        }
        .hero-inner { position: relative; z-index: 2; }
        .hero-badge {
            display: inline-flex; align-items: center; gap: 0.4rem;
            background: rgba(14,165,166,0.2);
            border: 1px solid rgba(14,165,166,0.4);
            color: #5eead4;
            padding: 0.3rem 0.9rem;
            border-radius: 999px;
            font-size: 0.78rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            margin-bottom: 0.8rem;
        }
        .hero-badge::before { content: '\25CF'; font-size: 0.6rem; animation: blink 1.4s step-end infinite; }
        @keyframes blink { 50% { opacity: 0; } }
        .hero-shell h1 {
            margin: 0 0 0.5rem 0;
            font-size: 2.4rem;
            font-weight: 900;
            background: linear-gradient(90deg, #ffffff 0%, #a5f3eb 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }
        .hero-shell p {
            margin: 0;
            opacity: 0.82;
            font-size: 1rem;
            font-weight: 400;
            line-height: 1.7;
            max-width: 600px;
        }
        .hero-stats {
            display: flex;
            gap: 2rem;
            margin-top: 1.4rem;
        }
        .hero-stat {
            display: flex; flex-direction: column;
            border-left: 2px solid rgba(94,234,212,0.3);
            padding-left: 1rem;
        }
        .hero-stat-val {
            font-size: 1.6rem;
            font-weight: 900;
            color: #5eead4;
            line-height: 1;
        }
        .hero-stat-lbl {
            font-size: 0.75rem;
            color: rgba(255,255,255,0.6);
            margin-top: 0.2rem;
        }

        /* --- Controls Bar --- */
        div.stButton > button {
            border-radius: 14px !important;
            font-family: 'Cairo', sans-serif !important;
            font-weight: 700 !important;
            transition: all .2s ease !important;
        }
        div.stButton > button:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 20px rgba(14,165,166,0.25) !important;
        }

        /* --- Section Shell --- */
        .section-shell {
            background: rgba(255,255,255,0.04);
            border: 1px solid rgba(255,255,255,0.09);
            border-radius: 20px;
            padding: 1rem 1.3rem;
            margin-bottom: 1.2rem;
            backdrop-filter: blur(12px);
        }
        .section-shell h3 {
            color: #e2f0ff;
            font-size: 1.05rem;
            margin: 0 0 0.3rem 0;
        }
        .section-shell p {
            color: #8fafc8;
            margin: 0;
            font-size: 0.9rem;
        }

        /* --- News Card --- */
        .card-shell {
            background: rgba(16, 28, 50, 0.85);
            border: 1px solid rgba(255,255,255,0.07);
            border-radius: 20px;
            overflow: hidden;
            box-shadow: 0 8px 32px rgba(0,0,0,0.35);
            margin-bottom: 1rem;
            transition: transform .25s ease, box-shadow .25s ease, border-color .25s ease;
        }
        .card-shell:hover {
            transform: translateY(-5px);
            box-shadow: 0 20px 48px rgba(14,165,166,0.22);
            border-color: rgba(14,165,166,0.4);
        }
        .card-shell.high-score {
            border: 1.5px solid rgba(14,165,166,0.55);
            box-shadow: 0 12px 40px rgba(14,165,166,0.2);
        }
        .card-shell.high-score:hover {
            box-shadow: 0 24px 56px rgba(14,165,166,0.35);
        }
        .card-img-wrap {
            position: relative;
            width: 100%;
            height: 150px;
            overflow: hidden;
            background: #0d1b2e;
        }
        .card-img-wrap::after {
            content: '';
            position: absolute;
            bottom: 0; left: 0; right: 0;
            height: 50%;
            background: linear-gradient(transparent, rgba(10,18,36,0.9));
            pointer-events: none;
        }
        .card-shell img {
            width: 100%;
            height: 150px;
            object-fit: cover;
            display: block;
            transition: transform .35s ease;
        }
        .card-shell:hover img { transform: scale(1.05); }
        .card-content {
            padding: 0.85rem 0.9rem 0.6rem 0.9rem;
        }
        .card-meta {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 0.4rem;
            color: #6b8399;
            font-size: 0.75rem;
            margin-bottom: 0.5rem;
        }
        .card-source {
            display: inline-flex; align-items: center; gap: 0.3rem;
            background: rgba(99,102,241,0.12);
            color: #a5b4fc;
            padding: 0.18rem 0.55rem;
            border-radius: 6px;
            font-size: 0.72rem;
            font-weight: 700;
        }
        .score-badge {
            display: inline-block;
            background: rgba(14,165,166,0.15);
            color: #5eead4;
            padding: 0.18rem 0.5rem;
            border-radius: 6px;
            font-weight: 700;
            font-size: 0.72rem;
        }
        .card-title {
            color: #e8f0fb;
            font-size: 0.88rem;
            font-weight: 700;
            line-height: 1.55;
            margin-bottom: 0.45rem;
            min-height: 2.7rem;
            display: -webkit-box;
            -webkit-line-clamp: 3;
            -webkit-box-orient: vertical;
            overflow: hidden;
        }
        .card-desc {
            color: #6b8399;
            font-size: 0.78rem;
            line-height: 1.55;
            margin-bottom: 0.7rem;
            display: -webkit-box;
            -webkit-line-clamp: 2;
            -webkit-box-orient: vertical;
            overflow: hidden;
        }
        .card-link {
            display: inline-flex;
            align-items: center;
            gap: 0.35rem;
            color: #38bdf8;
            font-size: 0.78rem;
            font-weight: 700;
            text-decoration: none;
            transition: color .18s;
        }
        .card-link::after {
        content: ' →';
        margin-left: 6px;
        transition: transform 0.2s ease;
        }

        .card-link:hover::after {
        transform: translateX(4px);
        }

        /* --- Review Page --- */
        .review-head {
            background: rgba(255,255,255,0.04);
            border: 1px solid rgba(255,255,255,0.09);
            border-radius: 16px;
            padding: 0.9rem 1rem;
            margin-bottom: 0.8rem;
        }
        .review-head h4 {
            color: #e2f0ff;
            font-size: 0.95rem;
            line-height: 1.65;
            margin: 0 0 0.35rem 0;
        }
        .review-head p {
            color: #6b8399;
            font-size: 0.8rem;
            margin: 0;
        }
        .review-link {
            display: inline-block;
            margin-top: 0.6rem;
            background: rgba(56,189,248,0.1);
            color: #38bdf8;
            border: 1px solid rgba(56,189,248,0.25);
            border-radius: 999px;
            padding: 0.4rem 0.9rem;
            text-decoration: none;
            font-size: 0.84rem;
            font-weight: 700;
            transition: all .18s;
        }
        .review-link:hover {
            background: rgba(56,189,248,0.18);
            color: #7dd3fc;
        }

        /* â”€â”€ Streamlit overrides â”€â”€ */
        .stMetric label { color: #8fafc8 !important; font-family: 'Cairo', sans-serif !important; }
        .stMetric [data-testid="metric-container"] { background: rgba(255,255,255,0.04); border-radius: 14px; padding: 0.6rem 1rem; border: 1px solid rgba(255,255,255,0.07); }
        div[data-testid="stCheckbox"] label { color: #8fafc8 !important; font-family: 'Cairo', sans-serif !important; }
        .stTextInput input, .stTextArea textarea {
            background: rgba(255,255,255,0.05) !important;
            border-color: rgba(255,255,255,0.1) !important;
            color: #e2f0ff !important;
            border-radius: 12px !important;
            font-family: 'Cairo', sans-serif !important;
        }
        .stRadio label { color: #8fafc8 !important; }
        div[data-testid="stExpander"] { border-color: rgba(255,255,255,0.09) !important; border-radius: 14px !important; }
        .stWarning, .stSuccess, .stError, .stInfo { border-radius: 12px !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

# Hero is rendered after we know how many items to show
_hero_placeholder = st.empty()
_load_status_placeholder = st.empty()


@st.cache_data(show_spinner=False)
def _load_news_cached(cache_path: str, cache_mtime: float) -> list:
    del cache_mtime
    path = Path(cache_path)
    if not path.exists():
        return []
    try:
        payload = path.read_text(encoding="utf-8").strip()
        if not payload:
            return []
        data = json.loads(payload)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _cache_mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except OSError:
        return 0.0


def _last_updated_label(path: Path) -> str:
    mtime = _cache_mtime(path)
    if not mtime:
        return "N/A"
    return datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M")


def _read_cached_news() -> list:
    return _load_news_cached(str(LATEST_CACHE_PATH), _cache_mtime(LATEST_CACHE_PATH))


def load_news(show_progress: bool = False) -> list:
    if not show_progress:
        with st.spinner("Loading cached news feed..."):
            return _read_cached_news()

    progress_shell = _load_status_placeholder.container()
    progress_shell.markdown(
        """
        <div class="section-shell">
            <h3>Feed Progress</h3>
            <p>Loading cached news from local storage.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    progress_bar = progress_shell.progress(5)
    status_line = progress_shell.empty()
    detail_line = progress_shell.empty()

    status_line.markdown("**Reading `cache/latest.json`...**")
    progress_bar.progress(45)
    news_items = _read_cached_news()
    progress_bar.progress(100)

    last_updated = _last_updated_label(LATEST_CACHE_PATH)
    if news_items:
        status_line.success("Cached feed loaded successfully.")
        detail_line.caption(f"Loaded {len(news_items)} cached articles | Last updated {last_updated}")
    else:
        status_line.warning("No cached feed found.")
        detail_line.caption("Run `pipeline_scheduler.py` to generate `cache/latest.json`.")
    return news_items


def clear_selection_state() -> None:
    for key in list(st.session_state.keys()):
        if key.startswith("chk_"):
            del st.session_state[key]


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


if "refresh_requested" not in st.session_state:
    st.session_state.refresh_requested = False

if "processed_news" not in st.session_state:
    st.session_state.processed_news = None

should_reload_news = "raw_news" not in st.session_state

if st.session_state.get("refresh_requested"):
    should_reload_news = True
    st.session_state.refresh_requested = False

if should_reload_news:
    st.session_state.raw_news = load_news(show_progress=True)
    st.session_state.processed_news = None
    clear_selection_state()

raw_news = sorted(
    st.session_state.raw_news,
    key=lambda x: x.get("rss_score", x.get("score", 0)),
    reverse=True,
)
for idx in range(len(raw_news)):
    st.session_state.setdefault(f"chk_{idx}", False)

if st.session_state.processed_news is None:
    selected_count = sum(1 for idx in range(len(raw_news)) if st.session_state.get(f"chk_{idx}", False))

    control_a, control_b, control_c, control_d = st.columns([1, 1, 1, 1.2])
    with control_a:
        if st.button("Select All", width='stretch') and raw_news:
            reset_selection_flags(len(raw_news), True)
            st.rerun()
    with control_b:
        if st.button("Clear Selection", width='stretch') and raw_news:
            reset_selection_flags(len(raw_news), False)
            st.rerun()
    with control_c:
        if st.button("Refresh Feed", width='stretch'):
            st.session_state.refresh_requested = True
            st.session_state.processed_news = None
            st.rerun()
    with control_d:
        st.metric("Selected News", selected_count)

    total_news = len(raw_news)
    last_updated_value = _last_updated_label(LATEST_CACHE_PATH)
    _hero_placeholder.markdown(
        f"""
        <div class="hero-shell">
          <div class="hero-inner">
            <div class="hero-badge">LIVE</div>
            <h1>AI Financial News Hub</h1>
            <p>استعرض أبرز الأخبار المالية السعودية عالية الأولوية، اختر المناسب منها وأنشئ تقريراً احترافياً بضغطة زر.</p>
            <div class="hero-stats">
              <div class="hero-stat"><span class="hero-stat-val">{total_news}</span><span class="hero-stat-lbl">خبر متاح</span></div>
              <div class="hero-stat"><span class="hero-stat-val" id="sel-count-hero">{selected_count}</span><span class="hero-stat-lbl">تم تحديده</span></div>
              <div class="hero-stat"><span class="hero-stat-val">{last_updated_value}</span><span class="hero-stat-lbl">آخر تحديث</span></div>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="section-shell">
            <h3>📁 مرحلة اختيار الأخبار</h3>
            <p>اختر الأخبار الأنسب من البطاقات أدناه. يمكنك زيارة الخبر الأصلي قبل التحديد. النهائي يُرتَّب تلقائياً وفق الأهمية.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if not raw_news:
        st.warning("No cached news available. Please run pipeline_scheduler.py.")
    else:
        for start in range(0, len(raw_news), CARD_COLUMNS):
            columns = st.columns(CARD_COLUMNS, gap="small")
            for offset in range(CARD_COLUMNS):
                idx = start + offset
                if idx >= len(raw_news):
                    continue

                item = raw_news[idx]
                title = html.escape(str(item.get("title", "")))
                source = html.escape(str(item.get("source", "")))
                description = html.escape(str(item.get("description", ""))[:120])
                score = item.get("rss_score", item.get("score", 0))
                published_date = format_card_date(item.get("published", ""))
                image_url = get_news_image_url(item, prefer_original=True)
                card_class = "card-shell high-score" if score >= 100 else "card-shell"
                card_num = idx + 1

                with columns[offset]:
                    st.markdown(
                        f"""
                        <div class="{card_class}">
                            <div class="card-img-wrap">
                                <img src="{html.escape(image_url)}" alt="" />
                            </div>
                            <div class="card-content">
                                <div class="card-meta">
                                    <span class="card-source">#{card_num} {source}</span>
                                    <span class="score-badge">{score}</span>
                                </div>
                                <div class="card-meta"><span>📅 {html.escape(published_date)}</span></div>
                                <div class="card-title">{title}</div>
                                <div class="card-desc">{description}…</div>
                                <a class="card-link" href="{html.escape(item.get('link', '#'))}" target="_blank" rel="noopener noreferrer">قراءة الخبر</a>
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
                    st.checkbox("✓ اختر", key=f"chk_{idx}")

    selected_news = [item for idx, item in enumerate(raw_news) if st.session_state.get(f"chk_{idx}", False)]

    if st.button("Generate Report", type="primary", width='stretch'):
        if not selected_news:
            st.error("Select at least one article before generating the report.")
        else:
            with st.spinner("Resolving articles, extracting content, and generating structured summaries..."):
                from llm_processor import process_all_news

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
        if st.button("Back to Selection", width='stretch'):
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
                    st.image(preview_image, width='stretch')

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
                        st.image(get_news_image_url({"image_keyword": image_keyword, "title": title_val}), width='stretch')

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

    if st.button("Finalize and Export", type="primary", width='stretch'):
        if not edited_news_pool:
            st.error("Keep at least one article before export.")
        else:
            with st.status("Generating final outputs...", expanded=True) as status:
                base_dir = os.path.dirname(os.path.abspath(__file__))
                export_dir = os.path.join(base_dir, "exports")
                os.makedirs(export_dir, exist_ok=True)
                export_stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                export_stem = f"weekly_news_interactive_{st.session_state.session_token}_{export_stamp}"
                html_path = os.path.join(export_dir, f"{export_stem}.html")
                pdf_path = os.path.join(export_dir, f"{export_stem}.pdf")
                img_path = os.path.join(export_dir, f"{export_stem}.jpg")

                edited_news_pool.sort(key=lambda item: item.get("final_score", 0), reverse=True)
                export_result = export_report_assets(
                    edited_news_pool,
                    html_output=html_path,
                    pdf_output=pdf_path,
                    image_output=img_path,
                    issue_num=issue_num_input,
                    custom_ar_date=custom_ar_date or None,
                    custom_en_date=custom_en_date or None,
                )
                st.write("HTML report generated.")
                html_path = export_result.get("html_path", html_path)
                pdf_path = export_result.get("pdf_path", pdf_path)
                img_path = export_result.get("image_path", img_path)
                pdf_success = export_result.get("pdf_success", False)
                img_success = export_result.get("image_success", False)
                export_error = export_result.get("export_error", "")
                if export_error:
                    st.warning(f"Playwright export failed: {export_error}")

                status.update(label="Report generation complete.", state="complete")

            download_cols = st.columns(3)
            with download_cols[0]:
                with open(html_path, "rb") as handle:
                    st.download_button("Download HTML", handle, file_name="report.html", width='stretch')
            with download_cols[1]:
                if pdf_success and os.path.exists(pdf_path):
                    with open(pdf_path, "rb") as handle:
                        st.download_button("Download PDF", handle, file_name="report.pdf", width='stretch')
            with download_cols[2]:
                if img_success and os.path.exists(img_path):
                    with open(img_path, "rb") as handle:
                        st.download_button(
                            "Download Image",
                            handle,
                            file_name="report.jpg",
                            mime="image/jpeg",
                            width='stretch',
                        )
