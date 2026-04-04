import argparse
import webbrowser
import os
from utils import setup_logger
from news_fetcher import fetch_valid_news
from llm_processor import process_all_news
from html_generator import generate_report
from playwright.sync_api import sync_playwright

logger = setup_logger(__name__)

def generate_pdf_and_image(html_path: str, pdf_path: str, img_path: str):
    logger.info(f"Generating PDF and Image layouts...")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            # Force viewport width so the CSS grid aligns perfectly
            page.set_viewport_size({"width": 1000, "height": 1200})
            
            # Navigate to the local file
            page.goto(f"file://{html_path}", wait_until="networkidle")
            
            # Enforce screen media queries
            page.emulate_media(media="screen")
            
            # Mathematically calculate the exact pixel height of the full report
            content_height = page.evaluate("() => document.documentElement.scrollHeight") + 40
            
            # Produce 1 seamless continuous PDF page
            page.pdf(
                path=pdf_path, 
                width="1000px", 
                height=f"{content_height}px", 
                print_background=True, 
                page_ranges="1",
                margin={"top":"0", "right":"0", "bottom":"0", "left":"0"}
            )
            
            # Produce a high-resolution full-page image
            page.screenshot(path=img_path, full_page=True)
            
            browser.close()
        logger.info(f"Successfully generated 1-page PDF -> {pdf_path}")
        logger.info(f"Successfully generated Image -> {img_path}")
    except Exception as e:
        logger.error(f"Failed to generate exports. Ensure playwright is installed via 'playwright install chromium'. Error: {e}")

def main():
    parser = argparse.ArgumentParser(description="Generate a professional news report using local LLM.")
    parser.add_argument(
        "--mode", 
        type=str, 
        choices=["daily", "weekly"], 
        default="weekly", 
        help="Run mode for the report (daily or weekly)"
    )
    parser.add_argument(
        "--issue", 
        type=str,
        default="80",
        help="The Issue Number to display in the header"
    )
    args = parser.parse_args()
    
    logger.info(f"Starting {args.mode} news generation pipeline for Issue {args.issue}...")

    # 1. Fetch
    logger.info("=== STEP 1: Fetching News ===")
    raw_news = fetch_valid_news(target=10)
    
    if not raw_news:
        logger.error("No news fetched. Exiting.")
        return

    # 2. Process with LLM
    logger.info("=== STEP 2: Processing via Ollama ===")
    processed_news = process_all_news(raw_news)
    
    if not processed_news:
        logger.error("No news successfully processed by LLM. Exiting.")
        return

    # 3. Rank
    logger.info("=== STEP 3: Ranking News ===")
    processed_news.sort(key=lambda x: float(x.get("final_score", 0)), reverse=True)
    top_news = processed_news[:4]
    
    logger.info(f"Selected Top {len(top_news)} news items for the report.")

    # 4. Generate HTML and output
    logger.info("=== STEP 4: Generating Outputs (HTML/PDF/Image) ===")
    prefix = "weekly_news" if args.mode == "weekly" else "daily_news"
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(base_dir, f"{prefix}.html")
    pdf_path = os.path.join(base_dir, f"{prefix}.pdf")
    img_path = os.path.join(base_dir, f"{prefix}.png")
    
    # Regular Aesthetic HTML Report
    generate_report(top_news, html_path, issue_num=args.issue)
    
    # High-idelity PDF and Full-page PNG render
    generate_pdf_and_image(html_path, pdf_path, img_path)

    # Automatically open the original HTML in default browser
    logger.info(f"Opening report in browser: {html_path}")
    webbrowser.open(f"file://{html_path}")

    logger.info(f"Pipeline completed successfully!")

if __name__ == "__main__":
    main()
