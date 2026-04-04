from playwright.sync_api import sync_playwright

url = "https://www.argaam.com/ar/article/articledetail/id/1893483"

with sync_playwright() as p:
    browser = p.chromium.launch(
        headless=True,
        args=[
            '--disable-blink-features=AutomationControlled'
        ]
    )
    context = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    )
    context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    page = context.new_page()
    print("Navigating...")
    page.goto(url, wait_until="domcontentloaded", timeout=15000)
    page.wait_for_load_state("domcontentloaded", timeout=5000)
    page.wait_for_timeout(2000)
    
    print("Evaluating DOM...")
    extracted = page.evaluate("""
        () => {
            const selectors = [
                'article', '.article-body', '#article-body', '.article-content', '.entry-content', 
                '.post-content', '[itemprop="articleBody"]', '.content-details', '.TextContent',
                '#story_body', '.story-content', '.post-entry', '.article_content', '#news_content'
            ];
            let hits = [];
            for (let sel of selectors) {
                let el = document.querySelector(sel);
                if (el) hits.push({selector: sel, length: el.innerText.length});
            }
            return hits;
        }
    """)
    print("Found Standard Selectors:", extracted)
    
    browser.close()
