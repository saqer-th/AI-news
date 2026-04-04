import feedparser
import requests
import trafilatura

url = 'https://news.google.com/rss/search?q=تاسي&hl=ar'
feed = feedparser.parse(url)

print("Fetched feed:", len(feed.entries))

if feed.entries:
    first_link = feed.entries[0].link
    print("Original Link:", first_link)
    
    # Let's try requests to resolve redirect
    try:
        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
        resp = session.get(first_link, allow_redirects=True, timeout=5)
        print("Resolved Link:", resp.url)
        
        import re
        html_text = resp.text
        print("HTML length:", len(html_text))
        
        # Search for ALL absolute URLs
        matches = re.findall(r'https?://[a-zA-Z0-9.\-/%?=]+', html_text)
        found_target = None
        for m in matches:
            if "google" not in m and "gstatic" not in m and "w3.org" not in m:
                found_target = m
                break
                
        print("FOUND TRUE URL:", found_target)
        
    except Exception as e:
        print("Error:", e)
