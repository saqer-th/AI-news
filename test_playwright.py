import feedparser
import urllib.parse
import re

query = "اقتصاد السعودية"
url = f"https://news.google.com/rss/search?q={urllib.parse.quote(query)}&hl=ar&gl=SA&ceid=SA:ar"

feed = feedparser.parse(url)

def extract_real_url(entry):
    # ✅ 1. حاول من links
    for link_obj in entry.get("links", []):
        href = link_obj.get("href", "")
        if href.startswith("http") and "news.google.com" not in href:
            return href

    # ✅ 2. حاول من summary
    raw = entry.get("summary", "") or entry.get("description", "")
    matches = re.findall(r'href="([^"]+)"', raw)

    for href in matches:
        if href.startswith("http") and "news.google.com" not in href:
            return href

    # ❌ ما حصلنا شيء
    return None


for entry in feed.entries[:5]:
    print("📰", entry.title)

    real_link = extract_real_url(entry)

    print("🔗", real_link)
    print("-" * 50)