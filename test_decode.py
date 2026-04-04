import base64
import re

def decode_google_news_url(url: str) -> str:
    try:
        if "news.google.com/rss/articles/" not in url:
            return url
        
        prefix = "articles/"
        idx = url.find(prefix) + len(prefix)
        encoded = url[idx:].split('?')[0]
        
        padding = 4 - (len(encoded) % 4)
        encoded += "=" * padding
        
        decoded_bytes = base64.urlsafe_b64decode(encoded)
        print("RAW BYTES:")
        print(decoded_bytes)
        
        matches = re.findall(b'https?://[a-zA-Z0-9./\-_%?=]+', decoded_bytes)
        print("Matches:", matches)
        
        for match in matches:
            decoded_url = match.decode('utf-8', errors='ignore')
            if "google" not in decoded_url:
                return decoded_url
    except Exception as e:
        print("Error:", e)
        
    return url

url = "https://news.google.com/rss/articles/CBMi_gFBVV95cUxOOURCdEhLZDN0R2U2TmZPM2xNV3RJSERydXNURkFSdi1ENHRIb0ZPbnBxT0lPR3lRa3JJTWVrTUFkXzFCZkY4NzJ6VTh4TGpCMFNUYmM2eHFJVzhWVy0zYmtXNDQ1cHVFTm1qY2JzTXBLTk1qTnVFRUJWaHhnOWJ0cV9fdzVPS1lVVWc0YnFsMVBiN3g1bGs3a01TX09mZEdJR181OGpnRzYyaU5lcVF3RlIzYnREeW5MaEExUHFkNEZSM045bE5tQU1OdUVRNEE2UzNfSkY2a2pJdVk5MFVEVGZ0a2kwUXZ5QjhSZjFVSUljYzREbkgtbGhnNGhTQQ?oc=5"
print("Decoded:", decode_google_news_url(url))

import requests
import trafilatura

decoded = decode_google_news_url(url)
if decoded != url:
    print("Testing trafilatura on:", decoded)
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    })
    resp = session.get(decoded, allow_redirects=True, timeout=15)
    print("Status:", resp.status_code)
    result = trafilatura.extract(resp.text, include_comments=False, include_tables=False)
    print("Extracted len:", len(result) if result else 0)
    if result:
        print(result[:200])

