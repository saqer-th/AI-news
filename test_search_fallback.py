import unittest
from unittest.mock import patch

import requests

import news_fetcher


class _FakeResponse:
    def __init__(self, url: str, text: str, status_code: int = 200) -> None:
        self.url = url
        self.text = text
        self.status_code = status_code
        self.apparent_encoding = "utf-8"
        self.encoding = "utf-8"

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class SearchFallbackTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_resolve_cache = news_fetcher._RESOLVE_CACHE
        self.original_resolve_disk_cache = news_fetcher._RESOLVE_DISK_CACHE
        self.original_resolve_cache_dirty = news_fetcher._RESOLVE_CACHE_DIRTY
        self.original_search_cache = news_fetcher._SEARCH_CACHE
        self.original_search_disk_cache = news_fetcher._SEARCH_DISK_CACHE
        self.original_search_cache_dirty = news_fetcher._SEARCH_CACHE_DIRTY
        news_fetcher._RESOLVE_CACHE = news_fetcher.TTLMemoryCache(maxsize=16, ttl_seconds=60)
        news_fetcher._RESOLVE_DISK_CACHE = {}
        news_fetcher._RESOLVE_CACHE_DIRTY = False
        news_fetcher._SEARCH_CACHE = news_fetcher.TTLMemoryCache(maxsize=16, ttl_seconds=60)
        news_fetcher._SEARCH_DISK_CACHE = {}
        news_fetcher._SEARCH_CACHE_DIRTY = False

    def tearDown(self) -> None:
        news_fetcher._RESOLVE_CACHE = self.original_resolve_cache
        news_fetcher._RESOLVE_DISK_CACHE = self.original_resolve_disk_cache
        news_fetcher._RESOLVE_CACHE_DIRTY = self.original_resolve_cache_dirty
        news_fetcher._SEARCH_CACHE = self.original_search_cache
        news_fetcher._SEARCH_DISK_CACHE = self.original_search_disk_cache
        news_fetcher._SEARCH_CACHE_DIRTY = self.original_search_cache_dirty

    def test_clean_title_strips_known_source_suffixes(self) -> None:
        self.assertEqual(news_fetcher.clean_title("قفزة في أرباح البنك - عكاظ"), "قفزة في أرباح البنك")
        self.assertEqual(news_fetcher.clean_title("Saudi stocks rise - Bloomberg"), "Saudi stocks rise")

    def test_validate_match_requires_expected_domain(self) -> None:
        self.assertTrue(
            news_fetcher.validate_match(
                "ارتفاع أرباح أرامكو في الربع الأول",
                "https://www.argaam.com/ar/article/articledetail/id/123",
                candidate_title="ارتفاع أرباح أرامكو في الربع الأول - أرقام",
                expected_source="argaam.com",
            )
        )
        self.assertFalse(
            news_fetcher.validate_match(
                "ارتفاع أرباح أرامكو في الربع الأول",
                "https://sabq.org/business/example-story",
                candidate_title="ارتفاع أرباح أرامكو في الربع الأول - سبق",
                expected_source="argaam.com",
            )
        )

    @unittest.skipUnless(news_fetcher._BS4_AVAILABLE, "BeautifulSoup is required for HTML search parsing.")
    def test_search_real_url_returns_first_valid_allowed_result(self) -> None:
        ddg_html = """
        <html>
          <body>
            <a class="result__a" href="//duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.argaam.com%2Far%2Farticle%2Farticledetail%2Fid%2F123">
              ارتفاع أرباح أرامكو في الربع الأول - أرقام
            </a>
          </body>
        </html>
        """
        fake_response = _FakeResponse(
            "https://html.duckduckgo.com/html/?q=test",
            ddg_html,
        )

        with patch.object(news_fetcher, "request_with_retry", return_value=fake_response) as mocked_request:
            resolved = news_fetcher.search_real_url(
                "ارتفاع أرباح أرامكو في الربع الأول - أرقام",
                expected_source="argaam.com",
            )

        self.assertEqual(resolved, "https://www.argaam.com/ar/article/articledetail/id/123")
        self.assertGreaterEqual(mocked_request.call_count, 1)

    def test_resolver_retries_cached_google_null_with_title_search(self) -> None:
        google_url = "https://news.google.com/rss/articles/example?oc=5"
        news_fetcher._set_cached_resolve_value(google_url, None)

        with patch.object(
            news_fetcher,
            "search_real_url",
            return_value="https://sabq.org/business/example-story",
        ) as mocked_search:
            resolved = news_fetcher._resolve_final_url_optimized(
                google_url,
                article_title="ارتفاع أرباح البنوك السعودية - سبق",
                source_hint="sabq.org",
            )

        self.assertEqual(resolved, "https://sabq.org/business/example-story")
        mocked_search.assert_called_once()


if __name__ == "__main__":
    unittest.main()
