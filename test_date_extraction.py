import unittest

import news_fetcher


class DateExtractionTests(unittest.TestCase):
    def test_parse_day_first_numeric_date(self) -> None:
        parsed = news_fetcher.parse_page_date_text("09-04-2026")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.year, 2026)
        self.assertEqual(parsed.month, 4)
        self.assertEqual(parsed.day, 9)

    def test_extract_maaal_date_from_json_ld(self) -> None:
        html = """
        <script type="application/ld+json">
        {"datePublished":"2026-04-09T14:28:05.000000Z"}
        </script>
        """
        parsed = news_fetcher._extract_maaal_date_from_html(html)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.year, 2026)
        self.assertEqual(parsed.month, 4)
        self.assertEqual(parsed.day, 9)

    def test_extract_date_from_url_path(self) -> None:
        url = "https://www.alarabiya.net/amp/aswaq/videos/aswaq-open/2026/04/09/example"
        parsed = news_fetcher._extract_date_from_url_path(url)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.year, 2026)
        self.assertEqual(parsed.month, 4)
        self.assertEqual(parsed.day, 9)

    def test_extract_spa_prefers_parseable_gregorian_date(self) -> None:
        html = """
        <meta name="description" content="واشنطن 21 شوال 1447 هـ الموافق 09 أبريل 2026 م واس">
        """
        parsed = news_fetcher._extract_spa_date_from_html(html)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.year, 2026)
        self.assertEqual(parsed.month, 4)
        self.assertEqual(parsed.day, 9)

    def test_extract_maaal_date_from_arabic_text(self) -> None:
        html = "<div>نشر في 9 أبريل 2026</div>"
        parsed = news_fetcher._extract_maaal_date_from_html(html)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.year, 2026)
        self.assertEqual(parsed.month, 4)
        self.assertEqual(parsed.day, 9)


if __name__ == "__main__":
    unittest.main()
