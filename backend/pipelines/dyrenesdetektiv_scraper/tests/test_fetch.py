"""Tests for dyrenesdetektiv_scraper bronze fetch logic."""

import json

import pytest

from bronze.fetch import (
    DEFAULT_USER_AGENT,
    DyrenesDetektivBronze,
    iter_index_pages,
)

BASE = "https://dyrenesdetektiv.dk"


def _index_payload(start: int, count: int) -> list[dict]:
    return [
        {
            "id": i,
            "slug": f"slug-{i}",
            "link": f"{BASE}/kontroller/slug-{i}/",
            "date": "2022-01-01T00:00:00",
            "modified": "2022-01-01T00:00:00",
            "kontrol_tag": [1, 2, 3],
        }
        for i in range(start, start + count)
    ]


class TestIterIndexPages:
    def test_paginates_until_total_pages_reached(self, requests_mock):
        # requests_mock returns each response in order for repeated calls.
        requests_mock.get(
            f"{BASE}/wp-json/wp/v2/kontrol",
            [
                {
                    "json": _index_payload(1, 100),
                    "headers": {"X-WP-TotalPages": "3", "X-WP-Total": "230"},
                },
                {
                    "json": _index_payload(101, 100),
                    "headers": {"X-WP-TotalPages": "3", "X-WP-Total": "230"},
                },
                {
                    "json": _index_payload(201, 30),
                    "headers": {"X-WP-TotalPages": "3", "X-WP-Total": "230"},
                },
            ],
        )
        records = list(iter_index_pages(BASE, page_size=100))
        assert len(records) == 100 + 100 + 30
        assert records[0]["id"] == 1
        assert records[-1]["id"] == 230

    def test_passes_user_agent_header(self, requests_mock):
        requests_mock.get(
            f"{BASE}/wp-json/wp/v2/kontrol",
            json=_index_payload(1, 5),
            headers={"X-WP-TotalPages": "1", "X-WP-Total": "5"},
        )
        list(iter_index_pages(BASE, page_size=100))
        assert requests_mock.last_request.headers["User-Agent"] == DEFAULT_USER_AGENT


class TestBronzeRun:
    @pytest.fixture()
    def bronze_runner(self, tmp_path):
        return DyrenesDetektivBronze(
            base_url=BASE,
            output_dir=tmp_path,
            sleep_seconds=0,  # No throttling under test.
        )

    def test_limit_caps_detail_fetches(self, requests_mock, bronze_runner, tmp_path):
        import re

        requests_mock.get(
            f"{BASE}/wp-json/wp/v2/kontrol",
            json=_index_payload(1, 50),
            headers={"X-WP-TotalPages": "1", "X-WP-Total": "50"},
        )
        requests_mock.get(f"{BASE}/wp-json/wp/v2/kontrol_tag", json=[])
        # Catch every detail-page request via regex.
        requests_mock.get(
            re.compile(rf"{re.escape(BASE)}/kontroller/slug-\d+/"),
            text='<html><body class="postid-1">stub</body></html>',
        )
        manifest = bronze_runner.run(limit=3)
        assert manifest["detail_count"] == 3
        details_dir = tmp_path / "details"
        assert len(list(details_dir.glob("*.html"))) == 3

    def test_partial_failure_does_not_abort(self, requests_mock, bronze_runner, tmp_path):
        requests_mock.get(
            f"{BASE}/wp-json/wp/v2/kontrol",
            json=_index_payload(1, 3),
            headers={"X-WP-TotalPages": "1", "X-WP-Total": "3"},
        )
        requests_mock.get(f"{BASE}/wp-json/wp/v2/kontrol_tag", json=[])
        # First detail fails, the rest succeed.
        requests_mock.get(f"{BASE}/kontroller/slug-1/", status_code=500)
        requests_mock.get(f"{BASE}/kontroller/slug-2/", text="<html>ok</html>")
        requests_mock.get(f"{BASE}/kontroller/slug-3/", text="<html>ok</html>")
        manifest = bronze_runner.run()
        assert manifest["detail_count"] == 2
        assert manifest["detail_errors"] == 1

    def test_writes_index_and_metadata_files(self, requests_mock, bronze_runner, tmp_path):
        requests_mock.get(
            f"{BASE}/wp-json/wp/v2/kontrol",
            json=_index_payload(1, 1),
            headers={"X-WP-TotalPages": "1", "X-WP-Total": "1"},
        )
        requests_mock.get(
            f"{BASE}/wp-json/wp/v2/kontrol_tag", json=[{"id": 1, "name": "2022", "slug": "2022"}]
        )
        requests_mock.get(f"{BASE}/kontroller/slug-1/", text="<html>ok</html>")
        bronze_runner.run()
        index = json.loads((tmp_path / "index.json").read_text())
        tags = json.loads((tmp_path / "kontrol_tag.json").read_text())
        metadata = json.loads((tmp_path / "metadata.json").read_text())
        assert len(index) == 1
        assert tags[0]["name"] == "2022"
        assert metadata["source_url"].startswith(BASE)
        assert metadata["index_count"] == 1
        assert metadata["detail_count"] == 1
