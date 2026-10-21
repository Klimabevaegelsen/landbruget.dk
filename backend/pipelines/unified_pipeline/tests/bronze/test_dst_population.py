"""Tests for the FOLK1A StatBank fetch and parser."""

from unittest.mock import Mock

import requests

from unified_pipeline.bronze.dst_population import fetch_folk1a_csv, parse_folk1a_csv

FOLK1A_SAMPLE = """\ufeffOMRÅDE;TID;INDHOLD
000;2026K3;6031699
084;2026K3;1949336
101;2026K3;670389
147;2026K3;105947
"""


def test_parse_folk1a_csv_excludes_denmark_total_and_regions():
    assert parse_folk1a_csv(FOLK1A_SAMPLE) == (
        "2026K3",
        6031699,
        {101: 670389, 147: 105947},
    )


def test_fetch_folk1a_csv_sends_verified_request(monkeypatch):
    response = Mock(text=FOLK1A_SAMPLE)
    response.raise_for_status.return_value = None
    post = Mock(return_value=response)
    monkeypatch.setattr(requests, "post", post)

    result = fetch_folk1a_csv()

    assert result == FOLK1A_SAMPLE
    post.assert_called_once_with(
        "https://api.statbank.dk/v1/data",
        json={
            "table": "FOLK1A",
            "format": "CSV",
            "lang": "en",
            "valuePresentation": "Code",
            "variables": [
                {"code": "OMRÅDE", "values": ["*"]},
                {"code": "TID", "values": ["(1)"]},
            ],
        },
        timeout=60,
    )


def test_fetch_folk1a_csv_retries_request_failures(monkeypatch):
    successful_response = Mock(text=FOLK1A_SAMPLE)
    successful_response.raise_for_status.return_value = None
    post = Mock(side_effect=[requests.Timeout("first"), successful_response])
    monkeypatch.setattr(requests, "post", post)

    assert fetch_folk1a_csv() == FOLK1A_SAMPLE
    assert post.call_count == 2


def test_fetch_folk1a_csv_makes_three_retries_before_raising(monkeypatch):
    post = Mock(side_effect=requests.Timeout("timeout"))
    monkeypatch.setattr(requests, "post", post)

    try:
        fetch_folk1a_csv()
    except RuntimeError as error:
        assert "3 retries" in str(error)
    else:
        raise AssertionError("fetch_folk1a_csv should raise after exhausting retries")
    assert post.call_count == 4
