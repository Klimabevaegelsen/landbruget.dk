"""API-key secrecy checks for the BBR Datafordeler fetch path."""

from __future__ import annotations

import json
import traceback
from urllib.parse import quote, quote_plus

import pytest
import requests
from loguru import logger

from child_receptors import main as cli
from child_receptors.bronze import bbr_playgrounds

KEY = "SeCrEt-df+key/with=chars&x"


def assert_no_secret(text_or_bytes: str | bytes) -> None:
    text = text_or_bytes.decode("utf-8", errors="replace") if isinstance(text_or_bytes, bytes) else text_or_bytes
    for secret in (KEY, quote(KEY, safe=""), quote_plus(KEY)):
        assert secret not in text


def assert_exception_no_secret(error: BaseException) -> None:
    pending = [error]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        assert_no_secret(str(current))
        assert_no_secret(repr(current))
        if current.__cause__ is not None:
            pending.append(current.__cause__)
        if current.__context__ is not None:
            pending.append(current.__context__)
    assert_no_secret("".join(traceback.format_exception(error)))


class FakeResponse:
    def __init__(self, *, status_code: int = 200, text: str = "", payload: object | None = None):
        self.status_code = status_code
        self.text = text
        self.payload = payload

    def json(self) -> object:
        return self.payload


def _fetch_kommune(post, *, retries: int = 1) -> list[dict]:
    return bbr_playgrounds.fetch_kommune(
        "0101",
        api_key=KEY,
        virkningstid="2026-10-10T12:00:00Z",
        post=post,
        retries=retries,
        backoff=lambda _delay: None,
    )


def test_api_key_is_a_parameter_and_never_appears_in_request_url_or_body() -> None:
    calls: list[tuple[str, dict]] = []

    def mocked_post(url: str, **kwargs):
        calls.append((url, kwargs))
        return FakeResponse(payload={"data": {"BBR_TekniskAnlaeg": {"nodes": [], "pageInfo": {}}}})

    assert _fetch_kommune(mocked_post) == []
    assert len(calls) == 1
    url, kwargs = calls[0]
    assert url == bbr_playgrounds.BASE_URL
    assert kwargs["params"] == {"apikey": KEY}
    assert_no_secret(json.dumps(kwargs["json"]))


@pytest.mark.parametrize("status_code", [400, 401, 403, 500])
def test_http_error_response_never_leaks_api_key(status_code: int) -> None:
    body = (
        f"Bad Request for url {bbr_playgrounds.BASE_URL}?apikey={KEY}"
        f"&encoded={quote(KEY, safe='')}&plus={quote_plus(KEY)}&apiKey=unrelated-secret"
    )

    with pytest.raises(RuntimeError) as excinfo:
        _fetch_kommune(lambda _url, **_kwargs: FakeResponse(status_code=status_code, text=body))

    assert_exception_no_secret(excinfo.value)
    assert "apiKey=unrelated-secret" not in "".join(traceback.format_exception(excinfo.value))


@pytest.mark.parametrize(
    "error_factory",
    [
        pytest.param(
            lambda: requests.exceptions.ConnectionError(
                "HTTPSConnectionPool(host='graphql.datafordeler.dk', port=443): "
                f"Max retries exceeded with url: /BBR/v2?apikey={quote_plus(KEY)}"
            ),
            id="encoded-connection-error",
        ),
        pytest.param(
            lambda: requests.exceptions.HTTPError(
                f"HTTP error for https://graphql.datafordeler.dk/BBR/v2?apikey={KEY}"
            ),
            id="raw-http-error",
        ),
    ],
)
def test_transport_exception_never_leaks_api_key(error_factory) -> None:
    def mocked_post(_url, **_kwargs):
        raise error_factory()

    with pytest.raises(RuntimeError) as excinfo:
        _fetch_kommune(mocked_post)

    assert_exception_no_secret(excinfo.value)


def test_graphql_errors_payload_never_leaks_api_key() -> None:
    payload = {
        "errors": [
            f"raw={KEY}",
            f"encoded={quote(KEY, safe='')}",
            f"plus={quote_plus(KEY)}",
        ]
    }

    with pytest.raises(RuntimeError) as excinfo:
        _fetch_kommune(lambda _url, **_kwargs: FakeResponse(payload=payload))

    assert_exception_no_secret(excinfo.value)


def test_malformed_json_body_never_leaks_api_key() -> None:
    body = f"invalid response body: raw={KEY} encoded={quote(KEY, safe='')}"

    class MalformedResponse(FakeResponse):
        def json(self) -> object:
            raise ValueError(body)

    with pytest.raises(RuntimeError) as excinfo:
        _fetch_kommune(lambda _url, **_kwargs: MalformedResponse())

    assert_exception_no_secret(excinfo.value)


def test_national_fetch_never_leaks_transport_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATAFORDELER_GRAPHQL_API_KEY", KEY)

    def mocked_post(_url, **_kwargs):
        raise requests.exceptions.ConnectionError(
            "HTTPSConnectionPool(host='graphql.datafordeler.dk', port=443): "
            f"Max retries exceeded with url: /BBR/v2?apikey={quote_plus(KEY)}"
        )

    with pytest.raises(RuntimeError) as excinfo:
        bbr_playgrounds.fetch(
            kommunekoder=("0101",),
            post=mocked_post,
            backoff=lambda _delay: None,
        )

    assert_exception_no_secret(excinfo.value)


def test_missing_api_key_names_environment_variable_without_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATAFORDELER_GRAPHQL_API_KEY", raising=False)

    with pytest.raises(RuntimeError, match="DATAFORDELER_GRAPHQL_API_KEY") as excinfo:
        bbr_playgrounds.fetch(kommunekoder=())

    assert_exception_no_secret(excinfo.value)


def _run_cli_with_mocked_bbr(monkeypatch: pytest.MonkeyPatch, post) -> None:
    monkeypatch.setenv("DATAFORDELER_GRAPHQL_API_KEY", KEY)

    build_silver = cli.build_silver

    def build_with_small_floors(bronze_root, **kwargs):
        return build_silver(
            bronze_root,
            minimums={"daycare": 0, "school": 0, "playground": 0, "daycare_kommune_count": 0},
            **kwargs,
        )

    monkeypatch.setitem(
        cli.FETCHERS,
        "bbr",
        lambda: bbr_playgrounds.fetch(
            kommunekoder=("0101",),
            post=post,
            backoff=lambda _delay: None,
        ),
    )
    monkeypatch.setattr(cli, "build_silver", build_with_small_floors)


def test_cli_success_does_not_write_or_log_api_key(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    capfd: pytest.CaptureFixture[str],
) -> None:
    def mocked_post(_url, **_kwargs):
        return FakeResponse(
            payload={
                "data": {
                    "BBR_TekniskAnlaeg": {
                        "nodes": [
                            {
                                "id_lokalId": "bbr-1",
                                "kommunekode": "0101",
                                "status": "6",
                                "registreringFra": "2026-01-01T00:00:00Z",
                                "virkningFra": "2026-01-01T00:00:00Z",
                                "tek109Koordinat": {"wkt": "POINT (724300 6175800)"},
                            }
                        ],
                        "pageInfo": {"hasNextPage": False},
                    }
                }
            }
        )

    _run_cli_with_mocked_bbr(monkeypatch, mocked_post)
    log_output = []
    sink_id = logger.add(log_output.append, format="{message}")
    try:
        assert cli.main(["--layer", "all", "--sources", "bbr", "--no-upload", "--local-dir", str(tmp_path)]) == 0
    finally:
        logger.remove(sink_id)

    captured = capfd.readouterr()
    assert_no_secret(captured.out)
    assert_no_secret(captured.err)
    assert_no_secret("".join(log_output))
    for path in tmp_path.rglob("*"):
        if path.is_file():
            assert_no_secret(path.read_bytes())


def test_cli_failure_does_not_log_or_raise_api_key(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    capfd: pytest.CaptureFixture[str],
) -> None:
    def mocked_post(_url, **_kwargs):
        raise requests.exceptions.ConnectionError(
            "HTTPSConnectionPool(host='graphql.datafordeler.dk', port=443): "
            f"Max retries exceeded with url: /BBR/v2?apikey={quote_plus(KEY)}"
        )

    _run_cli_with_mocked_bbr(monkeypatch, mocked_post)
    log_output = []
    sink_id = logger.add(log_output.append, format="{message}")
    try:
        with pytest.raises(RuntimeError) as excinfo:
            cli.main(["--layer", "all", "--sources", "bbr", "--no-upload", "--local-dir", str(tmp_path)])
    finally:
        logger.remove(sink_id)

    captured = capfd.readouterr()
    assert_no_secret(captured.out)
    assert_no_secret(captured.err)
    assert_no_secret("".join(log_output))
    assert_exception_no_secret(excinfo.value)
