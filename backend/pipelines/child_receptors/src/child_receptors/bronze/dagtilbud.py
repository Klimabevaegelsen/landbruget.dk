"""STIL Dagtilbudsregisteret export fetched through headless Chromium."""

from __future__ import annotations

import asyncio
import base64
import csv
import io
import time
from collections.abc import Callable

from child_receptors.config import SOURCE_URLS

URL = SOURCE_URLS["dagtilbud"]
TARGETS = {
    "daginstitution": "ctl00$ContentPlaceHolder1$LinkButtonDag",
    "anvisningsenhed": "ctl00$ContentPlaceHolder1$LinkButtonAnv",
    "alternativ_adresse": "ctl00$ContentPlaceHolder1$LinkButtonAlt",
    "samlet": "ctl00$ContentPlaceHolder1$LinkButtonOejeblik",
}
EXPECTED_COLUMNS = {
    "daginstitution": "daginstitutionsNummer",
    "anvisningsenhed": "anvisningsenhedsNummer",
    "alternativ_adresse": "alternativAdresseNummer",
    "samlet": "dagtilbudsType",
}
JS_POSTBACK = """async (target) => {
  const form = document.forms[0];
  const formData = new FormData(form);
  formData.set('__EVENTTARGET', target);
  formData.set('__EVENTARGUMENT', '');
  const response = await fetch(location.href, {
    method: 'POST', body: new URLSearchParams(formData)
  });
  const bytes = new Uint8Array(await response.arrayBuffer());
  let binary = '';
  for (let offset = 0; offset < bytes.length; offset += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(offset, offset + 0x8000));
  }
  return {status: response.status, b64: btoa(binary)};
}"""


def validate_exports(exports: dict[str, bytes]) -> None:
    """Reject challenge HTML and malformed exports before any bytes are saved."""
    for name, first_column in EXPECTED_COLUMNS.items():
        if name not in exports:
            raise ValueError(f"Dagtilbud export is missing {name}")
        body = exports[name]
        try:
            reader = csv.reader(io.StringIO(body.decode("utf-8-sig"), newline=""), delimiter=";")
            header = next(reader)
        except (UnicodeDecodeError, StopIteration, csv.Error) as exc:
            raise ValueError(f"Dagtilbud {name} response is not a CSV export") from exc
        if not header or header[0].strip() != first_column:
            raise ValueError(f"Dagtilbud {name} response has an unexpected header (possible bot challenge)")
        if name == "anvisningsenhed":
            row_count = sum(1 for row in reader if row)
            if row_count < 4000:
                raise ValueError(f"Dagtilbud anvisningsenhed has only {row_count} data rows; expected at least 4000")


async def _fetch_session(playwright_factory: Callable[[], object] | None = None) -> dict[str, bytes]:
    if playwright_factory is None:
        from playwright.async_api import async_playwright

        playwright_factory = async_playwright
    exports: dict[str, bytes] = {}
    async with playwright_factory() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        try:
            page = await browser.new_page(locale="da-DK")
            await page.goto(URL, wait_until="networkidle")
            if "Eksport af data" not in await page.content():
                raise ValueError("Dagtilbud site returned a bot challenge page")
            for name, target in TARGETS.items():
                response = await page.evaluate(JS_POSTBACK, target)
                body = base64.b64decode(response["b64"])
                if response.get("status") != 200:
                    raise ValueError(f"Dagtilbud {name} request returned HTTP {response.get('status')}")
                exports[f"dagtilbudsregister_{name}.csv"] = body
        finally:
            await browser.close()
    files = {name.removeprefix("dagtilbudsregister_").removesuffix(".csv"): data for name, data in exports.items()}
    validate_exports(files)
    return exports


async def fetch_dagtilbud(*, retries: int = 3, backoff: Callable[[float], None] = time.sleep) -> dict[str, bytes]:
    """Fetch all four CSVs, retrying the entire browser session up to three times."""
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            return await _fetch_session()
        except Exception as exc:
            last_error = exc
            if attempt + 1 < retries:
                backoff(2**attempt)
    raise RuntimeError(f"Dagtilbud browser fetch failed after {retries} attempts") from last_error


def fetch() -> dict[str, bytes]:
    """Synchronous CLI bridge for the async Playwright client."""
    return asyncio.run(fetch_dagtilbud())
