"""Tests for Svineflytning bronze service-level status handling."""

import importlib
import sys
from datetime import date
from pathlib import Path

import pytest


def _svineflytning_module():
    pipeline_dir = Path(__file__).resolve().parent.parent
    for name, module in list(sys.modules.items()):
        if (
            name == "bronze"
            or name.startswith("bronze.")
            or name == "zeep"
            or name.startswith("zeep.")
        ):
            del sys.modules[name]
            continue
        if (
            name == "lxml"
            or name.startswith("lxml.")
            or name == "cryptography"
            or name.startswith("cryptography.")
        ) and not str(getattr(module, "__file__", "")):
            del sys.modules[name]
    if str(pipeline_dir) in sys.path:
        sys.path.remove(str(pipeline_dir))
    sys.path.insert(0, str(pipeline_dir))
    return importlib.import_module("bronze.load_svineflytning")


def test_raise_for_non_ok_response_allows_ok_status():
    _raise_for_non_ok_response = _svineflytning_module()._raise_for_non_ok_response

    _raise_for_non_ok_response({"GLRCHRWSInfoOutbound": {"ReturSvar": "OK"}})


def test_raise_for_non_ok_response_reports_glr_message():
    _raise_for_non_ok_response = _svineflytning_module()._raise_for_non_ok_response

    response_info = {
        "GLRCHRWSInfoOutbound": {
            "ReturSvar": "SYSTEMFEJL",
            "GLRCHRSvarMeddelelser": {
                "Meddelelse": [
                    {
                        "MeddelelseType": "ERROR",
                        "MeddelelseKode": "GLR0003",
                        "MeddelelseTekst": "Fejl i brugernavn/password : ",
                    }
                ]
            },
        }
    }

    with pytest.raises(RuntimeError, match=r"SYSTEMFEJL.*GLR0003"):
        _raise_for_non_ok_response(response_info)


def test_fetch_movements_uses_current_fvm_client_id(monkeypatch):
    fetch_movements = _svineflytning_module().fetch_movements
    captured_requests = []

    class FakeService:
        def listAlleFlytningerIPerioden(self, request):  # noqa: N802
            captured_requests.append(request)
            return {"GLRCHRWSInfoOutbound": {"ReturSvar": "OK"}}

    class FakeClient:
        service = FakeService()

    monkeypatch.setenv("FVM_USERNAME", "12345678")
    monkeypatch.setenv("FVM_CLIENT_ID", "assigned-client")

    fetch_movements(FakeClient(), date(2026, 1, 1), date(2026, 1, 1))

    assert captured_requests[0]["GLRCHRWSInfoInbound"]["KlientId"] == "assigned-client"
