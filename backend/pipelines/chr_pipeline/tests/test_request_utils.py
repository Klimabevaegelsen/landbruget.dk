"""Tests for CHR SOAP request utilities."""

import importlib
import sys
from pathlib import Path


def _chr_module(module_name: str):
    chr_pipeline_dir = Path(__file__).resolve().parent.parent
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
    if str(chr_pipeline_dir) in sys.path:
        sys.path.remove(str(chr_pipeline_dir))
    sys.path.insert(0, str(chr_pipeline_dir))
    return importlib.import_module(module_name)


def test_create_base_request_defaults_to_landbrugsdata(monkeypatch):
    create_base_request = _chr_module("bronze.utils").create_base_request
    monkeypatch.setenv("LANDBRUGSDATA_UUID_NAMESPACE", "00000000-0000-4000-8000-000000000000")
    monkeypatch.delenv("FVM_CLIENT_ID", raising=False)

    request = create_base_request("12345678")

    assert request["KlientId"] == "LandbrugsData"
    assert request["BrugerNavn"] == "12345678"
    assert request["TrackID"].startswith("chr_pipeline-")


def test_create_base_request_uses_fvm_client_id(monkeypatch):
    create_base_request = _chr_module("bronze.utils").create_base_request
    monkeypatch.setenv("LANDBRUGSDATA_UUID_NAMESPACE", "00000000-0000-4000-8000-000000000000")
    monkeypatch.setenv("FVM_CLIENT_ID", "assigned-client")

    request = create_base_request("12345678")

    assert request["KlientId"] == "assigned-client"
