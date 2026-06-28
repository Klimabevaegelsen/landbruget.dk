"""Tests for CHR Stamdata parsing and cache fallback."""

import importlib
import sys
from pathlib import Path
from types import SimpleNamespace


def _chr_main():
    chr_pipeline_dir = Path(__file__).resolve().parent.parent
    for name, module in list(sys.modules.items()):
        if name in {"main", "zeep", "bronze"} or name.startswith(("zeep.", "bronze.")):
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
    return importlib.import_module("main")


def test_parse_stamdata_combinations_from_cached_bronze_export():
    chr_main = _chr_main()
    cached_response = [
        {
            "GLRCHRWSInfoOutbound": {"ReturSvar": "OK"},
            "Response": [
                {
                    "DyreArtKode": 12,
                    "BrugsArtKode": 101,
                    "DyreArtTekst": "Cattle",
                    "BrugsArtTekst": "Dairy cattle",
                },
                {
                    "DyreArtKode": 15,
                    "BrugsArtKode": 201,
                    "DyreArtTekst": "Pigs",
                    "BrugsArtTekst": "Slaughter pigs",
                },
            ],
        }
    ]

    combinations = chr_main._parse_stamdata_combinations(cached_response, test_species_codes=[12])

    assert combinations == [
        {
            "species_code": 12,
            "usage_code": 101,
            "species_text": "Cattle",
            "usage_text": "Dairy cattle",
        }
    ]


def test_fetch_stamdata_uses_cached_fallback_for_empty_live_response(monkeypatch):
    chr_main = _chr_main()
    fallback_combinations = [
        {
            "species_code": 12,
            "usage_code": 101,
            "species_text": "Cattle",
            "usage_text": "Dairy cattle",
        }
    ]
    calls = {}

    def fake_load_species_usage_combinations(_client, _username):
        return SimpleNamespace(GLRCHRWSInfoOutbound={"ReturSvar": "OK"}, Response=[])

    def fake_save_raw_data(**kwargs):
        calls["saved_identifier"] = kwargs["identifier"]

    def fake_load_cached_stamdata_combinations(test_species_codes=None):
        calls["test_species_codes"] = test_species_codes
        return fallback_combinations

    monkeypatch.setattr(
        chr_main, "load_species_usage_combinations", fake_load_species_usage_combinations
    )
    monkeypatch.setattr(chr_main, "save_raw_data", fake_save_raw_data)
    monkeypatch.setattr(
        chr_main, "_load_cached_stamdata_combinations", fake_load_cached_stamdata_combinations
    )

    combinations = chr_main.fetch_stamdata(
        client=object(), username="test-user", test_species_codes=[12]
    )

    assert combinations == fallback_combinations
    assert calls == {
        "saved_identifier": "all",
        "test_species_codes": [12],
    }


def test_fetch_stamdata_does_not_use_cached_fallback_for_non_ok_response(monkeypatch):
    chr_main = _chr_main()
    calls = {"fallback_called": False}

    def fake_load_species_usage_combinations(_client, _username):
        return SimpleNamespace(
            GLRCHRWSInfoOutbound={
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
            },
            Response=[],
        )

    def fake_save_raw_data(**_kwargs):
        calls["saved"] = True

    def fake_load_cached_stamdata_combinations(test_species_codes=None):
        calls["fallback_called"] = True
        return [
            {
                "species_code": 12,
                "usage_code": 101,
                "species_text": "Cattle",
                "usage_text": "Dairy cattle",
            }
        ]

    monkeypatch.setattr(
        chr_main, "load_species_usage_combinations", fake_load_species_usage_combinations
    )
    monkeypatch.setattr(chr_main, "save_raw_data", fake_save_raw_data)
    monkeypatch.setattr(
        chr_main, "_load_cached_stamdata_combinations", fake_load_cached_stamdata_combinations
    )

    combinations = chr_main.fetch_stamdata(client=object(), username="test-user")

    assert combinations == []
    assert calls == {"fallback_called": False, "saved": True}
