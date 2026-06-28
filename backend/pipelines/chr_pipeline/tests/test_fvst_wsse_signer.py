"""Tests for FVST WS-Security signing used by CHR clients."""

import importlib
import sys
from pathlib import Path


class FakeCertificate:
    def public_bytes(self, encoding):
        return b"certificate"


class FakePrivateKey:
    def sign(self, data, padding, algorithm):
        assert data
        return b"signature"


def _auth_module():
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
    return importlib.import_module("bronze.auth")


def test_fvst_signed_wsse_signs_required_security_parts():
    auth = _auth_module()
    etree = importlib.import_module("lxml.etree")
    envelope = etree.fromstring(
        b"""
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
          <soapenv:Body>
            <ns0:ExampleRequest xmlns:ns0="urn:test"/>
          </soapenv:Body>
        </soapenv:Envelope>
        """
    )

    signer = auth.FVSTSignedWSSE("12345678", "secret", FakeCertificate(), FakePrivateKey())
    signed_envelope, _ = signer.apply(envelope, {})

    security = signed_envelope.find(f".//{{{auth.WSSE_NS}}}Security")
    assert security is not None
    assert security.find(f"{{{auth.WSSE_NS}}}BinarySecurityToken") is not None
    assert security.find(f"{{{auth.WSSE_NS}}}UsernameToken") is not None
    assert security.find(f"{{{auth.WSU_NS}}}Timestamp") is not None

    body = signed_envelope.find("{http://schemas.xmlsoap.org/soap/envelope/}Body")
    assert body is not None
    assert body.get(f"{{{auth.WSU_NS}}}Id")

    references = signed_envelope.findall(f".//{{{auth.DS_NS}}}Reference")
    assert len(references) == 4
    referenced_ids = {reference.get("URI", "").lstrip("#") for reference in references}
    assert body.get(f"{{{auth.WSU_NS}}}Id") in referenced_ids
    assert (
        security.find(f"{{{auth.WSSE_NS}}}BinarySecurityToken").get(f"{{{auth.WSU_NS}}}Id")
        in referenced_ids
    )
    assert (
        security.find(f"{{{auth.WSSE_NS}}}UsernameToken").get(f"{{{auth.WSU_NS}}}Id")
        in referenced_ids
    )
    assert (
        security.find(f"{{{auth.WSU_NS}}}Timestamp").get(f"{{{auth.WSU_NS}}}Id") in referenced_ids
    )
