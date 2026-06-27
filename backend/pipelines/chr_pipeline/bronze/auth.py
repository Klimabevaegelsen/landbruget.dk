"""Authentication and SOAP client creation for CHR pipeline."""

import base64
import hashlib
import os
import secrets
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import certifi
import requests
from common.logging_utils import get_pipeline_logger
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.hazmat.primitives.serialization.pkcs12 import load_key_and_certificates
from dotenv import find_dotenv, load_dotenv
from lxml import etree
from requests import Session
from zeep import Client, Settings
from zeep.transports import Transport
from zeep.wsse.username import UsernameToken

# Load environment variables (walks up to find root .env)
load_dotenv(find_dotenv(usecwd=True))

from common.secrets import init_secrets  # noqa: E402

init_secrets()

# Set up logging
logger = get_pipeline_logger("backend.pipelines.chr_pipeline.bronze.auth")

SOAP11_NS = "http://schemas.xmlsoap.org/soap/envelope/"
SOAP12_NS = "http://www.w3.org/2003/05/soap-envelope"
WSSE_NS = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"
WSU_NS = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd"
DS_NS = "http://www.w3.org/2000/09/xmldsig#"
EC_NS = "http://www.w3.org/2001/10/xml-exc-c14n#"
PASSWORD_TEXT_TYPE = (
    "http://docs.oasis-open.org/wss/2004/01/"
    "oasis-200401-wss-username-token-profile-1.0#PasswordText"
)
BASE64_ENCODING_TYPE = (
    "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary"
)
X509_VALUE_TYPE = (
    "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-x509-token-profile-1.0#X509v3"
)
EXCL_C14N_ALGORITHM = "http://www.w3.org/2001/10/xml-exc-c14n#"
RSA_SHA1_ALGORITHM = "http://www.w3.org/2000/09/xmldsig#rsa-sha1"
SHA256_DIGEST_ALGORITHM = "http://www.w3.org/2001/04/xmlenc#sha256"

# API Endpoints (WSDL URLs)
ENDPOINTS = {
    "chr_dyr": "https://ws.fvst.dk/service/CHR_dyrWS?wsdl",
    "stamdata": "https://ws.fvst.dk/service/CHR_stamdataWS?wsdl",
    "diko": "https://ws.fvst.dk/service/DIKOWS?wsdl",
    "ejendom": "https://ws.fvst.dk/service/CHR_ejendomWS?wsdl",
    "besaetning": "https://ws.fvst.dk/service/CHR_besaetningWS?wsdl",
}


class FVSTSignedWSSE:
    """VetStat-style FVST WS-Security signer for CHR SOAP requests."""

    def __init__(self, username: str, password: str, certificate: Any, private_key: Any):
        self.username = username
        self.password = password
        self.certificate = certificate
        self.private_key = private_key

    def apply(
        self, envelope: etree._Element, headers: dict[str, Any]
    ) -> tuple[Any, dict[str, Any]]:
        self._apply_security_header(envelope)
        return envelope, headers

    def verify(self, envelope: Any) -> Any:
        # FVST signs responses too, but the previous CHR clients did not verify response
        # signatures. Keep that behavior here and only change outgoing request auth.
        return envelope

    def _apply_security_header(self, envelope: etree._Element) -> None:
        soap_ns = etree.QName(envelope).namespace or SOAP11_NS
        if soap_ns not in {SOAP11_NS, SOAP12_NS}:
            soap_ns = SOAP11_NS

        header = envelope.find(f"{{{soap_ns}}}Header")
        body = envelope.find(f"{{{soap_ns}}}Body")
        if body is None:
            raise ValueError("SOAP Body not found; cannot sign FVST request")
        if header is None:
            header = etree.Element(etree.QName(soap_ns, "Header"))
            envelope.insert(0, header)

        for existing_security in header.findall(f"{{{WSSE_NS}}}Security"):
            header.remove(existing_security)

        nsmap = {
            "wsse": WSSE_NS,
            "wsu": WSU_NS,
            "ds": DS_NS,
            "ec": EC_NS,
        }
        security = etree.Element(etree.QName(WSSE_NS, "Security"), nsmap=nsmap)
        header.insert(0, security)

        now = datetime.now(UTC).replace(microsecond=0)
        created = now.isoformat().replace("+00:00", "Z")
        expires = (now + timedelta(minutes=5)).isoformat().replace("+00:00", "Z")

        body_id = _ensure_wsu_id(body, "id-")
        binary_token_id = _new_id("X509-")
        username_token_id = _new_id("UsernameToken-")
        timestamp_id = _new_id("TS-")
        signature_id = _new_id("SIG-")
        key_info_id = _new_id("KI-")
        security_token_ref_id = _new_id("STR-")

        binary_token = etree.SubElement(
            security,
            etree.QName(WSSE_NS, "BinarySecurityToken"),
            {
                etree.QName(WSU_NS, "Id"): binary_token_id,
                "EncodingType": BASE64_ENCODING_TYPE,
                "ValueType": X509_VALUE_TYPE,
            },
        )
        binary_token.text = base64.b64encode(self.certificate.public_bytes(Encoding.DER)).decode()

        username_token = etree.SubElement(
            security,
            etree.QName(WSSE_NS, "UsernameToken"),
            {etree.QName(WSU_NS, "Id"): username_token_id},
        )
        etree.SubElement(username_token, etree.QName(WSSE_NS, "Username")).text = self.username
        etree.SubElement(
            username_token,
            etree.QName(WSSE_NS, "Password"),
            {"Type": PASSWORD_TEXT_TYPE},
        ).text = self.password
        etree.SubElement(
            username_token,
            etree.QName(WSSE_NS, "Nonce"),
            {"EncodingType": BASE64_ENCODING_TYPE},
        ).text = base64.b64encode(secrets.token_bytes(16)).decode()
        etree.SubElement(username_token, etree.QName(WSU_NS, "Created")).text = created

        timestamp = etree.SubElement(
            security,
            etree.QName(WSU_NS, "Timestamp"),
            {etree.QName(WSU_NS, "Id"): timestamp_id},
        )
        etree.SubElement(timestamp, etree.QName(WSU_NS, "Created")).text = created
        etree.SubElement(timestamp, etree.QName(WSU_NS, "Expires")).text = expires

        signature = etree.SubElement(
            security,
            etree.QName(DS_NS, "Signature"),
            {"Id": signature_id},
        )
        signed_info = etree.SubElement(signature, etree.QName(DS_NS, "SignedInfo"))
        canonicalization = etree.SubElement(
            signed_info,
            etree.QName(DS_NS, "CanonicalizationMethod"),
            {"Algorithm": EXCL_C14N_ALGORITHM},
        )
        _add_inclusive_namespaces(canonicalization, _inclusive_prefixes(signed_info))
        etree.SubElement(
            signed_info,
            etree.QName(DS_NS, "SignatureMethod"),
            {"Algorithm": RSA_SHA1_ALGORITHM},
        )

        references = [
            (body_id, body),
            (timestamp_id, timestamp),
            (username_token_id, username_token),
            (binary_token_id, binary_token),
        ]
        for element_id, element in references:
            _append_reference(signed_info, element_id, element)

        signed_info_prefixes = _inclusive_prefixes(signed_info)
        signed_info_c14n = _canonicalize(signed_info, signed_info_prefixes)
        signature_value = self.private_key.sign(
            signed_info_c14n,
            padding.PKCS1v15(),
            hashes.SHA1(),
        )
        etree.SubElement(signature, etree.QName(DS_NS, "SignatureValue")).text = base64.b64encode(
            signature_value
        ).decode()

        key_info = etree.SubElement(
            signature,
            etree.QName(DS_NS, "KeyInfo"),
            {"Id": key_info_id},
        )
        security_token_reference = etree.SubElement(
            key_info,
            etree.QName(WSSE_NS, "SecurityTokenReference"),
            {etree.QName(WSU_NS, "Id"): security_token_ref_id},
        )
        etree.SubElement(
            security_token_reference,
            etree.QName(WSSE_NS, "Reference"),
            {
                "URI": f"#{binary_token_id}",
                "ValueType": X509_VALUE_TYPE,
            },
        )


def create_fvst_wsse(username: str, password: str, certificate: Any, private_key: Any) -> Any:
    """Create FVST-compatible WS-Security with UsernameToken and cert signature."""
    return FVSTSignedWSSE(username, password, certificate, private_key)


def _new_id(prefix: str) -> str:
    return f"{prefix}{uuid.uuid4().hex.upper()}"


def _ensure_wsu_id(element: etree._Element, prefix: str) -> str:
    existing = element.get(etree.QName(WSU_NS, "Id"))
    if existing:
        return existing
    new_id = _new_id(prefix)
    element.set(etree.QName(WSU_NS, "Id"), new_id)
    return new_id


def _inclusive_prefixes(element: etree._Element) -> list[str]:
    prefixes = []
    current: etree._Element | None = element
    while current is not None:
        for prefix in current.nsmap:
            if prefix and prefix not in prefixes:
                prefixes.append(prefix)
        current = current.getparent()
    for prefix in ["ds", "ec", "wsse", "wsu"]:
        if prefix not in prefixes:
            prefixes.append(prefix)
    return prefixes


def _add_inclusive_namespaces(parent: etree._Element, prefixes: list[str]) -> None:
    etree.SubElement(
        parent,
        etree.QName(EC_NS, "InclusiveNamespaces"),
        {"PrefixList": " ".join(prefixes)},
    )


def _canonicalize(element: etree._Element, prefixes: list[str]) -> bytes:
    return etree.tostring(
        element,
        method="c14n",
        exclusive=True,
        inclusive_ns_prefixes=prefixes,
        with_comments=False,
    )


def _append_reference(
    signed_info: etree._Element, element_id: str, element: etree._Element
) -> None:
    prefixes = _inclusive_prefixes(element)
    reference = etree.SubElement(
        signed_info,
        etree.QName(DS_NS, "Reference"),
        {"URI": f"#{element_id}"},
    )
    transforms = etree.SubElement(reference, etree.QName(DS_NS, "Transforms"))
    transform = etree.SubElement(
        transforms,
        etree.QName(DS_NS, "Transform"),
        {"Algorithm": EXCL_C14N_ALGORITHM},
    )
    _add_inclusive_namespaces(transform, prefixes)
    etree.SubElement(
        reference,
        etree.QName(DS_NS, "DigestMethod"),
        {"Algorithm": SHA256_DIGEST_ALGORITHM},
    )
    digest = hashlib.sha256(_canonicalize(element, prefixes)).digest()
    etree.SubElement(reference, etree.QName(DS_NS, "DigestValue")).text = base64.b64encode(
        digest
    ).decode()


def get_fvm_credentials() -> tuple[str, str, Any, Any]:
    """Get FVM username, password, VetStat certificate, and private key for robust authentication."""
    # Get required environment variables
    username = os.getenv("FVM_USERNAME")
    password = os.getenv("FVM_PASSWORD")
    cert_base64 = os.getenv("VETSTAT_CERTIFICATE")
    cert_path = os.getenv("VETSTAT_CERTIFICATE_PATH")
    cert_password = os.getenv("VETSTAT_CERTIFICATE_PASSWORD")

    # Debug log the state of environment variables (masking sensitive data)
    logger.debug("Environment variable status:")
    logger.debug(f"FVM_USERNAME: {'[SET]' if username else '[MISSING]'}")
    logger.debug(f"FVM_PASSWORD: {'[SET]' if password else '[MISSING]'}")
    logger.debug(f"VETSTAT_CERTIFICATE: {'[SET]' if cert_base64 else '[MISSING]'}")
    logger.debug(f"VETSTAT_CERTIFICATE_PATH: {'[SET]' if cert_path else '[MISSING]'}")
    logger.debug(f"VETSTAT_CERTIFICATE_PASSWORD: {'[SET]' if cert_password else '[MISSING]'}")

    # Check for missing variables
    missing_vars = []
    if not username:
        missing_vars.append("FVM_USERNAME")
    if not password:
        missing_vars.append("FVM_PASSWORD")
    if not cert_base64 and not cert_path:
        missing_vars.append("VETSTAT_CERTIFICATE or VETSTAT_CERTIFICATE_PATH")
    if not cert_password:
        missing_vars.append("VETSTAT_CERTIFICATE_PASSWORD")

    if missing_vars:
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    try:
        p12_data = None

        # Try to get certificate data from base64 string first
        if cert_base64:
            try:
                if isinstance(cert_base64, bytes):
                    logger.debug(
                        "Using binary certificate data from VETSTAT_CERTIFICATE environment variable"
                    )
                    p12_data = cert_base64
                    logger.debug(f"Using binary certificate data. Length: {len(p12_data)} bytes")
                else:
                    logger.debug(
                        "Using base64 certificate from VETSTAT_CERTIFICATE environment variable"
                    )
                    p12_data = base64.b64decode(cert_base64, validate=True)
                    logger.debug(
                        f"Successfully decoded base64 certificate. Decoded length: {len(p12_data)} bytes"
                    )
            except Exception as decode_error:
                logger.error(f"Failed to process certificate data: {decode_error!s}")
                p12_data = None

        # If base64 decoding failed or wasn't provided, try reading from file
        if not p12_data and cert_path:
            try:
                logger.debug(f"Reading certificate from file: {cert_path}")
                with open(cert_path, "rb") as f:
                    p12_data = f.read()
                logger.debug(f"Successfully read certificate file. Length: {len(p12_data)} bytes")
            except Exception as file_error:
                logger.error(f"Failed to read certificate file {cert_path}: {file_error!s}")
                raise ValueError(f"Failed to read certificate file: {file_error!s}") from file_error

        if not p12_data:
            raise ValueError(
                "No valid certificate data found from either environment variable or file"
            )

        # Load the certificate and private key from the data
        try:
            private_key, certificate, _ = load_key_and_certificates(
                p12_data, cert_password.encode("utf-8")
            )
            logger.debug("Successfully loaded private key and certificate from PKCS12 data")
        except Exception as cert_error:
            logger.error(f"Failed to load certificate with provided password: {cert_error!s}")
            raise ValueError("Failed to load certificate with provided password") from cert_error

        if not private_key or not certificate:
            raise ValueError("Failed to load private key or certificate from decoded data")

        return username, password, certificate, private_key

    except Exception as e:
        logger.error(f"Failed to load VetStat certificate/key: {e!s}")
        raise


def get_legacy_fvm_credentials() -> tuple[str, str]:
    """Get FVM credentials from environment variables (legacy simple auth)."""
    username = os.getenv("FVM_USERNAME")
    password = os.getenv("FVM_PASSWORD")

    if not username or not password:
        raise ValueError("FVM_USERNAME/PASSWORD must be set in environment variables")

    return username, password


def create_soap_client(
    wsdl_url: str, username: str, password: str, certificate: Any = None
) -> Client:
    """Create a legacy Zeep SOAP client with UsernameToken-only authentication."""
    session = Session()
    session.verify = certifi.where()

    adapter = requests.adapters.HTTPAdapter()
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    transport = Transport(session=session)
    try:
        client = Client(
            wsdl_url,
            settings=Settings(strict=False, xml_huge_tree=True),
            transport=transport,
            wsse=UsernameToken(username, password),
        )
        logger.info(f"Successfully created SOAP client for {wsdl_url}")
        return client
    except Exception as e:
        logger.error(f"Failed to create SOAP client for {wsdl_url}: {e}")
        raise


def create_signed_soap_client(
    wsdl_url: str, username: str, password: str, certificate: Any, private_key: Any
) -> Client:
    """Create a Zeep SOAP client using FVST's certificate-backed WS-Security model."""
    session = Session()
    session.verify = certifi.where()

    adapter = requests.adapters.HTTPAdapter()
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    transport = Transport(session=session)
    try:
        client = Client(
            wsdl_url,
            settings=Settings(strict=False, xml_huge_tree=True),
            transport=transport,
            wsse=create_fvst_wsse(username, password, certificate, private_key),
        )
        logger.info("Successfully created signed SOAP client for %s", wsdl_url)
        return client
    except Exception as e:
        logger.error("Failed to create signed SOAP client for %s: %s", wsdl_url, e)
        raise


def create_robust_soap_client(endpoint_name: str) -> tuple[Client, str, str, Any, Any]:
    """Create SOAP client with robust certificate-based authentication.

    Returns:
        Tuple of (client, username, password, certificate, private_key)
    """
    if endpoint_name not in ENDPOINTS:
        raise ValueError(f"Unknown endpoint: {endpoint_name}. Available: {list(ENDPOINTS.keys())}")

    wsdl_url = ENDPOINTS[endpoint_name]

    try:
        # Try robust authentication first
        username, password, certificate, private_key = get_fvm_credentials()
        logger.info(f"Using signed FVST authentication for {endpoint_name}")
    except Exception as e:
        if os.getenv("FVM_ALLOW_LEGACY_WSSE", "").lower() != "true":
            raise
        logger.warning(
            "Signed authentication failed for %s; using legacy auth: %s", endpoint_name, e
        )
        username, password = get_legacy_fvm_credentials()
        certificate = None
        private_key = None
        client = create_soap_client(wsdl_url, username, password, certificate)
        return client, username, password, certificate, private_key

    client = create_signed_soap_client(wsdl_url, username, password, certificate, private_key)

    return client, username, password, certificate, private_key


def create_chr_dyr_client() -> Client:
    """Create CHR_dyr SOAP client with robust authentication."""
    client, _, _, _, _ = create_robust_soap_client("chr_dyr")
    return client


def create_stamdata_client() -> Client:
    """Create CHR_stamdata SOAP client with robust authentication."""
    client, _, _, _, _ = create_robust_soap_client("stamdata")
    return client


def create_diko_client() -> Client:
    """Create CHR_diko SOAP client with robust authentication."""
    client, _, _, _, _ = create_robust_soap_client("diko")
    return client


def create_ejendom_client() -> Client:
    """Create CHR_ejendom SOAP client with robust authentication."""
    client, _, _, _, _ = create_robust_soap_client("ejendom")
    return client


def create_besaetning_client() -> Client:
    """Create CHR_besaetning SOAP client with robust authentication."""
    client, _, _, _, _ = create_robust_soap_client("besaetning")
    return client
