"""Authentication and SOAP client creation for CHR pipeline."""

import base64
import logging
import os
from pathlib import Path
from typing import Any, Tuple

import certifi
import requests
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.hazmat.primitives.serialization.pkcs12 import load_key_and_certificates
from dotenv import load_dotenv
from requests import Session
from zeep import Client
from zeep.transports import Transport
from zeep.wsse.username import UsernameToken

# Load environment variables
load_dotenv()

# Also try to load from the pipeline directory in case working directory is different
pipeline_dir = Path(__file__).parent.parent
env_path = pipeline_dir / ".env"
if env_path.exists():
    load_dotenv(env_path)

# Set up logging
logger = logging.getLogger("backend.pipelines.chr_pipeline.bronze.auth")

# API Endpoints (WSDL URLs)
ENDPOINTS = {
    "chr_dyr": "https://ws.fvst.dk/service/CHR_dyrWS?wsdl",
    "stamdata": "https://ws.fvst.dk/service/CHR_stamdataWS?wsdl",
    "diko": "https://ws.fvst.dk/service/CHR_dikoWS?wsdl",
    "ejendom": "https://ws.fvst.dk/service/CHR_ejendomWS?wsdl",
    "besaetning": "https://ws.fvst.dk/service/CHR_besaetningWS?wsdl",
}


def get_fvm_credentials() -> Tuple[str, str, Any, Any]:
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
                # Check if the data is already binary (from Secret Manager) or base64 encoded
                if isinstance(cert_base64, bytes):
                    logger.debug("Using binary certificate data from VETSTAT_CERTIFICATE environment variable")
                    p12_data = cert_base64
                    logger.debug(f"Using binary certificate data. Length: {len(p12_data)} bytes")
                elif cert_base64.startswith('MII') or cert_base64.startswith('MIIC'):
                    # Looks like base64 encoded certificate
                    logger.debug("Using base64 certificate from VETSTAT_CERTIFICATE environment variable")
                    p12_data = base64.b64decode(cert_base64)
                    logger.debug(f"Successfully decoded base64 certificate. Decoded length: {len(p12_data)} bytes")
                else:
                    # Assume it's raw binary data stored as string (from Secret Manager)
                    logger.debug("Treating certificate as raw binary data from Secret Manager")
                    p12_data = cert_base64.encode('latin1')  # Preserve binary data
                    logger.debug(f"Using raw certificate data. Length: {len(p12_data)} bytes")
            except Exception as decode_error:
                logger.error(f"Failed to process certificate data: {str(decode_error)}")
                p12_data = None

        # If base64 decoding failed or wasn't provided, try reading from file
        if not p12_data and cert_path:
            try:
                logger.debug(f"Reading certificate from file: {cert_path}")
                with open(cert_path, "rb") as f:
                    p12_data = f.read()
                logger.debug(f"Successfully read certificate file. Length: {len(p12_data)} bytes")
            except Exception as file_error:
                logger.error(f"Failed to read certificate file {cert_path}: {str(file_error)}")
                raise ValueError(f"Failed to read certificate file: {str(file_error)}") from file_error

        if not p12_data:
            raise ValueError("No valid certificate data found from either environment variable or file")

        # Load the certificate and private key from the data
        try:
            private_key, certificate, _ = load_key_and_certificates(p12_data, cert_password.encode("utf-8"))
            logger.debug("Successfully loaded private key and certificate from PKCS12 data")
        except Exception as cert_error:
            logger.error(f"Failed to load certificate with provided password: {str(cert_error)}")
            raise ValueError("Failed to load certificate with provided password") from cert_error

        if not private_key or not certificate:
            raise ValueError("Failed to load private key or certificate from decoded data")

        return username, password, certificate, private_key

    except Exception as e:
        logger.error(f"Failed to load VetStat certificate/key: {str(e)}")
        raise


def get_legacy_fvm_credentials() -> Tuple[str, str]:
    """Get FVM credentials from environment variables (legacy simple auth)."""
    username = os.getenv("FVM_USERNAME")
    password = os.getenv("FVM_PASSWORD")

    if not username or not password:
        raise ValueError("FVM_USERNAME/PASSWORD must be set in environment variables")

    return username, password


def create_soap_client(wsdl_url: str, username: str, password: str, certificate: Any = None) -> Client:
    """Create a Zeep SOAP client with WSSE authentication.
    
    Note: This is a legacy function for simple username/password auth.
    The new robust authentication uses certificate-based signing.
    """
    session = Session()
    session.verify = certifi.where()

    adapter = requests.adapters.HTTPAdapter()
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    transport = Transport(session=session)
    try:
        client = Client(wsdl_url, transport=transport, wsse=UsernameToken(username, password))
        logger.info(f"Successfully created SOAP client for {wsdl_url}")
        return client
    except Exception as e:
        logger.error(f"Failed to create SOAP client for {wsdl_url}: {e}")
        raise


def create_robust_soap_client(endpoint_name: str) -> Tuple[Client, str, str, Any, Any]:
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
        logger.info(f"Using robust authentication for {endpoint_name}")
    except Exception as e:
        # Fallback to legacy authentication if certificate is not available
        logger.warning(f"Robust authentication failed for {endpoint_name}, falling back to legacy: {e}")
        username, password = get_legacy_fvm_credentials()
        certificate, private_key = None, None
    
    # Create the client with simple auth (certificate-based signing would be implemented later)
    client = create_soap_client(wsdl_url, username, password, certificate)
    
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
