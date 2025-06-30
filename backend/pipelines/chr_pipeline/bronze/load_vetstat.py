"""Module for loading VetStat antibiotics data - Bronze Layer."""

import base64
import hashlib
import logging
import os
import secrets
import uuid
from datetime import date, datetime, timedelta
from typing import Any, List, Optional, Tuple

import requests
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.hazmat.primitives.serialization.pkcs12 import load_key_and_certificates
from dotenv import load_dotenv
from lxml import etree

# Import the exporter function
from .export import save_raw_data

# Set up logging
logger = logging.getLogger("backend.pipelines.chr_pipeline.bronze.load_vetstat")

# Load environment variables
load_dotenv()

# Get Google Cloud Project ID from environment variable
GOOGLE_CLOUD_PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
if not GOOGLE_CLOUD_PROJECT_ID:
    logger.warning("GOOGLE_CLOUD_PROJECT environment variable is not set")

# Secret Manager Secret IDs
FVM_USERNAME_SECRET_ID = "fvm_username"
FVM_PASSWORD_SECRET_ID = "fvm_password"
VETSTAT_CERTIFICATE_SECRET_ID = "vetstat-certificate"
VETSTAT_CERTIFICATE_PASSWORD_SECRET_ID = "vetstat-certificate-password"

# API Endpoints
VETSTAT_ENDPOINT = "https://vetstat.fvst.dk/vetstat/services/external/CHRWS"
SOAP_ACTION = "http://vetstat.fvst.dk/chr/hentAntibiotikaforbrug"

# Default Client ID
DEFAULT_CLIENT_ID = "LandbrugsData"

# XML Namespaces
NAMESPACES = {
    "soapenv": "http://schemas.xmlsoap.org/soap/envelope/",
    "wsse": "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd",
    "wsu": "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd",
    "ds": "http://www.w3.org/2000/09/xmldsig#",
    "ec": "http://www.w3.org/2001/10/xml-exc-c14n#",
    "eks": "http://vetstat.fvst.dk/ekstern",
    "glr": "http://www.logica.com/glrchr",
}

# --- Credential Handling ---


def get_vetstat_credentials() -> Tuple[str, str, Any, Any]:
    """Get FVM username, password, VetStat certificate, and private key."""
    # Load environment variables from .env file if it exists
    load_dotenv()

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
                logger.debug("Using base64 certificate from VETSTAT_CERTIFICATE environment variable")
                p12_data = base64.b64decode(cert_base64)
                logger.debug(f"Successfully decoded base64 certificate. Decoded length: {len(p12_data)} bytes")
            except Exception as decode_error:
                logger.error(f"Failed to decode base64 certificate: {str(decode_error)}")
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


# --- XML Helper Functions (Adapted from fetch_chr_details.py) ---


def compute_digest(element: etree._Element, inclusive_prefixes: List[str]) -> str:
    """Canonicalize (C14N) the element and compute its SHA-256 digest in Base64."""
    # Ensure the element is detached if necessary or use a copy
    try:
        c14n_bytes = etree.tostring(
            element, method="c14n", exclusive=True, inclusive_ns_prefixes=inclusive_prefixes, with_comments=False
        )
        sha256_hash = hashlib.sha256(c14n_bytes).digest()
        return base64.b64encode(sha256_hash).decode()
    except Exception as e:
        logger.error(f"Error during C14N or digest computation for element {element.tag}: {e}")
        # Log the problematic element for debugging
        logger.debug(f"Problematic Element Tag: {element.tag}")  # Simplified log message
        raise


def get_element_prefixes(element_type: str) -> List[str]:
    """Get the appropriate namespace prefixes based on element type for C14N."""
    # These prefixes were likely determined by observing successful requests
    # or documentation for the VetStat WS-Security profile.
    prefix_mappings = {
        "Body": ["ds", "ec", "eks", "glr", "wsse"],  # Adjusted based on sample
        "Timestamp": ["wsse", "ds", "ec", "eks", "glr", "soapenv"],  # Adjusted
        "UsernameToken": ["ds", "ec", "eks", "glr", "soapenv", "wsse"],  # Adjusted
        "BinarySecurityToken": [],  # Typically no inclusive prefixes for the token itself
        # The following might need adjustment based on the exact signature structure
        "SecurityTokenReference": ["wsse", "ds", "ec", "eks", "glr", "soapenv"],
        "Signature": ["ds", "ec", "eks", "glr", "soapenv", "wsse", "wsu"],
        "SignedInfo": ["ds", "ec", "eks", "glr", "soapenv", "wsse", "wsu"],
    }
    # Defaulting to a common set if not specifically mapped
    return prefix_mappings.get(element_type, ["ds", "ec", "eks", "glr", "wsse"])


def generate_uuid_id(prefix: str) -> str:
    """Generate a UUID-based ID with a specific prefix."""
    return f"{prefix}{uuid.uuid4().hex.upper()}"


def update_security_elements(root: etree._Element, username: str, password: str, certificate: Any):
    """Update WS-Security elements: Timestamps, Nonce, Username, Password, BinarySecurityToken."""
    now_utc = datetime.utcnow()
    expires_utc = now_utc + timedelta(hours=1)  # Standard 1-hour expiry
    created_str = now_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    expires_str = expires_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    # Update BinarySecurityToken value
    binary_token = root.find(".//wsse:BinarySecurityToken", NAMESPACES)
    if binary_token is not None:
        binary_token.text = base64.b64encode(certificate.public_bytes(Encoding.DER)).decode()
    else:
        logger.warning("BinarySecurityToken element not found in template.")

    # Update Username and Password
    username_token = root.find(".//wsse:UsernameToken", NAMESPACES)
    if username_token is not None:
        user_el = username_token.find("./wsse:Username", NAMESPACES)
        pass_el = username_token.find("./wsse:Password", NAMESPACES)
        if user_el is not None:
            user_el.text = username
        if pass_el is not None:
            pass_el.text = password
    else:
        logger.warning("UsernameToken element not found in template.")

    # Update Nonce
    nonce_el = root.find(".//wsse:Nonce", NAMESPACES)
    if nonce_el is not None:
        nonce_el.text = base64.b64encode(secrets.token_bytes(16)).decode()
    else:
        logger.warning("Nonce element not found in template.")

    # Update Timestamps (Created and Expires)
    # Need to update *both* wsu:Created and wsu:Expires within the Timestamp element
    ts_created_el = root.find(".//wsu:Timestamp/wsu:Created", NAMESPACES)
    ts_expires_el = root.find(".//wsu:Timestamp/wsu:Expires", NAMESPACES)
    if ts_created_el is not None:
        ts_created_el.text = created_str
    if ts_expires_el is not None:
        ts_expires_el.text = expires_str
    # Also update the Created element within UsernameToken if it exists
    ut_created_el = root.find(".//wsse:UsernameToken/wsu:Created", NAMESPACES)
    if ut_created_el is not None:
        ut_created_el.text = created_str


def update_references_and_digests(root: etree._Element):
    """Update all ds:Reference URIs and their corresponding ds:DigestValue."""
    references = root.xpath("//ds:Reference", namespaces=NAMESPACES)
    if not references:
        logger.warning("No ds:Reference elements found to update.")
        return

    logger.info(f"Updating digests for {len(references)} references.")
    for ref in references:
        uri = ref.get("URI")
        if not uri or not uri.startswith("#"):
            logger.warning(f"Skipping reference with invalid or missing URI: {uri}")
            continue

        id_value = uri.lstrip("#")
        # Search for the element by its wsu:Id or Id attribute
        referenced_element = root.xpath(f"//*[@wsu:Id='{id_value}' or @Id='{id_value}']", namespaces=NAMESPACES)

        if referenced_element:
            element = referenced_element[0]
            # Extract local name for prefix lookup
            element_type = etree.QName(element.tag).localname
            prefixes = get_element_prefixes(element_type)
            logger.debug(f"Calculating digest for URI {uri} ({element.tag}) using prefixes: {prefixes}")
            try:
                new_digest = compute_digest(element, prefixes)
                digest_value_el = ref.find("./ds:DigestValue", NAMESPACES)
                if digest_value_el is not None:
                    digest_value_el.text = new_digest
                    logger.debug(f"Updated DigestValue for {uri} to: {new_digest[:10]}...")
                else:
                    logger.warning(f"ds:DigestValue element not found within reference for URI: {uri}")
            except Exception as e:
                logger.error(f"Failed to compute or set digest for URI {uri}: {e}")
        else:
            logger.warning(f"Referenced element not found for URI: {uri}")


def sign_document(root: etree._Element, private_key: Any):
    """Calculate and insert the ds:SignatureValue based on the ds:SignedInfo."""
    signed_info = root.find(".//ds:SignedInfo", NAMESPACES)
    if signed_info is None:
        logger.error("ds:SignedInfo element not found. Cannot sign document.")
        raise ValueError("SignedInfo element is missing")

    signature_value_el = root.find(".//ds:SignatureValue", NAMESPACES)
    if signature_value_el is None:
        logger.error("ds:SignatureValue element not found. Cannot insert signature.")
        raise ValueError("SignatureValue element is missing")

    logger.info("Canonicalizing SignedInfo and generating signature...")
    try:
        # Use the specific inclusive prefixes required for SignedInfo canonicalization
        signed_info_prefixes = get_element_prefixes("SignedInfo")
        signed_info_c14n = etree.tostring(
            signed_info, method="c14n", exclusive=True, inclusive_ns_prefixes=signed_info_prefixes, with_comments=False
        )

        # VetStat uses RSA-SHA1 for the signature
        signature = private_key.sign(
            signed_info_c14n,
            padding.PKCS1v15(),
            hashes.SHA1(),  # IMPORTANT: VetStat requires SHA1 for the signature itself
        )

        encoded_signature = base64.b64encode(signature).decode()
        signature_value_el.text = encoded_signature
        logger.info("Successfully generated and inserted signature")

    except Exception:
        logger.error("Error during signing process")
        raise


# --- SOAP Envelope Creation ---


def create_soap_envelope_template(
    username: str, chr_number: int, periode_fra: str, periode_til: str, species_code: int
) -> etree._Element:
    """Create the basic structure of the SOAP envelope with placeholders and IDs."""
    # Generate dynamic IDs for security elements
    binary_token_id = generate_uuid_id("X509-")
    username_token_id = generate_uuid_id("UsernameToken-")
    timestamp_id = generate_uuid_id("TS-")
    signature_id = generate_uuid_id("SIG-")
    body_id = f"id-{uuid.uuid4().hex.upper()}"
    key_info_id = generate_uuid_id("KI-")
    str_id = generate_uuid_id("STR-")

    # Construct the XML string template
    # Using f-strings carefully, ensuring proper escaping if needed (though not complex here)
    # Pay close attention to namespaces and prefixes matching the NAMESPACES dict
    xml_template = f"""
<soapenv:Envelope xmlns:ds="http://www.w3.org/2000/09/xmldsig#" xmlns:ec="http://www.w3.org/2001/10/xml-exc-c14n#" xmlns:eks="http://vetstat.fvst.dk/ekstern" xmlns:glr="http://www.logica.com/glrchr" xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
  <soapenv:Header>
    <wsse:Security>
      <wsse:BinarySecurityToken EncodingType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary" ValueType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-x509-token-profile-1.0#X509v3" wsu:Id="{binary_token_id}">PLACEHOLDER_CERT</wsse:BinarySecurityToken>
      <wsse:UsernameToken wsu:Id="{username_token_id}">
        <wsse:Username>PLACEHOLDER_USER</wsse:Username>
        <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">PLACEHOLDER_PASS</wsse:Password>
        <wsse:Nonce EncodingType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary">PLACEHOLDER_NONCE</wsse:Nonce>
        <wsu:Created>PLACEHOLDER_CREATED</wsu:Created>
      </wsse:UsernameToken>
      <wsu:Timestamp wsu:Id="{timestamp_id}">
        <wsu:Created>PLACEHOLDER_CREATED</wsu:Created>
        <wsu:Expires>PLACEHOLDER_EXPIRES</wsu:Expires>
      </wsu:Timestamp>
      <ds:Signature Id="{signature_id}">
        <ds:SignedInfo>
          <ds:CanonicalizationMethod Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#">
            <ec:InclusiveNamespaces PrefixList="ds ec eks glr soapenv wsse wsu"/>
          </ds:CanonicalizationMethod>
          <ds:SignatureMethod Algorithm="http://www.w3.org/2000/09/xmldsig#rsa-sha1"/>
          <ds:Reference URI="#{body_id}">
            <ds:Transforms>
              <ds:Transform Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#">
                <ec:InclusiveNamespaces PrefixList="ds ec eks glr wsse"/>
              </ds:Transform>
            </ds:Transforms>
            <ds:DigestMethod Algorithm="http://www.w3.org/2001/04/xmlenc#sha256"/>
            <ds:DigestValue>PLACEHOLDER_DIGEST_BODY</ds:DigestValue>
          </ds:Reference>
          <ds:Reference URI="#{timestamp_id}">
            <ds:Transforms>
              <ds:Transform Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#">
                <ec:InclusiveNamespaces PrefixList="wsse ds ec eks glr soapenv"/>
              </ds:Transform>
            </ds:Transforms>
            <ds:DigestMethod Algorithm="http://www.w3.org/2001/04/xmlenc#sha256"/>
            <ds:DigestValue>PLACEHOLDER_DIGEST_TS</ds:DigestValue>
          </ds:Reference>
          <ds:Reference URI="#{username_token_id}">
            <ds:Transforms>
              <ds:Transform Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#">
                <ec:InclusiveNamespaces PrefixList="ds ec eks glr soapenv wsse"/>
              </ds:Transform>
            </ds:Transforms>
            <ds:DigestMethod Algorithm="http://www.w3.org/2001/04/xmlenc#sha256"/>
            <ds:DigestValue>PLACEHOLDER_DIGEST_UT</ds:DigestValue>
          </ds:Reference>
          <ds:Reference URI="#{binary_token_id}">
            <ds:Transforms>
              <ds:Transform Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#">
                <ec:InclusiveNamespaces PrefixList=""/>
              </ds:Transform>
            </ds:Transforms>
            <ds:DigestMethod Algorithm="http://www.w3.org/2001/04/xmlenc#sha256"/>
            <ds:DigestValue>PLACEHOLDER_DIGEST_BST</ds:DigestValue>
          </ds:Reference>
        </ds:SignedInfo>
        <ds:SignatureValue>PLACEHOLDER_SIGNATURE</ds:SignatureValue>
        <ds:KeyInfo Id="{key_info_id}">
          <wsse:SecurityTokenReference wsu:Id="{str_id}">
            <wsse:Reference URI="#{binary_token_id}" ValueType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-x509-token-profile-1.0#X509v3"/>
          </wsse:SecurityTokenReference>
        </ds:KeyInfo>
      </ds:Signature>
    </wsse:Security>
  </soapenv:Header>
  <soapenv:Body wsu:Id="{body_id}">
    <eks:VetStat_CHRHentAntibiotikaForbrugRequest>
      <glr:GLRCHRWSInfoInbound>
        <glr:KlientId>{DEFAULT_CLIENT_ID}</glr:KlientId>
        <glr:BrugerNavn>{username}</glr:BrugerNavn>
        <glr:SessionId>1</glr:SessionId>
        <glr:IPAdresse></glr:IPAdresse>
        <glr:TrackID>{generate_uuid_id('vetstat_request-')}</glr:TrackID>
      </glr:GLRCHRWSInfoInbound>
      <eks:Request>
        <glr:DyreArtKode>{species_code}</glr:DyreArtKode>
        <eks:PeriodeFra>{periode_fra}</eks:PeriodeFra>
        <eks:PeriodeTil>{periode_til}</eks:PeriodeTil>
        <eks:CHRNummer>{chr_number}</eks:CHRNummer>
      </eks:Request>
    </eks:VetStat_CHRHentAntibiotikaForbrugRequest>
  </soapenv:Body>
</soapenv:Envelope>
"""
    # Parse the string into an lxml Element object
    try:
        # Remove insignificant whitespace during parsing to avoid issues with C14N
        parser = etree.XMLParser(remove_blank_text=True)
        root = etree.fromstring(xml_template.encode("utf-8"), parser=parser)
        return root
    except etree.XMLSyntaxError as e:
        logger.error(f"XML Syntax Error in template: {e}")
        logger.error(f"Template content:\n{xml_template}")
        raise


# --- Main Loading Function ---


def load_vetstat_antibiotics(chr_number: int, species_code: int, period_from: date, period_to: date) -> Optional[str]:
    """Fetch raw antibiotics data XML from VetStat for a given CHR, species, and period."""
    logger.info(
        f"Preparing VetStat request for CHR: {chr_number}, Species: {species_code}, Period: {period_from} to {period_to}"
    )

    try:
        # 1. Get Credentials (including cert/key)
        username, password, certificate, private_key = get_vetstat_credentials()

        # 2. Create SOAP Envelope Template
        root = create_soap_envelope_template(
            username, chr_number, period_from.isoformat(), period_to.isoformat(), species_code
        )

        # 3. Update Security Elements (Timestamps, Nonce, User/Pass, Cert)
        update_security_elements(root, username, password, certificate)

        # 4. Update References and Calculate Digests
        # Ensure this happens *after* updating the elements being referenced
        update_references_and_digests(root)

        # 5. Sign the Document (Calculate and Insert SignatureValue)
        sign_document(root, private_key)

        # 6. Serialize the final XML
        # Use unicode encoding for direct use with requests, which handles byte encoding.
        signed_xml_string = etree.tostring(root, pretty_print=False, encoding="unicode")
        logger.debug("Successfully prepared signed VetStat SOAP request.")

        # 7. Send Request via requests library
        headers = {"Content-Type": "text/xml;charset=UTF-8", "SOAPAction": SOAP_ACTION}
        logger.debug(f"Sending request to {VETSTAT_ENDPOINT}")
        response = requests.post(VETSTAT_ENDPOINT, data=signed_xml_string, headers=headers)

        # 8. Handle Response
        if response.status_code == 200:
            logger.info(f"Successfully fetched VetStat data for CHR: {chr_number}")
            raw_xml_response = response.text

            # Parse the XML response to extract the data
            try:
                # Parse the XML response
                root = etree.fromstring(raw_xml_response.encode("utf-8"))

                # Define the namespaces used in the XML
                ns = {"soap": "http://schemas.xmlsoap.org/soap/envelope/", "ns2": "http://vetstat.fvst.dk/ekstern"}

                # Extract the data from the XML
                data_elements = root.xpath("//ns2:Data", namespaces=ns)
                logger.info(f"Found {len(data_elements)} data elements in XML response for CHR {chr_number}")

                if data_elements:
                    # Convert the XML elements to JSON objects
                    json_data = []
                    for element in data_elements:
                        # Extract all child elements directly
                        item = {}

                        # Process all direct child elements
                        for child in element:
                            tag = etree.QName(child.tag).localname
                            if child.text and child.text.strip():
                                item[tag] = child.text.strip()

                        # Make sure CHR number is included
                        if "CHRNummer" not in item and str(chr_number):
                            item["CHRNummer"] = str(chr_number)

                        # Make sure species code is included
                        if "DyreArtKode" not in item and str(species_code):
                            item["DyreArtKode"] = str(species_code)

                        # Only add items that have data
                        if item:
                            json_data.append(item)
                            logger.info(f"Extracted data item with keys: {list(item.keys())}")

                    # Save both the raw XML and the parsed JSON
                    save_raw_data(
                        raw_response=raw_xml_response,
                        data_type="vetstat_antibiotics",
                        identifier=f"{chr_number}_{species_code}",
                    )

                    # Also save the parsed JSON data
                    for item in json_data:
                        save_raw_data(
                            raw_response=item,
                            data_type="vetstat_antibiotics",
                            identifier=f"{chr_number}_{species_code}",
                        )

                    logger.info(f"Parsed {len(json_data)} antibiotic usage records from XML response")
                else:
                    logger.warning(f"No antibiotic usage data found in XML response for CHR {chr_number}")
                    # Save just the raw XML
                    save_raw_data(
                        raw_response=raw_xml_response,
                        data_type="vetstat_antibiotics",
                        identifier=f"{chr_number}_{species_code}",
                    )
            except Exception as e:
                logger.error(f"Failed to parse XML response for CHR {chr_number}: {e}")
                # Save the raw XML response even if parsing fails
                save_raw_data(
                    raw_response=raw_xml_response,
                    data_type="vetstat_antibiotics",
                    identifier=f"{chr_number}_{species_code}",
                )

            return raw_xml_response
        elif response.status_code == 500:
            # This is a normal response when there's no data
            logger.error(f"VetStat request failed for CHR {chr_number}: HTTP 500")
            return None
        else:
            logger.error(f"Unexpected response from VetStat API for CHR {chr_number}: {response.status_code}")
            logger.error(f"Response content:\n{response.text}")
            return None

    except Exception as e:
        logger.error(f"Failed to execute VetStat request for CHR {chr_number}: {e}")
        return None


# Remove all test functions and test execution code at the end
if __name__ == "__main__":
    # Use CHR and Species identified previously
    TEST_CHR_NUMBER = 28400
    TEST_SPECIES_CODE = 15  # Pigs

    # Define a test period (e.g., first 3 months of 2023)
    TEST_PERIOD_FROM = date(2023, 1, 1)
    TEST_PERIOD_TO = date(2023, 3, 31)

    # Call the main loading function
    vetstat_response_xml = load_vetstat_antibiotics(
        TEST_CHR_NUMBER, TEST_SPECIES_CODE, TEST_PERIOD_FROM, TEST_PERIOD_TO
    )

    if vetstat_response_xml:
        logger.info("Successfully received VetStat response XML.")
    else:
        logger.warning("Failed to retrieve VetStat response.")
