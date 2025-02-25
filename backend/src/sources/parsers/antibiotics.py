"""Parser for VetStat antibiotic data."""
import base64
import datetime
import hashlib
import secrets
import requests
import uuid
import logging
from typing import Dict, List, Any, Optional
from datetime import timedelta
from lxml import etree
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.hazmat.primitives.serialization.pkcs12 import load_key_and_certificates
from copy import deepcopy
import pandas as pd
from google.cloud import secretmanager
from ..base import BaseSource
from .chr_species import CHRSpeciesParser
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)

namespaces = {
    'soapenv': 'http://schemas.xmlsoap.org/soap/envelope/',
    'wsse': 'http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd',
    'wsu': 'http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd',
    'ds': 'http://www.w3.org/2000/09/xmldsig#',
    'ec': 'http://www.w3.org/2001/10/xml-exc-c14n#',
    'eks': 'http://vetstat.fvst.dk/ekstern',
    'glr': 'http://www.logica.com/glrchr'
}

class VetStatAntibioticsParser(BaseSource):
    """Parser for VetStat antibiotic data."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        
        # Get credentials from Google Secret Manager
        client = secretmanager.SecretManagerServiceClient()
        project_id = "landbrugsdata-1"
        
        def get_secret(secret_id: str) -> str:
            name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
            response = client.access_secret_version(request={"name": name})
            return response.payload.data.decode("UTF-8") if secret_id != "vetstat-certificate" else response.payload.data
        
        # Get credentials
        self.username = get_secret('fvm_username')
        self.password = get_secret('fvm_password')
        self.p12_data = get_secret('vetstat-certificate')
        self.p12_password = get_secret('vetstat-certificate-password')
        
        logger.info("Initialized VetStat antibiotics parser")

    @property
    def source_id(self) -> str:
        """Return the source ID."""
        return "antibiotics"

    def compute_digest(self, element, inclusive_prefixes):
        """Canonicalize (C14N) the element and compute its SHA-256 digest in Base64."""
        c14n_bytes = etree.tostring(
            element,
            method="c14n",
            exclusive=True,
            inclusive_ns_prefixes=inclusive_prefixes,
            with_comments=False
        )
        sha256_hash = hashlib.sha256(c14n_bytes).digest()
        return base64.b64encode(sha256_hash).decode()

    def get_element_prefixes(self, element_type):
        """Get the appropriate namespace prefixes based on element type."""
        prefix_mappings = {
            'Body': ["ds", "ec", "eks", "glr", "wsse"],
            'Timestamp': ["wsse", "ds", "ec", "eks", "glr", "soapenv", "wsse"],
            'UsernameToken': ["ds", "ec", "eks", "glr", "soapenv", "wsse"],
            'BinarySecurityToken': [],
            'SecurityTokenReference': ["wsse", "ds", "ec", "eks", "glr", "soapenv"],
            'Signature': ["ds", "ec", "eks", "glr", "soapenv", "wsse", "wsu"],
            'SignedInfo': ["ds", "ec", "eks", "glr", "soapenv", "wsse", "wsu"],
        }
        return prefix_mappings.get(element_type, ["ds", "ec", "eks", "glr", "wsse"])

    def generate_uuid_id(self, prefix):
        """Generate a UUID-based ID with a specific prefix."""
        return f"{prefix}{uuid.uuid4().hex.upper()}"

    def replace_all_ids(self, root):
        """Replace all IDs in the document with new UUIDs and track the replacements."""
        id_mappings = {}
        
        # Find all elements with wsu:Id or Id attributes
        for element in root.xpath("//*[@wsu:Id or @Id]", namespaces=namespaces):
            # Handle wsu:Id
            wsu_id = element.get(f"{{{namespaces['wsu']}}}Id")
            if wsu_id:
                prefix = wsu_id.split('-')[0] + '-'
                new_id = self.generate_uuid_id(prefix)
                id_mappings[wsu_id] = new_id
                element.set(f"{{{namespaces['wsu']}}}Id", new_id)
            
            # Handle plain Id
            plain_id = element.get('Id')
            if plain_id:
                prefix = plain_id.split('-')[0] + '-'
                new_id = self.generate_uuid_id(prefix)
                id_mappings[plain_id] = new_id
                element.set('Id', new_id)
        
        # Update all references
        for ref in root.xpath("//ds:Reference|//wsse:Reference", namespaces=namespaces):
            uri = ref.get('URI')
            if uri and uri.lstrip('#') in id_mappings:
                ref.set('URI', f"#{id_mappings[uri.lstrip('#')]}")
        
        return id_mappings

    def update_security_elements(self, root, username, password, certificate):
        """Update all security-related elements in the XML."""
        now_utc = datetime.datetime.utcnow()
        expires_utc = now_utc + timedelta(hours=1)
        
        # Update BinarySecurityToken
        binary_token = root.find(".//wsse:BinarySecurityToken", namespaces)
        if binary_token is not None:
            binary_token.text = base64.b64encode(certificate.public_bytes(Encoding.DER)).decode()

        # Update Username and Password
        username_token = root.find(".//wsse:UsernameToken", namespaces)
        if username_token is not None:
            user_el = username_token.find("./wsse:Username", namespaces)
            pass_el = username_token.find("./wsse:Password", namespaces)
            if user_el is not None:
                user_el.text = username
            if pass_el is not None:
                pass_el.text = password

        # Update Nonce
        nonce_el = root.find(".//wsse:Nonce", namespaces)
        if nonce_el is not None:
            nonce_el.text = base64.b64encode(secrets.token_bytes(16)).decode()

        # Update Timestamps
        created_str = now_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        expires_str = expires_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        for created_el in root.xpath("//wsu:Created", namespaces=namespaces):
            created_el.text = created_str
        for expires_el in root.xpath("//wsu:Expires", namespaces=namespaces):
            expires_el.text = expires_str

    def update_references_and_digests(self, root):
        """Update all reference URIs and their corresponding digests."""
        references = root.xpath("//ds:Reference", namespaces=namespaces)
        
        for ref in references:
            uri = ref.get('URI')
            if uri:
                id_value = uri.lstrip('#')
                referenced_element = root.xpath(f"//*[@wsu:Id='{id_value}' or @Id='{id_value}']", namespaces=namespaces)
                
                if referenced_element:
                    element = referenced_element[0]
                    element_type = element.tag.split('}')[-1]
                    prefixes = self.get_element_prefixes(element_type)
                    new_digest = self.compute_digest(element, prefixes)
                    
                    digest_value = ref.find('./ds:DigestValue', namespaces=namespaces)
                    if digest_value is not None:
                        digest_value.text = new_digest

    def sign_document(self, root, private_key):
        """Sign the document with the provided private key."""
        signed_info = root.find(".//ds:SignedInfo", namespaces)
        if signed_info is not None:
            signed_info_c14n = etree.tostring(
                signed_info,
                method="c14n",
                exclusive=True,
                inclusive_ns_prefixes=["ds", "ec", "eks", "glr", "soapenv", "wsse", "wsu"],
                with_comments=False
            )
            
            signature = private_key.sign(
                signed_info_c14n,
                padding.PKCS1v15(),
                hashes.SHA1()
            )
            
            signature_value = root.find(".//ds:SignatureValue", namespaces=namespaces)
            if signature_value is not None:
                signature_value.text = base64.b64encode(signature).decode()

    def create_soap_envelope(self, username, password, certificate, chr_number, periode_fra, periode_til, species_code):
        """Create SOAP envelope with exact whitespace control."""
        
        # Generate IDs that will be referenced multiple times
        binary_token_id = self.generate_uuid_id("X509-")
        username_token_id = self.generate_uuid_id("UsernameToken-")
        timestamp_id = self.generate_uuid_id("TS-")
        signature_id = self.generate_uuid_id("SIG-")
        body_id = f"id-{uuid.uuid4().hex.upper()}"
        key_info_id = self.generate_uuid_id("KI-")
        str_id = self.generate_uuid_id("STR-")
        
        # Current timestamp and expiry
        now = datetime.datetime.utcnow()
        expires = now + timedelta(hours=1)
        created_str = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        expires_str = expires.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        
        # Store the XML parts in a list for better control
        xml_parts = []
        
        # Start building the XML with exact whitespace
        xml_parts.extend([
            '<soapenv:Envelope xmlns:ds="http://www.w3.org/2000/09/xmldsig#" ',
            'xmlns:ec="http://www.w3.org/2001/10/xml-exc-c14n#" ',
            'xmlns:eks="http://vetstat.fvst.dk/ekstern" ',
            'xmlns:glr="http://www.logica.com/glrchr" ',
            'xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" ',
            'xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd" ',
            'xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">',
            '\n  <soapenv:Header>',
            '\n    <wsse:Security>',
            '\n      <wsse:BinarySecurityToken EncodingType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary" ',
            'ValueType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-x509-token-profile-1.0#X509v3" ',
            f'wsu:Id="{binary_token_id}">',
            base64.b64encode(certificate.public_bytes(Encoding.DER)).decode(),
            '</wsse:BinarySecurityToken>',
            f'\n      <wsse:UsernameToken wsu:Id="{username_token_id}">',
            f'\n        <wsse:Username>{username}</wsse:Username>',
            '\n        <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">',
            f'{password}</wsse:Password>',
            '\n        <wsse:Nonce EncodingType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary">',
            f'{base64.b64encode(secrets.token_bytes(16)).decode()}</wsse:Nonce>',
            f'\n        <wsu:Created>{created_str}</wsu:Created>',
            '\n      </wsse:UsernameToken>',
            f'\n      <wsu:Timestamp wsu:Id="{timestamp_id}">',
            f'\n        <wsu:Created>{created_str}</wsu:Created>',
            f'\n        <wsu:Expires>{expires_str}</wsu:Expires>',
            '\n      </wsu:Timestamp>',
            f'\n      <ds:Signature Id="{signature_id}">',
            '\n        <ds:SignedInfo>',
            '\n          <ds:CanonicalizationMethod Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#">',
            '\n            <ec:InclusiveNamespaces PrefixList="ds ec eks glr soapenv wsse wsu"/>',
            '\n          </ds:CanonicalizationMethod>',
            '\n          <ds:SignatureMethod Algorithm="http://www.w3.org/2000/09/xmldsig#rsa-sha1"/>',
            f'\n          <ds:Reference URI="#{body_id}">',
            '\n            <ds:Transforms>',
            '\n              <ds:Transform Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#">',
            '\n                <ec:InclusiveNamespaces PrefixList="ds ec eks glr wsse"/>',
            '\n              </ds:Transform>',
            '\n            </ds:Transforms>',
            '\n            <ds:DigestMethod Algorithm="http://www.w3.org/2001/04/xmlenc#sha256"/>',
            '\n            <ds:DigestValue></ds:DigestValue>',
            '\n          </ds:Reference>',
            f'\n          <ds:Reference URI="#{timestamp_id}">',
            '\n            <ds:Transforms>',
            '\n              <ds:Transform Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#">',
            '\n                <ec:InclusiveNamespaces PrefixList="wsse ds ec eks glr soapenv wsse"/>',
            '\n              </ds:Transform>',
            '\n            </ds:Transforms>',
            '\n            <ds:DigestMethod Algorithm="http://www.w3.org/2001/04/xmlenc#sha256"/>',
            '\n            <ds:DigestValue></ds:DigestValue>',
            '\n          </ds:Reference>',
            f'\n          <ds:Reference URI="#{username_token_id}">',
            '\n            <ds:Transforms>',
            '\n              <ds:Transform Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#">',
            '\n                <ec:InclusiveNamespaces PrefixList="ds ec eks glr soapenv wsse"/>',
            '\n              </ds:Transform>',
            '\n            </ds:Transforms>',
            '\n            <ds:DigestMethod Algorithm="http://www.w3.org/2001/04/xmlenc#sha256"/>',
            '\n            <ds:DigestValue></ds:DigestValue>',
            '\n          </ds:Reference>',
            f'\n          <ds:Reference URI="#{binary_token_id}">',
            '\n            <ds:Transforms>',
            '\n              <ds:Transform Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#">',
            '\n                <ec:InclusiveNamespaces PrefixList=""/>',
            '\n              </ds:Transform>',
            '\n            </ds:Transforms>',
            '\n            <ds:DigestMethod Algorithm="http://www.w3.org/2001/04/xmlenc#sha256"/>',
            '\n            <ds:DigestValue></ds:DigestValue>',
            '\n          </ds:Reference>',
            '\n        </ds:SignedInfo>',
            '\n        <ds:SignatureValue></ds:SignatureValue>',
            f'\n        <ds:KeyInfo Id="{key_info_id}">',
            f'\n          <wsse:SecurityTokenReference wsu:Id="{str_id}">',
            f'\n            <wsse:Reference URI="#{binary_token_id}" ',
            'ValueType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-x509-token-profile-1.0#X509v3"/>',
            '\n          </wsse:SecurityTokenReference>',
            '\n        </ds:KeyInfo>',
            '\n      </ds:Signature>',
            '\n    </wsse:Security>',
            '\n  </soapenv:Header>',
            f'\n  <soapenv:Body wsu:Id="{body_id}">',
            '\n    <eks:VetStat_CHRHentAntibiotikaForbrugRequest>',
            '\n      <glr:GLRCHRWSInfoInbound>',
            '\n        <glr:KlientId>?</glr:KlientId>',
            f'\n        <glr:BrugerNavn>{username}</glr:BrugerNavn>',
            '\n        <glr:SessionId>?</glr:SessionId>',
            '\n        <glr:IPAdresse>?</glr:IPAdresse>',
            '\n        <glr:TrackID>?</glr:TrackID>',
            '\n      </glr:GLRCHRWSInfoInbound>',
            '\n      <eks:Request>',
            f'\n        <glr:DyreArtKode>{species_code}</glr:DyreArtKode>',
            f'\n        <eks:PeriodeFra>{periode_fra}</eks:PeriodeFra>',
            f'\n        <eks:PeriodeTil>{periode_til}</eks:PeriodeTil>',
            f'\n        <eks:CHRNummer>{chr_number}</eks:CHRNummer>',
            '\n      </eks:Request>',
            '\n    </eks:VetStat_CHRHentAntibiotikaForbrugRequest>',
            '\n  </soapenv:Body>',
            '\n</soapenv:Envelope>'
        ])
        
        # Join all parts and parse the result
        xml_string = ''.join(xml_parts)
        return etree.fromstring(xml_string.encode('utf-8'))

    def process_soap_request(self, chr_number: int, periode_fra: str, periode_til: str, species_code: int) -> Optional[Dict[str, Any]]:
        """Process the SOAP request with new UUIDs for all IDs."""
        try:
            # Load certificate and private key
            private_key, certificate, _ = load_key_and_certificates(self.p12_data, self.p12_password.encode())
            
            # Create envelope from scratch
            root = self.create_soap_envelope(self.username, self.password, certificate, chr_number, periode_fra, periode_til, species_code)
            
            # Update references and digests
            self.update_references_and_digests(root)
            
            # Sign the document
            self.sign_document(root, private_key)
            
            # Convert to string
            modified_xml = etree.tostring(root, pretty_print=True, encoding='unicode')
            
            # Send request
            response = requests.post(
                "https://vetstat.fvst.dk/vetstat/services/external/CHRWS",
                data=modified_xml,
                headers={
                    "Content-Type": "text/xml;charset=UTF-8",
                    "SOAPAction": "http://vetstat.fvst.dk/chr/hentAntibiotikaforbrug"
                }
            )
            
            if response.status_code != 200:
                logger.error(f"Error response from VetStat API: {response.status_code} - {response.text}")
                return None
                
            # Parse response
            response_root = etree.fromstring(response.content)
            
            # Add VetStat namespace
            namespaces['ns2'] = 'http://vetstat.fvst.dk/ekstern'
            
            # Extract data from response
            data_elements = response_root.findall('.//ns2:Data', namespaces)
            if not data_elements:
                logger.warning(f"No data found in response for CHR {chr_number}")
                return None
            
            results = []
            for data in data_elements:
                result = {
                    'chr_number': chr_number,
                    'year': self.safe_int(data.findtext('.//ns2:Aar', namespaces=namespaces)),
                    'month': self.safe_int(data.findtext('.//ns2:Maaned', namespaces=namespaces)),
                    'species_code': self.safe_int(data.findtext('.//DyreArtKode', namespaces=namespaces)),
                    'species_text': self.safe_str(data.findtext('.//DyreArtTekst', namespaces=namespaces)),
                    'age_group_code': self.safe_int(data.findtext('.//ns2:Aldersgruppekode', namespaces=namespaces)),
                    'age_group': self.safe_str(data.findtext('.//ns2:Aldersgruppe', namespaces=namespaces)),
                    'rolling_9m_avg': self.safe_float(data.findtext('.//ns2:Rul9MdrGns', namespaces=namespaces)),
                    'rolling_12m_avg': self.safe_float(data.findtext('.//ns2:Rul12MdrGns', namespaces=namespaces)),
                    'cvr_number': self.safe_str(data.findtext('.//ns2:CVRNummer', namespaces=namespaces)),
                    'municipality_code': self.safe_int(data.findtext('.//ns2:Kommunenr', namespaces=namespaces)),
                    'municipality_name': self.safe_str(data.findtext('.//ns2:Kommunenavn', namespaces=namespaces)),
                    'region_code': self.safe_int(data.findtext('.//ns2:Regionsnr', namespaces=namespaces)),
                    'region_name': self.safe_str(data.findtext('.//ns2:Regionsnavn', namespaces=namespaces)),
                    'animal_days': self.safe_int(data.findtext('.//ns2:Dyredage', namespaces=namespaces)),
                    'animal_doses': self.safe_float(data.findtext('.//ns2:Dyredoser', namespaces=namespaces)),
                    'add_per_100_animals_per_day': self.safe_float(data.findtext('.//ns2:ADDPer100DyrPerDag', namespaces=namespaces)),
                    'threshold_value': self.safe_float(data.findtext('.//ns2:Graensevaerdi', namespaces=namespaces)),
                    'period_from': periode_fra,
                    'period_to': periode_til
                }
                results.append({k: v for k, v in result.items() if v is not None})
            
            return results
            
        except Exception as e:
            logger.error(f"Error processing SOAP request for CHR {chr_number}: {str(e)}", exc_info=True)
            return None

    def safe_int(self, value: Any) -> Optional[int]:
        """Safely convert value to int, return None if not possible."""
        if value is None:
            return None
        try:
            val_str = str(value).strip()
            return int(val_str) if val_str else None
        except (ValueError, TypeError, AttributeError):
            return None

    def safe_str(self, value: Any) -> Optional[str]:
        """Safely convert value to string, return None if empty."""
        if value is None:
            return None
        try:
            val_str = str(value).strip()
            return val_str if val_str else None
        except (ValueError, TypeError, AttributeError):
            return None

    def safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float, return None if not possible."""
        if value is None:
            return None
        try:
            val_str = str(value).strip().replace(',', '.')
            return float(val_str) if val_str else None
        except (ValueError, TypeError, AttributeError):
            return None

    async def process_species(self, species_code: int) -> Optional[pd.DataFrame]:
        """Process antibiotic data for a given species code."""
        try:
            # Get CHR numbers for this species code
            logger.info(f"Getting CHR numbers for species code {species_code}")
            species_parser = CHRSpeciesParser(self.config)
            chr_numbers_df = await species_parser.get_chr_numbers_async(species_code)
            
            if chr_numbers_df.empty:
                logger.warning(f"No CHR numbers found for species code {species_code}")
                return None
            
            # Calculate period for last 12 completed months
            today = datetime.date.today()
            last_day_prev_month = today.replace(day=1) - timedelta(days=1)
            first_day = (last_day_prev_month - relativedelta(months=11)).replace(day=1)
            
            logger.info(f"Fetching data for period: {first_day} to {last_day_prev_month}")
            
            # Process each CHR number
            results = []
            for chr_number in chr_numbers_df['chr_number'].unique():
                try:
                    result = self.process_soap_request(
                        chr_number=chr_number,
                        periode_fra=first_day.isoformat(),
                        periode_til=last_day_prev_month.isoformat(),
                        species_code=species_code
                    )
                    if result:
                        results.append(result)
                        
                except Exception as e:
                    logger.error(f"Error processing CHR number {chr_number}: {str(e)}", exc_info=True)
                    continue
            
            if results:
                df = pd.DataFrame(results)
                await self.store(df, f'antibiotics_{species_code}')
                return df
            
            return None
            
        except Exception as e:
            logger.error(f"Error processing species code {species_code}: {str(e)}", exc_info=True)
            return None

    async def fetch(self) -> pd.DataFrame:
        """Fetch antibiotic data from VetStat."""
        try:
            # First get species data
            species_parser = CHRSpeciesParser(self.config)
            species_data = await species_parser.get_species_usage_combinations_async()
            
            if species_data.empty:
                logger.error("Failed to fetch species data")
                return pd.DataFrame()
            
            # Process each species code
            results = []
            for species_code in species_data['species_code'].unique():
                logger.info(f"Processing species code: {species_code}")
                result = await self.process_species(species_code)
                if result is not None:
                    results.append(result)
            
            # Combine all results
            if results:
                return pd.concat(results, ignore_index=True)
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Error during fetch: {str(e)}")
            return pd.DataFrame()

    def fetch_sync(self) -> pd.DataFrame:
        """Synchronous version of fetch."""
        import asyncio
        return asyncio.run(self.fetch())

    async def sync(self) -> Optional[int]:
        """Full sync process: fetch and store"""
        try:
            df = await self.fetch()
            if await self.store(df):
                return len(df)
            return None
        except Exception as e:
            logger.error(f"Sync failed for {self.source_id}: {str(e)}")
            return None