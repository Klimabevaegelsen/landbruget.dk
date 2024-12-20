import logging
from typing import Dict, List, Any, Optional
from zeep import Client
from zeep.transports import Transport
from requests import Session
from zeep.wsse.username import UsernameToken
import certifi
from datetime import datetime
import pandas as pd
from ...base import Source
import time
from google.cloud import secretmanager

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logging.getLogger('zeep').setLevel(logging.WARNING)

class HerdDataParser(Source):
    WSDL_URLS = {
        'stamdata': 'https://ws.fvst.dk/service/CHR_stamdataWS?WSDL',
        'besaetning': 'https://ws.fvst.dk/service/CHR_besaetningWS?wsdl',
        'ejendom': 'https://ws.fvst.dk/service/CHR_ejendomWS?wsdl',
        'ejer': 'https://ws.fvst.dk/service/CHR_ejerWS?wsdl',
    }

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        
        # Get credentials from Google Secret Manager
        client = secretmanager.SecretManagerServiceClient()
        project_id = "landbrugsdata-1"
        
        def get_secret(secret_id: str) -> str:
            name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
            response = client.access_secret_version(request={"name": name})
            return response.payload.data.decode("UTF-8")
        
        # Get credentials
        self.username = get_secret('fvm_username')
        self.password = get_secret('fvm_password')
        
        # Initialize session with SSL verification
        session = Session()
        session.verify = certifi.where()
        self.transport = Transport(session=session)
        
        # Initialize clients for different services
        self.clients = {}
        for service_name, url in config['wsdl_urls'].items():
            self.clients[service_name] = Client(
                url,
                transport=self.transport,
                wsse=UsernameToken(self.username, self.password)
            )
        
        logger.info("Initialized CHR web service clients")

    def get_species_usage_combinations(self) -> List[Dict[str, int]]:
        """Get valid species and usage type combinations."""
        try:
            request = {
                'GLRCHRWSInfoInbound': {
                    'BrugerNavn': self.username,
                    'KlientId': 'LandbrugsData',
                    'SessionId': '1',
                    'IPAdresse': '',
                    'TrackID': 'list_combinations'
                },
                'Request': {}
            }
            
            logger.debug("Sending request to list species/usage combinations")
            result = self.clients['stamdata'].service.ListDyrearterMedBrugsarter(request)
            
            combinations = []
            if hasattr(result, 'Response'):
                # The combinations are in the Response array
                for combo in result.Response:
                    combinations.append({
                        'species_code': int(combo.DyreArtKode),
                        'usage_code': int(combo.BrugsArtKode)
                    })
                        
            logger.info(f"Found {len(combinations)} valid species/usage combinations")
            return combinations
            
        except Exception as e:
            logger.error(f"Error getting species/usage combinations: {str(e)}")
            return []

    def get_herd_details(self, herd_number: int, species_code: int) -> Dict[str, Any]:
        """Get detailed information for a specific herd."""
        try:
            request = {
                'GLRCHRWSInfoInbound': {
                    'BrugerNavn': self.username,
                    'KlientId': 'LandbrugsData',
                    'SessionId': '1',
                    'IPAdresse': '',
                    'TrackID': f'herd_details_{herd_number}'
                },
                'Request': {
                    'BesaetningsNummer': herd_number,
                    'DyreArtKode': species_code
                }
            }
            
            logger.debug(f"Fetching details for herd {herd_number}")
            result = self.clients['besaetning'].service.hentStamoplysninger(request)
            
            if not hasattr(result, 'Response'):
                logger.warning(f"No data found for herd {herd_number}")
                return {}
            
            # Parse all available fields from the response
            details = {
                'herd_number': herd_number,
                'species_code': species_code,
                'status': getattr(result.Response, 'Status', None),
                'start_date': getattr(result.Response, 'StartDato', None),
                'end_date': getattr(result.Response, 'SlutDato', None),
                'usage_code': getattr(result.Response, 'BrugsArtKode', None),
                'chk_number': getattr(result.Response, 'CHKNummer', None),
                'property_number': getattr(result.Response, 'EjendomsNummer', None),
                'owner_number': getattr(result.Response, 'EjerNummer', None),
                'user_number': getattr(result.Response, 'BrugerNummer', None),
                'veterinarian_number': getattr(result.Response, 'DyrlaegeNummer', None),
                'fetched_at': datetime.now().isoformat()
            }
            
            # Remove None values
            return {k: v for k, v in details.items() if v is not None}
            
        except Exception as e:
            logger.error(f"Error getting herd details for {herd_number}: {str(e)}")
            return {}

    def get_herds_for_combination(self, species_code: int, usage_code: int) -> List[int]:
        """Get all herd numbers for a species/usage combination with pagination."""
        try:
            all_herds = set()  # Use a set to detect duplicates
            bes_nr_fra = 0
            page = 1
            
            while True:
                request = {
                    'GLRCHRWSInfoInbound': {
                        'BrugerNavn': self.username,
                        'KlientId': 'LandbrugsData',
                        'SessionId': '1',
                        'IPAdresse': '',
                        'TrackID': f'list_herds_{species_code}_{usage_code}_{bes_nr_fra}'
                    },
                    'Request': {
                        'DyreArtKode': species_code,
                        'BrugsArtKode': usage_code,
                        'BesNrFra': bes_nr_fra
                    }
                }
                
                # Keep the arg0= parameter in the service call
                result = self.clients['besaetning'].service.listBesaetningerMedBrugsart(request)
                
                # Check if we have a valid response
                if hasattr(result, '_return') and hasattr(result._return, 'Response'):
                    # Extract herd numbers from response
                    for herd in result._return.Response:
                        if hasattr(herd, 'BesaetningsNummer'):
                            all_herds.add(int(herd.BesaetningsNummer))
                    
                    # Check if more results available
                    has_more = getattr(result._return, 'FlereBesaetninger', False)
                    if has_more:
                        bes_nr_fra = int(result._return.BesNrTil) + 1
                    else:
                        break
                else:
                    break
                
                # Add small delay to avoid overwhelming the service
                time.sleep(0.1)
                
            logger.info(f"Found {len(all_herds)} herds for species {species_code}, usage {usage_code}")
            return list(all_herds)
            
        except Exception as e:
            logger.error(f"Error listing herds for species {species_code}, usage {usage_code}: {str(e)}")
            return []

    def _parse_herd_details(self, response: Any) -> Dict[str, Any]:
        """Parse the herd details response into a structured dictionary."""
        return {
            'herd_number': getattr(response, 'BesaetningsNummer', None),
            'species_code': getattr(response, 'DyreArtKode', None),
            'usage_code': getattr(response, 'BrugsArtKode', None),
            'status': getattr(response, 'Status', None),
            'start_date': getattr(response, 'StartDato', None),
            'end_date': getattr(response, 'SlutDato', None),
            # Add more fields as needed
        }

    def get_all_herds(self) -> List[Dict[str, Any]]:
        """Get all herds by iterating through species/usage combinations."""
        all_herds = []
        
        # Get valid species/usage combinations first
        combinations = self.get_species_usage_combinations()
        
        for combo in combinations:
            try:
                response = self.herd_client.service.listBesaetninger(
                    arg0={
                        'GLRCHRWSInfoInbound': self._get_base_request_info(),
                        'Request': {
                            'DyreArtKode': combo['species_code'],
                            'BrugsArtKode': combo['usage_code']
                        }
                    }
                )
                
                # Parse response and add herds
                batch_herds = self._parse_herd_list_response(response)
                all_herds.extend(batch_herds)
                
                # Add small delay to avoid overwhelming the service
                time.sleep(0.1)
                    
            except Exception as e:
                logger.error(f"Error fetching herds for species {combo['species_code']}, usage {combo['usage_code']}: {e}")
                continue
                    
        return all_herds

    def _parse_herd_list_response(self, response: Any) -> List[Dict[str, Any]]:
        """Parse the herd list response into structured data."""
        herds = []
        
        response_data = getattr(response, 'return', None)
        if not response_data or not hasattr(response_data, 'Response'):
            return herds
        
        for herd in response_data.Response:
            herds.append({
                'herd_number': getattr(herd, 'BesaetningsNummer', None),
                'species_code': getattr(herd, 'DyreArtKode', None),
                'usage_code': getattr(herd, 'BrugsArtKode', None),
                'chk_number': getattr(herd, 'CHKNummer', None),
                'status': getattr(herd, 'Status', None)
            })
        
        return herds

    # Required implementation of abstract methods
    def fetch(self) -> pd.DataFrame:
        """Fetch all herd data and return as DataFrame"""
        herds = []
        
        # Get all valid species/usage combinations
        combinations = self._get_species_usage_combinations()
        
        # Fetch herds for each combination
        for species_code, usage_code in combinations:
            batch_herds = self._fetch_herds_for_combination(species_code, usage_code)
            herds.extend(batch_herds)
        
        # Convert to DataFrame
        df = pd.DataFrame(herds)
        return df

    def sync(self) -> None:
        """Sync herd data to database"""
        df = self.fetch()
        # TODO: Implement database sync
        pass

    def _fetch_herds_for_combination(self, species_code: int, usage_code: int) -> List[Dict[str, Any]]:
        """Fetch all herds for a specific species/usage combination."""
        all_herds = []
        
        try:
            request = {
                'GLRCHRWSInfoInbound': {
                    'KlientId': 'LandbrugsData',
                    'BrugerNavn': self.username,
                    'SessionId': '1',
                    'IPAdresse': '',
                    'TrackID': f'fetch_herds_{species_code}_{usage_code}'
                },
                'Request': {
                    'DyreArtKode': species_code,
                    'BrugsArtKode': usage_code
                }
            }
            
            response = self.herd_client.service.listBesaetninger(arg0=request)
            
            # Parse response and extract herds
            if hasattr(response, '_return') and hasattr(response._return, 'Response'):
                for herd in response._return.Response:
                    herd_data = {
                        'herd_number': getattr(herd, 'BesaetningsNummer', None),
                        'species_code': getattr(herd, 'DyreArtKode', None),
                        'usage_code': getattr(herd, 'BrugsArtKode', None),
                        'chk_number': getattr(herd, 'CHKNummer', None),
                        'status': getattr(herd, 'Status', None)
                    }
                    all_herds.append(herd_data)
            
            # Add small delay to avoid overwhelming the service
            time.sleep(0.1)
            
            return all_herds
            
        except Exception as e:
            logger.error(f"Error fetching herds for species {species_code}, usage {usage_code}: {e}")
            return []