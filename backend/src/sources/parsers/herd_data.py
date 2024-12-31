import logging
from typing import Dict, List, Any, Optional
from zeep import Client
from zeep.transports import Transport
from requests import Session
from zeep.wsse.username import UsernameToken
import certifi
from datetime import datetime
import pandas as pd
from ..base import BaseSource
import time
import os
from google.cloud import secretmanager
from pyproj import Transformer
import asyncio

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logging.getLogger('zeep').setLevel(logging.WARNING)

class HerdDataParser(BaseSource):
    """Parser for CHR herd data"""
    
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
        for service_name, url in self.WSDL_URLS.items():
            self.clients[service_name] = Client(
                url,
                transport=self.transport,
                wsse=UsernameToken(self.username, self.password)
            )
        
        logger.info("Initialized CHR web service clients")

    @property
    def source_id(self) -> str:
        return "herd_data"

    async def fetch(self) -> pd.DataFrame:
        """Fetch data from the CHR web services"""
        return self.fetch_sync()  # Call the sync version

    def fetch_sync(self) -> pd.DataFrame:
        """Synchronous version of fetch"""
        results = []
        total_combinations = 0
        current_combination = 0
        
        try:
            # Track unique entities to avoid duplicates
            seen_properties = set()
            seen_owners = set()
            seen_users = set()
            seen_practices = set()
            
            # Data containers
            properties_data = []
            owners_data = []
            users_data = []
            practices_data = []
            herds_data = []
            
            # 1. Fetch stamdata (reference data)
            logger.info("Fetching stamdata (reference data)...")
            combinations = self.get_species_usage_combinations()
            if not combinations:
                logger.error("No species/usage combinations found")
                return pd.DataFrame()
                
            total_combinations = len(combinations)
            logger.info(f"Found {total_combinations} species/usage combinations to process")
            
            stamdata_df = pd.DataFrame(combinations)
            stamdata_df.columns = [c.lower() for c in stamdata_df.columns]
            self._upload_to_storage(stamdata_df, 'stamdata')
            results.append(('stamdata', len(stamdata_df)))
            
            # 2. Fetch all other data
            logger.info("Fetching herd and related data...")
            batch_size = 1000
            processed = 0
            
            for combo in combinations:
                current_combination += 1
                species_code = combo['species_code']
                usage_code = combo['usage_code']
                
                logger.info(f"Processing combination {current_combination}/{total_combinations}: "
                          f"species={species_code} ({combo['species_text']}), "
                          f"usage={usage_code} ({combo['usage_text']})")
                
                try:
                    # For testing, limit to 100 herds per combination
                    herds = self.get_herds_for_combination(species_code, usage_code, limit=100)
                    if not herds:
                        logger.info(f"No herds found for species {species_code}, usage {usage_code}")
                        continue
                        
                    total_herds = len(herds)
                    logger.info(f"Found {total_herds} herds for species {species_code}, usage {usage_code}")
                    
                    for herd_idx, herd_number in enumerate(herds, 1):
                        try:
                            logger.info(f"[{current_combination}/{total_combinations}] "
                                      f"Processing herd {herd_idx}/{total_herds}: {herd_number} "
                                      f"(species={species_code}, usage={usage_code})")
                            
                            details = self.get_herd_details(herd_number, species_code)
                            if not details:
                                logger.warning(f"No details found for herd {herd_number}")
                                continue
                            
                            chr_number = details.get('chr_number')
                            if not chr_number:
                                continue
                                
                            # Extract base herd data with animal counts
                            if 'herd_sizes' in details and details['herd_sizes']:
                                for size in details['herd_sizes']:
                                    if not size['type'].lower().startswith('i alt'):  # Skip total rows
                                        herd_row = {
                                            # Primary key
                                            'herd_number': details.get('herd_number'),
                                            'chr_number': chr_number,
                                            
                                            # Herd details
                                            'species_code': details.get('species_code'),
                                            'species_text': details.get('species_text'),
                                            'usage_code': details.get('usage_code'),
                                            'usage_text': details.get('usage_text'),
                                            'business_type_code': details.get('business_type_code'),
                                            'business_type': details.get('business_type'),
                                            'herd_type_code': details.get('herd_type_code'),
                                            'herd_type': details.get('herd_type'),
                                            'trade_code': details.get('trade_code'),
                                            'trade_text': details.get('trade_text'),
                                            'is_organic': details.get('is_organic'),
                                            'herd_status': details.get('herd_status'),
                                            
                                            # Dates
                                            'creation_date': details.get('creation_date'),
                                            'last_update': details.get('last_update'),
                                            'end_date': details.get('end_date'),
                                            'last_size_update': details.get('last_size_update'),
                                            'fetched_at': details.get('fetched_at'),
                                            
                                            # Animal counts
                                            'animal_type': size['type'],
                                            'count': size['size']
                                        }
                                        herds_data.append(herd_row)
                            
                            # Handle property details
                            if chr_number not in seen_properties:
                                property_details = self.get_property_details(chr_number)
                                if property_details:
                                    # Convert coordinates
                                    coords = property_details.get('stable_coordinates_utm32', {})
                                    if coords:
                                        lat, lon = self._convert_coordinates(coords)
                                        if lat and lon:
                                            property_details['latitude'] = lat
                                            property_details['longitude'] = lon
                                    
                                    # Add primary key
                                    property_details['chr_number'] = chr_number
                                    properties_data.append(property_details)
                                    seen_properties.add(chr_number)
                            
                            # Handle owner info
                            if 'owner' in details and details['owner']:
                                owner = details['owner']
                                owner_id = owner.get('cvr_number') or owner.get('cpr_number')
                                if owner_id:
                                    owner_key = f"{owner_id}_{chr_number}"
                                    if owner_key not in seen_owners:
                                        owners_data.append({
                                            # Primary keys
                                            'cvr_number': owner.get('cvr_number'),
                                            'cpr_number': owner.get('cpr_number'),
                                            'chr_number': chr_number,  # Link back to property
                                            
                                            # Contact info
                                            'name': owner.get('name'),
                                            'address': owner.get('address'),
                                            'city': owner.get('city'),
                                            'postal_code': owner.get('postal_code'),
                                            'postal_district': owner.get('postal_district'),
                                            'municipality_code': owner.get('municipality_code'),
                                            'municipality_name': owner.get('municipality_name'),
                                            'country': owner.get('country'),
                                            'phone': owner.get('phone'),
                                            'mobile': owner.get('mobile'),
                                            'email': owner.get('email'),
                                            
                                            # Privacy flags
                                            'address_protection': owner.get('address_protection', 0),
                                            'advertising_protection': owner.get('advertising_protection', 0)
                                        })
                                        seen_owners.add(owner_key)
                            
                            # Handle user info
                            if 'user' in details and details['user']:
                                user = details['user']
                                user_id = user.get('cvr_number') or user.get('cpr_number')
                                if user_id:
                                    user_key = f"{user_id}_{chr_number}"
                                    if user_key not in seen_users:
                                        users_data.append({
                                            # Primary keys
                                            'cvr_number': user.get('cvr_number'),
                                            'cpr_number': user.get('cpr_number'),
                                            'chr_number': chr_number,  # Link back to property
                                            
                                            # Contact info
                                            'name': user.get('name'),
                                            'address': user.get('address'),
                                            'city': user.get('city'),
                                            'postal_code': user.get('postal_code'),
                                            'postal_district': user.get('postal_district'),
                                            'municipality_code': user.get('municipality_code'),
                                            'municipality_name': user.get('municipality_name'),
                                            'country': user.get('country'),
                                            'phone': user.get('phone'),
                                            'mobile': user.get('mobile'),
                                            'email': user.get('email'),
                                            
                                            # Privacy flags
                                            'address_protection': user.get('address_protection', 0),
                                            'advertising_protection': user.get('advertising_protection', 0)
                                        })
                                        seen_users.add(user_key)
                            
                            # Handle veterinary practice info
                            if 'veterinary_practice' in details and details['veterinary_practice']:
                                practice = details['veterinary_practice']
                                practice_id = practice.get('number')
                                if practice_id:
                                    practice_key = f"{practice_id}_{chr_number}"
                                    if practice_key not in seen_practices:
                                        practices_data.append({
                                            # Primary key
                                            'number': practice_id,
                                            'chr_number': chr_number,  # Link back to property
                                            
                                            # Practice details
                                            'name': practice.get('name'),
                                            'address': practice.get('address'),
                                            'city': practice.get('city'),
                                            'postal_code': practice.get('postal_code'),
                                            'postal_district': practice.get('postal_district'),
                                            'phone': practice.get('phone'),
                                            'mobile': practice.get('mobile'),
                                            'email': practice.get('email')
                                        })
                                        seen_practices.add(practice_key)
                            
                            processed += 1
                            
                            # Upload batches
                            if processed % batch_size == 0:
                                self._upload_batch_data(herds_data, properties_data, owners_data, users_data, practices_data)
                                herds_data = []
                                properties_data = []
                                owners_data = []
                                users_data = []
                                practices_data = []
                            
                        except Exception as e:
                            logger.error(f"Error processing herd {herd_number}: {str(e)}")
                            continue
                
                except Exception as e:
                    logger.error(f"Error processing combination {current_combination}/{total_combinations}: {str(e)}")
                    continue
            
            # Upload remaining data
            if any([herds_data, properties_data, owners_data, users_data, practices_data]):
                self._upload_batch_data(herds_data, properties_data, owners_data, users_data, practices_data)
            
            return pd.DataFrame(results, columns=['table_name', 'record_count'])
            
        except Exception as e:
            logger.error(f"Error in herd data sync: {str(e)}", exc_info=True)
            raise

    async def sync(self) -> Optional[int]:
        """Sync herd data to Cloud Storage"""
        try:
            logger.info("Starting herd data sync...")
            
            # Get all valid species/usage combinations
            combinations = self.get_species_usage_combinations()
            if not combinations:
                logger.warning("No species/usage combinations found")
                return 0
            
            logger.info(f"Found {len(combinations)} species/usage combinations to process")
            
            # Track all unique entities across all combinations
            all_herds = set()
            seen_properties = set()
            seen_owners = set()
            seen_users = set()
            seen_practices = set()
            
            # Data containers
            herds_data = []      # Basic herd info with animal counts
            properties_data = [] # Property details
            owners_data = []     # Owner information
            users_data = []      # User information
            practices_data = []  # Veterinary practice information
            
            # Process combinations in chunks
            chunk_size = 5  # Process 5 combinations at a time
            for i in range(0, len(combinations), chunk_size):
                chunk = combinations[i:i + chunk_size]
                
                # Process each combination in parallel
                tasks = []
                for combo in chunk:
                    species_code = combo['species_code']
                    usage_code = combo['usage_code']
                    
                    # Get herds for this combination
                    herd_numbers = self.get_herds_for_combination(species_code, usage_code)
                    if not herd_numbers:
                        continue
                    
                    # Process herds in parallel
                    for herd_number in herd_numbers:
                        if herd_number in all_herds:
                            continue
                        
                        # Create task for processing this herd
                        task = asyncio.create_task(self._process_herd(
                            herd_number, 
                            species_code, 
                            all_herds,
                            seen_properties,
                            seen_owners,
                            seen_users,
                            seen_practices,
                            herds_data,
                            properties_data,
                            owners_data,
                            users_data,
                            practices_data
                        ))
                        tasks.append(task)
                        
                        # Process in batches of 50 herds
                        if len(tasks) >= 50:
                            await asyncio.gather(*tasks)
                            tasks = []
                            
                            # Upload data if we have enough
                            if len(herds_data) >= 5000:
                                self._upload_batch_data(herds_data, properties_data, owners_data, users_data, practices_data)
                                herds_data = []
                                properties_data = []
                                owners_data = []
                                users_data = []
                                practices_data = []
                
                # Process any remaining tasks
                if tasks:
                    await asyncio.gather(*tasks)
            
            # Upload any remaining data
            if any([herds_data, properties_data, owners_data, users_data, practices_data]):
                self._upload_batch_data(herds_data, properties_data, owners_data, users_data, practices_data)
            
            logger.info("Herd data sync completed successfully")
            return len(all_herds)
            
        except Exception as e:
            logger.error(f"Error in herd data sync: {str(e)}")
            raise

    async def _process_herd(self, herd_number: int, species_code: int,
                          all_herds: set, seen_properties: set, seen_owners: set,
                          seen_users: set, seen_practices: set,
                          herds_data: list, properties_data: list,
                          owners_data: list, users_data: list, practices_data: list) -> None:
        """Process a single herd asynchronously."""
        try:
            # Get herd details
            details = self.get_herd_details(herd_number, species_code)
            if not details:
                return
            
            chr_number = details.get('chr_number')
            if not chr_number:
                return
            
            # Extract base herd data with animal counts
            if 'herd_sizes' in details and details['herd_sizes']:
                for size in details['herd_sizes']:
                    if not size['type'].lower().startswith('i alt'):
                        herd_row = {
                            'herd_number': details.get('herd_number'),
                            'chr_number': chr_number,
                            'species_code': details.get('species_code'),
                            'species_text': details.get('species_text'),
                            'usage_code': details.get('usage_code'),
                            'usage_text': details.get('usage_text'),
                            'business_type_code': details.get('business_type_code'),
                            'business_type': details.get('business_type'),
                            'herd_type_code': details.get('herd_type_code'),
                            'herd_type': details.get('herd_type'),
                            'trade_code': details.get('trade_code'),
                            'trade_text': details.get('trade_text'),
                            'is_organic': details.get('is_organic'),
                            'herd_status': details.get('herd_status'),
                            'creation_date': details.get('creation_date'),
                            'last_update': details.get('last_update'),
                            'end_date': details.get('end_date'),
                            'last_size_update': details.get('last_size_update'),
                            'fetched_at': details.get('fetched_at'),
                            'animal_type': size['type'],
                            'count': size['size']
                        }
                        herds_data.append(herd_row)
            
            # Handle property details
            if chr_number not in seen_properties:
                property_details = self.get_property_details(chr_number)
                if property_details:
                    properties_data.append(property_details)
                    seen_properties.add(chr_number)
            
            # Handle owner info
            if 'owner' in details and details['owner']:
                owner = details['owner']
                owner_id = owner.get('cvr_number') or owner.get('cpr_number')
                if owner_id:
                    owner_key = f"{owner_id}_{chr_number}"
                    if owner_key not in seen_owners:
                        owners_data.append({
                            'cvr_number': owner.get('cvr_number'),
                            'cpr_number': owner.get('cpr_number'),
                            'chr_number': chr_number,
                            'name': owner.get('name'),
                            'address': owner.get('address'),
                            'city': owner.get('city'),
                            'postal_code': owner.get('postal_code'),
                            'postal_district': owner.get('postal_district'),
                            'municipality_code': owner.get('municipality_code'),
                            'municipality_name': owner.get('municipality_name'),
                            'country': owner.get('country'),
                            'phone': owner.get('phone'),
                            'mobile': owner.get('mobile'),
                            'email': owner.get('email'),
                            'address_protection': owner.get('address_protection', 0),
                            'advertising_protection': owner.get('advertising_protection', 0)
                        })
                        seen_owners.add(owner_key)
            
            # Handle user info
            if 'user' in details and details['user']:
                user = details['user']
                user_id = user.get('cvr_number') or user.get('cpr_number')
                if user_id:
                    user_key = f"{user_id}_{chr_number}"
                    if user_key not in seen_users:
                        users_data.append({
                            'cvr_number': user.get('cvr_number'),
                            'cpr_number': user.get('cpr_number'),
                            'chr_number': chr_number,
                            'name': user.get('name'),
                            'address': user.get('address'),
                            'city': user.get('city'),
                            'postal_code': user.get('postal_code'),
                            'postal_district': user.get('postal_district'),
                            'municipality_code': user.get('municipality_code'),
                            'municipality_name': user.get('municipality_name'),
                            'country': user.get('country'),
                            'phone': user.get('phone'),
                            'mobile': user.get('mobile'),
                            'email': user.get('email'),
                            'address_protection': user.get('address_protection', 0),
                            'advertising_protection': user.get('advertising_protection', 0)
                        })
                        seen_users.add(user_key)
            
            # Handle veterinary practice info
            if 'veterinary_practice' in details and details['veterinary_practice']:
                practice = details['veterinary_practice']
                practice_id = practice.get('number')
                if practice_id:
                    practice_key = f"{practice_id}_{chr_number}"
                    if practice_key not in seen_practices:
                        practices_data.append({
                            'number': practice_id,
                            'chr_number': chr_number,
                            'name': practice.get('name'),
                            'address': practice.get('address'),
                            'city': practice.get('city'),
                            'postal_code': practice.get('postal_code'),
                            'postal_district': practice.get('postal_district'),
                            'phone': practice.get('phone'),
                            'mobile': practice.get('mobile'),
                            'email': practice.get('email')
                        })
                        seen_practices.add(practice_key)
            
            # Mark herd as processed
            all_herds.add(herd_number)
            
        except Exception as e:
            logger.error(f"Error processing herd {herd_number}: {str(e)}")
            return

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
            
            # Debug: Print raw response structure
            logger.info(f"Raw response attributes: {dir(result)}")
            if hasattr(result, 'Response'):
                logger.info(f"Response attributes: {dir(result.Response)}")
                # Log the first few combinations to understand structure
                for i, combo in enumerate(result.Response[:5]):
                    logger.info(f"Sample combination {i}: species={getattr(combo, 'DyreArtKode', None)}, "
                              f"species_text={getattr(combo, 'DyreArtTekst', None)}, "
                              f"usage={getattr(combo, 'BrugsArtKode', None)}, "
                              f"usage_text={getattr(combo, 'BrugsArtTekst', None)}")
            
            combinations = []
            if hasattr(result, 'Response') and result.Response:
                # The combinations are in the Response array
                for combo in result.Response:
                    species_code = getattr(combo, 'DyreArtKode', None)
                    species_text = getattr(combo, 'DyreArtTekst', '')
                    usage_code = getattr(combo, 'BrugsArtKode', None)
                    usage_text = getattr(combo, 'BrugsArtTekst', '')
                    
                    if species_code is not None and usage_code is not None:
                        try:
                            species_int = int(species_code)
                            usage_int = int(usage_code)
                            combinations.append({
                                'species_code': species_int,
                                'species_text': species_text,
                                'usage_code': usage_int,
                                'usage_text': usage_text
                            })
                            logger.debug(f"Added combination: species={species_int} ({species_text}), "
                                       f"usage={usage_int} ({usage_text})")
                        except (ValueError, TypeError) as e:
                            logger.warning(f"Invalid combination found: species={species_code}, usage={usage_code}: {e}")
                    else:
                        logger.warning(f"Incomplete combination found: species={species_code}, usage={usage_code}")
                        
            logger.info(f"Found {len(combinations)} valid species/usage combinations")
            return combinations
            
        except Exception as e:
            logger.error(f"Error getting species/usage combinations: {str(e)}")
            return []

    def safe_str(self, value: Any) -> Optional[str]:
        """Safely convert value to string, return None if empty"""
        if value is None:
            return None
        try:
            val_str = str(value).strip()
            return val_str if val_str else None
        except (ValueError, TypeError, AttributeError):
            return None
            
    def safe_int(self, value: Any) -> Optional[int]:
        """Safely convert value to int, return None if not possible"""
        if value is None:
            return None
        try:
            val_str = str(value).strip()
            return int(val_str) if val_str else None
        except (ValueError, TypeError, AttributeError):
            return None

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
                    'BesaetningsNummer': str(herd_number),
                    'DyreArtKode': str(species_code)
                }
            }
            
            logger.debug(f"Fetching details for herd {herd_number}")
            result = self.clients['besaetning'].service.hentStamoplysninger(request)
            
            if not hasattr(result, 'Response') or not result.Response:
                logger.warning(f"No data found for herd {herd_number}")
                return {}
            
            # Response is a list with one item containing a Besaetning object
            response_data = result.Response[0].Besaetning
            
            # Parse all available fields from the response
            details = {
                # Basic identification
                'herd_number': herd_number,  # We already know this is valid
                'chr_number': self.safe_int(getattr(response_data, 'ChrNummer', None)),
                
                # Species and usage information
                'species_code': species_code,  # We already know this is valid
                'species_text': self.safe_str(getattr(response_data, 'DyreArtTekst', None)),
                'usage_code': self.safe_int(getattr(response_data, 'BrugsArtKode', None)),
                'usage_text': self.safe_str(getattr(response_data, 'BrugsArtTekst', None)),
                
                # Business information
                'business_type_code': self.safe_int(getattr(response_data, 'VirksomhedsArtKode', None)),
                'business_type': self.safe_str(getattr(response_data, 'VirksomhedsArtTekst', None)),
                'herd_type_code': self.safe_int(getattr(response_data, 'BesaetningsTypeKode', None)),
                'herd_type': self.safe_str(getattr(response_data, 'BesaetningsTypeTekst', None)),
                'trade_code': self.safe_int(getattr(response_data, 'OmsaetningsKode', None)),
                'trade_text': self.safe_str(getattr(response_data, 'OmsaetningsTekst', None)),
                'is_organic': int(getattr(response_data, 'Oekologisk', 'Nej') == 'Ja'),
                'herd_status': self.safe_str(getattr(response_data, 'BesaetningsStatus', None)),
                
                # Dates
                'creation_date': self._parse_date(getattr(response_data, 'DatoOpret', None)),
                'last_update': self._parse_date(getattr(response_data, 'DatoOpdatering', None)),
                'end_date': self._parse_date(getattr(response_data, 'DatoOphoer', None)),
                'last_size_update': self._parse_date(getattr(response_data, 'BesStrDatoAjourfoert', None)),
                
                # Related entities
                'owner': self._parse_person_info(getattr(response_data, 'Ejer', None)),
                'user': self._parse_person_info(getattr(response_data, 'Bruger', None)),
                'veterinary_practice': self._parse_practice_info(getattr(response_data, 'BesPraksis', None)),
                'delivery_declarations': self._parse_delivery_declarations(getattr(response_data, 'LeveringsErklaeringer', None)),
                
                'fetched_at': datetime.now().isoformat()
            }
            
            # Parse herd sizes if available
            if hasattr(response_data, 'BesStr'):
                details['herd_sizes'] = []
                for size_info in response_data.BesStr:
                    size_type = self.safe_str(getattr(size_info, 'BesaetningsStoerrelseTekst', None))
                    size_value = getattr(size_info, 'BesaetningsStoerrelse', None)
                    if size_type and size_value is not None:
                        size_int = self.safe_int(size_value)
                        if size_int is not None and not size_type.lower().startswith('i alt'):
                            details['herd_sizes'].append({
                                'type': size_type,
                                'size': size_int
                            })
            
            # Remove None values
            return {k: v for k, v in details.items() if v is not None}
            
        except Exception as e:
            logger.error(f"Error getting herd details for {herd_number}: {str(e)}")
            return {}

    def _parse_date(self, date_value: Any) -> Optional[str]:
        """Parse date value to ISO format string"""
        if not date_value:
            return None
            
        try:
            if isinstance(date_value, datetime):
                return date_value.date().isoformat()
                
            if isinstance(date_value, str):
                # The API returns dates in YYYY-MM-DD format
                return date_value
                
            return None
            
        except Exception as e:
            logger.error(f"Error parsing date {date_value}: {str(e)}")
            return None

    def _parse_person_info(self, person_data: Any) -> Optional[Dict[str, Any]]:
        """Parse person (owner/user) information."""
        if not person_data:
            return None
            
        info = {
            'cpr_number': self.safe_str(getattr(person_data, 'CprNummer', None)),
            'cvr_number': self.safe_str(getattr(person_data, 'CvrNummer', None)),
            'name': self.safe_str(getattr(person_data, 'Navn', None)),
            'address': self.safe_str(getattr(person_data, 'Adresse', None)),
            'city': self.safe_str(getattr(person_data, 'ByNavn', None)),
            'postal_code': self.safe_str(getattr(person_data, 'PostNummer', None)),
            'postal_district': self.safe_str(getattr(person_data, 'PostDistrikt', None)),
            'municipality_code': self.safe_int(getattr(person_data, 'KommuneNummer', None)),
            'municipality_name': self.safe_str(getattr(person_data, 'KommuneNavn', None)),
            'country': self.safe_str(getattr(person_data, 'Land', None)),
            'phone': self.safe_str(getattr(person_data, 'TelefonNummer', None)),
            'mobile': self.safe_str(getattr(person_data, 'MobilNummer', None)),
            'email': self.safe_str(getattr(person_data, 'Email', None)),
            'address_protection': int(getattr(person_data, 'Adressebeskyttelse', 'Nej') == 'Ja'),
            'advertising_protection': int(getattr(person_data, 'Reklamebeskyttelse', 'Nej') == 'Ja')
        }
        
        return {k: v for k, v in info.items() if v is not None}
    
    def _parse_practice_info(self, practice_data: Any) -> Optional[Dict[str, Any]]:
        """Parse veterinary practice information."""
        if not practice_data:
            return None
            
        info = {
            'number': self.safe_int(getattr(practice_data, 'PraksisNr', None)),
            'name': self.safe_str(getattr(practice_data, 'PraksisNavn', None)),
            'address': self.safe_str(getattr(practice_data, 'PraksisAdresse', None)),
            'city': self.safe_str(getattr(practice_data, 'PraksisByNavn', None)),
            'postal_code': self.safe_str(getattr(practice_data, 'PraksisPostNummer', None)),
            'postal_district': self.safe_str(getattr(practice_data, 'PraksisPostDistrikt', None)),
            'phone': self.safe_str(getattr(practice_data, 'PraksisTelefonNummer', None)),
            'mobile': self.safe_str(getattr(practice_data, 'PraksisMobilNummer', None)),
            'email': self.safe_str(getattr(practice_data, 'PraksisEmail', None))
        }
        
        return {k: v for k, v in info.items() if v is not None}
    
    def _parse_delivery_declarations(self, declarations_data: Any) -> Optional[List[Dict[str, Any]]]:
        """Parse delivery declarations information."""
        if not declarations_data or not hasattr(declarations_data, 'LeveringsErklaering'):
            return None
            
        declarations = []
        for decl in declarations_data.LeveringsErklaering:
            info = {
                'sender_chr_number': self.safe_int(getattr(decl, 'ChrNummerAfsender', None)),
                'sender_herd_number': self.safe_int(getattr(decl, 'BesaetningsNummerAfsender', None)),
                'receiver_chr_number': self.safe_int(getattr(decl, 'ChrNummerModtager', None)),
                'receiver_herd_number': self.safe_int(getattr(decl, 'BesaetningsNummerModtager', None)),
                'own_production': int(getattr(decl, 'EgenProduktion', 'Nej') == 'Ja'),
                'breeding_only': int(getattr(decl, 'KunAvlsdyr', 'Nej') == 'Ja'),
                'group_delivery': int(getattr(decl, 'IndgaarIGruppevisLevering', 'Nej') == 'Ja'),
                'start_date': self._parse_date(getattr(decl, 'DatoStart', None)),
                'end_date': self._parse_date(getattr(decl, 'DatoSlut', None))
            }
            declarations.append({k: v for k, v in info.items() if v is not None})
            
        return declarations if declarations else None

    def get_herds_for_combination(self, species_code: int, usage_code: int, limit: Optional[int] = None) -> List[int]:
        """Get herd numbers for a species/usage combination with pagination."""
        try:
            all_herds = set()  # Use a set to detect duplicates
            page = 1
            max_retries = 3
            bes_nr_fra = 0
            
            while True:
                logger.info(f"Fetching page {page} for species {species_code}, usage {usage_code} "
                          f"(starting from herd {bes_nr_fra}, current count: {len(all_herds)})")
                
                retry_count = 0
                while retry_count < max_retries:
                    try:
                        request = {
                            'GLRCHRWSInfoInbound': {
                                'BrugerNavn': self.username,
                                'KlientId': 'LandbrugsData',
                                'SessionId': '1',
                                'IPAdresse': '',
                                'TrackID': f'list_herds_{species_code}_{usage_code}_{bes_nr_fra}'
                            },
                            'Request': {
                                'DyreArtKode': str(species_code),
                                'BrugsArtKode': str(usage_code),
                                'BesNrFra': str(bes_nr_fra)
                            }
                        }
                        
                        result = self.clients['besaetning'].service.listBesaetningerMedBrugsart(request)
                        break
                    except Exception as e:
                        retry_count += 1
                        if retry_count == max_retries:
                            logger.error(f"Failed to fetch page {page} after {max_retries} retries: {str(e)}")
                            return sorted(list(all_herds))
                        logger.warning(f"Retry {retry_count}/{max_retries} for page {page}: {str(e)}")
                        time.sleep(1)
                
                if not hasattr(result, 'Response'):
                    logger.warning(f"No Response attribute in result for page {page}")
                    break
                
                # Get metadata
                fra_bes_nr = getattr(result.Response, 'FraBesNr', None)
                til_bes_nr = getattr(result.Response, 'TilBesNr', None)
                api_antal = getattr(result.Response, 'antal', None)
                has_more = getattr(result.Response, 'FlereBesaetninger', False)
                
                logger.info(f"Response metadata: FraBesNr={fra_bes_nr}, TilBesNr={til_bes_nr}, "
                          f"antal={api_antal}, FlereBesaetninger={has_more}")
                
                if not hasattr(result.Response, 'BesaetningsnummerListe'):
                    logger.warning(f"No BesaetningsnummerListe in response for page {page}")
                    break
                
                if not hasattr(result.Response.BesaetningsnummerListe, 'BesNrListe'):
                    logger.warning(f"No BesNrListe in BesaetningsnummerListe for page {page}")
                    break
                
                # Get the list of herd numbers
                herd_numbers = result.Response.BesaetningsnummerListe.BesNrListe
                if not herd_numbers:
                    logger.info(f"No herds found on page {page}")
                    break
                
                # Convert to list if needed
                if not isinstance(herd_numbers, list):
                    herd_numbers = list(herd_numbers)
                
                # Process numbers
                new_herds = 0
                duplicates = 0
                page_min = float('inf')
                page_max = 0
                
                for herd in herd_numbers:
                    if herd is not None:
                        try:
                            herd_int = int(herd)
                            if herd_int > 0:
                                page_min = min(page_min, herd_int)
                                page_max = max(page_max, herd_int)
                                if herd_int not in all_herds:
                                    all_herds.add(herd_int)
                                    new_herds += 1
                                else:
                                    duplicates += 1
                        except (ValueError, TypeError):
                            continue
                
                logger.info(f"Page {page} stats: {new_herds} new herds, {duplicates} duplicates")
                if new_herds > 0:
                    logger.info(f"Page range: {page_min} - {page_max}")
                
                # Check if we should continue
                if new_herds == 0:
                    logger.info("No new herds found, stopping pagination")
                    break
                
                if has_more and til_bes_nr is not None:
                    try:
                        next_bes_nr = int(til_bes_nr) + 1
                        if next_bes_nr <= bes_nr_fra:
                            logger.warning(f"Next starting point ({next_bes_nr}) is not greater than current ({bes_nr_fra})")
                            break
                        bes_nr_fra = next_bes_nr
                        page += 1
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid TilBesNr value: {til_bes_nr}")
                        break
                else:
                    logger.info("No more pages indicated by API")
                    break
                
                # Add small delay to avoid overwhelming the service
                time.sleep(0.2)
            
            total_herds = len(all_herds)
            logger.info(f"Found {total_herds} total herds across {page} pages")
            if all_herds:
                logger.info(f"Overall range: {min(all_herds)} - {max(all_herds)}")
            
            return sorted(list(all_herds))
            
        except Exception as e:
            logger.error(f"Error listing herds for species {species_code}, usage {usage_code}: {str(e)}")
            return []

    async def analyze_all_combinations(self):
        """Analyze all species/usage combinations to find those with exactly 10000 herds."""
        try:
            # First get all valid combinations
            combinations = self.get_species_usage_combinations()
            logger.info(f"Found {len(combinations)} valid species/usage combinations")
            
            # Track combinations with exactly 10000 herds
            suspicious_combinations = []
            all_results = []
            
            # Process combinations in chunks to avoid overwhelming the API
            chunk_size = 5  # Process 5 combinations at a time
            for i in range(0, len(combinations), chunk_size):
                chunk = combinations[i:i + chunk_size]
                
                # Process each combination in the chunk
                for combo in chunk:
                    species_code = combo['species_code']
                    usage_code = combo['usage_code']
                    species_text = combo['species_text']
                    usage_text = combo['usage_text']
                    
                    logger.info(f"\nTesting combination {i+1}/{len(combinations)}: "
                              f"species {species_code} ({species_text}), "
                              f"usage {usage_code} ({usage_text})")
                    
                    herds = self.get_herds_for_combination(species_code, usage_code)
                    count = len(herds)
                    
                    result = {
                        'species_code': species_code,
                        'species_text': species_text,
                        'usage_code': usage_code,
                        'usage_text': usage_text,
                        'herd_count': count
                    }
                    
                    if herds:
                        result.update({
                            'min_herd': min(herds),
                            'max_herd': max(herds)
                        })
                    
                    all_results.append(result)
                    
                    if count == 10000:
                        logger.warning(f"Found combination with exactly 10000 herds!")
                        suspicious_combinations.append(result)
                    else:
                        logger.info(f"Found {count} herds")
                    
                    # Add a small delay to avoid overwhelming the service
                    time.sleep(0.1)  # Reduced from 0.2s to 0.1s
            
            # Log summary
            logger.info("\n=== Summary ===")
            logger.info(f"Tested {len(combinations)} combinations")
            logger.info(f"Found {len(suspicious_combinations)} combinations with exactly 10000 herds:")
            
            for combo in suspicious_combinations:
                logger.info(f"\nSpecies {combo['species_code']} ({combo['species_text']}), "
                          f"Usage {combo['usage_code']} ({combo['usage_text']})")
                logger.info(f"Herd range: {combo.get('min_herd', 'N/A')} - {combo.get('max_herd', 'N/A')}")
            
            # Also log distribution of herd counts
            counts = [r['herd_count'] for r in all_results]
            if counts:
                logger.info("\nHerd count distribution:")
                logger.info(f"Min count: {min(counts)}")
                logger.info(f"Max count: {max(counts)}")
                logger.info(f"Number of empty combinations (0 herds): {counts.count(0)}")
                logger.info(f"Number of combinations with exactly 10000 herds: {counts.count(10000)}")
                
                # Count ranges
                ranges = [(0, 100), (100, 1000), (1000, 5000), (5000, 9999), (10000, 10000), (10001, float('inf'))]
                for start, end in ranges:
                    count = len([c for c in counts if start <= c <= end])
                    logger.info(f"Combinations with {start}-{end} herds: {count}")
            
            return suspicious_combinations
            
        except Exception as e:
            logger.error(f"Error analyzing combinations: {str(e)}")
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

    def get_property_details(self, chr_number: int) -> Dict[str, Any]:
        """Get detailed property information for a CHR number."""
        try:
            request = {
                'GLRCHRWSInfoInbound': {
                    'BrugerNavn': self.username,
                    'KlientId': 'LandbrugsData',
                    'SessionId': '1',
                    'IPAdresse': '',
                    'TrackID': f'property_details_{chr_number}'
                },
                'Request': {
                    'ChrNummer': chr_number
                }
            }
            
            logger.debug(f"Fetching property details for CHR {chr_number}")
            result = self.clients['ejendom'].service.hentCHRStamoplysninger(request)
            
            if not hasattr(result, 'Response') or not result.Response:
                logger.warning(f"No property data found for CHR {chr_number}")
                return {}
            
            # Response is a list with one item
            response_data = result.Response[0]
            
            # Parse property information
            details = {
                'chr_number': int(chr_number),
                
                # Property information
                'property_address': str(getattr(response_data.Ejendom, 'Adresse', '')).strip() or None,
                'property_city': str(getattr(response_data.Ejendom, 'ByNavn', '')).strip() or None,
                'property_postal_code': str(getattr(response_data.Ejendom, 'PostNummer', '')).strip() or None,
                'property_postal_district': str(getattr(response_data.Ejendom, 'PostDistrikt', '')).strip() or None,
                'property_municipality_code': int(getattr(response_data.Ejendom, 'KommuneNummer', 0)) or None,
                'property_municipality_name': str(getattr(response_data.Ejendom, 'KommuneNavn', '')).strip() or None,
                'property_created_date': self._parse_date(getattr(response_data.Ejendom, 'DatoOpret', None)),
                'property_updated_date': self._parse_date(getattr(response_data.Ejendom, 'DatoOpdatering', None)),
                
                # Food authority information
                'food_region_number': int(getattr(response_data.FVST, 'FoedevareRegionsNummer', 0)) or None,
                'food_region_name': str(getattr(response_data.FVST, 'FoedevareRegionsNavn', '')).strip() or None,
                'veterinary_dept_name': str(getattr(response_data.FVST, 'VeterinaerAfdelingsNavn', '')).strip() or None,
                'veterinary_section_name': str(getattr(response_data.FVST, 'VeterinaerSektionsNavn', '')).strip() or None,
            }
            
            # Add coordinates if available
            if hasattr(response_data, 'StaldKoordinater'):
                coords = response_data.StaldKoordinater
                x = getattr(coords, 'StaldKoordinatX', None)
                y = getattr(coords, 'StaldKoordinatY', None)
                
                if x is not None and y is not None:
                    try:
                        x = float(x)
                        y = float(y)
                        
                        # Store original UTM32 coordinates
                        details['stable_coordinates_utm32'] = {
                            'x': x,
                            'y': y
                        }
                        
                        # Convert to WGS84 if coordinates seem valid
                        if 400000 <= x <= 900000 and 6000000 <= y <= 6500000:  # Valid range for Denmark
                            from pyproj import Transformer
                            transformer = Transformer.from_crs("EPSG:25832", "EPSG:4326")
                            lat, lon = transformer.transform(x, y)
                            
                            # Validate converted coordinates
                            if 54 <= lat <= 58 and 8 <= lon <= 13:  # Valid range for Denmark
                                details['latitude'] = lat
                                details['longitude'] = lon
                            else:
                                logger.warning(f"Converted coordinates outside Denmark for CHR {chr_number}: {lat}, {lon}")
                        else:
                            logger.warning(f"UTM32 coordinates outside valid range for CHR {chr_number}: {x}, {y}")
                            
                    except Exception as e:
                        logger.error(f"Error processing coordinates for CHR {chr_number}: {str(e)}")
            
            # Remove None values
            return {k: v for k, v in details.items() if v is not None}
            
        except Exception as e:
            logger.error(f"Error getting property details for CHR {chr_number}: {str(e)}")
            return {}

    def _prepare_dataframe_for_bigquery(self, df: pd.DataFrame, nested_columns: List[str] = None) -> pd.DataFrame:
        """Prepare a DataFrame for BigQuery by:
        1. Flattening nested dictionaries
        2. Converting datetime objects to ISO format strings
        3. Converting booleans to integers
        4. Ensuring column names are BigQuery-compatible
        """
        # Make a copy to avoid modifying the original
        df = df.copy()
        
        # Flatten nested dictionaries into columns
        if nested_columns:
            for col in nested_columns:
                if col in df.columns:
                    nested_df = pd.json_normalize(df[col].dropna())
                    if not nested_df.empty:
                        # Prefix the new columns with the original column name
                        nested_df.columns = [f"{col}_{c}" for c in nested_df.columns]
                        # Drop the original column and join the new columns
                        df = df.drop(columns=[col]).join(nested_df)
        
        # Convert datetime objects to ISO format strings
        date_columns = df.select_dtypes(include=['datetime64']).columns
        for col in date_columns:
            df[col] = df[col].dt.strftime('%Y-%m-%d')
        
        # Convert boolean columns to integers
        bool_columns = df.select_dtypes(include=['bool']).columns
        for col in bool_columns:
            df[col] = df[col].astype(int)
        
        # Ensure column names are BigQuery compatible (no dots, spaces)
        df.columns = [c.replace('.', '_').replace(' ', '_').lower() for c in df.columns]
        
        return df

    def _convert_coordinates(self, coords: dict) -> tuple[float, float]:
        """Convert UTM32 coordinates to WGS84 with validation"""
        try:
            if not coords or 'x' not in coords or 'y' not in coords:
                return None, None
                
            x = float(coords['x'])
            y = float(coords['y'])
            
            # Valid range for Denmark in UTM32
            if not (400000 <= x <= 900000 and 6000000 <= y <= 6500000):
                logger.warning(f"Coordinates outside UTM32 range for Denmark: {x}, {y}")
                return None, None
                
            transformer = Transformer.from_crs("EPSG:25832", "EPSG:4326")
            lat, lon = transformer.transform(x, y)
            
            # Validate converted coordinates (Denmark bounds)
            if not (54 <= lat <= 58 and 8 <= lon <= 13):
                logger.warning(f"Converted coordinates outside Denmark: {lat}, {lon}")
                return None, None
                
            return lat, lon
            
        except Exception as e:
            logger.error(f"Error converting coordinates: {str(e)}")
            return None, None

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

    def _upload_batch_data(self, herds_data: list, properties_data: list, owners_data: list, users_data: list, practices_data: list) -> None:
        """Upload batches of data to storage."""
        try:
            # Convert lists to DataFrames
            if herds_data:
                herds_df = pd.DataFrame(herds_data)
                self._upload_to_storage(herds_df, 'herds')
            
            if properties_data:
                properties_df = pd.DataFrame(properties_data)
                self._upload_to_storage(properties_df, 'properties')
            
            if owners_data:
                owners_df = pd.DataFrame(owners_data)
                self._upload_to_storage(owners_df, 'owners')
            
            if users_data:
                users_df = pd.DataFrame(users_data)
                self._upload_to_storage(users_df, 'users')
            
            if practices_data:
                practices_df = pd.DataFrame(practices_data)
                self._upload_to_storage(practices_df, 'practices')
                
        except Exception as e:
            logger.error(f"Error uploading batch data: {str(e)}")
            raise

    async def process_herd(self, herd_number: int, species_code: int) -> Dict[str, Any]:
        """Process a single herd, fetching details and property information."""
        try:
            herd_details = await self.get_herd_details(herd_number, species_code)
            if not herd_details:
                return {}
            
            chr_number = herd_details.get('chr_number')
            if not chr_number:
                return herd_details
            
            property_details = await self.get_property_details(chr_number)
            if not property_details:
                return herd_details
            
            return {**herd_details, **property_details}
            
        except Exception as e:
            logger.error(f"Error processing herd {herd_number}: {str(e)}")
            return {}