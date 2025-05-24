#!/bin/bash

# Startup script for SFTP to GCS transfer VM with JSON to Parquet processing
# This script runs when the VM starts, performs the transfer with privacy transformations

set -e

# Log everything
exec > >(tee /var/log/sftp-transfer.log) 2>&1
echo "Starting SFTP to GCS transfer with processing at $(date)"
sync

# Install required packages
apt-get update
apt-get install -y python3-pip python3-venv git dnsutils iputils-ping unzip

# Create virtual environment
python3 -m venv /opt/transfer-env
source /opt/transfer-env/bin/activate

# Install required Python packages (added ijson, pyarrow, uuid for processing)
pip install google-cloud-storage google-cloud-secret-manager paramiko ijson pyarrow uuid

# Create the enhanced transfer script with processing
cat > /opt/transfer_script.py << 'EOF'
#!/usr/bin/env python3

import os
import tempfile
import logging
import paramiko
import json
import ijson
import pyarrow as pa
import pyarrow.parquet as pq
import uuid
import zipfile
from datetime import datetime
from pathlib import Path
from google.cloud import storage, secretmanager
import subprocess
import sys
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def flush_logs():
    """Ensure stdout and stderr are flushed."""
    sys.stdout.flush()
    sys.stderr.flush()

class PropertyDataProcessor:
    """Handles privacy transformations and Parquet conversion."""
    
    def __init__(self):
        self.cpr_to_uuid_mapping = {}  # Cache for consistent UUID mapping
        
    def generate_uuid_for_cpr(self, cpr_id):
        """Generate consistent UUID for CPR numbers."""
        if not cpr_id:
            return None
        if cpr_id not in self.cpr_to_uuid_mapping:
            self.cpr_to_uuid_mapping[cpr_id] = str(uuid.uuid4())
        return self.cpr_to_uuid_mapping[cpr_id]
    
    def has_foreign_address(self, person_data):
        """Check if person has foreign address information."""
        if not person_data:
            return False
        udrejse_indrejse = person_data.get('UdrejseIndrejse', {})
        return bool(udrejse_indrejse.get('Udenlandsadresse') or 
                   udrejse_indrejse.get('cprLandUdrejse') or 
                   udrejse_indrejse.get('cprLandekodeUdrejse'))
    
    def transform_person_data(self, properties):
        """Apply privacy transformations to person ownership data."""
        if not properties or 'ejendePerson' not in properties:
            return properties
        
        person_data = properties['ejendePerson'].get('Person', {})
        if not person_data:
            return properties
        
        # Create transformed copy
        transformed = properties.copy()
        transformed_person = person_data.copy()
        
        # 1. Remove gender field
        if 'koen' in transformed_person:
            del transformed_person['koen']
        
        # 2. Replace CPR ID with UUID
        if 'id' in transformed_person:
            original_id = transformed_person['id']
            transformed_person['id'] = self.generate_uuid_for_cpr(original_id)
        
        # 3. Remove ALL personal address information (including municipality)
        if 'CprAdresse' in transformed_person:
            del transformed_person['CprAdresse']
        
        # Remove detailed address information
        if 'Adresseoplysninger' in transformed_person:
            del transformed_person['Adresseoplysninger']
        
        # 4. Add abroad flag
        transformed_person['lives_abroad'] = self.has_foreign_address(person_data)
        
        # 5. Keep privacy protection notice (Beskyttelser) as is
        # No changes needed - this stays
        
        # 6. Remove foreign address details but keep the flag
        if 'UdrejseIndrejse' in transformed_person:
            del transformed_person['UdrejseIndrejse']
        
        # 7. Remove birth date (potential CPR info)
        if 'foedselsdato' in transformed_person:
            del transformed_person['foedselsdato']
        if 'foedselsdatoUsikkerhedsmarkering' in transformed_person:
            del transformed_person['foedselsdatoUsikkerhedsmarkering']
        
        # Update the transformed properties
        transformed['ejendePerson']['Person'] = transformed_person
        
        return transformed
    
    def process_json_to_parquet(self, json_file_path, output_parquet_path):
        """Stream process JSON file and convert to Parquet with transformations."""
        logger.info(f"Processing {json_file_path} to {output_parquet_path}")
        flush_logs()
        
        all_records = []
        feature_count = 0
        
        try:
            with open(json_file_path, 'rb') as f:
                # Stream parse the GeoJSON features
                for feature in ijson.items(f, 'features.item'):
                    try:
                        # Apply privacy transformations
                        if 'properties' in feature:
                            feature['properties'] = self.transform_person_data(feature['properties'])
                        
                        all_records.append(feature)
                        feature_count += 1
                        
                        if feature_count % 50000 == 0:  # Log every 50K
                            logger.info(f"Processed {feature_count:,} features...")
                            flush_logs()
                    
                    except Exception as e:
                        logger.warning(f"Error processing feature {feature_count}: {e}")
                        continue
            
            # Write all records at once - much more efficient
            if all_records:
                logger.info(f"Writing {len(all_records):,} records to Parquet...")
                flush_logs()
                table = pa.Table.from_pylist(all_records)
                pq.write_table(table, output_parquet_path, compression='snappy')
                logger.info(f"Parquet file written: {output_parquet_path}")
                flush_logs()
            
            logger.info(f"Successfully processed {feature_count:,} features to {output_parquet_path}")
            flush_logs()
            
        except Exception as e:
            logger.error(f"Error processing JSON file: {e}")
            logger.error(traceback.format_exc())
            flush_logs()
            raise

class SFTPToGCSTransferWithProcessing:
    def __init__(self):
        self.project_id = "landbrugsdata-1"
        self.bucket_name = "landbrugsdata-raw-data"
        self.storage_client = storage.Client()
        self.secret_client = secretmanager.SecretManagerServiceClient()
        self.processor = PropertyDataProcessor()
        logger.info("SFTPToGCSTransferWithProcessing initialized.")
        flush_logs()

    def get_secret(self, secret_name):
        logger.debug(f"Attempting to get secret: {secret_name}")
        flush_logs()
        name = f"projects/{self.project_id}/secrets/{secret_name}/versions/latest"
        try:
            response = self.secret_client.access_secret_version(name=name)
            secret_value = response.payload.data.decode('UTF-8').strip()
            logger.debug(f"Successfully retrieved secret: {secret_name}")
            flush_logs()
            return secret_value
        except Exception as e:
            logger.error(f"Failed to retrieve secret: {secret_name}. Error: {e}")
            logger.error(traceback.format_exc())
            flush_logs()
            raise

    def get_sftp_client(self):
        logger.info("Attempting to create SFTP client...")
        flush_logs()
        try:
            host = self.get_secret("datafordeler-sftp-host")
            username = self.get_secret("datafordeler-sftp-username")
            private_key_data = self.get_secret("ssh-brugerdatafordeleren")
            
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            import socket
            import time
            
            def resolve_hostname_with_retry(hostname, max_attempts=5, delay=10):
                logger.info(f"Starting DNS resolution for {hostname}.")
                flush_logs()
                for attempt in range(1, max_attempts + 1):
                    try:
                        logger.debug(f"DNS RESOLUTION ATTEMPT {attempt}/{max_attempts} for {hostname}")
                        flush_logs()
                        
                        logger.debug(f"Attempting Python DNS resolution for {hostname}")
                        flush_logs()
                        try:
                            logger.debug("Trying socket.getaddrinfo...")
                            flush_logs()
                            result = socket.getaddrinfo(hostname, None, socket.AF_INET)
                            ip = result[0][4][0]
                            logger.info(f"DNS resolution successful: {hostname} -> {ip}")
                            flush_logs()
                            return ip
                        except Exception as e:
                            logger.warning(f"getaddrinfo failed for {hostname}: {e}")
                            flush_logs()
                        
                    except socket.gaierror as e:
                        logger.warning(f"DNS resolution attempt {attempt} for {hostname} failed with gaierror: {e}")
                        
                        if attempt < max_attempts:
                            logger.info(f"Waiting {delay} seconds before DNS retry for {hostname} (attempt {attempt+1}/{max_attempts})...")
                            flush_logs()
                            time.sleep(delay)
                        else:
                            logger.error(f"All DNS resolution attempts failed for {hostname}")
                            flush_logs()
                            raise
                    except Exception as e:
                        logger.error(f"Unexpected error in DNS resolution attempt {attempt}: {e}")
                        logger.error(traceback.format_exc())
                        flush_logs()
                        if attempt < max_attempts:
                            logger.info(f"Waiting {delay} seconds before retry...")
                            flush_logs()
                            time.sleep(delay)
                        else:
                            logger.error(f"All DNS resolution attempts failed for {hostname}")
                            flush_logs()
                            raise
            
            host_ip = resolve_hostname_with_retry(host)
            
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.key') as key_file:
                key_file.write(private_key_data)
                if not private_key_data.endswith('\n'):
                    key_file.write('\n')
                key_file.flush()
                key_file_path = key_file.name
                logger.debug(f"Private key written to temporary file: {key_file_path}")
                flush_logs()

                try:
                    private_key = paramiko.RSAKey.from_private_key_file(key_file_path)
                    logger.info(f"Attempting SSH connection to {host_ip} (user: {username})...")
                    flush_logs()
                    ssh.connect(
                        hostname=host_ip, port=22, username=username, pkey=private_key,
                        timeout=60, banner_timeout=30, auth_timeout=30,
                        allow_agent=False, look_for_keys=False
                    )
                    logger.info("SSH connection established successfully.")
                    flush_logs()
                    
                    sftp = ssh.open_sftp()
                    logger.info("SFTP session opened successfully.")
                    flush_logs()
                    return sftp
                except Exception as e:
                    logger.error(f"SSH/SFTP connection failed: {e}")
                    logger.error(traceback.format_exc())
                    flush_logs()
                    raise
                finally:
                    logger.info(f"Deleting temporary key file: {key_file_path}")
                    flush_logs()
                    os.unlink(key_file_path)
                    
        except Exception as e:
            logger.error(f"Failed to connect to SFTP: {e}")
            logger.error(traceback.format_exc())
            flush_logs()
            raise
    
    def find_latest_file(self, sftp):
        logger.info("Finding latest .zip file on SFTP server...")
        flush_logs()
        latest_file = None
        latest_time = None
        
        try:
            for entry in sftp.listdir_attr('.'):
                logger.debug(f"SFTP entry: {entry.filename}, mtime: {entry.st_mtime}")
                flush_logs()
                if not entry.longname.startswith('d') and entry.filename.endswith('.zip'):
                    if latest_time is None or entry.st_mtime > latest_time:
                        latest_file = entry.filename
                        latest_time = entry.st_mtime
                        logger.debug(f"New latest candidate: {latest_file} (mtime: {latest_time})")
                        flush_logs()
            
            if latest_file is None:
                logger.error("No .zip files found on SFTP server.")
                flush_logs()
                raise FileNotFoundError("No .zip files found on SFTP server")
                
            logger.info(f"Found latest file: {latest_file}")
            flush_logs()
            return latest_file
        except Exception as e:
            logger.error(f"Error finding latest file: {e}")
            logger.error(traceback.format_exc())
            flush_logs()
            raise

    def transfer_and_process_file(self):
        logger.info("Starting file transfer and processing...")
        flush_logs()
        sftp = None
        try:
            sftp = self.get_sftp_client()
            latest_file = self.find_latest_file(sftp)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            with tempfile.TemporaryDirectory() as tmpdir:
                # Download ZIP
                local_zip_path = Path(tmpdir) / latest_file
                logger.info(f"Downloading {latest_file}...")
                flush_logs()
                sftp.get(latest_file, str(local_zip_path))
                logger.info(f"Download complete. File size: {local_zip_path.stat().st_size / (1024**3):.1f} GB")
                flush_logs()
                
                # Extract JSON from ZIP
                json_file_path = None
                with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
                    for file_info in zip_ref.filelist:
                        if file_info.filename.endswith('.json') and not file_info.filename.endswith('_Metadata.json'):
                            json_file_path = Path(tmpdir) / file_info.filename
                            logger.info(f"Extracting {file_info.filename}...")
                            flush_logs()
                            zip_ref.extract(file_info.filename, tmpdir)
                            break
                
                if not json_file_path:
                    raise FileNotFoundError("No JSON file found in ZIP")
                
                logger.info(f"Extracted JSON file: {json_file_path} ({json_file_path.stat().st_size / (1024**3):.1f} GB)")
                flush_logs()
                
                # Process JSON to Parquet
                parquet_file_path = Path(tmpdir) / f"property_owners_{timestamp}.parquet"
                logger.info("Starting JSON to Parquet processing with privacy transformations...")
                flush_logs()
                
                self.processor.process_json_to_parquet(str(json_file_path), str(parquet_file_path))
                
                # Clean up JSON file immediately to save disk space (14GB SSD is limited)
                logger.info(f"Removing JSON file to save disk space: {json_file_path}")
                flush_logs()
                json_file_path.unlink()
                
                logger.info(f"Processing complete. Parquet file size: {parquet_file_path.stat().st_size / (1024**2):.1f} MB")
                flush_logs()
                
                # Upload Parquet to GCS
                gcs_filename = f"silver/property_owners/{timestamp}_property_owners_processed.parquet"
                bucket = self.storage_client.bucket(self.bucket_name)
                blob = bucket.blob(gcs_filename)
                
                logger.info(f"Uploading to GCS: gs://{self.bucket_name}/{gcs_filename}")
                flush_logs()
                blob.upload_from_filename(str(parquet_file_path))
                logger.info("GCS upload complete.")
                flush_logs()
                
                # Also upload original ZIP for backup
                original_gcs_filename = f"bronze/property_owners/{timestamp}_{latest_file}"
                original_blob = bucket.blob(original_gcs_filename)
                logger.info(f"Uploading original ZIP to: gs://{self.bucket_name}/{original_gcs_filename}")
                flush_logs()
                original_blob.upload_from_filename(str(local_zip_path))
                logger.info("Original ZIP backup upload complete.")
                flush_logs()

            logger.info("Transfer and processing completed successfully.")
            flush_logs()

        except Exception as e:
            logger.error(f"Transfer and processing failed: {e}")
            logger.error(traceback.format_exc())
            flush_logs()
            raise
        finally:
            if sftp:
                try:
                    sftp.close()
                    logger.info("SFTP connection closed.")
                    flush_logs()
                except Exception as e:
                    logger.warning(f"Error closing SFTP: {e}")
                    flush_logs()

def main_transfer_and_shutdown():
    logger.info("=== Starting main_transfer_and_shutdown ===")
    flush_logs()
    
    try:
        transfer_instance = SFTPToGCSTransferWithProcessing()
        transfer_instance.transfer_and_process_file()
        logger.info("Transfer and processing successful.")
        flush_logs()
        success = True
    except Exception as e:
        logger.error(f"Transfer and processing failed: {e}")
        logger.error(traceback.format_exc())
        flush_logs()
        success = False
    
    logger.info(f"Process success status: {success}")
    flush_logs()
    
    return success

if __name__ == "__main__":
    logger.info("=== Starting Property Owners SFTP Transfer with Processing ===")
    flush_logs()
    
    success = main_transfer_and_shutdown()
    
    logger.info("Python script finished.")
    flush_logs()

EOF

chmod +x /opt/transfer_script.py

# Get current VM metadata for self-deletion
VM_NAME=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/name)
ZONE=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/zone | awk -F/ '{print $NF}')
PROJECT_ID=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/project/project-id)

# Function to delete the VM
delete_vm() {
  echo "Attempting to delete VM: $VM_NAME in zone: $ZONE project: $PROJECT_ID"
  sync
  gcloud compute instances delete "$VM_NAME" --zone="$ZONE" --project="$PROJECT_ID" --quiet
  echo "gcloud delete command executed for $VM_NAME."
  sync
}

# Run the enhanced transfer script
echo "Executing enhanced transfer script with processing..."
sync
python3 /opt/transfer_script.py || (echo "Python script failed." && echo "ERROR: Processing failed." >&2)

echo "SFTP to GCS transfer with processing finished at $(date)."

# Self-delete VM after successful completion
echo "VM will self-delete in 1 minute to allow log inspection."
sleep 60
delete_vm

sync
echo "Script finished."
exit 0 