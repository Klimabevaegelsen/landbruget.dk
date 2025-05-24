#!/bin/bash

# Startup script for SFTP to GCS transfer VM
# This script runs when the VM starts, performs the transfer, and DELETION IS CURRENTLY DISABLED FOR DEBUGGING

set -e

# Log everything
exec > >(tee /var/log/sftp-transfer.log) 2>&1
echo "Starting SFTP to GCS transfer at $(date)"
sync # Try to flush initial log

# Install required packages
apt-get update
apt-get install -y python3-pip python3-venv git dnsutils iputils-ping

# Create virtual environment
python3 -m venv /opt/transfer-env
source /opt/transfer-env/bin/activate

# Install required Python packages
pip install google-cloud-storage google-cloud-secret-manager paramiko

# Create the transfer script
cat > /opt/transfer_script.py << 'EOF'
#!/usr/bin/env python3

import os
import tempfile
import logging
import paramiko
from datetime import datetime
from pathlib import Path
from google.cloud import storage, secretmanager
import subprocess
import sys
import traceback # Added for more detailed error logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Handler to flush logs immediately
class FlushFileHandler(logging.FileHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()

# If you want to log to a file within the python script as well (optional)
# file_handler = FlushFileHandler('/var/log/python_transfer_details.log')
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# file_handler.setFormatter(formatter)
# logger.addHandler(file_handler)

def flush_logs():
    """Ensure stdout and stderr are flushed."""
    sys.stdout.flush()
    sys.stderr.flush()

class SFTPToGCSTransfer:
    def __init__(self):
        self.project_id = "landbrugsdata-1"
        self.bucket_name = "landbrugsdata-raw-data"
        self.storage_client = storage.Client()
        self.secret_client = secretmanager.SecretManagerServiceClient()
        logger.info("SFTPToGCSTransfer class initialized.")
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
                        
                        # Test system DNS tools first
                        try:
                            logger.debug("Testing with getent...")
                            flush_logs()
                            result = subprocess.run(['getent', 'hosts', hostname], 
                                                  capture_output=True, text=True, timeout=10)
                            if result.returncode == 0:
                                logger.debug(f"getent result: {result.stdout.strip()}")
                            else:
                                logger.warning(f"getent failed for {hostname}: {result.stderr}")
                            flush_logs()
                        except Exception as e:
                            logger.warning(f"getent test failed for {hostname}: {e}")
                            flush_logs()
                        
                        try:
                            logger.debug("Testing with nslookup...")
                            flush_logs()
                            result = subprocess.run(['nslookup', hostname], 
                                                  capture_output=True, text=True, timeout=10)
                            if result.returncode == 0 and result.stdout:
                                logger.debug(f"nslookup stdout for {hostname}: {result.stdout.strip()}")
                            elif result.stderr:
                                logger.warning(f"nslookup stderr for {hostname}: {result.stderr.strip()}")
                            flush_logs()
                        except Exception as e:
                            logger.warning(f"nslookup test failed for {hostname}: {e}")
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
                        logger.warning(f"Error details: errno={getattr(e, 'errno', 'unknown')}")
                        
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
                        logger.error(f"Error type: {type(e).__name__}")
                        logger.error(f"Traceback: {traceback.format_exc()}")
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
            
            try:
                logger.info(f"Testing TCP connection to {host_ip}:22...")
                flush_logs()
                sock = socket.create_connection((host_ip, 22), timeout=30)
                sock.close()
                logger.info("TCP connection successful")
                flush_logs()
            except Exception as e:
                logger.error(f"TCP connection failed: {e}")
                logger.error(traceback.format_exc())
                flush_logs()
                raise
            
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.key') as key_file:
                key_file.write(private_key_data)
                if not private_key_data.endswith('\n'):
                    key_file.write('\n')
                key_file.flush()
                key_file_path = key_file.name # Store for logging
                logger.debug(f"Private key written to temporary file: {key_file_path}")
                flush_logs()

                try:
                    logger.debug(f"Loading RSA private key from temporary file...")
                    flush_logs()
                    private_key = paramiko.RSAKey.from_private_key_file(key_file_path)
                    logger.debug("RSA key loaded successfully")
                    flush_logs()
                    
                    logger.info(f"Attempting SSH connection to {host_ip} (user: {username})...")
                    flush_logs()
                    ssh.connect(
                        hostname=host_ip, port=22, username=username, pkey=private_key,
                        timeout=60, banner_timeout=30, auth_timeout=30,
                        disabled_algorithms={'pubkeys': [], 'kex': [], 'keys': [], 'ciphers': [], 'macs': []},
                        allow_agent=False, look_for_keys=False
                    )
                    # logger.info(f"SSH connection established successfully. Server version: {ssh.get_transport().server_version}, Client version: {ssh.get_transport().local_version}")
                    # Attempting a more robust way to get server banner, or just a generic success message
                    banner = ssh.get_transport().get_banner()
                    if banner:
                        logger.info(f"SSH connection established successfully. Server banner: {banner.strip()}")
                    else:
                        logger.info("SSH connection established successfully.")
                    flush_logs()
                    
                    sftp = ssh.open_sftp()
                    # logger.info(f"SFTP session opened successfully. Server SFTP version: {sftp.protocol.version}")
                    logger.info("SFTP session opened successfully.") # Simplified logging
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
            for entry in sftp.listdir_attr('.'): # Check current directory
                logger.debug(f"SFTP entry: {entry.filename}, mtime: {entry.st_mtime}, longname: {entry.longname}")
                flush_logs()
                if not entry.longname.startswith('d') and entry.filename.endswith('.zip'):
                    if latest_time is None or entry.st_mtime > latest_time:
                        latest_file = entry.filename
                        latest_time = entry.st_mtime
                        logger.debug(f"New latest candidate: {latest_file} (mtime: {latest_time})")
                        flush_logs()
            
            if latest_file is None:
                logger.error("No .zip files found on SFTP server in current directory.")
                flush_logs()
                raise FileNotFoundError("No .zip files found on SFTP server")
                
            logger.info(f"Found latest file: {latest_file}")
            flush_logs()
            return latest_file
        except Exception as e:
            logger.error(f"Error finding latest file on SFTP server: {e}")
            logger.error(traceback.format_exc())
            flush_logs()
            raise

    def transfer_file(self):
        logger.info("Starting file transfer process...")
        flush_logs()
        sftp = None
        try:
            logger.info("Connecting to SFTP server for transfer...")
            flush_logs()
            sftp = self.get_sftp_client()
            
            latest_file = self.find_latest_file(sftp)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            gcs_filename = f"bronze/property_owners/{timestamp}_{latest_file}"
            
            bucket = self.storage_client.bucket(self.bucket_name)
            blob = bucket.blob(gcs_filename)
            
            logger.info(f"Target GCS path: gs://{self.bucket_name}/{gcs_filename}")
            flush_logs()

            with tempfile.TemporaryDirectory() as tmpdir:
                local_path = Path(tmpdir) / latest_file
                logger.debug(f"Temporary local path for download: {local_path}")
                flush_logs()

                # SFTP Download
                try:
                    logger.info(f"Attempting SFTP download: {latest_file}...")
                    flush_logs()
                    sftp.get(latest_file, str(local_path))
                    logger.info(f"SFTP download of {latest_file} complete. Local file size: {local_path.stat().st_size} bytes.")
                    flush_logs()
                except Exception as e:
                    logger.error(f"SFTP download of {latest_file} failed: {e}")
                    logger.error(traceback.format_exc())
                    flush_logs()
                    raise # Re-raise to be caught by outer try/except

                # GCS Upload
                try:
                    logger.info(f"Attempting GCS upload for {latest_file} to {gcs_filename}...")
                    flush_logs()
                    blob.upload_from_filename(str(local_path))
                    logger.info(f"GCS upload complete for {gcs_filename}.")
                    flush_logs()
                except Exception as e:
                    logger.error(f"GCS upload of {local_path} failed: {e}")
                    logger.error(traceback.format_exc())
                    flush_logs()
                    raise # Re-raise to be caught by outer try/except

            logger.info("File transfer process completed successfully.")
            flush_logs()

        except Exception as e:
            logger.error(f"Overall transfer_file process failed: {e}")
            logger.error(traceback.format_exc()) # Log full traceback
            flush_logs()
            # Do not re-raise here if we want the script to attempt cleanup/VM deletion
            # However, for debugging, re-raising might be useful if not deleting VM
            raise # Re-raise for now as VM deletion is disabled
        finally:
            if sftp:
                try:
                    logger.info("Closing SFTP connection...")
                    flush_logs()
                    sftp.close()
                    if sftp.sock: # If transport is open
                        sftp.sock.close()
                    logger.info("SFTP connection closed.")
                    flush_logs()
                except Exception as e:
                    logger.warning(f"Error closing SFTP connection: {e}")
                    logger.error(traceback.format_exc())
                    flush_logs()

def main_transfer_and_shutdown():
    logger.info("=== Starting main_transfer_and_shutdown ===")
    flush_logs()
    transfer_instance = None
    success = False
    try:
        transfer_instance = SFTPToGCSTransfer()
        transfer_instance.transfer_file()
        logger.info("Transfer process initiated and returned from transfer_file method.")
        flush_logs()
        # Assuming if transfer_file doesn't raise an exception, it's a success for now
        # More robust success checking might be needed (e.g., check GCS object existence)
        success = True # Mark as success if no exceptions from transfer_file
    except Exception as e:
        logger.error(f"An error occurred in main_transfer_and_shutdown: {e}")
        logger.error(traceback.format_exc())
        flush_logs()
        # success remains False
    finally:
        logger.info(f"Transfer success status: {success}")
        logger.info("--- DEBUG: VM Deletion is currently disabled ---")
        flush_logs()
        # delete_vm() # DEBUG: Temporarily commented out
        # if success:
        #    logger.info("Transfer successful, preparing to delete VM.")
        #    delete_vm()
        # else:
        #    logger.error("Transfer failed, VM will NOT be deleted due to errors (or if deletion is disabled).")
        
    logger.info("=== main_transfer_and_shutdown finished ===")
    flush_logs()

if __name__ == "__main__":
    # Add DNS debug before actual script run
    logger.info("=== Initial DNS DEBUG (Pre-Transfer) ===")
    flush_logs()
    # subprocess.run(["echo", "Testing DNS resolution for ftp2.datafordeler.dk..."], check=False) # Removed
    # subprocess.run(["echo", "System time: $(date)"], check=False) # Removed
    # subprocess.run(["echo", "Uptime: $(uptime)"], check=False) # Removed
    # subprocess.run(["echo", "--- nslookup test ---"], check=False) # Removed
    # subprocess.run(["nslookup", "ftp2.datafordeler.dk"], check=False) # Removed
    # subprocess.run(["echo", "--- dig test ---"], check=False) # Removed
    # subprocess.run(["dig", "ftp2.datafordeler.dk"], check=False) # Removed
    # subprocess.run(["echo", "--- ping test ---\\"], check=False) # Removed
    # ping_result = subprocess.run(["ping", "-c", "2", "-W", "5", "ftp2.datafordeler.dk"], capture_output=True, text=True) # Removed
    # logger.info(f"Ping stdout: {ping_result.stdout}") # Removed
    # logger.info(f"Ping stderr: {ping_result.stderr}") # Removed
    # if ping_result.returncode != 0: # Removed
    #    logger.warning(f"ping failed with code {ping_result.returncode}") # Removed
    # subprocess.run(["echo", "--- getent test ---"], check=False) # Removed
    # subprocess.run(["getent", "hosts", "ftp2.datafordeler.dk"], check=False) # Removed
    
    logger.info("Performing a quick Python socket test for DNS...")
    python_socket_test_command = '''import socket, time; \
print(f"Pre-transfer DNS check at {time.time()}:"); \
try: \
    ip = socket.gethostbyname(\'ftp2.datafordeler.dk\'); \
    print(f"Python socket.gethostbyname(\'ftp2.datafordeler.dk\') SUCCESS: {ip}"); \
    addr_info = socket.getaddrinfo(\'ftp2.datafordeler.dk\', None, socket.AF_INET); \
    print(f"Python socket.getaddrinfo(\'ftp2.datafordeler.dk\') SUCCESS: {addr_info[0][4][0]}"); \
except Exception as e: \
    print(f"Pre-transfer DNS check FAILED: {e}")'''
    subprocess.run(["python3", "-c", python_socket_test_command], check=False)
    
    # subprocess.run(["echo", "--- Python socket test ---"], check=False) # Combined above
    # Fixed f-string issue by constructing the command string carefully
    # python_socket_test_command = '''import socket, time; \
# print(f"Testing at {time.time()}"); \
# print(f"Python socket.gethostbyname SUCCESS: {socket.gethostbyname(\\'ftp2.datafordeler.dk\\')}"); \
# print(f"Python socket.getaddrinfo SUCCESS: {socket.getaddrinfo(\\'ftp2.datafordeler.dk\\', None, socket.AF_INET)[0][4][0]}")'''
    # subprocess.run(["python3", "-c", python_socket_test_command], check=False) # Combined above
    # subprocess.run(["echo", "--- systemd-resolved status ---"], check=False) # Removed
    # subprocess.run(["systemctl", "status", "systemd-resolved.service", "--no-pager"], check=False) # Removed
    # subprocess.run(["echo", "--- /etc/resolv.conf ---"], check=False) # Removed
    # subprocess.run(["cat", "/etc/resolv.conf"], check=False) # Removed
    logger.info("=== END Initial DNS DEBUG ===")
    flush_logs()
    # sync # Try to flush logs before python script <-- This was the misplaced sync

    logger.info("Running SFTP to GCS transfer...")
    flush_logs()
    
    main_transfer_and_shutdown()
    logger.info("Python script /opt/transfer_script.py finished.")
    flush_logs()
    # sync # Try to flush logs after python script

EOF

chmod +x /opt/transfer_script.py

# Get current VM name and zone for self-deletion
VM_NAME=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/name)
ZONE=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/zone | awk -F/ '{print $NF}')
PROJECT_ID=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/project/project-id)

# Function to delete the VM
delete_vm() {
  echo "Attempting to delete VM: $VM_NAME in zone: $ZONE project: $PROJECT_ID"
  sync
  gcloud compute instances delete "$VM_NAME" --zone="$ZONE" --project="$PROJECT_ID" --quiet
  echo "gcloud delete command executed for $VM_NAME. Check console for status."
  # echo "DEBUG: VM Deletion is currently disabled. Would have deleted $VM_NAME."
  sync
}

# Run the transfer script
echo "Executing /opt/transfer_script.py..."
sync # Try to flush logs before python script - MOVED HERE
python3 /opt/transfer_script.py || (echo "Python script failed with an error." && echo "ERROR: Python script failed. VM deletion still disabled for debugging." >&2)

echo "SFTP to GCS transfer script finished its course at $(date)."

# DEBUG: VM Deletion is currently disabled
#echo "INFO: VM Deletion is currently DISABLED. You will need to manually delete this VM: $(hostname)"
echo "VM will self-delete in 1 minute to allow log inspection if needed."
sleep 60
delete_vm

# Final sync before potential shutdown (though shutdown is disabled)
sync
echo "Script finished."
exit 0 