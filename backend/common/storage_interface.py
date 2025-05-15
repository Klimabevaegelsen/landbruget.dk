import os
import json
from google.cloud import storage
import pandas as pd
from io import BytesIO

class StorageInterface:
    """Interface for saving JSON data to different storage backends."""
    def save_json(self, data, dst_path):
        raise NotImplementedError("save_json must be implemented by subclasses")
    
    def save_parquet(self, data, dst_path):
        """Save data as a Parquet file."""
        raise NotImplementedError("save_parquet must be implemented by subclasses")
    
    def read_json(self, src_path):
        """Load JSON data from the storage backend."""
        raise NotImplementedError("read_json must be implemented by subclasses")
    

class LocalStorage(StorageInterface):
    """Save JSON files to the local filesystem."""
    def __init__(self, base_dir):
        self.base_dir = base_dir

    def save_json(self, data, dst_path):
        full_path = os.path.join(self.base_dir, dst_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def save_parquet(self, data, dst_path):
        """Save data as Parquet locally."""
        full_path = os.path.join(self.base_dir, dst_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        df = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
        df.to_parquet(full_path, index=False)

    def read_json(self, src_path):
        full_path = os.path.join(self.base_dir, src_path)
        with open(full_path, 'r', encoding='utf-8') as f:
            return json.load(f)

class GCSStorage(StorageInterface):
    """Save JSON files to a Google Cloud Storage bucket."""
    def __init__(self, bucket_name):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def save_json(self, data, dst_path):
        blob = self.bucket.blob(dst_path)
        blob.upload_from_string(json.dumps(data), content_type="application/json")

    def save_parquet(self, data, dst_path):
        """Save data as Parquet to GCS."""
        df = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        blob = self.bucket.blob(dst_path)
        blob.upload_from_string(buffer.getvalue(), content_type="application/octet-stream")

    def read_json(self, src_path):
        blob = self.bucket.blob(src_path)
        content = blob.download_as_string()
        return json.loads(content)
