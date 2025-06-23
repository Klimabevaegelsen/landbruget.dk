# Google Drive Data Pipeline Architecture

This document describes the technical architecture of the Google Drive Data Pipeline, including component design, data flow, and integration points.

## Overview

The Google Drive Data Pipeline follows a medallion architecture with Bronze and Silver layers. The pipeline fetches files from Google Drive, stores them in the Bronze layer with metadata, and processes them into standardized formats in the Silver layer.

## System Architecture

### High-Level Components

```
┌────────────┐     ┌────────────┐     ┌────────────┐     ┌────────────┐
│  Google    │     │  Bronze    │     │  Silver    │     │  Storage   │
│  Drive API │────▶│  Layer     │────▶│  Layer     │────▶│  Manager   │
└────────────┘     └────────────┘     └────────────┘     └────────────┘
                          │                  │                  │
                          ▼                  ▼                  ▼
                   ┌────────────┐     ┌────────────┐     ┌────────────┐
                   │  Metadata  │     │Transformers│     │  Local/GCS │
                   │  Manager   │     │& Validators│     │  Storage   │
                   └────────────┘     └────────────┘     └────────────┘
```

### Component Details

1. **GoogleDriveFetcher**
   - Authenticates with Google Drive API
   - Lists files and folders recursively
   - Downloads files with retry capability
   - Extracts file metadata

2. **BronzeProcessor**
   - Orchestrates Bronze layer processing
   - Mirrors source folder structure
   - Creates timestamped run directories
   - Manages file deduplication
   - Tracks processing progress

3. **SilverProcessor**
   - Orchestrates Silver layer processing
   - Coordinates transformers and validators
   - Applies schemas to transformed data
   - Handles PII detection and masking
   - Manages output file formats

4. **Transformers**
   - ExcelTransformer: Processes Excel files (.xlsx, .xls)
   - AdvancedPDFTransformer: Extracts data from PDF files
   - Additional transformers for specific file types

5. **StorageManager**
   - Abstracts storage operations
   - Supports local filesystem and GCS
   - Manages file organization and paths
   - Handles serialization and deserialization

6. **MetadataManager**
   - Generates and validates file metadata
   - Calculates checksums for deduplication
   - Tracks file lineage and provenance
   - Creates standardized metadata format

## Data Flow

### End-to-End Flow

1. User initiates pipeline with command-line options
2. Pipeline authenticates with Google Drive
3. Bronze layer lists files and folders in Drive
4. Files are downloaded to Bronze layer with metadata
5. Silver layer reads files from Bronze layer
6. Transformers process files based on type
7. Validators check and clean data
8. Processed data is written to Silver layer in standardized format

### Bronze Layer Flow

```
┌────────────┐     ┌────────────┐     ┌────────────┐     ┌────────────┐
│  List      │     │  Download  │     │  Generate  │     │  Save      │
│  Files     │────▶│  Files     │────▶│  Metadata  │────▶│  Files     │
└────────────┘     └────────────┘     └────────────┘     └────────────┘
```

### Silver Layer Flow

```
┌────────────┐     ┌────────────┐     ┌────────────┐     ┌────────────┐
│  Read      │     │ Transform  │     │  Validate  │     │  Write     │
│  Files     │────▶│  Data      │────▶│  & Clean   │────▶│  Parquet   │
└────────────┘     └────────────┘     └────────────┘     └────────────┘
```

## Directory Structure

The pipeline follows a modular directory structure:

```
backend/pipelines/drive_data_pipeline/
├── config/                  # Configuration management
├── bronze/                  # Bronze layer components
│   ├── drive/               # Google Drive integration
│   ├── processor.py         # Bronze processor
│   ├── metadata.py          # Metadata management
│   └── storage.py           # Bronze storage
├── silver/                  # Silver layer components
│   ├── transformers/        # File transformers
│   ├── validators/          # Data validators
│   ├── models/              # Data models
│   ├── processor.py         # Silver processor
│   └── storage.py           # Silver storage
├── utils/                   # Utility functions
│   ├── logging.py           # Logging utilities
│   ├── error_handling.py    # Error handling
│   ├── storage.py           # Storage utilities
│   └── helpers.py           # Helper functions
└── main.py                  # Main entry point
```

## Key Interfaces

### GoogleDriveFetcher Interface

```python
class GoogleDriveFetcher:
    def list_folder_contents(self, folder_id: str, recursive: bool = False) -> list[dict]:
        """List contents of a Google Drive folder."""
        
    def get_file_metadata(self, file_id: str) -> dict:
        """Get metadata for a specific file."""
        
    def download_file(self, file_id: str, destination_path: str) -> bool:
        """Download a file from Google Drive."""
```

### StorageManager Interface

```python
class StorageManager(ABC):
    @abstractmethod
    def save_file(self, content: bytes, path: str) -> bool:
        """Save file content to the specified path."""
        
    @abstractmethod
    def read_file(self, path: str) -> bytes:
        """Read file content from the specified path."""
        
    @abstractmethod
    def file_exists(self, path: str) -> bool:
        """Check if a file exists at the specified path."""
        
    @abstractmethod
    def create_directory(self, path: str) -> bool:
        """Create a directory at the specified path."""
```

### Transformer Interface

```python
class BaseTransformer(ABC):
    @abstractmethod
    def transform(self, file_path: Path, metadata: FileMetadata, output_dir: Path) -> TransformResult:
        """Transform a file from Bronze to Silver format."""
```

## Data Models

### FileMetadata

```python
class FileMetadata(BaseModel):
    file_id: str
    original_filename: str
    original_subfolder: str
    file_path: str
    file_extension: str
    checksum: str
    mime_type: str
    content_type: str = None
    file_size: int
    record_count: int = None
    modified_time: str
    download_time: str
    drive_path: str = None
```

### TransformResult

```python
class TransformResult(BaseModel):
    success: bool
    output_path: str = None
    record_count: int = None
    error: str = None
    warnings: list[str] = []
```

## Error Handling

The pipeline implements comprehensive error handling:

1. **Retry Mechanism**: Exponential backoff for transient errors
2. **Error Logging**: Structured logging with context information
3. **Error Propagation**: Errors are captured but don't stop the pipeline
4. **Recovery**: The pipeline can resume from failures

## Configuration

The pipeline is configured through:

1. **Environment Variables**: Basic configuration
2. **Command-Line Arguments**: Runtime configuration
3. **Configuration Files**: Complex configurations
4. **Code Constants**: Default values and fallbacks

## Security Considerations

1. **Authentication**: Service account credentials for Google Drive
2. **Storage Security**: Proper permissions for data storage
3. **PII Handling**: Detection and masking of PII data
4. **Logging**: No sensitive information in logs

## Performance Considerations

1. **Parallel Processing**: Configurable worker pool for downloads
2. **Chunked Downloads**: Large files are downloaded in chunks
3. **Streaming Processing**: Files are processed as streams when possible
4. **Resource Management**: CPU and memory usage is optimized

## Extensibility

The pipeline is designed for extensibility:

1. **New File Types**: Add new transformers for additional file types
2. **New Storage Backends**: Implement new storage manager classes
3. **Custom Validators**: Add new validators for specific data types
4. **Schema Evolution**: Support for evolving data schemas 