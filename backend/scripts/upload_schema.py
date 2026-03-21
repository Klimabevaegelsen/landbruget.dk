#!/usr/bin/env python3
"""
Upload schema documentation to Gemini File Search Store.

This script uploads schema documentation markdown files to a Gemini File Search Store
for use with Gemini API queries. It handles file management (deletion of old versions),
store creation, and provides dry-run mode for testing.

Requirements:
    - google-genai SDK (NOT google-generativeai)
    - Environment variables configured (see .env.example)

Usage:
    # Upload to existing store
    python upload_schema.py --store-id <store-id>

    # Create new store and upload
    python upload_schema.py --create-store

    # Dry run (show what would be uploaded)
    python upload_schema.py --store-id <store-id> --dry-run

Environment Variables:
    GOOGLE_API_KEY: Gemini API key (required)
    GEMINI_FILE_SEARCH_STORE_ID: Default store ID (optional)
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, ClassVar

# Add parent directory to path to access common modules
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from common.logging_utils import setup_pipeline_logger
except ImportError:
    # Fallback if common module not available
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    setup_pipeline_logger = None

# Import Google GenAI SDK
try:
    from google import genai
except ImportError:
    sys.exit(1)


class SchemaUploadError(Exception):
    """Custom exception for schema upload errors."""


class GeminiSchemaUploader:
    """Upload schema documentation to Gemini File Search Store."""

    # Default schema files to upload
    DEFAULT_SCHEMA_FILES: ClassVar[list[str]] = [
        "schema/tables.md",
        "schema/columns.md",
        "schema/relationships.md",
        "schema/example_queries.md",
    ]

    # Gemini model for operations
    MODEL_NAME = "gemini-2.0-flash-exp"

    def __init__(
        self,
        api_key: str,
        store_id: str | None = None,
        schema_dir: str = "schema",
        logger: logging.Logger | None = None,
    ):
        """
        Initialize schema uploader.

        Args:
            api_key: Google API key for Gemini
            store_id: Optional existing file search store ID
            schema_dir: Directory containing schema markdown files
            logger: Optional logger instance
        """
        self.api_key = api_key
        self.store_id = store_id
        self.schema_dir = Path(schema_dir)

        if logger:
            self.logger = logger
        elif setup_pipeline_logger:
            self.logger = setup_pipeline_logger(name="schema_upload", level="INFO", console_output=True)
        else:
            self.logger = logging.getLogger("schema_upload")

        # Initialize Gemini client
        self.client = genai.Client(api_key=self.api_key)

        self.upload_stats: dict[str, Any] = {
            "uploaded": 0,
            "skipped": 0,
            "deleted": 0,
            "errors": 0,
        }

    def create_file_search_store(self, display_name: str) -> str:
        """
        Create a new file search store.

        Args:
            display_name: Display name for the store

        Returns:
            Store ID

        Raises:
            SchemaUploadError: If store creation fails
        """
        try:
            self.logger.info(f"Creating new file search store: {display_name}")

            # Create store using the correct API
            corpus = self.client.corpora.create(display_name=display_name)

            store_id = corpus.name.split("/")[-1]  # Extract ID from name

            self.logger.info(f"✅ Created store: {store_id}")
            self.logger.info(f"   Display name: {display_name}")

            return store_id

        except Exception as e:
            raise SchemaUploadError(f"Failed to create file search store: {e}") from e

    def list_files_in_store(self) -> list[dict[str, Any]]:
        """
        List all files in the store.

        Returns:
            List of file metadata dictionaries

        Raises:
            SchemaUploadError: If listing fails
        """
        if not self.store_id:
            raise SchemaUploadError("No store ID provided")

        try:
            self.logger.info(f"Listing files in store: {self.store_id}")

            # List documents in corpus
            corpus_name = f"corpora/{self.store_id}"
            documents = self.client.corpora.list_documents(corpus=corpus_name)

            files = []
            for doc in documents:
                files.append(
                    {
                        "name": doc.name,
                        "display_name": doc.display_name,
                        "create_time": getattr(doc, "create_time", None),
                    }
                )

            self.logger.info(f"Found {len(files)} files in store")
            return files

        except Exception as e:
            raise SchemaUploadError(f"Failed to list files in store: {e}") from e

    def delete_old_files(self, dry_run: bool = False) -> int:
        """
        Delete all existing files from the store.

        Args:
            dry_run: If True, only log what would be deleted

        Returns:
            Number of files deleted (or would be deleted)

        Raises:
            SchemaUploadError: If deletion fails
        """
        if not self.store_id:
            raise SchemaUploadError("No store ID provided")

        try:
            files = self.list_files_in_store()

            if not files:
                self.logger.info("No files to delete")
                return 0

            deleted_count = 0
            for file in files:
                file_name = file.get("name", "unknown")
                display_name = file.get("display_name", "unknown")

                if dry_run:
                    self.logger.info(f"[DRY RUN] Would delete: {display_name} ({file_name})")
                else:
                    self.logger.info(f"Deleting: {display_name} ({file_name})")
                    try:
                        self.client.corpora.delete_document(name=file_name)
                        deleted_count += 1
                    except Exception as e:
                        self.logger.warning(f"Failed to delete {file_name}: {e}")

            if not dry_run:
                self.upload_stats["deleted"] = deleted_count
                self.logger.info(f"✅ Deleted {deleted_count} files")
            else:
                self.logger.info(f"[DRY RUN] Would delete {len(files)} files")

            return deleted_count if not dry_run else len(files)

        except Exception as e:
            raise SchemaUploadError(f"Failed to delete old files: {e}") from e

    def upload_file(self, file_path: Path, display_name: str | None = None, dry_run: bool = False) -> str | None:
        """
        Upload a single file to the store.

        Args:
            file_path: Path to file to upload
            display_name: Optional display name (defaults to filename)
            dry_run: If True, only log what would be uploaded

        Returns:
            Document name/ID if uploaded, None if skipped

        Raises:
            SchemaUploadError: If upload fails
        """
        if not self.store_id:
            raise SchemaUploadError("No store ID provided")

        if not file_path.exists():
            self.logger.warning(f"File not found: {file_path}")
            self.upload_stats["skipped"] += 1
            return None

        display_name = display_name or file_path.name

        try:
            file_size = file_path.stat().st_size
            file_size_kb = file_size / 1024

            if dry_run:
                self.logger.info(f"[DRY RUN] Would upload: {display_name} ({file_size_kb:.1f} KB)")
                return None

            self.logger.info(f"Uploading: {display_name} ({file_size_kb:.1f} KB)")

            # Read file content
            with file_path.open(encoding="utf-8") as f:
                content = f.read()

            # Upload to corpus as document
            corpus_name = f"corpora/{self.store_id}"
            document = self.client.corpora.create_document(
                corpus=corpus_name,
                display_name=display_name,
            )

            # Create chunk with content
            self.client.corpora.create_chunk(document=document.name, data={"string_value": content})

            self.upload_stats["uploaded"] += 1
            self.logger.info(f"✅ Uploaded: {display_name}")

            return document.name

        except Exception as e:
            self.upload_stats["errors"] += 1
            self.logger.error(f"Failed to upload {file_path}: {e}")
            return None

    def find_schema_files(self) -> list[Path]:
        """
        Find all schema markdown files to upload.

        Returns:
            List of file paths to upload
        """
        found_files = []

        for file_pattern in self.DEFAULT_SCHEMA_FILES:
            file_path = Path(file_pattern)

            # Try absolute path first
            if file_path.exists():
                found_files.append(file_path)
            # Try relative to schema_dir
            elif (self.schema_dir / file_path.name).exists():
                found_files.append(self.schema_dir / file_path.name)
            else:
                self.logger.warning(f"Schema file not found: {file_pattern}")

        # Also search for any other .md files in schema directory
        if self.schema_dir.exists():
            for md_file in self.schema_dir.glob("*.md"):
                if md_file not in found_files:
                    found_files.append(md_file)

        return sorted(found_files)

    def upload_all_schema_files(self, dry_run: bool = False, clean_first: bool = True) -> dict[str, Any]:
        """
        Upload all schema files to the store.

        Args:
            dry_run: If True, only show what would be uploaded
            clean_first: If True, delete old files before uploading

        Returns:
            Dictionary with upload statistics

        Raises:
            SchemaUploadError: If upload fails
        """
        self.logger.info("=" * 60)
        self.logger.info(f"{'[DRY RUN] ' if dry_run else ''}Uploading schema documentation")
        self.logger.info(f"Store ID: {self.store_id}")
        self.logger.info(f"Schema directory: {self.schema_dir}")
        self.logger.info("=" * 60)

        start_time = datetime.now()

        # Clean old files if requested
        if clean_first:
            self.delete_old_files(dry_run=dry_run)

        # Find files to upload
        files_to_upload = self.find_schema_files()

        if not files_to_upload:
            self.logger.warning("No schema files found to upload")
            return self.upload_stats

        self.logger.info(f"\nFound {len(files_to_upload)} files to upload:\n")
        for file_path in files_to_upload:
            self.logger.info(f"  - {file_path}")
        self.logger.info("")

        # Upload each file
        for file_path in files_to_upload:
            self.upload_file(file_path, dry_run=dry_run)

        duration = (datetime.now() - start_time).total_seconds()

        # Generate summary
        self.logger.info("=" * 60)
        self.logger.info(f"{'[DRY RUN] ' if dry_run else ''}Upload completed in {duration:.1f} seconds")
        self.logger.info(f"Files uploaded: {self.upload_stats['uploaded']}")
        self.logger.info(f"Files deleted: {self.upload_stats['deleted']}")
        self.logger.info(f"Files skipped: {self.upload_stats['skipped']}")
        self.logger.info(f"Errors: {self.upload_stats['errors']}")
        self.logger.info("=" * 60)

        return self.upload_stats


def validate_environment() -> dict[str, str]:
    """
    Validate that required environment variables are set.

    Returns:
        Dictionary with environment variable values

    Raises:
        SchemaUploadError: If required variables are missing
    """
    api_key = os.getenv("GOOGLE_API_KEY")

    if not api_key:
        raise SchemaUploadError(
            "Missing required environment variable: GOOGLE_API_KEY\nPlease set it in your .env file or environment."
        )

    return {
        "api_key": api_key,
        "store_id": os.getenv("GEMINI_FILE_SEARCH_STORE_ID"),
    }


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Upload schema documentation to Gemini File Search Store",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument("--create-store", action="store_true", help="Create a new File Search Store")
    parser.add_argument(
        "--store-id",
        type=str,
        help="Existing store ID to upload to (or from GEMINI_FILE_SEARCH_STORE_ID env var)",
    )
    parser.add_argument(
        "--store-name",
        type=str,
        default="Landbruget.dk Schema Documentation",
        help="Display name for new store (default: 'Landbruget.dk Schema Documentation')",
    )
    parser.add_argument(
        "--schema-dir",
        type=str,
        default="schema",
        help="Directory containing schema markdown files (default: schema)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be uploaded without uploading",
    )
    parser.add_argument(
        "--no-clean",
        action="store_true",
        help="Don't delete old files before uploading",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed progress")

    args = parser.parse_args()

    # Setup logging
    if setup_pipeline_logger:
        logger = setup_pipeline_logger(
            name="schema_upload",
            level="DEBUG" if args.verbose else "INFO",
            console_output=True,
        )
    else:
        logger = logging.getLogger("schema_upload")
        logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)

    try:
        # Validate environment
        env_vars = validate_environment()

        # Determine store ID
        store_id = args.store_id or env_vars.get("store_id")

        if args.create_store:
            # Create new store
            uploader = GeminiSchemaUploader(
                api_key=env_vars["api_key"],
                schema_dir=args.schema_dir,
                logger=logger,
            )

            store_id = uploader.create_file_search_store(display_name=args.store_name)

            logger.info("")
            logger.info("=" * 60)
            logger.info("Store created successfully!")
            logger.info(f"Store ID: {store_id}")
            logger.info("")
            logger.info("Add this to your .env file:")
            logger.info(f"GEMINI_FILE_SEARCH_STORE_ID={store_id}")
            logger.info("=" * 60)
            logger.info("")

            # Set for upload
            uploader.store_id = store_id
        else:
            if not store_id:
                raise SchemaUploadError(
                    "No store ID provided. Either:\n"
                    "1. Use --create-store to create a new store\n"
                    "2. Use --store-id <id> to specify an existing store\n"
                    "3. Set GEMINI_FILE_SEARCH_STORE_ID in your .env file"
                )

            # Create uploader with existing store
            uploader = GeminiSchemaUploader(
                api_key=env_vars["api_key"],
                store_id=store_id,
                schema_dir=args.schema_dir,
                logger=logger,
            )

        # Upload files
        results = uploader.upload_all_schema_files(dry_run=args.dry_run, clean_first=not args.no_clean)

        # Return appropriate exit code
        if results["errors"] > 0:
            return 1
        return 0

    except SchemaUploadError as e:
        logger.error(f"Upload error: {e}")
        return 1
    except KeyboardInterrupt:
        logger.warning("Upload interrupted by user")
        return 130
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
