"""Pydantic models for Google Drive API responses."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class DriveFile(BaseModel):
    """Model for a Google Drive file."""

    id: str = Field(..., description="Google Drive file ID")
    name: str = Field(..., description="File name")
    mime_type: str = Field(..., description="MIME type of the file")
    parent_ids: list[str] = Field(default_factory=list, description="Parent folder IDs")
    modified_time: datetime = Field(..., description="Last modified time")
    size: int | None = Field(None, description="File size in bytes")
    web_view_link: str | None = Field(None, description="Web view link")
    download_url: str | None = Field(None, description="Download URL")
    path: str | None = Field(None, description="Full path within Drive")

    @classmethod
    def from_api_response(cls, file_data: dict[str, Any], parent_path: str = "") -> "DriveFile":
        """Create a DriveFile from a Google Drive API response.

        Args:
            file_data: Google Drive API response for a file
            parent_path: Path of the parent folder

        Returns:
            DriveFile instance
        """
        parents = file_data.get("parents", [])
        path = f"{parent_path}/{file_data['name']}" if parent_path else file_data["name"]
        
        return cls(
            id=file_data["id"],
            name=file_data["name"],
            mime_type=file_data["mimeType"],
            parent_ids=parents,
            modified_time=datetime.fromisoformat(file_data["modifiedTime"].replace("Z", "+00:00")),
            size=int(file_data.get("size", 0)) if "size" in file_data else None,
            web_view_link=file_data.get("webViewLink"),
            download_url=None,  # Will be set by the fetcher
            path=path,
        )


class DriveFolder(BaseModel):
    """Model for a Google Drive folder."""

    id: str = Field(..., description="Google Drive folder ID")
    name: str = Field(..., description="Folder name")
    parent_ids: list[str] = Field(default_factory=list, description="Parent folder IDs")
    path: str = Field("", description="Full path of the folder")
    files: list[DriveFile] = Field(default_factory=list, description="Files in the folder")
    subfolders: list["DriveFolder"] = Field(default_factory=list, description="Subfolders")

    @classmethod
    def from_api_response(cls, folder_data: dict[str, Any], parent_path: str = "") -> "DriveFolder":
        """Create a DriveFolder from a Google Drive API response.

        Args:
            folder_data: Google Drive API response for a folder
            parent_path: Path of the parent folder

        Returns:
            DriveFolder instance
        """
        parents = folder_data.get("parents", [])
        path = f"{parent_path}/{folder_data['name']}" if parent_path else folder_data["name"]
        
        return cls(
            id=folder_data["id"],
            name=folder_data["name"],
            parent_ids=parents,
            path=path,
        ) 