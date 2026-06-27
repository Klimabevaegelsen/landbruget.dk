"""Publish exported research datasets to external repositories.

The command is intentionally conservative:

* it validates the exported bundle before any network call;
* it creates a single archive for repository upload;
* it defaults to a dry-run plan;
* final publication requires --confirm-publish.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import os
import sys
import uuid
import zipfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

import duckdb

DEFAULT_DATASET_NAME = "pesticide-field-use-allocations"
DEFAULT_DATASET_VERSION = "v1"
DEFAULT_ARCHIVE_NAME = f"{DEFAULT_DATASET_NAME}-{DEFAULT_DATASET_VERSION}.zip"
DEFAULT_TITLE = "Landbruget.dk historical pesticide field-use allocations for Denmark"
DEFAULT_DESCRIPTION = (
    "Field-level allocations of cumulative reported pesticide product use for Denmark. "
    "Rows are not individual spray events, and public files exclude raw CVR numbers."
)


class PublicationError(RuntimeError):
    """Raised when a dataset cannot be published safely."""


@dataclass(frozen=True)
class PublicationMetadata:
    title: str
    description: str
    creators: list[dict[str, str]]
    keywords: list[str]
    version: str
    publication_date: str
    license_id: str | None
    canonical_doi: str | None


@dataclass(frozen=True)
class PublicationResult:
    target: str
    status: str
    record_id: str | None = None
    url: str | None = None
    doi: str | None = None
    notes: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "target": self.target,
            "status": self.status,
            "record_id": self.record_id,
            "url": self.url,
            "doi": self.doi,
            "notes": list(self.notes),
        }


class DatasetBundle:
    """Local dataset export plus derived upload archive."""

    def __init__(
        self,
        source_dir: Path,
        *,
        archive_path: Path | None = None,
        dataset_name: str = DEFAULT_DATASET_NAME,
        version: str = DEFAULT_DATASET_VERSION,
    ) -> None:
        self.source_dir = source_dir.resolve()
        self.dataset_name = dataset_name
        self.version = version
        self.archive_path = (
            archive_path.resolve()
            if archive_path
            else self.source_dir.parent / f"{dataset_name}-{version}.zip"
        )

    def validate(self) -> list[str]:
        errors: list[str] = []
        if not self.source_dir.exists():
            errors.append(f"source directory does not exist: {self.source_dir}")
            return errors
        if not self.source_dir.is_dir():
            errors.append(f"source path is not a directory: {self.source_dir}")
            return errors

        required = [
            "README.md",
            "datapackage.json",
            "checksums.txt",
            "examples/duckdb.sql",
            "quality/all_years.csv",
        ]
        errors.extend(
            f"missing required dataset file: {relative_path}"
            for relative_path in required
            if not (self.source_dir / relative_path).exists()
        )

        if not (self.source_dir / "use_allocations").is_dir():
            errors.append("missing use_allocations/ resource directory")
        if (self.source_dir / "applications").exists():
            errors.append("found legacy applications/ directory; publish use_allocations/ instead")

        errors.extend(self._validate_checksums())
        errors.extend(self._validate_parquet_privacy())
        return errors

    def iter_files(self) -> list[Path]:
        return sorted(path for path in self.source_dir.rglob("*") if path.is_file())

    def create_archive(self) -> Path:
        validation_errors = self.validate()
        if validation_errors:
            raise PublicationError(
                "dataset validation failed:\n- " + "\n- ".join(validation_errors)
            )

        self.archive_path.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(
            self.archive_path,
            "w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=9,
        ) as archive:
            for path in self.iter_files():
                archive.write(path, path.relative_to(self.source_dir).as_posix())
        return self.archive_path

    def load_metadata(self, args: argparse.Namespace) -> PublicationMetadata:
        datapackage_path = self.source_dir / "datapackage.json"
        datapackage: dict[str, Any] = {}
        if datapackage_path.exists():
            datapackage = json.loads(datapackage_path.read_text(encoding="utf-8"))

        creators = [{"name": creator} for creator in args.creator]
        if not creators:
            creators = [{"name": "Klimabevægelsen / Landbruget.dk"}]

        return PublicationMetadata(
            title=args.title or str(datapackage.get("title") or DEFAULT_TITLE),
            description=args.description
            or str(datapackage.get("description") or DEFAULT_DESCRIPTION),
            creators=creators,
            keywords=list(datapackage.get("keywords") or []),
            version=args.version,
            publication_date=args.publication_date,
            license_id=args.license_id,
            canonical_doi=args.canonical_doi,
        )

    def _validate_checksums(self) -> list[str]:
        checksums_path = self.source_dir / "checksums.txt"
        if not checksums_path.exists():
            return []

        errors: list[str] = []
        for line_number, line in enumerate(
            checksums_path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            if not line.strip():
                continue
            try:
                expected, relative_path = line.split(None, 1)
            except ValueError:
                errors.append(f"invalid checksums.txt line {line_number}: {line}")
                continue
            file_path = self.source_dir / relative_path.strip()
            if not file_path.exists():
                errors.append(f"checksum references missing file: {relative_path}")
                continue
            actual = sha256_file(file_path)
            if actual != expected:
                errors.append(
                    f"checksum mismatch for {relative_path}: expected {expected}, got {actual}"
                )
        return errors

    def _validate_parquet_privacy(self) -> list[str]:
        errors: list[str] = []
        parquet_files = sorted(self.source_dir.rglob("*.parquet"))
        if not parquet_files:
            return ["no parquet files found in dataset bundle"]

        with duckdb.connect(":memory:") as conn:
            for path in parquet_files:
                columns = conn.execute(
                    "DESCRIBE SELECT * FROM read_parquet(?)",
                    [str(path)],
                ).fetchall()
                blocked = [
                    row[0] for row in columns if "cvr" in str(row[0]).lower().replace("-", "_")
                ]
                if blocked:
                    relative_path = path.relative_to(self.source_dir).as_posix()
                    errors.append(f"{relative_path} exposes CVR-like columns: {', '.join(blocked)}")
        return errors


class HttpClient:
    """Small JSON/binary HTTP client for repository APIs."""

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        json_body: Any | None = None,
        data: bytes | None = None,
    ) -> tuple[int, dict[str, str], Any]:
        request_headers = headers.copy() if headers else {}
        body = data
        if json_body is not None:
            body = json.dumps(json_body).encode("utf-8")
            request_headers.setdefault("Content-Type", "application/json")

        request = Request(url, data=body, headers=request_headers, method=method.upper())
        try:
            with urlopen(request, timeout=120) as response:
                response_body = response.read()
                return (
                    response.status,
                    dict(response.headers.items()),
                    parse_response(response_body),
                )
        except HTTPError as exc:
            body_text = exc.read().decode("utf-8", errors="replace")
            raise PublicationError(
                f"{method} {url} failed with HTTP {exc.code}: {body_text}"
            ) from exc
        except URLError as exc:
            raise PublicationError(f"{method} {url} failed: {exc}") from exc

    def upload_file(
        self,
        method: str,
        url: str,
        path: Path,
        *,
        headers: dict[str, str] | None = None,
        content_type: str = "application/octet-stream",
    ) -> tuple[int, dict[str, str], Any]:
        request_headers = headers.copy() if headers else {}
        request_headers.setdefault("Content-Type", content_type)
        return self.request(method, url, headers=request_headers, data=path.read_bytes())

    def multipart(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        fields: dict[str, str] | None = None,
        files: dict[str, tuple[str, bytes, str]] | None = None,
    ) -> tuple[int, dict[str, str], Any]:
        boundary = f"landbruget-{uuid.uuid4().hex}"
        body = build_multipart_body(boundary, fields or {}, files or {})
        request_headers = headers.copy() if headers else {}
        request_headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
        return self.request(method, url, headers=request_headers, data=body)


def publish_zenodo(
    *,
    bundle: DatasetBundle,
    metadata: PublicationMetadata,
    archive_path: Path,
    client: HttpClient,
    dry_run: bool,
    confirm_publish: bool,
    sandbox: bool,
) -> PublicationResult:
    token = os.getenv("ZENODO_TOKEN")
    base_url = "https://sandbox.zenodo.org" if sandbox else "https://zenodo.org"
    if dry_run:
        return PublicationResult(
            target="zenodo",
            status="dry-run",
            notes=(
                f"would create deposition at {base_url}",
                f"would upload {archive_path.name} ({archive_path.stat().st_size} bytes)",
                "would leave draft unpublished unless --confirm-publish is supplied",
            ),
        )
    require_token("ZENODO_TOKEN", token)
    require_license(metadata, "Zenodo")

    headers = {"Authorization": f"Bearer {token}"}
    _, _, created = client.request(
        "POST",
        f"{base_url}/api/deposit/depositions",
        headers=headers,
        json_body={},
    )
    deposition_id = str(created["id"])
    bucket_url = str(created["links"]["bucket"])

    client.request(
        "PUT",
        f"{base_url}/api/deposit/depositions/{deposition_id}",
        headers=headers,
        json_body={"metadata": zenodo_metadata(metadata)},
    )
    upload_url = f"{bucket_url}/{quote(archive_path.name)}"
    client.upload_file("PUT", upload_url, archive_path, headers=headers)

    status = "draft"
    if confirm_publish:
        _, _, published = client.request(
            "POST",
            f"{base_url}/api/deposit/depositions/{deposition_id}/actions/publish",
            headers=headers,
        )
        status = "published"
        doi = extract_nested(published, "metadata", "doi")
        url = extract_nested(published, "links", "html")
    else:
        _, _, draft = client.request(
            "GET",
            f"{base_url}/api/deposit/depositions/{deposition_id}",
            headers=headers,
        )
        doi = extract_nested(draft, "metadata", "prereserve_doi", "doi")
        url = extract_nested(draft, "links", "html")

    return PublicationResult(
        target="zenodo",
        status=status,
        record_id=deposition_id,
        url=url,
        doi=doi,
        notes=(f"uploaded archive for {bundle.dataset_name} {bundle.version}",),
    )


def publish_figshare(
    *,
    metadata: PublicationMetadata,
    archive_path: Path,
    client: HttpClient,
    dry_run: bool,
    confirm_publish: bool,
    create_private_link: bool,
    figshare_license_id: int | None,
) -> PublicationResult:
    token = os.getenv("FIGSHARE_TOKEN")
    base_url = "https://api.figshare.com/v2"
    if dry_run:
        return PublicationResult(
            target="figshare",
            status="dry-run",
            notes=(
                "would create a private dataset article",
                f"would upload {archive_path.name} in Figshare upload-service parts",
                "would publish only with --confirm-publish",
            ),
        )
    require_token("FIGSHARE_TOKEN", token)
    if figshare_license_id is None:
        raise PublicationError("Figshare publishing requires --figshare-license-id")

    headers = {"Authorization": f"token {token}"}
    article_payload: dict[str, Any] = {
        "title": metadata.title,
        "description": metadata.description,
        "defined_type": "dataset",
        "authors": metadata.creators,
        "keywords": metadata.keywords,
        "license": figshare_license_id,
    }
    if metadata.canonical_doi:
        article_payload["references"] = [
            f"https://doi.org/{metadata.canonical_doi.removeprefix('doi:')}"
        ]

    _, headers_out, article_body = client.request(
        "POST",
        f"{base_url}/account/articles",
        headers=headers,
        json_body=article_payload,
    )
    article_url = absolute_api_location(location_from(headers_out, article_body), base_url)
    _, _, article = client.request("GET", article_url, headers=headers)
    article_id = str(article["id"])

    upload_figshare_file(client, headers, base_url, article_id, archive_path)

    private_url: str | None = None
    if create_private_link:
        _, private_headers, private_body = client.request(
            "POST",
            f"{base_url}/account/articles/{article_id}/private_links",
            headers=headers,
            json_body={},
        )
        private_url = location_from(private_headers, private_body)

    status = "draft"
    public_url = private_url or f"https://figshare.com/account/articles/{article_id}"
    if confirm_publish:
        _, publish_headers, _ = client.request(
            "POST",
            f"{base_url}/account/articles/{article_id}/publish",
            headers=headers,
        )
        status = "published"
        public_url = publish_headers.get("Location", public_url)

    return PublicationResult(
        target="figshare",
        status=status,
        record_id=article_id,
        url=public_url,
        notes=(f"uploaded archive {archive_path.name}",),
    )


def publish_dataverse(
    *,
    metadata: PublicationMetadata,
    archive_path: Path,
    client: HttpClient,
    dry_run: bool,
    confirm_publish: bool,
    dataverse_server: str,
    dataverse_collection: str,
    dataverse_subject: str,
) -> PublicationResult:
    token = os.getenv("DATAVERSE_TOKEN")
    server = dataverse_server.rstrip("/")
    if dry_run:
        return PublicationResult(
            target="dataverse",
            status="dry-run",
            notes=(
                f"would create dataset under {server}/dataverse/{dataverse_collection}",
                f"would upload {archive_path.name} as one dataset file",
                "would publish only with --confirm-publish",
            ),
        )
    require_token("DATAVERSE_TOKEN", token)

    headers = {"X-Dataverse-key": token}
    _, _, created = client.request(
        "POST",
        f"{server}/api/dataverses/{quote(dataverse_collection)}/datasets",
        headers={**headers, "Content-Type": "application/json"},
        json_body=dataverse_dataset_json(metadata, dataverse_subject),
    )
    dataset = created.get("data", created)
    persistent_id = str(dataset.get("persistentId") or dataset.get("persistent_id") or "")
    dataset_id = str(dataset.get("id") or "")
    if not persistent_id and not dataset_id:
        raise PublicationError(f"Dataverse create response did not include dataset id: {created}")

    dataset_ref = ":persistentId" if persistent_id else dataset_id
    query = f"?{urlencode({'persistentId': persistent_id})}" if persistent_id else ""
    client.multipart(
        "POST",
        f"{server}/api/datasets/{dataset_ref}/add{query}",
        headers=headers,
        fields={
            "jsonData": json.dumps(
                {
                    "description": "Versioned archive of public pesticide field-use allocation dataset.",
                    "categories": ["Data"],
                    "restrict": "false",
                }
            )
        },
        files={
            "file": (
                archive_path.name,
                archive_path.read_bytes(),
                mimetypes.guess_type(archive_path.name)[0] or "application/zip",
            )
        },
    )

    status = "draft"
    if confirm_publish:
        if persistent_id:
            publish_url = (
                f"{server}/api/datasets/:persistentId/actions/:publish?"
                f"{urlencode({'persistentId': persistent_id, 'type': 'major'})}"
            )
        else:
            publish_url = f"{server}/api/datasets/{dataset_id}/actions/:publish?type=major"
        client.request(
            "POST",
            publish_url,
            headers=headers,
        )
        status = "published"

    url = f"{server}/dataset.xhtml?persistentId={quote(persistent_id)}" if persistent_id else None
    return PublicationResult(
        target="dataverse",
        status=status,
        record_id=persistent_id or dataset_id,
        url=url,
        doi=persistent_id if persistent_id.startswith("doi:") else None,
        notes=(f"uploaded archive {archive_path.name}",),
    )


def upload_figshare_file(
    client: HttpClient,
    headers: dict[str, str],
    base_url: str,
    article_id: str,
    archive_path: Path,
) -> None:
    md5_digest, file_size = md5_file(archive_path)
    _, upload_headers, upload_body = client.request(
        "POST",
        f"{base_url}/account/articles/{article_id}/files",
        headers=headers,
        json_body={"name": archive_path.name, "md5": md5_digest, "size": file_size},
    )
    file_url = absolute_api_location(location_from(upload_headers, upload_body), base_url)
    _, _, file_info = client.request("GET", file_url, headers=headers)
    upload_url = str(file_info["upload_url"])
    file_id = str(file_info["id"])

    _, _, parts_response = client.request("GET", upload_url, headers=headers)
    with archive_path.open("rb") as handle:
        for part in parts_response["parts"]:
            handle.seek(int(part["startOffset"]))
            data = handle.read(int(part["endOffset"]) - int(part["startOffset"]) + 1)
            client.request("PUT", f"{upload_url}/{part['partNo']}", headers=headers, data=data)
    client.request(
        "POST", f"{base_url}/account/articles/{article_id}/files/{file_id}", headers=headers
    )


def zenodo_metadata(metadata: PublicationMetadata) -> dict[str, Any]:
    related_identifiers = []
    if metadata.canonical_doi:
        related_identifiers.append(
            {
                "identifier": metadata.canonical_doi.removeprefix("doi:"),
                "relation": "isAlternateIdentifier",
                "scheme": "doi",
            }
        )

    return {
        "upload_type": "dataset",
        "title": metadata.title,
        "creators": metadata.creators,
        "description": metadata.description,
        "keywords": metadata.keywords,
        "version": metadata.version,
        "publication_date": metadata.publication_date,
        "access_right": "open",
        "license": metadata.license_id,
        "related_identifiers": related_identifiers,
    }


def dataverse_dataset_json(metadata: PublicationMetadata, subject: str) -> dict[str, Any]:
    fields: list[dict[str, Any]] = [
        primitive_field("title", metadata.title),
        compound_field(
            "author",
            [
                {
                    "authorName": primitive_field("authorName", creator["name"]),
                }
                for creator in metadata.creators
            ],
        ),
        compound_field(
            "datasetContact",
            [
                {
                    "datasetContactEmail": primitive_field(
                        "datasetContactEmail",
                        os.getenv("DATAVERSE_CONTACT_EMAIL", "kontakt@landbruget.dk"),
                    )
                }
            ],
        ),
        compound_field(
            "dsDescription",
            [
                {
                    "dsDescriptionValue": primitive_field(
                        "dsDescriptionValue",
                        metadata.description,
                    )
                }
            ],
        ),
        controlled_field("subject", [subject]),
    ]
    if metadata.keywords:
        fields.append(
            compound_field(
                "keyword",
                [
                    {"keywordValue": primitive_field("keywordValue", keyword)}
                    for keyword in metadata.keywords
                ],
            )
        )
    if metadata.canonical_doi:
        fields.append(primitive_field("otherId", metadata.canonical_doi))

    return {"datasetVersion": {"metadataBlocks": {"citation": {"fields": fields}}}}


def primitive_field(type_name: str, value: str) -> dict[str, Any]:
    return {
        "typeName": type_name,
        "multiple": False,
        "typeClass": "primitive",
        "value": value,
    }


def controlled_field(type_name: str, values: list[str]) -> dict[str, Any]:
    return {
        "typeName": type_name,
        "multiple": True,
        "typeClass": "controlledVocabulary",
        "value": values,
    }


def compound_field(type_name: str, values: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "typeName": type_name,
        "multiple": True,
        "typeClass": "compound",
        "value": values,
    }


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def md5_file(path: Path) -> tuple[str, int]:
    digest = hashlib.md5(usedforsecurity=False)
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            size += len(chunk)
            digest.update(chunk)
    return digest.hexdigest(), size


def parse_response(data: bytes) -> Any:
    if not data:
        return {}
    try:
        return json.loads(data.decode("utf-8"))
    except ValueError:
        return data


def build_multipart_body(
    boundary: str,
    fields: dict[str, str],
    files: dict[str, tuple[str, bytes, str]],
) -> bytes:
    chunks: list[bytes] = []
    for name, value in fields.items():
        chunks.extend(
            [
                f"--{boundary}\r\n".encode(),
                f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode(),
                value.encode("utf-8"),
                b"\r\n",
            ]
        )
    for name, (filename, content, content_type) in files.items():
        chunks.extend(
            [
                f"--{boundary}\r\n".encode(),
                (
                    f'Content-Disposition: form-data; name="{name}"; filename="{filename}"\r\n'
                ).encode(),
                f"Content-Type: {content_type}\r\n\r\n".encode(),
                content,
                b"\r\n",
            ]
        )
    chunks.append(f"--{boundary}--\r\n".encode())
    return b"".join(chunks)


def require_token(name: str, token: str | None) -> None:
    if not token:
        raise PublicationError(f"{name} is required for non-dry-run publishing")


def require_license(metadata: PublicationMetadata, platform_name: str) -> None:
    if not metadata.license_id:
        raise PublicationError(f"{platform_name} publishing requires --license-id")


def location_from(headers: dict[str, str], body: Any) -> str:
    if headers.get("Location"):
        return headers["Location"]
    if isinstance(body, dict):
        location = body.get("location") or body.get("Location")
        if location:
            return str(location)
    raise PublicationError(f"response did not include a Location header or body field: {body}")


def absolute_api_location(location: str, base_url: str) -> str:
    if location.startswith("http://") or location.startswith("https://"):
        return location
    return f"{base_url.rstrip('/')}/{location.lstrip('/')}"


def extract_nested(body: Any, *keys: str) -> str | None:
    current = body
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return str(current) if current is not None else None


def run(args: argparse.Namespace, client: HttpClient | None = None) -> list[PublicationResult]:
    bundle = DatasetBundle(
        Path(args.source_dir),
        archive_path=Path(args.archive_path) if args.archive_path else None,
        dataset_name=args.dataset_name,
        version=args.version,
    )
    validation_errors = bundle.validate()
    if validation_errors:
        raise PublicationError("dataset validation failed:\n- " + "\n- ".join(validation_errors))

    archive_path = bundle.create_archive()
    metadata = bundle.load_metadata(args)
    http_client = client or HttpClient()
    dry_run = args.dry_run or not (args.create_draft or args.confirm_publish)

    results: list[PublicationResult] = []
    for target in args.target:
        if target == "zenodo":
            results.append(
                publish_zenodo(
                    bundle=bundle,
                    metadata=metadata,
                    archive_path=archive_path,
                    client=http_client,
                    dry_run=dry_run,
                    confirm_publish=args.confirm_publish,
                    sandbox=args.zenodo_sandbox,
                )
            )
        elif target == "figshare":
            results.append(
                publish_figshare(
                    metadata=metadata,
                    archive_path=archive_path,
                    client=http_client,
                    dry_run=dry_run,
                    confirm_publish=args.confirm_publish,
                    create_private_link=args.figshare_private_link,
                    figshare_license_id=args.figshare_license_id,
                )
            )
        elif target == "dataverse":
            results.append(
                publish_dataverse(
                    metadata=metadata,
                    archive_path=archive_path,
                    client=http_client,
                    dry_run=dry_run,
                    confirm_publish=args.confirm_publish,
                    dataverse_server=args.dataverse_server,
                    dataverse_collection=args.dataverse_collection,
                    dataverse_subject=args.dataverse_subject,
                )
            )
        else:
            raise PublicationError(f"unknown publication target: {target}")
    return results


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-dir", required=True, help="Path to exported dataset version root."
    )
    parser.add_argument(
        "--archive-path", help="Optional output path for the generated zip archive."
    )
    parser.add_argument("--dataset-name", default=DEFAULT_DATASET_NAME)
    parser.add_argument("--version", default=DEFAULT_DATASET_VERSION)
    parser.add_argument(
        "--target",
        action="append",
        choices=["zenodo", "figshare", "dataverse"],
        required=True,
        help="Publication target. Repeat to publish to multiple repositories.",
    )
    parser.add_argument("--title")
    parser.add_argument("--description")
    parser.add_argument("--creator", action="append", default=[])
    parser.add_argument("--publication-date", default=date.today().isoformat())
    parser.add_argument("--license-id", help="Repository license id, e.g. cc-by-4.0 for Zenodo.")
    parser.add_argument("--canonical-doi", help="Canonical DOI to cross-reference on mirrors.")
    parser.add_argument("--dry-run", action="store_true", help="Validate and print plan only.")
    parser.add_argument(
        "--create-draft",
        action="store_true",
        help="Create draft/private records and upload files without final publication.",
    )
    parser.add_argument(
        "--confirm-publish",
        action="store_true",
        help="Create/upload and final-publish records. Without --create-draft or this flag, the command is a dry-run.",
    )
    parser.add_argument("--zenodo-sandbox", action="store_true", help="Use sandbox.zenodo.org.")
    parser.add_argument("--figshare-license-id", type=int)
    parser.add_argument("--figshare-private-link", action="store_true")
    parser.add_argument(
        "--dataverse-server",
        default=os.getenv("DATAVERSE_SERVER", "https://dataverse.harvard.edu"),
    )
    parser.add_argument(
        "--dataverse-collection",
        default=os.getenv("DATAVERSE_COLLECTION_ALIAS", "root"),
    )
    parser.add_argument("--dataverse-subject", default="Agricultural Sciences")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        results = run(args)
    except PublicationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(json.dumps([result.as_dict() for result in results], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
