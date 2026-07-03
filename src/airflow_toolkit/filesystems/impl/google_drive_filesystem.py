from __future__ import annotations

import logging
from io import BytesIO

from airflow_toolkit.filesystems.filesystem_protocol import FilesystemProtocol
from airflow_toolkit.providers.google.hooks.drive import GoogleDriveHook

logger = logging.getLogger(__name__)


class GoogleDriveFilesystem(FilesystemProtocol):
    """
    Path convention: folder/subfolder/file.csv (no leading slash, from Drive root).

    Paths are resolved to Google Drive file/folder IDs via name-based traversal.
    IDs are cached in-memory for the lifetime of this instance (one Airflow task).

    Supports both personal Drive (root = "root") and Shared Drives (Team Drives).
    Set `shared_drive_id` in the connection extra to enable Shared Drive mode.
    """

    def __init__(self, hook: GoogleDriveHook):
        self.hook = hook
        self._id_cache: dict[str, str] = {}

    @property
    def _root_id(self) -> str:
        return self.hook.shared_drive_id or "root"

    @property
    def _drive_list_kwargs(self) -> dict:
        if self.hook.shared_drive_id:
            return {
                "includeItemsFromAllDrives": True,
                "supportsAllDrives": True,
                "corpora": "drive",
                "driveId": self.hook.shared_drive_id,
            }
        return {}

    # ── path → ID resolution ─────────────────────────────────────────────────

    def _resolve_id(self, path: str, is_folder: bool = False) -> str | None:
        path = path.strip("/")
        if path in self._id_cache:
            return self._id_cache[path]

        service = self.hook.get_service()
        parts = [p for p in path.split("/") if p]
        parent_id = self._root_id
        accumulated = ""

        for i, part in enumerate(parts):
            is_last = i == len(parts) - 1
            q = f"name='{part}' and '{parent_id}' in parents and trashed=false"
            if not is_last or is_folder:
                q += " and mimeType='application/vnd.google-apps.folder'"

            result = (
                service.files()
                .list(
                    q=q, fields="files(id)", spaces="drive", **self._drive_list_kwargs
                )
                .execute()
            )
            files = result.get("files", [])
            if not files:
                return None
            parent_id = files[0]["id"]
            accumulated = f"{accumulated}/{part}".lstrip("/")
            self._id_cache[accumulated] = parent_id

        return parent_id

    def _ensure_folder(self, path: str) -> str:
        path = path.strip("/")
        if not path:
            return self._root_id
        if path in self._id_cache:
            return self._id_cache[path]

        service = self.hook.get_service()
        parts = [p for p in path.split("/") if p]
        parent_id = self._root_id
        accumulated = ""

        for part in parts:
            accumulated = f"{accumulated}/{part}".lstrip("/")
            if accumulated in self._id_cache:
                parent_id = self._id_cache[accumulated]
                continue

            q = f"name='{part}' and '{parent_id}' in parents and trashed=false and mimeType='application/vnd.google-apps.folder'"
            result = (
                service.files()
                .list(
                    q=q, fields="files(id)", spaces="drive", **self._drive_list_kwargs
                )
                .execute()
            )
            files = result.get("files", [])

            if files:
                parent_id = files[0]["id"]
            else:
                meta: dict = {
                    "name": part,
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": [parent_id],
                }
                folder = (
                    service.files()
                    .create(body=meta, fields="id", supportsAllDrives=True)
                    .execute()
                )
                parent_id = folder["id"]

            self._id_cache[accumulated] = parent_id

        return parent_id

    # ── FilesystemProtocol ────────────────────────────────────────────────────

    def read(self, path: str) -> bytes:
        from googleapiclient.http import MediaIoBaseDownload

        service = self.hook.get_service()
        file_id = self._resolve_id(path)
        if not file_id:
            raise FileNotFoundError(f"File not found in Google Drive: {path}")

        request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
        out = BytesIO()
        downloader = MediaIoBaseDownload(out, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        out.seek(0)
        return out.read()

    def write(self, data: str | bytes | BytesIO, path: str):
        from googleapiclient.http import MediaIoBaseUpload

        if isinstance(data, str):
            data = data.encode()
        elif isinstance(data, BytesIO):
            data = data.getvalue()

        service = self.hook.get_service()
        parts = path.strip("/").split("/")
        filename = parts[-1]
        folder_path = "/".join(parts[:-1])
        parent_id = self._ensure_folder(folder_path) if folder_path else self._root_id

        media = MediaIoBaseUpload(
            BytesIO(data), mimetype="application/octet-stream", resumable=False
        )
        existing_id = self._resolve_id(path)

        if existing_id:
            service.files().update(
                fileId=existing_id,
                media_body=media,
                supportsAllDrives=True,
            ).execute()
        else:
            meta = {"name": filename, "parents": [parent_id]}
            service.files().create(
                body=meta,
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            ).execute()

    def delete_file(self, path: str):
        path = path.strip("/")
        service = self.hook.get_service()
        file_id = self._resolve_id(path)
        if file_id:
            logger.info(f'Deleting Google Drive file "{path}" (id={file_id})')
            service.files().delete(fileId=file_id, supportsAllDrives=True).execute()
            self._id_cache.pop(path, None)

    def create_prefix(self, prefix: str):
        self._ensure_folder(prefix)

    def delete_prefix(self, prefix: str):
        prefix = prefix.strip("/")
        service = self.hook.get_service()
        folder_id = self._resolve_id(prefix, is_folder=True)
        if folder_id:
            logger.info(f'Deleting Google Drive folder "{prefix}" (id={folder_id})')
            service.files().delete(fileId=folder_id, supportsAllDrives=True).execute()
            self._id_cache = {
                k: v for k, v in self._id_cache.items() if not k.startswith(prefix)
            }

    def check_file(self, path: str) -> bool:
        return self._resolve_id(path) is not None

    def check_prefix(self, prefix: str) -> bool:
        return self._resolve_id(prefix, is_folder=True) is not None

    def list_files(self, prefix: str) -> list[str]:
        results: list[str] = []
        folder_id = self._resolve_id(prefix.strip("/"), is_folder=True)
        if folder_id:
            self._list_recursive(
                self.hook.get_service(), folder_id, prefix.rstrip("/"), results
            )
        return results

    def _list_recursive(
        self, service, folder_id: str, prefix_path: str, results: list[str]
    ) -> None:
        q = f"'{folder_id}' in parents and trashed=false"
        result = (
            service.files()
            .list(
                q=q,
                fields="files(id,name,mimeType)",
                spaces="drive",
                **self._drive_list_kwargs,
            )
            .execute()
        )
        for item in result.get("files", []):
            full_path = f"{prefix_path}/{item['name']}"
            if item["mimeType"] == "application/vnd.google-apps.folder":
                self._list_recursive(service, item["id"], full_path, results)
            else:
                results.append(full_path)
