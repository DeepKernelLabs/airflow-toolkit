import logging
from io import BytesIO

from airflow.providers.google.cloud.hooks.gcs import GCSHook

from airflow_toolkit.filesystems.filesystem_protocol import FilesystemProtocol

logger = logging.getLogger(__name__)


class GCSFilesystem(FilesystemProtocol):
    def __init__(self, hook: GCSHook):
        self.hook = hook

    def read(self, path: str) -> bytes:
        bucket_name, object_name = _get_bucket_and_key_name(path)
        return self.hook.download(bucket_name, object_name)

    def write(self, data: str | bytes | BytesIO, path: str):
        bucket_name, object_name = _get_bucket_and_key_name(path)
        if isinstance(data, BytesIO):
            data = data.getvalue()
        self.hook.upload(bucket_name, object_name, data=data)

    def delete_file(self, path: str):
        bucket_name, object_name = _get_bucket_and_key_name(path)
        logger.info(f'Deleting gcs object "{object_name}" from bucket "{bucket_name}"')
        self.hook.delete(bucket_name, object_name)

    def create_prefix(self, prefix: str):
        bucket_name, key_prefix = _get_bucket_and_key_name(prefix)
        if not self.hook.get_bucket(bucket_name).exists():
            self.hook.create_bucket(bucket_name)
        self.hook.upload(bucket_name, key_prefix, data="")

    def delete_prefix(self, prefix: str):
        bucket_name, key_prefix = _get_bucket_and_key_name(prefix)
        objects = self.hook.list(bucket_name, prefix=key_prefix)
        if objects:
            logger.info(
                f'Deleting {len(objects)} object(s) under prefix "{key_prefix}" '
                f'in bucket "{bucket_name}"'
            )
            for object_name in objects:
                self.hook.delete(bucket_name, object_name)

    def check_file(self, path: str) -> bool:
        bucket_name, object_name = _get_bucket_and_key_name(path)
        return self.hook.exists(bucket_name, object_name)

    def check_prefix(self, prefix: str) -> bool:
        bucket_name, key_prefix = _get_bucket_and_key_name(prefix)
        objects = self.hook.list(bucket_name, prefix=key_prefix, max_results=1)
        return bool(objects)

    def list_files(self, prefix: str) -> list[str]:
        bucket_name, key_prefix = _get_bucket_and_key_name(prefix)
        objects = self.hook.list(bucket_name, prefix=key_prefix)
        return [f"{bucket_name}/{object_path}" for object_path in (objects or [])]


def _get_bucket_and_key_name(path: str) -> tuple[str, str]:
    parts = path.split("/")
    return parts[0], "/".join(parts[1:])
