import logging
import subprocess
from datetime import datetime
from pathlib import Path
from google.cloud import storage

log = logging.getLogger(__name__)


class GCloud:

    def __init__(self, bucket_name: str) -> None:
        client = storage.Client()
        # https://console.cloud.google.com/storage/browser/[bucket-id]/
        self.bucket_name = bucket_name + '/'
        self.bucket = client.get_bucket(bucket_name)

    def put(self, local: str, remote: str, is_dir: bool = False) -> None:
        if is_dir:
            self.__exec(local, f'gs://{self.bucket_name}{remote}', is_dir)
        else:
            blob = self.bucket.blob(remote)
            blob.upload_from_filename(local)
            log.info(f'File {local} uploaded to {self.bucket_name}{remote}.')

    def mv(self, local: str, remote: str, is_dir: bool = False) -> None:
        """put and then remove"""
        self.put(local, remote, is_dir)
        Path(local).unlink()

    def __exec(self, p1: str, p2: str, is_dir: bool) -> None:
        if not Path(p2).exists():
            Path(p2).mkdir(exist_ok=True, parents=True)
        recursive = '-r ' if is_dir else ''
        cmd = f'gsutil -m cp {recursive}{p1} {p2}'
        log.info(f"Using cmd: {cmd}")
        subprocess.check_call(cmd, shell=True)
        log.info(f'Copied {p1} into {p2}')

    def get(self, remote: str, local: str, is_dir: bool = False) -> None:

        if is_dir:
            self.__exec(f'gs://{self.bucket_name}{remote}', local, is_dir)
        else:
            blob = self.bucket.blob(remote)
            blob.download_to_filename(local)
            log.info(f'File {local} download from {self.bucket_name}{remote}.')

    def exists(self, remote: str) -> bool:
        blob = self.bucket.blob(remote)
        return blob.exists()

    def put_timestamp(self, remote: str) -> None:
        self.put_text(datetime.now().isoformat(), remote)

    def put_text(self, text: str, remote: str) -> None:
        blob = self.bucket.blob(remote)
        blob.upload_from_string(text, remote)
        log.info(f'Wrote input string to {self.bucket_name}{remote}.')