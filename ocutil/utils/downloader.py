# ocutil/utils/downloader.py

import os
import logging
import concurrent.futures
from tqdm import tqdm
from ocutil.utils.oci_manager import OCIManager

logger = logging.getLogger('ocutil.downloader')

class Downloader:
    def __init__(self, oci_manager: OCIManager):
        self.oci_manager = oci_manager
        self.object_storage = self.oci_manager.object_storage
        self.namespace = self.oci_manager.namespace

    def download_single_file(self, bucket_name: str, object_name: str, local_path: str):
        """
        Downloads a single file from OCI Object Storage with a progress bar.
        """
        try:
            response = self.object_storage.get_object(self.namespace, bucket_name, object_name)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            total_size = None
            if response.headers and "Content-Length" in response.headers:
                total_size = int(response.headers["Content-Length"])

            with open(local_path, 'wb') as f, tqdm(
                    total=total_size, unit='B', unit_scale=True,
                    desc=f"Downloading {object_name}"
                ) as pbar:
                for chunk in response.data.raw.stream(1024 * 1024, decode_content=False):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
            logger.info(f"Successfully downloaded '{object_name}' to '{local_path}'.")
        except Exception as e:
            logger.error(f"Error downloading '{object_name}': {e}")

    def download_folder(self, bucket_name: str, object_path: str, destination: str, parallel_count: int):
        """
        Downloads all objects under the given folder (object_path) from OCI Object Storage into
        a local directory using the OCI CLI bulk-download command.
        
        This function constructs a command equivalent to:
        oci os object bulk-download --bucket-name <bucket_name> --download-dir <destination>
            --prefix <folder_prefix> --parallel-operations-count <parallel_count> --overwrite
        """
        # Ensure the prefix ends with '/'
        prefix = object_path if object_path.endswith('/') else f"{object_path}/"
        
        # Build the CLI command.
        cmd = [
            "oci", "os", "object", "bulk-download",
            "--bucket-name", bucket_name,
            "--download-dir", destination,
            "--prefix", prefix,
            "--parallel-operations-count", str(parallel_count),
            "--overwrite"  # to avoid interactive prompt when >1000 objects
        ]
        
        import subprocess
        logger.info(f"Executing command: {' '.join(cmd)}")
        try:
            subprocess.run(cmd, check=True)
            logger.info("Bulk download operation completed via CLI.")
        except Exception as e:
            logger.error(f"Error executing CLI bulk-download: {e}")
