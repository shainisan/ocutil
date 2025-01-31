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

    def download_folder(self, bucket_name: str, object_prefix: str, destination: str, parallel_count: int):
        """
        Downloads all objects with the given prefix from OCI Object Storage into a local directory using parallel downloads.
        """
        try:
            logger.info(f"Listing objects in bucket '{bucket_name}' with prefix '{object_prefix}'...")
            objects = []
            list_response = self.object_storage.list_objects(self.namespace, bucket_name, prefix=object_prefix)
            objects.extend(list_response.data.objects)
            while list_response.has_next_page:
                list_response = self.object_storage.list_objects(
                    self.namespace, bucket_name, prefix=object_prefix, page=list_response.next_page)
                objects.extend(list_response.data.objects)
        except Exception as e:
            logger.error(f"Error listing objects: {e}")
            return

        # Filter out directory markers (names ending with '/') and ignore the prefix itself if present.
        files_to_download = [obj for obj in objects
                             if not obj.name.endswith('/') and obj.name != object_prefix.rstrip('/')]

        if not files_to_download:
            logger.warning("No objects found to download.")
            return

        logger.info(f"Downloading {len(files_to_download)} objects to '{destination}' using {parallel_count} threads...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_count) as executor:
            futures = []
            for obj in files_to_download:
                # Compute the relative path and remove any leading slash.
                relative_path = obj.name[len(object_prefix):] if obj.name.startswith(object_prefix) else obj.name
                relative_path = relative_path.lstrip('/')  # Remove leading slash if present
                local_file_path = os.path.join(destination, relative_path.replace('/', os.sep))
                futures.append(executor.submit(self.download_single_file, bucket_name, obj.name, local_file_path))
            concurrent.futures.wait(futures)
        logger.info("Bulk download operation completed.")
