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

    def download_folder(self, bucket_name: str, object_path: str, destination: str, parallel_count: int, limit: int = 1000):
        """
        Downloads all objects under the given remote folder (object_path) from OCI Object Storage
        into a local directory. This implementation uses pagination (via the 'start_after' parameter)
        to list all objects and then downloads them concurrently.
        
        The local directory structure is built by stripping the remote folder prefix from each object's
        full key.
        """
        # Ensure the prefix ends with a trailing slash.
        prefix = object_path if object_path.endswith('/') else object_path + '/'
        
        all_objects = []
        start_after = None  # For pagination using start_after
        page = 1

        logger.info(f"Listing objects in remote folder '{prefix}'...")
        while True:
            if start_after:
                response = self.object_storage.list_objects(
                    namespace_name=self.namespace,
                    bucket_name=bucket_name,
                    prefix=prefix,
                    limit=limit,
                    start_after=start_after,
                    fields="name"
                )
            else:
                response = self.object_storage.list_objects(
                    namespace_name=self.namespace,
                    bucket_name=bucket_name,
                    prefix=prefix,
                    limit=limit,
                    fields="name"
                )
            objects = response.data.objects or []
            all_objects.extend(objects)
            logger.info(f"Requesting page {page} (start_after={start_after})... Page {page} returned {len(objects)} objects.")
            if len(objects) < limit:
                # This page is the last one.
                break
            # Set start_after to the last object's name for the next page.
            start_after = objects[-1].name
            page += 1

        logger.info(f"Found a total of {len(all_objects)} objects in the folder '{prefix}'.")

        # Download all objects concurrently.
        with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_count) as executor:
            futures = []
            for obj in all_objects:
                # Compute the relative path by stripping the remote folder prefix.
                relative_path = obj.name[len(prefix):]
                local_file_path = os.path.join(destination, relative_path)
                futures.append(executor.submit(self.download_single_file, bucket_name, obj.name, local_file_path))
            concurrent.futures.wait(futures)

        logger.info("Bulk download operation completed.")
