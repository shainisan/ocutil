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
        a local directory using parallel downloads.
        
        This function performs two listings:
        1. Using the full folder prefix (object_path + '/')
        2. Using the "part-" prefix (object_path + '/part-')
        
        The two listings are merged (by object name) to ensure that every file in the folder is downloaded.
        Directory markers (keys ending with '/') are skipped.
        """
        try:
            # Define the two prefixes.
            full_prefix = object_path if object_path.endswith('/') else f"{object_path}/"
            part_prefix = full_prefix + "part-"
            
            logger.info(f"Listing objects with full prefix '{full_prefix}'...")
            objects_full = []
            list_response = self.object_storage.list_objects(self.namespace, bucket_name, prefix=full_prefix)
            objects_full.extend(list_response.data.objects)
            while list_response.next_page:
                list_response = self.object_storage.list_objects(self.namespace, bucket_name, prefix=full_prefix, page=list_response.next_page)
                objects_full.extend(list_response.data.objects)
            
            logger.info(f"Listing objects with part prefix '{part_prefix}'...")
            objects_parts = []
            list_response = self.object_storage.list_objects(self.namespace, bucket_name, prefix=part_prefix)
            objects_parts.extend(list_response.data.objects)
            while list_response.next_page:
                list_response = self.object_storage.list_objects(self.namespace, bucket_name, prefix=part_prefix, page=list_response.next_page)
                objects_parts.extend(list_response.data.objects)
            
            # Merge the two listings (unique by object name).
            merged = {obj.name: obj for obj in (objects_full + objects_parts)}
            merged_objects = list(merged.values())
        except Exception as e:
            logger.error(f"Error listing objects: {e}")
            return

        # Filter out any directory markers (keys ending with '/')
        files_to_download = [obj for obj in merged_objects if not obj.name.endswith('/')]
        if not files_to_download:
            logger.warning("No objects found to download.")
            return

        logger.info(f"Downloading {len(files_to_download)} objects to '{destination}' using {parallel_count} threads...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_count) as executor:
            futures = []
            for obj in files_to_download:
                # Compute a relative path based on the full folder prefix.
                if obj.name.startswith(full_prefix):
                    relative_path = obj.name[len(full_prefix):]
                else:
                    relative_path = obj.name
                relative_path = relative_path.lstrip('/')
                local_file_path = os.path.join(destination, relative_path.replace('/', os.sep))
                futures.append(executor.submit(self.download_single_file, bucket_name, obj.name, local_file_path))
            concurrent.futures.wait(futures)
        logger.info("Bulk download operation completed.")
