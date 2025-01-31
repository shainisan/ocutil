# utils/downloader.py

import os
import concurrent.futures
from urllib.parse import urlparse
from .oci_manager import OCIManager

class Downloader:
    def __init__(self, oci_manager: OCIManager):
        self.oci_manager = oci_manager
        self.object_storage = self.oci_manager.object_storage
        self.namespace = self.oci_manager.namespace

    def download_single_file(self, bucket_name: str, object_name: str, local_path: str):
        """
        Downloads a single file from OCI Object Storage.
        """
        try:
            get_object_response = self.object_storage.get_object(self.namespace, bucket_name, object_name)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, 'wb') as f:
                for chunk in get_object_response.data.raw.stream(1024 * 1024, decode_content=False):
                    if chunk:
                        f.write(chunk)
            print(f"Successfully downloaded '{object_name}' to '{local_path}'")
        except Exception as e:
            print(f"Error downloading '{object_name}': {e}")

    def download_folder(self, remote_prefix: str, destination: str, parallel_count: int):
        """
        Downloads all objects under a given prefix from OCI Object Storage to a local directory using parallel downloads.
        """
        bucket_name, object_prefix = self.parse_remote_path(remote_prefix)

        # List all objects with the given prefix
        print(f"Listing objects in Bucket: '{bucket_name}', Prefix: '{object_prefix}'...")
        try:
            objects = []
            list_objects_response = self.object_storage.list_objects(self.namespace, bucket_name, prefix=object_prefix)
            objects.extend(list_objects_response.data.objects)
            while list_objects_response.has_next_page:
                list_objects_response = self.object_storage.list_objects(
                    self.namespace, bucket_name, prefix=object_prefix, page=list_objects_response.next_page)
                objects.extend(list_objects_response.data.objects)
        except Exception as e:
            print(f"Error listing objects: {e}")
            return

        # Filter out directory objects (those ending with '/') and the prefix object itself
        files_to_download = [obj for obj in objects if not obj.name.endswith('/') and obj.name != object_prefix.rstrip('/')]

        if not files_to_download:
            print("No objects found to download.")
            return

        print(f"Downloading {len(files_to_download)} objects to '{destination}' using {parallel_count} parallel threads...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_count) as executor:
            futures = []
            for obj in files_to_download:
                object_name = obj.name
                relative_path = object_name[len(object_prefix):] if object_prefix else object_name
                local_path = os.path.join(destination, relative_path.replace('/', os.sep))
                futures.append(executor.submit(self.download_single_file, bucket_name, object_name, local_path))
            
            # Wait for all downloads to complete
            for future in concurrent.futures.as_completed(futures):
                pass  # All output is handled in download_single_file

        print("Bulk download operation completed.")

    def parse_remote_path(self, remote_path: str):
        """
        Parses the remote path to extract bucket name and object prefix.
        """
        parsed = urlparse(remote_path)
        if parsed.scheme != "oc":
            raise ValueError("Remote path must start with 'oc://'")
        
        bucket_name = parsed.netloc
        object_prefix = parsed.path.lstrip('/')
        
        return bucket_name, object_prefix