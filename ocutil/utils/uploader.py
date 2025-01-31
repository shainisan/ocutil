# utils/uploader.py

import os
import concurrent.futures
from .oci_manager import OCIManager

class Uploader:
    def __init__(self, oci_manager: OCIManager):
        self.oci_manager = oci_manager
        self.object_storage = self.oci_manager.object_storage
        self.namespace = self.oci_manager.namespace

    def upload_single_file(self, local_file: str, bucket_name: str, object_path: str):
        """
        Uploads a single file to OCI Object Storage.
        """
        if not os.path.isfile(local_file):
            print(f"Error: Local file '{local_file}' does not exist.")
            return

        object_name = object_path  # No trailing slash

        try:
            with open(local_file, 'rb') as f:
                response = self.object_storage.put_object(self.namespace, bucket_name, object_name, f)
            if response.status == 200:
                print(f"Successfully uploaded '{local_file}' to '{object_name}'")
            else:
                print(f"Failed to upload '{local_file}' with status: {response.status}")
        except Exception as e:
            print(f"Error uploading '{local_file}': {e}")

    def upload_folder(self, local_dir: str, bucket_name: str, object_prefix: str, parallel_count: int):
        """
        Uploads all files in a local directory to OCI Object Storage using parallel uploads.
        """
        if not os.path.isdir(local_dir):
            print(f"Error: Local directory '{local_dir}' does not exist.")
            return

        # Collect all files to upload
        files_to_upload = []
        for root, dirs, files in os.walk(local_dir):
            for file in files:
                full_path = os.path.join(root, file)
                relative_path = os.path.relpath(full_path, local_dir)
                object_name = f"{object_prefix}{relative_path.replace(os.sep, '/')}" if object_prefix else relative_path.replace(os.sep, '/')
                files_to_upload.append((object_name, full_path))

        print(f"Uploading {len(files_to_upload)} files to Bucket: '{bucket_name}', Prefix: '{object_prefix}' using {parallel_count} parallel threads...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_count) as executor:
            futures = []
            for object_name, full_path in files_to_upload:
                futures.append(executor.submit(self.upload_single_file, full_path, bucket_name, object_name))
            
            # Wait for all uploads to complete
            for future in concurrent.futures.as_completed(futures):
                pass  # All output is handled in upload_single_file

        print("Bulk upload operation completed.")