# utils/uploader.py

import os
import logging
import concurrent.futures
from tqdm import tqdm
from ocutil.utils.oci_manager import OCIManager

logger = logging.getLogger('ocutil.uploader')

class ProgressFileReader:
    """
    A wrapper for a file object that updates a tqdm progress bar as data is read.
    """
    def __init__(self, file_obj, pbar):
        self.file_obj = file_obj
        self.pbar = pbar

    def read(self, size):
        data = self.file_obj.read(size)
        self.pbar.update(len(data))
        return data

    def __getattr__(self, attr):
        return getattr(self.file_obj, attr)

class Uploader:
    def __init__(self, oci_manager: OCIManager):
        self.oci_manager = oci_manager
        self.object_storage = self.oci_manager.object_storage
        self.namespace = self.oci_manager.namespace

    def upload_single_file(self, local_file: str, bucket_name: str, object_path: str):
        """
        Uploads a single file to OCI Object Storage with a progress bar.
        """
        if not os.path.isfile(local_file):
            logger.error(f"Local file '{local_file}' does not exist.")
            return

        try:
            file_size = os.path.getsize(local_file)
            with open(local_file, 'rb') as f, tqdm(
                    total=file_size, unit='B', unit_scale=True,
                    desc=f"Uploading {os.path.basename(local_file)}"
                ) as pbar:
                wrapped_file = ProgressFileReader(f, pbar)
                response = self.object_storage.put_object(self.namespace, bucket_name, object_path, wrapped_file)
            if response.status == 200:
                logger.info(f"Successfully uploaded '{local_file}' to '{object_path}'.")
            else:
                logger.error(f"Failed to upload '{local_file}'. Response status: {response.status}")
        except Exception as e:
            logger.error(f"Error uploading '{local_file}': {e}")

    def upload_folder(self, local_dir: str, bucket_name: str, object_prefix: str, parallel_count: int):
        """
        Uploads all files from a local directory to OCI Object Storage using parallel uploads.
        """
        if not os.path.isdir(local_dir):
            logger.error(f"Local directory '{local_dir}' does not exist.")
            return

        files_to_upload = []
        for root, _, files in os.walk(local_dir):
            for file in files:
                full_path = os.path.join(root, file)
                relative_path = os.path.relpath(full_path, local_dir)
                # Ensure object_prefix ends with a slash if not empty.
                prefix = f"{object_prefix.rstrip('/')}/" if object_prefix else ""
                object_name = f"{prefix}{relative_path.replace(os.sep, '/')}"
                files_to_upload.append((object_name, full_path))

        logger.info(f"Uploading {len(files_to_upload)} files to bucket '{bucket_name}' under prefix '{object_prefix}' using {parallel_count} threads...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_count) as executor:
            futures = []
            for object_name, full_path in files_to_upload:
                futures.append(executor.submit(self.upload_single_file, full_path, bucket_name, object_name))
            concurrent.futures.wait(futures)
        logger.info("Bulk upload operation completed.")
