import os
import logging
import concurrent.futures
import time
from rich.progress import Progress
from ocutil.utils.oci_manager import OCIManager

logger = logging.getLogger('ocutil.uploader')

class ProgressFileReader:
    """
    A wrapper for a file object that updates a Rich progress bar as data is read.
    """
    def __init__(self, file_obj, progress, task_id):
        self.file_obj = file_obj
        self.progress = progress
        self.task_id = task_id

    def read(self, size):
        data = self.file_obj.read(size)
        self.progress.update(self.task_id, advance=len(data))
        return data

    def __getattr__(self, attr):
        return getattr(self.file_obj, attr)

class Uploader:
    def __init__(self, oci_manager: OCIManager, dry_run=False):
        self.oci_manager = oci_manager
        self.object_storage = self.oci_manager.object_storage
        self.namespace = self.oci_manager.namespace
        self.dry_run = dry_run

    def upload_single_file(self, local_file: str, bucket_name: str, object_path: str):
        """
        Uploads a single file to OCI Object Storage with a Rich progress bar and retry logic.
        """
        if not os.path.isfile(local_file):
            logger.error(f"Local file '{local_file}' does not exist.")
            return

        if self.dry_run:
            logger.info(f"DRY-RUN: Would upload '{local_file}' to bucket '{bucket_name}' as '{object_path}'.")
            return

        max_retries = 3
        retry_delay = 1
        for attempt in range(max_retries):
            try:
                file_size = os.path.getsize(local_file)
                with Progress() as progress:
                    task_id = progress.add_task(f"Uploading {os.path.basename(local_file)}", total=file_size)
                    with open(local_file, 'rb') as f:
                        wrapped_file = ProgressFileReader(f, progress, task_id)
                        response = self.object_storage.put_object(self.namespace, bucket_name, object_path, wrapped_file)
                if response.status == 200:
                    logger.info(f"Successfully uploaded '{local_file}' to '{object_path}'.")
                else:
                    logger.error(f"Failed to upload '{local_file}'. Response status: {response.status}")
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Upload failed for '{local_file}', retrying in {retry_delay} seconds. Error: {e}")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logger.error(f"Error uploading '{local_file}': {e}")

    def upload_folder(self, local_dir: str, bucket_name: str, object_prefix: str, parallel_count: int):
        """
        Uploads all files from a local directory to OCI Object Storage using an aggregate Rich progress bar.
        A final summary report is logged upon completion.
        """
        if not os.path.isdir(local_dir):
            logger.error(f"Local directory '{local_dir}' does not exist.")
            return

        files_to_upload = []
        total_size = 0
        for root, _, files in os.walk(local_dir):
            for file in files:
                full_path = os.path.join(root, file)
                file_size = os.path.getsize(full_path)
                total_size += file_size
                relative_path = os.path.relpath(full_path, local_dir)
                prefix = f"{object_prefix.rstrip('/')}/" if object_prefix else ""
                object_name = f"{prefix}{relative_path.replace(os.sep, '/')}"
                files_to_upload.append((object_name, full_path, file_size))

        logger.info(f"Uploading {len(files_to_upload)} files to bucket '{bucket_name}' under prefix '{object_prefix}' using {parallel_count} threads...")

        if self.dry_run:
            for object_name, full_path, _ in files_to_upload:
                logger.info(f"DRY-RUN: Would upload '{full_path}' as '{object_name}'.")
            logger.info("DRY-RUN: Bulk upload simulation complete.")
            return

        start_time = time.time()
        with Progress() as progress:
            overall_task = progress.add_task("Overall Upload Progress", total=total_size)
            with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_count) as executor:
                futures = {}
                def upload_file(object_name, full_path, file_size):
                    max_retries = 3
                    retry_delay = 1
                    for attempt in range(max_retries):
                        try:
                            with open(full_path, 'rb') as f:
                                response = self.object_storage.put_object(self.namespace, bucket_name, object_name, f)
                            if response.status == 200:
                                logger.info(f"Successfully uploaded '{full_path}' to '{object_name}'.")
                            else:
                                logger.error(f"Failed to upload '{full_path}'. Response status: {response.status}")
                            return file_size
                        except Exception as e:
                            if attempt < max_retries - 1:
                                logger.warning(f"Upload failed for '{full_path}', retrying in {retry_delay} seconds. Error: {e}")
                                time.sleep(retry_delay)
                                retry_delay *= 2
                            else:
                                logger.error(f"Error uploading '{full_path}': {e}")
                                return 0

                for object_name, full_path, file_size in files_to_upload:
                    futures[executor.submit(upload_file, object_name, full_path, file_size)] = file_size

                for future in concurrent.futures.as_completed(futures):
                    bytes_uploaded = future.result()
                    progress.update(overall_task, advance=bytes_uploaded)
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Bulk upload operation completed in {duration:.2f} seconds. Total bytes uploaded: {total_size}")
