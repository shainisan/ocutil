import os
import logging
import concurrent.futures
import time
from rich.progress import Progress
from ocutil.utils.oci_manager import OCIManager

logger = logging.getLogger('ocutil.downloader')

class Downloader:
    def __init__(self, oci_manager: OCIManager, dry_run=False):
        self.oci_manager = oci_manager
        self.object_storage = self.oci_manager.object_storage
        self.namespace = self.oci_manager.namespace
        self.dry_run = dry_run

    def download_single_file(self, bucket_name: str, object_name: str, local_path: str):
        """
        Downloads a single file from OCI Object Storage with a Rich progress bar and retry logic.
        (Used for interactive single file downloads.)
        """
        if self.dry_run:
            logger.info(f"DRY-RUN: Would download '{object_name}' from bucket '{bucket_name}' to '{local_path}'.")
            return

        max_retries = 3
        retry_delay = 1
        for attempt in range(max_retries):
            try:
                response = self.object_storage.get_object(self.namespace, bucket_name, object_name)
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                total_size = int(response.headers["Content-Length"]) if response.headers and "Content-Length" in response.headers else None
                with Progress() as progress:
                    task = progress.add_task(f"Downloading {object_name}", total=total_size)
                    with open(local_path, 'wb') as f:
                        for chunk in response.data.raw.stream(1024 * 1024, decode_content=False):
                            if chunk:
                                f.write(chunk)
                                progress.update(task, advance=len(chunk))
                logger.info(f"Successfully downloaded '{object_name}' to '{local_path}'.")
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Download failed for '{object_name}', retrying in {retry_delay} seconds. Error: {e}")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logger.error(f"Error downloading '{object_name}': {e}")

    def _download_file_no_progress(self, bucket_name: str, object_name: str, local_path: str):
        """
        Downloads a single file without creating its own Progress display.
        This is used by bulk download to avoid multiple live displays.
        """
        max_retries = 3
        retry_delay = 1
        for attempt in range(max_retries):
            try:
                response = self.object_storage.get_object(self.namespace, bucket_name, object_name)
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                with open(local_path, 'wb') as f:
                    for chunk in response.data.raw.stream(1024 * 1024, decode_content=False):
                        if chunk:
                            f.write(chunk)
                logger.info(f"Successfully downloaded '{object_name}' to '{local_path}'.")
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Download failed for '{object_name}', retrying in {retry_delay} seconds. Error: {e}")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logger.error(f"Error downloading '{object_name}': {e}")

    def download_folder(self, bucket_name: str, object_path: str, destination: str, parallel_count: int, limit: int = 1000):
        """
        Downloads all objects under the given remote folder (object_path) from OCI Object Storage
        into a local directory using pagination and an aggregate Rich progress bar.
        A final summary report is logged upon completion.
        """
        prefix = object_path if object_path.endswith('/') else object_path + '/'
        all_objects = []
        start_after = None
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
                break
            start_after = objects[-1].name
            page += 1

        logger.info(f"Found a total of {len(all_objects)} objects in the folder '{prefix}'.")

        if self.dry_run:
            for obj in all_objects:
                relative_path = obj.name[len(prefix):]
                if not relative_path:
                    continue
                local_file_path = os.path.join(destination, relative_path)
                logger.info(f"DRY-RUN: Would download '{obj.name}' to '{local_file_path}'.")
            logger.info("DRY-RUN: Bulk download simulation complete.")
            return

        total_files = len(all_objects)
        start_time = time.time()
        with Progress() as progress:
            overall_task = progress.add_task("Overall Download Progress", total=total_files)
            with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_count) as executor:
                futures = {}
                def download_file(obj_name):
                    relative_path = obj_name[len(prefix):]
                    if not relative_path:
                        return 0
                    local_file_path = os.path.join(destination, relative_path)
                    self._download_file_no_progress(bucket_name, obj_name, local_file_path)
                    return 1

                for obj in all_objects:
                    futures[executor.submit(download_file, obj.name)] = 1
                for future in concurrent.futures.as_completed(futures):
                    progress.update(overall_task, advance=1)
            end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Bulk download operation completed in {duration:.2f} seconds. Total files downloaded: {total_files}")
