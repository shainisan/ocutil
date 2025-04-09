# downloader.py
import os
import logging
import concurrent.futures
import time
import oci # Import oci for exceptions
from rich.progress import Progress, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn, TransferSpeedColumn
from ocutil.utils.oci_manager import OCIManager

logger = logging.getLogger('ocutil.downloader')

class Downloader:
    def __init__(self, oci_manager: OCIManager, dry_run=False):
        self.oci_manager = oci_manager
        self.object_storage = self.oci_manager.object_storage
        self.namespace = self.oci_manager.namespace
        self.dry_run = dry_run
        # Define progress bar columns suitable for file counts
        self.progress_columns = [
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("files"),
            TimeRemainingColumn(),
        ]


    def download_single_file(self, bucket_name: str, object_name: str, local_path: str):
        """
        Downloads a single file from OCI Object Storage with a Rich progress bar and retry logic.
        (Used for interactive single file downloads.)
        """
        if self.dry_run:
            logger.info(f"DRY-RUN: Would download '{object_name}' from bucket '{bucket_name}' to '{local_path}'.")
            return True # Indicate success for dry run

        max_retries = 3
        retry_delay = 2 # Slightly longer initial delay
        for attempt in range(max_retries):
            try:
                # Ensure local directory exists
                os.makedirs(os.path.dirname(local_path), exist_ok=True)

                # Get object metadata first to get size for progress bar
                head_response = self.object_storage.head_object(self.namespace, bucket_name, object_name)
                total_size = int(head_response.headers.get("Content-Length", 0))

                # Get the object stream
                response = self.object_storage.get_object(self.namespace, bucket_name, object_name)

                # Use Rich Progress for single file download
                with Progress(
                    TextColumn("[bold blue]{task.description}", justify="right"),
                    BarColumn(bar_width=None),
                    "[progress.percentage]{task.percentage:>3.1f}%",
                    "•",
                    TransferSpeedColumn(),
                    "•",
                    TimeRemainingColumn(),
                    transient=True # Clear progress bar on completion
                 ) as progress:
                    task = progress.add_task(f"Downloading {os.path.basename(object_name)}", total=total_size)
                    bytes_downloaded = 0
                    with open(local_path, 'wb') as f:
                        # Use a reasonable chunk size
                        for chunk in response.data.raw.stream(1024 * 1024, decode_content=False):
                            if chunk:
                                f.write(chunk)
                                chunk_len = len(chunk)
                                bytes_downloaded += chunk_len
                                progress.update(task, advance=chunk_len)

                # Verify downloaded size matches expected size
                if total_size > 0 and bytes_downloaded != total_size:
                     logger.warning(f"Downloaded size ({bytes_downloaded}) does not match expected size ({total_size}) for '{object_name}'.")
                     # Decide if this is a failure or just a warning
                     # raise IOError(f"Incomplete download for {object_name}") # Option to make it fail

                logger.info(f"Successfully downloaded '{object_name}' to '{local_path}'.")
                return True # Success

            except oci.exceptions.ServiceError as e:
                if e.status == 404:
                    logger.error(f"Error downloading '{object_name}': Object not found (404).")
                    return False # No point retrying 404
                elif attempt < max_retries - 1:
                    logger.warning(f"Download failed for '{object_name}' (Attempt {attempt+1}/{max_retries}), retrying in {retry_delay} seconds. Status: {e.status}. Error: {e}")
                    time.sleep(retry_delay)
                    retry_delay *= 2 # Exponential backoff
                else:
                    logger.error(f"Error downloading '{object_name}' after {max_retries} attempts. Status: {e.status}. Error: {e}")
                    return False # Failed after retries
            except Exception as e:
                 if attempt < max_retries - 1:
                    logger.warning(f"Download failed for '{object_name}' (Attempt {attempt+1}/{max_retries}), retrying in {retry_delay} seconds. Error: {e}")
                    time.sleep(retry_delay)
                    retry_delay *= 2 # Exponential backoff
                 else:
                    logger.error(f"Error downloading '{object_name}' after {max_retries} attempts: {e}")
                    return False # Failed after retries
        return False # Should not be reached, but ensures a return path


    def _download_worker(self, bucket_name: str, object_name: str, destination_dir: str, prefix_len: int):
        """
        Worker function to download a single file (part of a bulk download).
        Returns: (bool: success, str: object_name, str|None: error_message)
        """
        relative_path = object_name[prefix_len:]
        if not relative_path: # Skip the folder pseudo-object if listed
             return True, object_name, None

        local_file_path = os.path.join(destination_dir, relative_path)
        max_retries = 3
        retry_delay = 1

        try:
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        except OSError as e:
            logger.error(f"Failed to create directory for '{local_file_path}'. Error: {e}")
            return False, object_name, f"Directory creation failed: {e}"

        for attempt in range(max_retries):
            try:
                response = self.object_storage.get_object(self.namespace, bucket_name, object_name)
                # No individual progress bar here, just download
                with open(local_file_path, 'wb') as f:
                    # Stream data efficiently
                    for chunk in response.data.raw.stream(1024 * 1024, decode_content=False):
                        if chunk:
                            f.write(chunk)
                # Minimal success log for bulk, summary will be more informative
                # logger.debug(f"Successfully downloaded '{object_name}' to '{local_file_path}'.")
                return True, object_name, None # Success

            except oci.exceptions.ServiceError as e:
                 if e.status == 404:
                     error_msg = f"Object not found (404)"
                     logger.error(f"Download failed for '{object_name}': {error_msg}")
                     return False, object_name, error_msg
                 elif attempt < max_retries - 1:
                     logger.warning(f"Download attempt {attempt+1} failed for '{object_name}', retrying in {retry_delay}s. Status: {e.status}. Error: {e}")
                     time.sleep(retry_delay)
                     retry_delay *= 2
                 else:
                     error_msg = f"Failed after {max_retries} attempts. Status: {e.status}. Error: {e}"
                     logger.error(f"Error downloading '{object_name}': {error_msg}")
                     return False, object_name, error_msg # Failed after retries
            except Exception as e:
                 if attempt < max_retries - 1:
                     logger.warning(f"Download attempt {attempt+1} failed for '{object_name}', retrying in {retry_delay}s. Error: {e}")
                     time.sleep(retry_delay)
                     retry_delay *= 2
                 else:
                     error_msg = f"Failed after {max_retries} attempts: {e}"
                     logger.error(f"Error downloading '{object_name}': {error_msg}")
                     return False, object_name, error_msg # Failed after retries
        return False, object_name, "Download failed after retries (unknown reason)" # Fallback


    def _execute_parallel_download(self, tasks: list, bucket_name: str, destination_dir: str, prefix_len: int, parallel_count: int):
        """
        Manages the parallel execution of download tasks using ThreadPoolExecutor.
        tasks: List of object names to download.
        """
        total_files = len(tasks)
        succeeded_count = 0
        failed_downloads = []
        start_time = time.time()

        with Progress(*self.progress_columns, transient=True) as progress:
            overall_task = progress.add_task("Overall Download Progress", total=total_files)
            # Use ThreadPoolExecutor for I/O-bound tasks
            with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_count, thread_name_prefix='Downloader') as executor:
                # Submit all download tasks
                future_to_object = {
                    executor.submit(self._download_worker, bucket_name, obj_name, destination_dir, prefix_len): obj_name
                    for obj_name in tasks
                }

                for future in concurrent.futures.as_completed(future_to_object):
                    obj_name = future_to_object[future]
                    try:
                        success, _, error_message = future.result()
                        if success:
                            succeeded_count += 1
                        else:
                            failed_downloads.append((obj_name, error_message))
                            logger.warning(f"Failed to download '{obj_name}': {error_message}")
                    except Exception as exc:
                        # Catch exceptions raised if _download_worker itself fails unexpectedly
                        failed_downloads.append((obj_name, str(exc)))
                        logger.error(f"Download task for '{obj_name}' generated an exception: {exc}")

                    # Advance progress regardless of success/failure for completion tracking
                    progress.update(overall_task, advance=1)

        end_time = time.time()
        duration = end_time - start_time
        logger.info("-" * 30 + " Download Summary " + "-" * 30)
        logger.info(f"Operation completed in {duration:.2f} seconds.")
        logger.info(f"Total files attempted: {total_files}")
        logger.info(f"Successfully downloaded: {succeeded_count}")
        logger.info(f"Failed to download: {len(failed_downloads)}")
        if failed_downloads:
            logger.warning("Failed items:")
            for name, err in failed_downloads:
                logger.warning(f"  - {name}: {err}")
        logger.info("-" * (60 + len(" Download Summary ")))


    def download_folder(self, bucket_name: str, object_path: str, destination: str, parallel_count: int, limit: int = 1000):
        """
        Downloads all objects under the given remote folder (object_path) from OCI Object Storage
        into a local directory using pagination and parallel execution.
        """
        prefix = object_path if object_path.endswith('/') else object_path.rstrip('/') + '/'
        prefix_len = len(prefix)
        all_object_names = []
        start_after = None
        page = 1

        logger.info(f"Listing objects in remote folder '{prefix}'...")
        try:
            while True:
                logger.debug(f"Requesting object list page {page} (start_after={start_after})...")
                list_params = {
                    'namespace_name': self.namespace,
                    'bucket_name': bucket_name,
                    'prefix': prefix,
                    'limit': limit,
                    'fields': "name" # Only need the name
                }
                if start_after:
                    list_params['start_after'] = start_after

                response = self.object_storage.list_objects(**list_params)
                objects = response.data.objects or []
                # Filter out the potential folder marker itself if object_path wasn't ending with /
                # and just add the names
                current_page_names = [obj.name for obj in objects if obj.name != object_path.rstrip('/')]

                if not current_page_names:
                     logger.debug(f"Page {page} returned 0 relevant objects. Ending listing.")
                     break # No more objects found in this page

                all_object_names.extend(current_page_names)
                logger.debug(f"Page {page} returned {len(current_page_names)} relevant objects (Total found: {len(all_object_names)}).")

                if len(objects) < limit:
                     logger.debug(f"Last page reached (returned {len(objects)} < limit {limit}).")
                     break # Last page

                # Use the name of the last object *returned by the API* for pagination
                start_after = objects[-1].name
                page += 1
        except oci.exceptions.ServiceError as e:
             logger.error(f"Failed to list objects in bucket '{bucket_name}' with prefix '{prefix}'. Error: {e}")
             return
        except Exception as e:
            logger.error(f"An unexpected error occurred during object listing: {e}")
            return


        logger.info(f"Found a total of {len(all_object_names)} objects to download in folder '{prefix}'.")

        if not all_object_names:
            logger.info("No objects found to download.")
            return

        if self.dry_run:
            logger.info("DRY-RUN: Simulating bulk download...")
            for obj_name in all_object_names:
                relative_path = obj_name[prefix_len:]
                if not relative_path: continue # Should be filtered already, but safety check
                local_file_path = os.path.join(destination, relative_path)
                logger.info(f"DRY-RUN: Would download '{obj_name}' to '{local_file_path}'.")
            logger.info("DRY-RUN: Bulk download simulation complete.")
            return

        # Ensure base destination directory exists
        try:
             os.makedirs(destination, exist_ok=True)
        except OSError as e:
             logger.error(f"Failed to create base destination directory '{destination}'. Error: {e}")
             return

        # Execute the parallel download
        self._execute_parallel_download(all_object_names, bucket_name, destination, prefix_len, parallel_count)