# uploader.py
import os
import logging
import concurrent.futures
import time
import threading
import oci # Import oci for exceptions
# Import UploadManager
from oci.object_storage import UploadManager
from ocutil.utils.oci_manager import OCIManager

# Import Rich progress components
from rich.progress import Progress, TextColumn, BarColumn, DownloadColumn, TransferSpeedColumn, TimeRemainingColumn

logger = logging.getLogger('ocutil.uploader')

# ProgressFileReader class is no longer needed as UploadManager uses a callback

class Uploader:
    def __init__(self, oci_manager: OCIManager, dry_run=False):
        self.oci_manager = oci_manager
        self.object_storage = self.oci_manager.object_storage
        self.namespace = self.oci_manager.namespace
        self.dry_run = dry_run
        # Define progress bar columns suitable for byte transfers
        self.progress_columns = [
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            DownloadColumn(), # Shows bytes transferred / total bytes
            TransferSpeedColumn(),
            TimeRemainingColumn(),
        ]
        # Initialize UploadManager once - it's generally safe to reuse
        # Pass allow_parallel_uploads=True to enable parallel part uploads for *single large files*
        # The UploadManager itself uses threads internally for this feature.
        self.upload_manager = UploadManager(self.object_storage, allow_parallel_uploads=True)
        logger.debug("Initialized OCI UploadManager.")


    def upload_single_file(self, local_file: str, bucket_name: str, object_path: str):
        """
        Uploads a single file to OCI Object Storage using UploadManager
        with a Rich progress bar and retry logic.
        """
        if not os.path.isfile(local_file):
            logger.error(f"Local file '{local_file}' does not exist or is not a file.")
            return False

        if self.dry_run:
            logger.info(f"DRY-RUN: Would upload '{local_file}' to bucket '{bucket_name}' as '{object_path}'.")
            return True # Indicate success for dry run

        max_retries = 3
        retry_delay = 2 # Slightly longer initial delay
        try:
            file_size = os.path.getsize(local_file)
        except OSError as e:
             logger.error(f"Could not get size of '{local_file}': {e}")
             return False

        for attempt in range(max_retries):
            try:
                # Setup Rich Progress bar
                with Progress(*self.progress_columns, transient=True) as progress:
                    task_id = progress.add_task(f"Uploading {os.path.basename(local_file)}", total=file_size)

                    # Define the progress callback for UploadManager
                    def single_file_progress_callback(bytes_uploaded):
                        progress.update(task_id, completed=bytes_uploaded)

                    # Use UploadManager to upload the file
                    # It expects the file path, not a file object
                    logger.debug(f"Attempt {attempt+1}: Calling UploadManager.upload_file for '{local_file}'")
                    response = self.upload_manager.upload_file(
                        namespace_name=self.namespace,
                        bucket_name=bucket_name,
                        object_name=object_path,
                        file_path=local_file,
                        progress_callback=single_file_progress_callback if file_size > 0 else None # Avoid callback for zero-byte files
                        # Can add optional part_size, allow_multipart, etc. if needed
                    )

                # Check response status after upload completes
                # UploadManager usually raises errors, but we check status for certainty
                if 200 <= response.status < 300:
                    logger.info(f"Successfully uploaded '{local_file}' to '{object_path}'.")
                    return True # Success
                else:
                    # Should not typically be reached if UploadManager raises exceptions on failure
                    logger.error(f"UploadManager returned non-success status for '{local_file}': {response.status}")
                    # Simulate an exception to trigger retry logic if necessary
                    raise oci.exceptions.ServiceError(
                        status=response.status,
                        code="UploadManagerNonSuccessStatus",
                        headers=response.headers,
                        message=f"UploadManager returned status {response.status}"
                    )

            except oci.exceptions.ServiceError as e:
                 # Don't retry 404 on bucket or authentication issues
                 if e.status in [401, 403, 404]:
                       logger.error(f"Upload failed for '{local_file}' with non-retriable status {e.status}: {e}")
                       return False
                 # Handle potential 429 Rate Limiting specifically (optional enhancement)
                 elif e.status == 429:
                       wait_time = retry_delay * 2 # Apply longer backoff for rate limit
                       logger.warning(f"Rate limit (429) hit for '{local_file}' (Attempt {attempt+1}/{max_retries}), retrying in {wait_time} seconds...")
                       if attempt < max_retries - 1:
                           time.sleep(wait_time)
                           retry_delay = wait_time # Use the longer delay for next potential retry
                       else:
                           logger.error(f"Error uploading '{local_file}' after {max_retries} attempts due to rate limiting.")
                           return False # Failed after retries
                 elif attempt < max_retries - 1:
                     logger.warning(f"Upload failed for '{local_file}' (Attempt {attempt+1}/{max_retries}), retrying in {retry_delay} seconds. Status: {e.status}. Error: {e}")
                     time.sleep(retry_delay)
                     retry_delay *= 2 # Standard exponential backoff
                 else:
                     logger.error(f"Error uploading '{local_file}' after {max_retries} attempts. Status: {e.status}. Error: {e}")
                     return False # Failed after retries
            except Exception as e:
                 # Catch other potential errors (network, file reading handled by UploadManager)
                 if attempt < max_retries - 1:
                     logger.warning(f"Upload failed for '{local_file}' (Attempt {attempt+1}/{max_retries}), retrying in {retry_delay} seconds. Error: {e}")
                     time.sleep(retry_delay)
                     retry_delay *= 2
                 else:
                     logger.error(f"Error uploading '{local_file}' after {max_retries} attempts: {e}")
                     return False # Failed after retries
        return False # Failed all retries


    def _upload_worker(self, local_file: str, bucket_name: str, object_name: str):
        """
        Worker function to upload a single file (part of a bulk upload) using UploadManager.
        Returns: (bool: success, str: local_file, int: bytes_uploaded, str|None: error_message)
        """
        max_retries = 3
        retry_delay = 1
        file_size = 0 # Initialize size

        try:
            # Check file existence and get size *before* attempting upload
            if not os.path.isfile(local_file):
                raise FileNotFoundError(f"File not found during worker execution: {local_file}")
            file_size = os.path.getsize(local_file)

            for attempt in range(max_retries):
                try:
                    # Use UploadManager - no manual file opening needed
                    # No progress_callback here for bulk worker efficiency
                    # Rely on UploadManager's internal retries for part failures
                    logger.debug(f"Worker {os.getpid()}/{threading.current_thread().name}: Attempt {attempt+1} uploading '{local_file}' using UploadManager.")
                    response = self.upload_manager.upload_file(
                        namespace_name=self.namespace,
                        bucket_name=bucket_name,
                        object_name=object_name,
                        file_path=local_file
                    )

                    # Check status after successful call return
                    if 200 <= response.status < 300:
                         # Minimal success log for bulk
                         logger.debug(f"Worker {os.getpid()}/{threading.current_thread().name}: Successfully uploaded '{local_file}' to '{object_name}'.")
                         return True, local_file, file_size, None # Success
                    else:
                         # Should not happen if UploadManager raises errors, but handle defensively
                         raise oci.exceptions.ServiceError(
                             status=response.status,
                             code="UploadManagerNonSuccessStatus",
                             headers=response.headers,
                             message=f"UploadManager returned status {response.status}"
                         )

                except oci.exceptions.ServiceError as e:
                    # Non-retriable errors
                    if e.status in [401, 403, 404]:
                         error_msg = f"Non-retriable status {e.status}: {e}"
                         logger.error(f"Upload failed for '{local_file}': {error_msg}")
                         return False, local_file, 0, error_msg
                    # Handle potential 429 Rate Limiting specifically (optional enhancement)
                    elif e.status == 429:
                        wait_time = retry_delay * 2 # Apply longer backoff for rate limit
                        logger.warning(f"Rate limit (429) hit for '{local_file}' (Attempt {attempt+1}/{max_retries}), retrying in {wait_time} seconds...")
                        if attempt < max_retries - 1:
                            time.sleep(wait_time)
                            retry_delay = wait_time # Use longer delay
                        else:
                            error_msg = f"Failed after {max_retries} attempts due to rate limiting (429)."
                            logger.error(f"Error uploading '{local_file}': {error_msg}")
                            return False, local_file, 0, error_msg
                    elif attempt < max_retries - 1:
                        logger.warning(f"Upload attempt {attempt+1} failed for '{local_file}', retrying in {retry_delay}s. Status: {e.status}. Error: {e}")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        error_msg = f"Failed after {max_retries} attempts. Status: {e.status}. Error: {e}"
                        logger.error(f"Error uploading '{local_file}': {error_msg}")
                        return False, local_file, 0, error_msg # Failed after retries
                except Exception as e:
                    # Other errors (e.g., file read errors during upload are now handled inside UploadManager)
                    # Catch potential setup issues or unexpected UploadManager errors
                    if attempt < max_retries - 1:
                        logger.warning(f"Upload attempt {attempt+1} failed for '{local_file}', retrying in {retry_delay}s. Error: {e}")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                         error_msg = f"Failed after {max_retries} attempts: {e}"
                         logger.error(f"Error uploading '{local_file}': {error_msg}")
                         return False, local_file, 0, error_msg # Failed after retries

            # Fallback if loop finishes unexpectedly
            return False, local_file, 0, "Upload failed after retries (unknown worker error)"

        except FileNotFoundError as e:
             logger.error(f"Upload failed for '{local_file}': {e}")
             return False, local_file, 0, str(e)
        except OSError as e:
            # Catch errors getting size
            logger.error(f"Upload preparation failed for '{local_file}' (get size): {e}")
            return False, local_file, 0, f"Preparation failed (get size): {e}"
        except Exception as e:
             # Catch other unexpected setup issues
             logger.error(f"Upload preparation failed for '{local_file}': {e}")
             return False, local_file, 0, f"Preparation failed: {e}"

    # --- _execute_parallel_upload method remains the same ---
    # It receives the file_size correctly from the modified _upload_worker
    def _execute_parallel_upload(self, tasks: list, bucket_name: str, parallel_count: int):
        """
        Manages the parallel execution of upload tasks using ThreadPoolExecutor.
        tasks: List of tuples: (object_name, full_path, file_size)
        """
        total_files = len(tasks)
        total_size = sum(t[2] for t in tasks)
        succeeded_count = 0
        succeeded_bytes = 0
        failed_uploads = []
        start_time = time.time()

        # Need threading module for thread name logging in worker
        import threading

        with Progress(*self.progress_columns, transient=True) as progress:
            overall_task = progress.add_task("Overall Upload Progress", total=total_size)
            # Use ThreadPoolExecutor for I/O-bound tasks
            with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_count, thread_name_prefix='Uploader') as executor:
                # Submit all upload tasks
                future_to_file = {
                    # Pass args to _upload_worker
                    executor.submit(self._upload_worker, full_path, bucket_name, obj_name): (full_path, obj_name)
                    for obj_name, full_path, _ in tasks # Use pre-calculated size only for total
                }

                for future in concurrent.futures.as_completed(future_to_file):
                    local_path, obj_name = future_to_file[future]
                    try:
                        # Get result tuple from worker
                        success, _, bytes_uploaded, error_message = future.result()
                        if success:
                            succeeded_count += 1
                            succeeded_bytes += bytes_uploaded
                            progress.update(overall_task, advance=bytes_uploaded)
                        else:
                            failed_uploads.append((local_path, obj_name, error_message))
                            # Warning already logged by worker on failure
                    except Exception as exc:
                        # Catch exceptions raised if _upload_worker itself fails unexpectedly
                        failed_uploads.append((local_path, obj_name, f"Future exception: {exc}"))
                        logger.error(f"Upload task for '{local_path}' generated an exception: {exc}", exc_info=True) # Add traceback
                        # Do not advance progress for unexpected exceptions

        end_time = time.time()
        duration = end_time - start_time
        # Ensure MiB calculation is correct, handle division by zero
        total_size_mib = total_size / (1024 * 1024) if total_size else 0
        succeeded_mib = succeeded_bytes / (1024 * 1024) if succeeded_bytes else 0

        logger.info("-" * 30 + " Upload Summary " + "-" * 30)
        logger.info(f"Operation completed in {duration:.2f} seconds.")
        logger.info(f"Total files attempted: {total_files} ({total_size_mib:.2f} MiB)")
        logger.info(f"Successfully uploaded: {succeeded_count} files ({succeeded_mib:.2f} MiB)")
        logger.info(f"Failed to upload: {len(failed_uploads)}")
        if failed_uploads:
            logger.warning("Failed items:")
            for path, name, err in failed_uploads:
                logger.warning(f"  - {path} (as {name}): {err}")
        logger.info("-" * (60 + len(" Upload Summary ")))


    # --- upload_files method remains the same ---
    # It calls _execute_parallel_upload which uses the modified _upload_worker
    def upload_files(self, file_list: list, bucket_name: str, parallel_count: int):
         """
         Uploads an explicit list of files in parallel.
         file_list: A list of tuples, where each tuple is (local_file_path, object_name)
         """
         tasks = []
         logger.info(f"Preparing to upload {len(file_list)} specified files...")
         for local_file, object_name in file_list:
             if not os.path.isfile(local_file):
                 logger.warning(f"Skipping non-existent file: {local_file}")
                 continue
             try:
                 file_size = os.path.getsize(local_file)
                 tasks.append((object_name, local_file, file_size))
             except OSError as e:
                  logger.warning(f"Skipping file '{local_file}' due to error getting size: {e}")

         if not tasks:
              logger.error("No valid files found to upload from the provided list.")
              return

         total_files = len(tasks)
         total_size_mb = sum(t[2] for t in tasks) / (1024 * 1024) if tasks else 0
         logger.info(f"Starting upload of {total_files} files ({total_size_mb:.2f} MiB) to bucket '{bucket_name}' using {parallel_count} threads...")

         if self.dry_run:
             logger.info("DRY-RUN: Simulating file list upload...")
             for object_name, full_path, _ in tasks:
                 logger.info(f"DRY-RUN: Would upload '{full_path}' as '{object_name}'.")
             logger.info("DRY-RUN: File list upload simulation complete.")
             return

         self._execute_parallel_upload(tasks, bucket_name, parallel_count)


    # --- upload_folder method remains the same ---
    # It calls _execute_parallel_upload which uses the modified _upload_worker
    def upload_folder(self, local_dir: str, bucket_name: str, object_prefix: str, parallel_count: int):
        """
        Uploads all files from a local directory to OCI Object Storage using parallel execution.
        """
        if not os.path.isdir(local_dir):
            logger.error(f"Local directory '{local_dir}' does not exist or is not a directory.")
            return

        tasks = []
        logger.info(f"Scanning directory '{local_dir}' for files to upload...")
        for root, _, files in os.walk(local_dir):
            for file in files:
                full_path = os.path.join(root, file)
                try:
                     if os.path.islink(full_path):
                          logger.debug(f"Skipping symbolic link: {full_path}")
                          continue
                     if not os.path.isfile(full_path):
                          logger.debug(f"Skipping non-file item: {full_path}")
                          continue

                     file_size = os.path.getsize(full_path)
                     relative_path = os.path.relpath(full_path, local_dir)
                     # Ensure forward slashes for object storage paths
                     object_name_parts = [part for part in relative_path.split(os.sep)]

                     # Handle prefixing correctly
                     if object_prefix:
                         # Ensure prefix doesn't have leading/trailing slashes for joining
                         cleaned_prefix = object_prefix.strip('/')
                         # Join prefix and relative path parts
                         final_object_name = "/".join([cleaned_prefix] + object_name_parts)
                     else:
                          # No prefix, just use relative path parts
                          final_object_name = "/".join(object_name_parts)

                     tasks.append((final_object_name, full_path, file_size))
                     # Reduce debug noise during scan
                     # logger.debug(f"Found file: '{full_path}' -> OCI object: '{final_object_name}' (Size: {file_size})")

                except OSError as e:
                    logger.warning(f"Skipping file '{full_path}' due to error: {e}")
                except Exception as e:
                     logger.warning(f"Skipping file '{full_path}' due to unexpected error during scanning: {e}")

        if not tasks:
            logger.info(f"No files found to upload in directory '{local_dir}'.")
            return

        total_files = len(tasks)
        total_size_mb = sum(t[2] for t in tasks) / (1024 * 1024) if tasks else 0
        logger.info(f"Found {total_files} files ({total_size_mb:.2f} MiB) to upload.")
        logger.info(f"Starting bulk upload to bucket '{bucket_name}' under prefix '{object_prefix or '<bucket root>'}' using {parallel_count} threads...") # Clarify prefix


        if self.dry_run:
            logger.info("DRY-RUN: Simulating bulk folder upload...")
            for object_name, full_path, _ in tasks:
                logger.info(f"DRY-RUN: Would upload '{full_path}' as '{object_name}'.")
            logger.info("DRY-RUN: Bulk folder upload simulation complete.")
            return

        self._execute_parallel_upload(tasks, bucket_name, parallel_count)