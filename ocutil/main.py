#!/usr/bin/env python3
import argparse
import os
import sys
import multiprocessing
import logging
import time
import glob # Import glob directly
from urllib.parse import urlparse

import oci # Import oci for potential exceptions

from ocutil.utils.oci_manager import OCIManager
from ocutil.utils.uploader import Uploader
from ocutil.utils.downloader import Downloader

# --- is_remote_path, parse_remote_path, setup_logging ---
# (Keep these functions as they are in your original code)
def is_remote_path(path: str) -> bool:
    return path.startswith("oc://")

def parse_remote_path(remote_path: str):
    parsed = urlparse(remote_path)
    if parsed.scheme != "oc":
        raise ValueError("Remote path must start with 'oc://'")
    bucket_name = parsed.netloc
    object_path = parsed.path.lstrip('/')
    return bucket_name, object_path

def setup_logging(log_file=None, verbose=False):
    log_level = logging.DEBUG if verbose else logging.INFO
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        # Ensure log directory exists
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            try:
                os.makedirs(log_dir)
            except OSError as e:
                 print(f"Warning: Could not create log directory '{log_dir}': {e}", file=sys.stderr)
                 log_file = None # Disable file logging if dir creation fails
        if log_file:
             handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s [%(levelname)s] %(threadName)s %(name)s: %(message)s',
        handlers=handlers,
        datefmt='%Y-%m-%d %H:%M:%S' # Added date format
    )
    # Silence overly verbose OCI SDK logs unless verbose is specifically requested
    oci_log_level = logging.DEBUG if verbose else logging.WARNING
    logging.getLogger('oci').setLevel(oci_log_level)
    return logging.getLogger('ocutil') # Return the main application logger

def adjust_remote_object_path(local_source: str, object_path: str) -> str:
    """
    Adjusts the remote object path.
    - If object_path ends with '/', append local basename.
    - If object_path has no extension AND is not just the basename, treat as prefix and append basename.
    - If object_path is empty, use local basename.
    - Otherwise, use object_path as is (assumed to be a full file path).
    """
    local_basename = os.path.basename(local_source)

    if not object_path: # Destination is just oc://bucket
        return local_basename
    elif object_path.endswith('/'): # Destination is oc://bucket/prefix/
        return object_path + local_basename
    else:
        # Check if destination looks like a directory (no extension)
        name, ext = os.path.splitext(object_path)
        # If it has no extension OR if the object_path itself is intended as a prefix folder
        # (this logic might need refinement based on exact desired gsutil behavior)
        # A common case: ocutil file.txt oc://bucket/target_prefix
        # We want it to become oc://bucket/target_prefix/file.txt
        # If the target *could* be a file: ocutil file.txt oc://bucket/target_name
        # We want it to become oc://bucket/target_name
        # Let's simplify: if the target *doesn't* contain the source filename, treat it as a prefix
        if '/' in object_path and not object_path.endswith(local_basename):
             # It looks like a prefix path was given, treat it as such
             return object_path.rstrip('/') + '/' + local_basename
        # Otherwise, assume the destination is the intended full object name
        return object_path


def main():
    parser = argparse.ArgumentParser(
        description="ocutil: Oracle Cloud Object Storage CLI (similar to gsutil)."
    )
    parser.add_argument("source", help="Source path. Can be local file/dir/wildcard or remote (oc://bucket/path).")
    parser.add_argument("destination", help="Destination path. Can be remote (oc://bucket/path) or local directory.")
    parser.add_argument("--config-profile", default="DEFAULT", help="OCI config profile (default: DEFAULT).")
    parser.add_argument("--parallel", type=int, default=multiprocessing.cpu_count(),
                        help="Number of parallel threads for bulk operations (default: number of CPUs)")
    parser.add_argument("--dry-run", action="store_true", help="Simulate actions without transferring data")
    parser.add_argument("--log-file", type=str, help="File to write logs to")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging (includes OCI SDK debug logs)")
    args = parser.parse_args()

    logger = setup_logging(log_file=args.log_file, verbose=args.verbose)
    parallel_count = max(1, args.parallel) # Ensure at least one thread

    operation_successful = False # Track overall outcome

    try:
        oci_manager = OCIManager(config_profile=args.config_profile) # Initialize once

        # --- Download Operation ---
        if is_remote_path(args.source):
            if is_remote_path(args.destination):
                 logger.error("Invalid command: Cannot specify remote paths for both source and destination.")
                 parser.print_help()
                 sys.exit(1)

            remote_path = args.source
            local_destination = args.destination

            # Destination must be a directory for downloads
            # Create it if it doesn't exist
            try:
                 if not os.path.exists(local_destination):
                     logger.info(f"Destination directory '{local_destination}' does not exist. Creating it.")
                     os.makedirs(local_destination, exist_ok=True)
                 elif not os.path.isdir(local_destination):
                     logger.error(f"Destination path '{local_destination}' exists but is not a directory.")
                     sys.exit(1)
            except OSError as e:
                  logger.error(f"Failed to create destination directory '{local_destination}': {e}")
                  sys.exit(1)


            downloader = Downloader(oci_manager=oci_manager, dry_run=args.dry_run)
            bucket_name, object_path = parse_remote_path(remote_path)

            # Determine if source is likely a single file or a folder prefix
            is_folder_download = False
            is_single_file_download = False

            if object_path.endswith('/'):
                is_folder_download = True
                logger.info(f"Source path ends with '/', treating as folder download: '{object_path}'")
            elif not object_path: # oc://bucket-name case (download whole bucket)
                 is_folder_download = True
                 logger.info(f"Source path has no object path, treating as full bucket download.")
            else:
                # Try head_object to see if it's an exact file match
                try:
                    logger.debug(f"Checking if '{object_path}' is a single object...")
                    oci_manager.object_storage.head_object(oci_manager.namespace, bucket_name, object_path)
                    is_single_file_download = True
                    logger.info(f"Source path '{object_path}' matches a single object.")
                except oci.exceptions.ServiceError as e:
                    if e.status == 404:
                        # Not an exact file match, check if it's a prefix for other objects
                        logger.debug(f"'{object_path}' not found as a single object (404). Checking if it's a prefix...")
                        list_response = oci_manager.object_storage.list_objects(
                            oci_manager.namespace, bucket_name, prefix=object_path, limit=1
                        )
                        if list_response.data.objects:
                             is_folder_download = True
                             logger.info(f"Source path '{object_path}' is not a single object but matches existing object prefixes. Treating as folder download.")
                        else:
                             logger.error(f"Source path '{remote_path}' does not exist as an object or a valid prefix.")
                             sys.exit(1)
                    else:
                         logger.error(f"Error checking source path '{remote_path}': {e.status} - {e.message}")
                         sys.exit(1)
                except Exception as e:
                     logger.error(f"Unexpected error checking source path '{remote_path}': {e}")
                     sys.exit(1)

            # --- Execute Download ---
            if is_single_file_download:
                # Destination file path within the target directory
                local_file_path = os.path.join(local_destination, os.path.basename(object_path))
                logger.info(f"Initiating single file download: '{remote_path}' -> '{local_file_path}'.")
                operation_successful = downloader.download_single_file(bucket_name, object_path, local_file_path)

            elif is_folder_download:
                # For folder downloads, OCI objects matching the prefix are placed directly
                # into the specified local_destination directory, maintaining relative structure.
                logger.info(f"Initiating bulk download with {parallel_count} parallel threads: '{remote_path}' -> '{local_destination}/'.")
                # download_folder handles the summary internally now
                downloader.download_folder(bucket_name, object_path, local_destination, parallel_count=parallel_count)
                # Assume success unless errors were logged by downloader
                operation_successful = True # Or check downloader status if it returns one

            else:
                 # This case should ideally be handled by the checks above
                 logger.error(f"Could not determine whether '{remote_path}' is a file or folder.")
                 sys.exit(1)

        # --- Upload Operation ---
        elif is_remote_path(args.destination):
            local_source = args.source
            remote_destination = args.destination

            uploader = Uploader(oci_manager=oci_manager, dry_run=args.dry_run)
            try:
                bucket_name, object_path = parse_remote_path(remote_destination)
            except ValueError as e:
                logger.error(f"Error parsing remote destination path: {e}")
                sys.exit(1)

            # --- Handle Wildcard Upload ---
            if '*' in local_source or '?' in local_source or '[' in local_source: # Check for glob patterns
                 logger.info(f"Source '{local_source}' contains wildcard characters. Expanding matches...")
                 # Use glob to find matching files
                 files_matched = glob.glob(local_source)

                 if not files_matched:
                     logger.error(f"No local files matched the pattern: {local_source}")
                     sys.exit(1)

                 # Filter out directories from glob results, only upload files
                 files_to_upload = [f for f in files_matched if os.path.isfile(f)]

                 if not files_to_upload:
                     logger.error(f"Pattern '{local_source}' matched only directories or non-file items. Nothing to upload.")
                     sys.exit(1)

                 logger.info(f"Found {len(files_to_upload)} files matching pattern to upload.")

                 # Prepare list for upload_files: [(local_path, object_name), ...]
                 upload_list = []
                 for file_path in files_to_upload:
                      # When uploading multiple files (wildcard), the destination object_path acts as a prefix.
                      # Append the basename of the local file to the destination prefix.
                      local_basename = os.path.basename(file_path)
                      if object_path:
                           # Ensure object_path ends with / if it's meant as a prefix
                           prefix = object_path.rstrip('/') + '/'
                           final_object_name = prefix + local_basename
                      else: # Uploading to bucket root
                           final_object_name = local_basename
                      upload_list.append((file_path, final_object_name))
                      logger.debug(f"Queueing wildcard match: '{file_path}' -> '{final_object_name}'")

                 # Use the new upload_files method for parallel execution
                 uploader.upload_files(upload_list, bucket_name, parallel_count=parallel_count)
                 operation_successful = True # Assume success unless errors logged by uploader

            # --- Handle Single File Upload ---
            elif os.path.isfile(local_source):
                 # Adjust destination path if necessary (e.g., target is prefix)
                 final_object_path = adjust_remote_object_path(local_source, object_path)
                 logger.info(f"Initiating single file upload: '{local_source}' -> 'oc://{bucket_name}/{final_object_path}'.")
                 operation_successful = uploader.upload_single_file(local_source, bucket_name, final_object_path)

            # --- Handle Folder Upload ---
            elif os.path.isdir(local_source):
                 # When uploading a directory, the destination object_path acts as a prefix.
                 # If object_path is empty, use the source directory's basename as the prefix.
                 final_object_prefix = object_path
                 if not final_object_prefix:
                       final_object_prefix = os.path.basename(os.path.normpath(local_source))
                       logger.info(f"No remote prefix specified, using source directory name as prefix: '{final_object_prefix}'")

                 logger.info(f"Initiating bulk upload of folder '{local_source}' with {parallel_count} parallel threads to 'oc://{bucket_name}/{final_object_prefix}/'.")
                 uploader.upload_folder(local_source, bucket_name, final_object_prefix, parallel_count=parallel_count)
                 operation_successful = True # Assume success unless errors logged by uploader

            else:
                 logger.error(f"Local source path '{local_source}' is not a valid file, directory, or wildcard pattern.")
                 sys.exit(1)

        # --- Invalid Command ---
        else:
            logger.error("Invalid command: One of source or destination must be a remote path (oc://...). Both cannot be local.")
            parser.print_help()
            sys.exit(1)

        # Final status message based on operation outcome
        if operation_successful:
            logger.info("Operation completed.") # Summary logs handled within classes now
        else:
            logger.error("Operation finished with errors.")
            sys.exit(1) # Exit with error code if single file ops failed

    except oci.exceptions.ConfigFileNotFound as e:
         logger.error(f"OCI Configuration Error: {e}. Please ensure ~/.oci/config exists and is configured.")
         sys.exit(1)
    except oci.exceptions.MissingConfigValue as e:
         logger.error(f"OCI Configuration Error: Missing value in profile '{args.config_profile}'. {e}")
         sys.exit(1)
    except oci.exceptions.RequestException as e:
         logger.error(f"OCI API Request Error: {e.status} - {e.message}")
         # Log more details if available and verbose
         if args.verbose and hasattr(e, 'headers') and e.headers.get('opc-request-id'):
              logger.debug(f"OCI Request ID: {e.headers.get('opc-request-id')}")
         sys.exit(1)
    except Exception as ex:
        logger.error(f"An unexpected error occurred: {ex}", exc_info=args.verbose) # Show traceback if verbose
        sys.exit(1)

if __name__ == "__main__":
    main()