#!/usr/bin/env python3
import argparse
import os
import sys
import multiprocessing
import logging
import time
import glob
# import math # No longer needed here
# import datetime # No longer needed here
from urllib.parse import urlparse

import oci

# Core Utils
from ocutil.utils.oci_manager import OCIManager
from ocutil.utils.formatters import human_readable_size # Import formatter

# Command Handlers/Classes
from ocutil.utils.uploader import Uploader
from ocutil.utils.downloader import Downloader
from ocutil.utils.lister import Lister # Import the new Lister


# --- Helper Functions ---
def is_remote_path(path: str) -> bool:
    """Return True if the given path starts with 'oc://'."""
    return path.startswith("oc://")

def parse_remote_path(remote_path: str):
    """
    Parses a remote path of the form oc://bucket-name/path/to/object.

    Returns:
        bucket_name (str): Name of the OCI bucket.
        object_path (str): Path to the object inside the bucket (prefix).
    """
    parsed = urlparse(remote_path)
    if parsed.scheme != "oc":
        raise ValueError("Remote path must start with 'oc://'")
    bucket_name = parsed.netloc
    if not bucket_name:
        raise ValueError("Bucket name cannot be empty in remote path.")
    # Ensure object_path does not have a leading slash (for consistency)
    object_path = parsed.path.lstrip('/')
    return bucket_name, object_path

def setup_logging(log_file=None, verbose=False):
    """Configure logging format, level, and handlers."""
    log_level = logging.DEBUG if verbose else logging.INFO
    # Default handler writes to stderr
    handlers = [logging.StreamHandler(sys.stderr)]
    log_formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(threadName)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            try:
                os.makedirs(log_dir)
            except OSError as e:
                 print(f"Warning: Could not create log directory '{log_dir}': {e}", file=sys.stderr)
                 log_file = None # Disable file logging if dir creation fails
        if log_file:
             try:
                 file_handler = logging.FileHandler(log_file)
                 file_handler.setFormatter(log_formatter)
                 handlers.append(file_handler)
             except OSError as e:
                  print(f"Warning: Could not open log file '{log_file}': {e}", file=sys.stderr)

    # Configure root logger
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s [%(levelname)s] %(threadName)s %(name)s: %(message)s',
        handlers=handlers, # Use handlers list defined above
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Set levels for noisy libraries
    oci_log_level = logging.DEBUG if verbose else logging.WARNING
    logging.getLogger('oci').setLevel(oci_log_level)
    urllib3_log_level = logging.DEBUG if verbose else logging.WARNING
    logging.getLogger("urllib3.connectionpool").setLevel(urllib3_log_level)

    # Return the main application logger instance
    return logging.getLogger('ocutil')


def adjust_remote_object_path(local_source: str, object_path: str) -> str:
    """
    Adjusts the remote object path for single file uploads based on destination format.
    """
    local_basename = os.path.basename(local_source)
    # If destination is empty (oc://bucket) or ends with '/' (oc://bucket/prefix/)
    if not object_path or object_path.endswith('/'):
        return object_path + local_basename
    # If destination looks like a file path already
    # (This check might need refinement depending on desired gsutil parity)
    # A simple check: if it contains the basename already, assume it's intentional.
    # Otherwise, treat the destination as a prefix.
    elif '/' in object_path and not object_path.endswith('/' + local_basename):
        # Check if the last part of object_path might be intended as filename
        _, potential_ext = os.path.splitext(object_path)
        if not potential_ext: # Treat as prefix if no extension
             return object_path.rstrip('/') + '/' + local_basename
    # Default: Assume object_path is the intended full remote filename
    return object_path


# --- CP Command Handler (Contains logic moved from previous main) ---
def handle_cp_command(args, oci_manager: OCIManager, logger: logging.Logger):
    """Handles the logic for the 'cp' command."""
    parallel_count = max(1, args.parallel) # Ensure at least one thread

    # --- Download Operation ---
    if is_remote_path(args.source):
        if is_remote_path(args.destination):
             logger.error("Invalid 'cp' command: Both source and destination cannot be remote paths.")
             sys.exit(1)
        # ... (rest of download logic from previous version) ...
        remote_path = args.source
        local_destination = args.destination
        # (Create local dir, check file/folder, call downloader...)
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
        try:
            bucket_name, object_path = parse_remote_path(remote_path)
        except ValueError as e:
             logger.error(f"Invalid source OCI path format: {e}")
             sys.exit(1)

        is_folder_download = False
        is_single_file_download = False
        # (Logic to determine if single file or folder download...)
        if not object_path:
             is_folder_download = True
             logger.info(f"Source path is bucket root, treating as full bucket download.")
        elif object_path.endswith('/'):
            is_folder_download = True
            logger.info(f"Source path ends with '/', treating as folder download: '{object_path}'")
        else:
            # (head_object / list_objects check...)
            try:
                logger.debug(f"Checking if '{object_path}' is a single object...")
                oci_manager.object_storage.head_object(oci_manager.namespace, bucket_name, object_path)
                is_single_file_download = True
                logger.info(f"Source path '{object_path}' matches a single object.")
            except oci.exceptions.ServiceError as e:
                if e.status == 404:
                    logger.debug(f"'{object_path}' not found as a single object (404). Checking if it's a prefix...")
                    list_response = oci_manager.object_storage.list_objects(
                        oci_manager.namespace, bucket_name, prefix=object_path, limit=1, fields="name"
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

        operation_successful = False
        if is_single_file_download:
            local_file_path = os.path.join(local_destination, os.path.basename(object_path))
            logger.info(f"Initiating single file download: '{remote_path}' -> '{local_file_path}'.")
            operation_successful = downloader.download_single_file(bucket_name, object_path, local_file_path)
        elif is_folder_download:
            logger.info(f"Initiating bulk download with {parallel_count} parallel threads: '{remote_path}' -> '{local_destination}/'.")
            downloader.download_folder(bucket_name, object_path, local_destination, parallel_count=parallel_count)
            operation_successful = True # Assume success unless downloader logged errors
        else:
             logger.error(f"Could not determine download type for '{remote_path}'.")
             sys.exit(1)

        if not operation_successful:
             logger.error("Download operation finished with errors.")
             sys.exit(1)


    # --- Upload Operation ---
    elif is_remote_path(args.destination):
        if is_remote_path(args.source):
             logger.error("Invalid 'cp' command: Both source and destination cannot be remote paths.")
             sys.exit(1)
        # ... (rest of upload logic from previous version) ...
        local_source = args.source
        remote_destination = args.destination
        # (Handle wildcards, single file, folder, call uploader...)
        uploader = Uploader(oci_manager=oci_manager, dry_run=args.dry_run)
        try:
            bucket_name, object_path = parse_remote_path(remote_destination)
        except ValueError as e:
            logger.error(f"Error parsing remote destination path: {e}")
            sys.exit(1)

        # (Wildcard check...)
        if '*' in local_source or '?' in local_source or '[' in local_source:
             logger.info(f"Source '{local_source}' contains wildcard characters. Expanding matches...")
             # (glob logic...)
             files_matched = glob.glob(local_source)
             if not files_matched:
                 logger.error(f"No local files matched the pattern: {local_source}")
                 sys.exit(1)
             files_to_upload = [f for f in files_matched if os.path.isfile(f)]
             if not files_to_upload:
                 logger.error(f"Pattern '{local_source}' matched only directories or non-file items. Nothing to upload.")
                 sys.exit(1)
             # (Build upload list...)
             logger.info(f"Found {len(files_to_upload)} files matching pattern to upload.")
             upload_list = []
             # (Determine final object name based on destination...)
             for file_path in files_to_upload:
                  local_basename = os.path.basename(file_path)
                  # Treat destination as prefix for wildcard uploads
                  prefix = object_path.rstrip('/') + '/' if object_path else ''
                  final_object_name = prefix + local_basename
                  upload_list.append((file_path, final_object_name))
                  logger.debug(f"Queueing wildcard match: '{file_path}' -> '{final_object_name}'")

             uploader.upload_files(upload_list, bucket_name, parallel_count=parallel_count)

        elif os.path.isfile(local_source):
             # (Single file upload...)
             final_object_path = adjust_remote_object_path(local_source, object_path)
             logger.info(f"Initiating single file upload: '{local_source}' -> 'oc://{bucket_name}/{final_object_path}'.")
             operation_successful = uploader.upload_single_file(local_source, bucket_name, final_object_path)
             if not operation_successful:
                  logger.error("Upload operation finished with errors.")
                  sys.exit(1)
        elif os.path.isdir(local_source):
             # (Folder upload...)
             final_object_prefix = object_path
             if not final_object_prefix:
                   final_object_prefix = os.path.basename(os.path.normpath(local_source))
                   logger.info(f"No remote prefix specified, using source directory name as prefix: '{final_object_prefix}'")

             logger.info(f"Initiating bulk upload of folder '{local_source}' with {parallel_count} parallel threads to 'oc://{bucket_name}/{final_object_prefix}/'.")
             uploader.upload_folder(local_source, bucket_name, final_object_prefix, parallel_count=parallel_count)
        else:
             logger.error(f"Local source path '{local_source}' is not a valid file, directory, or wildcard pattern.")
             sys.exit(1)

    # --- Invalid Local/Local cp Command ---
    else:
        logger.error("Invalid 'cp' command: One of source or destination must be a remote path (oc://...). Both cannot be local.")
        sys.exit(1)

    logger.info("cp command finished.") # Info log for completion of cp


# --- Main execution block ---
def main():
    parser = argparse.ArgumentParser(
        description="ocutil: Oracle Cloud Object Storage CLI.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  ocutil cp local_file.txt oc://my-bucket/remote_file.txt
  ocutil cp oc://my-bucket/some_folder/ local_download_dir/
  ocutil cp local_folder/ oc://my-bucket/remote_folder_prefix/
  ocutil cp 'local_folder/*.log' oc://my-bucket/logs/
  ocutil ls oc://my-bucket/some_folder/
  ocutil ls oc://my-bucket/ -lHr
""" # Added recursive example
    )
    # Common arguments
    parser.add_argument("--config-profile", default="DEFAULT", help="OCI config profile (default: DEFAULT).")
    parser.add_argument("--log-file", type=str, help="File to write logs to")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging (DEBUG level)")

    # Add subparsers
    subparsers = parser.add_subparsers(dest='command', required=True, metavar='COMMAND',
                                       help="Available commands: cp, ls") # List commands

    # --- CP Sub-command Parser ---
    cp_parser = subparsers.add_parser('cp', help='Copy files/objects between local and OCI Object Storage.')
    cp_parser.add_argument("source", help="Source path (local or oc://...). Wildcards allowed for local source.")
    cp_parser.add_argument("destination", help="Destination path (local or oc://...).")
    cp_parser.add_argument("--parallel", type=int, default=multiprocessing.cpu_count(),
                        help="Number of parallel threads for bulk operations (default: number of CPUs)")
    cp_parser.add_argument("--dry-run", action="store_true", help="Simulate actions without transferring data")

    # --- LS Sub-command Parser ---
    ls_parser = subparsers.add_parser('ls', help='List objects and prefixes in OCI Object Storage.')
    ls_parser.add_argument("oci_path", help="OCI path (e.g., oc://bucket-name/prefix/). Use oc://bucket-name/ to list bucket root.")
    ls_parser.add_argument("-l", "--long", action="store_true", help="Display long format including size and modification time.")
    ls_parser.add_argument("-H", "--human-readable", action="store_true", help="Display sizes in human-readable format (KiB, MiB, etc.). Requires -l.")
    ls_parser.add_argument("-r", "--recursive", action="store_true", help="Recursively list objects under the prefix.")

    # --- Parse Arguments ---
    args = parser.parse_args()

    # --- Setup ---
    logger = setup_logging(log_file=args.log_file, verbose=args.verbose)

    try:
        # Initialize OCI Manager (common for all commands)
        logger.debug(f"Initializing OCIManager with profile: {args.config_profile}")
        oci_manager = OCIManager(config_profile=args.config_profile)
        logger.debug(f"Using OCI Namespace: {oci_manager.namespace}")
    except oci.exceptions.ConfigFileNotFound as e:
         logger.error(f"OCI Configuration Error: {e}. Please ensure ~/.oci/config exists and is configured.")
         sys.exit(1)
    except oci.exceptions.MissingConfigValue as e:
         logger.error(f"OCI Configuration Error: Missing value in profile '{args.config_profile}'. {e}")
         sys.exit(1)
    except Exception as ex:
        logger.error(f"Failed to initialize OCI Manager: {ex}", exc_info=args.verbose)
        sys.exit(1)

    # --- Dispatch to Command Handler ---
    start_time = time.time()
    try:
        if args.command == 'cp':
            # Call the 'cp' handler
            handle_cp_command(args, oci_manager, logger)
        elif args.command == 'ls':
            # Handle 'ls' command by creating Lister and calling its method
            try:
                bucket_name, prefix = parse_remote_path(args.oci_path)
                lister = Lister(oci_manager)
                lister.list_path(
                    bucket_name=bucket_name,
                    prefix=prefix,
                    long_format=args.long,
                    human_readable=args.human_readable,
                    recursive=args.recursive
                )
            except ValueError as e:
                # Catch path parsing errors specific to ls path format
                logger.error(f"Invalid OCI path format for 'ls': {e}")
                sys.exit(1)
            # Note: OCI API/listing errors are handled within lister.list_path now
        else:
            # This case should not be reached due to 'required=True' in add_subparsers
            logger.error(f"Internal error: Unknown command '{args.command}'")
            parser.print_help()
            sys.exit(1)

        # Log overall completion time at DEBUG level
        end_time = time.time()
        logger.debug(f"Command '{args.command}' completed successfully in {end_time - start_time:.2f} seconds.")

    except oci.exceptions.RequestException as e:
         # Catch OCI request errors that might propagate up (though handlers should catch most)
         logger.error(f"OCI API Request Error: {e.status} - {e.message}")
         if args.verbose and hasattr(e, 'headers') and e.headers.get('opc-request-id'):
              logger.debug(f"OCI Request ID: {e.headers.get('opc-request-id')}")
         sys.exit(1)
    except KeyboardInterrupt:
         logger.warning("Operation interrupted by user (Ctrl+C).")
         sys.exit(130) # Standard exit code for Ctrl+C
    except Exception as ex:
        # Catch any other unexpected errors during command execution
        logger.error(f"An unexpected error occurred during command execution: {ex}", exc_info=args.verbose)
        sys.exit(1)

if __name__ == "__main__":
    main()