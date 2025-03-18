#!/usr/bin/env python3
import argparse
import os
import sys
import multiprocessing
import logging
import time
from urllib.parse import urlparse

import oci

from ocutil.utils.oci_manager import OCIManager
from ocutil.utils.uploader import Uploader
from ocutil.utils.downloader import Downloader

def is_remote_path(path: str) -> bool:
    """Return True if the given path starts with 'oc://'."""
    return path.startswith("oc://")

def parse_remote_path(remote_path: str):
    """
    Parses a remote path of the form oc://bucket-name/path/to/object.
    
    Returns:
        bucket_name (str): Name of the OCI bucket.
        object_path (str): Path to the object inside the bucket.
    """
    parsed = urlparse(remote_path)
    if parsed.scheme != "oc":
        raise ValueError("Remote path must start with 'oc://'")
    bucket_name = parsed.netloc
    # Ensure object_path does not have a leading slash (for consistency)
    object_path = parsed.path.lstrip('/')
    return bucket_name, object_path

def setup_logging(log_file=None, verbose=False):
    """Configure logging format, level, and handlers."""
    log_level = logging.DEBUG if verbose else logging.INFO
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s [%(levelname)s] %(threadName)s %(name)s: %(message)s',
        handlers=handlers
    )
    return logging.getLogger('ocutil')

def adjust_remote_object_path(local_source: str, object_path: str) -> str:
    """
    Adjusts the remote object path for single file uploads.
    - If object_path is empty, returns the basename of the local_source.
    - If object_path has no extension and does not equal the basename of local_source,
      then treats object_path as a folder name and appends the basename.
    - Otherwise, returns object_path as-is.
    """
    if not object_path:
        return os.path.basename(local_source)
    else:
        name, ext = os.path.splitext(object_path)
        if ext == "" and (object_path.rstrip('/') != os.path.basename(local_source)):
            return object_path.rstrip('/') + '/' + os.path.basename(local_source)
        return object_path

def main():
    parser = argparse.ArgumentParser(
        description="ocutil: Oracle Cloud Object Storage CLI (similar to gsutil)."
    )
    parser.add_argument("source", help="Source path. Can be a local file/directory or a remote path (oc://bucket/path).")
    parser.add_argument("destination", help="Destination path. Can be a remote path (oc://bucket/path) or a local directory.")
    parser.add_argument("--config-profile", default="DEFAULT", help="OCI config profile (default: DEFAULT).")
    parser.add_argument("--parallel", type=int, default=multiprocessing.cpu_count(),
                        help="Number of parallel threads for bulk operations (default: number of CPUs)")
    parser.add_argument("--dry-run", action="store_true", help="Simulate actions without transferring data")
    parser.add_argument("--log-file", type=str, help="File to write logs to")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()

    logger = setup_logging(log_file=args.log_file, verbose=args.verbose)
    parallel_count = args.parallel

    try:
        if is_remote_path(args.source):
            # Download operation
            remote_path = args.source
            local_destination = args.destination

            if not os.path.isdir(local_destination):
                logger.error(f"Destination '{local_destination}' is not a valid directory.")
                sys.exit(1)

            oci_manager = OCIManager(config_profile=args.config_profile)
            # Pass the dry-run flag to Downloader
            downloader = Downloader(oci_manager=oci_manager, dry_run=args.dry_run)

            bucket_name, object_path = parse_remote_path(remote_path)
            # Determine if we should treat this as a file or folder.
            if object_path.endswith('/'):
                is_file = False
            else:
                try:
                    oci_manager.object_storage.head_object(oci_manager.namespace, bucket_name, object_path)
                    is_file = True
                except Exception as e:
                    logger.info(f"head_object did not find a file at '{object_path}'; assuming it's a folder. ({e})")
                    is_file = False

            if is_file:
                local_file_path = os.path.join(local_destination, os.path.basename(object_path))
                logger.info(f"Initiating single file download into '{local_file_path}'.")
                if args.dry_run:
                    logger.info(f"DRY-RUN: Would download file '{object_path}' from bucket '{bucket_name}' to '{local_file_path}'.")
                else:
                    downloader.download_single_file(bucket_name, object_path, local_file_path)
            else:
                # When downloading a folder, use the last part of the object_path as the local folder name.
                folder_name = os.path.basename(os.path.normpath(object_path))
                new_destination = os.path.join(local_destination, folder_name)
                logger.info(f"Initiating bulk download with {parallel_count} parallel threads into '{new_destination}'.")
                if args.dry_run:
                    logger.info(f"DRY-RUN: Would download folder '{object_path}' from bucket '{bucket_name}' to '{new_destination}' with {parallel_count} threads.")
                else:
                    downloader.download_folder(bucket_name, object_path, new_destination, parallel_count=parallel_count)

        elif is_remote_path(args.destination):
            # Upload operation
            local_source = args.source
            remote_destination = args.destination

            oci_manager = OCIManager(config_profile=args.config_profile)
            # Pass the dry-run flag to Uploader
            uploader = Uploader(oci_manager=oci_manager, dry_run=args.dry_run)

            try:
                bucket_name, object_path = parse_remote_path(remote_destination)
            except ValueError as e:
                logger.error(f"Error parsing remote path: {e}")
                sys.exit(1)

            # --- NEW LOGIC: Check for wildcard in source ---
            if '*' in local_source:
                import glob
                files = glob.glob(local_source)
                if not files:
                    logger.error(f"No files matched wildcard: {local_source}")
                    sys.exit(1)
                for file_path in files:
                    adjusted_path = adjust_remote_object_path(file_path, object_path)
                    if args.dry_run:
                        logger.info(f"DRY-RUN: Would upload '{file_path}' as '{adjusted_path}' to bucket '{bucket_name}'.")
                    else:
                        logger.info(f"Initiating single file upload for '{file_path}' as '{adjusted_path}'.")
                        uploader.upload_single_file(file_path, bucket_name, adjusted_path)
            elif os.path.isfile(local_source):
                object_path = adjust_remote_object_path(local_source, object_path)
                logger.info(f"Adjusted object path for file upload: '{object_path}'")
                if args.dry_run:
                    logger.info(f"DRY-RUN: Would upload file '{local_source}' to bucket '{bucket_name}' as '{object_path}'.")
                else:
                    logger.info("Initiating single file upload.")
                    uploader.upload_single_file(local_source, bucket_name, object_path)
            elif os.path.isdir(local_source):
                if not object_path:
                    object_path = os.path.basename(os.path.normpath(local_source))
                elif object_path.endswith('/'):
                    object_path = object_path.rstrip('/') + '/' + os.path.basename(os.path.normpath(local_source))
                if args.dry_run:
                    logger.info(f"DRY-RUN: Would bulk upload folder '{local_source}' to bucket '{bucket_name}' under prefix '{object_path}' with {parallel_count} threads.")
                else:
                    logger.info(f"Initiating bulk upload of folder '{local_source}' with {parallel_count} parallel threads.")
                    uploader.upload_folder(local_source, bucket_name, object_path, parallel_count=parallel_count)
            else:
                logger.error(f"Source '{local_source}' is neither a file nor a directory.")
                sys.exit(1)
        else:
            logger.error("Invalid command. Either the source or destination must be a remote path (oc://).")
            parser.print_help()
            sys.exit(1)

        logger.info("Operation completed successfully.")

    except Exception as ex:
        logger.error(f"Operation failed: {ex}")
        sys.exit(1)

if __name__ == "__main__":
    main()
