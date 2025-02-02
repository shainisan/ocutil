#!/usr/bin/env python3
import argparse
import os
import sys
import multiprocessing
import logging
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

def setup_logging():
    """Configure logging format and level."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
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
    logger = setup_logging()

    parser = argparse.ArgumentParser(
        description="ocutil: Oracle Cloud Object Storage CLI (similar to gsutil)."
    )
    parser.add_argument("source", help="Source path. Can be a local file/directory or a remote path (oc://bucket/path).")
    parser.add_argument("destination", help="Destination path. Can be a remote path (oc://bucket/path) or a local directory.")
    parser.add_argument(
        "--config-profile",
        default="DEFAULT",
        help="OCI config profile (default: DEFAULT)."
    )
    args = parser.parse_args()

    source = args.source
    destination = args.destination
    config_profile = args.config_profile
    cpu_count = multiprocessing.cpu_count()

    # Determine operation based on which argument is remote
    try:
        if is_remote_path(source):
            # Download operation
            remote_path = source
            local_destination = destination

            if not os.path.isdir(local_destination):
                logger.error(f"Destination '{local_destination}' is not a valid directory.")
                sys.exit(1)

            oci_manager = OCIManager(config_profile=config_profile)
            downloader = Downloader(oci_manager=oci_manager)

            bucket_name, object_path = parse_remote_path(remote_path)
            # Use the raw folder path (object_path) as returned from parse_remote_path.
            folder_name = os.path.basename(os.path.normpath(object_path))
            new_destination = os.path.join(local_destination, folder_name)
            logger.info(f"Initiating bulk download with {cpu_count} parallel threads into '{new_destination}'.")
            # Pass the raw folder path to the downloader.
            downloader.download_folder(bucket_name, object_path, new_destination, parallel_count=cpu_count)

        elif is_remote_path(destination):
            # Upload operation
            local_source = source
            remote_destination = destination

            oci_manager = OCIManager(config_profile=config_profile)
            uploader = Uploader(oci_manager=oci_manager)

            try:
                bucket_name, object_path = parse_remote_path(remote_destination)
            except ValueError as e:
                logger.error(f"Error parsing remote path: {e}")
                return

            # --- NEW LOGIC: Check for wildcard in source ---
            if '*' in source:
                # Expand the wildcard into a list of files.
                import glob
                files = glob.glob(source)
                if not files:
                    logger.error(f"No files matched wildcard: {source}")
                    sys.exit(1)
                for file_path in files:
                    adjusted_path = adjust_remote_object_path(file_path, object_path)
                    logger.info(f"Initiating single file upload for '{file_path}' as '{adjusted_path}'.")
                    uploader.upload_single_file(file_path, bucket_name, adjusted_path)
            elif os.path.isfile(local_source):
                object_path = adjust_remote_object_path(local_source, object_path)
                logger.info(f"Adjusted object path for file upload: '{object_path}'")
                logger.info("Initiating single file upload.")
                uploader.upload_single_file(local_source, bucket_name, object_path)
            elif os.path.isdir(local_source):
                if not object_path:
                    object_path = os.path.basename(os.path.normpath(local_source))
                elif object_path.endswith('/'):
                    object_path = object_path.rstrip('/') + '/' + os.path.basename(os.path.normpath(local_source))
                logger.info(f"Initiating bulk upload of folder '{local_source}' with {cpu_count} parallel threads.")
                uploader.upload_folder(local_source, bucket_name, object_path, parallel_count=cpu_count)
            else:
                logger.error(f"Source '{local_source}' is neither a file nor a directory.")
                return
        else:
            logger.error("Invalid command. Either the source or destination must be a remote path (oc://).")
            parser.print_help()
            sys.exit(1)

    except Exception as ex:
        logger.error(f"Operation failed: {ex}")
        sys.exit(1)

if __name__ == "__main__":
    main()