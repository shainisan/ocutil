#!/usr/bin/env python3

import argparse
import os
import sys
import multiprocessing
from urllib.parse import urlparse
import oci  # Ensure oci is imported for exception handling

from ocutil.utils.oci_manager import OCIManager
from ocutil.utils.uploader import Uploader
from ocutil.utils.downloader import Downloader

import logging

def is_remote_path(path):
    return path.startswith("oc://")

def parse_remote_path(remote_path):
    """
    Parses a remote path of the form oc://bucket-name/path/to/object
    Returns:
        bucket_name (str): Name of the OCI bucket
        object_path (str): Path to the object inside the bucket
    """
    parsed = urlparse(remote_path)
    if parsed.scheme != "oc":
        raise ValueError("Remote path must start with 'oc://'")
    
    bucket_name = parsed.netloc
    object_path = parsed.path.lstrip('/')
    
    return bucket_name, object_path

def main():
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    logger = logging.getLogger('ocutil')

    parser = argparse.ArgumentParser(
        description="ocutil: Oracle Cloud Object Storage CLI (similar to gsutil)."
    )
    parser.add_argument("source", help="Source path. Can be a local file or a remote path (oc://bucket/path).")
    parser.add_argument("destination", help="Destination path. Can be a remote path (oc://bucket/path) or a local directory (.)")
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

    if is_remote_path(source):
        # Download operation
        remote_path = source
        local_destination = destination

        if not os.path.isdir(local_destination):
            logger.error(f"Destination '{local_destination}' is not a valid directory.")
            return

        oci_manager = OCIManager(config_profile=config_profile)
        downloader = Downloader(oci_manager=oci_manager)

        bucket_name, object_path = parse_remote_path(remote_path)

        if remote_path.endswith('/'):
            # Treat as folder
            folder_name = os.path.basename(os.path.normpath(object_path))
            new_destination = os.path.join(local_destination, folder_name)
            logger.info(f"Using bulk download with {cpu_count} parallel threads into '{new_destination}'.")
            downloader.download_folder(remote_path, new_destination, parallel_count=cpu_count)
        else:
            # Attempt to treat as single file; if it fails, treat as folder
            try:
                # Attempt to retrieve the exact object to determine if it's a single file
                oci_manager.object_storage.get_object(oci_manager.namespace, bucket_name, object_path)
                # If no exception, it's a single file
                logger.info(f"Using single file download with 1 thread.")
                local_path = os.path.join(local_destination, os.path.basename(object_path))
                downloader.download_single_file(bucket_name, object_path, local_path)
                logger.info("Download operation completed.")
            except oci.exceptions.ServiceError as e:
                if e.status == 404:
                    # Object doesn't exist; treat as bulk download
                    logger.info(f"Object not found. Treating as bulk download with {cpu_count} parallel threads.")
                    # Extract folder name
                    folder_name = os.path.basename(os.path.normpath(object_path))
                    new_destination = os.path.join(local_destination, folder_name)
                    # Ensure the prefix ends with '/'
                    if not object_path.endswith('/'):
                        object_prefix = f"{object_path}/"
                    else:
                        object_prefix = object_path
                    remote_prefix = f"oc://{bucket_name}/{object_prefix}"
                    downloader.download_folder(remote_prefix, new_destination, parallel_count=cpu_count)
                else:
                    logger.error(f"Error retrieving object: {e}")
                    return
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                return

    elif is_remote_path(destination):
        # Upload operation
        local_source = source
        remote_destination = destination

        oci_manager = OCIManager(config_profile=config_profile)
        uploader = Uploader(oci_manager=oci_manager)

        if os.path.isfile(local_source):
            logger.info(f"Using single file upload with 1 thread.")
            try:
                bucket_name, object_path = parse_remote_path(remote_destination)
            except ValueError as e:
                logger.error(f"Error parsing remote path: {e}")
                return

            # **Add the following block to handle directory destinations**
            if object_path.endswith('/'):
                # Append the filename to the object path
                object_path = os.path.join(object_path, os.path.basename(local_source)).replace('\\', '/')
                logger.info(f"Appending filename to object path: '{object_path}'")

            uploader.upload_single_file(local_source, bucket_name, object_path)
        elif os.path.isdir(local_source):
            logger.info(f"Using bulk upload with {cpu_count} parallel threads.")
            try:
                bucket_name, object_path = parse_remote_path(remote_destination)
            except ValueError as e:
                logger.error(f"Error parsing remote path: {e}")
                return
            uploader.upload_folder(local_source, bucket_name, object_path, parallel_count=cpu_count)
        else:
            logger.error(f"Error: Source '{local_source}' is neither a file nor a directory.")
            return
    else:
        logger.error("Error: Invalid command. Ensure that either the source or destination is a remote path starting with 'oc://'.")
        logger.info("Usage:")
        logger.info("  To upload: ocutil <local_file_or_directory> oc://bucket/path")
        logger.info("  To download: ocutil oc://bucket/path <destination_directory>")
        sys.exit(1)

if __name__ == "__main__":
    main()