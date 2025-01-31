#!/usr/bin/env python3
import argparse
import os
import oci
import sys
from urllib.parse import urlparse

def is_remote_path(path):
    return path.startswith("oc://")

def parse_remote_path(remote_path):
    """
    Parses a remote path of the form oc://bucket-name/path/to/object/
    Returns:
        bucket_name (str): Name of the OCI bucket
        object_prefix (str): Path prefix inside the bucket
    """
    parsed = urlparse(remote_path)
    if parsed.scheme != "oc":
        raise ValueError("Remote path must start with 'oc://'")
    
    # Remove leading '/' from path
    path = parsed.path.lstrip('/')
    if '/' in path:
        bucket_name, object_prefix = path.split('/', 1)
        object_prefix = object_prefix.rstrip('/') + '/'  # Ensure it ends with '/'
    else:
        bucket_name = path
        object_prefix = ''
    
    return bucket_name, object_prefix

def upload_file(local_file, bucket_name, object_prefix, config_profile):
    """
    Uploads a single file to OCI Object Storage
    """
    if not os.path.isfile(local_file):
        print(f"Error: Local file '{local_file}' does not exist.")
        return

    local_filename = os.path.basename(local_file)
    object_name = f"{object_prefix}{local_filename}" if object_prefix else local_filename

    try:
        config = oci.config.from_file("~/.oci/config", config_profile)
    except Exception as e:
        print(f"Error loading OCI config: {e}")
        return

    object_storage = oci.object_storage.ObjectStorageClient(config)

    try:
        namespace = object_storage.get_namespace().data
    except Exception as e:
        print(f"Error retrieving namespace: {e}")
        return

    print(f"Uploading '{local_file}' to Bucket: '{bucket_name}', Path: '{object_name}'...")
    try:
        with open(local_file, 'rb') as f:
            response = object_storage.put_object(namespace, bucket_name, object_name, f)
    except Exception as e:
        print(f"Error uploading file: {e}")
        return

    if response.status == 200:
        print("Upload successful!")
    else:
        print(f"Upload failed with status: {response.status}")

def download_folder(remote_path, destination, config_profile):
    """
    Downloads all objects under a given prefix from OCI Object Storage to a local directory
    """
    try:
        bucket_name, object_prefix = parse_remote_path(remote_path)
    except ValueError as e:
        print(f"Error parsing remote path: {e}")
        return

    try:
        config = oci.config.from_file("~/.oci/config", config_profile)
    except Exception as e:
        print(f"Error loading OCI config: {e}")
        return

    object_storage = oci.object_storage.ObjectStorageClient(config)

    try:
        namespace = object_storage.get_namespace().data
    except Exception as e:
        print(f"Error retrieving namespace: {e}")
        return

    # List all objects with the given prefix
    print(f"Listing objects in Bucket: '{bucket_name}', Prefix: '{object_prefix}'...")
    try:
        objects = []
        list_objects_response = object_storage.list_objects(namespace, bucket_name, prefix=object_prefix)
        objects.extend(list_objects_response.data.objects)
        while list_objects_response.has_next_page:
            list_objects_response = object_storage.list_objects(namespace, bucket_name, prefix=object_prefix, page=list_objects_response.next_page)
            objects.extend(list_objects_response.data.objects)
    except Exception as e:
        print(f"Error listing objects: {e}")
        return

    if not objects:
        print("No objects found to download.")
        return

    # Download each object
    for obj in objects:
        object_name = obj.name
        relative_path = object_name[len(object_prefix):] if object_prefix else object_name
        local_path = os.path.join(destination, relative_path)

        # Ensure the local directory exists
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        print(f"Downloading '{object_name}' to '{local_path}'...")
        try:
            get_object_response = object_storage.get_object(namespace, bucket_name, object_name)
            with open(local_path, 'wb') as f:
                for chunk in get_object_response.data.raw.stream(1024 * 1024, decode_content=False):
                    if chunk:
                        f.write(chunk)
        except Exception as e:
            print(f"Error downloading '{object_name}': {e}")
            continue

    print("Download completed successfully!")

def main():
    parser = argparse.ArgumentParser(
        description="ocutil: Oracle Cloud Object Storage CLI (similar to gsutil)."
    )
    parser.add_argument("source", help="Source path. Can be a local file or a remote path (oc://bucket/path/).")
    parser.add_argument("destination", help="Destination path. Can be a remote path (oc://bucket/path/) or a local directory (.)")
    parser.add_argument(
        "--config-profile",
        default="DEFAULT",
        help="OCI config profile (default: DEFAULT)."
    )
    args = parser.parse_args()

    source = args.source
    destination = args.destination
    config_profile = args.config_profile

    if is_remote_path(source):
        # Download operation
        remote_path = source
        local_destination = destination

        if not os.path.isdir(local_destination):
            print(f"Error: Destination '{local_destination}' is not a valid directory.")
            return

        download_folder(remote_path, local_destination, config_profile)
    elif is_remote_path(destination):
        # Upload operation
        local_file = source
        remote_path = destination

        try:
            bucket_name, object_prefix = parse_remote_path(remote_path)
        except ValueError as e:
            print(f"Error parsing remote path: {e}")
            return

        upload_file(local_file, bucket_name, object_prefix, config_profile)
    else:
        print("Error: Invalid command. Ensure that either the source or destination is a remote path starting with 'oc://'.")
        print("Usage:")
        print("  To upload: ocutil <local_file> oc://bucket/path/")
        print("  To download: ocutil oc://bucket/path/ <destination_directory>")
        sys.exit(1)

if __name__ == "__main__":
    main()
