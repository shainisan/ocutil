#!/usr/bin/env python3
import argparse
import os
import oci

def main():
    parser = argparse.ArgumentParser(
        description="ocutil: Oracle Cloud Object Storage CLI (similar to gsutil)."
    )
    parser.add_argument("local_file", help="Path to the local file to upload.")
    parser.add_argument("bucket_with_path", help="Bucket name followed by path prefix, e.g., bucket-name/path/to/folder/")
    parser.add_argument(
        "--config-profile",
        default="DEFAULT",
        help="OCI config profile (default: DEFAULT)."
    )
    args = parser.parse_args()

    # Check if the local file exists
    if not os.path.isfile(args.local_file):
        print(f"Error: local file '{args.local_file}' not found.")
        return

    # Parse the bucket name and path prefix from the second argument
    bucket_with_path = args.bucket_with_path.strip('/')
    if '/' in bucket_with_path:
        bucket_name, path_prefix = bucket_with_path.split('/', 1)
        path_prefix = path_prefix.rstrip('/') + '/'  # Ensure it ends with '/'
    else:
        bucket_name = bucket_with_path
        path_prefix = ''

    # Construct the object name in the bucket
    local_filename = os.path.basename(args.local_file)
    object_name = f"{path_prefix}{local_filename}" if path_prefix else local_filename

    # Load OCI config
    try:
        config = oci.config.from_file("~/.oci/config", args.config_profile)
    except Exception as e:
        print(f"Error loading OCI config: {e}")
        return

    # Create an Object Storage client
    object_storage = oci.object_storage.ObjectStorageClient(config)

    # Get the namespace for your tenancy
    try:
        namespace = object_storage.get_namespace().data
    except Exception as e:
        print(f"Error retrieving namespace: {e}")
        return

    # Perform the upload
    print(f"Uploading '{args.local_file}' to Bucket: '{bucket_name}', Path: '{object_name}'...")
    try:
        with open(args.local_file, 'rb') as f:
            response = object_storage.put_object(namespace, bucket_name, object_name, f)
    except Exception as e:
        print(f"Error uploading file: {e}")
        return

    # Check if upload was successful
    if response.status == 200:
        print("Upload successful!")
    else:
        print(f"Upload failed with status: {response.status}")

if __name__ == "__main__":
    main()
