#!/usr/bin/env python3
import argparse
import os
import oci

def main():
    parser = argparse.ArgumentParser(
        description="ocutil: Oracle Cloud Object Storage CLI (similar to gsutil)."
    )
    parser.add_argument("local_file", help="Path to the local file to upload.")
    parser.add_argument("bucket_name", help="Name of the OCI bucket.")
    parser.add_argument("object_path_prefix", help="Folder or path prefix in the bucket.")
    parser.add_argument(
        "--config-profile",
        default="DEFAULT",
        help="OCI config profile (default: DEFAULT)."
    )
    args = parser.parse_args()

    if not os.path.isfile(args.local_file):
        print(f"Error: local file '{args.local_file}' not found.")
        return

    # Construct the object name in the bucket
    local_filename = os.path.basename(args.local_file)
    if not args.object_path_prefix.endswith("/"):
        object_name = f"{args.object_path_prefix}/{local_filename}"
    else:
        object_name = f"{args.object_path_prefix}{local_filename}"

    config = oci.config.from_file("~/.oci/config", args.config_profile)
    object_storage = oci.object_storage.ObjectStorageClient(config)
    namespace = object_storage.get_namespace().data

    print(f"Uploading '{args.local_file}' to {args.bucket_name}/{object_name}...")
    with open(args.local_file, 'rb') as f:
        response = object_storage.put_object(namespace, args.bucket_name, object_name, f)

    if response.status == 200:
        print("Upload successful!")
    else:
        print(f"Upload failed with status {response.status}")

if __name__ == "__main__":
    main()
