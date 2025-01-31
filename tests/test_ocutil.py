# tests/test_ocutil.py

# run: python -m unittest discover tests

import os
import tempfile
import shutil
import unittest
import logging

from ocutil.utils.oci_manager import OCIManager
from ocutil.utils.uploader import Uploader
from ocutil.utils.downloader import Downloader

logger = logging.getLogger(__name__)

class TestOCUtil(unittest.TestCase):
    BUCKET_NAME = "test-bucket"  # Make sure this bucket exists in your OCI tenancy

    def setUp(self):
        # Initialize the OCI manager, uploader, and downloader.
        self.oci_manager = OCIManager()
        self.uploader = Uploader(self.oci_manager)
        self.downloader = Downloader(self.oci_manager)

        # Create a list to keep track of uploaded remote objects for cleanup.
        self.uploaded_objects = []

        # Create a temporary directory for test files.
        self.test_dir = tempfile.mkdtemp()

        # Create a folder with two files.
        self.folder_path = os.path.join(self.test_dir, "test_folder")
        os.makedirs(self.folder_path, exist_ok=True)
        with open(os.path.join(self.folder_path, "file1.txt"), "w") as f:
            f.write("This is file 1")
        with open(os.path.join(self.folder_path, "file2.txt"), "w") as f:
            f.write("This is file 2")

        # Create a single file.
        self.single_file_path = os.path.join(self.test_dir, "single_file.txt")
        with open(self.single_file_path, "w") as f:
            f.write("This is a single file")

        # Create a destination directory for downloads.
        self.download_dir = os.path.join(self.test_dir, "download")
        os.makedirs(self.download_dir, exist_ok=True)

    def tearDown(self):
        # Delete all uploaded objects from the bucket.
        for remote_object in self.uploaded_objects:
            try:
                self.oci_manager.object_storage.delete_object(
                    self.oci_manager.namespace, self.BUCKET_NAME, remote_object
                )
                logger.info("Deleted remote object: %s", remote_object)
            except Exception as e:
                logger.warning("Failed to delete remote object %s: %s", remote_object, e)
        # Cleanup local temporary directory.
        shutil.rmtree(self.test_dir)

    def test_upload_and_download_folder_with_trailing_slash(self):
        # Use a remote prefix that ends with a slash.
        object_prefix = "test_folder/"  # trailing slash provided

        # Upload the folder.
        self.uploader.upload_folder(self.folder_path, self.BUCKET_NAME, object_prefix, parallel_count=2)
        # Record expected uploaded objects.
        self.uploaded_objects.extend([
            f"{object_prefix}file1.txt",
            f"{object_prefix}file2.txt"
        ])

        # Download the folder.
        download_destination = os.path.join(self.download_dir, "test_folder")
        self.downloader.download_folder(self.BUCKET_NAME, object_prefix, download_destination, parallel_count=2)

        # Verify that the files have been downloaded correctly.
        file1_path = os.path.join(download_destination, "file1.txt")
        file2_path = os.path.join(download_destination, "file2.txt")
        self.assertTrue(os.path.exists(file1_path))
        self.assertTrue(os.path.exists(file2_path))
        with open(file1_path, "r") as f:
            content1 = f.read()
        with open(file2_path, "r") as f:
            content2 = f.read()
        self.assertEqual(content1, "This is file 1")
        self.assertEqual(content2, "This is file 2")

    def test_upload_and_download_folder_without_trailing_slash(self):
        # Use a remote prefix that does NOT end with a slash.
        object_prefix = "test_folder_no_slash"  # no trailing slash provided

        # Upload the folder.
        self.uploader.upload_folder(self.folder_path, self.BUCKET_NAME, object_prefix, parallel_count=2)
        # Even if no trailing slash was provided, uploader should use:
        self.uploaded_objects.extend([
            f"{object_prefix}/file1.txt",
            f"{object_prefix}/file2.txt"
        ])

        # Download the folder.
        download_destination = os.path.join(self.download_dir, "test_folder_no_slash")
        self.downloader.download_folder(self.BUCKET_NAME, object_prefix, download_destination, parallel_count=2)

        # Verify that the files have been downloaded correctly.
        file1_path = os.path.join(download_destination, "file1.txt")
        file2_path = os.path.join(download_destination, "file2.txt")
        self.assertTrue(os.path.exists(file1_path))
        self.assertTrue(os.path.exists(file2_path))
        with open(file1_path, "r") as f:
            content1 = f.read()
        with open(file2_path, "r") as f:
            content2 = f.read()
        self.assertEqual(content1, "This is file 1")
        self.assertEqual(content2, "This is file 2")

    def test_upload_and_download_single_file_with_explicit_object_name(self):
        # Upload a single file using an explicit remote object name.
        remote_object_path = "explicit_single_file.txt"
        self.uploader.upload_single_file(self.single_file_path, self.BUCKET_NAME, remote_object_path)
        self.uploaded_objects.append(remote_object_path)

        # Download the single file.
        download_destination = os.path.join(self.download_dir, "downloaded_explicit_single_file.txt")
        self.downloader.download_single_file(self.BUCKET_NAME, remote_object_path, download_destination)
        self.assertTrue(os.path.exists(download_destination))
        with open(download_destination, "r") as f:
            content = f.read()
        self.assertEqual(content, "This is a single file")

    def test_upload_and_download_single_file_without_object_path(self):
        # Test that if we provide a destination with no object path (or with a trailing slash)
        # the uploader appends the basename of the source file.
        # Simulate what main.py does.
        adjusted_object_path = os.path.basename(self.single_file_path)  # e.g., "single_file.txt"
        self.uploader.upload_single_file(self.single_file_path, self.BUCKET_NAME, adjusted_object_path)
        self.uploaded_objects.append(adjusted_object_path)

        # Now download using the adjusted object path.
        download_destination = os.path.join(self.download_dir, "downloaded_single_file_no_path.txt")
        self.downloader.download_single_file(self.BUCKET_NAME, adjusted_object_path, download_destination)
        self.assertTrue(os.path.exists(download_destination))
        with open(download_destination, "r") as f:
            content = f.read()
        self.assertEqual(content, "This is a single file")

    def test_upload_nonexistent_file(self):
        # Attempt to upload a file that does not exist and capture the logged error.
        nonexistent_file = os.path.join(self.test_dir, "does_not_exist.txt")
        remote_object_path = "does_not_exist.txt"
        with self.assertLogs("ocutil.uploader", level="ERROR") as log:
            self.uploader.upload_single_file(nonexistent_file, self.BUCKET_NAME, remote_object_path)
        self.assertTrue(any("does not exist" in message for message in log.output))

    def test_download_nonexistent_object(self):
        # Attempt to download a non-existent object and capture the logged error.
        remote_object_path = "nonexistent_object.txt"
        download_destination = os.path.join(self.download_dir, "nonexistent_object.txt")
        with self.assertLogs("ocutil.downloader", level="ERROR") as log:
            self.downloader.download_single_file(self.BUCKET_NAME, remote_object_path, download_destination)
        self.assertTrue(any("Error downloading" in message for message in log.output))

if __name__ == "__main__":
    unittest.main()
