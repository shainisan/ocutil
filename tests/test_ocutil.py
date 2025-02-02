# tests/test_ocutil.py

# Run tests with: python -m unittest discover -v tests

import os
import sys
import tempfile
import shutil
import unittest
import logging
from unittest.mock import patch

from ocutil.utils.oci_manager import OCIManager
from ocutil.utils.uploader import Uploader
from ocutil.utils.downloader import Downloader

# Import the helper function for adjusting the remote object path.
# (Ensure that you add adjust_remote_object_path in ocutil/main.py.)
from ocutil.main import adjust_remote_object_path, main

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
        # Test that if we provide an empty destination the uploader appends the basename of the source file.
        adjusted_object_path = adjust_remote_object_path(self.single_file_path, "")
        self.uploader.upload_single_file(self.single_file_path, self.BUCKET_NAME, adjusted_object_path)
        self.uploaded_objects.append(adjusted_object_path)

        # Now download using the adjusted object path.
        download_destination = os.path.join(self.download_dir, "downloaded_single_file_no_path.txt")
        self.downloader.download_single_file(self.BUCKET_NAME, adjusted_object_path, download_destination)
        self.assertTrue(os.path.exists(download_destination))
        with open(download_destination, "r") as f:
            content = f.read()
        self.assertEqual(content, "This is a single file")

    def test_upload_single_file_destination_folder_without_trailing_slash(self):
        # Test that if we provide a remote destination that looks like a folder (no trailing slash)
        # and is not equal to the source file's basename, the uploader appends the basename.
        remote_object_path = "folder"  # provided as folder
        expected_object_path = "folder/" + os.path.basename(self.single_file_path)
        adjusted_object_path = adjust_remote_object_path(self.single_file_path, remote_object_path)
        self.assertEqual(adjusted_object_path, expected_object_path,
                         "Expected adjusted object path to be folder/<basename>")

        # Now use the adjusted object path for upload.
        self.uploader.upload_single_file(self.single_file_path, self.BUCKET_NAME, adjusted_object_path)
        self.uploaded_objects.append(adjusted_object_path)

        # Download and verify.
        download_destination = os.path.join(self.download_dir, "downloaded_single_file_folder.txt")
        self.downloader.download_single_file(self.BUCKET_NAME, adjusted_object_path, download_destination)
        self.assertTrue(os.path.exists(download_destination))
        with open(download_destination, "r") as f:
            content = f.read()
        self.assertEqual(content, "This is a single file")

    def test_upload_single_file_destination_folder_with_trailing_slash(self):
        # Test that if we provide a remote destination with a trailing slash,
        # the uploader appends the basename.
        remote_object_path = "folder/"  # provided as folder with trailing slash
        expected_object_path = "folder/" + os.path.basename(self.single_file_path)
        adjusted_object_path = adjust_remote_object_path(self.single_file_path, remote_object_path)
        self.assertEqual(adjusted_object_path, expected_object_path,
                         "Expected adjusted object path to be folder/<basename>")

        # Now use the adjusted object path for upload.
        self.uploader.upload_single_file(self.single_file_path, self.BUCKET_NAME, adjusted_object_path)
        self.uploaded_objects.append(adjusted_object_path)

        # Download and verify.
        download_destination = os.path.join(self.download_dir, "downloaded_single_file_folder_trailing.txt")
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

    # Additional tests for adjust_remote_object_path behavior.
    def test_adjust_remote_object_path_empty(self):
        # If no object_path is provided, should return basename.
        self.assertEqual(adjust_remote_object_path("dummy.txt", ""), "dummy.txt")

    def test_adjust_remote_object_path_explicit_filename(self):
        # If an explicit filename is provided, it should remain unchanged.
        self.assertEqual(adjust_remote_object_path("dummy.txt", "custom.txt"), "custom.txt")

    def test_adjust_remote_object_path_same_as_basename(self):
        # If the provided object_path is the same as the basename, it should not be modified.
        basename = os.path.basename("dummy.txt")
        self.assertEqual(adjust_remote_object_path("dummy.txt", basename), basename)

    # --- New tests to simulate main() behavior for directory uploads ---

    def test_main_upload_folder_without_wildcard(self):
        """
        Simulate calling:
            ocutil folder/ oc://test-bucket/
        Expect that since the source (folder/) does not include a wildcard,
        the main() logic will wrap the folder by appending the folder’s basename.
        """
        test_args = ["ocutil", self.folder_path, f"oc://{self.BUCKET_NAME}/"]
        with patch.object(sys, 'argv', test_args):
            with patch('ocutil.utils.uploader.Uploader.upload_folder') as mock_upload_folder:
                main()
                # Since uploader.upload_folder is called positionally:
                # arguments: (local_source, bucket_name, object_prefix, parallel_count=...)
                args = mock_upload_folder.call_args.args
                expected_prefix = os.path.basename(os.path.normpath(self.folder_path))
                self.assertEqual(args[2], expected_prefix,
                                "Expected the folder basename to be appended as the remote prefix.")

    def test_main_upload_folder_with_wildcard(self):
        """
        Simulate calling:
            ocutil folder/* oc://test-bucket/
        Since the source includes a wildcard ('*'), main() should expand it and
        upload each file individually via upload_single_file.
        """
        test_args = ["ocutil", os.path.join(self.folder_path, "*"), f"oc://{self.BUCKET_NAME}/"]
        with patch.object(sys, 'argv', test_args):
            with patch('ocutil.utils.uploader.Uploader.upload_single_file') as mock_upload_single:
                main()
                # In self.folder_path we created 2 files.
                self.assertEqual(mock_upload_single.call_count, 2,
                                "Expected two single file uploads when using wildcard.")


    def test_download_folder_pagination(self):
        """
        Simulate a paginated response when listing objects from the bucket.
        The first call returns two objects and a next page token,
        and the second call returns one additional object.
        Verify that all three files are downloaded.
        """
        # Create dummy objects with a 'name' attribute.
        class DummyObject:
            def __init__(self, name):
                self.name = name

        # Create a dummy list response structure similar to OCI SDK's response.
        class DummyListResponse:
            def __init__(self, objects, next_page):
                self.data = type("Data", (), {"objects": objects})
                self.next_page = next_page

        # Create dummy objects.
        dummy_obj1 = DummyObject("prefix/file1.txt")
        dummy_obj2 = DummyObject("prefix/file2.txt")
        dummy_obj3 = DummyObject("prefix/file3.txt")

        # Create two dummy responses: first with a next_page token, then no token.
        first_response = DummyListResponse([dummy_obj1, dummy_obj2], "token")
        second_response = DummyListResponse([dummy_obj3], None)

        # Patch the list_objects method to return our dummy responses sequentially.
        with patch.object(self.oci_manager.object_storage, 'list_objects', side_effect=[first_response, second_response]) as mock_list:
            # Patch download_single_file so that we don't actually try to download data.
            with patch.object(self.downloader, 'download_single_file') as mock_download:
                # Call download_folder using a prefix that matches our dummy objects.
                self.downloader.download_folder("dummy-bucket", "prefix/", self.download_dir, parallel_count=2)
                
                # Check that list_objects was called twice.
                self.assertEqual(mock_list.call_count, 2, "Expected two calls to list_objects due to pagination.")
                
                # We expect download_single_file to be called three times (one per dummy object).
                self.assertEqual(mock_download.call_count, 3, "Expected three download calls for three objects.")

                # Optionally, verify that each dummy object's full name was passed to download_single_file.
                downloaded_names = [call_args[0][1] for call_args in mock_download.call_args_list]
                expected_names = ["prefix/file1.txt", "prefix/file2.txt", "prefix/file3.txt"]
                self.assertCountEqual(downloaded_names, expected_names,
                                      "Downloaded object names do not match expected names.")

if __name__ == "__main__":
    unittest.main()