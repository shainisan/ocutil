# tests/test_ocutil.py

# run with:
# python -m unittest discover -v tests

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

# Import the helper function for adjusting the remote object path and the main entry.
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

    def test_download_folder_pagination(self):
        """
        Simulate a folder download with pagination by patching the list_objects method.
        We simulate three pages:
        - Page 1: returns two objects.
        - Page 2: returns two objects.
        - Page 3: returns an empty list.
        We then verify that _download_file_no_progress is called exactly 4 times.
        """
        # Create dummy objects to simulate listing results.
        DummyObj = lambda name: type("DummyObj", (), {"name": name})
        dummy_obj1 = DummyObj("prefix/obj1.txt")
        dummy_obj2 = DummyObj("prefix/obj2.txt")
        dummy_obj3 = DummyObj("prefix/obj3.txt")
        dummy_obj4 = DummyObj("prefix/obj4.txt")

        # Build three dummy responses.
        DummyResponse = lambda objects: type("DummyResponse", (), {
            "data": type("DummyData", (), {"objects": objects}),
            "headers": {}
        })
        responses = [
            DummyResponse([dummy_obj1, dummy_obj2]),
            DummyResponse([dummy_obj3, dummy_obj4]),
            DummyResponse([])  # Third page: no objects
        ]

        call_count = 0
        def side_effect(*args, **kwargs):
            nonlocal call_count
            response = responses[call_count]
            call_count += 1
            return response

        # Patch the list_objects method on the oci_manager's object_storage
        with patch.object(self.oci_manager.object_storage, 'list_objects', side_effect=side_effect) as mock_list:
            with patch.object(self.downloader, '_download_file_no_progress') as mock_download:
                # Call download_folder with a small limit to force pagination.
                self.downloader.download_folder("dummy-bucket", "prefix", self.download_dir, parallel_count=2, limit=2)
                # We expect 4 download calls.
                self.assertEqual(mock_download.call_count, 4)



    def test_upload_and_download_folder_without_trailing_slash(self):
        object_prefix = "test_folder_no_slash"  # no trailing slash provided

        self.uploader.upload_folder(self.folder_path, self.BUCKET_NAME, object_prefix, parallel_count=2)
        self.uploaded_objects.extend([
            f"{object_prefix}/file1.txt",
            f"{object_prefix}/file2.txt"
        ])

        download_destination = os.path.join(self.download_dir, "test_folder_no_slash")
        self.downloader.download_folder(self.BUCKET_NAME, object_prefix, download_destination, parallel_count=2)

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
        remote_object_path = "explicit_single_file.txt"
        self.uploader.upload_single_file(self.single_file_path, self.BUCKET_NAME, remote_object_path)
        self.uploaded_objects.append(remote_object_path)

        download_destination = os.path.join(self.download_dir, "downloaded_explicit_single_file.txt")
        self.downloader.download_single_file(self.BUCKET_NAME, remote_object_path, download_destination)
        self.assertTrue(os.path.exists(download_destination))
        with open(download_destination, "r") as f:
            content = f.read()
        self.assertEqual(content, "This is a single file")

    def test_upload_and_download_single_file_without_object_path(self):
        adjusted_object_path = adjust_remote_object_path(self.single_file_path, "")
        self.uploader.upload_single_file(self.single_file_path, self.BUCKET_NAME, adjusted_object_path)
        self.uploaded_objects.append(adjusted_object_path)

        download_destination = os.path.join(self.download_dir, "downloaded_single_file_no_path.txt")
        self.downloader.download_single_file(self.BUCKET_NAME, adjusted_object_path, download_destination)
        self.assertTrue(os.path.exists(download_destination))
        with open(download_destination, "r") as f:
            content = f.read()
        self.assertEqual(content, "This is a single file")

    def test_upload_single_file_destination_folder_without_trailing_slash(self):
        remote_object_path = "folder"
        expected_object_path = "folder/" + os.path.basename(self.single_file_path)
        adjusted_object_path = adjust_remote_object_path(self.single_file_path, remote_object_path)
        self.assertEqual(adjusted_object_path, expected_object_path)

        self.uploader.upload_single_file(self.single_file_path, self.BUCKET_NAME, adjusted_object_path)
        self.uploaded_objects.append(adjusted_object_path)

        download_destination = os.path.join(self.download_dir, "downloaded_single_file_folder.txt")
        self.downloader.download_single_file(self.BUCKET_NAME, adjusted_object_path, download_destination)
        self.assertTrue(os.path.exists(download_destination))
        with open(download_destination, "r") as f:
            content = f.read()
        self.assertEqual(content, "This is a single file")

    def test_upload_single_file_destination_folder_with_trailing_slash(self):
        remote_object_path = "folder/"
        expected_object_path = "folder/" + os.path.basename(self.single_file_path)
        adjusted_object_path = adjust_remote_object_path(self.single_file_path, remote_object_path)
        self.assertEqual(adjusted_object_path, expected_object_path)

        self.uploader.upload_single_file(self.single_file_path, self.BUCKET_NAME, adjusted_object_path)
        self.uploaded_objects.append(adjusted_object_path)

        download_destination = os.path.join(self.download_dir, "downloaded_single_file_folder_trailing.txt")
        self.downloader.download_single_file(self.BUCKET_NAME, adjusted_object_path, download_destination)
        self.assertTrue(os.path.exists(download_destination))
        with open(download_destination, "r") as f:
            content = f.read()
        self.assertEqual(content, "This is a single file")

    def test_upload_nonexistent_file(self):
        nonexistent_file = os.path.join(self.test_dir, "does_not_exist.txt")
        remote_object_path = "does_not_exist.txt"
        with self.assertLogs("ocutil.uploader", level="ERROR") as log:
            self.uploader.upload_single_file(nonexistent_file, self.BUCKET_NAME, remote_object_path)
        self.assertTrue(any("does not exist" in message for message in log.output))

    def test_download_nonexistent_object(self):
        remote_object_path = "nonexistent_object.txt"
        download_destination = os.path.join(self.download_dir, "nonexistent_object.txt")
        with self.assertLogs("ocutil.downloader", level="ERROR") as log:
            self.downloader.download_single_file(self.BUCKET_NAME, remote_object_path, download_destination)
        self.assertTrue(any("Error downloading" in message for message in log.output))

    def test_adjust_remote_object_path_empty(self):
        self.assertEqual(adjust_remote_object_path("dummy.txt", ""), "dummy.txt")

    def test_adjust_remote_object_path_explicit_filename(self):
        self.assertEqual(adjust_remote_object_path("dummy.txt", "custom.txt"), "custom.txt")

    def test_adjust_remote_object_path_same_as_basename(self):
        basename = os.path.basename("dummy.txt")
        self.assertEqual(adjust_remote_object_path("dummy.txt", basename), basename)

    def test_main_upload_folder_without_wildcard(self):
        test_args = ["ocutil", self.folder_path, f"oc://{self.BUCKET_NAME}/"]
        with patch.object(sys, 'argv', test_args):
            with patch('ocutil.utils.uploader.Uploader.upload_folder') as mock_upload_folder:
                main()
                args = mock_upload_folder.call_args.args
                expected_prefix = os.path.basename(os.path.normpath(self.folder_path))
                self.assertEqual(args[2], expected_prefix)

    def test_main_upload_folder_with_wildcard(self):
        test_args = ["ocutil", os.path.join(self.folder_path, "*"), f"oc://{self.BUCKET_NAME}/"]
        with patch.object(sys, 'argv', test_args):
            with patch('ocutil.utils.uploader.Uploader.upload_single_file') as mock_upload_single:
                main()
                self.assertEqual(mock_upload_single.call_count, 2)

    def test_main_download_folder_prefix_trailing_slash(self):
        test_args = ["ocutil", "oc://dummy-bucket/folder/", self.download_dir]
        with patch.object(sys, 'argv', test_args):
            with patch('ocutil.utils.downloader.Downloader.download_folder') as mock_download_folder:
                main()
                args = mock_download_folder.call_args.args
                object_prefix = args[1]
                self.assertTrue(object_prefix.endswith('/'))

    def test_dry_run_upload_folder(self):
        """Test that dry-run upload logs intended actions without performing any upload."""
        dry_run_uploader = Uploader(self.oci_manager, dry_run=True)
        with self.assertLogs("ocutil.uploader", level="INFO") as log:
            dry_run_uploader.upload_folder(self.folder_path, self.BUCKET_NAME, "dry_run_folder/", parallel_count=2)
            # Assert that the log contains DRY-RUN messages.
            self.assertTrue(any("DRY-RUN:" in message for message in log.output))
    
    def test_dry_run_download_folder(self):
        """Test that dry-run download logs intended actions without performing any download."""
        dry_run_downloader = Downloader(self.oci_manager, dry_run=True)
        with self.assertLogs("ocutil.downloader", level="INFO") as log:
            dry_run_downloader.download_folder(self.BUCKET_NAME, "dry_run_folder/", self.download_dir, parallel_count=2)
            # Assert that the log contains DRY-RUN messages.
            self.assertTrue(any("DRY-RUN:" in message for message in log.output))
    
    def test_upload_retry_error(self):
        """Simulate an error during single file upload to test retry logic."""
        # Force put_object to always raise an exception.
        with patch.object(self.oci_manager.object_storage, 'put_object', side_effect=Exception("Simulated error")) as mock_put:
            with self.assertLogs("ocutil.uploader", level="WARNING") as log:
                self.uploader.upload_single_file(self.single_file_path, self.BUCKET_NAME, "error_simulated.txt")
            # Check that retry warnings are logged.
            self.assertTrue(any("retrying" in message for message in log.output))
    
    def test_download_retry_error(self):
        """Simulate an error during single file download to test retry logic."""
        with patch.object(self.oci_manager.object_storage, 'get_object', side_effect=Exception("Simulated error")) as mock_get:
            with self.assertLogs("ocutil.downloader", level="WARNING") as log:
                self.downloader.download_single_file(self.BUCKET_NAME, "error_simulated.txt", os.path.join(self.download_dir, "error_simulated.txt"))
            self.assertTrue(any("retrying" in message for message in log.output))
    
    def test_summary_report_upload(self):
        """Test that bulk upload logs a summary report with duration and total bytes uploaded."""
        with self.assertLogs("ocutil.uploader", level="INFO") as log:
            self.uploader.upload_folder(self.folder_path, self.BUCKET_NAME, "summary_folder/", parallel_count=2)
        summary_msgs = [msg for msg in log.output if "Bulk upload operation completed" in msg]
        self.assertTrue(len(summary_msgs) > 0, "No bulk upload summary report logged.")
    
    def test_summary_report_download(self):
        """Test that bulk download logs a summary report with duration and total file count."""
        # First, upload the folder so that there are files to download.
        self.uploader.upload_folder(self.folder_path, self.BUCKET_NAME, "summary_download/", parallel_count=2)
        self.uploaded_objects.extend([
            f"summary_download/file1.txt",
            f"summary_download/file2.txt"
        ])
        with self.assertLogs("ocutil.downloader", level="INFO") as log:
            self.downloader.download_folder(self.BUCKET_NAME, "summary_download/", self.download_dir, parallel_count=2)
        summary_msgs = [msg for msg in log.output if "Bulk download operation completed" in msg]
        self.assertTrue(len(summary_msgs) > 0, "No bulk download summary report logged.")

if __name__ == "__main__":
    unittest.main()
