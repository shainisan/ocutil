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
        Simulate a paginated response by verifying that the CLI command is called.
        Since our new implementation uses subprocess.run, we patch it and check that the command
        includes the expected parameters.
        """
        from subprocess import run as subprocess_run
        test_prefix = "prefix/"
        download_dest = self.download_dir  # use a dummy destination

        # Patch subprocess.run in our downloader module.
        with patch("ocutil.utils.downloader.subprocess.run") as mock_run:
            self.downloader.download_folder("dummy-bucket", test_prefix, download_dest, parallel_count=2)
            # Verify that subprocess.run was called at least once.
            self.assertTrue(mock_run.called, "Expected subprocess.run to be called for bulk download.")
            # Extract the command from the first call.
            cmd = mock_run.call_args[0][0]
            # Check that the command includes the proper options.
            self.assertIn("--bucket-name", cmd)
            self.assertIn("dummy-bucket", cmd)
            self.assertIn("--download-dir", cmd)
            self.assertIn(download_dest, cmd)
            self.assertIn("--prefix", cmd)
            # The prefix should be test_prefix with a trailing slash.
            expected_cli_prefix = test_prefix if test_prefix.endswith('/') else f"{test_prefix}/"
            self.assertIn(expected_cli_prefix, cmd)


    def test_main_download_folder_prefix_trailing_slash(self):
        """
        Simulate calling:
            ocutil oc://dummy-bucket/folder/ <destination>
        and verify that main() calls Downloader.download_folder with a prefix that ends with a slash.
        """
        # We simulate a remote folder download
        test_args = ["ocutil", "oc://dummy-bucket/folder/", self.download_dir]
        with patch.object(sys, 'argv', test_args):
            with patch('ocutil.utils.downloader.Downloader.download_folder') as mock_download_folder:
                main()
                # Grab the arguments with which download_folder was called.
                # The signature is: download_folder(bucket_name, object_prefix, destination, parallel_count)
                args = mock_download_folder.call_args.args
                object_prefix = args[1]
                self.assertTrue(object_prefix.endswith('/'),
                                "Expected object prefix to end with '/' for folder downloads")


    def test_main_download_parquet_folder_prefix(self):
        """
        Simulate calling:
            ocutil oc://dummy-bucket/datasets/ML/ml_dataset_v3/processed/eval_set_ML_v3.parquet/ <destination>
        and verify that main() sets the download prefix correctly for a Parquet folder.
        
        Expected behavior:
        - parse_remote_path() should return:
            bucket_name = "dummy-bucket"
            object_path = "datasets/ML/ml_dataset_v3/processed/eval_set_ML_v3.parquet"
        - Since the remote path ends with '/' and object_path ends with ".parquet",
            main() should set the prefix to: "datasets/ML/ml_dataset_v3/processed/eval_set_ML_v3.parquet/part-"
        """
        # Prepare dummy arguments.
        remote_path = "oc://dummy-bucket/datasets/ML/ml_dataset_v3/processed/eval_set_ML_v3.parquet/"
        destination = "/dummy/destination"
        test_args = ["ocutil", remote_path, destination]
        
        from ocutil.main import main  # import main() from your main.py
        
        # Patch sys.argv to simulate command-line invocation.
        with patch.object(sys, 'argv', test_args):
            # Patch Downloader.download_folder to capture its call without executing the download.
            with patch('ocutil.utils.downloader.Downloader.download_folder') as mock_download_folder:
                main()
                # Downloader.download_folder is called with the following signature:
                # download_folder(bucket_name, prefix, destination, parallel_count)
                self.assertTrue(mock_download_folder.called, "Downloader.download_folder should be called.")
                args, kwargs = mock_download_folder.call_args
                bucket_used = args[0]
                prefix_used = args[1]
                # The expected prefix is the object_path from parse_remote_path with "/part-" appended.
                expected_prefix = "datasets/ML/ml_dataset_v3/processed/eval_set_ML_v3.parquet/part-"
                
                self.assertEqual(bucket_used, "dummy-bucket",
                                "Expected bucket to be 'dummy-bucket'.")
                self.assertEqual(prefix_used, expected_prefix,
                                f"Expected prefix to be '{expected_prefix}', but got '{prefix_used}'.")

if __name__ == "__main__":
    unittest.main()