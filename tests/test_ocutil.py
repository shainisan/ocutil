# tests/test_ocutil.py

# run with:
# python -m unittest discover -s tests -v

import os
import sys
import tempfile
import shutil
import unittest
import logging
import io # For capturing stdout/stderr
import contextlib # For redirect_stdout/stderr
import datetime
import re # For checking ls -lH output patterns
from unittest.mock import patch, MagicMock, ANY # Import ANY for flexible arg matching

# --- Potentially Needed OCI Classes for Mocking ---
# Import specific classes if needed for detailed mocking, otherwise MagicMock often suffices
import oci
# Example: from oci.object_storage.models import ObjectSummary, ListObjects

# --- Classes being tested ---
from ocutil.utils.oci_manager import OCIManager
from ocutil.utils.uploader import Uploader
from ocutil.utils.downloader import Downloader
from ocutil.utils.lister import Lister # Import the Lister
from ocutil.utils.formatters import human_readable_size # Import formatter

# --- Main script and helpers ---
# Import the main entry point and potentially helpers if needed directly
# Note: Testing main directly can be complex due to argparse/exit calls
# We will patch sys.argv and relevant methods instead where needed
from ocutil.main import main, adjust_remote_object_path, parse_remote_path

# --- Configure Logging for Tests (Optional) ---
# You might want to configure logging differently for tests,
# e.g., increase level to avoid clutter unless debugging tests.
# logging.basicConfig(level=logging.WARNING) # Example
logger = logging.getLogger(__name__) # Use module logger for test-specific info

# --- Dummy OCI Models for Mocking list_objects ---
# Using simple classes instead of importing full OCI models for mocking flexibility
class MockObjectSummary:
    def __init__(self, name, size=0, time_modified=None):
        self.name = name
        self.size = size
        self.time_modified = time_modified or datetime.datetime.now(datetime.timezone.utc)

class MockListData:
    def __init__(self, objects=None, prefixes=None, next_start_with=None, next_start_after=None):
        self.objects = objects if objects is not None else []
        self.prefixes = prefixes if prefixes is not None else []
        self.next_start_with = next_start_with
        self.next_start_after = next_start_after

class MockListResponse:
    def __init__(self, data, status=200, headers=None):
        self.data = data
        self.status = status
        self.headers = headers or {}

# --- Test Class ---

# Decorator to skip tests if OCI Manager fails initialization (e.g., bad config)
# Useful because many tests depend on self.oci_manager
_oci_manager_instance = None
def setUpModule():
    global _oci_manager_instance
    try:
        # Attempt to initialize OCIManager once for the whole module
        _oci_manager_instance = OCIManager()
        print(f"\nINFO: OCI Manager initialized successfully for tests (Namespace: {_oci_manager_instance.namespace}). Using bucket: {TestOCUtil.BUCKET_NAME}")
    except Exception as e:
        print(f"\nWARNING: Failed to initialize OCIManager in setUpModule: {e}")
        print("WARNING: Integration tests requiring OCI connection will be skipped.")
        _oci_manager_instance = None

def skip_if_oci_uninitialized(test_func):
    """Decorator to skip tests if OCIManager failed to initialize."""
    def wrapper(*args, **kwargs):
        if _oci_manager_instance is None:
            args[0].skipTest("Skipping test: OCIManager failed to initialize (check OCI config/connectivity)")
        else:
            return test_func(*args, **kwargs)
    return wrapper


class TestOCUtil(unittest.TestCase):
    BUCKET_NAME = "your-unique-test-bucket-name" # <<<--- CHANGE THIS to your actual test bucket name
    # IMPORTANT: Ensure this bucket exists and your OCI profile has permissions.
    # The tests WILL create and delete objects inside this bucket.

    @classmethod
    def setUpClass(cls):
        # Perform bucket existence check once
        if _oci_manager_instance:
            try:
                print(f"INFO: Checking if test bucket '{cls.BUCKET_NAME}' exists...")
                _oci_manager_instance.object_storage.head_bucket(
                    _oci_manager_instance.namespace, cls.BUCKET_NAME
                )
                print(f"INFO: Test bucket '{cls.BUCKET_NAME}' found.")
            except oci.exceptions.ServiceError as e:
                if e.status == 404:
                    print(f"\n\nERROR: Test bucket '{cls.BUCKET_NAME}' not found or accessible.")
                    print("Please create the bucket or check permissions/config profile.")
                    print("Skipping all integration tests.\n")
                    global _oci_manager_instance
                    _oci_manager_instance = None # Prevent tests from running
                else:
                    print(f"\n\nERROR: Could not verify test bucket '{cls.BUCKET_NAME}': {e}")
                    print("Skipping all integration tests.\n")
                    global _oci_manager_instance # pylint: disable=global-variable-not-assigned
                    _oci_manager_instance = None
            except Exception as e:
                 print(f"\n\nERROR: Unexpected error checking test bucket: {e}")
                 print("Skipping all integration tests.\n")
                 global _oci_manager_instance # pylint: disable=global-variable-not-assigned
                 _oci_manager_instance = None


    def setUp(self):
        """Set up test fixtures before each test method."""
        if _oci_manager_instance is None:
            self.skipTest("Skipping test setup: OCIManager failed to initialize")

        self.oci_manager = _oci_manager_instance # Use shared instance
        # Re-init Uploader/Downloader/Lister to ensure clean state if needed
        self.uploader = Uploader(self.oci_manager)
        self.downloader = Downloader(self.oci_manager)
        self.lister = Lister(self.oci_manager) # Instantiate Lister

        self.uploaded_objects = set() # Use a set to avoid duplicate delete attempts

        self.test_dir = tempfile.mkdtemp(prefix="ocutil_test_")
        logger.debug(f"Created temp dir: {self.test_dir}")

        # Create a standard folder structure
        self.folder_path = os.path.join(self.test_dir, "test_folder")
        os.makedirs(self.folder_path, exist_ok=True)
        self.file1_path = os.path.join(self.folder_path, "file1.txt")
        self.file2_path = os.path.join(self.folder_path, "file2.txt")
        with open(self.file1_path, "w") as f: f.write("This is file 1")
        with open(self.file2_path, "w") as f: f.write("This is file 2")

        # Create a nested structure
        self.sub_folder_path = os.path.join(self.folder_path, "sub_folder")
        os.makedirs(self.sub_folder_path, exist_ok=True)
        self.sub_file_path = os.path.join(self.sub_folder_path, "sub_file.txt")
        with open(self.sub_file_path, "w") as f: f.write("This is sub file")


        # Create a single file.
        self.single_file_path = os.path.join(self.test_dir, "single_file.txt")
        with open(self.single_file_path, "w") as f: f.write("This is a single file")

        # Create an empty file
        self.empty_file_path = os.path.join(self.test_dir, "empty_file.txt")
        open(self.empty_file_path, 'a').close()


        # Create a destination directory for downloads.
        self.download_dir = os.path.join(self.test_dir, "download")
        os.makedirs(self.download_dir, exist_ok=True)

    def tearDown(self):
        """Clean up resources after each test method."""
        # Delete all tracked uploaded objects from the bucket.
        if self.oci_manager and self.uploaded_objects:
            # Use bulk delete if many objects? For now, delete individually.
            # OCI SDK has delete_objects but requires listing/preparing input.
            logger.debug(f"Cleaning up {len(self.uploaded_objects)} remote objects...")
            # Copy set before iterating as delete might fail halfway
            objects_to_delete = list(self.uploaded_objects)
            self.uploaded_objects.clear() # Clear original set
            for remote_object in objects_to_delete:
                try:
                    logger.debug(f"Deleting remote object: {remote_object}")
                    self.oci_manager.object_storage.delete_object(
                        self.oci_manager.namespace, self.BUCKET_NAME, remote_object
                    )
                    # logger.info("Deleted remote object: %s", remote_object)
                except oci.exceptions.ServiceError as e:
                    # Ignore 404 errors during cleanup (object might already be gone)
                    if e.status != 404:
                        logger.warning("Failed to delete remote object %s: %s", remote_object, e)
                except Exception as e:
                     logger.warning("Unexpected error deleting remote object %s: %s", remote_object, e)

        # Cleanup local temporary directory. Use ignore_errors=True for robustness.
        if os.path.exists(self.test_dir):
             logger.debug(f"Removing temp dir: {self.test_dir}")
             shutil.rmtree(self.test_dir, ignore_errors=True)

    # --- Helper Methods ---
    def _upload_test_file(self, local_path, remote_path_suffix):
        """Helper to upload a file and track it for cleanup."""
        if not self.oci_manager: self.skipTest("OCI Manager not initialized")
        full_remote_path = remote_path_suffix
        logger.debug(f"Helper uploading '{local_path}' to '{full_remote_path}'")
        try:
             # Use upload_single_file for testing the actual mechanism
             success = self.uploader.upload_single_file(local_path, self.BUCKET_NAME, full_remote_path)
             if success:
                 self.uploaded_objects.add(full_remote_path)
             else:
                  self.fail(f"Helper failed to upload {local_path} to {full_remote_path}")
        except Exception as e:
             self.fail(f"Helper failed to upload {local_path} due to exception: {e}")

    def _run_main_and_capture(self, args_list):
        """Runs main with patched argv and captures stdout/stderr."""
        logger.debug(f"Running main with args: {args_list}")
        # Ensure first arg is script name placeholder
        if args_list[0] != "ocutil":
             args_list.insert(0, "ocutil")

        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()
        exit_code = 0 # Assume success unless exit is called

        # Patch sys.argv, sys.exit and redirect streams
        with patch.object(sys, 'argv', args_list):
             # Patch sys.exit to prevent test runner from exiting
             with patch.object(sys, 'exit') as mock_exit:
                  mock_exit.side_effect = lambda code=0: exec(f"global exit_code; exit_code = {code}")
                  # Redirect stdout and stderr
                  with contextlib.redirect_stdout(stdout_capture):
                       with contextlib.redirect_stderr(stderr_capture):
                            # We might need to redirect logging as well if we want to capture stderr logs
                            # For now, assume logger writes to stderr by default capture
                            try:
                                 main()
                            except SystemExit as e:
                                 # Capture exit code if main calls sys.exit directly
                                 exit_code = e.code if isinstance(e.code, int) else 1 # Default to 1 if code is None/str
                            except Exception as e:
                                 # Catch other exceptions to prevent test failure, log them instead
                                 logger.error(f"Exception during main execution in test: {e}", exc_info=True)
                                 exit_code = 1 # Indicate failure

        stdout = stdout_capture.getvalue()
        stderr = stderr_capture.getvalue()
        logger.debug(f"main stdout:\n{stdout}")
        logger.debug(f"main stderr:\n{stderr}")
        logger.debug(f"main exit_code: {exit_code}")
        return stdout, stderr, exit_code


    # --- CP Command Tests (Existing, Modified where needed) ---

    @skip_if_oci_uninitialized
    def test_upload_and_download_folder_with_trailing_slash(self):
        # Use a remote prefix that ends with a slash.
        object_prefix = "cp_test_folder_with_slash/"
        # Upload the folder.
        self.uploader.upload_folder(self.folder_path, self.BUCKET_NAME, object_prefix, parallel_count=2)
        self.uploaded_objects.add(f"{object_prefix}file1.txt")
        self.uploaded_objects.add(f"{object_prefix}file2.txt")
        self.uploaded_objects.add(f"{object_prefix}sub_folder/sub_file.txt") # Include subfolder file

        # Download the folder.
        download_destination = os.path.join(self.download_dir, "downloaded_folder_with_slash")
        # Note: download_folder puts contents *into* destination
        self.downloader.download_folder(self.BUCKET_NAME, object_prefix, download_destination, parallel_count=2)

        # Verify that the files have been downloaded correctly.
        file1_path = os.path.join(download_destination, "file1.txt")
        file2_path = os.path.join(download_destination, "file2.txt")
        sub_file_path = os.path.join(download_destination, "sub_folder/sub_file.txt") # Check subfolder

        self.assertTrue(os.path.exists(file1_path), f"{file1_path} does not exist")
        self.assertTrue(os.path.exists(file2_path), f"{file2_path} does not exist")
        self.assertTrue(os.path.exists(sub_file_path), f"{sub_file_path} does not exist")
        with open(file1_path, "r") as f: content1 = f.read()
        with open(file2_path, "r") as f: content2 = f.read()
        with open(sub_file_path, "r") as f: content3 = f.read()
        self.assertEqual(content1, "This is file 1")
        self.assertEqual(content2, "This is file 2")
        self.assertEqual(content3, "This is sub file")

    # MODIFIED: Patch target fixed
    def test_download_folder_pagination(self):
        """Simulate download pagination by patching list_objects and worker."""
        DummyObj = lambda name: type("DummyObj", (), {"name": name})
        DummyResponse = lambda objects, next_token_key=None, next_token_val=None: type(
            "DummyResponse", (), {
                "data": type("DummyData", (), {
                    "objects": objects,
                    "prefixes": [], # Assume no prefixes for simplicity here
                    pagination_key: next_token_val if next_token_key else None
                 }),
                "headers": {}
             }
        )

        # Simulate recursive listing pagination (uses next_start_after)
        pagination_key = 'next_start_after'
        obj1, obj2, obj3, obj4 = DummyObj("prefix/obj1"), DummyObj("prefix/obj2"), DummyObj("prefix/obj3"), DummyObj("prefix/obj4")
        responses = [
            MockListResponse(data=MockListData(objects=[obj1, obj2], next_start_after=obj2.name)),
            MockListResponse(data=MockListData(objects=[obj3, obj4], next_start_after=obj4.name)),
            MockListResponse(data=MockListData(objects=[], next_start_after=None)),
        ]
        side_effect = MagicMock(side_effect=responses)

        # Patch list_objects and the actual download worker
        with patch.object(self.oci_manager.object_storage, 'list_objects', side_effect=side_effect):
            # Patch the CORRECT worker function name
            with patch.object(self.downloader, '_download_worker', return_value=(True, "dummy", None)) as mock_download_worker:
                # Need to use a real OCIManager if not skipping tests
                temp_downloader = Downloader(self.oci_manager) if self.oci_manager else Downloader(MagicMock()) # Use mock manager if skipped
                # Call download_folder with limit to force pagination checks (limit=2)
                # Recursive listing simulation doesn't use delimiter, uses start_after
                temp_downloader.download_folder("dummy-bucket", "prefix/", self.download_dir, parallel_count=1, limit=2)

                # Assert list_objects called multiple times
                self.assertGreaterEqual(side_effect.call_count, 3, "list_objects should be called at least 3 times for 3 pages")
                # Check pagination args (example check on second call)
                args, kwargs = side_effect.call_args_list[1] # Second call
                self.assertEqual(kwargs.get('start_after'), obj2.name)

                # Assert download worker called 4 times
                self.assertEqual(mock_download_worker.call_count, 4)

    @skip_if_oci_uninitialized
    def test_upload_and_download_folder_without_trailing_slash(self):
        object_prefix = "cp_test_folder_no_slash" # no trailing slash provided
        self.uploader.upload_folder(self.folder_path, self.BUCKET_NAME, object_prefix, parallel_count=2)
        # Objects are created *under* the prefix
        self.uploaded_objects.add(f"{object_prefix}/file1.txt")
        self.uploaded_objects.add(f"{object_prefix}/file2.txt")
        self.uploaded_objects.add(f"{object_prefix}/sub_folder/sub_file.txt")

        download_destination = os.path.join(self.download_dir, "downloaded_folder_no_slash")
        self.downloader.download_folder(self.BUCKET_NAME, object_prefix, download_destination, parallel_count=2)

        # Check downloaded files
        file1_path = os.path.join(download_destination, "file1.txt")
        file2_path = os.path.join(download_destination, "file2.txt")
        sub_file_path = os.path.join(download_destination, "sub_folder/sub_file.txt")
        self.assertTrue(os.path.exists(file1_path))
        self.assertTrue(os.path.exists(file2_path))
        self.assertTrue(os.path.exists(sub_file_path))
        # Content checks omitted for brevity, covered in other test

    @skip_if_oci_uninitialized
    def test_upload_and_download_single_file_with_explicit_object_name(self):
        remote_object_path = "cp_explicit_single.txt"
        self.uploader.upload_single_file(self.single_file_path, self.BUCKET_NAME, remote_object_path)
        self.uploaded_objects.add(remote_object_path)

        download_destination = os.path.join(self.download_dir, "downloaded_explicit.txt")
        self.downloader.download_single_file(self.BUCKET_NAME, remote_object_path, download_destination)
        self.assertTrue(os.path.exists(download_destination))
        with open(download_destination, "r") as f: content = f.read()
        self.assertEqual(content, "This is a single file")

    @skip_if_oci_uninitialized
    def test_upload_and_download_single_file_to_bucket_root(self):
        # Destination is just oc://bucket-name
        adjusted_object_path = adjust_remote_object_path(self.single_file_path, "") # Should return "single_file.txt"
        self.assertEqual(adjusted_object_path, "single_file.txt")
        self.uploader.upload_single_file(self.single_file_path, self.BUCKET_NAME, adjusted_object_path)
        self.uploaded_objects.add(adjusted_object_path)

        download_destination = os.path.join(self.download_dir, "downloaded_root.txt")
        # Download requires target directory, file placed inside
        self.downloader.download_single_file(self.BUCKET_NAME, adjusted_object_path, download_destination)
        # The downloaded file should be named based on object name, not dest file name
        expected_download_path = os.path.join(self.download_dir, adjusted_object_path) # Should be download_dir/single_file.txt
        self.assertTrue(os.path.exists(expected_download_path))
        with open(expected_download_path, "r") as f: content = f.read()
        self.assertEqual(content, "This is a single file")


    @skip_if_oci_uninitialized
    def test_upload_single_file_destination_folder_without_trailing_slash(self):
        remote_object_path = "cp_dest_folder" # Treat as prefix
        expected_adjusted_path = "cp_dest_folder/single_file.txt"
        # Assuming adjust_remote_object_path correctly identifies 'folder' as a prefix here
        adjusted_object_path = adjust_remote_object_path(self.single_file_path, remote_object_path)
        self.assertEqual(adjusted_object_path, expected_adjusted_path)

        self.uploader.upload_single_file(self.single_file_path, self.BUCKET_NAME, adjusted_object_path)
        self.uploaded_objects.add(adjusted_object_path)

        download_destination_file = os.path.join(self.download_dir, "dl_single_in_folder.txt")
        self.downloader.download_single_file(self.BUCKET_NAME, adjusted_object_path, download_destination_file)
        self.assertTrue(os.path.exists(download_destination_file))
        with open(download_destination_file, "r") as f: content = f.read()
        self.assertEqual(content, "This is a single file")

    @skip_if_oci_uninitialized
    def test_upload_single_file_destination_folder_with_trailing_slash(self):
        remote_object_path = "cp_dest_folder_slash/"
        expected_adjusted_path = "cp_dest_folder_slash/single_file.txt"
        adjusted_object_path = adjust_remote_object_path(self.single_file_path, remote_object_path)
        self.assertEqual(adjusted_object_path, expected_adjusted_path)

        self.uploader.upload_single_file(self.single_file_path, self.BUCKET_NAME, adjusted_object_path)
        self.uploaded_objects.add(adjusted_object_path)

        download_destination_file = os.path.join(self.download_dir, "dl_single_in_folder_slash.txt")
        self.downloader.download_single_file(self.BUCKET_NAME, adjusted_object_path, download_destination_file)
        self.assertTrue(os.path.exists(download_destination_file))
        with open(download_destination_file, "r") as f: content = f.read()
        self.assertEqual(content, "This is a single file")

    # --- Error Handling Tests ---
    def test_upload_nonexistent_file(self):
        nonexistent_file = os.path.join(self.test_dir, "does_not_exist.txt")
        remote_object_path = "error_does_not_exist.txt"
        # Check logger output OR check return value
        # Using assertLogs requires the logger name to be exact
        with self.assertLogs("ocutil.uploader", level="ERROR") as log_cm:
             success = self.uploader.upload_single_file(nonexistent_file, self.BUCKET_NAME, remote_object_path)
             self.assertFalse(success, "upload_single_file should return False for non-existent file")
        # Check log message content
        self.assertTrue(any("does not exist or is not a file" in message for message in log_cm.output),
                        f"Expected 'does not exist' error message, got: {log_cm.output}")


    def test_download_nonexistent_object(self):
        remote_object_path = "error_nonexistent_object.txt"
        download_destination = os.path.join(self.download_dir, "error_nonexistent_dl.txt")
        # Need to mock head_object or get_object to raise 404
        mock_response_404 = oci.exceptions.ServiceError(status=404, code="ObjectNotFound", message="Not Found", headers={})
        with patch.object(self.oci_manager.object_storage, 'head_object', side_effect=mock_response_404):
             with patch.object(self.oci_manager.object_storage, 'get_object', side_effect=mock_response_404):
                  with self.assertLogs("ocutil.downloader", level="ERROR") as log_cm:
                       success = self.downloader.download_single_file(self.BUCKET_NAME, remote_object_path, download_destination)
                       self.assertFalse(success, "download_single_file should return False for non-existent object")
                  # Check log message content for specific 404 handling if added, or general error
                  self.assertTrue(any("Object not found (404)" in message or "Error downloading" in message for message in log_cm.output),
                                  f"Expected 'Object not found' or 'Error downloading' message, got: {log_cm.output}")
        self.assertFalse(os.path.exists(download_destination)) # Ensure file was not created

    # --- adjust_remote_object_path Tests ---
    def test_adjust_remote_object_path_empty(self):
        self.assertEqual(adjust_remote_object_path("dummy.txt", ""), "dummy.txt")

    def test_adjust_remote_object_path_explicit_filename(self):
        self.assertEqual(adjust_remote_object_path("dummy.txt", "custom.txt"), "custom.txt")

    def test_adjust_remote_object_path_dest_is_prefix_no_slash(self):
        self.assertEqual(adjust_remote_object_path("dummy.txt", "prefix"), "prefix/dummy.txt")

    def test_adjust_remote_object_path_dest_is_prefix_with_slash(self):
        self.assertEqual(adjust_remote_object_path("dummy.txt", "prefix/"), "prefix/dummy.txt")

    def test_adjust_remote_object_path_dest_subfolder_no_slash(self):
         self.assertEqual(adjust_remote_object_path("dummy.txt", "prefix/sub"), "prefix/sub/dummy.txt")

    def test_adjust_remote_object_path_dest_subfolder_with_slash(self):
         self.assertEqual(adjust_remote_object_path("dummy.txt", "prefix/sub/"), "prefix/sub/dummy.txt")


    # --- main Function Flow Tests (using patching) ---

    @patch('ocutil.main.OCIManager') # Patch manager to avoid real connection setup
    @patch('ocutil.main.Uploader') # Patch Uploader class
    def test_main_cp_upload_folder_flow(self, mock_uploader_cls, mock_oci_manager_cls):
        """Test main() dispatches correctly for folder upload."""
        mock_uploader_instance = mock_uploader_cls.return_value
        test_args = ["ocutil", "cp", self.folder_path, f"oc://{self.BUCKET_NAME}/remote_dest_prefix/"]
        stdout, stderr, exit_code = self._run_main_and_capture(test_args)

        self.assertEqual(exit_code, 0)
        mock_uploader_instance.upload_folder.assert_called_once()
        # Check args passed to upload_folder
        call_args = mock_uploader_instance.upload_folder.call_args.args
        self.assertEqual(call_args[0], self.folder_path) # local_dir
        self.assertEqual(call_args[1], self.BUCKET_NAME) # bucket_name
        self.assertEqual(call_args[2], "remote_dest_prefix") # object_prefix (stripped slash)


    # MODIFIED: Patch target fixed
    @patch('ocutil.main.OCIManager')
    @patch('ocutil.main.Uploader')
    def test_main_cp_upload_wildcard_flow(self, mock_uploader_cls, mock_oci_manager_cls):
        """Test main() dispatches correctly for wildcard upload."""
        mock_uploader_instance = mock_uploader_cls.return_value
        # Use a pattern guaranteed to match the two files in self.folder_path
        wildcard_path = os.path.join(self.folder_path, "file*.txt")
        test_args = ["ocutil", "cp", wildcard_path, f"oc://{self.BUCKET_NAME}/wildcard_dest/"]
        stdout, stderr, exit_code = self._run_main_and_capture(test_args)

        self.assertEqual(exit_code, 0)
        # Check that upload_files (new method for wildcards) was called
        mock_uploader_instance.upload_files.assert_called_once()
        # Check the list passed to upload_files
        call_args_list = mock_uploader_instance.upload_files.call_args.args[0]
        self.assertEqual(len(call_args_list), 2) # file1.txt, file2.txt
        # Check expected remote names (should be under wildcard_dest/)
        expected_remote1 = "wildcard_dest/file1.txt"
        expected_remote2 = "wildcard_dest/file2.txt"
        actual_files = sorted([(local, remote) for local, remote in call_args_list])
        self.assertEqual(actual_files[0][0], self.file1_path)
        self.assertEqual(actual_files[0][1], expected_remote1)
        self.assertEqual(actual_files[1][0], self.file2_path)
        self.assertEqual(actual_files[1][1], expected_remote2)


    @patch('ocutil.main.OCIManager')
    @patch('ocutil.main.Downloader') # Patch Downloader class
    def test_main_cp_download_folder_flow(self, mock_downloader_cls, mock_oci_manager_cls):
        """Test main() dispatches correctly for folder download."""
        mock_downloader_instance = mock_downloader_cls.return_value
        remote_src = f"oc://{self.BUCKET_NAME}/remote_src_folder/"
        test_args = ["ocutil", "cp", remote_src, self.download_dir]
        stdout, stderr, exit_code = self._run_main_and_capture(test_args)

        self.assertEqual(exit_code, 0)
        # Assert download_folder was called
        mock_downloader_instance.download_folder.assert_called_once()
        call_args = mock_downloader_instance.download_folder.call_args.args
        self.assertEqual(call_args[0], self.BUCKET_NAME)
        self.assertEqual(call_args[1], "remote_src_folder/") # prefix
        self.assertEqual(call_args[2], self.download_dir) # destination

    # --- Dry Run Tests (Unchanged conceptually, check logger name if needed) ---
    def test_dry_run_upload_folder(self):
        dry_run_uploader = Uploader(self.oci_manager, dry_run=True)
        with self.assertLogs("ocutil.uploader", level="INFO") as log:
            dry_run_uploader.upload_folder(self.folder_path, self.BUCKET_NAME, "dry_run_folder/", parallel_count=2)
        self.assertTrue(any("DRY-RUN:" in message for message in log.output))

    def test_dry_run_download_folder(self):
        # Need to mock list_objects to return something for dry run download
        mock_obj = MockObjectSummary(name="dry_run_folder/file.txt")
        mock_response = MockListResponse(data=MockListData(objects=[mock_obj]))
        with patch.object(self.oci_manager.object_storage, 'list_objects', return_value=mock_response):
             dry_run_downloader = Downloader(self.oci_manager, dry_run=True)
             with self.assertLogs("ocutil.downloader", level="INFO") as log:
                 dry_run_downloader.download_folder(self.BUCKET_NAME, "dry_run_folder/", self.download_dir, parallel_count=2)
             # Assert that the log contains DRY-RUN messages.
             self.assertTrue(any("DRY-RUN: Would download" in message for message in log.output))


    # --- Retry Tests ---

    # MODIFIED: Patch target fixed
    def test_upload_retry_error(self):
        """Simulate an error during single file upload to test retry logic."""
        # Force upload_manager.upload_file to raise an error
        # Simulate a retriable service error (e.g., 500)
        simulated_error = oci.exceptions.ServiceError(status=500, code="InternalError", message="Simulated error", headers={})
        # Need to patch the method on the *instance* used by the test's uploader
        with patch.object(self.uploader.upload_manager, 'upload_file', side_effect=simulated_error) as mock_upload_mngr:
            # Check logger warnings for retry messages
            with self.assertLogs("ocutil.uploader", level="WARNING") as log:
                 success = self.uploader.upload_single_file(self.single_file_path, self.BUCKET_NAME, "error_simulated_upload.txt")
            self.assertFalse(success) # Should fail after retries
            # Check that upload_file was called multiple times (initial + retries)
            self.assertGreater(mock_upload_mngr.call_count, 1, "UploadManager.upload_file should be called more than once due to retries")
            # Check that retry warnings are logged.
            self.assertTrue(any("retrying" in message for message in log.output),
                             f"Expected retry message, got: {log.output}")

    def test_download_retry_error(self):
        """Simulate an error during single file download to test retry logic."""
        simulated_error = oci.exceptions.ServiceError(status=500, code="InternalError", message="Simulated error", headers={})
        # Need to patch both head_object (for size check) and get_object
        # Let head_object succeed, but get_object fail
        mock_head_response = MagicMock(headers={"Content-Length": "100"})
        with patch.object(self.oci_manager.object_storage, 'head_object', return_value=mock_head_response):
            with patch.object(self.oci_manager.object_storage, 'get_object', side_effect=simulated_error) as mock_get:
                with self.assertLogs("ocutil.downloader", level="WARNING") as log:
                     success = self.downloader.download_single_file(self.BUCKET_NAME, "error_simulated_dl.txt", os.path.join(self.download_dir, "error_dl.txt"))
                self.assertFalse(success) # Should fail after retries
                self.assertGreater(mock_get.call_count, 1, "get_object should be called more than once due to retries")
                self.assertTrue(any("retrying" in message for message in log.output),
                                 f"Expected retry message, got: {log.output}")

    # --- Summary Report Tests (Unchanged) ---
    @skip_if_oci_uninitialized
    def test_summary_report_upload(self):
        with self.assertLogs("ocutil.uploader", level="INFO") as log:
            self.uploader.upload_folder(self.folder_path, self.BUCKET_NAME, "summary_upload/", parallel_count=2)
            # Add objects to cleanup list AFTER successful upload
            self.uploaded_objects.add("summary_upload/file1.txt")
            self.uploaded_objects.add("summary_upload/file2.txt")
            self.uploaded_objects.add("summary_upload/sub_folder/sub_file.txt")
        # Check for summary message structure
        self.assertTrue(any("Upload Summary" in msg and "Operation completed" in msg for msg in log.output),
                        "No upload summary report logged.")

    @skip_if_oci_uninitialized
    def test_summary_report_download(self):
        # Setup: Upload folder first
        setup_prefix = "summary_download/"
        self.uploader.upload_folder(self.folder_path, self.BUCKET_NAME, setup_prefix, parallel_count=2)
        self.uploaded_objects.add(f"{setup_prefix}file1.txt")
        self.uploaded_objects.add(f"{setup_prefix}file2.txt")
        self.uploaded_objects.add(f"{setup_prefix}sub_folder/sub_file.txt")

        # Test: Download and check logs
        with self.assertLogs("ocutil.downloader", level="INFO") as log:
            self.downloader.download_folder(self.BUCKET_NAME, setup_prefix, self.download_dir, parallel_count=2)
        self.assertTrue(any("Download Summary" in msg and "Operation completed" in msg for msg in log.output),
                        "No download summary report logged.")


    # ======================================================
    # ============ NEW LS Command Tests ====================
    # ======================================================

    @skip_if_oci_uninitialized
    def test_main_ls_simple_slash(self):
        """Test 'ls' command with simple output and trailing slash."""
        prefix = "ls_simple_slash/"
        file1 = f"{prefix}file_a.txt"
        file2 = f"{prefix}sub/file_b.txt"
        self._upload_test_file(self.file1_path, file1)
        self._upload_test_file(self.file2_path, file2) # Creates implicit 'sub/' prefix

        stdout, stderr, exit_code = self._run_main_and_capture(
            ["ls", f"oc://{self.BUCKET_NAME}/{prefix}"]
        )

        self.assertEqual(exit_code, 0)
        # Output should be relative paths, dirs end with /
        expected_lines = sorted(["file_a.txt", "sub/"])
        actual_lines = sorted([line for line in stdout.splitlines() if line]) # Ignore empty lines
        self.assertListEqual(actual_lines, expected_lines)

    @skip_if_oci_uninitialized
    def test_main_ls_simple_no_slash(self):
        """Test 'ls' command with simple output and no trailing slash."""
        prefix = "ls_simple_no_slash" # Note: No trailing slash in prefix name
        prefix_with_slash = prefix + "/"
        file1 = f"{prefix_with_slash}file_a.txt"
        file2 = f"{prefix_with_slash}sub/file_b.txt"
        self._upload_test_file(self.file1_path, file1)
        self._upload_test_file(self.file2_path, file2)

        # Run ls command *without* trailing slash
        stdout, stderr, exit_code = self._run_main_and_capture(
            ["ls", f"oc://{self.BUCKET_NAME}/{prefix}"] # Request without slash
        )

        self.assertEqual(exit_code, 0)
        # Output should be relative paths, dirs end with /
        expected_lines = sorted(["file_a.txt", "sub/"])
        actual_lines = sorted([line for line in stdout.splitlines() if line])
        self.assertListEqual(actual_lines, expected_lines)


    @skip_if_oci_uninitialized
    def test_main_ls_long_human(self):
        """Test 'ls -lH' command output format."""
        prefix = "ls_long_human/"
        file1 = f"{prefix}file_a.txt"    # Content: "This is file 1" (14 bytes)
        sub_prefix = f"{prefix}sub_dir/"
        # Ensure empty prefix object is also created for testing <DIR>
        self._upload_test_file(self.empty_file_path, f"{sub_prefix}dummy.txt")

        # Upload file1 with known content/size
        self._upload_test_file(self.file1_path, file1)

        stdout, stderr, exit_code = self._run_main_and_capture(
            ["ls", "-lH", f"oc://{self.BUCKET_NAME}/{prefix}"]
        )

        self.assertEqual(exit_code, 0)
        output_lines = [line for line in stdout.splitlines() if line]

        # Check for directory entry format (approximate check)
        dir_pattern = r"^\s*\s+\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\s+<DIR>\s+sub_dir/$"
        self.assertTrue(any(re.search(dir_pattern, line) for line in output_lines),
                        f"<DIR> entry for 'sub_dir/' not found or format incorrect in:\n{stdout}")

        # Check for file entry format (approximate check)
        # Size should be human-readable (e.g., "14 B") and right-aligned
        file_pattern = r"^\s*14 B\s+\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\s+file_a.txt$"
        self.assertTrue(any(re.search(file_pattern, line) for line in output_lines),
                        f"File entry for 'file_a.txt' (14 B) not found or format incorrect in:\n{stdout}")


    @skip_if_oci_uninitialized
    def test_main_ls_recursive(self):
        """Test 'ls -r' recursive listing."""
        prefix = "ls_recursive/"
        file1 = f"{prefix}file_a.txt"
        file2 = f"{prefix}sub/file_b.txt"
        file3 = f"{prefix}sub/deeper/file_c.txt"
        self._upload_test_file(self.file1_path, file1)
        self._upload_test_file(self.file2_path, file2)
        self._upload_test_file(self.single_file_path, file3)

        stdout, stderr, exit_code = self._run_main_and_capture(
            ["ls", "-r", f"oc://{self.BUCKET_NAME}/{prefix}"]
        )

        self.assertEqual(exit_code, 0)
        # Output should contain full relative paths from prefix
        expected_files = sorted([
            "file_a.txt",
            "sub/file_b.txt",
            "sub/deeper/file_c.txt"
        ])
        actual_files = sorted([line for line in stdout.splitlines() if line])
        self.assertListEqual(actual_files, expected_files)


    @skip_if_oci_uninitialized
    def test_main_ls_empty(self):
        """Test 'ls' on an empty prefix."""
        prefix = "ls_empty_test/"
        # Ensure prefix exists but is empty (upload and delete is one way)
        dummy_file = f"{prefix}delete_me.txt"
        self._upload_test_file(self.empty_file_path, dummy_file)
        self.oci_manager.object_storage.delete_object(self.oci_manager.namespace, self.BUCKET_NAME, dummy_file)
        # Remove from cleanup list as it's already deleted
        if dummy_file in self.uploaded_objects: self.uploaded_objects.remove(dummy_file)

        stdout, stderr, exit_code = self._run_main_and_capture(
            ["ls", f"oc://{self.BUCKET_NAME}/{prefix}"]
        )

        self.assertEqual(exit_code, 0)
        # Standard output should be empty
        self.assertEqual(stdout.strip(), "")


    def test_main_ls_nonexistent_prefix(self):
        """Test 'ls' on a non-existent prefix."""
        # No need to skip if OCI uninitialized, should just fail path parsing or listing
        prefix = "ls_nonexistent_prefix/"
        # Run main and capture output/exit code
        stdout, stderr, exit_code = self._run_main_and_capture(
            ["ls", f"oc://{self.BUCKET_NAME}/{prefix}"]
        )
        # Expect exit code 1 (failure)
        self.assertNotEqual(exit_code, 0, "Expected non-zero exit code for non-existent prefix")
        # Expect specific error message on stderr (logged via logger)
        self.assertIn(f"Error: No objects found at 'oc://{self.BUCKET_NAME}/{prefix}'", stderr)


    # --- Lister Unit Tests (Mocking list_objects) ---

    @patch('ocutil.utils.lister.Lister._print_results') # Don't care about printing here
    @patch('ocutil.utils.lister.Lister.object_storage') # Patch the client used by Lister
    def test_lister_pagination_non_recursive(self, mock_object_storage, mock_print):
        """Test Lister pagination logic with delimiter and next_start_with."""
        obj1 = MockObjectSummary(name="prefix/obj1")
        obj2 = MockObjectSummary(name="prefix/obj2")
        pfx1 = "prefix/sub1/"
        pfx2 = "prefix/sub2/"
        # Page 1: obj1, pfx1, next = 'prefix/obj2'
        # Page 2: obj2, pfx2, next = None
        responses = [
            MockListResponse(data=MockListData(objects=[obj1], prefixes=[pfx1], next_start_with="prefix/obj2")),
            MockListResponse(data=MockListData(objects=[obj2], prefixes=[pfx2], next_start_with=None)),
        ]
        mock_object_storage.list_objects.side_effect = responses

        lister = Lister(MagicMock()) # Use mock manager, we patch storage client
        lister.object_storage = mock_object_storage # Assign the patched client

        lister.list_path("bucket", "prefix/", long_format=False, human_readable=False, recursive=False)

        # Check list_objects calls
        self.assertEqual(mock_object_storage.list_objects.call_count, 2)
        # Check first call args
        args1, kwargs1 = mock_object_storage.list_objects.call_args_list[0]
        self.assertEqual(kwargs1.get('prefix'), "prefix/")
        self.assertEqual(kwargs1.get('delimiter'), "/")
        self.assertNotIn('start', kwargs1)
        # Check second call args
        args2, kwargs2 = mock_object_storage.list_objects.call_args_list[1]
        self.assertEqual(kwargs2.get('prefix'), "prefix/")
        self.assertEqual(kwargs2.get('delimiter'), "/")
        self.assertEqual(kwargs2.get('start'), "prefix/obj2") # Check pagination param

    @patch('ocutil.utils.lister.Lister._print_results')
    @patch('ocutil.utils.lister.Lister.object_storage')
    def test_lister_pagination_recursive(self, mock_object_storage, mock_print):
        """Test Lister pagination logic recursive with next_start_after."""
        obj1 = MockObjectSummary(name="prefix/obj1")
        obj2 = MockObjectSummary(name="prefix/obj2")
        obj3 = MockObjectSummary(name="prefix/sub/obj3")
        # Page 1: obj1, obj2, next = 'prefix/obj2'
        # Page 2: obj3, next = None
        responses = [
            MockListResponse(data=MockListData(objects=[obj1, obj2], next_start_after=obj2.name)),
            MockListResponse(data=MockListData(objects=[obj3], next_start_after=None)),
        ]
        mock_object_storage.list_objects.side_effect = responses

        lister = Lister(MagicMock())
        lister.object_storage = mock_object_storage

        lister.list_path("bucket", "prefix/", long_format=False, human_readable=False, recursive=True)

        # Check list_objects calls
        self.assertEqual(mock_object_storage.list_objects.call_count, 2)
        # Check first call args
        args1, kwargs1 = mock_object_storage.list_objects.call_args_list[0]
        self.assertEqual(kwargs1.get('prefix'), "prefix/")
        self.assertNotIn('delimiter', kwargs1)
        self.assertNotIn('start_after', kwargs1)
        # Check second call args
        args2, kwargs2 = mock_object_storage.list_objects.call_args_list[1]
        self.assertEqual(kwargs2.get('prefix'), "prefix/")
        self.assertNotIn('delimiter', kwargs2)
        self.assertEqual(kwargs2.get('start_after'), obj2.name) # Check pagination param


    @patch('ocutil.utils.lister.Lister._print_results')
    @patch('ocutil.utils.lister.Lister.object_storage')
    def test_lister_api_prefix_adjustment(self, mock_object_storage, mock_print):
        """Test Lister adjusts prefix for API call when non-recursive and no trailing slash."""
        mock_object_storage.list_objects.return_value = MockListResponse(data=MockListData()) # Return empty is fine

        lister = Lister(MagicMock())
        lister.object_storage = mock_object_storage

        # Call with no trailing slash
        lister.list_path("bucket", "folder", long_format=False, human_readable=False, recursive=False)

        # Assert list_objects was called with the adjusted prefix ending in '/'
        mock_object_storage.list_objects.assert_called_once()
        args, kwargs = mock_object_storage.list_objects.call_args
        self.assertEqual(kwargs.get('prefix'), "folder/")
        self.assertEqual(kwargs.get('delimiter'), "/")


    # ======================================================
    # ============ NEW UploadManager Tests =================
    # ======================================================

    @patch('ocutil.utils.uploader.UploadManager.upload_file', return_value=MagicMock(status=200))
    def test_uploader_single_file_calls_upload_manager(self, mock_upload_mngr_call):
        """Test that upload_single_file uses UploadManager.upload_file"""
        # No need for real OCI connection here
        uploader = Uploader(MagicMock(), dry_run=False)
        # Need to mock os.path.getsize used internally now
        with patch('os.path.getsize', return_value=100):
             success = uploader.upload_single_file(self.single_file_path, "bucket", "object.txt")

        self.assertTrue(success)
        mock_upload_mngr_call.assert_called_once()
        args, kwargs = mock_upload_mngr_call.call_args
        self.assertEqual(kwargs.get('file_path'), self.single_file_path)
        self.assertEqual(kwargs.get('object_name'), "object.txt")


    @patch('ocutil.utils.uploader.UploadManager.upload_file', return_value=MagicMock(status=200))
    def test_uploader_folder_calls_upload_manager(self, mock_upload_mngr_call):
        """Test that upload_folder uses UploadManager via _upload_worker"""
        # No need for real OCI connection here
        uploader = Uploader(MagicMock(), dry_run=False)

        # Call upload_folder which uses ThreadPoolExecutor -> _upload_worker
        # We check if upload_manager.upload_file was called by the worker
        uploader.upload_folder(self.folder_path, "bucket", "remote_prefix", parallel_count=1)

        # Check calls for the files within self.folder_path
        # file1.txt, file2.txt, sub_folder/sub_file.txt
        self.assertEqual(mock_upload_mngr_call.call_count, 3)

        # Verify one of the calls (e.g., file1.txt)
        # Finding the specific call args can be tricky if order isn't guaranteed
        found_file1 = False
        expected_remote1 = "remote_prefix/file1.txt"
        for call in mock_upload_mngr_call.call_args_list:
             args, kwargs = call
             if kwargs.get('file_path') == self.file1_path and kwargs.get('object_name') == expected_remote1:
                  found_file1 = True
                  break
        self.assertTrue(found_file1, f"Call to upload_file for {self.file1_path} not found")


# --- Main execution ---
if __name__ == "__main__":
    # Add handler to root logger to see output from tested modules if needed
    # logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    unittest.main()