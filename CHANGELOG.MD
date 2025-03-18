**V2**
=======================

**Changelog Summary:**

- **CLI Enhancements:** Added (optional) `--parallel`, `--dry-run`, `--log-file`, and `--verbose` options for configurable concurrency, simulation mode, and improved logging.
- **Error Handling & Retry:** Introduced exponential backoff retry logic for uploads and downloads.
- **UI Improvements:** Replaced tqdm progress bars with Rich's aggregated progress displays and added final summary reports.
- **Bulk Download Fix:** Implemented a helper to avoid multiple live progress displays during concurrent downloads.
- **Test Updates:** Adjusted tests to patch correct methods and reflect new features.

**Example usage:**
```ocutil /path/to/local/folder oc://mybucket/path --dry-run --parallel 4 --verbose --log-file upload.log```

