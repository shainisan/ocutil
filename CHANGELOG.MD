**2.0.2**, 09.04.2025
=======================
**Changelog Summary:**
- **New Feature: `ls` Command:** Added the `ls` command to list objects and prefixes within OCI Object Storage (e.g., `ocutil ls oc://bucket/prefix`). Supports standard, long (`-l`), human-readable size (`-lH`), and recursive (`-r`) listing formats. Features user-friendly relative path output with directory indicators.
- **Enhanced Bulk Transfer Robustness:** Significantly improved reliability when copying (`cp`) large numbers of files by refining parallel error handling, ensuring individual file failures don't halt the entire process, and providing clearer aggregate failure reporting in summaries.
- **Improved Upload Performance:** Utilizes OCI SDK's `UploadManager` for `cp` uploads, potentially increasing speed for large files via automatic parallel multipart uploads.
- **Bug Fixes & Refinements:** Fixed inconsistent behavior in `ls` based on trailing slashes in the input path. Corrected underlying pagination parameters (`start` vs `start_with`) and relative path calculations in `ls`. Resolved specific `NameError` and `ValueError` issues. Refactored `main` script to use command subparsers (`cp`, `ls`) and extracted listing logic into a dedicated `Lister` class for better structure.
- **Testing:** Significantly expanded test suite coverage for core transfer logic, error handling, `UploadManager` integration, and the new `ls` command variations.

**2.0.1**, 03.04.2025
=======================
**Changelog Summary:**
- when passphrase isn't in the confic, the user is promped to type it. Better for security.

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

