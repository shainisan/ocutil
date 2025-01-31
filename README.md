# ocutil

**ocutil** is a Python-based command-line utility for interacting with Oracle Cloud Object Storage, offering functionalities similar to `gsutil`. It allows users to upload and download single files or entire directories with ease and efficiency.

## Features

- **Upload Single Files:** Easily upload individual files to a specified OCI bucket and path.
- **Bulk Upload Folders:** Upload entire directories with all nested files and subdirectories using parallel threads for faster performance.
- **Download Single Files:** Download individual files from OCI Object Storage to your local machine.
- **Bulk Download Folders:** Download entire directories from OCI Object Storage, preserving the directory structure.
- **Parallel Operations:** Utilize multiple CPU cores to perform uploads and downloads concurrently, enhancing speed and efficiency.

## Installation

### **Prerequisites**

- **Python 3.6+**
- **pip** (Python package installer)
- **OCI CLI Credentials:** Ensure you have your OCI configuration file set up at `~/.oci/config`.

### **Installation:**

    pip install --upgrade git+https://github.com/shainisan/ocutil.git


### **Commands**

#### 1. Uploading
Upload a Single File:

ocutil /path/to/local/file.txt oc://bucket-name/path/to/destination/file.txt

Upload a Folder:

ocutil /path/to/local/folder oc://bucket-name/path/to/destination/

#### 2. Downloading
Download a Single File:

ocutil oc://bucket-name/path/to/source/file.txt /path/to/local/destination/

Download a Folder:

ocutil oc://bucket-name/path/to/source/ /path/to/local/destination/

