# ocutil

**ocutil** is a simple cli for interacting with Oracle Cloud Storagein a similar way to `gsutil`.ly, enhancing speed and efficiency.

## Installation
(of course that you need to setup OCI CLI by yourself)

    pip install git+https://github.com/shainisan/ocutil.git


### **Commands**

#### 1. Uploading
File:

`ocutil /path/to/local/file.txt oc://bucket-name/path/to/destination/file.txt`

Folder:

`ocutil /path/to/local/folder oc://bucket-name/path/to/destination/`

#### 2. Downloading
File:

`ocutil oc://bucket-name/path/to/source/file.txt /path/to/local/destination/`

Folder:

`ocutil oc://bucket-name/path/to/source/ /path/to/local/destination/`

