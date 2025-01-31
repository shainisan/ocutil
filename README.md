# ocutil

**ocutil** is a lightweight command-line tool for interacting with Oracle Cloud Object Storage — similar to `gsutil`.

> **Note:** Ensure that you have set up the OCI CLI configuration before using ocutil.

## Installation

Install directly from GitHub:

```bash
pip install git+https://github.com/shainisan/ocutil.git
```

## Usage

The general syntax is:

```bash
ocutil <source> <destination>
```

Where `<source>` and `<destination>` can be either local paths or remote paths using the format `oc://bucket-name/path`.

### Uploading

- **Upload a file:**

  ```bash
  ocutil /path/to/local/file.txt oc://my-bucket/path/to/destination/file.txt
  ```

- **Upload a folder (wrapped as a folder):**

  ```bash
  ocutil /path/to/local/folder oc://my-bucket/path/to/destination/
  ```

- **Upload folder contents without wrapping (using wildcard):**

  ```bash
  ocutil /path/to/local/folder/* oc://my-bucket/path/to/destination/
  ```

### Downloading

- **Download a file:**

  ```bash
  ocutil oc://my-bucket/path/to/source/file.txt /path/to/local/destination/
  ```

- **Download a folder:**

  ```bash
  ocutil oc://my-bucket/path/to/source/ /path/to/local/destination/
  ```

## License

This project is licensed under the MIT License.