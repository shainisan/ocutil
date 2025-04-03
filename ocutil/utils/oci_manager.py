# oci_manager.py
import os
import oci
import getpass # Import the getpass module
import logging # Import logging

# Get a logger instance (optional but good practice)
logger = logging.getLogger(__name__)

class OCIManager:
    def __init__(self, config_profile='DEFAULT'):
        self.config_profile = config_profile
        self.config = self.load_config()
        # We initialize the client *after* potentially getting the passphrase
        self.object_storage = self.initialize_object_storage_client()
        # Getting namespace requires an initialized client
        if self.object_storage:
             self.namespace = self.get_namespace()
        else:
             # Handle case where client initialization failed even after prompt
             raise Exception("Failed to initialize Object Storage Client.")


    def load_config(self):
        """Loads the OCI configuration, expanding the user's home directory."""
        try:
            config_path = os.path.expanduser("~/.oci/config")
            # Check if config file exists before trying to load
            if not os.path.exists(config_path):
                 raise FileNotFoundError(f"OCI config file not found at: {config_path}")
            
            # Load the configuration profile
            config = oci.config.from_file(config_path, self.config_profile)
            
            # Expand the key_file path if present
            if 'key_file' in config:
                config['key_file'] = os.path.expanduser(config['key_file'])
                if not os.path.exists(config['key_file']):
                     raise FileNotFoundError(f"OCI key file specified in profile '{self.config_profile}' not found at: {config['key_file']}")

            logger.debug(f"Loaded OCI config for profile '{self.config_profile}' from '{config_path}'.")
            return config
        except oci.exceptions.ConfigFileNotFound as e:
             raise Exception(f"Error loading OCI config: OCI config file not found ({e})")
        except oci.exceptions.MissingConfigValue as e:
             raise Exception(f"Error loading OCI config: Missing value in profile '{self.config_profile}' ({e})")
        except FileNotFoundError as e: # Catch explicit FileNotFoundError
             raise Exception(f"Error loading OCI config: {e}")
        except Exception as e:
            # Catch any other general exceptions during config loading
            raise Exception(f"An unexpected error occurred loading OCI config: {e}")

    def initialize_object_storage_client(self):
        """
        Initializes the ObjectStorageClient.
        If initialization fails due to a missing passphrase, prompts the user for it.
        """
        try:
            # First attempt to initialize with the loaded config
            logger.debug("Attempting to initialize Object Storage Client...")
            client = oci.object_storage.ObjectStorageClient(self.config)
            logger.debug("Object Storage Client initialized successfully.")
            return client
        except Exception as e:
            # Check if the error message indicates a required passphrase
            # The exact error message might vary slightly depending on OCI SDK version
            # or underlying crypto libraries, so checking for keywords is safer.
            error_str = str(e).lower()
            if "pass phrase" in error_str or "passphrase" in error_str or "password" in error_str:
                logger.warning(f"Key file '{self.config.get('key_file', 'N/A')}' requires a passphrase, but none was found in the config profile '{self.config_profile}'.")
                try:
                    # Prompt securely for the passphrase
                    passphrase = getpass.getpass(f"Enter passphrase for OCI profile '{self.config_profile}': ")
                    
                    # Add the obtained passphrase to the config dictionary
                    # The OCI SDK expects the key 'pass_phrase'
                    self.config['pass_phrase'] = passphrase
                    
                    # Retry initializing the client with the updated config
                    logger.info("Retrying Object Storage Client initialization with provided passphrase...")
                    client = oci.object_storage.ObjectStorageClient(self.config)
                    logger.debug("Object Storage Client initialized successfully after passphrase entry.")
                    return client
                except Exception as retry_e:
                    # Handle errors during the retry (e.g., wrong passphrase entered)
                    raise Exception(f"Error initializing Object Storage Client after passphrase entry: {retry_e}")
            else:
                # If the error is not about a passphrase, re-raise it
                raise Exception(f"Error initializing Object Storage Client: {e}")

    def get_namespace(self):
        """Retrieves the object storage namespace."""
        if not self.object_storage:
             raise Exception("Cannot get namespace: Object Storage Client is not initialized.")
        try:
            logger.debug("Retrieving Object Storage namespace...")
            namespace = self.object_storage.get_namespace().data
            logger.debug(f"Namespace retrieved: {namespace}")
            return namespace
        except Exception as e:
            raise Exception(f"Error retrieving namespace: {e}")