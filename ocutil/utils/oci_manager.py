import os
import oci

class OCIManager:
    def __init__(self, config_profile='DEFAULT'):
        self.config_profile = config_profile
        self.config = self.load_config()
        self.object_storage = self.initialize_object_storage_client()
        self.namespace = self.get_namespace()

    def load_config(self):
        try:
            # Expand the home directory if needed.
            config_path = os.path.expanduser("~/.oci/config")
            config = oci.config.from_file(config_path, self.config_profile)
            return config
        except Exception as e:
            raise Exception(f"Error loading OCI config: {e}")

    def initialize_object_storage_client(self):
        try:
            return oci.object_storage.ObjectStorageClient(self.config)
        except Exception as e:
            raise Exception(f"Error initializing Object Storage Client: {e}")

    def get_namespace(self):
        try:
            return self.object_storage.get_namespace().data
        except Exception as e:
            raise Exception(f"Error retrieving namespace: {e}")
