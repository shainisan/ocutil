# utils/oci_manager.py

import oci

class OCIManager:
    def __init__(self, config_profile='DEFAULT'):
        self.config_profile = config_profile
        self.config = self.load_config()
        self.object_storage = self.initialize_object_storage_client()
        self.namespace = self.get_namespace()

    def load_config(self):
        try:
            config = oci.config.from_file("~/.oci/config", self.config_profile)
            return config
        except Exception as e:
            raise Exception(f"Error loading OCI config: {e}")

    def initialize_object_storage_client(self):
        try:
            object_storage = oci.object_storage.ObjectStorageClient(self.config)
            return object_storage
        except Exception as e:
            raise Exception(f"Error initializing Object Storage Client: {e}")

    def get_namespace(self):
        try:
            namespace = self.object_storage.get_namespace().data
            return namespace
        except Exception as e:
            raise Exception(f"Error retrieving namespace: {e}")
