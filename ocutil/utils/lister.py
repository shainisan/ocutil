# ocutil/utils/lister.py
import logging
import datetime
import sys
import textwrap # For potential future use if needed

import oci
from oci.object_storage.models import ObjectSummary

# Assuming formatters.py exists in the same directory
from .formatters import human_readable_size
from .oci_manager import OCIManager

logger = logging.getLogger('ocutil.lister')

class Lister:
    """Handles listing objects in OCI Object Storage."""

    def __init__(self, oci_manager: OCIManager):
        self.oci_manager = oci_manager
        self.object_storage = oci_manager.object_storage
        self.namespace = oci_manager.namespace
        logger.debug("Lister initialized.")

    def list_path(self, bucket_name: str, prefix: str, long_format: bool, human_readable: bool, recursive: bool):
        """
        Lists objects and prefixes for a given path and prints the results to stdout.
        Ensures non-recursive listings use a trailing slash in the API call prefix.
        Logs operational messages to stderr (via logger).
        """
        logger.debug(f"Attempting to list objects for user path: oc://{bucket_name}/{prefix}")

        # Determine the prefix to use for the API call.
        # For non-recursive, ensure it ends with '/' unless it's empty (bucket root).
        api_prefix = prefix
        if not recursive and api_prefix and not api_prefix.endswith('/'):
            api_prefix += '/'
            logger.debug(f"Adjusted prefix for API call to: {api_prefix}")

        all_objects = []
        all_prefixes = set()
        start_token = None
        start_after = None

        list_params = {
            "namespace_name": self.namespace,
            "bucket_name": bucket_name,
            "prefix": api_prefix, # Use the adjusted prefix for the API call
            "limit": 1000
        }

        fields = "name"
        if long_format or human_readable:
            fields += ",size,timeModified"
        list_params["fields"] = fields

        if not recursive:
            list_params["delimiter"] = '/'
            pagination_key = 'next_start_with'
            pagination_param = 'start'
        else:
            # For recursive, remove delimiter if it was added implicitly
            list_params.pop("delimiter", None)
            pagination_key = 'next_start_after'
            pagination_param = 'start_after'

        logger.debug(f"Listing parameters for API call: {list_params}")
        page = 1
        found_anything = False
        while True:
            try:
                current_page_token = start_token if not recursive else start_after
                logger.debug(f"Requesting object list page {page} ({pagination_param}={current_page_token})...")
                # Update the correct pagination parameter for the request
                if current_page_token:
                    list_params[pagination_param] = current_page_token
                else:
                    # Remove pagination param if None to avoid sending empty value
                    list_params.pop(pagination_param, None)


                response = self.object_storage.list_objects(**list_params)

                page_objects = response.data.objects or []
                page_prefixes = response.data.prefixes or [] if not recursive else []

                if page_objects or page_prefixes:
                    found_anything = True

                next_page_token = getattr(response.data, pagination_key, None)

                all_objects.extend(page_objects)
                if page_prefixes:
                    all_prefixes.update(page_prefixes)
                logger.debug(f"Page {page} returned {len(page_objects)} objects and {len(page_prefixes)} prefixes.")

                if next_page_token:
                    if not recursive:
                        start_token = next_page_token
                        start_after = None
                    else:
                        if page_objects:
                            start_after = page_objects[-1].name
                            start_token = None
                        else:
                            logger.warning("Pagination token present, but no objects returned. Stopping.")
                            break
                    page += 1
                else:
                    logger.debug("No more pages found.")
                    break

            except oci.exceptions.ServiceError as e:
                if e.status == 404 and not found_anything:
                    logger.error(f"Error: No objects found at '{'oc://' + bucket_name + '/' + prefix}'")
                    break
                elif e.status == 404 and ('BucketNotFound' in str(e.code) or 'NamespaceNotFound' in str(e.code)):
                    logger.error(f"Error: Bucket or Namespace not found: '{bucket_name}'")
                    sys.exit(1)
                else:
                    logger.error(f"Error: Failed to list objects: {e.status} - {e.message}")
                    logger.debug(f"Error details: {e}")
                    sys.exit(1)
            except Exception as e:
                logger.error(f"Error: An unexpected error occurred during listing: {e}", exc_info=True)
                sys.exit(1)

        # --- Print Results ---
        if found_anything or recursive:
            # Pass the ORIGINAL requested prefix for calculating relative paths for display
            self._print_results(all_objects, list(all_prefixes), bucket_name, prefix, long_format, human_readable, recursive)
            logger.debug(f"Found {len(all_objects)} objects" + (f" and {len(all_prefixes)} prefixes." if not recursive else "."))
        # No need for an else here, the 404 handler or lack of items printed covers it

    def _print_results(self, objects: list[ObjectSummary], prefixes: list[str], bucket_name: str, requested_prefix: str, long_format: bool, human_readable: bool, recursive: bool):
        """Formats and prints the listing results to stdout."""

        items_to_display = []
        processed_relative_paths = set() # Avoid duplicates after processing

        # --- Determine how much of the prefix needs to be stripped ---
        # If request was 'folder', strip 'folder/' (len+1) from results like 'folder/file.txt'
        # If request was 'folder/', strip 'folder/' (len) from results like 'folder/file.txt'
        prefix_to_strip = requested_prefix
        if not recursive and requested_prefix and not requested_prefix.endswith('/'):
            # Ensure we strip including the slash for requests like 'folder'
            prefix_to_strip += '/'
        strip_len = len(prefix_to_strip)
        # ---

        # Process Prefixes (Directories) if not recursive
        if not recursive:
            sorted_prefixes = sorted(prefixes)
            for pfx in sorted_prefixes:
                # Skip if the prefix is exactly the one we are listing (self-listing)
                if pfx == prefix_to_strip:
                     continue

                # Calculate relative path
                relative_path = pfx[strip_len:] if pfx.startswith(prefix_to_strip) else pfx
                if not relative_path: continue # Skip empty results

                # Ensure trailing slash for display, but only one
                relative_path = relative_path.rstrip('/') + '/'

                # Avoid adding if already processed (e.g., via object listing edge cases)
                if relative_path not in processed_relative_paths:
                    items_to_display.append({'name': relative_path, 'type': 'DIR', 'size': None, 'time_modified': None})
                    processed_relative_paths.add(relative_path)

        # Process Objects
        objects.sort(key=lambda o: o.name) # Sort objects by name
        for obj in objects:
            # Skip the object that represents the directory itself (e.g. object named 'folder' when listing 'folder/')
            if not recursive and obj.name == requested_prefix.rstrip('/'):
                continue

            # Calculate relative path
            relative_path = obj.name[strip_len:] if obj.name.startswith(prefix_to_strip) else obj.name
            if not relative_path: continue # Skip empty results

            # Avoid adding duplicates
            if relative_path not in processed_relative_paths:
                items_to_display.append({'name': relative_path, 'type': 'FILE', 'size': obj.size, 'time_modified': obj.time_modified})
                processed_relative_paths.add(relative_path)

        # --- Print to stdout ---
        if not items_to_display:
             logger.debug("No items to display after filtering.")
             return

        # Calculate alignment width for size column if needed
        max_size_width = 10 # Default/minimum width
        if long_format:
             try:
                  # Get formatted sizes only for files to determine max width
                  formatted_sizes = [
                       human_readable_size(item['size']) if human_readable else str(item['size'] or 0)
                       for item in items_to_display if item['type'] == 'FILE'
                  ]
                  if formatted_sizes:
                       max_size_width = max(len(s) for s in formatted_sizes)
                       max_size_width = max(max_size_width, 10) # Enforce minimum width
             except ValueError:
                  pass # No files, keep default width

        # Sort final list for consistent output order (Dirs first, then Files, then alphabetically)
        items_to_display.sort(key=lambda x: (x['type'] == 'FILE', x['name']))

        for item in items_to_display:
            if long_format:
                if item['type'] == 'DIR':
                    # Use consistent padding, mark as DIR
                    print(f"{' ':>{max_size_width}} {' ':19} {'<DIR>':>7} {item['name']}")
                else: # FILE
                    size_str = human_readable_size(item['size']) if human_readable else str(item['size'] or 0)
                    mod_time = item['time_modified']
                    time_str = mod_time.strftime('%Y-%m-%d %H:%M:%S') if isinstance(mod_time, datetime.datetime) else "N/A"
                    # Right align size, use calculated width
                    print(f"{size_str:>{max_size_width}} {time_str:19} {' ':>7} {item['name']}")
            else: # Simple format
                # Name for DIR already includes trailing slash from processing above
                print(item['name'])