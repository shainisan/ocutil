# ocutil/utils/formatters.py
import math

def human_readable_size(size_bytes: int | None) -> str:
    """Converts a size in bytes to a human-readable string."""
    if size_bytes is None: # Handle case where size might be None from API
        return "N/A"
    if size_bytes == 0:
        return "0 B" # Add space for alignment
    size_name = ("B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB")
    # Handle potential negative sizes if API ever returns them unexpectedly
    num_bytes = abs(size_bytes)
    i = int(math.floor(math.log(num_bytes, 1024))) if num_bytes > 0 else 0

    # Prevent index errors for extremely large numbers
    if i >= len(size_name):
        i = len(size_name) - 1

    p = math.pow(1024, i)
    s = round(num_bytes / p, 1) # Use 1 decimal place for readability

    # Don't show decimal for bytes or if it's '.0'
    if i == 0 or s == math.floor(s):
        s = int(s)

    return f"{s} {size_name[i]}"