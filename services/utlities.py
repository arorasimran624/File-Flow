import hashlib
import json

def get_row_hash(row_data: dict) -> str:
    """Generate a hash for a row dictionary."""
    row_str = json.dumps(row_data, sort_keys=True)  # deterministic string
    return hashlib.md5(row_str.encode("utf-8")).hexdigest()
