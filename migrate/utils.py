"""Utility functions for Solr to OpenSearch migration."""
import hashlib
import json
import os


def get_hash(d):
    """Generate hash from dictionary for unique naming."""
    json_str = json.dumps(d, sort_keys=True, ensure_ascii=True)
    hash_obj = hashlib.sha256(json_str.encode("utf-8"), usedforsecurity=False)
    return str(int(hash_obj.hexdigest(), 16) % (10 ** 8))


def read_json_file_data(file):
    """Read JSON data from file, return empty dict if file doesn't exist."""
    if os.path.exists(file):
        with open(file, encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = {}
    return data


def write_json_file_data(data, file_name):
    """Write JSON data to file with formatting."""
    with open(file_name, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, sort_keys=True)
