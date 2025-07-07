import hashlib
import json


def compute_md5_hash(data: dict) -> str:
    try:
        data_str = json.dumps(data, sort_keys=True, default=str)
    except TypeError:
        data_str = json.dumps({k: str(v) for k, v in data.items()}, sort_keys=True)
    return hashlib.md5(data_str.encode('utf-8')).hexdigest()

