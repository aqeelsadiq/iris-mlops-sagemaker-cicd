# monitoring/record_preprocessor.py
import json

FEATURES = ["sepal_length", "sepal_width", "petal_length", "petal_width"]

def _parse_csv(data: str):
    parts = [p.strip() for p in data.strip().split(",")]
    if len(parts) < 4:
        return None
    try:
        return [float(parts[0]), float(parts[1]), float(parts[2]), float(parts[3])]
    except Exception:
        return None

def _parse_json(data: str):
    try:
        obj = json.loads(data)
    except Exception:
        return None

    row = None
    if isinstance(obj, dict) and "instances" in obj and obj["instances"]:
        row = obj["instances"][0]
    elif isinstance(obj, list):
        row = obj[0] if (obj and isinstance(obj[0], list)) else obj

    if not isinstance(row, list) or len(row) < 4:
        return None

    try:
        return [float(row[0]), float(row[1]), float(row[2]), float(row[3])]
    except Exception:
        return None

def preprocess_handler(inference_record, logger=None):
    """
    REQUIRED by SageMaker Model Monitor.
    Return dict (one row) or [] to skip record.
    """
    try:
        ep_in = inference_record.endpoint_input
        content_type = (ep_in.content_type or "").lower()
        data = ep_in.data
    except Exception as e:
        if logger:
            logger.warning(f"Cannot read endpoint_input: {e}")
        return []

    if data is None:
        return []

    if isinstance(data, (bytes, bytearray)):
        data = data.decode("utf-8", errors="ignore")

    values = _parse_json(data) if "json" in content_type else _parse_csv(data)
    if values is None:
        if logger:
            logger.warning(f"Skipping un-parseable record: ct={content_type}, data={str(data)[:200]}")
        return []

    return {
        FEATURES[0]: values[0],
        FEATURES[1]: values[1],
        FEATURES[2]: values[2],
        FEATURES[3]: values[3],
    }