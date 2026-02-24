# # monitoring/record_preprocessor.py
# import json

# FEATURES = ["sepal_length", "sepal_width", "petal_length", "petal_width"]

# def _parse_csv(data: str):
#     parts = [p.strip() for p in data.strip().split(",")]
#     if len(parts) < 4:
#         return None
#     try:
#         return [float(parts[0]), float(parts[1]), float(parts[2]), float(parts[3])]
#     except Exception:
#         return None

# def _parse_json(data: str):
#     try:
#         obj = json.loads(data)
#     except Exception:
#         return None

#     row = None
#     if isinstance(obj, dict) and "instances" in obj and obj["instances"]:
#         row = obj["instances"][0]
#     elif isinstance(obj, list):
#         row = obj[0] if (obj and isinstance(obj[0], list)) else obj

#     if not isinstance(row, list) or len(row) < 4:
#         return None

#     try:
#         return [float(row[0]), float(row[1]), float(row[2]), float(row[3])]
#     except Exception:
#         return None

# def preprocess_handler(inference_record, logger=None):
#     """
#     REQUIRED by SageMaker Model Monitor.
#     Return dict (one row) or [] to skip record.
#     """
#     try:
#         ep_in = inference_record.endpoint_input
#         content_type = (ep_in.content_type or "").lower()
#         data = ep_in.data
#     except Exception as e:
#         if logger:
#             logger.warning(f"Cannot read endpoint_input: {e}")
#         return []

#     if data is None:
#         return []

#     if isinstance(data, (bytes, bytearray)):
#         data = data.decode("utf-8", errors="ignore")

#     values = _parse_json(data) if "json" in content_type else _parse_csv(data)
#     if values is None:
#         if logger:
#             logger.warning(f"Skipping un-parseable record: ct={content_type}, data={str(data)[:200]}")
#         return []

#     return {
#         FEATURES[0]: values[0],
#         FEATURES[1]: values[1],
#         FEATURES[2]: values[2],
#         FEATURES[3]: values[3],
#     }




# monitoring/record_preprocessor.py
import json

FEATURES = ["sepal_length", "sepal_width", "petal_length", "petal_width"]

def _as_float4(x):
    if not isinstance(x, (list, tuple)) or len(x) < 4:
        return None
    try:
        return [float(x[0]), float(x[1]), float(x[2]), float(x[3])]
    except Exception:
        return None

def _parse_json_any(obj):
    """
    Accept many common shapes:
      - {"instances":[[...]]}
      - {"instances":[...]}
      - {"inputs":[...]} / {"features":[...]} / {"data":[...]}
      - {"sepal_length":5.1, "sepal_width":3.5, ...}
      - [[...]] / [...]
    """
    # 1) dict with feature names
    if isinstance(obj, dict):
        # direct feature dict
        if all(k in obj for k in FEATURES):
            try:
                return [float(obj[FEATURES[0]]), float(obj[FEATURES[1]]), float(obj[FEATURES[2]]), float(obj[FEATURES[3]])]
            except Exception:
                return None

        # common wrapper keys
        for key in ["instances", "inputs", "features", "data"]:
            if key in obj:
                v = obj[key]
                # instances could be [[...]] or [...]
                if isinstance(v, list) and v:
                    if isinstance(v[0], list):
                        return _as_float4(v[0])
                    return _as_float4(v)

        return None

    # 2) list
    if isinstance(obj, list):
        if obj and isinstance(obj[0], list):
            return _as_float4(obj[0])
        return _as_float4(obj)

    return None

def _parse_csv(s: str):
    parts = [p.strip() for p in s.strip().split(",")]
    if len(parts) < 4:
        return None
    try:
        return [float(parts[0]), float(parts[1]), float(parts[2]), float(parts[3])]
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

    # bytes -> str
    if isinstance(data, (bytes, bytearray)):
        data = data.decode("utf-8", errors="ignore")

    values = None

    # JSON handling (string or already-parsed object)
    if "json" in content_type:
        try:
            obj = json.loads(data) if isinstance(data, str) else data
        except Exception:
            obj = data  # if it's already a dict/list
        values = _parse_json_any(obj)
    else:
        # CSV fallback
        if isinstance(data, str):
            values = _parse_csv(data)

    if values is None:
        if logger:
            logger.warning(f"Skipping record: ct={content_type}, data={str(data)[:200]}")
        return []

    return {
        FEATURES[0]: values[0],
        FEATURES[1]: values[1],
        FEATURES[2]: values[2],
        FEATURES[3]: values[3],
    }