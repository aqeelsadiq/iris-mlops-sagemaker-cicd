# monitoring/record_preprocessor.py
"""
SageMaker Model Monitor record preprocessor.

Goal:
- Always output the expected 4 feature columns so Model Monitor doesn't see "0 columns".
- Robustly parse common CSV/JSON request shapes.
- If a record is unparseable, emit NaNs (instead of skipping) to keep schema consistent.

Expected output columns (baseline constraints should match):
  sepal_length, sepal_width, petal_length, petal_width
"""

import json
import base64
import math
import re

FEATURES = ["sepal_length", "sepal_width", "petal_length", "petal_width"]


def _get_attr(obj, *names, default=None):
    """Safely read an attribute from obj using multiple possible names."""
    for n in names:
        if hasattr(obj, n):
            v = getattr(obj, n)
            if v is not None:
                return v
    return default


def _nan_row():
    """Return a row with the correct columns but NaN values."""
    return {f: math.nan for f in FEATURES}


def _as_float4(x):
    """Convert list/tuple to 4 floats."""
    if not isinstance(x, (list, tuple)) or len(x) < 4:
        return None
    try:
        return [float(x[0]), float(x[1]), float(x[2]), float(x[3])]
    except Exception:
        return None


def _parse_json_any(obj):
    """
    Accept common JSON shapes:
      - {"instances":[[...]]}
      - {"instances":[...]}
      - {"inputs":[...]} / {"features":[...]} / {"data":[...]}
      - {"sepal_length":5.1, "sepal_width":3.5, ...}
      - [[...]] / [...]
    """
    if isinstance(obj, dict):
        # Direct feature dict
        if all(k in obj for k in FEATURES):
            try:
                return [float(obj[f]) for f in FEATURES]
            except Exception:
                return None

        # Wrapper keys
        for key in ["instances", "inputs", "features", "data"]:
            if key in obj:
                v = obj[key]
                if isinstance(v, list) and v:
                    if isinstance(v[0], list):
                        return _as_float4(v[0])
                    return _as_float4(v)

        return None

    if isinstance(obj, list):
        if obj and isinstance(obj[0], list):
            return _as_float4(obj[0])
        return _as_float4(obj)

    return None


def _parse_csv(s: str):
    """
    Parse CSV (or whitespace-separated) numeric payloads.
    - tolerates newlines (takes the last non-empty line)
    - tolerates header lines (header won't parse to float, so it will fall through)
    - splits on commas OR whitespace
    """
    s = (s or "").strip()
    if not s:
        return None

    lines = [ln.strip() for ln in s.splitlines() if ln.strip()]
    if not lines:
        return None

    # If the request accidentally includes header + data, last line is typically the data row.
    last = lines[-1]

    # Split on comma or whitespace (handles: "1,2,3,4" or "1 2 3 4")
    parts = [p.strip() for p in re.split(r"[,\s]+", last) if p.strip()]
    if len(parts) < 4:
        return None

    try:
        return [float(parts[0]), float(parts[1]), float(parts[2]), float(parts[3])]
    except Exception:
        return None


def preprocess_handler(inference_record, logger=None):
    """
    REQUIRED by SageMaker Model Monitor.
    Must return:
      - dict representing one row, OR
      - [] to skip.

    We intentionally DO NOT skip unparseable records. We emit NaNs to keep schema stable
    and avoid "Number of columns in current dataset: 0".
    """
    # Read endpoint input record
    try:
        ep_in = inference_record.endpoint_input
    except Exception as e:
        if logger:
            logger.warning(f"Cannot read endpoint_input: {e}")
        return _nan_row()

    data = _get_attr(ep_in, "data")
    if data is None:
        return _nan_row()

    # Content-Type: in capture records this often appears as observedContentType
    content_type = (
        _get_attr(ep_in, "content_type", "observed_content_type", "observedContentType", default="") or ""
    ).lower()

    # Encoding: may be BASE64 for captured payloads
    encoding = (_get_attr(ep_in, "encoding", default="") or "").upper()

    # bytes -> str
    if isinstance(data, (bytes, bytearray)):
        data = data.decode("utf-8", errors="ignore")

    # BASE64 decode if needed
    if encoding == "BASE64" and isinstance(data, str):
        try:
            data = base64.b64decode(data).decode("utf-8", errors="ignore")
        except Exception as e:
            if logger:
                logger.warning(f"BASE64 decode failed: {e}")
            return _nan_row()

    values = None

    # Parse JSON
    if "json" in content_type:
        try:
            obj = json.loads(data) if isinstance(data, str) else data
        except Exception:
            obj = data  # if already dict/list
        values = _parse_json_any(obj)
    else:
        # Parse CSV
        if isinstance(data, str):
            values = _parse_csv(data)

    # If unparseable: emit NaNs (do NOT skip)
    if values is None:
        if logger:
            logger.warning(
                f"Unparseable record -> emitting NaNs. ct={content_type} enc={encoding} sample={str(data)[:200]}"
            )
        return _nan_row()

    # Build stable output schema
    return {
        FEATURES[0]: values[0],
        FEATURES[1]: values[1],
        FEATURES[2]: values[2],
        FEATURES[3]: values[3],
    }