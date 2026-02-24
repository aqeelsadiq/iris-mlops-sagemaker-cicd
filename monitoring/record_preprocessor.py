# monitoring/record_preprocessor.py
import os
import json
import glob

INPUT_DIR = "/opt/ml/processing/input"
OUTPUT_DIR = "/opt/ml/processing/output"

# Your baseline column order (must match baseline/features.csv)
HEADER = ["sepal_length", "sepal_width", "petal_length", "petal_width"]


def extract_features_from_capture(record: dict):
    """
    DataCapture JSON structure usually contains endpointInput / endpointOutput.
    We only need endpointInput (the request sent to the endpoint).
    """
    cap = record.get("captureData", {})

    ep_in = cap.get("endpointInput", {})
    data = ep_in.get("data", None)
    content_type = ep_in.get("contentType", "")

    if data is None:
        return None

    # If request was JSON, it may look like: {"instances":[[...]]} or [[...]]
    if "application/json" in content_type:
        try:
            obj = json.loads(data)
            if isinstance(obj, dict) and "instances" in obj:
                row = obj["instances"][0]
            elif isinstance(obj, list):
                row = obj[0] if len(obj) > 0 and isinstance(obj[0], list) else obj
            else:
                return None
            return [float(x) for x in row[:4]]
        except Exception:
            return None

    # If request was CSV: "5.1,3.5,1.4,0.2"
    if "text/csv" in content_type or "csv" in content_type:
        try:
            parts = [p.strip() for p in data.strip().split(",")]
            return [float(x) for x in parts[:4]]
        except Exception:
            return None

    return None


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, "preprocessed.csv")

    # Collect all captured files downloaded by Model Monitor
    files = glob.glob(os.path.join(INPUT_DIR, "**", "*"), recursive=True)
    files = [f for f in files if os.path.isfile(f)]

    rows = []
    for f in files:
        # DataCapture files are JSON lines
        try:
            with open(f, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    rec = json.loads(line)
                    feats = extract_features_from_capture(rec)
                    if feats is not None and len(feats) == 4:
                        rows.append(feats)
        except Exception:
            # ignore non-json capture artifacts
            continue

    # Write CSV with HEADER (because your baseline is header=True)
    with open(out_path, "w", encoding="utf-8") as out:
        out.write(",".join(HEADER) + "\n")
        for r in rows:
            out.write(",".join(str(x) for x in r) + "\n")

    print(f"✅ Wrote {len(rows)} rows to {out_path}")


if __name__ == "__main__":
    main()