import os
import json
import boto3
import math

s3 = boto3.client("s3")
cw = boto3.client("cloudwatch")

BASELINE_S3_URI = os.environ["BASELINE_S3_URI"]
METRIC_NAMESPACE = os.environ.get("METRIC_NAMESPACE", "Iris/Drift")

FEATURES = ["sepal_length", "sepal_width", "petal_length", "petal_width"]


def split_s3_uri(uri: str):
    no = uri.replace("s3://", "", 1)
    b, k = no.split("/", 1)
    return b, k


def load_baseline():
    b, k = split_s3_uri(BASELINE_S3_URI)
    obj = s3.get_object(Bucket=b, Key=k)
    return json.loads(obj["Body"].read().decode("utf-8"))


def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None


def psi(expected_counts, actual_counts, eps=1e-6):
    # PSI = sum((a - e)*ln(a/e))
    res = 0.0
    for e, a in zip(expected_counts, actual_counts):
        e = max(e, eps)
        a = max(a, eps)
        res += (a - e) * math.log(a / e)
    return float(res)


def hist_counts(values, edges):
    counts = [0] * (len(edges) - 1)
    for v in values:
        # last bin inclusive
        for i in range(len(edges) - 1):
            lo, hi = edges[i], edges[i + 1]
            if (i < len(edges) - 2 and lo <= v < hi) or (i == len(edges) - 2 and lo <= v <= hi):
                counts[i] += 1
                break
    total = sum(counts) or 1
    return [c / total for c in counts]


def extract_records_from_jsonl(raw: str):
    # SageMaker data capture JSONL: each line is JSON with endpointInput.data etc
    rows = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        evt = json.loads(line)

        # input can be CSV string like "5.1,3.5,1.4,0.2" OR JSON
        inp = evt.get("endpointInput", {})
        data = inp.get("data")

        if data is None:
            continue

        # CSV
        if isinstance(data, str) and "," in data:
            parts = [p.strip() for p in data.split(",")]
            if len(parts) >= 4:
                vals = [safe_float(parts[i]) for i in range(4)]
                if all(v is not None for v in vals):
                    rows.append(vals)
            continue

        # JSON (sometimes base64; ignore here)
        if isinstance(data, dict):
            # expecting {"instances":[{...}]}
            inst = data.get("instances") or data.get("inputs") or []
            for r in inst:
                vals = [safe_float(r.get(f)) for f in FEATURES]
                if all(v is not None for v in vals):
                    rows.append(vals)

    return rows


def put_metric(feature_name, value, endpoint_name):
    cw.put_metric_data(
        Namespace=METRIC_NAMESPACE,
        MetricData=[
            {
                "MetricName": "PSI",
                "Dimensions": [
                    {"Name": "Feature", "Value": feature_name},
                    {"Name": "Endpoint", "Value": endpoint_name},
                ],
                "Value": float(value),
                "Unit": "None",
            }
        ],
    )


def lambda_handler(event, context):
    baseline = load_baseline()

    # S3 trigger event
    rec = event["Records"][0]
    bucket = rec["s3"]["bucket"]["name"]
    key = rec["s3"]["object"]["key"]

    endpoint_name = os.environ.get("ENDPOINT_NAME", "unknown-endpoint")

    obj = s3.get_object(Bucket=bucket, Key=key)
    raw = obj["Body"].read().decode("utf-8", errors="ignore")

    rows = extract_records_from_jsonl(raw)
    if not rows:
        return {"ok": True, "msg": "No valid rows in capture file", "key": key}

    # transpose values per feature
    cols = list(zip(*rows))  # 4 lists

    results = {}
    for i, f in enumerate(FEATURES):
        edges = baseline["features"][f]["bins"]

        # expected distribution from baseline: build from baseline stats using baseline CSV is better,
        # but we stored only bins. We approximate expected by assuming baseline counts uniform across bins
        # OR you can extend baseline_build.py to also store baseline bin frequencies.
        # For production, store baseline bin frequencies.
        # Here we compute baseline frequencies by using a normal approx using mean/std.
        # Keep it simple: we’ll use uniform expected.
        expected = [1.0 / (len(edges) - 1)] * (len(edges) - 1)

        actual = hist_counts([float(v) for v in cols[i]], edges)
        val = psi(expected, actual)

        results[f] = val
        put_metric(f, val, endpoint_name)

    return {"ok": True, "key": key, "psi": results}