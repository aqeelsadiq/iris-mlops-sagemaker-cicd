import argparse
import json
import io
import boto3
import numpy as np
import pandas as pd


FEATURES = ["sepal_length", "sepal_width", "petal_length", "petal_width"]
COLS = FEATURES + ["species"]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--region", required=True)
    p.add_argument("--train-data-s3-uri", required=True)
    p.add_argument("--baseline-s3-uri", required=True)
    return p.parse_args()


def split_s3_uri(uri: str):
    if not uri.startswith("s3://"):
        raise ValueError(f"Not an S3 URI: {uri}")
    no = uri[5:]
    bucket, key = no.split("/", 1)
    return bucket, key


def compute_hist_bins(x: np.ndarray, bins=20):
    # stable bins; avoid crazy outliers
    lo = float(np.nanpercentile(x, 1))
    hi = float(np.nanpercentile(x, 99))
    if hi <= lo:
        hi = lo + 1.0
    edges = np.linspace(lo, hi, bins + 1).tolist()
    return edges


def main():
    args = parse_args()
    s3 = boto3.client("s3", region_name=args.region)

    in_bucket, in_key = split_s3_uri(args.train_data_s3_uri)
    obj = s3.get_object(Bucket=in_bucket, Key=in_key)
    body = obj["Body"].read()

    # UCI Iris: no header
    df = pd.read_csv(io.BytesIO(body), header=None, names=COLS).dropna()
    if df.empty:
        raise ValueError("Training dataset is empty after dropna().")

    baseline = {"features": {}, "schema": {"features": FEATURES, "format": "csv"}}

    for f in FEATURES:
        x = pd.to_numeric(df[f], errors="coerce").dropna().to_numpy()
        baseline["features"][f] = {
            "mean": float(np.mean(x)),
            "std": float(np.std(x) + 1e-9),
            "min": float(np.min(x)),
            "max": float(np.max(x)),
            "bins": compute_hist_bins(x, bins=20),
        }

    out_bucket, out_key = split_s3_uri(args.baseline_s3_uri)
    s3.put_object(
        Bucket=out_bucket,
        Key=out_key,
        Body=json.dumps(baseline, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    print("✅ Baseline written to:", args.baseline_s3_uri)
    print("Features:", FEATURES)


if __name__ == "__main__":
    main()