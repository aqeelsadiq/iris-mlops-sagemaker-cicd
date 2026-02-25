# import argparse
# import boto3
# import sagemaker
# from sagemaker.model_monitor import DefaultModelMonitor
# from sagemaker.s3 import S3Uploader


# def parse_args():
#     p = argparse.ArgumentParser()
#     p.add_argument("--region", required=True)
#     p.add_argument("--role-arn", required=True)

#     p.add_argument("--baseline-data-s3-uri", required=True)     # e.g. s3://bucket/datasets/iris/features.csv
#     p.add_argument("--baseline-output-s3-uri", required=True)   # e.g. s3://bucket/monitoring/baseline/

#     p.add_argument("--instance-type", default="ml.t3.medium")
#     p.add_argument("--instance-count", type=int, default=1)

#     # schema is optional, but helps Model Monitor know column names/types
#     p.add_argument("--include-schema", action="store_true", help="Upload a simple dataset_format.json schema")
#     return p.parse_args()


# def main():
#     args = parse_args()

#     boto_sess = boto3.Session(region_name=args.region)
#     sm_sess = sagemaker.Session(boto_session=boto_sess)

#     monitor = DefaultModelMonitor(
#         role=args.role_arn,
#         instance_count=args.instance_count,
#         instance_type=args.instance_type,
#         sagemaker_session=sm_sess,
#         volume_size_in_gb=30,
#         max_runtime_in_seconds=3600,
#     )

#     # Optional: dataset_format.json so columns are named and consistent
#     dataset_format_s3_uri = None
#     if args.include_schema:
#         schema = {
#             "version": 1.0,
#             "datasetFormat": {"csv": {"header": True}},
#             "columns": [
#                 {"name": "sepal_length", "type": "float"},
#                 {"name": "sepal_width", "type": "float"},
#                 {"name": "petal_length", "type": "float"},
#                 {"name": "petal_width", "type": "float"},
#             ],
#         }
#         tmp_path = "/tmp/dataset_format.json"
#         import json
#         with open(tmp_path, "w") as f:
#             json.dump(schema, f)

#         dataset_format_s3_uri = S3Uploader.upload(
#             local_path=tmp_path,
#             desired_s3_uri=args.baseline_output_s3_uri.rstrip("/") + "/schema",
#             sagemaker_session=sm_sess,
#         )
#         print("✅ Uploaded dataset_format.json:", dataset_format_s3_uri)

#     print("▶ Creating baseline...")
#     monitor.suggest_baseline(
#         baseline_dataset=args.baseline_data_s3_uri,
#         dataset_format={"csv": {"header": True}},
#         output_s3_uri=args.baseline_output_s3_uri,
#         wait=True,
#         logs=True,
#         # If schema uploaded, pass it
#         record_preprocessor_script=None,
#         post_analytics_processor_script=None,
#     )

#     # Baseline files are written inside output_s3_uri (usually directly under it)
#     print("✅ Baseline created.")
#     print("Baseline output:", args.baseline_output_s3_uri)
#     print("Expected files: statistics.json, constraints.json")


# if __name__ == "__main__":
#     main()




#claude code
"""
mm_create_baseline.py
---------------------
Creates a Data Quality baseline using SageMaker Model Monitor.

KEY FIX: The same record_preprocessor.py used by the monitoring schedule
MUST also be used here so that baseline statistics/constraints are computed
with the same column schema as live captured data.

Usage:
    python monitoring/mm_create_baseline.py \
        --region us-east-1 \
        --role-arn arn:aws:iam::123456789012:role/SageMakerRole \
        --baseline-data-s3-uri s3://my-bucket/datasets/iris/features.csv \
        --baseline-output-s3-uri s3://my-bucket/monitoring/baseline/ \
        --preprocessor-local-path ./monitoring/record_preprocessor.py \
        --preprocessor-s3-uri s3://my-bucket/monitoring/scripts/
"""

import argparse
import json
import os
import boto3
import sagemaker
from sagemaker.model_monitor import DefaultModelMonitor
from sagemaker.s3 import S3Uploader


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--region", required=True)
    p.add_argument("--role-arn", required=True)

    p.add_argument("--baseline-data-s3-uri", required=True,
                   help="S3 URI of features.csv (with header) for baseline")
    p.add_argument("--baseline-output-s3-uri", required=True,
                   help="S3 URI prefix where statistics.json & constraints.json will be written")

    # ── Preprocessor (REQUIRED — must match the schedule) ────────────────────
    p.add_argument("--preprocessor-local-path",
                   default="./monitoring/record_preprocessor.py",
                   help="Local path to record_preprocessor.py")
    p.add_argument("--preprocessor-s3-uri", required=True,
                   help="S3 URI prefix where the preprocessor script will be uploaded")

    p.add_argument("--instance-type", default="ml.m5.large")
    p.add_argument("--instance-count", type=int, default=1)
    return p.parse_args()


def upload_preprocessor(local_path: str, s3_uri_prefix: str, sm_sess) -> str:
    """Upload the preprocessor script to S3 and return its S3 URI."""
    if not os.path.isfile(local_path):
        raise FileNotFoundError(
            f"Preprocessor script not found: {local_path}\n"
            "Make sure you run this from the project root directory."
        )
    s3_uri = S3Uploader.upload(
        local_path=local_path,
        desired_s3_uri=s3_uri_prefix.rstrip("/"),
        sagemaker_session=sm_sess,
    )
    print(f"✅ Uploaded preprocessor to: {s3_uri}")
    return s3_uri


def main():
    args = parse_args()

    boto_sess = boto3.Session(region_name=args.region)
    sm_sess = sagemaker.Session(boto_session=boto_sess)

    # ── 1. Upload the preprocessor script to S3 ───────────────────────────────
    preprocessor_s3_uri = upload_preprocessor(
        local_path=args.preprocessor_local_path,
        s3_uri_prefix=args.preprocessor_s3_uri,
        sm_sess=sm_sess,
    )

    # ── 2. Create the monitor instance ────────────────────────────────────────
    monitor = DefaultModelMonitor(
        role=args.role_arn,
        instance_count=args.instance_count,
        instance_type=args.instance_type,
        sagemaker_session=sm_sess,
        volume_size_in_gb=30,
        max_runtime_in_seconds=3600,
    )

    # ── 3. Run baseline job ───────────────────────────────────────────────────
    print("\n▶ Creating baseline (this may take 5–10 minutes)...")
    print(f"  Input data  : {args.baseline_data_s3_uri}")
    print(f"  Output      : {args.baseline_output_s3_uri}")
    print(f"  Preprocessor: {preprocessor_s3_uri}")

    monitor.suggest_baseline(
        baseline_dataset=args.baseline_data_s3_uri,
        dataset_format={"csv": {"header": True}},
        output_s3_uri=args.baseline_output_s3_uri,
        # ✅ CRITICAL FIX: use the same preprocessor as the monitoring schedule
        record_preprocessor_script=preprocessor_s3_uri,
        post_analytics_processor_script=None,
        wait=True,
        logs=True,
    )

    print("\n✅ Baseline job complete.")
    print(f"   statistics.json  → {args.baseline_output_s3_uri.rstrip('/')}/statistics.json")
    print(f"   constraints.json → {args.baseline_output_s3_uri.rstrip('/')}/constraints.json")
    print("\nNext step: run mm_create_schedule.py")


if __name__ == "__main__":
    main()