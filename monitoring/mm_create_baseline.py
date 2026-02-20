import argparse
import boto3
import sagemaker
from sagemaker.model_monitor import DefaultModelMonitor
from sagemaker.s3 import S3Uploader


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--region", required=True)
    p.add_argument("--role-arn", required=True)

    p.add_argument("--baseline-data-s3-uri", required=True)     # e.g. s3://bucket/datasets/iris/features.csv
    p.add_argument("--baseline-output-s3-uri", required=True)   # e.g. s3://bucket/monitoring/baseline/

    p.add_argument("--instance-type", default="ml.t3.medium")
    p.add_argument("--instance-count", type=int, default=1)

    # schema is optional, but helps Model Monitor know column names/types
    p.add_argument("--include-schema", action="store_true", help="Upload a simple dataset_format.json schema")
    return p.parse_args()


def main():
    args = parse_args()

    boto_sess = boto3.Session(region_name=args.region)
    sm_sess = sagemaker.Session(boto_session=boto_sess)

    monitor = DefaultModelMonitor(
        role=args.role_arn,
        instance_count=args.instance_count,
        instance_type=args.instance_type,
        sagemaker_session=sm_sess,
        volume_size_in_gb=30,
        max_runtime_in_seconds=3600,
    )

    # Optional: dataset_format.json so columns are named and consistent
    dataset_format_s3_uri = None
    if args.include_schema:
        schema = {
            "version": 1.0,
            "datasetFormat": {"csv": {"header": True}},
            "columns": [
                {"name": "sepal_length", "type": "float"},
                {"name": "sepal_width", "type": "float"},
                {"name": "petal_length", "type": "float"},
                {"name": "petal_width", "type": "float"},
            ],
        }
        tmp_path = "/tmp/dataset_format.json"
        import json
        with open(tmp_path, "w") as f:
            json.dump(schema, f)

        dataset_format_s3_uri = S3Uploader.upload(
            local_path=tmp_path,
            desired_s3_uri=args.baseline_output_s3_uri.rstrip("/") + "/schema",
            sagemaker_session=sm_sess,
        )
        print("✅ Uploaded dataset_format.json:", dataset_format_s3_uri)

    print("▶ Creating baseline...")
    monitor.suggest_baseline(
        baseline_dataset=args.baseline_data_s3_uri,
        dataset_format={"csv": {"header": True}},
        output_s3_uri=args.baseline_output_s3_uri,
        wait=True,
        logs=True,
        # If schema uploaded, pass it
        record_preprocessor_script=None,
        post_analytics_processor_script=None,
    )

    # Baseline files are written inside output_s3_uri (usually directly under it)
    print("✅ Baseline created.")
    print("Baseline output:", args.baseline_output_s3_uri)
    print("Expected files: statistics.json, constraints.json")


if __name__ == "__main__":
    main()