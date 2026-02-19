import argparse
import boto3
import sagemaker
from sagemaker.model_monitor import DefaultModelMonitor
from sagemaker.session import Session

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--region", required=True)
    p.add_argument("--role-arn", required=True)
    p.add_argument("--default-bucket", required=True)
    p.add_argument("--baseline-dataset-s3-uri", required=True)
    p.add_argument("--baseline-output-s3-uri", required=True)  # s3://.../monitoring/baseline/
    return p.parse_args()

def main():
    args = parse_args()

    boto_sess = boto3.Session(region_name=args.region)
    sm_sess = sagemaker.Session(boto_session=boto_sess, default_bucket=args.default_bucket)

    monitor = DefaultModelMonitor(
        role=args.role_arn,
        instance_count=1,
        instance_type="ml.m5.large",
        volume_size_in_gb=20,
        max_runtime_in_seconds=3600,
        sagemaker_session=sm_sess
    )

    # For CSV input, Model Monitor expects headerless or headerful, but schema is learned from baseline.
    # Iris has label column "species". Model Monitor baseline is data-quality baseline; label column is fine.
    monitor.suggest_baseline(
        baseline_dataset=args.baseline_dataset_s3_uri,
        dataset_format=sagemaker.model_monitor.DatasetFormat.csv(header=True),
        output_s3_uri=args.baseline_output_s3_uri,
        wait=True,
        logs=True,
    )

    print("✅ Baseline created at:", args.baseline_output_s3_uri)
    print("   It contains statistics.json + constraints.json")

if __name__ == "__main__":
    main()
