# monitoring/create_baseline.py
import argparse
import boto3
import sagemaker
from sagemaker.model_monitor import DefaultModelMonitor, DatasetFormat

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--region", required=True)
    p.add_argument("--role-arn", required=True)
    p.add_argument("--default-bucket", required=True)
    p.add_argument("--baseline-dataset-s3-uri", required=True)
    p.add_argument("--baseline-output-s3-uri", required=True)
    return p.parse_args()

def main():
    args = parse_args()

    print("SageMaker SDK version:", sagemaker.__version__)

    boto_sess = boto3.Session(region_name=args.region)
    sm_sess = sagemaker.Session(boto_session=boto_sess, default_bucket=args.default_bucket)

    monitor = DefaultModelMonitor(
        role=args.role_arn,
        instance_count=1,
        instance_type="ml.t3.medium",
        volume_size_in_gb=20,
        max_runtime_in_seconds=3600,
        sagemaker_session=sm_sess,
    )

    monitor.suggest_baseline(
        baseline_dataset=args.baseline_dataset_s3_uri,
        dataset_format=DatasetFormat.csv(header=True),
        output_s3_uri=args.baseline_output_s3_uri,
        wait=True,
        logs=True,
    )

    print("✅ Baseline created:", args.baseline_output_s3_uri)

if __name__ == "__main__":
    main()
