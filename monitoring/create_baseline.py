# monitoring/create_baseline.py
import argparse
import boto3
from sagemaker.session import Session
from sagemaker.model_monitor import DefaultModelMonitor


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--region", required=True)
    p.add_argument("--role-arn", required=True)

    # This MUST be features.csv (header + 4 cols, no label)
    p.add_argument("--baseline-data-s3-uri", required=True)

    # Output prefix like: s3://bucket/monitoring/baseline/
    p.add_argument("--baseline-output-s3-uri", required=True)

    p.add_argument("--instance-type", default="ml.t3.medium")
    p.add_argument("--instance-count", type=int, default=1)
    p.add_argument("--volume-size", type=int, default=20)
    p.add_argument("--max-runtime", type=int, default=3600)
    p.add_argument("--job-name", default="baseline-suggestion-job-1")
    return p.parse_args()


def main():
    args = parse_args()

    boto_sess = boto3.Session(region_name=args.region)
    sm_sess = Session(boto_session=boto_sess)

    monitor = DefaultModelMonitor(
        role=args.role_arn,
        instance_type=args.instance_type,
        instance_count=args.instance_count,
        volume_size_in_gb=args.volume_size,
        max_runtime_in_seconds=args.max_runtime,
        sagemaker_session=sm_sess,
    )

    # features.csv has a header row
    monitor.suggest_baseline(
        baseline_dataset=args.baseline_data_s3_uri,
        dataset_format={"csv": {"header": True}},
        output_s3_uri=args.baseline-output_s3_uri if False else args.baseline_output_s3_uri,  # safety
        wait=True,
        logs=True,
        job_name=args.job_name,
    )

    # These methods return the *local* file paths once job finishes;
    # for scheduling we will reference the S3 output prefix directly.
    print("✅ Baseline created")
    print("Baseline output prefix:", args.baseline_output_s3_uri)
    print("Expected statistics file:", args.baseline_output_s3_uri.rstrip("/") + "/statistics.json")
    print("Expected constraints file:", args.baseline_output_s3_uri.rstrip("/") + "/constraints.json")


if __name__ == "__main__":
    main()
