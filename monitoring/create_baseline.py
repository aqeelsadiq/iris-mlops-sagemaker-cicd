# monitoring/create_baseline.py
import argparse
import boto3
from botocore.exceptions import ClientError
from sagemaker.session import Session
from sagemaker.model_monitor import DefaultModelMonitor


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--region", required=True)
    p.add_argument("--role-arn", required=True)
    p.add_argument("--baseline-data-s3-uri", required=True)
    p.add_argument("--baseline-output-s3-uri", required=True)
    p.add_argument("--instance-type", default="ml.m5.2xlarge")  # ✅ safer default
    p.add_argument("--instance-count", type=int, default=1)
    p.add_argument("--volume-size", type=int, default=30)
    p.add_argument("--max-runtime", type=int, default=3600)
    p.add_argument("--job-name", default="baseline-suggestion")
    p.add_argument("--skip-if-exists", action="store_true", default=True)
    return p.parse_args()


def s3_prefix_exists(s3_client, s3_uri: str) -> bool:
    # s3://bucket/prefix/
    if not s3_uri.startswith("s3://"):
        return False
    path = s3_uri.replace("s3://", "", 1)
    bucket, _, prefix = path.partition("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=5)
    return resp.get("KeyCount", 0) > 0


def main():
    args = parse_args()

    boto_sess = boto3.Session(region_name=args.region)
    sm_sess = Session(boto_session=boto_sess)
    s3 = boto_sess.client("s3")

    # ✅ Optional idempotency: if baseline output already has files, skip
    if args.skip_if_exists and s3_prefix_exists(s3, args.baseline_output_s3_uri):
        print(f"✅ Baseline output already exists at {args.baseline_output_s3_uri}. Skipping baseline creation.")
        return

    monitor = DefaultModelMonitor(
        role=args.role_arn,
        instance_type=args.instance_type,
        instance_count=args.instance_count,
        volume_size_in_gb=args.volume_size,
        max_runtime_in_seconds=args.max_runtime,
        sagemaker_session=sm_sess,
    )

    # Use a unique-ish processing job name to avoid collisions in reruns
    # (SageMaker requires job names unique per account/region at a time)
    job_name = f"{args.job_name}-{int(__import__('time').time())}"

    print("Starting baseline suggestion job...")
    print("Baseline dataset:", args.baseline_data_s3_uri)
    print("Output S3 URI:", args.baseline_output_s3_uri)
    print("Instance:", args.instance_type, "x", args.instance_count)
    print("Processing job name:", job_name)

    try:
        monitor.suggest_baseline(
            baseline_dataset=args.baseline_data_s3_uri,
            dataset_format={"csv": {"header": True}},
            output_s3_uri=args.baseline_output_s3_uri,
            wait=True,
            logs=True,
            job_name=job_name,
        )
    except Exception as e:
        print("❌ Baseline suggestion failed.")
        print("Most common fix: use a bigger instance (ml.m5.xlarge or ml.m5.2xlarge).")
        raise

    print("✅ Baseline created")
    print("Statistics:", monitor.baseline_statistics())
    print("Constraints:", monitor.baseline_constraints())


if __name__ == "__main__":
    main()
