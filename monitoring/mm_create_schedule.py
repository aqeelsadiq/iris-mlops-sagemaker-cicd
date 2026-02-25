# # monitoring/mm_create_schedule.py
# import argparse
# import boto3
# import sagemaker
# from botocore.exceptions import ClientError
# from sagemaker.model_monitor import DefaultModelMonitor, EndpointInput


# def parse_args():
#     p = argparse.ArgumentParser()

#     p.add_argument("--region", required=True)
#     p.add_argument("--role-arn", required=True)

#     p.add_argument("--endpoint-name", required=True)
#     p.add_argument("--schedule-name", required=True)

#     # Folder containing statistics.json + constraints.json
#     p.add_argument("--baseline-s3-uri", required=True)

#     # Where monitoring outputs/reports should be written
#     p.add_argument("--monitor-output-s3-uri", required=True)

#     # Keeping it in args for your workflow compatibility (not always required by SDK)
#     p.add_argument("--datacapture-s3-uri", required=True)

#     # processing infra
#     p.add_argument("--instance-type", default="ml.m5.large")
#     p.add_argument("--instance-count", type=int, default=1)
#     p.add_argument("--volume-size", type=int, default=20)
#     p.add_argument("--max-runtime", type=int, default=3600)

#     # hourly example: cron(0 * ? * * *)
#     p.add_argument("--cron", default="cron(0 * ? * * *)")

#     return p.parse_args()


# def s3_join(prefix: str, key: str) -> str:
#     return prefix.rstrip("/") + "/" + key.lstrip("/")


# def delete_existing_schedule(sm_client, name: str):
#     try:
#         sm_client.describe_monitoring_schedule(MonitoringScheduleName=name)
#     except sm_client.exceptions.ResourceNotFound:
#         return

#     print(f"ℹ️ Monitoring schedule exists, deleting: {name}")
#     sm_client.delete_monitoring_schedule(MonitoringScheduleName=name)

#     # waiter name varies in some SDKs/accounts; do a simple poll to be safe
#     for _ in range(60):
#         try:
#             sm_client.describe_monitoring_schedule(MonitoringScheduleName=name)
#         except sm_client.exceptions.ResourceNotFound:
#             print("✅ Deleted old schedule.")
#             return
#         import time
#         time.sleep(5)

#     raise TimeoutError(f"Timed out waiting for schedule deletion: {name}")


# def main():
#     args = parse_args()

#     boto_sess = boto3.Session(region_name=args.region)
#     sm_client = boto_sess.client("sagemaker")
#     sess = sagemaker.Session(boto_session=boto_sess)

#     baseline_stats = s3_join(args.baseline_s3_uri, "statistics.json")
#     baseline_constraints = s3_join(args.baseline_s3_uri, "constraints.json")

#     print("Creating/updating monitoring schedule...")
#     print("Schedule    :", args.schedule_name)
#     print("Endpoint    :", args.endpoint_name)
#     print("Baseline    :", args.baseline_s3_uri)
#     print("Reports     :", args.monitor_output_s3_uri)
#     print("DataCapture :", args.datacapture_s3_uri)
#     print("Stats       :", baseline_stats)
#     print("Constraints :", baseline_constraints)

#     # Recreate schedule (avoid update edge cases)
#     delete_existing_schedule(sm_client, args.schedule_name)

#     monitor = DefaultModelMonitor(
#         role=args.role_arn,
#         instance_count=args.instance_count,
#         instance_type=args.instance_type,
#         volume_size_in_gb=args.volume_size,
#         max_runtime_in_seconds=args.max_runtime,
#         sagemaker_session=sess,
#     )

#     endpoint_input = EndpointInput(
#         endpoint_name=args.endpoint_name,
#         destination="/opt/ml/processing/input",
#     )

#     # ✅ Compatibility strategy:
#     # Some SDKs accept endpoint_input, some accept endpoint_input + other params.
#     # Your SDK DOES NOT accept dataset_format, so we NEVER pass it.
#     try:
#         monitor.create_monitoring_schedule(
#             monitor_schedule_name=args.schedule_name,
#             endpoint_input=endpoint_input,
#             output_s3_uri=args.monitor_output_s3_uri,
#             statistics=baseline_stats,
#             constraints=baseline_constraints,
#             schedule_cron_expression=args.cron,
#             enable_cloudwatch_metrics=True,
#             record_preprocessor_script="./monitoring/record_preprocessor.py"  # ✅ ADD THIS

#         )
#     except TypeError as e:
#         # Fallback for older signature variants (rare, but helps)
#         # Try without endpoint_input and use endpoint_name if supported
#         msg = str(e)
#         print("⚠️ TypeError calling create_monitoring_schedule:", msg)
#         print("Trying fallback signature...")

#         monitor.create_monitoring_schedule(
#             monitor_schedule_name=args.schedule_name,
#             endpoint_name=args.endpoint_name,
#             output_s3_uri=args.monitor_output_s3_uri,
#             statistics=baseline_stats,
#             constraints=baseline_constraints,
#             schedule_cron_expression=args.cron,
#         )

#     print("✅ Monitoring schedule created:", args.schedule_name)
#     print("Next steps:")
#     print("1) Invoke endpoint a few times (to generate DataCapture)")
#     print("2) Wait until next cron window triggers the monitoring job")
#     print("3) Check SageMaker -> Model monitor schedules and S3 reports prefix")


# if __name__ == "__main__":
#     main()


#claude code
"""
mm_create_schedule.py
---------------------
Creates (or recreates) a SageMaker Model Monitor data-quality schedule.

KEY FIXES:
  1. record_preprocessor_script must be an S3 URI — not a local path.
  2. The same S3 preprocessor URI used in mm_create_baseline.py is reused here
     so the live schema matches the baseline schema exactly.
  3. Robust deletion/recreation to avoid stale schedule state errors.

Usage:
    python monitoring/mm_create_schedule.py \
        --region us-east-1 \
        --role-arn arn:aws:iam::123456789012:role/SageMakerRole \
        --endpoint-name my-endpoint \
        --schedule-name my-monitor-schedule \
        --baseline-s3-uri s3://my-bucket/monitoring/baseline/ \
        --monitor-output-s3-uri s3://my-bucket/monitoring/reports/ \
        --datacapture-s3-uri s3://my-bucket/datacapture/ \
        --preprocessor-s3-uri s3://my-bucket/monitoring/scripts/record_preprocessor.py
"""

import argparse
import time
import boto3
import sagemaker
from sagemaker.model_monitor import DefaultModelMonitor, EndpointInput


def parse_args():
    p = argparse.ArgumentParser()

    p.add_argument("--region", required=True)
    p.add_argument("--role-arn", required=True)

    p.add_argument("--endpoint-name", required=True)
    p.add_argument("--schedule-name", required=True)

    # Folder that contains statistics.json + constraints.json
    p.add_argument("--baseline-s3-uri", required=True,
                   help="S3 URI prefix where baseline output was written (contains statistics.json & constraints.json)")

    # Where monitoring job reports are written
    p.add_argument("--monitor-output-s3-uri", required=True)

    # Data capture S3 URI (informational; EndpointInput reads directly from captured data)
    p.add_argument("--datacapture-s3-uri", required=True)

    # ✅ CRITICAL FIX: must be an S3 URI, NOT a local path
    p.add_argument("--preprocessor-s3-uri", required=True,
                   help="S3 URI of record_preprocessor.py (upload it first with mm_create_baseline.py)")

    # Processing infrastructure
    p.add_argument("--instance-type", default="ml.m5.large")
    p.add_argument("--instance-count", type=int, default=1)
    p.add_argument("--volume-size", type=int, default=20)
    p.add_argument("--max-runtime", type=int, default=3600)

    # Cron schedule (default: every hour)
    p.add_argument("--cron", default="cron(0 * ? * * *)",
                   help="CloudWatch cron expression (default: hourly)")

    return p.parse_args()


def s3_join(prefix: str, filename: str) -> str:
    return prefix.rstrip("/") + "/" + filename.lstrip("/")


def delete_schedule_if_exists(sm_client, name: str, timeout: int = 300):
    """Delete an existing monitoring schedule and wait for it to be gone."""
    try:
        sm_client.describe_monitoring_schedule(MonitoringScheduleName=name)
    except sm_client.exceptions.ResourceNotFound:
        return  # Already gone

    print(f"ℹ️  Deleting existing schedule: {name}")
    sm_client.delete_monitoring_schedule(MonitoringScheduleName=name)

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            sm_client.describe_monitoring_schedule(MonitoringScheduleName=name)
            time.sleep(5)
        except sm_client.exceptions.ResourceNotFound:
            print("✅ Old schedule deleted.")
            return

    raise TimeoutError(f"Timed out waiting for schedule deletion: {name}")


def main():
    args = parse_args()

    boto_sess = boto3.Session(region_name=args.region)
    sm_client = boto_sess.client("sagemaker")
    sess = sagemaker.Session(boto_session=boto_sess)

    baseline_stats       = s3_join(args.baseline_s3_uri, "statistics.json")
    baseline_constraints = s3_join(args.baseline_s3_uri, "constraints.json")

    print("=" * 60)
    print("Creating monitoring schedule")
    print(f"  Schedule      : {args.schedule_name}")
    print(f"  Endpoint      : {args.endpoint_name}")
    print(f"  Baseline stats: {baseline_stats}")
    print(f"  Constraints   : {baseline_constraints}")
    print(f"  Reports       : {args.monitor_output_s3_uri}")
    print(f"  DataCapture   : {args.datacapture_s3_uri}")
    print(f"  Preprocessor  : {args.preprocessor_s3_uri}")
    print(f"  Cron          : {args.cron}")
    print("=" * 60)

    # ── 1. Delete any pre-existing schedule (clean slate) ─────────────────────
    delete_schedule_if_exists(sm_client, args.schedule_name)

    # ── 2. Build monitor + schedule ───────────────────────────────────────────
    monitor = DefaultModelMonitor(
        role=args.role_arn,
        instance_count=args.instance_count,
        instance_type=args.instance_type,
        volume_size_in_gb=args.volume_size,
        max_runtime_in_seconds=args.max_runtime,
        sagemaker_session=sess,
    )

    endpoint_input = EndpointInput(
        endpoint_name=args.endpoint_name,
        destination="/opt/ml/processing/input/endpoint",
    )

    monitor.create_monitoring_schedule(
        monitor_schedule_name=args.schedule_name,
        endpoint_input=endpoint_input,
        output_s3_uri=args.monitor_output_s3_uri,
        statistics=baseline_stats,
        constraints=baseline_constraints,
        schedule_cron_expression=args.cron,
        enable_cloudwatch_metrics=True,
        # ✅ CRITICAL FIX: S3 URI — not a local file path
        record_preprocessor_script=args.preprocessor_s3_uri,
    )

    print(f"\n✅ Monitoring schedule created: {args.schedule_name}")
    print("\nNext steps:")
    print("  1) Invoke the endpoint several times to generate DataCapture files.")
    print("  2) Wait for the next cron window (or manually trigger an execution).")
    print("  3) Check: SageMaker Console → Endpoints → Monitor → Monitoring jobs")
    print(f"  4) Reports will appear under: {args.monitor_output_s3_uri}")


if __name__ == "__main__":
    main()