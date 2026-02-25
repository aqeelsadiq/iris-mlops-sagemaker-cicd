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
Creates a SageMaker Model Monitor data-quality schedule (idempotent).

This schedule checks captured inputs against baseline constraints/statistics.
"""

import argparse
import time
import boto3
import sagemaker
from botocore.exceptions import ClientError
try:
    from sagemaker.model_monitor import DefaultModelMonitor, EndpointInput
except Exception:
    from sagemaker.model_monitoring import DefaultModelMonitor, EndpointInput

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--region", required=True)
    p.add_argument("--role-arn", required=True)
    p.add_argument("--endpoint-name", required=True)
    p.add_argument("--schedule-name", required=True)

    p.add_argument("--baseline-s3-uri", required=True)          # folder containing statistics.json/constraints.json
    p.add_argument("--monitor-output-s3-uri", required=True)    # reports output prefix
    p.add_argument("--preprocessor-s3-uri", required=True)      # FULL S3 URI to record_preprocessor.py

    p.add_argument("--instance-type", default="ml.m5.large")
    p.add_argument("--instance-count", type=int, default=1)
    p.add_argument("--volume-size", type=int, default=20)
    p.add_argument("--max-runtime", type=int, default=3600)
    p.add_argument("--cron", default="cron(0 * ? * * *)")       # hourly by default
    return p.parse_args()


def s3_join(prefix: str, filename: str) -> str:
    return prefix.rstrip("/") + "/" + filename.lstrip("/")


def delete_schedule_if_exists(sm, name: str, timeout: int = 300):
    try:
        sm.describe_monitoring_schedule(MonitoringScheduleName=name)
    except sm.exceptions.ResourceNotFound:
        return

    print(f"ℹ️ Deleting old schedule: {name}")
    sm.delete_monitoring_schedule(MonitoringScheduleName=name)

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            sm.describe_monitoring_schedule(MonitoringScheduleName=name)
            time.sleep(5)
        except sm.exceptions.ResourceNotFound:
            print("✅ Old schedule deleted.")
            return

    raise TimeoutError(f"Timed out waiting to delete schedule: {name}")


def main():
    args = parse_args()

    boto_sess = boto3.Session(region_name=args.region)
    sm = boto_sess.client("sagemaker")
    sess = sagemaker.Session(boto_session=boto_sess)

    baseline_stats = s3_join(args.baseline_s3_uri, "statistics.json")
    baseline_constraints = s3_join(args.baseline_s3_uri, "constraints.json")

    monitor_output = args.monitor_output_s3_uri.rstrip("/") + "/"

    print("=" * 60)
    print("Creating monitoring schedule")
    print(f"Schedule       : {args.schedule_name}")
    print(f"Endpoint       : {args.endpoint_name}")
    print(f"Stats          : {baseline_stats}")
    print(f"Constraints    : {baseline_constraints}")
    print(f"Reports        : {monitor_output}")
    print(f"Preprocessor   : {args.preprocessor_s3_uri}")
    print(f"Cron           : {args.cron}")
    print("=" * 60)

    delete_schedule_if_exists(sm, args.schedule_name)

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
        output_s3_uri=monitor_output,
        statistics=baseline_stats,
        constraints=baseline_constraints,
        schedule_cron_expression=args.cron,
        enable_cloudwatch_metrics=True,
        record_preprocessor_script=args.preprocessor_s3_uri,
    )

    print(f"\n✅ Schedule created: {args.schedule_name}")


if __name__ == "__main__":
    main()