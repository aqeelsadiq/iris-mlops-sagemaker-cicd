# monitoring/mm_create_schedule.py
import argparse
import boto3
import sagemaker
from sagemaker.model_monitor import DefaultModelMonitor
from sagemaker.model_monitor.dataset_format import DatasetFormat
from sagemaker.processing import ProcessingInput
from botocore.exceptions import ClientError


def parse_args():
    p = argparse.ArgumentParser()

    p.add_argument("--region", required=True)
    p.add_argument("--role-arn", required=True)

    p.add_argument("--endpoint-name", required=True)
    p.add_argument("--schedule-name", required=True)

    # Where baseline artifacts exist (must contain statistics.json + constraints.json)
    p.add_argument("--baseline-s3-uri", required=True)

    # Where monitoring outputs/reports should be written
    p.add_argument("--monitor-output-s3-uri", required=True)

    # DataCapture prefix (S3 URI) - OPTIONAL but recommended
    # Example: s3://bucket/monitoring/datacapture/
    p.add_argument("--datacapture-s3-uri", required=True)

    # processing infra
    p.add_argument("--instance-type", default="ml.m5.large")
    p.add_argument("--instance-count", type=int, default=1)
    p.add_argument("--volume-size", type=int, default=20)
    p.add_argument("--max-runtime", type=int, default=3600)

    # schedule
    # hourly example: cron(0 * ? * * *)
    p.add_argument("--cron", default="cron(0 * ? * * *)")

    return p.parse_args()


def s3_join(prefix: str, key: str) -> str:
    return prefix.rstrip("/") + "/" + key.lstrip("/")


def delete_existing_schedule(sm_client, name: str):
    try:
        sm_client.describe_monitoring_schedule(MonitoringScheduleName=name)
    except sm_client.exceptions.ResourceNotFound:
        return

    print(f"ℹ️ Monitoring schedule exists, deleting: {name}")
    sm_client.delete_monitoring_schedule(MonitoringScheduleName=name)

    # wait a bit until it's deleted
    waiter = sm_client.get_waiter("monitoring_schedule_deleted")
    waiter.wait(MonitoringScheduleName=name)
    print("✅ Deleted old schedule.")


def main():
    args = parse_args()

    boto_sess = boto3.Session(region_name=args.region)
    sm_client = boto_sess.client("sagemaker")
    sess = sagemaker.Session(boto_session=boto_sess)

    baseline_stats = s3_join(args.baseline_s3_uri, "statistics.json")
    baseline_constraints = s3_join(args.baseline_s3_uri, "constraints.json")

    print("Creating/updating monitoring schedule...")
    print("Schedule :", args.schedule_name)
    print("Endpoint :", args.endpoint_name)
    print("Baseline :", args.baseline_s3_uri)
    print("Reports  :", args.monitor_output_s3_uri)
    print("Capture  :", args.datacapture_s3_uri)
    print("Stats    :", baseline_stats)
    print("Constraints:", baseline_constraints)

    # If schedule already exists, delete and recreate (simplest + avoids update issues)
    delete_existing_schedule(sm_client, args.schedule_name)

    monitor = DefaultModelMonitor(
        role=args.role_arn,
        instance_count=args.instance_count,
        instance_type=args.instance_type,
        volume_size_in_gb=args.volume_size,
        max_runtime_in_seconds=args.max_runtime,
        sagemaker_session=sess,
    )

    # IMPORTANT:
    # monitoring_inputs tells the job where your captured endpoint data is.
    # DataCapture produces JSONLines (.jsonl)
    monitoring_inputs = [
        ProcessingInput(
            source=args.datacapture_s3_uri.rstrip("/") + "/",
            destination="/opt/ml/processing/input",
            input_name="endpoint-data-capture",
        )
    ]

    monitor.create_monitoring_schedule(
        monitor_schedule_name=args.schedule_name,
        endpoint_name=args.endpoint_name,
        output_s3_uri=args.monitor_output_s3_uri,
        statistics=baseline_stats,
        constraints=baseline_constraints,
        schedule_cron_expression=args.cron,
        monitoring_inputs=monitoring_inputs,
        # This format matches DataCapture jsonl
        dataset_format=DatasetFormat.json(),
    )

    print("✅ Monitoring schedule created:", args.schedule_name)
    print("Next: wait for first execution (based on cron), then check S3 reports prefix.")


if __name__ == "__main__":
    main()