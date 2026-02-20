# monitoring/mm_create_schedule.py
import argparse
import time

import boto3
import sagemaker
from sagemaker.model_monitor import DefaultModelMonitor
from sagemaker.model_monitor import EndpointInput
from botocore.exceptions import ClientError


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--region", required=True)
    p.add_argument("--role-arn", required=True)

    p.add_argument("--endpoint-name", required=True)
    p.add_argument("--schedule-name", required=True)

    # Baseline output folder containing constraints.json + statistics.json
    p.add_argument("--baseline-s3-uri", required=True)

    # Where monitoring reports go
    p.add_argument("--monitor-output-s3-uri", required=True)

    # DataCapture prefix (same prefix you enabled on the endpoint)
    # Example: s3://<bucket>/monitoring/datacapture/
    p.add_argument("--datacapture-s3-uri", required=True)

    # Instance for monitoring processing job
    p.add_argument("--instance-type", default="ml.t3.medium")
    p.add_argument("--instance-count", type=int, default=1)

    # Schedule frequency
    # For hourly: cron(0 * ? * * *)
    p.add_argument("--cron", default="cron(0 * ? * * *)")

    # Optional: how big each analysis window is
    p.add_argument("--analysis-start-offset", default="-PT1H")  # last 1 hour
    p.add_argument("--analysis-end-offset", default="PT0H")     # up to now

    return p.parse_args()


def ensure_schedule_deleted(sm_client, schedule_name: str):
    # If schedule exists, delete to recreate cleanly (idempotent)
    try:
        sm_client.describe_monitoring_schedule(MonitoringScheduleName=schedule_name)
        print(f"ℹ️ Schedule exists, deleting: {schedule_name}")
        sm_client.delete_monitoring_schedule(MonitoringScheduleName=schedule_name)

        # wait until deleted
        for _ in range(60):
            try:
                sm_client.describe_monitoring_schedule(MonitoringScheduleName=schedule_name)
                time.sleep(5)
            except sm_client.exceptions.ResourceNotFound:
                print("✅ Old schedule deleted")
                return
        raise TimeoutError("Timed out waiting for schedule deletion.")
    except sm_client.exceptions.ResourceNotFound:
        return


def main():
    args = parse_args()

    boto_sess = boto3.Session(region_name=args.region)
    sm_client = boto_sess.client("sagemaker")

    sess = sagemaker.Session(boto_session=boto_sess)

    monitor = DefaultModelMonitor(
        role=args.role_arn,
        instance_count=args.instance_count,
        instance_type=args.instance_type,
        volume_size_in_gb=20,
        max_runtime_in_seconds=3600,
        sagemaker_session=sess,
    )

    # ✅ Correct way: EndpointInput (NOT monitor.endpoint_input)
    endpoint_input = EndpointInput(
        endpoint_name=args.endpoint_name,
        destination=args.datacapture_s3_uri,
    )

    print("Creating/updating monitoring schedule...")
    print("Schedule:", args.schedule_name)
    print("Endpoint :", args.endpoint_name)
    print("Baseline :", args.baseline_s3_uri)
    print("Reports  :", args.monitor_output_s3_uri)
    print("Capture  :", args.datacapture_s3_uri)

    # Make idempotent: delete + recreate
    ensure_schedule_deleted(sm_client, args.schedule_name)

    monitor.create_monitoring_schedule(
        monitor_schedule_name=args.schedule_name,
        endpoint_input=endpoint_input,
        output_s3_uri=args.monitor_output_s3_uri,
        statistics=args.baseline_s3_uri.rstrip("/") + "/statistics.json",
        constraints=args.baseline_s3_uri.rstrip("/") + "/constraints.json",
        schedule_cron_expression=args.cron,
        # Optional analysis window
        analysis_start_time=args.analysis_start_offset,
        analysis_end_time=args.analysis_end_offset,
    )

    print("✅ Monitoring schedule created:", args.schedule_name)
    print("Next: invoke endpoint to generate captured data, then wait for next cron run.")
    print("Check reports in:", args.monitor_output_s3_uri)


if __name__ == "__main__":
    main()