import argparse
import boto3
import sagemaker
from sagemaker.model_monitor import DefaultModelMonitor


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--region", required=True)
    p.add_argument("--role-arn", required=True)

    p.add_argument("--endpoint-name", required=True)
    p.add_argument("--schedule-name", required=True)

    p.add_argument("--baseline-output-s3-uri", required=True)   # where statistics.json + constraints.json exist
    p.add_argument("--reports-s3-uri", required=True)           # where monitoring reports will go

    p.add_argument("--datacapture-s3-uri", required=True)       # your data capture prefix
    p.add_argument("--cron", default="cron(0 * ? * * *)")       # every hour

    p.add_argument("--instance-type", default="ml.t3.medium")
    p.add_argument("--instance-count", type=int, default=1)
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
        volume_size_in_gb=20,
        max_runtime_in_seconds=3600,
    )

    constraints_uri = args.baseline_output_s3_uri.rstrip("/") + "/constraints.json"
    statistics_uri = args.baseline_output_s3_uri.rstrip("/") + "/statistics.json"

    print("▶ Creating/updating monitoring schedule...")
    monitor.create_monitoring_schedule(
        monitor_schedule_name=args.schedule_name,
        endpoint_input=monitor.endpoint_input(
            endpoint_name=args.endpoint_name,
            destination=args.datacapture_s3_uri,  # where captured data already lands
        ),
        output_s3_uri=args.reports_s3_uri,
        constraints=constraints_uri,
        statistics=statistics_uri,
        schedule_cron_expression=args.cron,
    )

    print("✅ Monitoring schedule ensured:", args.schedule_name)
    print("Reports will appear in:", args.reports_s3_uri)
    print("Next: invoke the endpoint and wait for the schedule to run.")


if __name__ == "__main__":
    main()