# monitoring/create_monitor_schedule.py
import argparse
import boto3
from sagemaker.session import Session
from sagemaker.model_monitor import DefaultModelMonitor
from sagemaker.model_monitor import CronExpressionGenerator
from sagemaker.model_monitor import EndpointInput


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--region", required=True)
    p.add_argument("--role-arn", required=True)
    p.add_argument("--endpoint-name", required=True)
    p.add_argument("--schedule-name", required=True)

    # Baseline output prefix containing statistics.json + constraints.json
    p.add_argument("--baseline-s3-uri", required=True)

    # Where monitoring outputs (reports) go
    p.add_argument("--monitor-output-s3-uri", required=True)

    p.add_argument("--instance-type", default="ml.t3.medium")
    p.add_argument("--instance-count", type=int, default=1)
    p.add_argument("--volume-size", type=int, default=20)
    p.add_argument("--max-runtime", type=int, default=3600)

    # cron like: cron(0 * ? * * *)  -> hourly
    p.add_argument("--cron", required=True)
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

    baseline_prefix = args.baseline_s3_uri.rstrip("/")
    statistics_s3_uri = f"{baseline_prefix}/statistics.json"
    constraints_s3_uri = f"{baseline_prefix}/constraints.json"

    endpoint_input = EndpointInput(
        endpoint_name=args.endpoint_name,
        destination="/opt/ml/processing/input",
    )

    print("Creating monitoring schedule...")
    print("  schedule:", args.schedule_name)
    print("  endpoint:", args.endpoint_name)
    print("  statistics:", statistics_s3_uri)
    print("  constraints:", constraints_s3_uri)
    print("  output:", args.monitor_output_s3_uri)
    print("  cron:", args.cron)

    monitor.create_monitoring_schedule(
        monitor_schedule_name=args.schedule_name,
        endpoint_input=endpoint_input,
        output_s3_uri=args.monitor_output_s3_uri,
        statistics=statistics_s3_uri,
        constraints=constraints_s3_uri,
        schedule_cron_expression=args.cron,
    )

    print("✅ Monitoring schedule created:", args.schedule_name)


if __name__ == "__main__":
    main()
