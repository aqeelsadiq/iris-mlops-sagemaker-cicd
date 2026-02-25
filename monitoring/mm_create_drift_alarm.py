# # monitoring/mm_create_drift_alarm.py
# import argparse
# import boto3
# from botocore.exceptions import ClientError


# def parse_args():
#     p = argparse.ArgumentParser()
#     p.add_argument("--region", required=True)

#     # Monitor identifiers
#     p.add_argument("--schedule-name", required=True)
#     p.add_argument("--endpoint-name", required=True)

#     # Alerting
#     p.add_argument("--sns-topic-name", required=True)
#     p.add_argument("--email", required=True)

#     # Optional tuning
#     p.add_argument(
#         "--features",
#         default="sepal_length,sepal_width,petal_length,petal_width",
#         help="Comma-separated feature names used in metric names: feature_baseline_drift_<feature>",
#     )
#     p.add_argument("--period", type=int, default=3600, help="Alarm period in seconds (default 3600 = 1 hour)")
#     p.add_argument("--eval-periods", type=int, default=1, help="Evaluation periods (default 1)")
#     p.add_argument("--threshold", type=float, default=1, help="Threshold (default 1)")
#     return p.parse_args()


# def ensure_sns_topic_and_email(sns, topic_name: str, email: str) -> str:
#     # Create/Get topic
#     topic = sns.create_topic(Name=topic_name)
#     topic_arn = topic["TopicArn"]
#     print("✅ SNS Topic:", topic_arn)

#     # Subscribe email (idempotent-ish; SNS allows duplicates if you run many times)
#     sub = sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=email)
#     print("📩 Email subscription requested:", email)
#     print("   IMPORTANT: confirm the subscription from your inbox (only once).")
#     return topic_arn


# def put_drift_alarm(
#     cw,
#     *,
#     alarm_name: str,
#     topic_arn: str,
#     metric_name: str,
#     endpoint_name: str,
#     schedule_name: str,
#     period: int,
#     eval_periods: int,
#     threshold: float,
# ):
#     cw.put_metric_alarm(
#         AlarmName=alarm_name,
#         AlarmDescription=f"Data drift detected by SageMaker Model Monitor for metric {metric_name}",
#         ActionsEnabled=True,
#         AlarmActions=[topic_arn],
#         Namespace="/aws/sagemaker/Endpoints/data-metric",
#         MetricName=metric_name,
#         Statistic="Sum",
#         Period=period,
#         EvaluationPeriods=eval_periods,
#         Threshold=threshold,
#         ComparisonOperator="GreaterThanOrEqualToThreshold",
#         Dimensions=[
#             {"Name": "EndpointName", "Value": endpoint_name},
#             {"Name": "ScheduleName", "Value": schedule_name},
#         ],
#         TreatMissingData="notBreaching",
#     )
#     print("✅ Alarm created/updated:", alarm_name)


# def main():
#     args = parse_args()
#     sns = boto3.client("sns", region_name=args.region)
#     cw = boto3.client("cloudwatch", region_name=args.region)

#     topic_arn = ensure_sns_topic_and_email(sns, args.sns_topic_name, args.email)

#     # Create one alarm per feature drift metric
#     features = [f.strip() for f in args.features.split(",") if f.strip()]
#     if not features:
#         raise ValueError("No features provided. Use --features 'sepal_length,...'")

#     for feat in features:
#         metric_name = f"feature_baseline_drift_{feat}"
#         alarm_name = f"{args.schedule_name}-drift-{feat}"

#         put_drift_alarm(
#             cw,
#             alarm_name=alarm_name,
#             topic_arn=topic_arn,
#             metric_name=metric_name,
#             endpoint_name=args.endpoint_name,
#             schedule_name=args.schedule_name,
#             period=args.period,
#             eval_periods=args.eval_periods,
#             threshold=args.threshold,
#         )

#     print("\nNext checks:")
#     print("1) Invoke the endpoint a few times (to generate DataCapture).")
#     print("2) Wait for the next monitoring execution to run.")
#     print("3) CloudWatch -> Metrics -> SageMaker -> Endpoint data-metric")
#     print("   Look for: feature_baseline_drift_<feature> metrics.")
#     print("4) If drift happens, alarms go to ALARM and SNS emails you.")


# if __name__ == "__main__":
#     main()




#claude code
"""
mm_create_drift_alarm.py
------------------------
Creates CloudWatch alarms for each feature's drift metric emitted by
SageMaker Model Monitor, and wires them to an SNS email notification.

Usage:
    python monitoring/mm_create_drift_alarm.py \
        --region us-east-1 \
        --schedule-name my-monitor-schedule \
        --endpoint-name my-endpoint \
        --sns-topic-name iris-monitor-alerts \
        --email you@example.com
"""

import argparse
import boto3


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--region", required=True)

    p.add_argument("--schedule-name", required=True,
                   help="Name of the monitoring schedule (used in CloudWatch dimensions)")
    p.add_argument("--endpoint-name", required=True,
                   help="SageMaker endpoint name (used in CloudWatch dimensions)")

    p.add_argument("--sns-topic-name", required=True,
                   help="SNS topic name to create/reuse for alerts")
    p.add_argument("--email", required=True,
                   help="Email address to subscribe to the SNS topic")

    p.add_argument("--features",
                   default="sepal_length,sepal_width,petal_length,petal_width",
                   help="Comma-separated feature names matching baseline column names")
    p.add_argument("--period", type=int, default=3600,
                   help="Alarm evaluation period in seconds (default: 3600 = 1 hour)")
    p.add_argument("--eval-periods", type=int, default=1,
                   help="Number of periods to evaluate before alarming (default: 1)")
    p.add_argument("--threshold", type=float, default=1.0,
                   help="Drift metric threshold to trigger alarm (default: 1.0)")
    return p.parse_args()


def ensure_sns_topic(sns_client, topic_name: str, email: str) -> str:
    """Create SNS topic if needed, subscribe email, return topic ARN."""
    response = sns_client.create_topic(Name=topic_name)
    topic_arn = response["TopicArn"]
    print(f"✅ SNS Topic: {topic_arn}")

    sns_client.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=email)
    print(f"📩 Email subscription requested for: {email}")
    print("   ⚠️  Check your inbox and CONFIRM the subscription (only needed once).")
    return topic_arn


def put_drift_alarm(
    cw_client,
    *,
    alarm_name: str,
    topic_arn: str,
    metric_name: str,
    endpoint_name: str,
    schedule_name: str,
    period: int,
    eval_periods: int,
    threshold: float,
):
    """Create or update a CloudWatch alarm for a single drift metric."""
    cw_client.put_metric_alarm(
        AlarmName=alarm_name,
        AlarmDescription=(
            f"SageMaker Model Monitor detected data drift on metric: {metric_name}. "
            f"Endpoint: {endpoint_name}, Schedule: {schedule_name}"
        ),
        ActionsEnabled=True,
        AlarmActions=[topic_arn],
        OKActions=[topic_arn],
        Namespace="/aws/sagemaker/Endpoints/data-metrics",
        MetricName=metric_name,
        Statistic="Sum",
        Period=period,
        EvaluationPeriods=eval_periods,
        Threshold=threshold,
        ComparisonOperator="GreaterThanOrEqualToThreshold",
        Dimensions=[
            {"Name": "EndpointName", "Value": endpoint_name},
            {"Name": "ScheduleName", "Value": schedule_name},
        ],
        TreatMissingData="notBreaching",
    )
    print(f"✅ Alarm created/updated: {alarm_name}")


def main():
    args = parse_args()

    sns_client = boto3.client("sns", region_name=args.region)
    cw_client  = boto3.client("cloudwatch", region_name=args.region)

    # ── 1. Set up SNS topic + email subscription ──────────────────────────────
    topic_arn = ensure_sns_topic(sns_client, args.sns_topic_name, args.email)

    # ── 2. Create one alarm per feature ──────────────────────────────────────
    features = [f.strip() for f in args.features.split(",") if f.strip()]
    if not features:
        raise ValueError("No features provided via --features.")

    print(f"\nCreating {len(features)} drift alarm(s)...")
    for feat in features:
        metric_name = f"feature_baseline_drift_{feat}"
        alarm_name  = f"{args.schedule_name}-drift-{feat}"

        put_drift_alarm(
            cw_client,
            alarm_name=alarm_name,
            topic_arn=topic_arn,
            metric_name=metric_name,
            endpoint_name=args.endpoint_name,
            schedule_name=args.schedule_name,
            period=args.period,
            eval_periods=args.eval_periods,
            threshold=args.threshold,
        )

    print("\n✅ All drift alarms created.")
    print("\nHow to verify:")
    print("  1) Invoke the endpoint several times (generates DataCapture).")
    print("  2) Wait for the monitoring job to run (check cron schedule).")
    print("  3) CloudWatch → Metrics → /aws/sagemaker/Endpoints/data-metrics")
    print(f"     Look for: feature_baseline_drift_<feature> with dimensions:")
    print(f"       EndpointName={args.endpoint_name}")
    print(f"       ScheduleName={args.schedule_name}")
    print("  4) When drift is detected, alarms fire and SNS emails you.")


if __name__ == "__main__":
    main()