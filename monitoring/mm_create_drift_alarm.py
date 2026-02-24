# import argparse
# import boto3

# def parse_args():
#     p = argparse.ArgumentParser()
#     p.add_argument("--region", required=True)
#     p.add_argument("--schedule-name", required=True)
#     p.add_argument("--sns-topic-name", required=True)
#     p.add_argument("--email", required=True)
#     return p.parse_args()

# def main():
#     args = parse_args()

#     sns = boto3.client("sns", region_name=args.region)
#     cw = boto3.client("cloudwatch", region_name=args.region)

#     # Create SNS topic
#     topic = sns.create_topic(Name=args.sns_topic_name)
#     topic_arn = topic["TopicArn"]
#     print("SNS Topic:", topic_arn)

#     # Subscribe email
#     sns.subscribe(
#         TopicArn=topic_arn,
#         Protocol="email",
#         Endpoint=args.email
#     )
#     print("Confirm subscription in your email.")

#     # Create alarm on constraint violations
#     cw.put_metric_alarm(
#         AlarmName=f"{args.schedule_name}-drift-alarm",
#         AlarmDescription="Drift detected by SageMaker Model Monitor",
#         ActionsEnabled=True,
#         AlarmActions=[topic_arn],
#         MetricName="ConstraintViolations",
#         Namespace="aws/sagemaker/ModelMonitoring",
#         Statistic="Sum",
#         Period=3600,
#         EvaluationPeriods=1,
#         Threshold=1,
#         ComparisonOperator="GreaterThanOrEqualToThreshold",
#         Dimensions=[
#             {"Name": "MonitoringSchedule", "Value": args.schedule_name}
#         ],
#         TreatMissingData="notBreaching"
#     )

#     print("✅ Drift alarm created successfully.")

# if __name__ == "__main__":
#     main()



# monitoring/mm_create_drift_alarm.py
import argparse
import boto3
from botocore.exceptions import ClientError


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--region", required=True)

    # Monitor identifiers
    p.add_argument("--schedule-name", required=True)
    p.add_argument("--endpoint-name", required=True)

    # Alerting
    p.add_argument("--sns-topic-name", required=True)
    p.add_argument("--email", required=True)

    # Optional tuning
    p.add_argument(
        "--features",
        default="sepal_length,sepal_width,petal_length,petal_width",
        help="Comma-separated feature names used in metric names: feature_baseline_drift_<feature>",
    )
    p.add_argument("--period", type=int, default=3600, help="Alarm period in seconds (default 3600 = 1 hour)")
    p.add_argument("--eval-periods", type=int, default=1, help="Evaluation periods (default 1)")
    p.add_argument("--threshold", type=float, default=1, help="Threshold (default 1)")
    return p.parse_args()


def ensure_sns_topic_and_email(sns, topic_name: str, email: str) -> str:
    # Create/Get topic
    topic = sns.create_topic(Name=topic_name)
    topic_arn = topic["TopicArn"]
    print("✅ SNS Topic:", topic_arn)

    # Subscribe email (idempotent-ish; SNS allows duplicates if you run many times)
    sub = sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=email)
    print("📩 Email subscription requested:", email)
    print("   IMPORTANT: confirm the subscription from your inbox (only once).")
    return topic_arn


def put_drift_alarm(
    cw,
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
    cw.put_metric_alarm(
        AlarmName=alarm_name,
        AlarmDescription=f"Data drift detected by SageMaker Model Monitor for metric {metric_name}",
        ActionsEnabled=True,
        AlarmActions=[topic_arn],
        Namespace="/aws/sagemaker/Endpoints/data-metric",
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
    print("✅ Alarm created/updated:", alarm_name)


def main():
    args = parse_args()
    sns = boto3.client("sns", region_name=args.region)
    cw = boto3.client("cloudwatch", region_name=args.region)

    topic_arn = ensure_sns_topic_and_email(sns, args.sns_topic_name, args.email)

    # Create one alarm per feature drift metric
    features = [f.strip() for f in args.features.split(",") if f.strip()]
    if not features:
        raise ValueError("No features provided. Use --features 'sepal_length,...'")

    for feat in features:
        metric_name = f"feature_baseline_drift_{feat}"
        alarm_name = f"{args.schedule_name}-drift-{feat}"

        put_drift_alarm(
            cw,
            alarm_name=alarm_name,
            topic_arn=topic_arn,
            metric_name=metric_name,
            endpoint_name=args.endpoint_name,
            schedule_name=args.schedule_name,
            period=args.period,
            eval_periods=args.eval_periods,
            threshold=args.threshold,
        )

    print("\nNext checks:")
    print("1) Invoke the endpoint a few times (to generate DataCapture).")
    print("2) Wait for the next monitoring execution to run.")
    print("3) CloudWatch -> Metrics -> SageMaker -> Endpoint data-metric")
    print("   Look for: feature_baseline_drift_<feature> metrics.")
    print("4) If drift happens, alarms go to ALARM and SNS emails you.")


if __name__ == "__main__":
    main()