import argparse
import boto3
import json

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--region", required=True)
    p.add_argument("--schedule-name", required=True)
    p.add_argument("--email", required=True)

    p.add_argument("--sns-topic-name", default="aqeel-iris-monitor-alerts")
    p.add_argument("--alarm-name", default="aqeel-iris-monitor-violations")
    return p.parse_args()

def main():
    args = parse_args()

    sns = boto3.client("sns", region_name=args.region)
    cw = boto3.client("cloudwatch", region_name=args.region)

    topic = sns.create_topic(Name=args.sns_topic_name)
    topic_arn = topic["TopicArn"]

    sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=args.email)
    print("✅ SNS topic:", topic_arn)
    print("📩 Check your email and CONFIRM the subscription link.")

    # Model Monitor metrics use Namespace: AWS/SageMaker and dimensions include MonitoringSchedule
    cw.put_metric_alarm(
        AlarmName=args.alarm_name,
        AlarmDescription="Alarm when SageMaker Model Monitor detects constraint violations",
        Namespace="AWS/SageMaker",
        MetricName="ConstraintViolations",
        Dimensions=[
            {"Name": "MonitoringSchedule", "Value": args.schedule_name}
        ],
        Statistic="Sum",
        Period=3600,
        EvaluationPeriods=1,
        Threshold=1,
        ComparisonOperator="GreaterThanOrEqualToThreshold",
        AlarmActions=[topic_arn],
        TreatMissingData="notBreaching"
    )

    print("✅ CloudWatch Alarm created:", args.alarm_name)

if __name__ == "__main__":
    main()
