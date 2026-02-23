import argparse
import boto3

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--region", required=True)
    p.add_argument("--schedule-name", required=True)
    p.add_argument("--sns-topic-name", required=True)
    p.add_argument("--email", required=True)
    return p.parse_args()

def main():
    args = parse_args()

    sns = boto3.client("sns", region_name=args.region)
    cw = boto3.client("cloudwatch", region_name=args.region)

    # Create SNS topic
    topic = sns.create_topic(Name=args.sns_topic_name)
    topic_arn = topic["TopicArn"]
    print("SNS Topic:", topic_arn)

    # Subscribe email
    sns.subscribe(
        TopicArn=topic_arn,
        Protocol="email",
        Endpoint=args.email
    )
    print("Confirm subscription in your email.")

    # Create alarm on constraint violations
    cw.put_metric_alarm(
        AlarmName=f"{args.schedule_name}-drift-alarm",
        AlarmDescription="Drift detected by SageMaker Model Monitor",
        ActionsEnabled=True,
        AlarmActions=[topic_arn],
        MetricName="ConstraintViolations",
        Namespace="aws/sagemaker/ModelMonitoring",
        Statistic="Sum",
        Period=3600,
        EvaluationPeriods=1,
        Threshold=1,
        ComparisonOperator="GreaterThanOrEqualToThreshold",
        Dimensions=[
            {"Name": "MonitoringSchedule", "Value": args.schedule_name}
        ],
        TreatMissingData="notBreaching"
    )

    print("✅ Drift alarm created successfully.")

if __name__ == "__main__":
    main()