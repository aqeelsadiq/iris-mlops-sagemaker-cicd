# monitoring/create_alarm_sns.py
import argparse
import boto3
import hashlib


def _topic_name(schedule_name: str) -> str:
    # SNS topic name max 256, safe chars; keep it short anyway
    safe = "".join(ch if ch.isalnum() or ch in "-_" else "-" for ch in schedule_name)[:80]
    h = hashlib.sha1(schedule_name.encode("utf-8")).hexdigest()[:8]
    return f"{safe}-{h}"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--region", required=True)
    p.add_argument("--schedule-name", required=True)
    p.add_argument("--email", required=True)
    return p.parse_args()


def main():
    args = parse_args()

    sns = boto3.client("sns", region_name=args.region)
    events = boto3.client("events", region_name=args.region)

    topic_name = _topic_name(args.schedule_name)
    topic = sns.create_topic(Name=topic_name)
    topic_arn = topic["TopicArn"]

    print("SNS topic:", topic_arn)

    # Subscribe email (user must confirm via email)
    sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=args.email)
    print("✅ Subscription created. Confirm it from your email:", args.email)

    # EventBridge rule: SageMaker Processing Job state change -> Failed
    # We filter failures; optionally also filter job name prefix (best-effort).
    rule_name = f"{topic_name}-processing-failed"[:64]
    pattern = {
        "source": ["aws.sagemaker"],
        "detail-type": ["SageMaker Processing Job State Change"],
        "detail": {"ProcessingJobStatus": ["Failed", "Stopped"]},
    }

    events.put_rule(
        Name=rule_name,
        EventPattern=str(pattern).replace("'", '"'),
        State="ENABLED",
        Description=f"Notify when SageMaker Model Monitor processing job fails for schedule {args.schedule_name}",
    )

    # Add SNS topic target
    events.put_targets(
        Rule=rule_name,
        Targets=[
            {
                "Id": "snsTarget",
                "Arn": topic_arn,
            }
        ],
    )

    print("✅ EventBridge rule created:", rule_name)
    print("   It will notify on FAILED/STOPPED monitoring processing jobs.")
    print("   (Make sure you confirm the SNS subscription email.)")


if __name__ == "__main__":
    main()
