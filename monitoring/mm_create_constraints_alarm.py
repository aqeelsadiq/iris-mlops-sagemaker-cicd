"""
mm_create_constraints_alarm.py
------------------------------
Creates CloudWatch alarm for constraint violations emitted by Model Monitor,
and sends email via SNS.

This is the correct alarm type for your setup (not drift alarms).
"""

import argparse
import boto3


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--region", required=True)
    p.add_argument("--endpoint-name", required=True)
    p.add_argument("--schedule-name", required=True)

    p.add_argument("--sns-topic-name", required=True)
    p.add_argument("--email", required=True)

    p.add_argument("--period", type=int, default=300)
    p.add_argument("--eval-periods", type=int, default=1)
    p.add_argument("--threshold", type=float, default=1.0)
    return p.parse_args()


def ensure_sns_topic(sns, topic_name: str, email: str) -> str:
    topic_arn = sns.create_topic(Name=topic_name)["TopicArn"]
    sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=email)
    print(f"✅ SNS Topic: {topic_arn}")
    print(f"📩 Subscription sent to {email} (CONFIRM in email once).")
    return topic_arn


def pick_violation_metric(cw, endpoint_name: str, schedule_name: str):
    """
    Discover the actual violation metric name + namespace by scanning metrics
    that contain 'violation' and match endpoint + schedule dimensions.
    """
    # Common namespaces where Model Monitor publishes
    namespaces = [
        "/aws/sagemaker/ModelMonitoring",
        "aws/sagemaker/ModelMonitoring",
    ]

    def dims_match(dims):
        has_ep = any(d.get("Name") == "EndpointName" and d.get("Value") == endpoint_name for d in dims)
        has_sched = any(
            d.get("Value") == schedule_name and d.get("Name") in ("ScheduleName", "MonitoringScheduleName")
            for d in dims
        )
        return has_ep and has_sched

    candidates = []
    for ns in namespaces:
        resp = cw.list_metrics(Namespace=ns)
        for m in resp.get("Metrics", []):
            name = m.get("MetricName", "")
            if "violation" not in name.lower():
                continue
            dims = m.get("Dimensions", [])
            if dims_match(dims):
                candidates.append((ns, name, dims))

    if not candidates:
        return None, None, None

    # Prefer a metric that looks like total violations
    candidates.sort(key=lambda x: ("total" not in x[1].lower(), x[1]))
    return candidates[0]


def create_alarm(cw, *, alarm_name, topic_arn, namespace, metric_name, dimensions, period, eval_periods, threshold):
    cw.put_metric_alarm(
        AlarmName=alarm_name,
        AlarmDescription=f"Model Monitor constraint violations detected ({metric_name})",
        ActionsEnabled=True,
        AlarmActions=[topic_arn],
        OKActions=[topic_arn],
        Namespace=namespace,
        MetricName=metric_name,
        Statistic="Sum",
        Period=period,
        EvaluationPeriods=eval_periods,
        Threshold=threshold,
        ComparisonOperator="GreaterThanOrEqualToThreshold",
        Dimensions=dimensions,
        TreatMissingData="notBreaching",
    )
    print(f"✅ Alarm created: {alarm_name}")
    print(f"   Namespace : {namespace}")
    print(f"   Metric    : {metric_name}")
    print(f"   Dims      : {dimensions}")


def main():
    args = parse_args()
    sns = boto3.client("sns", region_name=args.region)
    cw = boto3.client("cloudwatch", region_name=args.region)

    topic_arn = ensure_sns_topic(sns, args.sns_topic_name, args.email)

    ns, metric, dims = pick_violation_metric(cw, args.endpoint_name, args.schedule_name)
    if not ns:
        raise RuntimeError(
            "❌ Could not find any 'violation' CloudWatch metric for your endpoint+schedule.\n"
            "Run at least ONE monitoring execution successfully, then rerun this script."
        )

    alarm_name = f"{args.schedule_name}-constraint-violations"
    create_alarm(
        cw,
        alarm_name=alarm_name,
        topic_arn=topic_arn,
        namespace=ns,
        metric_name=metric,
        dimensions=dims,
        period=args.period,
        eval_periods=args.eval_periods,
        threshold=args.threshold,
    )

    print("\n✅ Done. Now:")
    print("1) Confirm SNS subscription email.")
    print("2) Send bad inputs so violations occur.")
    print("3) After the next schedule run, alarm will email you.")


if __name__ == "__main__":
    main()