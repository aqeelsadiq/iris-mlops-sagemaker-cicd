import argparse
import json
import os
import time
import zipfile
import boto3


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--region", required=True)
    p.add_argument("--lambda-name", required=True)
    p.add_argument("--lambda-role-arn", required=True)
    p.add_argument("--bucket", required=True)
    p.add_argument("--datacapture-prefix", required=True)
    p.add_argument("--baseline-s3-uri", required=True)
    p.add_argument("--metric-namespace", required=True)
    p.add_argument("--alarm-name", required=True)
    p.add_argument("--drift-threshold", type=float, required=True)
    p.add_argument("--alert-email", required=True)
    return p.parse_args()


def zip_lambda(zip_path: str):
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        z.write("monitoring/drift_lambda.py", arcname="drift_lambda.py")


def ensure_lambda(lambda_client, name, role_arn, env_vars):
    zip_path = "/tmp/lambda.zip"
    zip_lambda(zip_path)

    with open(zip_path, "rb") as f:
        code_bytes = f.read()

    try:
        lambda_client.get_function(FunctionName=name)
        lambda_client.update_function_code(FunctionName=name, ZipFile=code_bytes)
        lambda_client.update_function_configuration(
            FunctionName=name,
            Runtime="python3.11",
            Handler="drift_lambda.lambda_handler",
            Role=role_arn,
            Timeout=60,
            MemorySize=256,
            Environment={"Variables": env_vars},
        )
        print("✅ Updated Lambda:", name)
    except lambda_client.exceptions.ResourceNotFoundException:
        lambda_client.create_function(
            FunctionName=name,
            Runtime="python3.11",
            Handler="drift_lambda.lambda_handler",
            Role=role_arn,
            Code={"ZipFile": code_bytes},
            Timeout=60,
            MemorySize=256,
            Environment={"Variables": env_vars},
            Publish=True,
        )
        print("✅ Created Lambda:", name)


def ensure_s3_trigger(s3, lambda_client, bucket, prefix, lambda_arn, lambda_name):
    # allow s3 to invoke
    stmt_id = f"s3invoke-{bucket}-{prefix.replace('/', '-')}".replace("--", "-")[:80]
    try:
        lambda_client.add_permission(
            FunctionName=lambda_name,
            StatementId=stmt_id,
            Action="lambda:InvokeFunction",
            Principal="s3.amazonaws.com",
            SourceArn=f"arn:aws:s3:::{bucket}",
        )
        print("✅ Added invoke permission for S3")
    except lambda_client.exceptions.ResourceConflictException:
        pass

    notif = s3.get_bucket_notification_configuration(Bucket=bucket)
    lambda_cfgs = notif.get("LambdaFunctionConfigurations", [])

    # remove existing with same lambda+prefix (avoid duplicates)
    new_cfgs = []
    for c in lambda_cfgs:
        rules = c.get("Filter", {}).get("Key", {}).get("FilterRules", [])
        p = None
        for r in rules:
            if r.get("Name") == "prefix":
                p = r.get("Value")
        if not (c.get("LambdaFunctionArn") == lambda_arn and p == prefix):
            new_cfgs.append(c)

    new_cfgs.append(
        {
            "LambdaFunctionArn": lambda_arn,
            "Events": ["s3:ObjectCreated:*"],
            "Filter": {"Key": {"FilterRules": [{"Name": "prefix", "Value": prefix}]}},
        }
    )

    s3.put_bucket_notification_configuration(
        Bucket=bucket,
        NotificationConfiguration={"LambdaFunctionConfigurations": new_cfgs},
    )
    print(f"✅ S3 trigger set: s3://{bucket}/{prefix} -> {lambda_name}")


def ensure_sns_and_alarm(region, metric_ns, alarm_name, threshold, email):
    sns = boto3.client("sns", region_name=region)
    cw = boto3.client("cloudwatch", region_name=region)

    topic = sns.create_topic(Name=f"{alarm_name}-topic")
    topic_arn = topic["TopicArn"]

    # subscribe email (user must confirm email once)
    sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=email)
    print("✅ SNS topic + email subscription created (confirm the email once):", email)

    # Alarm: triggers if ANY feature PSI > threshold (we alarm on maximum across dimensions by using metric math is more complex)
    # Simple approach: alarm per-feature is best. Here we alarm on a single feature dimension.
    # We'll create alarms for each feature.
    features = ["sepal_length", "sepal_width", "petal_length", "petal_width"]

    for f in features:
        cw.put_metric_alarm(
            AlarmName=f"{alarm_name}-{f}",
            AlarmDescription=f"Iris drift PSI alarm for {f}",
            ActionsEnabled=True,
            AlarmActions=[topic_arn],
            MetricName="PSI",
            Namespace=metric_ns,
            Statistic="Maximum",
            Period=300,
            EvaluationPeriods=1,
            Threshold=threshold,
            ComparisonOperator="GreaterThanThreshold",
            TreatMissingData="notBreaching",
            Dimensions=[{"Name": "Feature", "Value": f}],
        )
        print("✅ Alarm created:", f"{alarm_name}-{f}")


def main():
    args = parse_args()
    lambda_client = boto3.client("lambda", region_name=args.region)
    s3 = boto3.client("s3", region_name=args.region)

    env_vars = {
        "BASELINE_S3_URI": args.baseline_s3_uri,
        "METRIC_NAMESPACE": args.metric_namespace,
        "ENDPOINT_NAME": os.environ.get("ENDPOINT_NAME", "iris-endpoint"),
    }

    ensure_lambda(lambda_client, args.lambda_name, args.lambda_role_arn, env_vars)

    fn = lambda_client.get_function(FunctionName=args.lambda_name)
    lambda_arn = fn["Configuration"]["FunctionArn"]

    ensure_s3_trigger(s3, lambda_client, args.bucket, args.datacapture_prefix, lambda_arn, args.lambda_name)

    ensure_sns_and_alarm(
        args.region,
        args.metric_namespace,
        args.alarm_name,
        args.drift_threshold,
        args.alert_email,
    )

    print("✅ Monitoring deployed successfully.")


if __name__ == "__main__":
    main()