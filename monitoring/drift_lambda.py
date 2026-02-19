# monitoring/deploy_monitoring.py
"""
Deploy "real-time" monitoring for SageMaker endpoint data capture using:
- Lambda (process new datacapture objects written to S3)
- S3 event trigger on the datacapture prefix
- SNS topic + email subscription for alerts
- CloudWatch alarm on a Lambda metric (Errors by default)

This script is idempotent:
- Reuses resources if they already exist
- Preserves existing bucket notification configs (does NOT wipe others)
- Adds Lambda invoke permission BEFORE attaching S3 notification (required by S3 validation)

Example:
python monitoring/deploy_monitoring.py \
  --region us-east-1 \
  --bucket sagemaker-aqeel-iris-us-east-1-387867038403 \
  --datacapture-prefix monitoring/datacapture/ \
  --lambda-name aqeel-iris-drift-monitor \
  --sns-topic-name aqeel-iris-monitor-alerts \
  --alarm-name aqeel-iris-monitor-lambda-errors \
  --alert-email aqeel.sadiq3456@gmail.com \
  --lambda-role-arn arn:aws:iam::387867038403:role/AqeelIrisLambdaExecutionRole

Notes:
- You MUST provide a Lambda execution role that Lambda can assume (trust policy principal: lambda.amazonaws.com)
  and has permissions for S3 read, SNS publish, and CloudWatch logs.
- GitHub Actions runner must have IAM permissions to create/update Lambda, SNS, CloudWatch, and S3 notifications.
"""

import argparse
import base64
import hashlib
import io
import json
import os
import textwrap
import time
import zipfile
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError


# -----------------------------
# Args
# -----------------------------
def parse_args():
    p = argparse.ArgumentParser()

    p.add_argument("--region", required=True)

    p.add_argument("--bucket", required=True)
    p.add_argument("--datacapture-prefix", required=True, help="S3 key prefix (not s3://...) e.g. monitoring/datacapture/")

    p.add_argument("--lambda-name", required=True)
    p.add_argument("--lambda-role-arn", required=True)

    p.add_argument("--sns-topic-name", required=True)
    p.add_argument("--alert-email", required=True)

    p.add_argument("--alarm-name", required=True)
    p.add_argument("--alarm-threshold", type=float, default=1.0)
    p.add_argument("--alarm-period", type=int, default=300)
    p.add_argument("--alarm-evaluation-periods", type=int, default=1)

    # Optional tuning
    p.add_argument("--runtime", default="python3.11")
    p.add_argument("--memory-size", type=int, default=256)
    p.add_argument("--timeout", type=int, default=60)

    return p.parse_args()


# -----------------------------
# Helpers
# -----------------------------
def _now_suffix() -> str:
    return time.strftime("%Y%m%d%H%M%S", time.gmtime())


def _safe_statement_id(s: str) -> str:
    # Lambda StatementId constraints: <= 100 chars, letters/numbers/-/_ only
    cleaned = "".join(ch if ch.isalnum() or ch in "-_" else "-" for ch in s)
    return cleaned[:90]  # keep room


def _bucket_arn(bucket: str) -> str:
    return f"arn:aws:s3:::{bucket}"


def _normalize_prefix(prefix: str) -> str:
    # Must be key prefix only, no scheme
    if prefix.startswith("s3://"):
        raise ValueError(f"--datacapture-prefix must be an S3 KEY prefix, not a URI: {prefix}")
    prefix = prefix.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return prefix


def _get_bucket_region(s3, bucket: str) -> str:
    # For us-east-1, LocationConstraint is often None
    resp = s3.get_bucket_location(Bucket=bucket)
    loc = resp.get("LocationConstraint")
    return "us-east-1" if not loc else loc


# -----------------------------
# Lambda package
# -----------------------------
def build_lambda_zip() -> bytes:
    """
    Build a minimal Lambda function (no external deps) that:
    - Reads S3 object(s) from event
    - Parses JSON Lines (Model Monitor datacapture format)
    - Emits simple drift-like checks (very basic example)
    - Publishes alert to SNS when suspicious

    IMPORTANT: This is a lightweight "real-time" alerting approach.
    It's NOT the same as SageMaker Model Monitor schedule jobs, but it works even when processing quotas are 0.
    """
    lambda_py = textwrap.dedent(
        r"""
        import json
        import os
        import boto3

        s3 = boto3.client("s3")
        sns = boto3.client("sns")
        cw = boto3.client("cloudwatch")

        SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")
        METRIC_NAMESPACE = os.environ.get("METRIC_NAMESPACE", "IrisRealtimeMonitoring")
        METRIC_NAME = os.environ.get("METRIC_NAME", "DriftAlerts")

        # Example thresholds (tune as needed)
        # Here we just sanity-check numeric fields ranges for Iris features
        FEATURE_RANGES = {
            "sepal_length": (3.0, 9.0),
            "sepal_width": (1.0, 5.0),
            "petal_length": (1.0, 7.0),
            "petal_width": (0.0, 3.0),
        }

        def _publish_metric(count: int):
            try:
                cw.put_metric_data(
                    Namespace=METRIC_NAMESPACE,
                    MetricData=[{
                        "MetricName": METRIC_NAME,
                        "Value": float(count),
                        "Unit": "Count"
                    }]
                )
            except Exception as e:
                print("Failed to publish metric:", repr(e))

        def _alert(message: str, subject: str = "SageMaker datacapture alert"):
            if not SNS_TOPIC_ARN:
                print("SNS_TOPIC_ARN not set; skipping alert:", message)
                return
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=subject[:100],
                Message=message
            )

        def _parse_jsonl(content: str):
            for line in content.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except Exception:
                    # ignore malformed line
                    continue

        def _extract_features(record):
            # Datacapture record format:
            # record["captureData"]["endpointInput"]["data"] is a string payload (CSV or JSON)
            # We'll handle JSON that includes "instances" or direct list; CSV is harder without schema.
            try:
                cap = record.get("captureData", {})
                endpoint_input = cap.get("endpointInput", {})
                data = endpoint_input.get("data", "")
                observed = endpoint_input.get("observedContentType", "")
            except Exception:
                return None

            # JSON content types
            if "json" in (observed or "").lower():
                try:
                    payload = json.loads(data)
                    if isinstance(payload, dict) and "instances" in payload:
                        rows = payload["instances"]
                    else:
                        rows = payload
                    # Expect one row; if many, validate all
                    return rows
                except Exception:
                    return None

            # CSV: could be raw "5.1,3.5,1.4,0.2" — we can attempt parse
            if "csv" in (observed or "").lower():
                try:
                    # may include multiple rows separated by \n
                    rows = []
                    for ln in str(data).splitlines():
                        ln = ln.strip()
                        if not ln:
                            continue
                        parts = [p.strip() for p in ln.split(",")]
                        if len(parts) >= 4:
                            rows.append(parts[:4])
                    return rows
                except Exception:
                    return None

            return None

        def _validate_row(row):
            # Row can be dict with feature names OR list of 4 values
            issues = []

            if isinstance(row, dict):
                for k, (lo, hi) in FEATURE_RANGES.items():
                    if k not in row:
                        issues.append(f"missing:{k}")
                        continue
                    try:
                        v = float(row[k])
                        if v < lo or v > hi:
                            issues.append(f"out_of_range:{k}={v}")
                    except Exception:
                        issues.append(f"not_numeric:{k}")
                return issues

            # list/tuple: map to fixed order
            if isinstance(row, (list, tuple)) and len(row) >= 4:
                keys = ["sepal_length", "sepal_width", "petal_length", "petal_width"]
                for i, k in enumerate(keys):
                    lo, hi = FEATURE_RANGES[k]
                    try:
                        v = float(row[i])
                        if v < lo or v > hi:
                            issues.append(f"out_of_range:{k}={v}")
                    except Exception:
                        issues.append(f"not_numeric:{k}")
                return issues

            issues.append("invalid_row_format")
            return issues

        def handler(event, context):
            print("Event:", json.dumps(event)[:2000])

            total_issues = 0
            samples = []

            for rec in event.get("Records", []):
                try:
                    bucket = rec["s3"]["bucket"]["name"]
                    key = rec["s3"]["object"]["key"]
                except Exception:
                    continue

                # Only process jsonl files
                if not key.endswith(".jsonl"):
                    continue

                obj = s3.get_object(Bucket=bucket, Key=key)
                body = obj["Body"].read().decode("utf-8", errors="ignore")

                for item in _parse_jsonl(body):
                    rows = _extract_features(item)
                    if rows is None:
                        continue

                    # Normalize rows iterable
                    if isinstance(rows, dict):
                        rows_iter = [rows]
                    elif isinstance(rows, list):
                        rows_iter = rows
                    else:
                        rows_iter = [rows]

                    for row in rows_iter:
                        issues = _validate_row(row)
                        if issues:
                            total_issues += 1
                            if len(samples) < 5:
                                samples.append({"row": row, "issues": issues})

            # Emit metric always
            _publish_metric(total_issues)

            if total_issues > 0:
                msg = {
                    "message": "Detected potential data issues in datacapture.",
                    "issue_count": total_issues,
                    "samples": samples,
                }
                _alert(json.dumps(msg, indent=2), subject="Iris datacapture: potential drift/data issues")
                return {"status": "alerted", "issues": total_issues}

            return {"status": "ok", "issues": 0}
        """
    ).lstrip()

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", lambda_py)
    return buf.getvalue()


def ensure_lambda(lambda_client, args, sns_topic_arn: str) -> str:
    """
    Create or update Lambda function code/config.
    Returns Lambda function ARN.
    """
    zip_bytes = build_lambda_zip()
    code_sha = hashlib.sha256(zip_bytes).hexdigest()

    env_vars = {
        "SNS_TOPIC_ARN": sns_topic_arn,
        "METRIC_NAMESPACE": "IrisRealtimeMonitoring",
        "METRIC_NAME": "DriftAlerts",
    }

    try:
        resp = lambda_client.get_function(FunctionName=args.lambda_name)
        fn_arn = resp["Configuration"]["FunctionArn"]
        print(f"ℹ️ Lambda exists: {args.lambda_name}")

        # Update code
        lambda_client.update_function_code(
            FunctionName=args.lambda_name,
            ZipFile=zip_bytes,
            Publish=True,
        )

        # Update config
        lambda_client.update_function_configuration(
            FunctionName=args.lambda_name,
            Role=args.lambda_role_arn,
            Runtime=args.runtime,
            Handler="lambda_function.handler",
            Timeout=args.timeout,
            MemorySize=args.memory_size,
            Environment={"Variables": env_vars},
        )

        print(f"✅ Updated Lambda: {args.lambda_name} (code sha256={code_sha[:12]}...)")
        # After publish, config ARN is same base ARN
        return fn_arn

    except lambda_client.exceptions.ResourceNotFoundException:
        resp = lambda_client.create_function(
            FunctionName=args.lambda_name,
            Runtime=args.runtime,
            Role=args.lambda_role_arn,
            Handler="lambda_function.handler",
            Code={"ZipFile": zip_bytes},
            Timeout=args.timeout,
            MemorySize=args.memory_size,
            Publish=True,
            Environment={"Variables": env_vars},
        )
        fn_arn = resp["FunctionArn"]
        print(f"✅ Created Lambda: {args.lambda_name} (code sha256={code_sha[:12]}...)")
        return fn_arn


def ensure_lambda_invoke_permission(lambda_client, lambda_name: str, bucket: str) -> None:
    """
    Allow S3 to invoke Lambda. MUST exist before you attach S3 notification,
    otherwise S3 rejects PutBucketNotificationConfiguration with InvalidArgument.
    """
    statement_id = _safe_statement_id(f"s3-invoke-{bucket}-{lambda_name}")
    try:
        lambda_client.add_permission(
            FunctionName=lambda_name,
            StatementId=statement_id,
            Action="lambda:InvokeFunction",
            Principal="s3.amazonaws.com",
            SourceArn=_bucket_arn(bucket),
        )
        print("✅ Added invoke permission for S3")
    except lambda_client.exceptions.ResourceConflictException:
        print("ℹ️ Invoke permission already exists")


def ensure_s3_trigger(s3, lambda_client, bucket: str, prefix: str, lambda_arn: str, lambda_name: str) -> None:
    """
    Add an S3 ObjectCreated trigger for the given prefix to the Lambda.
    Preserves existing notifications for the bucket.
    """
    prefix = _normalize_prefix(prefix)

    # ✅ REQUIRED ORDER: permission first, then notification
    ensure_lambda_invoke_permission(lambda_client, lambda_name, bucket)

    # Get existing notifications (preserve them)
    current = s3.get_bucket_notification_configuration(Bucket=bucket)
    existing_lambda_cfgs = current.get("LambdaFunctionConfigurations", [])
    existing_topic_cfgs = current.get("TopicConfigurations", [])
    existing_queue_cfgs = current.get("QueueConfigurations", [])

    rule_id = f"{lambda_name}-datacapture"
    # Remove our rule if present (idempotent replace)
    kept = [c for c in existing_lambda_cfgs if c.get("Id") != rule_id]

    kept.append(
        {
            "Id": rule_id,
            "LambdaFunctionArn": lambda_arn,
            "Events": ["s3:ObjectCreated:*"],
            "Filter": {"Key": {"FilterRules": [{"Name": "prefix", "Value": prefix}]}},
        }
    )

    notif = {"LambdaFunctionConfigurations": kept}
    if existing_topic_cfgs:
        notif["TopicConfigurations"] = existing_topic_cfgs
    if existing_queue_cfgs:
        notif["QueueConfigurations"] = existing_queue_cfgs

    s3.put_bucket_notification_configuration(Bucket=bucket, NotificationConfiguration=notif)
    print(f"✅ S3 trigger configured on s3://{bucket}/{prefix} -> {lambda_name}")


# -----------------------------
# SNS
# -----------------------------
def ensure_sns_topic(sns, topic_name: str) -> str:
    resp = sns.create_topic(Name=topic_name)
    topic_arn = resp["TopicArn"]
    print("✅ SNS topic:", topic_arn)
    return topic_arn


def ensure_email_subscription(sns, topic_arn: str, email: str) -> None:
    # SNS create_topic is idempotent, but subscribe can duplicate.
    # We'll list and only subscribe if missing.
    subs = sns.list_subscriptions_by_topic(TopicArn=topic_arn).get("Subscriptions", [])
    for s in subs:
        if s.get("Protocol") == "email" and s.get("Endpoint") == email:
            print("ℹ️ Email subscription already exists (check email to confirm if pending).")
            return

    sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=email, ReturnSubscriptionArn=True)
    print("✅ Created email subscription. IMPORTANT: confirm the subscription from your email inbox.")


# -----------------------------
# CloudWatch Alarm
# -----------------------------
def ensure_alarm(cw, alarm_name: str, topic_arn: str, lambda_name: str, threshold: float, period: int, eval_periods: int):
    """
    Alarm on Lambda Errors >= threshold (default 1 in 5 minutes)
    """
    cw.put_metric_alarm(
        AlarmName=alarm_name,
        AlarmDescription="Alarm when Lambda errors occur in drift monitor.",
        Namespace="AWS/Lambda",
        MetricName="Errors",
        Dimensions=[{"Name": "FunctionName", "Value": lambda_name}],
        Statistic="Sum",
        Period=period,
        EvaluationPeriods=eval_periods,
        Threshold=threshold,
        ComparisonOperator="GreaterThanOrEqualToThreshold",
        TreatMissingData="notBreaching",
        AlarmActions=[topic_arn],
        OKActions=[topic_arn],
    )
    print("✅ CloudWatch alarm ensured:", alarm_name)


# -----------------------------
# Main
# -----------------------------
def main():
    args = parse_args()

    boto_sess = boto3.Session(region_name=args.region)
    s3 = boto_sess.client("s3")
    lambda_client = boto_sess.client("lambda")
    sns = boto_sess.client("sns")
    cw = boto_sess.client("cloudwatch")

    # Validate region alignment
    bucket_region = _get_bucket_region(s3, args.bucket)
    if bucket_region != args.region:
        raise RuntimeError(
            f"Bucket region mismatch: bucket={args.bucket} is in {bucket_region} but you set --region {args.region}. "
            f"Use the bucket region."
        )

    prefix = _normalize_prefix(args.datacapture_prefix)

    # 1) SNS topic + email subscription
    topic_arn = ensure_sns_topic(sns, args.sns_topic_name)
    ensure_email_subscription(sns, topic_arn, args.alert_email)

    # 2) Lambda create/update
    lambda_arn = ensure_lambda(lambda_client, args, topic_arn)

    # 3) S3 notification trigger
    ensure_s3_trigger(s3, lambda_client, args.bucket, prefix, lambda_arn, args.lambda_name)

    # 4) CloudWatch alarm
    ensure_alarm(
        cw,
        alarm_name=args.alarm_name,
        topic_arn=topic_arn,
        lambda_name=args.lambda_name,
        threshold=args.alarm_threshold,
        period=args.alarm_period,
        eval_periods=args.alarm_evaluation_periods,
    )

    print("✅ Realtime monitoring deployed successfully.")
    print("Next steps:")
    print(f"1) Confirm SNS email subscription for: {args.alert_email}")
    print("2) Invoke your SageMaker endpoint to generate datacapture objects.")
    print(f"3) Verify new objects land under: s3://{args.bucket}/{prefix}")
    print("4) Check Lambda logs in CloudWatch Logs if needed.")


if __name__ == "__main__":
    main()