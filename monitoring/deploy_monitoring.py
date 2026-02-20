# monitoring/deploy_monitoring.py
import argparse
import hashlib
import io
import json
import textwrap
import time
import zipfile

import boto3
from botocore.exceptions import ClientError


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--region", required=True)

    p.add_argument("--bucket", required=True)
    p.add_argument("--datacapture-prefix", required=True)

    p.add_argument("--lambda-name", required=True)
    p.add_argument("--lambda-role-arn", required=True)

    p.add_argument("--sns-topic-name", required=True)
    p.add_argument("--alert-email", required=True)

    p.add_argument("--alarm-name", required=True)
    p.add_argument("--alarm-threshold", type=float, default=1.0)
    p.add_argument("--alarm-period", type=int, default=300)
    p.add_argument("--alarm-evaluation-periods", type=int, default=1)

    p.add_argument("--runtime", default="python3.11")
    p.add_argument("--memory-size", type=int, default=256)
    p.add_argument("--timeout", type=int, default=60)

    return p.parse_args()


def _normalize_prefix(prefix: str) -> str:
    if prefix.startswith("s3://"):
        raise ValueError(f"--datacapture-prefix must be a KEY prefix, not S3 URI: {prefix}")
    prefix = prefix.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return prefix


def _bucket_arn(bucket: str) -> str:
    return f"arn:aws:s3:::{bucket}"


def _safe_statement_id(s: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in "-_" else "-" for ch in s)
    return cleaned[:90]


def build_lambda_zip() -> bytes:
    # minimal lambda (no deps)
    code = textwrap.dedent(
        r"""
        import json
        import os
        import boto3

        s3 = boto3.client("s3")
        sns = boto3.client("sns")
        cw  = boto3.client("cloudwatch")

        SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")
        METRIC_NAMESPACE = os.environ.get("METRIC_NAMESPACE", "IrisRealtimeMonitoring")
        METRIC_NAME = os.environ.get("METRIC_NAME", "DriftAlerts")

        FEATURE_RANGES = {
            "sepal_length": (3.0, 9.0),
            "sepal_width":  (1.0, 5.0),
            "petal_length": (1.0, 7.0),
            "petal_width":  (0.0, 3.0),
        }

        def _publish_metric(count: int):
            try:
                cw.put_metric_data(
                    Namespace=METRIC_NAMESPACE,
                    MetricData=[{"MetricName": METRIC_NAME, "Value": float(count), "Unit": "Count"}],
                )
            except Exception as e:
                print("metric error:", repr(e))

        def _alert(msg: str):
            if not SNS_TOPIC_ARN:
                print("SNS_TOPIC_ARN not set; skip alert:", msg[:500])
                return
            sns.publish(TopicArn=SNS_TOPIC_ARN, Subject="Iris datacapture alert", Message=msg)

        def _parse_jsonl(body: str):
            for line in body.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except Exception:
                    continue

        def _extract_rows(item: dict):
            cap = item.get("captureData", {})
            inp = cap.get("endpointInput", {})
            data = inp.get("data", "")
            ctype = (inp.get("observedContentType") or "").lower()

            if "json" in ctype:
                try:
                    payload = json.loads(data)
                    if isinstance(payload, dict) and "instances" in payload:
                        rows = payload["instances"]
                    else:
                        rows = payload
                    return rows if isinstance(rows, list) else [rows]
                except Exception:
                    return []

            if "csv" in ctype:
                rows = []
                for ln in str(data).splitlines():
                    parts = [p.strip() for p in ln.split(",")]
                    if len(parts) >= 4:
                        rows.append(parts[:4])
                return rows

            return []

        def _validate_row(row):
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

            return ["invalid_row_format"]

        def handler(event, context):
            print("event:", json.dumps(event)[:2000])
            total = 0
            samples = []

            for rec in event.get("Records", []):
                try:
                    bucket = rec["s3"]["bucket"]["name"]
                    key = rec["s3"]["object"]["key"]
                except Exception:
                    continue

                if not key.endswith(".jsonl"):
                    continue

                obj = s3.get_object(Bucket=bucket, Key=key)
                body = obj["Body"].read().decode("utf-8", errors="ignore")

                for item in _parse_jsonl(body):
                    for row in _extract_rows(item):
                        issues = _validate_row(row)
                        if issues:
                            total += 1
                            if len(samples) < 5:
                                samples.append({"row": row, "issues": issues})

            _publish_metric(total)

            if total > 0:
                _alert(json.dumps({"issue_count": total, "samples": samples}, indent=2))
                return {"status": "alerted", "issues": total}

            return {"status": "ok", "issues": 0}
        """
    ).lstrip()

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", code)
    return buf.getvalue()


def wait_lambda_ready(lambda_client, fn_name: str, timeout_sec: int = 180):
    """
    Wait until Lambda is Active AND LastUpdateStatus is Successful.
    This prevents ResourceConflictException during rapid reruns.
    """
    start = time.time()
    last = None
    while True:
        cfg = lambda_client.get_function_configuration(FunctionName=fn_name)
        state = cfg.get("State")
        upd = cfg.get("LastUpdateStatus")
        reason = cfg.get("StateReason") or cfg.get("LastUpdateStatusReason") or ""
        last = (state, upd, reason)

        if state == "Active" and upd in (None, "Successful"):
            return

        if upd == "Failed" or state == "Failed":
            raise RuntimeError(f"Lambda {fn_name} update failed: state={state}, lastUpdate={upd}, reason={reason}")

        if time.time() - start > timeout_sec:
            raise TimeoutError(f"Timeout waiting for Lambda {fn_name} ready. Last={last}")

        time.sleep(5)


def call_with_retry(fn, *, max_attempts=12, sleep_sec=5):
    """
    Retry helper for Lambda ResourceConflictException.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("ResourceConflictException", "TooManyRequestsException"):
                if attempt == max_attempts:
                    raise
                time.sleep(sleep_sec)
                continue
            raise


def ensure_sns_topic(sns, name: str) -> str:
    return sns.create_topic(Name=name)["TopicArn"]


def ensure_email_subscription(sns, topic_arn: str, email: str):
    subs = sns.list_subscriptions_by_topic(TopicArn=topic_arn).get("Subscriptions", [])
    for s in subs:
        if s.get("Protocol") == "email" and s.get("Endpoint") == email:
            print("ℹ️ Email subscription already exists (confirm in inbox if pending).")
            return
    sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=email, ReturnSubscriptionArn=True)
    print("✅ Created email subscription (confirm in inbox).")


def ensure_lambda(lambda_client, args, sns_topic_arn: str) -> str:
    zip_bytes = build_lambda_zip()
    code_sha = hashlib.sha256(zip_bytes).hexdigest()[:12]

    env_vars = {
        "SNS_TOPIC_ARN": sns_topic_arn,
        "METRIC_NAMESPACE": "IrisRealtimeMonitoring",
        "METRIC_NAME": "DriftAlerts",
    }

    try:
        existing = lambda_client.get_function(FunctionName=args.lambda_name)
        fn_arn = existing["Configuration"]["FunctionArn"]
        print(f"ℹ️ Lambda exists: {args.lambda_name}")

        # IMPORTANT: wait for previous updates to finish
        wait_lambda_ready(lambda_client, args.lambda_name)

        # Update code (retry if conflict)
        call_with_retry(
            lambda: lambda_client.update_function_code(
                FunctionName=args.lambda_name,
                ZipFile=zip_bytes,
                Publish=True,
            )
        )
        wait_lambda_ready(lambda_client, args.lambda_name)

        # Update config (retry if conflict)
        call_with_retry(
            lambda: lambda_client.update_function_configuration(
                FunctionName=args.lambda_name,
                Role=args.lambda_role_arn,
                Runtime=args.runtime,
                Handler="lambda_function.handler",
                Timeout=args.timeout,
                MemorySize=args.memory_size,
                Environment={"Variables": env_vars},
            )
        )
        wait_lambda_ready(lambda_client, args.lambda_name)

        print(f"✅ Updated Lambda: {args.lambda_name} (sha={code_sha})")
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
        wait_lambda_ready(lambda_client, args.lambda_name)
        print(f"✅ Created Lambda: {args.lambda_name} (sha={code_sha})")
        return fn_arn


def ensure_lambda_invoke_permission(lambda_client, lambda_name: str, bucket: str):
    sid = _safe_statement_id(f"s3-invoke-{bucket}-{lambda_name}")
    try:
        lambda_client.add_permission(
            FunctionName=lambda_name,
            StatementId=sid,
            Action="lambda:InvokeFunction",
            Principal="s3.amazonaws.com",
            SourceArn=_bucket_arn(bucket),
        )
        print("✅ Added invoke permission for S3")
    except lambda_client.exceptions.ResourceConflictException:
        print("ℹ️ Invoke permission already exists")


def ensure_s3_trigger(s3, lambda_client, bucket: str, prefix: str, lambda_arn: str, lambda_name: str):
    prefix = _normalize_prefix(prefix)

    # Must exist BEFORE putting bucket notification (S3 validates destination)
    ensure_lambda_invoke_permission(lambda_client, lambda_name, bucket)

    current = s3.get_bucket_notification_configuration(Bucket=bucket)
    lambdas = current.get("LambdaFunctionConfigurations", [])
    topics = current.get("TopicConfigurations", [])
    queues = current.get("QueueConfigurations", [])

    rule_id = f"{lambda_name}-datacapture"
    lambdas = [c for c in lambdas if c.get("Id") != rule_id]
    lambdas.append(
        {
            "Id": rule_id,
            "LambdaFunctionArn": lambda_arn,
            "Events": ["s3:ObjectCreated:*"],
            "Filter": {"Key": {"FilterRules": [{"Name": "prefix", "Value": prefix}]}},
        }
    )

    notif = {"LambdaFunctionConfigurations": lambdas}
    if topics:
        notif["TopicConfigurations"] = topics
    if queues:
        notif["QueueConfigurations"] = queues

    s3.put_bucket_notification_configuration(Bucket=bucket, NotificationConfiguration=notif)
    print(f"✅ S3 trigger configured: s3://{bucket}/{prefix} -> {lambda_name}")


def ensure_alarm(cw, alarm_name: str, topic_arn: str, lambda_name: str, threshold: float, period: int, eval_periods: int):
    cw.put_metric_alarm(
        AlarmName=alarm_name,
        AlarmDescription="Alarm when drift-monitor Lambda errors occur.",
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


def main():
    args = parse_args()
    prefix = _normalize_prefix(args.datacapture_prefix)

    sess = boto3.Session(region_name=args.region)
    s3 = sess.client("s3")
    lam = sess.client("lambda")
    sns = sess.client("sns")
    cw = sess.client("cloudwatch")

    # 1) SNS
    topic_arn = ensure_sns_topic(sns, args.sns_topic_name)
    print("✅ SNS topic:", topic_arn)
    ensure_email_subscription(sns, topic_arn, args.alert_email)

    # 2) Lambda
    lambda_arn = ensure_lambda(lam, args, topic_arn)

    # 3) S3 trigger
    ensure_s3_trigger(s3, lam, args.bucket, prefix, lambda_arn, args.lambda_name)

    # 4) Alarm
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
    print("Next:")
    print(f"1) Confirm SNS email subscription: {args.alert_email}")
    print(f"2) Invoke endpoint to generate datacapture under s3://{args.bucket}/{prefix}")
    print("3) If needed, check Lambda logs in CloudWatch Logs.")


if __name__ == "__main__":
    main()