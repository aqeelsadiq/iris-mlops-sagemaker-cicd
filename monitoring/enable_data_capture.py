# monitoring/enable_data_capture.py
import argparse
import time
import datetime as dt
import uuid
import boto3


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--region", required=True)
    p.add_argument("--endpoint-name", required=True)
    p.add_argument("--capture-s3-uri", required=True)  # e.g. s3://.../datacapture/
    p.add_argument("--sampling-percentage", type=int, default=100)
    return p.parse_args()


def short_config_name(endpoint_name: str) -> str:
    # Max 63 chars. Keep it short + unique.
    ts = dt.datetime.utcnow().strftime("%y%m%d%H%M")
    rand = uuid.uuid4().hex[:6]
    base = f"{endpoint_name}-dc-{ts}-{rand}"
    return base[:63]


def wait_for_endpoint(sm, endpoint_name: str, timeout_sec: int = 1800):
    start = time.time()
    last = None
    while True:
        desc = sm.describe_endpoint(EndpointName=endpoint_name)
        status = desc["EndpointStatus"]
        if status != last:
            print("Endpoint status:", status)
            last = status

        if status == "InService":
            return desc
        if status in ("Failed", "OutOfService"):
            raise RuntimeError(f"Endpoint entered bad state: {status} :: {desc.get('FailureReason')}")

        if time.time() - start > timeout_sec:
            raise TimeoutError("Timed out waiting for endpoint to become InService")

        time.sleep(15)


def main():
    args = parse_args()
    sm = boto3.client("sagemaker", region_name=args.region)

    # 1) Ensure endpoint is InService
    endpoint_desc = wait_for_endpoint(sm, args.endpoint_name)
    current_cfg = endpoint_desc["EndpointConfigName"]
    cfg_desc = sm.describe_endpoint_config(EndpointConfigName=current_cfg)

    # 2) If data capture already enabled, exit cleanly
    if "DataCaptureConfig" in cfg_desc and cfg_desc["DataCaptureConfig"].get("EnableCapture", False):
        print("✅ Data capture already enabled on endpoint config:", current_cfg)
        return

    # 3) Create a NEW endpoint config that copies production variants + adds DataCaptureConfig
    new_cfg_name = short_config_name(args.endpoint_name)

    production_variants = cfg_desc["ProductionVariants"]

    # Copy optional fields safely if present
    request = {
        "EndpointConfigName": new_cfg_name,
        "ProductionVariants": production_variants,
        "DataCaptureConfig": {
            "EnableCapture": True,
            "InitialSamplingPercentage": args.sampling_percentage,
            "DestinationS3Uri": args.capture_s3_uri,
            "CaptureOptions": [{"CaptureMode": "Input"}, {"CaptureMode": "Output"}],
            "CaptureContentTypeHeader": {
                "CsvContentTypes": ["text/csv"],
                "JsonContentTypes": ["application/json"],
            },
        },
    }

    # Some endpoint configs have extra fields; preserve them if present
    if "KmsKeyId" in cfg_desc:
        request["KmsKeyId"] = cfg_desc["KmsKeyId"]
    if "AsyncInferenceConfig" in cfg_desc:
        request["AsyncInferenceConfig"] = cfg_desc["AsyncInferenceConfig"]
    if "ShadowProductionVariants" in cfg_desc:
        request["ShadowProductionVariants"] = cfg_desc["ShadowProductionVariants"]
    if "ExplainerConfig" in cfg_desc:
        request["ExplainerConfig"] = cfg_desc["ExplainerConfig"]

    sm.create_endpoint_config(**request)
    print("✅ Created endpoint config:", new_cfg_name)

    # 4) Update endpoint to use the new config
    sm.update_endpoint(EndpointName=args.endpoint_name, EndpointConfigName=new_cfg_name)
    print("✅ Updating endpoint to new config (this can take a few minutes)...")

    # 5) Wait until endpoint returns to InService
    wait_for_endpoint(sm, args.endpoint_name)
    print("✅ Endpoint updated and InService. Data capture enabled.")


if __name__ == "__main__":
    main()
