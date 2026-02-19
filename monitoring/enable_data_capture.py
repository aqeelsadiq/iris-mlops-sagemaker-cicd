# monitoring/enable_data_capture.py

import argparse
import hashlib
import boto3
from botocore.exceptions import ClientError


def _safe_name(base: str, seed: str) -> str:
    base = base.replace("_", "-")
    base = "".join(c for c in base if c.isalnum() or c == "-").strip("-")
    h = hashlib.sha1(seed.encode()).hexdigest()[:8]
    name = f"{base}-{h}"
    return name[:63]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--region", required=True)
    p.add_argument("--endpoint-name", required=True)
    p.add_argument("--capture-s3-uri", required=True)
    p.add_argument("--sampling-percentage", type=int, default=100)
    return p.parse_args()


def main():
    args = parse_args()
    sm = boto3.client("sagemaker", region_name=args.region)

    # 1️⃣ Get endpoint
    print("Describing endpoint...")
    ep = sm.describe_endpoint(EndpointName=args.endpoint_name)
    current_cfg_name = ep["EndpointConfigName"]
    print("Current endpoint config:", current_cfg_name)

    # 2️⃣ Try to describe endpoint config
    try:
        cfg = sm.describe_endpoint_config(
            EndpointConfigName=current_cfg_name
        )
        print("Endpoint config found.")
    except ClientError as e:
        print("❌ Endpoint config missing:", current_cfg_name)
        print("This usually happens after failed update.")
        print("Please redeploy endpoint or check existing configs.")
        raise e

    if not cfg.get("ProductionVariants"):
        raise RuntimeError("No ProductionVariants found.")

    # 3️⃣ Create new config with DataCapture enabled
    new_cfg_name = _safe_name(
        base=f"{args.endpoint_name}-capture",
        seed=current_cfg_name + args.capture_s3_uri,
    )

    print("Creating new endpoint config:", new_cfg_name)

    sm.create_endpoint_config(
        EndpointConfigName=new_cfg_name,
        ProductionVariants=cfg["ProductionVariants"],
        DataCaptureConfig={
            "EnableCapture": True,
            "InitialSamplingPercentage": args.sampling_percentage,
            "DestinationS3Uri": args.capture_s3_uri,
            "CaptureOptions": [
                {"CaptureMode": "Input"},
                {"CaptureMode": "Output"},
            ],
        },
    )

    # 4️⃣ Update endpoint
    print("Updating endpoint to new config...")
    sm.update_endpoint(
        EndpointName=args.endpoint_name,
        EndpointConfigName=new_cfg_name,
    )

    print("✅ Data capture enabled successfully.")
    print("New config:", new_cfg_name)


if __name__ == "__main__":
    main()
