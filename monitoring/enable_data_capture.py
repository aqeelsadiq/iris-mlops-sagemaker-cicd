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


def _get_endpoint_config(sm, endpoint_name: str) -> dict:
    ep = sm.describe_endpoint(EndpointName=endpoint_name)
    cfg_name = ep["EndpointConfigName"]
    cfg = sm.describe_endpoint_config(EndpointConfigName=cfg_name)
    return cfg


def _capture_matches(cfg: dict, capture_s3_uri: str, sampling: int) -> bool:
    dc = cfg.get("DataCaptureConfig")
    if not dc:
        return False
    if not dc.get("EnableCapture"):
        return False
    if dc.get("DestinationS3Uri") != capture_s3_uri:
        return False
    if int(dc.get("InitialSamplingPercentage", 0)) != int(sampling):
        return False
    return True


def main():
    args = parse_args()
    sm = boto3.client("sagemaker", region_name=args.region)

    print("Describing endpoint...")
    ep = sm.describe_endpoint(EndpointName=args.endpoint_name)
    current_cfg_name = ep["EndpointConfigName"]
    print("Current endpoint config:", current_cfg_name)

    cfg = sm.describe_endpoint_config(EndpointConfigName=current_cfg_name)

    # ✅ If already correct → do nothing
    if _capture_matches(cfg, args.capture_s3_uri, args.sampling_percentage):
        print("✅ Data capture already enabled with correct settings. Nothing to do.")
        return

    # Build deterministic name (so reruns try same name)
    desired_cfg_name = _safe_name(
        base=f"{args.endpoint_name}-capture",
        seed=current_cfg_name + args.capture_s3_uri + str(args.sampling_percentage),
    )

    # 1) If desired config already exists, reuse it (no create)
    desired_cfg = None
    try:
        desired_cfg = sm.describe_endpoint_config(EndpointConfigName=desired_cfg_name)
        print(f"✅ Endpoint config already exists: {desired_cfg_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ValidationException":
            raise

    # 2) If exists BUT doesn't match desired capture settings, create a new unique config name
    if desired_cfg and not _capture_matches(desired_cfg, args.capture_s3_uri, args.sampling_percentage):
        print("⚠️ Existing capture config name exists but settings differ. Creating a new unique config.")
        desired_cfg_name = _safe_name(
            base=f"{args.endpoint_name}-capture",
            seed=current_cfg_name + args.capture_s3_uri + str(args.sampling_percentage) + "v2",
        )
        desired_cfg = None

    # 3) Create config only if not existing
    if not desired_cfg:
        print("Creating new endpoint config:", desired_cfg_name)
        sm.create_endpoint_config(
            EndpointConfigName=desired_cfg_name,
            ProductionVariants=cfg["ProductionVariants"],
            DataCaptureConfig={
                "EnableCapture": True,
                "InitialSamplingPercentage": args.sampling_percentage,
                "DestinationS3Uri": args.capture_s3_uri,
                "CaptureOptions": [{"CaptureMode": "Input"}, {"CaptureMode": "Output"}],
            },
        )

    # 4) If endpoint is already using this config, stop
    if current_cfg_name == desired_cfg_name:
        print("✅ Endpoint is already using the desired config. Done.")
        return

    # 5) Update endpoint to new config
    print("Updating endpoint to new config...")
    sm.update_endpoint(
        EndpointName=args.endpoint_name,
        EndpointConfigName=desired_cfg_name,
    )

    print("✅ Data capture enabled.")
    print("New endpoint config:", desired_cfg_name)


if __name__ == "__main__":
    main()
