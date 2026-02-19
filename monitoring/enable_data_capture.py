# monitoring/enable_data_capture.py
import argparse
import hashlib
import boto3


def _safe_name(base: str, suffix: str, max_len: int = 63) -> str:
    """
    SageMaker endpoint config name must match:
      [a-zA-Z0-9](-*[a-zA-Z0-9]){0,62}
    and length <= 63.
    """
    base = (base or "endpoint").replace("_", "-")
    base = "".join(ch for ch in base if ch.isalnum() or ch == "-").strip("-")
    if not base:
        base = "endpoint"

    # small deterministic hash so repeated runs don't explode names
    h = hashlib.sha1(suffix.encode("utf-8")).hexdigest()[:8]
    name = f"{base}-{h}"
    return name[:max_len]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--region", required=True)
    p.add_argument("--endpoint-name", required=True)
    p.add_argument("--capture-s3-uri", required=True)  # e.g. s3://bucket/monitoring/datacapture/
    p.add_argument("--sampling-percentage", type=int, default=100)
    p.add_argument("--enable", action="store_true", default=True)
    return p.parse_args()


def main():
    args = parse_args()
    sm = boto3.client("sagemaker", region_name=args.region)

    ep = sm.describe_endpoint(EndpointName=args.endpoint_name)
    current_cfg_name = ep["EndpointConfigName"]
    cfg = sm.describe_endpoint_config(EndpointConfigName=current_cfg_name)

    if not cfg.get("ProductionVariants"):
        raise RuntimeError("EndpointConfig has no ProductionVariants")

    # Use first variant (common for single-variant endpoints)
    pv0 = cfg["ProductionVariants"][0]
    variant_name = pv0["VariantName"]

    # Create a new endpoint config with the SAME variants but with DataCaptureConfig enabled
    new_cfg_name = _safe_name(
        base=f"{args.endpoint_name}-datacapture",
        suffix=f"{current_cfg_name}:{args.capture_s3_uri}:{args.sampling_percentage}",
        max_len=63,
    )

    data_capture_cfg = {
        "EnableCapture": True,
        "InitialSamplingPercentage": args.sampling_percentage,
        "DestinationS3Uri": args.capture_s3_uri,
        "CaptureOptions": [{"CaptureMode": "Input"}, {"CaptureMode": "Output"}],
        "CaptureContentTypeHeader": {
            # keep broad; you can tighten later
            "CsvContentTypes": ["text/csv"],
            "JsonContentTypes": ["application/json"],
        },
    }

    create_payload = {
        "EndpointConfigName": new_cfg_name,
        "ProductionVariants": cfg["ProductionVariants"],
        # Keep optional fields if present
    }
    if "KmsKeyId" in cfg and cfg["KmsKeyId"]:
        create_payload["KmsKeyId"] = cfg["KmsKeyId"]
    create_payload["DataCaptureConfig"] = data_capture_cfg

    # Some accounts use ShadowProductionVariants
    if "ShadowProductionVariants" in cfg and cfg["ShadowProductionVariants"]:
        create_payload["ShadowProductionVariants"] = cfg["ShadowProductionVariants"]

    print(f"Creating endpoint config with DataCapture enabled: {new_cfg_name}")
    sm.create_endpoint_config(**create_payload)

    print(f"Updating endpoint to use config: {new_cfg_name}")
    sm.update_endpoint(EndpointName=args.endpoint_name, EndpointConfigName=new_cfg_name)

    print("✅ Data Capture enabled")
    print("   Endpoint:", args.endpoint_name)
    print("   Destination:", args.capture_s3_uri)
    print("   Sampling %:", args.sampling_percentage)
    print("   Variant:", variant_name)


if __name__ == "__main__":
    main()
