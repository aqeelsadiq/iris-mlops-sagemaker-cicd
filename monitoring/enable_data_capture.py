# import argparse
# import hashlib
# import boto3


# def parse_args():
#     p = argparse.ArgumentParser()
#     p.add_argument("--region", required=True)
#     p.add_argument("--endpoint-name", required=True)
#     p.add_argument("--capture-s3-uri", required=True)
#     p.add_argument("--sampling-percentage", type=int, default=100)
#     return p.parse_args()


# def stable_name(endpoint_name: str, capture_s3_uri: str, sampling: int) -> str:
#     h = hashlib.sha1(f"{endpoint_name}|{capture_s3_uri}|{sampling}".encode("utf-8")).hexdigest()[:8]
#     return f"{endpoint_name}-datacapture-{h}"


# def main():
#     args = parse_args()
#     sm = boto3.client("sagemaker", region_name=args.region)

#     ep = sm.describe_endpoint(EndpointName=args.endpoint_name)
#     current_cfg_name = ep["EndpointConfigName"]
#     cfg = sm.describe_endpoint_config(EndpointConfigName=current_cfg_name)

#     new_cfg_name = stable_name(args.endpoint_name, args.capture_s3_uri, args.sampling_percentage)

#     # If new config already exists, just update endpoint to it (idempotent)
#     try:
#         sm.describe_endpoint_config(EndpointConfigName=new_cfg_name)
#         print(f"✅ EndpointConfig already exists: {new_cfg_name}")
#     except sm.exceptions.ClientError:
#         prod_variants = cfg["ProductionVariants"]

#         data_capture = {
#             "EnableCapture": True,
#             "InitialSamplingPercentage": args.sampling_percentage,
#             "DestinationS3Uri": args.capture_s3_uri,
#             "CaptureOptions": [{"CaptureMode": "Input"}],
#             # "CaptureOptions": [{"CaptureMode": "Input"}, {"CaptureMode": "Output"}],
#             "CaptureContentTypeHeader": {
#                 "CsvContentTypes": ["text/csv"],
#                 "JsonContentTypes": ["application/json"],
#             },
#         }

#         create_kwargs = {
#             "EndpointConfigName": new_cfg_name,
#             "ProductionVariants": prod_variants,
#             "DataCaptureConfig": data_capture,
#         }

#         # keep KMS/VPC if present
#         if "KmsKeyId" in cfg:
#             create_kwargs["KmsKeyId"] = cfg["KmsKeyId"]
#         if "AsyncInferenceConfig" in cfg:
#             create_kwargs["AsyncInferenceConfig"] = cfg["AsyncInferenceConfig"]
#         if "ExplainerConfig" in cfg:
#             create_kwargs["ExplainerConfig"] = cfg["ExplainerConfig"]
#         if "ShadowProductionVariants" in cfg:
#             create_kwargs["ShadowProductionVariants"] = cfg["ShadowProductionVariants"]

#         sm.create_endpoint_config(**create_kwargs)
#         print(f"✅ Created EndpointConfig with DataCapture: {new_cfg_name}")

#     if current_cfg_name != new_cfg_name:
#         sm.update_endpoint(EndpointName=args.endpoint_name, EndpointConfigName=new_cfg_name)
#         print(f"✅ UpdateEndpoint started: {args.endpoint_name} -> {new_cfg_name}")
#     else:
#         print("✅ Data capture already enabled on current config.")


# if __name__ == "__main__":
#     main()






#claude code
"""
enable_data_capture.py
----------------------
Enables SageMaker Data Capture on an existing endpoint by creating a new
EndpointConfig (with DataCaptureConfig) and updating the endpoint to use it.

Usage:
    python monitoring/enable_data_capture.py \
        --region us-east-1 \
        --endpoint-name my-endpoint \
        --capture-s3-uri s3://my-bucket/datacapture \
        --sampling-percentage 100
"""

import argparse
import hashlib
import boto3


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--region", required=True)
    p.add_argument("--endpoint-name", required=True)
    p.add_argument("--capture-s3-uri", required=True,
                   help="S3 URI where captured data will be stored, e.g. s3://bucket/datacapture")
    p.add_argument("--sampling-percentage", type=int, default=100,
                   help="Percentage of requests to capture (default: 100)")
    return p.parse_args()


def stable_config_name(endpoint_name: str, capture_s3_uri: str, sampling: int) -> str:
    """Generate a deterministic, idempotent EndpointConfig name."""
    h = hashlib.sha1(
        f"{endpoint_name}|{capture_s3_uri}|{sampling}".encode("utf-8")
    ).hexdigest()[:8]
    # SageMaker names: max 63 chars, alphanumeric + hyphens
    base = f"{endpoint_name}-dc-{h}"
    return base[:63]


def main():
    args = parse_args()
    sm = boto3.client("sagemaker", region_name=args.region)

    # 1. Fetch current endpoint config
    ep = sm.describe_endpoint(EndpointName=args.endpoint_name)
    current_cfg_name = ep["EndpointConfigName"]
    cfg = sm.describe_endpoint_config(EndpointConfigName=current_cfg_name)

    new_cfg_name = stable_config_name(
        args.endpoint_name, args.capture_s3_uri, args.sampling_percentage
    )

    # 2. Create new config with DataCapture (idempotent)
    try:
        sm.describe_endpoint_config(EndpointConfigName=new_cfg_name)
        print(f"✅ EndpointConfig already exists (no-op): {new_cfg_name}")
    except sm.exceptions.ClientError:
        data_capture_config = {
            "EnableCapture": True,
            "InitialSamplingPercentage": args.sampling_percentage,
            "DestinationS3Uri": args.capture_s3_uri,
            # Capture BOTH input and output so Model Monitor has full data
            "CaptureOptions": [
                {"CaptureMode": "Input"},
                {"CaptureMode": "Output"},
            ],
            "CaptureContentTypeHeader": {
                "CsvContentTypes": ["text/csv"],
                "JsonContentTypes": ["application/json"],
            },
        }

        create_kwargs = {
            "EndpointConfigName": new_cfg_name,
            "ProductionVariants": cfg["ProductionVariants"],
            "DataCaptureConfig": data_capture_config,
        }

        # Preserve optional configs from the original endpoint config
        for key in ("KmsKeyId", "AsyncInferenceConfig", "ExplainerConfig", "ShadowProductionVariants"):
            if key in cfg:
                create_kwargs[key] = cfg[key]

        sm.create_endpoint_config(**create_kwargs)
        print(f"✅ Created EndpointConfig with DataCapture: {new_cfg_name}")

    # 3. Update endpoint only if config has changed
    if current_cfg_name != new_cfg_name:
        sm.update_endpoint(
            EndpointName=args.endpoint_name,
            EndpointConfigName=new_cfg_name,
        )
        print(f"✅ Endpoint update started: {args.endpoint_name} → {new_cfg_name}")
        print("   Wait for endpoint status to become 'InService' before running baseline/schedule.")
    else:
        print("✅ Data capture already enabled on the current config — no update needed.")


if __name__ == "__main__":
    main()