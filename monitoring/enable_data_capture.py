import argparse
import boto3

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--region", required=True)
    p.add_argument("--endpoint-name", required=True)
    p.add_argument("--capture-s3-uri", required=True)  # e.g. s3://.../monitoring/datacapture/
    p.add_argument("--initial-sampling-percentage", type=int, default=100)
    return p.parse_args()

def main():
    args = parse_args()
    sm = boto3.client("sagemaker", region_name=args.region)

    resp = sm.describe_endpoint(EndpointName=args.endpoint_name)
    config_name = resp["EndpointConfigName"]
    config = sm.describe_endpoint_config(EndpointConfigName=config_name)

    new_config_name = f"{config_name}-datacapture"

    # copy production variants
    production_variants = config["ProductionVariants"]

    data_capture_config = {
        "EnableCapture": True,
        "InitialSamplingPercentage": args.initial_sampling_percentage,
        "DestinationS3Uri": args.capture_s3_uri,
        "CaptureOptions": [{"CaptureMode": "Input"}, {"CaptureMode": "Output"}],
        "CaptureContentTypeHeader": {
            "CsvContentTypes": ["text/csv"],
            "JsonContentTypes": ["application/json"]
        }
    }

    sm.create_endpoint_config(
        EndpointConfigName=new_config_name,
        ProductionVariants=production_variants,
        DataCaptureConfig=data_capture_config
    )

    sm.update_endpoint(
        EndpointName=args.endpoint_name,
        EndpointConfigName=new_config_name
    )

    print("✅ DataCapture enabled. Endpoint updating to config:", new_config_name)

if __name__ == "__main__":
    main()
