import argparse
import boto3
import sagemaker
from sagemaker.sklearn.model import SKLearnModel


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--region", required=True)
    p.add_argument("--execution-role-arn", required=True)
    p.add_argument("--model-package-group-name", required=True)
    p.add_argument("--endpoint-name", required=True)
    p.add_argument("--instance-type", default="ml.m5.large")
    return p.parse_args()


def main():
    args = parse_args()

    sm = boto3.client("sagemaker", region_name=args.region)

    resp = sm.list_model_packages(
        ModelPackageGroupName=args.model_package_group_name,
        ModelApprovalStatus="Approved",
        SortBy="CreationTime",
        SortOrder="Descending",
        MaxResults=1,
    )
    if not resp["ModelPackageSummaryList"]:
        raise RuntimeError("No APPROVED model package found in the group.")

    pkg_arn = resp["ModelPackageSummaryList"][0]["ModelPackageArn"]
    desc = sm.describe_model_package(ModelPackageName=pkg_arn)

    model_data = desc["InferenceSpecification"]["Containers"][0]["ModelDataUrl"]
    print("Using approved package:", pkg_arn)
    print("ModelDataUrl:", model_data)

    sess = sagemaker.Session(boto3.Session(region_name=args.region))

    model = SKLearnModel(
        model_data=model_data,
        role=args.execution_role_arn,
        entry_point="inference.py",
        source_dir="src",
        framework_version="1.2-1",
        py_version="py3",
        sagemaker_session=sess,
    )

    model.deploy(
        endpoint_name=args.endpoint_name,
        initial_instance_count=1,
        instance_type=args.instance_type,
    )

    print("✅ Deployed endpoint:", args.endpoint_name)


if __name__ == "__main__":
    main()
