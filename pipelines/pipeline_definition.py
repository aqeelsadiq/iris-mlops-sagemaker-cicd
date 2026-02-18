# import argparse
# import boto3

# from sagemaker.image_uris import retrieve
# from sagemaker.processing import ScriptProcessor, ProcessingInput, ProcessingOutput
# from sagemaker.sklearn.estimator import SKLearn

# from sagemaker.workflow.pipeline import Pipeline
# from sagemaker.workflow.pipeline_context import PipelineSession
# from sagemaker.workflow.parameters import ParameterString, ParameterFloat
# from sagemaker.workflow.steps import ProcessingStep, TrainingStep
# from sagemaker.workflow.properties import PropertyFile
# from sagemaker.workflow.functions import JsonGet
# from sagemaker.workflow.conditions import ConditionGreaterThanOrEqualTo
# from sagemaker.workflow.condition_step import ConditionStep
# from sagemaker.workflow.step_collections import RegisterModel


# def parse_args():
#     p = argparse.ArgumentParser()
#     p.add_argument("--region", required=True)
#     p.add_argument("--role-arn", required=True)
#     p.add_argument("--pipeline-name", required=True)
#     p.add_argument("--model-package-group-name", required=True)
#     p.add_argument("--default-bucket", required=True)

#     p.add_argument("--train-data-s3-uri", required=True)
#     p.add_argument("--accuracy-threshold", default="0.90")

#     p.add_argument("--processing-instance-type", default="ml.t3.medium")
#     p.add_argument("--training-instance-type", default="ml.m5.large")
#     p.add_argument("--evaluation-instance-type", default="ml.t3.medium")
#     return p.parse_args()


# def main():
#     args = parse_args()

#     boto_sess = boto3.Session(region_name=args.region)
#     pipeline_sess = PipelineSession(
#         boto_session=boto_sess,
#         default_bucket=args.default_bucket,
#     )

#     train_data_param = ParameterString("TrainDataS3Uri", default_value=args.train_data_s3_uri)
#     acc_threshold_param = ParameterFloat("AccuracyThreshold", default_value=float(args.accuracy_threshold))

#     sklearn_image = retrieve(
#         framework="sklearn",
#         region=args.region,
#         version="1.2-1",
#         py_version="py3",
#         instance_type=args.processing_instance_type,
#     )

#     # 1) Preprocessing
#     preprocess_processor = ScriptProcessor(
#         image_uri=sklearn_image,
#         command=["python3"],
#         role=args.role_arn,
#         instance_type=args.processing_instance_type,
#         instance_count=1,
#         sagemaker_session=pipeline_sess,
#     )

#     step_preprocess = ProcessingStep(
#         name="IrisPreprocessing",
#         processor=preprocess_processor,
#         inputs=[ProcessingInput(source=train_data_param, destination="/opt/ml/processing/input")],
#         outputs=[
#             ProcessingOutput(output_name="train", source="/opt/ml/processing/train"),
#             ProcessingOutput(output_name="test", source="/opt/ml/processing/test"),
#         ],
#         code="src/preprocessing.py",
#     )

#     # 2) Training
#     estimator = SKLearn(
#         entry_point="training.py",
#         source_dir="src",
#         role=args.role_arn,
#         instance_type=args.training_instance_type,
#         framework_version="1.2-1",
#         py_version="py3",
#         sagemaker_session=pipeline_sess,
#     )

#     step_train = TrainingStep(
#         name="IrisTraining",
#         estimator=estimator,
#         inputs={
#             "train": step_preprocess.properties.ProcessingOutputConfig.Outputs["train"].S3Output.S3Uri
#         },
#     )

#     # 3) Evaluation
#     eval_processor = ScriptProcessor(
#         image_uri=sklearn_image,
#         command=["python3"],
#         role=args.role_arn,
#         instance_type=args.evaluation_instance_type,
#         instance_count=1,
#         sagemaker_session=pipeline_sess,
#     )

#     evaluation_report = PropertyFile(
#         name="EvaluationReport",
#         output_name="evaluation",
#         path="evaluation.json",
#     )

#     step_eval = ProcessingStep(
#         name="IrisEvaluation",
#         processor=eval_processor,
#         inputs=[
#             ProcessingInput(
#                 source=step_train.properties.ModelArtifacts.S3ModelArtifacts,
#                 destination="/opt/ml/processing/model",
#             ),
#             ProcessingInput(
#                 source=step_preprocess.properties.ProcessingOutputConfig.Outputs["test"].S3Output.S3Uri,
#                 destination="/opt/ml/processing/test",
#             ),
#         ],
#         outputs=[
#             ProcessingOutput(output_name="evaluation", source="/opt/ml/processing/evaluation")
#         ],
#         code="src/evaluation.py",
#         property_files=[evaluation_report],
#     )

#     # ✅ FIX: read accuracy from metric_groups schema
#     acc_value = JsonGet(
#     step_name=step_eval.name,
#     property_file=evaluation_report,
#     json_path="accuracy",
# )


#     condition = ConditionGreaterThanOrEqualTo(left=acc_value, right=acc_threshold_param)

#     # 4) Register Model (PendingManualApproval) + include inference code
#     register_step = RegisterModel(
#         name="IrisRegisterModel",
#         estimator=estimator,
#         model_data=step_train.properties.ModelArtifacts.S3ModelArtifacts,
#         content_types=["text/csv", "application/json"],
#         response_types=["application/json", "text/plain"],
#         inference_instances=["ml.m5.large"],
#         transform_instances=["ml.m5.large"],
#         model_package_group_name=args.model_package_group_name,
#         approval_status="PendingManualApproval",
#         description="Iris classifier - registered after passing evaluation threshold.",
#         entry_point="inference.py",
#         source_dir="src",
#     )

#     step_condition = ConditionStep(
#         name="IrisConditionEvaluation",
#         conditions=[condition],
#         if_steps=[register_step],
#         else_steps=[],
#     )

#     pipeline = Pipeline(
#         name=args.pipeline_name,
#         parameters=[train_data_param, acc_threshold_param],
#         steps=[step_preprocess, step_train, step_eval, step_condition],
#         sagemaker_session=pipeline_sess,
#     )

#     pipeline.upsert(role_arn=args.role_arn)
#     print(f"✅ Pipeline upserted successfully: {args.pipeline_name}")


# if __name__ == "__main__":
#     main()


# pipelines/pipeline_definition.py

import argparse
import boto3

from sagemaker.image_uris import retrieve
from sagemaker.processing import ScriptProcessor, ProcessingInput, ProcessingOutput
from sagemaker.sklearn.estimator import SKLearn

from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.pipeline_context import PipelineSession
from sagemaker.workflow.parameters import ParameterString, ParameterFloat
from sagemaker.workflow.steps import ProcessingStep, TrainingStep
from sagemaker.workflow.properties import PropertyFile
from sagemaker.workflow.functions import JsonGet
from sagemaker.workflow.conditions import ConditionGreaterThanOrEqualTo
from sagemaker.workflow.condition_step import ConditionStep
from sagemaker.workflow.step_collections import RegisterModel

# ✅ NEW: attach evaluation to Model Registry
from sagemaker.model_metrics import ModelMetrics, MetricsSource


def parse_args():
    p = argparse.ArgumentParser()

    p.add_argument("--region", required=True)
    p.add_argument("--role-arn", required=True)
    p.add_argument("--pipeline-name", required=True)
    p.add_argument("--model-package-group-name", required=True)
    p.add_argument("--default-bucket", required=True)

    p.add_argument("--train-data-s3-uri", required=True)
    p.add_argument("--accuracy-threshold", default="0.90")

    p.add_argument("--processing-instance-type", default="ml.t3.medium")
    p.add_argument("--training-instance-type", default="ml.m5.large")
    p.add_argument("--evaluation-instance-type", default="ml.t3.medium")

    return p.parse_args()


def main():
    args = parse_args()

    boto_sess = boto3.Session(region_name=args.region)
    pipeline_sess = PipelineSession(
        boto_session=boto_sess,
        default_bucket=args.default_bucket,
    )

    # Pipeline runtime parameters
    train_data_param = ParameterString("TrainDataS3Uri", default_value=args.train_data_s3_uri)
    acc_threshold_param = ParameterFloat("AccuracyThreshold", default_value=float(args.accuracy_threshold))

    # Container image for sklearn processing/training in this region
    sklearn_image = retrieve(
        framework="sklearn",
        region=args.region,
        version="1.2-1",
        py_version="py3",
        instance_type=args.processing_instance_type,
    )

    # -------------------------
    # 1) Preprocessing Step
    # -------------------------
    preprocess_processor = ScriptProcessor(
        image_uri=sklearn_image,
        command=["python3"],
        role=args.role_arn,
        instance_type=args.processing_instance_type,
        instance_count=1,
        sagemaker_session=pipeline_sess,
    )

    step_preprocess = ProcessingStep(
        name="IrisPreprocessing",
        processor=preprocess_processor,
        inputs=[
            ProcessingInput(
                source=train_data_param,
                destination="/opt/ml/processing/input",
            )
        ],
        outputs=[
            ProcessingOutput(output_name="train", source="/opt/ml/processing/train"),
            ProcessingOutput(output_name="test", source="/opt/ml/processing/test"),
        ],
        code="src/preprocessing.py",
    )

    # -------------------------
    # 2) Training Step
    # -------------------------
    estimator = SKLearn(
        entry_point="training.py",
        source_dir="src",
        role=args.role_arn,
        instance_type=args.training_instance_type,
        framework_version="1.2-1",
        py_version="py3",
        sagemaker_session=pipeline_sess,
    )

    step_train = TrainingStep(
        name="IrisTraining",
        estimator=estimator,
        inputs={
            "train": step_preprocess.properties.ProcessingOutputConfig.Outputs["train"].S3Output.S3Uri
        },
    )

    # -------------------------
    # 3) Evaluation Step
    # -------------------------
    eval_processor = ScriptProcessor(
        image_uri=sklearn_image,
        command=["python3"],
        role=args.role_arn,
        instance_type=args.evaluation_instance_type,
        instance_count=1,
        sagemaker_session=pipeline_sess,
    )

    # This tells the pipeline "there will be a JSON file called evaluation.json in output_name=evaluation"
    evaluation_report = PropertyFile(
        name="EvaluationReport",
        output_name="evaluation",
        path="evaluation.json",
    )

    step_eval = ProcessingStep(
        name="IrisEvaluation",
        processor=eval_processor,
        inputs=[
            ProcessingInput(
                source=step_train.properties.ModelArtifacts.S3ModelArtifacts,
                destination="/opt/ml/processing/model",
            ),
            ProcessingInput(
                source=step_preprocess.properties.ProcessingOutputConfig.Outputs["test"].S3Output.S3Uri,
                destination="/opt/ml/processing/test",
            ),
        ],
        outputs=[
            ProcessingOutput(output_name="evaluation", source="/opt/ml/processing/evaluation")
        ],
        code="src/evaluation.py",
        property_files=[evaluation_report],
    )

    # Read "accuracy" from evaluation.json so we can gate registration
    acc_value = JsonGet(
        step_name=step_eval.name,
        property_file=evaluation_report,
        json_path="accuracy",
    )

    condition = ConditionGreaterThanOrEqualTo(left=acc_value, right=acc_threshold_param)

    # ✅ NEW: attach evaluation.json to the Model Package (Model Registry "Evaluate" section)
    # S3Output.S3Uri points to the evaluation output folder; append /evaluation.json
    evaluation_json_s3_uri = (
        step_eval.properties.ProcessingOutputConfig.Outputs["evaluation"].S3Output.S3Uri
        + "/evaluation.json"
    )

    model_metrics = ModelMetrics(
        model_statistics=MetricsSource(
            s3_uri=evaluation_json_s3_uri,
            content_type="application/json",
        )
    )

    # -------------------------
    # 4) Register Model (manual approval)
    # -------------------------
    register_step = RegisterModel(
        name="IrisRegisterModel",
        estimator=estimator,
        model_data=step_train.properties.ModelArtifacts.S3ModelArtifacts,
        content_types=["text/csv", "application/json"],
        response_types=["application/json", "text/plain"],
        inference_instances=["ml.m5.large"],
        transform_instances=["ml.m5.large"],
        model_package_group_name=args.model_package_group_name,
        approval_status="PendingManualApproval",
        description="Iris classifier - registered after passing evaluation threshold.",
        entry_point="inference.py",
        source_dir="src",

        # ✅ This makes SageMaker Model Registry show evaluation content instead of "Undefined"
        model_metrics=model_metrics,
    )

    step_condition = ConditionStep(
        name="IrisConditionEvaluation",
        conditions=[condition],
        if_steps=[register_step],
        else_steps=[],
    )

    pipeline = Pipeline(
        name=args.pipeline_name,
        parameters=[train_data_param, acc_threshold_param],
        steps=[step_preprocess, step_train, step_eval, step_condition],
        sagemaker_session=pipeline_sess,
    )

    pipeline.upsert(role_arn=args.role_arn)
    print(f"✅ Pipeline upserted successfully: {args.pipeline_name}")


if __name__ == "__main__":
    main()
