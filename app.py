import json
import os
import boto3
import streamlit as st
from botocore.exceptions import ClientError


st.set_page_config(page_title="Iris Predictor (SageMaker)", page_icon="🌸", layout="centered")

st.title("🌸 Iris Species Prediction")
st.caption("Frontend app that calls a deployed SageMaker endpoint (real-time inference).")

# ---- Defaults (edit if you want) ----
DEFAULT_REGION = os.getenv("AWS_REGION", "us-east-1")
DEFAULT_ENDPOINT = os.getenv("SAGEMAKER_ENDPOINT_NAME", "iris-endpoint")

# ---- Sidebar config ----
st.sidebar.header("⚙️ Endpoint Settings")

endpoint_name = st.sidebar.text_input("Endpoint name", value=DEFAULT_ENDPOINT)
region = st.sidebar.text_input("AWS Region", value=DEFAULT_REGION)

auth_mode = st.sidebar.radio(
    "AWS Auth mode",
    options=["Use environment/instance role (recommended)", "Use access keys (manual)"],
    index=0,
)

aws_access_key_id = None
aws_secret_access_key = None
aws_session_token = None

if auth_mode == "Use access keys (manual)":
    aws_access_key_id = st.sidebar.text_input("AWS_ACCESS_KEY_ID", type="password")
    aws_secret_access_key = st.sidebar.text_input("AWS_SECRET_ACCESS_KEY", type="password")
    aws_session_token = st.sidebar.text_input("AWS_SESSION_TOKEN (optional)", type="password")

st.sidebar.markdown("---")
st.sidebar.caption(
    "Tip: In SageMaker Studio or an EC2 with IAM role, keep auth mode as recommended."
)

# ---- Input form ----
st.subheader("Input features")

col1, col2 = st.columns(2)
with col1:
    sepal_length = st.number_input("Sepal length", min_value=0.0, value=5.1, step=0.1, format="%.2f")
    petal_length = st.number_input("Petal length", min_value=0.0, value=1.4, step=0.1, format="%.2f")
with col2:
    sepal_width = st.number_input("Sepal width", min_value=0.0, value=3.5, step=0.1, format="%.2f")
    petal_width = st.number_input("Petal width", min_value=0.0, value=0.2, step=0.1, format="%.2f")

payload = {
    "instances": [
        {
            "sepal_length": float(sepal_length),
            "sepal_width": float(sepal_width),
            "petal_length": float(petal_length),
            "petal_width": float(petal_width),
        }
    ]
}

with st.expander("Show request payload"):
    st.code(json.dumps(payload, indent=2), language="json")

# ---- Invoke endpoint ----
def get_runtime_client():
    if auth_mode == "Use access keys (manual)":
        if not aws_access_key_id or not aws_secret_access_key:
            raise ValueError("Please provide AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the sidebar.")
        return boto3.client(
            "sagemaker-runtime",
            region_name=region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token if aws_session_token else None,
        )
    # Use default credential chain: env vars, shared config, instance profile, etc.
    return boto3.client("sagemaker-runtime", region_name=region)

def invoke(endpoint: str, body: dict) -> dict:
    runtime = get_runtime_client()
    resp = runtime.invoke_endpoint(
        EndpointName=endpoint,
        ContentType="application/json",
        Accept="application/json",
        Body=json.dumps(body).encode("utf-8"),
    )
    raw = resp["Body"].read().decode("utf-8")

    # Try JSON first, else return raw
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = {"raw": raw}

    return {"raw_text": raw, "parsed": parsed}

st.markdown("---")
if st.button("🔮 Predict", type="primary"):
    if not endpoint_name.strip():
        st.error("Please enter an endpoint name.")
    else:
        try:
            result = invoke(endpoint_name.strip(), payload)
            parsed = result["parsed"]

            # Expected from your inference.py:
            # {"class_index":[...], "species":[...]}
            species = None
            if isinstance(parsed, dict) and "species" in parsed:
                species_list = parsed.get("species")
                if isinstance(species_list, list) and species_list:
                    species = species_list[0]

            if species:
                st.success(f"✅ Predicted species: **{species}**")
            else:
                st.info("Prediction returned, but could not extract 'species' field. See raw output below.")

            st.subheader("Raw response")
            st.code(result["raw_text"], language="json")

        except ValueError as e:
            st.error(str(e))
        except ClientError as e:
            st.error("AWS ClientError while invoking endpoint.")
            st.code(str(e))
        except Exception as e:
            st.error("Unexpected error.")
            st.code(repr(e))
