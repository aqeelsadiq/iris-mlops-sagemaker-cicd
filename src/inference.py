import io
import json
import os
import joblib
import pandas as pd

FEATURES = ["sepal_length", "sepal_width", "petal_length", "petal_width"]

def model_fn(model_dir):
    model = joblib.load(os.path.join(model_dir, "model.joblib"))
    le = joblib.load(os.path.join(model_dir, "label_encoder.joblib"))
    return {"model": model, "le": le}

def input_fn(request_body, request_content_type):
    if request_content_type == "application/json":
        if isinstance(request_body, (bytes, bytearray)):
            request_body = request_body.decode("utf-8")
        payload = json.loads(request_body)
        data = payload["instances"] if isinstance(payload, dict) and "instances" in payload else payload
        return pd.DataFrame(data)

    if request_content_type == "text/csv":
        s = request_body.decode("utf-8") if isinstance(request_body, (bytes, bytearray)) else request_body
        df = pd.read_csv(io.StringIO(s))
        if list(df.columns) == [0, 1, 2, 3] and df.shape[1] == 4:
            df.columns = FEATURES
        return df

    raise ValueError(f"Unsupported content type: {request_content_type}")

def predict_fn(input_data, artifacts):
    model = artifacts["model"]
    le = artifacts["le"]

    X = input_data[FEATURES].copy()
    pred_idx = model.predict(X)
    pred_label = le.inverse_transform(pred_idx)

    return {"class_index": pred_idx, "species": pred_label}

def output_fn(prediction, response_content_type):
    if response_content_type == "application/json":
        return json.dumps({
            "class_index": prediction["class_index"].tolist(),
            "species": prediction["species"].tolist()
        }), response_content_type

    lines = [f"{i}\t{s}" for i, s in zip(prediction["class_index"], prediction["species"])]
    return "\n".join(lines), "text/plain"
