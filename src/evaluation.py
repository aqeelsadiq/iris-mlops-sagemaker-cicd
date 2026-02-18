import os
import json
import tarfile
import joblib
import pandas as pd
from sklearn.metrics import accuracy_score, f1_score

MODEL_INPUT_DIR = "/opt/ml/processing/model"
EXTRACT_DIR = "/opt/ml/processing/model_extracted"

TEST_PATH = "/opt/ml/processing/test/test.csv"
OUT_DIR = "/opt/ml/processing/evaluation"
OUT_FILE = os.path.join(OUT_DIR, "evaluation.json")

FEATURES = ["sepal_length", "sepal_width", "petal_length", "petal_width"]
LABEL_COL = "species"


def find_model_tar() -> str:
    candidate = os.path.join(MODEL_INPUT_DIR, "model.tar.gz")
    if os.path.exists(candidate):
        return candidate
    tars = [os.path.join(MODEL_INPUT_DIR, f) for f in os.listdir(MODEL_INPUT_DIR) if f.endswith(".tar.gz")]
    if not tars:
        raise FileNotFoundError(f"No model tar.gz found in {MODEL_INPUT_DIR}. Contents: {os.listdir(MODEL_INPUT_DIR)}")
    return tars[0]


def extract_tar(tar_path: str):
    os.makedirs(EXTRACT_DIR, exist_ok=True)
    with tarfile.open(tar_path, "r:gz") as tar:
        tar.extractall(EXTRACT_DIR)


def find_file(name: str) -> str:
    for root, _, files in os.walk(EXTRACT_DIR):
        if name in files:
            return os.path.join(root, name)
    raise FileNotFoundError(f"{name} not found under extracted model dir")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    tar_path = find_model_tar()
    extract_tar(tar_path)

    model = joblib.load(find_file("model.joblib"))
    le = joblib.load(find_file("label_encoder.joblib"))

    df = pd.read_csv(TEST_PATH).dropna()
    if df.empty:
        raise ValueError("Test dataset is empty.")

    X = df[FEATURES].copy()
    y_true = le.transform(df[LABEL_COL].astype(str))

    preds = model.predict(X)

    acc = float(accuracy_score(y_true, preds))
    f1 = float(f1_score(y_true, preds, average="macro"))

    # ✅ SIMPLE JSON (no nested schema issues)
    payload = {
        "accuracy": acc,
        "f1_macro": f1
    }

    with open(OUT_FILE, "w") as f:
        json.dump(payload, f)

    print("✅ evaluation.json written:", OUT_FILE)
    print(payload)


if __name__ == "__main__":
    main()
