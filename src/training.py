import argparse
import os
import joblib
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score

FEATURES = ["sepal_length", "sepal_width", "petal_length", "petal_width"]
LABEL_COL = "species"

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--train", type=str, default="/opt/ml/input/data/train/train.csv")
    p.add_argument("--model-dir", type=str, default="/opt/ml/model")
    return p.parse_args()

def main():
    args = parse_args()

    df = pd.read_csv(args.train)
    df = df.dropna()
    if df.empty:
        raise ValueError(f"Training data is empty: {args.train}")

    X = df[FEATURES].copy()
    y_raw = df[LABEL_COL].astype(str)

    le = LabelEncoder()
    y = le.fit_transform(y_raw)

    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = Pipeline(steps=[
        ("scaler", StandardScaler()),
        ("clf", LogisticRegression(max_iter=2000, multi_class="auto"))
    ])

    model.fit(X_train, y_train)

    preds = model.predict(X_val)
    acc = float(accuracy_score(y_val, preds))
    f1 = float(f1_score(y_val, preds, average="macro"))

    os.makedirs(args.model_dir, exist_ok=True)
    joblib.dump(model, os.path.join(args.model_dir, "model.joblib"))
    joblib.dump(le, os.path.join(args.model_dir, "label_encoder.joblib"))

    with open(os.path.join(args.model_dir, "train_metrics.txt"), "w") as f:
        f.write(f"accuracy={acc}\n")
        f.write(f"f1_macro={f1}\n")

    print("✅ Training complete")
    print("accuracy:", acc)
    print("f1_macro:", f1)
    print("classes:", list(le.classes_))

if __name__ == "__main__":
    main()
