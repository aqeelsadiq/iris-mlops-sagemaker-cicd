import os
import pandas as pd
from sklearn.model_selection import train_test_split

INPUT_FILE = "/opt/ml/processing/input/iris-data.csv"
TRAIN_OUT = "/opt/ml/processing/train/train.csv"
TEST_OUT = "/opt/ml/processing/test/test.csv"

COLS = ["sepal_length", "sepal_width", "petal_length", "petal_width", "species"]

def main():
    df = pd.read_csv(INPUT_FILE, header=None, names=COLS)

    # Drop empty rows (sometimes UCI has trailing newline)
    df = df.dropna()
    if df.empty:
        raise ValueError("Input dataset is empty after cleaning.")

    train_df, test_df = train_test_split(
        df, test_size=0.2, random_state=42, shuffle=True, stratify=df["species"]
    )

    os.makedirs(os.path.dirname(TRAIN_OUT), exist_ok=True)
    os.makedirs(os.path.dirname(TEST_OUT), exist_ok=True)

    train_df.to_csv(TRAIN_OUT, index=False)
    test_df.to_csv(TEST_OUT, index=False)

    print("✅ Preprocessing complete")
    print("Train:", TRAIN_OUT, "rows:", len(train_df))
    print("Test :", TEST_OUT, "rows:", len(test_df))

if __name__ == "__main__":
    main()
