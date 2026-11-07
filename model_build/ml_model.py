"""Train and persist the UPIGuard Random Forest classifier."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.model_selection import train_test_split

from training_data_generation import FEATURES, generate_dataset

BASE_DIR = Path(__file__).resolve().parent


def train(input_path: Path, model_path: Path, metrics_path: Path) -> dict:
    if input_path.exists():
        data = pd.read_csv(input_path)
    else:
        data = generate_dataset()
        input_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(input_path, index=False)

    required = set(FEATURES + ["is_fraud"])
    missing = required - set(data.columns)
    if missing:
        raise ValueError(f"Training CSV is missing required columns: {sorted(missing)}")
    if data[FEATURES + ["is_fraud"]].isnull().any().any():
        raise ValueError("Training data contains null values in features or target")

    X = data[FEATURES].astype(float)
    y = data["is_fraud"].astype(int)
    if y.nunique() != 2:
        raise ValueError("Training data must contain both legitimate and fraudulent examples")

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.20, random_state=42, stratify=y
    )
    model = RandomForestClassifier(
        n_estimators=250,
        max_depth=14,
        min_samples_leaf=2,
        class_weight="balanced_subsample",
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    predictions = model.predict(X_test)
    probabilities = model.predict_proba(X_test)[:, 1]
    report = classification_report(y_test, predictions, output_dict=True, zero_division=0)
    try:
        auc = float(roc_auc_score(y_test, probabilities))
    except ValueError:
        auc = None

    model_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, model_path)
    metrics = {
        "rows": int(len(data)),
        "fraud_rows": int(y.sum()),
        "fraud_rate": float(y.mean()),
        "feature_names": FEATURES,
        "roc_auc": auc,
        "classification_report": report,
        "note": "Synthetic-data holdout evaluation; not a production performance guarantee.",
    }
    metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    print("UPIGuard model training complete")
    print(f"Rows: {len(data):,}; fraud rate: {y.mean():.2%}")
    print(classification_report(y_test, predictions, zero_division=0))
    print(f"ROC-AUC: {auc}")
    print(f"Model saved to: {model_path}")
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=BASE_DIR / "synthetic_fraud_data.csv")
    parser.add_argument("--model", type=Path, default=BASE_DIR / "fraud_detection_model.pkl")
    parser.add_argument("--metrics", type=Path, default=BASE_DIR / "metrics.json")
    args = parser.parse_args()
    train(args.input, args.model, args.metrics)


if __name__ == "__main__":
    main()
