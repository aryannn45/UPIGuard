"""Generate reproducible synthetic UPI transactions for model development."""
from __future__ import annotations

from pathlib import Path
import argparse
import numpy as np
import pandas as pd

CITIES = [
    "Pune", "Mumbai", "Delhi", "Bengaluru", "Hyderabad",
    "Chennai", "Kolkata", "Ahmedabad", "Jaipur", "Ranchi",
]
FEATURES = [
    "transaction_amount", "transaction_hour", "account_age",
    "user_transactions_last_24h", "failed_attempts", "beneficiary_age",
    "is_new_beneficiary", "device_type", "city_code", "transaction_method",
]


def generate_dataset(rows: int = 10000, fraud_rate: float = 0.03, seed: int = 42) -> pd.DataFrame:
    if rows < 100 or not 0 < fraud_rate < 0.5:
        raise ValueError("rows must be >= 100 and fraud_rate must be between 0 and 0.5")
    rng = np.random.default_rng(seed)
    fraud_count = max(1, int(round(rows * fraud_rate)))
    labels = np.array([1] * fraud_count + [0] * (rows - fraud_count), dtype=np.int8)
    rng.shuffle(labels)

    records = []
    for label in labels:
        city_code = int(rng.integers(0, len(CITIES)))
        if label:
            row = {
                "transaction_amount": float(rng.uniform(5000, 50000)),
                "transaction_hour": int(rng.choice([0, 1, 2, 3, 4, 5, 23])),
                "account_age": int(rng.integers(1, 91)),
                "user_transactions_last_24h": int(rng.integers(8, 21)),
                "failed_attempts": int(rng.integers(3, 9)),
                "beneficiary_age": int(rng.integers(1, 61)),
                "is_new_beneficiary": int(rng.choice([0, 1], p=[0.20, 0.80])),
                "device_type": int(rng.choice([0, 1, 2], p=[0.55, 0.30, 0.15])),
                "city_code": city_code,
                "transaction_method": int(rng.choice([0, 1], p=[0.90, 0.10])),
                "is_fraud": 1,
            }
        else:
            row = {
                "transaction_amount": float(rng.uniform(50, 5000)),
                "transaction_hour": int(rng.integers(6, 23)),
                "account_age": int(rng.integers(90, 5001)),
                "user_transactions_last_24h": int(rng.integers(0, 8)),
                "failed_attempts": int(rng.choice([0, 1, 2], p=[0.65, 0.25, 0.10])),
                "beneficiary_age": int(rng.integers(60, 5001)),
                "is_new_beneficiary": int(rng.choice([0, 1], p=[0.90, 0.10])),
                "device_type": int(rng.choice([0, 1, 2], p=[0.55, 0.30, 0.15])),
                "city_code": city_code,
                "transaction_method": int(rng.choice([0, 1], p=[0.90, 0.10])),
                "is_fraud": 0,
            }
        row["city"] = CITIES[city_code]
        records.append(row)

    frame = pd.DataFrame.from_records(records)
    return frame[[*FEATURES, "is_fraud", "city"]]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--fraud-rate", type=float, default=0.03)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=Path(__file__).with_name("synthetic_fraud_data.csv"))
    args = parser.parse_args()
    data = generate_dataset(args.rows, args.fraud_rate, args.seed)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    data.to_csv(args.output, index=False)
    print(f"Saved {len(data):,} synthetic transactions to {args.output}")
    print(f"Fraud rows: {int(data['is_fraud'].sum()):,} ({data['is_fraud'].mean():.2%})")


if __name__ == "__main__":
    main()
