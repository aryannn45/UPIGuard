from model_build.training_data_generation import CITIES, FEATURES, generate_dataset


def test_generated_dataset_schema_and_class_balance():
    data = generate_dataset(rows=1000, fraud_rate=0.03, seed=7)
    assert list(data.columns) == FEATURES + ["is_fraud", "city"]
    assert len(data) == 1000
    assert int(data["is_fraud"].sum()) == 30
    assert set(data["is_fraud"].unique()) == {0, 1}
    assert set(data["city"].unique()).issubset(set(CITIES))
    assert data.isna().sum().sum() == 0


def test_suspicious_examples_have_high_risk_signals():
    data = generate_dataset(rows=1000, fraud_rate=0.10, seed=11)
    fraud = data[data["is_fraud"] == 1]
    normal = data[data["is_fraud"] == 0]
    assert fraud["transaction_amount"].median() > normal["transaction_amount"].median()
    assert fraud["failed_attempts"].median() > normal["failed_attempts"].median()
    assert fraud["account_age"].median() < normal["account_age"].median()
