CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id BIGSERIAL NOT NULL,
    account_id BIGINT NOT NULL,
    transaction_amount NUMERIC(14, 2) NOT NULL,
    transaction_time TIMESTAMPTZ NOT NULL,
    city TEXT NOT NULL DEFAULT 'Unknown',
    transaction_method SMALLINT NOT NULL DEFAULT 0,
    transaction_hour SMALLINT NOT NULL DEFAULT 0,
    account_age INTEGER NOT NULL DEFAULT 0,
    user_transactions_last_24h INTEGER NOT NULL DEFAULT 0,
    failed_attempts INTEGER NOT NULL DEFAULT 0,
    beneficiary_age INTEGER NOT NULL DEFAULT 0,
    is_new_beneficiary SMALLINT NOT NULL DEFAULT 0,
    device_type SMALLINT NOT NULL DEFAULT 0,
    city_code INTEGER NOT NULL DEFAULT 0,
    fraud_prediction SMALLINT NOT NULL CHECK (fraud_prediction IN (0, 1)),
    fraud_probability NUMERIC(8, 6) NOT NULL CHECK (fraud_probability >= 0 AND fraud_probability <= 1),
    risk_level TEXT NOT NULL CHECK (risk_level IN ('LOW', 'MEDIUM', 'HIGH')),
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (transaction_time, transaction_id),
    UNIQUE (transaction_time, kafka_partition, kafka_offset)
);

SELECT create_hypertable('transactions', 'transaction_time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_transactions_time ON transactions (transaction_time DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_fraud ON transactions (fraud_prediction, transaction_time DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_city ON transactions (city, transaction_time DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_risk ON transactions (risk_level, transaction_time DESC);
