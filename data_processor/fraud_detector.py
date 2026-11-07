"""UPIGuard Kafka -> Spark Structured Streaming -> ML -> TimescaleDB pipeline."""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import joblib
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType,
)

from risk import classify_probability

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
LOGGER = logging.getLogger("upiguard.stream")

FEATURES = [
    "transaction_amount", "transaction_hour", "account_age",
    "user_transactions_last_24h", "failed_attempts", "beneficiary_age",
    "is_new_beneficiary", "device_type", "city_code", "transaction_method",
]

SCHEMA = StructType([
    StructField("account_id", LongType(), True),
    StructField("transaction_amount", DoubleType(), True),
    StructField("transaction_time", StringType(), True),
    StructField("city", StringType(), True),
    StructField("transaction_method", IntegerType(), True),
    StructField("transaction_hour", IntegerType(), True),
    StructField("account_age", IntegerType(), True),
    StructField("user_transactions_last_24h", IntegerType(), True),
    StructField("failed_attempts", IntegerType(), True),
    StructField("beneficiary_age", IntegerType(), True),
    StructField("is_new_beneficiary", IntegerType(), True),
    StructField("device_type", IntegerType(), True),
    StructField("city_code", IntegerType(), True),
])

MODEL_PATH = Path(os.getenv("MODEL_PATH", "/opt/upiguard/model_build/fraud_detection_model.pkl"))


def load_model():
    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Model not found at {MODEL_PATH}. The model-trainer service must finish before Spark starts."
        )
    model = joblib.load(MODEL_PATH)
    model_features = list(getattr(model, "feature_names_in_", FEATURES))
    if model_features != FEATURES:
        raise ValueError(f"Model feature order mismatch. Expected {FEATURES}, found {model_features}")
    LOGGER.info("Loaded UPIGuard model: %s", MODEL_PATH)
    LOGGER.info("Features: %s", model_features)
    return model


def connect_db():
    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        raise RuntimeError("POSTGRES_PASSWORD is required; set it in the local .env file")
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "timescaledb"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "fraud_detection"),
        user=os.getenv("POSTGRES_USER", "user"),
        password=password,
        connect_timeout=10,
    )


def parse_time(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if value is None:
        return datetime.now(timezone.utc)
    normalized = str(value).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"Invalid transaction_time: {value!r}") from exc


def process_batch(batch_df, batch_id: int, model) -> None:
    rows = [row.asDict(recursive=True) for row in batch_df.toLocalIterator()]
    if not rows:
        LOGGER.info("Batch %s is empty", batch_id)
        return

    frame = pd.DataFrame(rows)
    missing = [feature for feature in FEATURES if feature not in frame.columns]
    if missing:
        raise ValueError(f"Kafka payload is missing features: {missing}")
    X = frame[FEATURES].astype(float)
    predictions = model.predict(X)
    probabilities = model.predict_proba(X)[:, 1]

    values: List[tuple] = []
    for source, prediction, probability in zip(rows, predictions, probabilities):
        probability = float(probability)
        transaction_time = parse_time(source.get("transaction_time"))
        values.append((
            int(source.get("account_id") or 0),
            float(source.get("transaction_amount") or 0.0),
            transaction_time,
            str(source.get("city") or "Unknown"),
            int(source.get("transaction_method") or 0),
            int(source.get("transaction_hour") or 0),
            int(source.get("account_age") or 0),
            int(source.get("user_transactions_last_24h") or 0),
            int(source.get("failed_attempts") or 0),
            int(source.get("beneficiary_age") or 0),
            int(source.get("is_new_beneficiary") or 0),
            int(source.get("device_type") or 0),
            int(source.get("city_code") or 0),
            int(prediction),
            probability,
            classify_probability(probability),
            int(source.get("kafka_partition") or 0),
            int(source.get("kafka_offset") or 0),
        ))

    insert_sql = """
        INSERT INTO transactions (
            account_id, transaction_amount, transaction_time, city,
            transaction_method, transaction_hour, account_age,
            user_transactions_last_24h, failed_attempts, beneficiary_age,
            is_new_beneficiary, device_type, city_code,
            fraud_prediction, fraud_probability, risk_level,
            kafka_partition, kafka_offset
        ) VALUES %s
        ON CONFLICT (transaction_time, kafka_partition, kafka_offset) DO NOTHING
    """
    conn = None
    try:
        conn = connect_db()
        with conn:
            with conn.cursor() as cursor:
                execute_values(cursor, insert_sql, values, page_size=500)
                inserted = cursor.rowcount
        LOGGER.info(
            "UPIGuard batch=%s processed=%s inserted=%s fraud_predictions=%s high_risk=%s",
            batch_id, len(rows), inserted, int(sum(predictions)),
            sum(1 for p in probabilities if p >= 0.70),
        )
    except Exception:
        LOGGER.exception("Database write failed for batch %s", batch_id)
        raise
    finally:
        if conn is not None:
            conn.close()


def main() -> None:
    model = load_model()
    kafka_brokers = os.getenv("KAFKA_BROKERS", "broker:29092")
    topic = os.getenv("KAFKA_TOPIC", "raw_transactions")
    checkpoint = os.getenv("CHECKPOINT_LOCATION", "/tmp/upiguard-checkpoints/fraud-stream")

    spark = (
        SparkSession.builder
        .appName("UPIGuard-RealTime-Fraud-Detection")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "2"))
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", topic)
        .option("startingOffsets", os.getenv("KAFKA_STARTING_OFFSETS", "latest"))
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", os.getenv("MAX_OFFSETS_PER_TRIGGER", "500"))
        .load()
    )

    parsed_stream = (
        raw_stream.select(
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            from_json(col("value").cast("string"), SCHEMA).alias("payload"),
        )
        .where(col("payload").isNotNull())
        .select("kafka_partition", "kafka_offset", "payload.*")
    )

    query = (
        parsed_stream.writeStream
        .foreachBatch(lambda batch_df, batch_id: process_batch(batch_df, batch_id, model))
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=os.getenv("SPARK_TRIGGER_INTERVAL", "5 seconds"))
        .start()
    )
    LOGGER.info("UPIGuard streaming query started; topic=%s brokers=%s", topic, kafka_brokers)
    query.awaitTermination()


if __name__ == "__main__":
    main()
