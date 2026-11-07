import json
import logging
import os
import random
import time
from datetime import datetime, timezone
from typing import Dict, List

from kafka import KafkaProducer
from kafka.errors import KafkaError

CITIES: List[str] = [
    "Pune", "Mumbai", "Delhi", "Bengaluru", "Hyderabad",
    "Chennai", "Kolkata", "Ahmedabad", "Jaipur", "Ranchi",
]

FEATURE_FIELDS = [
    "transaction_amount", "transaction_hour", "account_age",
    "user_transactions_last_24h", "failed_attempts", "beneficiary_age",
    "is_new_beneficiary", "device_type", "city_code", "transaction_method",
]

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
LOGGER = logging.getLogger("upiguard.producer")


def make_producer() -> KafkaProducer:
    brokers = [x.strip() for x in os.getenv("KAFKA_BROKERS", "broker:29092").split(",") if x.strip()]
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=brokers,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                retries=5,
                request_timeout_ms=30000,
                api_version_auto_timeout_ms=30000,
                max_block_ms=30000,
                acks="all",
            )
            LOGGER.info("Connected to Kafka brokers: %s", brokers)
            return producer
        except Exception:
            LOGGER.exception("Kafka is not ready; retrying in 5 seconds")
            time.sleep(5)


def base_transaction() -> Dict:
    city = random.choice(CITIES)
    now = datetime.now(timezone.utc)
    return {
        "account_id": random.choice([11111, 22222, 33333, 44444, 55555]),
        "transaction_time": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "city": city,
        "city_code": CITIES.index(city),
        "transaction_method": random.choices([0, 1], weights=[0.90, 0.10])[0],
        "device_type": random.choices([0, 1, 2], weights=[0.55, 0.30, 0.15])[0],
    }


def send_forever(transaction_factory, interval_seconds: float, label: str) -> None:
    topic = os.getenv("KAFKA_TOPIC", "raw_transactions")
    producer = make_producer()
    LOGGER.info("Starting %s producer; topic=%s interval=%ss", label, topic, interval_seconds)
    while True:
        try:
            transaction = transaction_factory()
            producer.send(topic, value=transaction).get(timeout=15)
            LOGGER.info("%s transaction sent: %s", label, json.dumps(transaction, sort_keys=True))
        except KafkaError:
            LOGGER.exception("Kafka send failed; recreating producer")
            try:
                producer.close(timeout=5)
            except Exception:
                pass
            producer = make_producer()
        except Exception:
            LOGGER.exception("Unexpected producer error")
        time.sleep(interval_seconds)


def create_normal_transaction() -> Dict:
    transaction = base_transaction()
    now = datetime.now(timezone.utc)
    transaction.update({
        "transaction_amount": round(random.uniform(50, 5000), 2),
        "transaction_hour": now.hour,
        "account_age": random.randint(90, 5000),
        "user_transactions_last_24h": random.randint(0, 7),
        "failed_attempts": random.choices([0, 1, 2], weights=[0.65, 0.25, 0.10])[0],
        "beneficiary_age": random.randint(60, 5000),
        "is_new_beneficiary": random.choices([0, 1], weights=[0.90, 0.10])[0],
    })
    return transaction


if __name__ == "__main__":
    send_forever(create_normal_transaction, 1.0, "UPIGuard normal")
