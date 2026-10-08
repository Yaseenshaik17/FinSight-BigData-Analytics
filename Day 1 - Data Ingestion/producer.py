import csv
import json
import time
from kafka import KafkaProducer

CSV_FILE_PATH = "/home/clouduser/Capstone Project/Bigdata Data set file/src-data/Transactions.csv"
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "txn-raw"

TARGET_MSGS_PER_SEC = 1000
BATCH_SIZE = 100

TRANSACTION_SCHEMA = {
    "type": "struct",
    "fields": [
        {"field": "step", "type": "int32", "optional": False},
        {"field": "type", "type": "string", "optional": False},
        {"field": "amount", "type": "double", "optional": False},
        {"field": "nameOrig", "type": "string", "optional": False},
        {"field": "oldbalanceOrg", "type": "double", "optional": False},
        {"field": "newbalanceOrig", "type": "double", "optional": False},
        {"field": "nameDest", "type": "string", "optional": False},
        {"field": "oldbalanceDest", "type": "double", "optional": False},
        {"field": "newbalanceDest", "type": "double", "optional": False},
        {"field": "isFraud", "type": "int32", "optional": False},
        {"field": "isFlaggedFraud", "type": "int32", "optional": False}
    ],
    "optional": False,
    "name": "Transaction"
}

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None
)


def build_payload(row):
    return {
        "step": int(row["step"]),
        "type": row["type"],
        "amount": float(row["amount"]),
        "nameOrig": row["nameOrig"],
        "oldbalanceOrg": float(row["oldbalanceOrg"]),
        "newbalanceOrig": float(row["newbalanceOrig"]),
        "nameDest": row["nameDest"],
        "oldbalanceDest": float(row["oldbalanceDest"]),
        "newbalanceDest": float(row["newbalanceDest"]),
        "isFraud": int(row["isFraud"]),
        "isFlaggedFraud": int(row["isFlaggedFraud"])
    }


def stream_transactions():

    sent_count = 0
    start_time = time.time()

    print("Starting Kafka producer...")
    print(f"Reading: {CSV_FILE_PATH}")
    print(f"Sending to topic: {TOPIC_NAME}")
    print(f"Target rate: {TARGET_MSGS_PER_SEC} messages/sec")

    with open(CSV_FILE_PATH, "r", newline="", encoding="utf-8") as csv_file:

        reader = csv.DictReader(csv_file)

        for row in reader:

            key = row.get("nameOrig")

            message = {
                "schema": TRANSACTION_SCHEMA,
                "payload": build_payload(row)
            }

            producer.send(
                TOPIC_NAME,
                key=key,
                value=message
            )

            sent_count += 1

            if sent_count % BATCH_SIZE == 0:

                elapsed = time.time() - start_time
                expected_time = sent_count / TARGET_MSGS_PER_SEC

                sleep_time = expected_time - elapsed

                if sleep_time > 0:
                    time.sleep(sleep_time)

            if sent_count % 10000 == 0:

                elapsed = time.time() - start_time
                rate = sent_count / elapsed

                print(
                    f"Sent {sent_count} messages | "
                    f"Rate: {rate:.1f} msg/sec"
                )

    producer.flush()
    producer.close()

    total_time = time.time() - start_time

    print(
        f"Done. Sent {sent_count} messages in "
        f"{total_time:.1f} seconds "
        f"(avg {sent_count / total_time:.1f} msg/sec)"
    )


if __name__ == "__main__":
    stream_transactions()
