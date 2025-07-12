import os
import json
import logging
from datetime import datetime
from confluent_kafka import Consumer, Producer
from sqlalchemy import text

from pipelines.resources.postgres import DbSessionLocal

KAFKA_SERVERS = os.environ["KAFKA_SERVERS"]
KAFKA_TOPIC_IN = "market.data.transformed"
KAFKA_TOPIC_OUT = KAFKA_TOPIC_IN
PARSED_SOURCE = "dnse"
CONSUME_BATCH_SIZE = 20000
INSERT_SQL_TEMPLATE = """
    INSERT INTO trading.stock_volume_profile_history (
        time, symbol, resolution, price, total_buy, total_sell, updated
    ) VALUES (
        :time, :symbol, :resolution, :price, :total_buy, :total_sell, :updated
    )
    ON CONFLICT (time, symbol, resolution, price) DO UPDATE
    SET
        total_buy = trading.stock_volume_profile_history.total_buy + EXCLUDED.total_buy,
        total_sell = trading.stock_volume_profile_history.total_sell + EXCLUDED.total_sell,
        updated = EXCLUDED.updated
    RETURNING time, symbol, resolution, price, total_buy, total_sell
"""


def _generate_volume_profile_keys(ts: datetime, symbol, price):
    return [
        (
            "m",
            (price, symbol, ts.strftime("%Y-%m-%d %H:%M:00"))
        ),
        (
            "h",
            (price, symbol, ts.strftime("%Y-%m-%d %H:00:00"))
        ),
        (
            "D",
            (price, symbol, ts.strftime("%Y-%m-%d 00:00:00"))
        ),
    ]

def generate_volume_profile_rows(msg):
    ts = datetime.fromtimestamp(msg["time"])
    now = datetime.now()
    rows = []
    for res, key in _generate_volume_profile_keys(ts, msg["symbol"], msg["price"]):
        row = {
            "key": key.__str__(),
            "time": datetime.strptime(key[2], "%Y-%m-%d %H:%M:%S"),
            "symbol": msg["symbol"],
            "price": msg["price"],
            "resolution": res,
            "total_buy": msg["vol"] if msg["side"] == "B" else 0.0,
            "total_sell": msg["vol"] if msg["side"] == "S" else 0.0,
            "updated": now,
        }
        rows.append(row)
    return rows

def run_volume_profile_pipeline():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_SERVERS,
        "group.id": "generate-volume-profile-batch",
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })
    producer = Producer({"bootstrap.servers": KAFKA_SERVERS})

    consumer.subscribe([KAFKA_TOPIC_IN])
    print("Consuming from Kafka...")

    while True:
        volume_profile_states = {}
        messages = consumer.consume(CONSUME_BATCH_SIZE, timeout=10)
        if messages is None:
            continue

        generated_records = []
        logging.info("Received %d messages", len(messages))
        for message in messages:
            if message.error():
                logging.error("Kafka error: %s", message.error())
                continue

            try:
                data = json.loads(message.value().decode("utf-8"))
            except Exception:
                logging.exception(f"Invalid message: {message.value()}")
                continue

            if data.get("data_type") != "ST" or data.get("source") != PARSED_SOURCE:
                continue

            records = generate_volume_profile_rows(data)
            generated_records.extend(records)

        # Aggregate the generated records into states
        if len(generated_records) == 0:
            continue

        logging.info("Aggregating %d generated records into volume profile states", len(generated_records))
        for record in generated_records:
            # {
            # 'time': datetime.datetime(2025, 6, 27, 13, 0),
            # 'symbol': 'VCB', 'price': 57.1,
            # 'resolution': 'h',
            # 'total_buy': 0.0, 'total_sell': 50.0,
            # 'updated': datetime.datetime(2025, 7, 1, 11, 1, 1, 186414),
            # 'data_type': 'VP'
            # }
            key_str = record.pop("key")
            if key_str not in volume_profile_states:
                volume_profile_states[key_str] = record
            else:
                volume_profile_states[key_str]["total_buy"] += record["total_buy"]
                volume_profile_states[key_str]["total_sell"] += record["total_sell"]
                volume_profile_states[key_str]["updated"] = record["updated"]

        try:
            with DbSessionLocal() as db:
                logging.info("Updating %d volume profile records into database", len(volume_profile_states))
                for item in volume_profile_states.values():
                    result = db.execute(text(INSERT_SQL_TEMPLATE), item).fetchone()
                    # Send to kafka
                    if result:
                        row = {
                            "time": int(result.time.timestamp()),
                            "symbol": result.symbol.strip(),
                            "resolution": result.resolution.strip(),
                            "price": result.price,
                            "total_buy": result.total_buy,
                            "total_sell": result.total_sell,
                            "source": PARSED_SOURCE,
                            "data_type": "VP",
                        }
                        producer.produce(
                            KAFKA_TOPIC_OUT,
                            key=row["symbol"],
                            value=json.dumps(row, default=str),
                        )
                db.commit()
                producer.flush()

            consumer.commit()
        except Exception as e:
            logging.exception("Failed to update DB or send to Kafka. Rolling back transaction. Error: %s", e)
        finally:
            producer.poll(0)  # clean up delivery queue (non-blocking)
