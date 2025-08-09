import multiprocessing
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import logging
import uuid
import random
import time
import json

# ----------------------------
# Kafka Config
# ----------------------------
KAFKA_BROKERS = "localhost:29092,localhost:39092,localhost:49092"
print("Using Kafka brokers:", KAFKA_BROKERS)
NUM_PARTITIONS = 5
REPLICATION_FACTOR = 3
TOPIC_NAME = "financial_transactions"
AGGREGATES_TOPIC = "transaction_aggregates"
ANOMALIES_TOPIC = "transaction_anomalies"

# Production-ready defaults with reasonable test responsiveness
producer_conf = {
    "bootstrap.servers": KAFKA_BROKERS,
    "acks": 0,
    "compression.type": "snappy",
    "linger.ms": 5,                    # smaller for quicker sends in testing
    "batch.size": 1_000_000,           # keep large for throughput
    "queue.buffering.max.messages": 1000000,
    "queue.buffering.max.kbytes": 1048576,
    "enable.idempotence": False
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ----------------------------
# Topic creation helper
# ----------------------------
def create_topic(topic_name):
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKERS})
    try:
        metadata = admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            topic = NewTopic(
                topic=topic_name,
                num_partitions=NUM_PARTITIONS,
                replication_factor=REPLICATION_FACTOR
            )
            fs = admin_client.create_topics([topic])
            for t, future in fs.items():
                try:
                    future.result()
                    logger.info(f"Topic '{topic_name}' created successfully.")
                except Exception as e:
                    logger.error(f"Failed to create topic '{topic_name}': {e}")
        else:
            logger.info(f"ℹTopic '{topic_name}' already exists.")
    except Exception as e:
        logger.error(f"Error checking/creating topic '{topic_name}': {e}")

# ----------------------------
# Transaction Generator
# ----------------------------
def generate_transaction():
    return dict(
        transactionId=str(uuid.uuid4()),
        userId=f"user_{random.randint(1, 100)}",
        amount=round(random.uniform(50000, 150000), 2),
        transactionTime=int(time.time() * 1000),  # milliseconds for Spark
        merchantId=f"merchant_{random.randint(1, 50)}",
        transactionType=random.choice(["purchase", "refund"]),
        location=f"location_{random.randint(1, 50)}",
        paymentMethod=random.choice(["credit_card", "debit_card", "paypal"]),
        international=random.choice([True, False]),
        currency=random.choice(["USD", "EUR", "GBP"])
    )

# ----------------------------
# Delivery Callback
# ----------------------------
def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

# ----------------------------
# Producer Worker
# ----------------------------
def produce_transaction(thread_id):
    local_producer = Producer(producer_conf)
    count = 0
    total_count = 0
    last_report_time = time.time()

    try:
        while True:
            transaction = generate_transaction()
            try:
                local_producer.produce(
                    topic=TOPIC_NAME,
                    key=transaction["userId"],
                    value=json.dumps(transaction).encode("utf-8"),
                    on_delivery=delivery_report
                )
                count += 1
                total_count += 1

                # Report every 2 seconds or every 1000 messages
                if count % 1000 == 0 or (time.time() - last_report_time) >= 2:
                    print(f"[Thread {thread_id}] Produced {count} msgs in last batch | Total: {total_count}")
                    count = 0
                    last_report_time = time.time()

                local_producer.poll(0)  # handle delivery callbacks
            except Exception as e:
                logger.error(f"Failed to produce: {e}")
    except KeyboardInterrupt:
        print(f"Thread {thread_id} interrupted, flushing...")
        local_producer.flush()
        print(f"Thread {thread_id} flushed & exiting.")

# ----------------------------
# Parallel Producer Start
# ----------------------------
def produce_data_in_parallel(num_processes):
    processes = []
    for i in range(num_processes):
        p = multiprocessing.Process(target=produce_transaction, args=(i,))
        #daemon = True
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

# ----------------------------
# Main Entry
# ----------------------------
if __name__ == "__main__":
    create_topic(TOPIC_NAME)
    create_topic('transaction_aggregates')
    create_topic('transaction_anomalies')
    produce_data_in_parallel(4)

