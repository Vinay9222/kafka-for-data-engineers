import json
import uuid

from confluent_kafka import Producer

producer_config = {
    "bootstrap.servers": "localhost:9092"
}

producer = Producer(producer_config)

def deliver_report(err,msg):
    if err:
        print(f" Deliver failed : {err}")
    else:
        print(f"Delivered successfully {msg.value().decode("utf-8")}")
        print(f"Delivered to {msg.topic()} partition {msg.partition()} offset : {msg.offset()}")

order = {
    "order_id": str(uuid.uuid4()),
    "user":"nana",
    "item":"pizza",
    "quantity":2
}

value = json.dumps(order).encode("utf-8")

producer.produce(
        topic="orders", 
        value=value,
        callback=deliver_report
    )

producer.flush()