import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

TOPIC_NAME = "eventos-ecommerce"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

EVENT_TYPES = ["view", "cart", "purchase"]
CATEGORY_CODES = [
    "electronics.smartphone",
    "appliances.kitchen.washer",
    "furniture.living_room.sofa",
    "computers.notebook",
    "beauty.skincare.face",
    None
]
BRANDS = ["samsung", "lg", "sony", "apple", "lenovo", "shiseido", None]

def generar_evento():
    return {
        "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "event_type": random.choice(EVENT_TYPES),
        "product_id": random.randint(1000000, 9999999),
        "category_id": random.randint(1000000000000000000, 9999999999999999999),
        "category_code": random.choice(CATEGORY_CODES),
        "brand": random.choice(BRANDS),
        "price": round(random.uniform(10, 1500), 2),
        "user_id": random.randint(100000000, 999999999),
        "user_session": f"session-{random.randint(1000, 9999)}"
    }

def main():
    print(f"Enviando eventos al topic '{TOPIC_NAME}'...")

    while True:
        evento = generar_evento()
        producer.send(TOPIC_NAME, value=evento)
        producer.flush()
        print("Evento enviado:", evento)
        time.sleep(2)

if __name__ == "__main__":
    main()