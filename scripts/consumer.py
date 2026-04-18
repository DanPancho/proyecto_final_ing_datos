import json
import psycopg2
from kafka import KafkaConsumer

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "ecommerce_db",
    "user": "ecommerceuser",
    "password": "password"
}

TOPIC_NAME = "eventos-ecommerce"

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="ecommerce-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

def insertar_usuario(cur, user_id):
    cur.execute(
        """
        INSERT INTO dim_usuarios (user_id)
        VALUES (%s)
        ON CONFLICT (user_id) DO NOTHING
        """,
        (user_id,)
    )

def obtener_rango_precio(price):
    if price is None:
        return None
    if price < 50:
        return "barato"
    elif price < 200:
        return "medio"
    return "alto"

def separar_categoria(category_code):
    if not category_code:
        return (None, None, None)
    partes = category_code.split(".")
    categoria_principal = partes[0] if len(partes) > 0 else None
    subcategoria = partes[1] if len(partes) > 1 else None
    tipo_producto = partes[2] if len(partes) > 2 else None
    return categoria_principal, subcategoria, tipo_producto

def insertar_producto(cur, product_id, category_id, category_code, brand, price):
    categoria_principal, subcategoria, tipo_producto = separar_categoria(category_code)
    brand_clean = brand.lower() if brand else "sin_marca"
    rango_precio = obtener_rango_precio(price)

    cur.execute(
        """
        INSERT INTO dim_productos (
            product_id, category_id, category_code, categoria_principal,
            subcategoria, tipo_producto, brand, brand_clean, price, rango_precio
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (product_id) DO NOTHING
        """,
        (
            product_id, category_id, category_code, categoria_principal,
            subcategoria, tipo_producto, brand, brand_clean, price, rango_precio
        )
    )

def insertar_evento(cur, event):
    tipo_interaccion = {
        "view": "visualizacion",
        "cart": "carrito",
        "purchase": "compra"
    }.get(event["event_type"], event["event_type"])

    es_compra = 1 if event["event_type"] == "purchase" else 0

    cur.execute(
        """
        INSERT INTO fact_eventos (
            event_time,
            event_type,
            tipo_interaccion,
            es_compra,
            product_id,
            user_id,
            user_session,
            price,
            origen_flujo
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            event["event_time"],
            event["event_type"],
            tipo_interaccion,
            es_compra,
            event["product_id"],
            event["user_id"],
            event["user_session"],
            event["price"],
            "streaming"
        )
    )

def main():
    print(f"Escuchando mensajes del topic '{TOPIC_NAME}'...")

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for message in consumer:
        event = message.value
        print("Mensaje recibido:", event)

        try:
            insertar_usuario(cur, event["user_id"])
            insertar_producto(
                cur,
                event["product_id"],
                event.get("category_id"),
                event.get("category_code"),
                event.get("brand"),
                event.get("price")
            )
            insertar_evento(cur, event)

            conn.commit()
            print("Evento insertado correctamente.\n")

        except Exception as e:
            conn.rollback()
            print("Error al insertar evento:", e)

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()