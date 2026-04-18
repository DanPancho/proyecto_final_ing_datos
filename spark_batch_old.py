from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lower, split
from pyspark.sql.functions import regexp_replace, to_timestamp

# Crear sesión
spark = SparkSession.builder \
    .appName("EcommerceBatch") \
    .getOrCreate()

csv_path = "/opt/data/data/processed/ecommerce_data_process.csv"

print("Leyendo CSV...")
df = spark.read.csv(csv_path, header=True, inferSchema=True)

# ==============================
# LIMPIEZA
# ==============================

df = df.filter(col("event_type").isNotNull())

# Convertir fecha
df = df.withColumn(
    "event_time",
    regexp_replace(col("event_time"), "\\[UTC\\]", "")
)

df = df.withColumn(
    "event_time",
    to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ssX")
)

# Normalizar brand
df = df.withColumn(
    "brand_clean",
    when(col("brand").isNull(), "sin_marca")
    .otherwise(lower(col("brand")))
)

# Separar category_code
df = df.withColumn("categoria_principal", split(col("category_code"), "\.").getItem(0))
df = df.withColumn("subcategoria", split(col("category_code"), "\.").getItem(1))
df = df.withColumn("tipo_producto", split(col("category_code"), "\.").getItem(2))

# Rango precio
df = df.withColumn(
    "rango_precio",
    when(col("price") < 50, "barato")
    .when((col("price") >= 50) & (col("price") < 200), "medio")
    .otherwise("alto")
)

# Es compra
df = df.withColumn(
    "es_compra",
    when(col("event_type") == "purchase", 1).otherwise(0)
)

# Tipo interacción
df = df.withColumn(
    "tipo_interaccion",
    when(col("event_type") == "view", "visualizacion")
    .when(col("event_type") == "cart", "carrito")
    .when(col("event_type") == "purchase", "compra")
)

# ==============================
# DIMENSIONES
# ==============================

print("Generando dim_productos...")

dim_productos = df.select(
    "product_id",
    "category_id",
    "category_code",
    "categoria_principal",
    "subcategoria",
    "tipo_producto",
    "brand",
    "brand_clean",
    "price",
    "rango_precio"
).dropDuplicates(["product_id"])

print("Generando dim_usuarios...")

dim_usuarios = df.select("user_id").dropDuplicates(["user_id"])

print("Generando fact_eventos...")

fact_eventos = df.select(
    "event_time",
    "event_type",
    "tipo_interaccion",
    "es_compra",
    "product_id",
    "user_id",
    "user_session",
    "price"
)

# ==============================
# CONEXIÓN A POSTGRES
# ==============================

url = "jdbc:postgresql://postgres:5432/ecommerce_db"

properties = {
    "user": "ecommerceuser",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# ==============================
# INSERTAR DATOS
# ==============================

print("Insertando dim_productos...")
dim_productos.write \
    .jdbc(url=url, table="dim_productos", mode="append", properties=properties)

print("Insertando dim_usuarios...")
dim_usuarios.write \
    .jdbc(url=url, table="dim_usuarios", mode="append", properties=properties)

print("Insertando fact_eventos...")
fact_eventos.write \
    .jdbc(url=url, table="fact_eventos", mode="append", properties=properties)

print("Carga completa con Spark.")

spark.stop()