from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lower, split, regexp_replace, to_timestamp

# ==============================
# CONFIGURACIÓN SPARK
# ==============================
spark = SparkSession.builder \
    .appName("EcommerceBatch") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.default.parallelism", "8") \
    .getOrCreate()

csv_path = "/opt/data/data/processed/ecommerce_data_process.csv"

print("Leyendo CSV...")

# IMPORTANTE:
# Evitamos inferSchema para ahorrar memoria.
df = spark.read.option("header", True).csv(csv_path)

# Repartir el dataset para usar mejor múltiples workers
df = df.repartition(8)

# ==============================
# LIMPIEZA Y TRANSFORMACIÓN
# ==============================
print("Transformando datos...")

df = df.filter(col("event_type").isNotNull())

# Limpiar event_time: quitar [UTC] y convertir a timestamp
df = df.withColumn(
    "event_time",
    regexp_replace(col("event_time"), "\\[UTC\\]", "")
)

# Intenta parsear formatos como:
# 2019-10-01T00:00Z
# 2019-10-01T00:00:01Z
df = df.withColumn(
    "event_time",
    to_timestamp(col("event_time"))
)

# Convertir tipos básicos
df = df.withColumn("product_id", col("product_id").cast("long"))
df = df.withColumn("category_id", col("category_id").cast("decimal(38,0)"))
df = df.withColumn("price", col("price").cast("double"))
df = df.withColumn("user_id", col("user_id").cast("long"))

# Normalizar brand
df = df.withColumn(
    "brand",
    when(col("brand").isNull(), "sin_marca").otherwise(col("brand"))
)

df = df.withColumn(
    "brand_clean",
    lower(col("brand"))
)

# category_code nulo
df = df.withColumn(
    "category_code",
    when(col("category_code").isNull(), "sin_categoria").otherwise(col("category_code"))
)

# Separar category_code
df = df.withColumn("categoria_principal", split(col("category_code"), "\\.").getItem(0))
df = df.withColumn("subcategoria", split(col("category_code"), "\\.").getItem(1))
df = df.withColumn("tipo_producto", split(col("category_code"), "\\.").getItem(2))

# Rango de precio
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
    .otherwise("desconocido")
)

# Origen flujo
df = df.withColumn("origen_flujo", when(col("event_type").isNotNull(), "batch"))

# Filtrar filas inválidas
df = df.filter(
    col("event_time").isNotNull() &
    col("product_id").isNotNull() &
    col("user_id").isNotNull()
)

print("Mostrando muestra de datos transformados:")
df.select("event_time", "event_type", "product_id", "user_id", "price").show(10, False)

# ==============================
# GENERAR DIMENSIONES Y HECHOS
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
).dropDuplicates(["product_id"]).repartition(4)

print("Generando dim_usuarios...")
dim_usuarios = df.select(
    "user_id"
).dropDuplicates(["user_id"]).repartition(4)

print("Generando fact_eventos...")
fact_eventos = df.select(
    "event_time",
    "event_type",
    "tipo_interaccion",
    "es_compra",
    "product_id",
    "user_id",
    "user_session",
    "price",
    "origen_flujo"
).repartition(4)

# ==============================
# CONEXIÓN A POSTGRESQL
# ==============================
url = "jdbc:postgresql://postgres:5432/ecommerce_db"

properties = {
    "user": "ecommerceuser",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# ==============================
# ESCRITURA EN POSTGRESQL
# IMPORTANTE: usar append, NO overwrite
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