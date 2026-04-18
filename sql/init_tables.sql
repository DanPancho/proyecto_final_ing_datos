CREATE TABLE IF NOT EXISTS dim_productos (
    product_id BIGINT PRIMARY KEY,
    category_id NUMERIC,
    category_code TEXT,
    categoria_principal TEXT,
    subcategoria TEXT,
    tipo_producto TEXT,
    brand TEXT,
    brand_clean TEXT,
    price NUMERIC(12,2),
    rango_precio TEXT
);

CREATE TABLE IF NOT EXISTS dim_usuarios (
    user_id BIGINT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS fact_eventos (
    evento_id SERIAL PRIMARY KEY,
    event_time TIMESTAMP,
    event_type VARCHAR(50),
    tipo_interaccion VARCHAR(50),
    es_compra INTEGER,
    product_id BIGINT,
    user_id BIGINT,
    user_session TEXT,
    price NUMERIC(12,2),
    origen_flujo VARCHAR(20),
    FOREIGN KEY (product_id) REFERENCES dim_productos(product_id),
    FOREIGN KEY (user_id) REFERENCES dim_usuarios(user_id)
);