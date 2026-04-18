import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "ecommerce_db",
    "user": "ecommerceuser",
    "password": "password"
}

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    with open("sql/init_tables.sql", "r", encoding="utf-8") as f:
        sql_script = f.read()

    cur.execute(sql_script)
    conn.commit()

    cur.close()
    conn.close()

    print("Tablas creadas correctamente.")

if __name__ == "__main__":
    main()