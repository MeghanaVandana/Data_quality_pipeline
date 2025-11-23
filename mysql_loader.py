import mysql.connector
from mysql.connector import Error
import pandas as pd

def load_to_mysql(df, table_name, db_config):

    try:
        conn = mysql.connector.connect(**db_config)
        cur = conn.cursor()

        # Create table if not exists
        create_cols = ", ".join([f"{col} TEXT" for col in df.columns])
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                {create_cols}
            )
        """)

        # Ensure all columns exist
        cur.execute(f"SHOW COLUMNS FROM {table_name}")
        existing_cols = {row[0] for row in cur.fetchall()}

        for col in df.columns:
            if col not in existing_cols:
                cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} TEXT")

        # Truncate before reload
        cur.execute(f"TRUNCATE TABLE {table_name}")

        # Insert data
        cols = ", ".join(df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))

        insert_sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
        cur.executemany(insert_sql, df.astype(str).values.tolist())

        conn.commit()
        cur.close()
        conn.close()
        print(f"Loaded {len(df)} rows into {table_name}")

    except Error as e:
        print("MYSQL ERROR:", e)
