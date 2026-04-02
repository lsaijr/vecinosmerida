import os
import mysql.connector

def get_conn():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        port=int(os.getenv("DB_PORT", 3306))
    )

def guardar_post(p):
    conn = get_conn()
    cursor = conn.cursor()

    sql = "INSERT INTO posts (titulo, contenido, categoria, telefono) VALUES (%s, %s, %s, %s)"

    cursor.execute(sql, (
        p.get("titulo"),
        p.get("contenido"),
        p.get("categoria"),
        p.get("telefono")
    ))

    conn.commit()
    cursor.close()
    conn.close()
