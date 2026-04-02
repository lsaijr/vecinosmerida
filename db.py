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

# ─── COLONIAS ────────────────────────────────────────────────
def obtener_colonias():
    conn = get_conn()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT id, nombre FROM colonias WHERE activa = 1 ORDER BY nombre")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

# ─── GRUPOS ──────────────────────────────────────────────────
def buscar_grupo(group_id):
    conn = get_conn()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM grupos_facebook WHERE group_id = %s", (group_id,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row

def registrar_grupo(group_id, nombre, tipo, colonia_ids, notas=""):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO grupos_facebook (group_id, nombre, tipo, notas) VALUES (%s, %s, %s, %s)",
        (group_id, nombre, tipo, notas)
    )
    nuevo_id = cursor.lastrowid
    for cid in colonia_ids:
        cursor.execute(
            "INSERT INTO grupos_colonias (grupo_id, colonia_id) VALUES (%s, %s)",
            (nuevo_id, cid)
        )
    conn.commit()
    cursor.close()
    conn.close()
    return nuevo_id

def actualizar_grupo_stats(group_id, total_posts):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE grupos_facebook SET total_posts = total_posts + %s, ultimo_proceso = CURDATE() WHERE group_id = %s",
        (total_posts, group_id)
    )
    conn.commit()
    cursor.close()
    conn.close()

def obtener_colonias_de_grupo(group_id):
    conn = get_conn()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT c.id, c.nombre FROM colonias c
        JOIN grupos_colonias gc ON gc.colonia_id = c.id
        JOIN grupos_facebook gf ON gf.id = gc.grupo_id
        WHERE gf.group_id = %s
    """, (group_id,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

# ─── CATEGORÍAS ──────────────────────────────────────────────
def obtener_categorias_negocios():
    conn = get_conn()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT id, nombre, emoji, color_hex FROM cat_categorias ORDER BY nombre")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

def obtener_categorias_noticias():
    conn = get_conn()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT id, nombre, slug, color, icono FROM categorias_noticias ORDER BY nombre")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

def obtener_categorias_alertas():
    conn = get_conn()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT id, nombre, slug, parent_id, color, icono FROM categorias_alertas ORDER BY parent_id, nombre")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

# ─── PIPELINE LOG ────────────────────────────────────────────
def registrar_pipeline_log(archivo_json, colonia, total_posts, negocios_nuevos,
                            negocios_dup, imagenes_ok, imagenes_fail, estado, error_msg=""):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO pipeline_log
        (archivo_json, colonia, total_posts, negocios_nuevos, negocios_dup,
         imagenes_ok, imagenes_fail, estado, error_msg)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (archivo_json, colonia, total_posts, negocios_nuevos, negocios_dup,
          imagenes_ok, imagenes_fail, estado, error_msg))
    conn.commit()
    cursor.close()
    conn.close()
