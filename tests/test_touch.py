"""
Tests para _touch, veces_visto, ultima_vez_visto y visible.

PRERREQUISITO: Las migrations de las 3 columnas deben estar aplicadas en producción
antes de correr estos tests.

Ejecutar:
    python -m pytest back/tests/test_touch.py -v
"""

import os
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import pymysql
import pymysql.cursors

def _conn():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        port=int(os.getenv("DB_PORT", 3306)),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )

def _fbid():
    return f"test_{uuid.uuid4().hex[:12]}"

def _post_fixture(fbid):
    return {
        "tipo": "negocio",
        "fbid_post": fbid,
        "titulo": f"Negocio test {fbid}",
        "descripcion": "Descripción de prueba para test de touch.",
        "categoria_id": 1,
        "autor": "test_autor",
        "fecha_captura": "2026-01-01",
        "fecha_post": "1 de enero a las 10:00",
        "imagenes_cloudinary": [],
    }

def _fetch_negocio(nid):
    # Conexión fresca por cada lectura — evita snapshot de REPEATABLE READ
    c = _conn()
    cur = c.cursor()
    cur.execute("SELECT id, veces_visto, ultima_vez_visto, visible FROM negocios WHERE id = %s", (nid,))
    row = cur.fetchone()
    cur.close(); c.close()
    return row

def _cleanup(conn, fbid):
    cur = conn.cursor()
    cur.execute("DELETE FROM negocios WHERE fbid_post = %s", (fbid,))
    conn.commit()
    cur.close()


# ─── TEST 1 ───────────────────────────────────────────────────────────────────
def test_insert_nuevo_inicializa_campos():
    """Post nuevo: veces_visto=1, ultima_vez_visto poblado, visible=1."""
    from db import insertar_negocio

    fbid = _fbid()
    try:
        nid, status = insertar_negocio(_post_fixture(fbid), colonia_id=1)
        assert status == "nuevo", f"Esperaba 'nuevo', got '{status}'"

        row = _fetch_negocio(nid)
        assert row is not None, "Fila no encontrada en DB"
        assert row["veces_visto"] == 1, f"veces_visto={row['veces_visto']}, esperaba 1"
        assert row["ultima_vez_visto"] is not None, "ultima_vez_visto es NULL"
        assert row["visible"] == 1, f"visible={row['visible']}, esperaba 1"

        print(f"  ✅ test 1 OK — nid={nid}, veces_visto=1, visible=1")
    finally:
        conn = _conn(); _cleanup(conn, fbid); conn.close()


# ─── TEST 2 ───────────────────────────────────────────────────────────────────
def test_reinsertar_actualiza_touch():
    """Re-insertar mismo fbid_post: veces_visto=2, ultima_vez_visto posterior."""
    from db import insertar_negocio

    fbid = _fbid()
    try:
        nid, _ = insertar_negocio(_post_fixture(fbid), colonia_id=1)
        t1 = _fetch_negocio(nid)["ultima_vez_visto"]

        time.sleep(1)

        nid2, status = insertar_negocio(_post_fixture(fbid), colonia_id=1)
        assert status == "duplicado", f"Esperaba 'duplicado', got '{status}'"
        assert nid2 == nid, f"ID cambió: {nid} → {nid2}"

        row = _fetch_negocio(nid)
        assert row["veces_visto"] == 2, f"veces_visto={row['veces_visto']}, esperaba 2"
        assert row["ultima_vez_visto"] > t1, (
            f"ultima_vez_visto no avanzó: t1={t1}, t2={row['ultima_vez_visto']}"
        )

        print(f"  ✅ test 2 OK — veces_visto=2, ultima_vez_visto actualizado")
    finally:
        conn = _conn(); _cleanup(conn, fbid); conn.close()


# ─── TEST 3 ───────────────────────────────────────────────────────────────────
def test_visible_0_oculta_negocio():
    """Negocio con visible=0 no aparece en query pública (WHERE visible=1)."""
    from db import insertar_negocio

    fbid = _fbid()
    try:
        nid, _ = insertar_negocio(_post_fixture(fbid), colonia_id=1)

        conn = _conn()
        cur = conn.cursor()
        cur.execute("UPDATE negocios SET visible = 0 WHERE id = %s", (nid,))
        conn.commit()
        cur.close(); conn.close()

        conn2 = _conn()
        cur2 = conn2.cursor()
        cur2.execute(
            "SELECT id FROM negocios WHERE activo = 1 AND visible = 1 AND id = %s",
            (nid,)
        )
        row = cur2.fetchone()
        cur2.close(); conn2.close()

        assert row is None, f"Negocio {nid} con visible=0 apareció en query pública"
        print(f"  ✅ test 3 OK — visible=0 oculta correctamente el negocio")
    finally:
        conn = _conn(); _cleanup(conn, fbid); conn.close()


if __name__ == "__main__":
    print("Ejecutando tests de _touch...\n")
    test_insert_nuevo_inicializa_campos()
    test_reinsertar_actualiza_touch()
    test_visible_0_oculta_negocio()
    print("\nTodos los tests pasaron ✅")
