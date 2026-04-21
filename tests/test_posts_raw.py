"""
Tests para el flujo posts_raw con tabla posts_raw_test en HostGator.

Cada test crea lo que necesita y limpia al final.
Ejecutar desde la raíz del proyecto:
    python -m pytest back/tests/test_posts_raw.py -v
o directamente:
    python back/tests/test_posts_raw.py
"""

import json
import os
import sys
from pathlib import Path

# Asegurar que db.py sea importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import mysql.connector

# ─── conexión directa (independiente de get_conn) ────────────────────
# NOTA: HostGator puede rechazar conexiones desde IPs no whitelisted.
# Si falla localmente, ejecutar desde Railway o habilitar acceso remoto
# en el panel de HostGator → MySQL → Remote MySQL.

import time

def _conn(retries=3):
    last = None
    for i in range(retries):
        try:
            return mysql.connector.connect(
                host=os.getenv("DB_HOST"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                database=os.getenv("DB_NAME"),
                port=int(os.getenv("DB_PORT", 3306)),
                ssl_disabled=True,
                connection_timeout=20,
                autocommit=False,
            )
        except mysql.connector.Error as e:
            last = e
            if i < retries - 1:
                time.sleep(2)
    raise last

# ─── setup / teardown de tabla de prueba ─────────────────────────────

TABLE = "posts_raw_test"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    fbid_post      VARCHAR(30)  NULL,
    group_id       VARCHAR(30)  NULL,
    colonia_id     INT          NULL,
    autor_id       VARCHAR(40)  NULL,
    fecha_captura  DATE         NULL,
    payload        JSON         NOT NULL,
    estado         ENUM('archivado','procesado','publicado','descartado') DEFAULT 'archivado',
    razon_descarte VARCHAR(50)  NULL,
    creado_en      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_fbid_group (fbid_post, group_id),
    INDEX idx_group (group_id),
    INDEX idx_estado (estado)
)
"""

def setup_table():
    conn = _conn()
    cur = conn.cursor()
    cur.execute(CREATE_SQL)
    conn.commit()
    cur.close()
    conn.close()

def teardown_rows(*fbids):
    """Limpia filas específicas por fbid_post (no dropea la tabla)."""
    if not fbids:
        return
    conn = _conn()
    cur = conn.cursor()
    placeholders = ",".join(["%s"] * len(fbids))
    cur.execute(f"DELETE FROM {TABLE} WHERE fbid_post IN ({placeholders})", list(fbids))
    conn.commit()
    cur.close()
    conn.close()

def fetch_row(fbid_post, group_id):
    conn = _conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        f"SELECT * FROM {TABLE} WHERE fbid_post=%s AND group_id=%s",
        (str(fbid_post)[:30], str(group_id)[:30]),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


# ─── importar funciones apuntando a tabla de prueba ──────────────────
# Las funciones de db.py usan la tabla "posts_raw" hardcodeada.
# Para tests usamos parches mínimos que reemplazan el nombre de tabla.

_ESTADOS_VALIDOS = {"archivado", "procesado", "publicado", "descartado"}


def _parsear_fecha(fecha_captura):
    """DD-MM-YYYY → date. Inline para no importar db.py (requiere Python 3.10+)."""
    if not fecha_captura or not isinstance(fecha_captura, str):
        return fecha_captura
    from datetime import datetime
    try:
        return datetime.strptime(fecha_captura, "%d-%m-%Y").date()
    except ValueError:
        return None


def _payload_limpio(p):
    imagenes_limpias = [
        {"fbid": img.get("fbid"), "alt": img.get("alt")}
        for img in (p.get("imagenes") or [])
        if isinstance(img, dict)
    ]
    payload = {k: v for k, v in p.items() if k not in ("imagenes_cloudinary", "_autor_db_id", "_es_empresa")}
    payload["imagenes"] = imagenes_limpias
    return payload


def _insertar_raw_test(fbid_post, group_id, colonia_id, autor_id, fecha_captura, payload_dict):
    """Versión de insertar_post_raw que escribe en posts_raw_test."""
    conn = _conn()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            INSERT INTO {TABLE}
              (fbid_post, group_id, colonia_id, autor_id, fecha_captura, payload, estado)
            VALUES (%s, %s, %s, %s, %s, %s, 'archivado')
            ON DUPLICATE KEY UPDATE payload = VALUES(payload)
            """,
            (
                str(fbid_post)[:30],
                str(group_id)[:30] if group_id else None,
                colonia_id,
                str(autor_id)[:40] if autor_id else None,
                _parsear_fecha(fecha_captura),
                json.dumps(payload_dict, ensure_ascii=False),
            ),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def _marcar_raw_test(fbid_post, group_id, estado, razon_descarte=None):
    """Versión de marcar_post_raw que opera en posts_raw_test."""
    if estado not in _ESTADOS_VALIDOS:
        raise ValueError(f"Estado inválido: {estado!r}")
    if estado != "descartado":
        razon_descarte = None
    conn = _conn()
    cur = conn.cursor()
    rows = 0
    try:
        cur.execute(
            f"UPDATE {TABLE} SET estado=%s, razon_descarte=%s "
            "WHERE fbid_post=%s AND group_id=%s",
            (estado, razon_descarte, str(fbid_post)[:30], str(group_id)[:30] if group_id else None),
        )
        rows = cur.rowcount
        conn.commit()
    finally:
        cur.close()
        conn.close()
    return rows


def _bulk_marcar_test(updates):
    """Versión de bulk_marcar_posts_raw que opera en posts_raw_test."""
    if not updates:
        return 0
    conn = _conn()
    cur = conn.cursor()
    updated = 0
    try:
        for u in updates:
            estado = u.get("estado")
            if estado not in _ESTADOS_VALIDOS:
                continue
            razon = u.get("razon_descarte") if estado == "descartado" else None
            fbid  = u.get("fbid_post")
            gid   = u.get("group_id")
            if not fbid:
                continue
            cur.execute(
                f"UPDATE {TABLE} SET estado=%s, razon_descarte=%s "
                "WHERE fbid_post=%s AND group_id=%s",
                (estado, razon, str(fbid)[:30], str(gid)[:30] if gid else None),
            )
            updated += cur.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    return updated


def _bulk_insert_test(posts, meta):
    """Versión de insertar_posts_raw_bulk que opera en posts_raw_test."""
    group_id      = meta.get("group_id")
    colonia_id    = meta.get("colonia_id")
    fecha_captura = _parsear_fecha(meta.get("fecha_captura"))
    conn = _conn()
    cur = conn.cursor()
    inserted = 0
    errors = []
    try:
        for p in posts:
            fbid_post = p.get("fbid_post")
            if not fbid_post:
                continue
            try:
                cur.execute(
                    f"""
                    INSERT INTO {TABLE}
                      (fbid_post, group_id, colonia_id, autor_id, fecha_captura, payload, estado)
                    VALUES (%s, %s, %s, %s, %s, %s, 'archivado')
                    ON DUPLICATE KEY UPDATE payload = VALUES(payload)
                    """,
                    (
                        str(fbid_post)[:30],
                        str(group_id)[:30] if group_id else None,
                        colonia_id,
                        str(p.get("autor_id") or "")[:40] or None,
                        fecha_captura,
                        json.dumps(_payload_limpio(p), ensure_ascii=False),
                    ),
                )
                inserted += 1
            except Exception as e:
                errors.append({"fbid_post": str(fbid_post), "error": str(e)[:120]})
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    return {"inserted": inserted, "errors": errors}


# ─── TESTS ───────────────────────────────────────────────────────────

FBID_A = "test_raw_001"
FBID_B = "test_raw_002"
FBID_C = "test_raw_003"
GID    = "test_group_99"

ALL_TEST_FBIDS = [FBID_A, FBID_B, FBID_C]


def test_insertar_post_raw():
    """insertar_post_raw crea con estado='archivado'. Segunda llamada actualiza payload."""
    _insertar_raw_test(FBID_A, GID, 15, "autor_1", "20-04-2026", {"texto": "original"})
    row = fetch_row(FBID_A, GID)
    assert row is not None, "Fila no encontrada tras INSERT"
    assert row["estado"] == "archivado", f"Estado esperado 'archivado', got {row['estado']}"
    payload = json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
    assert payload.get("texto") == "original"

    # ON DUPLICATE KEY UPDATE: actualizar payload sin cambiar estado
    _insertar_raw_test(FBID_A, GID, 15, "autor_1", "20-04-2026", {"texto": "actualizado"})
    row2 = fetch_row(FBID_A, GID)
    payload2 = json.loads(row2["payload"]) if isinstance(row2["payload"], str) else row2["payload"]
    assert payload2.get("texto") == "actualizado", "ON DUPLICATE KEY no actualizó payload"
    assert row2["estado"] == "archivado", "ON DUPLICATE KEY no debe cambiar estado"
    print("  ✓ insertar_post_raw")


def test_marcar_post_raw():
    """marcar_post_raw actualiza estado y devuelve rowcount."""
    _insertar_raw_test(FBID_B, GID, 15, "autor_2", "20-04-2026", {"texto": "b"})

    rows = _marcar_raw_test(FBID_B, GID, "procesado")
    assert rows == 1, f"Esperaba 1 fila actualizada, got {rows}"
    row = fetch_row(FBID_B, GID)
    assert row["estado"] == "procesado"

    rows2 = _marcar_raw_test(FBID_B, GID, "descartado", "clasif_ignorar")
    assert rows2 == 1
    row2 = fetch_row(FBID_B, GID)
    assert row2["estado"] == "descartado"
    assert row2["razon_descarte"] == "clasif_ignorar"

    # razon_descarte debe ser NULL cuando estado != 'descartado'
    _marcar_raw_test(FBID_B, GID, "publicado")
    row3 = fetch_row(FBID_B, GID)
    assert row3["estado"] == "publicado"
    assert row3["razon_descarte"] is None, "razon_descarte debe ser NULL para publicado"

    # estado inválido lanza ValueError
    try:
        _marcar_raw_test(FBID_B, GID, "estado_falso")
        assert False, "Debería haber lanzado ValueError"
    except ValueError:
        pass
    print("  ✓ marcar_post_raw")


def test_marcar_desync_devuelve_cero():
    """marcar_post_raw devuelve 0 cuando el registro no existe (desync)."""
    rows = _marcar_raw_test("fbid_que_no_existe", GID, "procesado")
    assert rows == 0, f"Esperaba 0 (desync), got {rows}"
    print("  ✓ marcar_post_raw desync → 0")


def test_bulk_marcar_posts_raw():
    """bulk_marcar_posts_raw actualiza múltiples filas en una transacción."""
    _insertar_raw_test(FBID_A, GID, 15, "autor_1", "20-04-2026", {"texto": "a"})
    _insertar_raw_test(FBID_B, GID, 15, "autor_2", "20-04-2026", {"texto": "b"})

    updates = [
        {"fbid_post": FBID_A, "group_id": GID, "estado": "procesado"},
        {"fbid_post": FBID_B, "group_id": GID, "estado": "procesado"},
    ]
    updated = _bulk_marcar_test(updates)
    assert updated == 2, f"Esperaba 2 filas actualizadas, got {updated}"
    assert fetch_row(FBID_A, GID)["estado"] == "procesado"
    assert fetch_row(FBID_B, GID)["estado"] == "procesado"
    print("  ✓ bulk_marcar_posts_raw")


def test_insertar_posts_raw_bulk():
    """insertar_posts_raw_bulk inserta masivamente y reporta inserted/errors."""
    posts = [
        {"fbid_post": FBID_A, "autor_id": "a1", "texto": "post a", "imagenes": []},
        {"fbid_post": FBID_B, "autor_id": "a2", "texto": "post b", "imagenes": []},
        {"fbid_post": FBID_C, "autor_id": "a3", "texto": "post c", "imagenes": []},
        {"fbid_post": None,   "autor_id": "a4", "texto": "sin fbid"},  # debe ignorarse
    ]
    meta = {"group_id": GID, "colonia_id": 15, "fecha_captura": "20-04-2026"}
    result = _bulk_insert_test(posts, meta)
    assert result["inserted"] == 3, f"Esperaba 3 insertados, got {result['inserted']}"
    assert len(result["errors"]) == 0
    for fbid in (FBID_A, FBID_B, FBID_C):
        row = fetch_row(fbid, GID)
        assert row is not None, f"Fila {fbid} no encontrada"
        assert row["estado"] == "archivado"
    print("  ✓ insertar_posts_raw_bulk")


def test_flujo_completo_ciclo_5_momentos():
    """
    Integración: simula el ciclo completo de 5 momentos para 3 posts.
    - FBID_A → negocios → publicado
    - FBID_B → ignorar → clasif_ignorar
    - FBID_C → error_insert → error_insert
    """
    posts = [
        {"fbid_post": FBID_A, "autor_id": "a1", "texto": "negocio", "imagenes": []},
        {"fbid_post": FBID_B, "autor_id": "a2", "texto": "ignorar", "imagenes": []},
        {"fbid_post": FBID_C, "autor_id": "a3", "texto": "error",   "imagenes": []},
    ]
    meta = {"group_id": GID, "colonia_id": 15, "fecha_captura": "20-04-2026"}

    # M1 — bulk insert (archivado)
    _bulk_insert_test(posts, meta)
    for fbid in (FBID_A, FBID_B, FBID_C):
        assert fetch_row(fbid, GID)["estado"] == "archivado"

    # M4 — bulk marcar a procesado
    updates_m4 = [{"fbid_post": f, "group_id": GID, "estado": "procesado"} for f in (FBID_A, FBID_B, FBID_C)]
    _bulk_marcar_test(updates_m4)
    for fbid in (FBID_A, FBID_B, FBID_C):
        assert fetch_row(fbid, GID)["estado"] == "procesado"

    # M5 — resultado por post
    _marcar_raw_test(FBID_A, GID, "publicado")
    _marcar_raw_test(FBID_B, GID, "descartado", "clasif_ignorar")
    _marcar_raw_test(FBID_C, GID, "descartado", "error_insert")

    row_a = fetch_row(FBID_A, GID)
    row_b = fetch_row(FBID_B, GID)
    row_c = fetch_row(FBID_C, GID)

    assert row_a["estado"] == "publicado"
    assert row_a["razon_descarte"] is None

    assert row_b["estado"] == "descartado"
    assert row_b["razon_descarte"] == "clasif_ignorar"

    assert row_c["estado"] == "descartado"
    assert row_c["razon_descarte"] == "error_insert"

    print("  ✓ flujo completo 5 momentos")


# ─── runner ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"\nConectando a {os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")
    print(f"Tabla de prueba: {TABLE}\n")

    setup_table()
    print("Tabla de prueba lista.\n")

    tests = [
        test_insertar_post_raw,
        test_marcar_post_raw,
        test_marcar_desync_devuelve_cero,
        test_bulk_marcar_posts_raw,
        test_insertar_posts_raw_bulk,
        test_flujo_completo_ciclo_5_momentos,
    ]

    def _safe_teardown():
        try:
            teardown_rows(*ALL_TEST_FBIDS)
        except Exception as e:
            print(f"  [teardown warning] {e}")

    passed = failed = 0
    for t in tests:
        _safe_teardown()
        try:
            t()
            passed += 1
        except Exception as e:
            print(f"  ✗ {t.__name__}: {e}")
            failed += 1

    _safe_teardown()
    print(f"\n{'='*50}")
    print(f"Resultado: {passed} pasados, {failed} fallidos")
    if failed:
        sys.exit(1)
