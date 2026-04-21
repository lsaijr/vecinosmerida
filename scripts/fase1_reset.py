"""
Fase 1 — Reset de DB + schema para modelo de negocio y analítica.

Ejecuta en orden:
  G1 — Reset y limpieza (truncate tablas de contenido, limpieza cat_categorias)
  G3 — Modelo de negocio (tabla planes + FK en autores)
  G5 — Capacidad analítica (nse en colonias, posts_raw, system_changelog)

Grupos 2 (bugs) y 4 (validaciones) son ediciones de código — no en este script.

Uso:
    python3 back/scripts/fase1_reset.py --dry-run   # muestra plan sin ejecutar
    python3 back/scripts/fase1_reset.py --apply     # ejecuta de verdad

Pre-requisitos:
  - Dump reciente de la DB existe (respaldado)
  - back/.env tiene DB_* vars
"""

import argparse
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(ENV_PATH)

import mysql.connector  # noqa: E402


def connect(retries: int = 3):
    last_err = None
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
            last_err = e
            print(f"  [reintento {i+1}/{retries}] {e.errno}: {e.msg}", flush=True)
            time.sleep(2)
    raise RuntimeError(f"No se pudo conectar tras {retries} intentos: {last_err}")


# ─── G1 — Reset y limpieza ─────────────────────────────────────────────
G1_TRUNCATES = [
    "negocios_imagenes",
    "negocios_ratings",
    "negocios",
    "perdidos_imagenes",
    "perdidos",
    "mascotas",
    "empleos",
    "alertas",
    "noticias",
    "autor_actividad",
]

G1_STEPS = [
    ("SET FOREIGN_KEY_CHECKS = 0", "desactivar FK checks temporalmente"),
    *[(f"TRUNCATE TABLE {t}", f"truncate {t}") for t in G1_TRUNCATES],
    (
        "DELETE FROM cat_categorias WHERE id IN (12, 14, 15, 16)",
        "eliminar cat 12 (General) + 14/15/16 (subcategorías mascotas)",
    ),
    ("SET FOREIGN_KEY_CHECKS = 1", "reactivar FK checks"),
    (
        "ALTER TABLE negocios MODIFY categoria_id INT NOT NULL",
        "forzar categoria_id NOT NULL en negocios",
    ),
]

# ─── G3 — Modelo de negocio (planes) ───────────────────────────────────
G3_STEPS = [
    (
        """
        CREATE TABLE IF NOT EXISTS planes (
            id             INT AUTO_INCREMENT PRIMARY KEY,
            nombre         VARCHAR(50)  NOT NULL UNIQUE,
            precio_mensual DECIMAL(10,2) NULL,
            precio_anual   DECIMAL(10,2) NULL,
            descripcion    VARCHAR(500) NULL,
            features       JSON         NULL,
            activo         TINYINT(1)   NOT NULL DEFAULT 1,
            orden          TINYINT      NOT NULL DEFAULT 0,
            creado_en      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
        )
        """,
        "crear tabla planes",
    ),
    (
        """
        INSERT IGNORE INTO planes (nombre, precio_mensual, precio_anual, descripcion, activo, orden) VALUES
            ('gratis', 0.00, 0.00,
             'Plan gratuito. Tarjeta estándar con modal al click, sin URL propia.', 1, 0),
            ('premium', NULL, NULL,
             'Plan premium. Tarjeta destacada + URL propia + galería. Precio pendiente.', 1, 1)
        """,
        "insertar planes iniciales (gratis, premium)",
    ),
    (
        "ALTER TABLE autores ADD COLUMN plan_id INT NULL AFTER plan",
        "agregar columna plan_id en autores",
    ),
    (
        "ALTER TABLE autores ADD CONSTRAINT fk_autores_plan "
        "FOREIGN KEY (plan_id) REFERENCES planes(id) ON DELETE SET NULL",
        "FK autores.plan_id → planes.id",
    ),
    (
        "ALTER TABLE autores DROP COLUMN plan",
        "eliminar columna plan vieja (todos NULL, nada que migrar)",
    ),
]

# ─── G5 — Capacidad analítica ──────────────────────────────────────────
G5_STEPS = [
    (
        "ALTER TABLE colonias ADD COLUMN nse "
        "ENUM('popular','medio_bajo','medio','medio_alto','alto') "
        "NOT NULL DEFAULT 'medio' AFTER ciudad",
        "agregar columna nse en colonias",
    ),
    (
        """
        CREATE TABLE IF NOT EXISTS posts_raw (
            id             BIGINT AUTO_INCREMENT PRIMARY KEY,
            fbid_post      VARCHAR(30)  NULL,
            group_id       VARCHAR(30)  NULL,
            colonia_id     INT          NULL,
            fecha_captura  DATE         NULL,
            payload        JSON         NOT NULL,
            creado_en      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_fbid (fbid_post),
            INDEX idx_group (group_id),
            INDEX idx_fecha (fecha_captura)
        )
        """,
        "crear tabla posts_raw (archivo append-only del JSON crudo)",
    ),
    (
        """
        CREATE TABLE IF NOT EXISTS system_changelog (
            id           INT AUTO_INCREMENT PRIMARY KEY,
            fecha        DATE         NOT NULL,
            tipo         ENUM('schema','data','config','deploy','otro') NOT NULL,
            descripcion  VARCHAR(500) NOT NULL,
            reversible   TINYINT(1)   NOT NULL DEFAULT 1,
            creado_en    TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
        )
        """,
        "crear tabla system_changelog",
    ),
    (
        """
        INSERT INTO system_changelog (fecha, tipo, descripcion, reversible) VALUES
            (CURDATE(), 'schema', 'Fase 1 — reset DB, crear planes/posts_raw/system_changelog, agregar nse en colonias, FK plan_id en autores', 0)
        """,
        "registrar Fase 1 en system_changelog",
    ),
]


def run_group(conn, name: str, steps, apply: bool):
    print(f"\n━━━ {name} ━━━")
    cur = conn.cursor()
    for sql, label in steps:
        sql_clean = " ".join(sql.strip().split())
        preview = sql_clean[:110] + ("…" if len(sql_clean) > 110 else "")
        print(f"  • {label}")
        print(f"    SQL: {preview}")
        if not apply:
            continue
        try:
            cur.execute(sql)
            # cada DDL hace autocommit implícito; los DML los comiteamos al final del grupo
            rowcount = cur.rowcount if cur.rowcount >= 0 else "-"
            print(f"    OK (rowcount={rowcount})")
        except mysql.connector.Error as e:
            print(f"    ❌ ERROR {e.errno}: {e.msg}", file=sys.stderr)
            conn.rollback()
            cur.close()
            raise
    conn.commit()
    cur.close()


def verify(conn):
    print("\n━━━ Verificación post-ejecución ━━━")
    cur = conn.cursor(dictionary=True)

    # 1. Tablas de contenido vacías
    for t in G1_TRUNCATES:
        cur.execute(f"SELECT COUNT(*) AS n FROM {t}")
        n = cur.fetchone()["n"]
        mark = "✓" if n == 0 else "✗"
        print(f"  {mark} {t}: {n} filas")

    # 2. cat_categorias sin 12/14/15/16
    cur.execute("SELECT id, nombre FROM cat_categorias WHERE id IN (12,14,15,16)")
    rows = cur.fetchall()
    mark = "✓" if not rows else "✗"
    print(f"  {mark} cat_categorias sin huérfanos (quedan: {rows})")

    # 3. categoria_id NOT NULL
    cur.execute("SHOW COLUMNS FROM negocios LIKE 'categoria_id'")
    col = cur.fetchone()
    mark = "✓" if col and col["Null"] == "NO" else "✗"
    print(f"  {mark} negocios.categoria_id NOT NULL (actual Null={col['Null'] if col else '?'})")

    # 4. Tabla planes existe con 2 filas
    cur.execute("SELECT id, nombre FROM planes ORDER BY orden")
    planes = cur.fetchall()
    mark = "✓" if len(planes) == 2 else "✗"
    print(f"  {mark} planes poblada: {planes}")

    # 5. autores tiene plan_id, no tiene plan
    cur.execute("SHOW COLUMNS FROM autores")
    cols = {c["Field"]: c for c in cur.fetchall()}
    has_plan_id = "plan_id" in cols
    has_plan = "plan" in cols
    mark = "✓" if has_plan_id and not has_plan else "✗"
    print(f"  {mark} autores: plan_id={'sí' if has_plan_id else 'NO'}, plan legacy={'NO' if not has_plan else 'sí (debería no estar)'}")

    # 6. colonias.nse
    cur.execute("SHOW COLUMNS FROM colonias LIKE 'nse'")
    col = cur.fetchone()
    mark = "✓" if col else "✗"
    print(f"  {mark} colonias.nse existe ({col['Type'] if col else '—'})")

    # 7. posts_raw y system_changelog
    for t in ("posts_raw", "system_changelog"):
        cur.execute(
            "SELECT COUNT(*) AS n FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
            (t,),
        )
        exists = cur.fetchone()["n"] > 0
        mark = "✓" if exists else "✗"
        print(f"  {mark} tabla {t} existe")

    cur.close()


def main():
    ap = argparse.ArgumentParser()
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true", help="muestra el plan sin ejecutar")
    group.add_argument("--apply", action="store_true", help="ejecuta las operaciones")
    args = ap.parse_args()

    print(f"{'[DRY-RUN]' if args.dry_run else '[APPLY]'} Fase 1 — Reset + Schema")
    print(f"Target: {os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")

    conn = connect()
    try:
        run_group(conn, "G1 — Reset y limpieza", G1_STEPS, args.apply)
        run_group(conn, "G3 — Modelo de negocio (planes)", G3_STEPS, args.apply)
        run_group(conn, "G5 — Capacidad analítica", G5_STEPS, args.apply)
        if args.apply:
            verify(conn)
        else:
            print("\n(dry-run: no se verifica, no se ejecutó nada)")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
