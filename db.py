from __future__ import annotations

import json
import os
import re
import unicodedata
from datetime import date, timedelta
from functools import lru_cache

import mysql.connector


# ─── PARSER DE FECHA FACEBOOK ────────────────────────────────
_MESES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}
_DIAS_SEMANA = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]

def parsear_fecha_fb(fecha_post: str | None, fecha_captura: str) -> str:
    """
    Convierte texto de fecha de Facebook a YYYY-MM-DD.
    fecha_captura viene del meta del JSON: "10-04-2026" (DD-MM-YYYY)
    Siempre devuelve una fecha — fallback es fecha_captura.
    """
    try:
        hoy = date(
            int(fecha_captura[6:10]),
            int(fecha_captura[3:5]),
            int(fecha_captura[0:2]),
        )
    except Exception:
        hoy = date.today()

    if not fecha_post:
        return str(hoy)

    f = fecha_post.strip().lower()

    # "32 min" / "2 h" / "13 h" → hoy
    if re.match(r'^\d+\s*(min|h)$', f):
        return str(hoy)

    # "1 día" / "3 días" → restar días
    m = re.match(r'^(\d+)\s*d[íi]as?$', f)
    if m:
        return str(hoy - timedelta(days=int(m.group(1))))

    # "1 semana" / "2 semanas" → restar semanas
    m = re.match(r'^(\d+)\s*semanas?$', f)
    if m:
        return str(hoy - timedelta(weeks=int(m.group(1))))

    # "1 mes" / "3 meses" → restar ~30 días por mes
    m = re.match(r'^(\d+)\s*meses?$', f)
    if m:
        return str(hoy - timedelta(days=int(m.group(1)) * 30))

    # "ayer..." → ayer
    if f.startswith("ayer"):
        return str(hoy - timedelta(days=1))

    # "lunes", "martes"... → día de semana más reciente
    for i, dia in enumerate(_DIAS_SEMANA):
        if f.startswith(dia):
            dias_atras = (hoy.weekday() - i) % 7 or 7
            return str(hoy - timedelta(days=dias_atras))

    # "18 de noviembre de 2025" → fecha exacta con año
    m = re.match(r'^(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})', f)
    if m:
        mes = _MESES.get(m.group(2))
        if mes:
            return f"{m.group(3)}-{mes:02d}-{int(m.group(1)):02d}"

    # "30 de marzo..." / "9 de febrero" → año de captura
    m = re.match(r'^(\d{1,2})\s+de\s+(\w+)', f)
    if m:
        mes = _MESES.get(m.group(2))
        if mes:
            return f"{hoy.year}-{mes:02d}-{int(m.group(1)):02d}"

    # fallback
    return str(hoy)


def get_conn():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        port=int(os.getenv("DB_PORT", 3306)),
    )


@lru_cache(maxsize=128)
def _column_exists(table_name, column_name):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
        """,
        (os.getenv("DB_NAME"), table_name, column_name),
    )
    exists = cursor.fetchone()[0] > 0
    cursor.close()
    conn.close()
    return exists


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
        (group_id, nombre, tipo, notas),
    )
    nuevo_id = cursor.lastrowid
    for cid in colonia_ids:
        cursor.execute(
            "INSERT INTO grupos_colonias (grupo_id, colonia_id) VALUES (%s, %s)",
            (nuevo_id, cid),
        )
    conn.commit()
    cursor.close()
    conn.close()
    return nuevo_id


def actualizar_grupo_stats(group_id, total_posts, miembros=None):
    conn = get_conn()
    cursor = conn.cursor()
    if miembros is not None:
        cursor.execute(
            "UPDATE grupos_facebook SET total_posts = total_posts + %s, ultimo_proceso = CURDATE(), miembros = %s WHERE group_id = %s",
            (total_posts, miembros, group_id),
        )
    else:
        cursor.execute(
            "UPDATE grupos_facebook SET total_posts = total_posts + %s, ultimo_proceso = CURDATE() WHERE group_id = %s",
            (total_posts, group_id),
        )
    conn.commit()
    cursor.close()
    conn.close()


def actualizar_colonias_grupo(group_id, colonia_ids):
    """Reemplaza las colonias asociadas a un grupo existente."""
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM grupos_facebook WHERE group_id = %s LIMIT 1", (group_id,))
    row = cursor.fetchone()
    if not row:
        cursor.close(); conn.close()
        return
    grupo_pk = row[0]
    cursor.execute("DELETE FROM grupos_colonias WHERE grupo_id = %s", (grupo_pk,))
    for cid in colonia_ids:
        if cid:
            cursor.execute(
                "INSERT INTO grupos_colonias (grupo_id, colonia_id) VALUES (%s, %s)",
                (grupo_pk, cid),
            )
    conn.commit()
    cursor.close()
    conn.close()


def obtener_colonias_de_grupo(group_id):
    conn = get_conn()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT c.id, c.nombre FROM colonias c
        JOIN grupos_colonias gc ON gc.colonia_id = c.id
        JOIN grupos_facebook gf ON gf.id = gc.grupo_id
        WHERE gf.group_id = %s
        """,
        (group_id,),
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


# ─── CATEGORÍAS ──────────────────────────────────────────────
def obtener_categorias_negocios():
    conn = get_conn()
    cursor = conn.cursor(dictionary=True)
    if _column_exists("cat_categorias", "keywords"):
        cursor.execute("SELECT id, nombre, emoji, color_hex, keywords FROM cat_categorias ORDER BY nombre")
    else:
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
    cursor.execute(
        """
        INSERT INTO pipeline_log
        (archivo_json, colonia, total_posts, negocios_nuevos, negocios_dup,
         imagenes_ok, imagenes_fail, estado, error_msg)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (archivo_json, colonia, total_posts, negocios_nuevos, negocios_dup,
         imagenes_ok, imagenes_fail, estado, error_msg),
    )
    conn.commit()
    cursor.close()
    conn.close()


# ─── DEDUPLICACIÓN POR FBID ───────────────────────────────────
def fbid_ya_existe(fbid):
    if not fbid:
        return False
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM negocios_imagenes WHERE fbid = %s LIMIT 1", (str(fbid),))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row is not None


def negocio_ya_existe(fbid_post):
    if not fbid_post:
        return None
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM negocios WHERE fbid_post = %s LIMIT 1", (str(fbid_post),))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row[0] if row else None


def _img_url(asset):
    if isinstance(asset, dict):
        return asset.get("url")
    return asset


def _img_fbid(asset, fallback=None):
    if isinstance(asset, dict):
        return asset.get("fbid") or fallback
    return fallback


def _img_alt(asset):
    if isinstance(asset, dict):
        return asset.get("alt")
    return None


def _img_public_id(asset):
    if isinstance(asset, dict):
        return asset.get("public_id")
    return None


def _grupos_origen_json(p):
    """Construye el JSON de grupos_origen a partir del post. Devuelve str o None."""
    val = p.get("grupos_origen")
    if val is None:
        return None
    if isinstance(val, str):
        return val  # ya serializado
    return json.dumps(val, ensure_ascii=False)


# ─── INSERCIÓN EN DB ─────────────────────────────────────────
def insertar_negocio(p, colonia_id):
    fbid_post = p.get("fbid_post")
    existing = negocio_ya_existe(fbid_post)
    if existing:
        return existing, "duplicado"

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO negocios
          (nombre, categoria_id, descripcion, telefono, whatsapp,
           facebook, colonia_id, fuente_autor, autor_id, fecha_captura,
           activo, fbid_post, fecha_post, fecha_post_dt, repeticiones,
           grupos_origen)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,1,%s,%s,%s,%s,%s)
        """,
        (
            p.get("titulo") or p.get("nombre", "")[:100],
            p.get("categoria_id"),
            p.get("descripcion", "")[:1200],
            p.get("telefono"),
            p.get("telefono"),
            p.get("url_post") or None,
            colonia_id,
            p.get("autor", "")[:200],
            p.get("_autor_db_id") or None,
            p.get("fecha_captura"),
            fbid_post,
            (p.get("fecha_post") or "")[:60] or None,
            parsear_fecha_fb(p.get("fecha_post"), p.get("fecha_captura", "")),
            int(p.get("repeticiones") or 1),
            _grupos_origen_json(p),
        ),
    )
    negocio_id = cursor.lastrowid

    for i, asset in enumerate(p.get("imagenes_cloudinary", [])):
        url = _img_url(asset)
        if not url:
            continue
        fbid = _img_fbid(asset, fallback=(p.get("imagenes", [{}] * 0)[i].get("fbid") if i < len(p.get("imagenes", [])) and isinstance(p.get("imagenes", [])[i], dict) else None))
        alt = _img_alt(asset)
        public_id = _img_public_id(asset)

        cols = ["negocio_id", "imagen_url", "fbid", "orden", "creado_en"]
        vals = [negocio_id, url, str(fbid) if fbid else None, i, "NOW()"]
        params = [negocio_id, url, str(fbid) if fbid else None, i]
        if _column_exists("negocios_imagenes", "alt_text"):
            cols.insert(3, "alt_text")
            vals.insert(3, "%s")
            params.insert(3, alt)
        if _column_exists("negocios_imagenes", "public_id"):
            insert_at = 4 if _column_exists("negocios_imagenes", "alt_text") else 3
            cols.insert(insert_at, "public_id")
            vals.insert(insert_at, "%s")
            params.insert(insert_at, public_id)

        sql_vals = []
        param_iter = iter(params)
        for v in vals:
            if v == "NOW()":
                sql_vals.append("NOW()")
            else:
                sql_vals.append("%s")
        cursor.execute(
            f"INSERT INTO negocios_imagenes ({', '.join(cols)}) VALUES ({', '.join(sql_vals)})",
            tuple(params),
        )

        if i == 0:
            cursor.execute(
                "UPDATE negocios SET imagen_cloudinary = %s WHERE id = %s",
                (url, negocio_id),
            )

    conn.commit()
    cursor.close()
    conn.close()
    return negocio_id, "nuevo"



def _slugify(texto: str, max_len: int = 60) -> str:
    s = unicodedata.normalize("NFKD", texto)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    return s[:max_len].rstrip("-")


def insertar_noticia(p, colonia_id):
    fbid_post = p.get("fbid_post")
    conn = get_conn()
    cursor = conn.cursor()
    if fbid_post:
        cursor.execute("SELECT id FROM noticias WHERE fbid_post = %s LIMIT 1", (str(fbid_post),))
        row = cursor.fetchone()
        if row:
            cursor.close(); conn.close()
            return row[0], "duplicado"

    imagen_principal = _img_url(p.get("imagenes_cloudinary", [None])[0]) if p.get("imagenes_cloudinary") else None
    cursor.execute(
        """
        INSERT INTO noticias
          (titulo, texto, categoria_id, colonia_id, autor, autor_id,
           imagen_url, url_post, fbid_post, fecha_publicacion,
           fecha_post, fecha_post_dt, grupos_origen, status)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'publicado')
        """,
        (
            (p.get("titulo") or "")[:200],
            p.get("texto", ""),
            p.get("categoria_id"),
            colonia_id,
            p.get("autor", "")[:200],
            p.get("_autor_db_id") or None,
            imagen_principal,
            p.get("url_post") or None,
            fbid_post,
            parsear_fecha_fb(p.get("fecha_post"), p.get("fecha_captura", "")),
            (p.get("fecha_post") or "")[:60] or None,
            parsear_fecha_fb(p.get("fecha_post"), p.get("fecha_captura", "")),
            _grupos_origen_json(p),
        ),
    )
    nid = cursor.lastrowid

    slug_base = _slugify(p.get("titulo") or "noticia")
    slug = f"{slug_base}-{nid}" if slug_base else f"noticia-{nid}"
    cursor.execute("UPDATE noticias SET slug = %s WHERE id = %s", (slug, nid))

    conn.commit()
    cursor.close()
    conn.close()
    return nid, "nuevo"



def insertar_alerta(p, colonia_id):
    fbid_post = p.get("fbid_post")
    conn = get_conn()
    cursor = conn.cursor()
    if fbid_post:
        cursor.execute("SELECT id FROM alertas WHERE fbid_post = %s LIMIT 1", (str(fbid_post),))
        row = cursor.fetchone()
        if row:
            cursor.close(); conn.close()
            return row[0], "duplicado"

    imagen_principal = _img_url(p.get("imagenes_cloudinary", [None])[0]) if p.get("imagenes_cloudinary") else None
    cursor.execute(
        """
        INSERT INTO alertas
          (texto, categoria_id, colonia_id, direccion_aprox,
           autor, autor_id, imagen_url, url_post, fbid_post, fecha,
           fecha_post, fecha_post_dt, grupos_origen)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            p.get("texto", ""),
            p.get("categoria_id"),
            colonia_id,
            p.get("direccion_aprox"),
            p.get("autor", "")[:200],
            p.get("_autor_db_id") or None,
            imagen_principal,
            p.get("url_post") or None,
            fbid_post,
            parsear_fecha_fb(p.get("fecha_post"), p.get("fecha_captura", "")),
            (p.get("fecha_post") or "")[:60] or None,
            parsear_fecha_fb(p.get("fecha_post"), p.get("fecha_captura", "")),
            _grupos_origen_json(p),
        ),
    )
    aid = cursor.lastrowid
    conn.commit()
    cursor.close()
    conn.close()
    return aid, "nuevo"


# ─── EMPLEO ──────────────────────────────────────────────────

EMPLEO_AREAS_SQL = """
CREATE TABLE IF NOT EXISTS empleo_areas (
  id        TINYINT AUTO_INCREMENT PRIMARY KEY,
  nombre    VARCHAR(60) NOT NULL,
  slug      VARCHAR(40) NOT NULL UNIQUE,
  icono     VARCHAR(10),
  color_hex VARCHAR(7)
);

INSERT IGNORE INTO empleo_areas (nombre, slug, icono, color_hex) VALUES
('Cocina y alimentos',      'cocina',          '🍳', '#f97316'),
('Ventas y atención',       'ventas',          '💼', '#10b981'),
('Seguridad',               'seguridad',       '🛡', '#3b82f6'),
('Transporte y logística',  'transporte',      '🚗', '#6366f1'),
('Administrativo',          'administrativo',  '📋', '#8b5cf6'),
('Limpieza y mantenimiento','limpieza',        '🧹', '#06b6d4'),
('Construcción',            'construccion',    '🏗', '#78716c'),
('Salud',                   'salud',           '🏥', '#ef4444'),
('Tecnología',              'tecnologia',      '💻', '#0ea5e9'),
('Educación',               'educacion',       '📚', '#7c3aed'),
('Almacén',                 'almacen',         '📦', '#f59e0b'),
('General',                 'general',         '💼', '#6b7280');

CREATE TABLE IF NOT EXISTS empleos (
  id              INT AUTO_INCREMENT PRIMARY KEY,
  tipo            ENUM('oferta','busqueda') NOT NULL DEFAULT 'oferta',
  area_id         TINYINT,
  puesto          VARCHAR(150),
  empresa         VARCHAR(150),
  descripcion     TEXT,
  horario         VARCHAR(100),
  zona            VARCHAR(100),
  telefono        VARCHAR(20),
  imagen_url      VARCHAR(500),
  autor           VARCHAR(200),
  colonia_id      INT,
  fbid_post       VARCHAR(30) UNIQUE,
  fecha_captura   DATE,
  activo          TINYINT DEFAULT 1,
  creado_en       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (area_id) REFERENCES empleo_areas(id)
);
"""


def empleo_ya_existe(fbid_post):
    if not fbid_post:
        return None
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM empleos WHERE fbid_post = %s LIMIT 1", (str(fbid_post),))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row[0] if row else None


def _get_area_id(area_val):
    """Retorna el id de empleo_areas (1-12).
    Acepta:
      - int/str numérico en rango 1-12 → devuelve directamente (area_id)
      - str de nombre de área           → busca en SLUG_MAP
    Fallback: None (el caller decide el default).
    """
    # Caso: valor numérico (entero o string numérico) — ya es el area_id
    try:
        v = int(area_val)
        if 1 <= v <= 12:
            return v
    except (TypeError, ValueError):
        pass

    # Caso: nombre de área en español
    SLUG_MAP = {
        'Cocina':          'cocina',
        'Ventas':          'ventas',
        'Seguridad':       'seguridad',
        'Transporte':      'transporte',
        'Administrativo':  'administrativo',
        'Limpieza':        'limpieza',
        'Construcción':    'construccion',
        'Salud':           'salud',
        'Tecnología':      'tecnologia',
        'Educación':       'educacion',
        'Almacén':         'almacen',
        'General':         'general',
    }
    slug = SLUG_MAP.get(str(area_val) if area_val else 'General', 'general')
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM empleo_areas WHERE slug = %s LIMIT 1", (slug,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row[0] if row else None


def insertar_empleo(p, colonia_id):
    fbid_post = p.get("fbid_post")
    existing  = empleo_ya_existe(fbid_post)
    if existing:
        return existing, "duplicado"

    area_id   = _get_area_id(p.get("area_id") or p.get("area") or None) or 12
    imgs      = p.get("imagenes_cloudinary") or []
    img_url   = _img_url(imgs[0]) if imgs else None
    telefono  = p.get("telefono") or None

    conn   = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO empleos
          (tipo, area_id, puesto, empresa, descripcion,
           horario, zona, telefono, imagen_url,
           autor, autor_id, colonia_id, fbid_post, fecha_captura,
           fecha_post, fecha_post_dt, grupos_origen)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            p.get("tipo_empleo", "oferta"),
            area_id,
            (p.get("puesto") or "")[:150] or None,
            (p.get("empresa") or p.get("autor") or "")[:150] or None,
            p.get("descripcion", "")[:4000],
            (p.get("horario") or "")[:100] or None,
            (p.get("zona") or "")[:100] or None,
            telefono,
            img_url,
            (p.get("autor") or "")[:200],
            p.get("_autor_db_id") or None,
            colonia_id,
            fbid_post,
            p.get("fecha_captura"),
            (p.get("fecha_post") or "")[:60] or None,
            parsear_fecha_fb(p.get("fecha_post"), p.get("fecha_captura", "")),
            _grupos_origen_json(p),
        ),
    )
    empleo_id = cursor.lastrowid
    conn.commit()
    cursor.close()
    conn.close()
    return empleo_id, "nuevo"


# ─── POSTS RAW ───────────────────────────────────────────────

_ESTADOS_RAW_VALIDOS = {'archivado', 'procesado', 'publicado', 'descartado'}


def _payload_limpio(p):
    """Payload sin url_temp (expiran 24-48h) ni datos internos del pipeline."""
    imagenes_limpias = [
        {"fbid": img.get("fbid"), "alt": img.get("alt")}
        for img in (p.get("imagenes") or [])
        if isinstance(img, dict)
    ]
    payload = {k: v for k, v in p.items() if k not in ("imagenes_cloudinary", "_autor_db_id", "_es_empresa")}
    payload["imagenes"] = imagenes_limpias
    return payload


def _parsear_fecha_captura(fecha_captura):
    if not fecha_captura or not isinstance(fecha_captura, str):
        return fecha_captura
    from datetime import datetime
    try:
        return datetime.strptime(fecha_captura, "%d-%m-%Y").date()
    except ValueError:
        return None


def insertar_post_raw(fbid_post, group_id, colonia_id, autor_id, fecha_captura, payload_dict):
    """INSERT con estado='archivado'. ON DUPLICATE KEY actualiza payload."""
    if not fbid_post:
        return
    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO posts_raw
              (fbid_post, group_id, colonia_id, autor_id, fecha_captura, payload, estado)
            VALUES (%s, %s, %s, %s, %s, %s, 'archivado')
            ON DUPLICATE KEY UPDATE payload = VALUES(payload)
            """,
            (
                str(fbid_post)[:30],
                str(group_id)[:30] if group_id else None,
                colonia_id,
                str(autor_id)[:40] if autor_id else None,
                _parsear_fecha_captura(fecha_captura),
                json.dumps(payload_dict, ensure_ascii=False),
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def marcar_post_raw(fbid_post, group_id, estado, razon_descarte=None):
    """UPDATE de estado en un registro. Valida ENUM. Retorna filas afectadas (0 = desync)."""
    if estado not in _ESTADOS_RAW_VALIDOS:
        raise ValueError(f"Estado inválido: {estado!r}. Válidos: {_ESTADOS_RAW_VALIDOS}")
    if estado != "descartado":
        razon_descarte = None

    conn = get_conn()
    cursor = conn.cursor()
    rows = 0
    try:
        cursor.execute(
            "UPDATE posts_raw SET estado = %s, razon_descarte = %s "
            "WHERE fbid_post = %s AND group_id = %s",
            (estado, razon_descarte, str(fbid_post)[:30], str(group_id)[:30] if group_id else None),
        )
        rows = cursor.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
    return rows


def bulk_marcar_posts_raw(updates):
    """
    Updates masivos en una transacción.
    updates: [{fbid_post, group_id, estado, razon_descarte?}]
    """
    if not updates:
        return 0
    conn = get_conn()
    cursor = conn.cursor()
    updated = 0
    try:
        for u in updates:
            estado = u.get("estado")
            if estado not in _ESTADOS_RAW_VALIDOS:
                continue
            razon = u.get("razon_descarte") if estado == "descartado" else None
            fbid  = u.get("fbid_post")
            gid   = u.get("group_id")
            if not fbid:
                continue
            cursor.execute(
                "UPDATE posts_raw SET estado = %s, razon_descarte = %s "
                "WHERE fbid_post = %s AND group_id = %s",
                (estado, razon, str(fbid)[:30], str(gid)[:30] if gid else None),
            )
            updated += cursor.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
    return updated


def insertar_posts_raw_bulk(posts, meta):
    """
    INSERT masivo en transacción. Ignora errores individuales.
    meta: {group_id, colonia_id, fecha_captura}
    Retorna {inserted: N, errors: [...]}
    """
    group_id      = meta.get("group_id")
    colonia_id    = meta.get("colonia_id")
    fecha_captura = _parsear_fecha_captura(meta.get("fecha_captura"))

    conn = get_conn()
    cursor = conn.cursor()
    inserted = 0
    errors = []

    try:
        for p in posts:
            fbid_post = p.get("fbid_post")
            if not fbid_post:
                continue
            try:
                cursor.execute(
                    """
                    INSERT INTO posts_raw
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
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

    return {"inserted": inserted, "errors": errors}


# ─── AUTORES Y ACTIVIDAD ─────────────────────────────────────

def upsert_autor(autor_id_fb, nombre):
    """
    Inserta el autor si no existe, o actualiza su nombre si cambió.
    Retorna el id interno (PK) del autor.
    """
    if not autor_id_fb:
        return None

    conn   = get_conn()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT id, nombre FROM autores WHERE autor_id_fb = %s LIMIT 1",
        (str(autor_id_fb),)
    )
    row = cursor.fetchone()

    if row:
        autor_db_id    = row[0]
        nombre_actual  = row[1] or ''
        nombre_nuevo   = (nombre or '').strip()[:200]
        # Actualizar nombre solo si cambió y el nuevo no está vacío
        if nombre_nuevo and nombre_nuevo != nombre_actual:
            cursor.execute(
                "UPDATE autores SET nombre = %s WHERE id = %s",
                (nombre_nuevo, autor_db_id)
            )
            conn.commit()
    else:
        cursor.execute(
            "INSERT INTO autores (autor_id_fb, nombre) VALUES (%s, %s)",
            (str(autor_id_fb), (nombre or '').strip()[:200])
        )
        conn.commit()
        autor_db_id = cursor.lastrowid

    cursor.close()
    conn.close()
    return autor_db_id


def registrar_actividad(autor_db_id, group_id, group_name, tipo_post,
                         fbid_post=None, fecha=None):
    """
    Registra una aparición del autor en un grupo con un tipo de post.
    Evita duplicados por fbid_post si está disponible.
    """
    if not autor_db_id:
        return

    conn   = get_conn()
    cursor = conn.cursor()

    # Evitar duplicado si ya registramos este post exacto
    if fbid_post:
        cursor.execute(
            "SELECT id FROM autor_actividad WHERE fbid_post = %s LIMIT 1",
            (str(fbid_post),)
        )
        if cursor.fetchone():
            cursor.close()
            conn.close()
            return

    cursor.execute(
        """
        INSERT INTO autor_actividad
          (autor_id, group_id, group_name, tipo_post, fbid_post, fecha)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            autor_db_id,
            (group_id   or '')[:30],
            (group_name or '')[:200],
            (tipo_post  or 'otro')[:30],
            str(fbid_post)[:30] if fbid_post else None,
            fecha,
        )
    )
    conn.commit()
    cursor.close()
    conn.close()


def detectar_tipo_nombre(nombre):
    """
    Detecta si un nombre de autor es empresa o persona.
    Retorna: 'empresa', 'persona', o 'desconocido'
    """
    if not nombre:
        return 'desconocido'

    n = nombre.lower().strip()

    EMPRESA_SIGNALS = [
        'tacos', 'tortas', 'pizza', 'burger', 'taqueria', 'taquería',
        'restaurante', 'restaurant', 'comida', 'cocina', 'fonda',
        'servicio', 'servicios', 'taller', 'grupo ', 'empresa',
        'comercial', 'distribuidora', 'ferreteria', 'ferrería',
        'salon ', 'salón ', 'estetica', 'estética', 'clinica', 'clínica',
        'farmacia', 'veterinaria', 'inmobiliaria', 'constructora',
        'decoracion', 'decoración', 'tienda', 'boutique', 'spa',
        'gym', 'gimnasio', 'academia', 'escuela', 'instituto',
        'consultorio', 'laboratorio', 'transporte', 'mudanzas',
        'papeleria', 'papelería', 'libreria', 'panaderia', 'reposteria',
        'lavanderia', 'plomero', 'electricista', 'carpintero',
        'herreria', 'herería', 'pintura', 's.a.', 'sas', 'sa de cv',
        'mx ', 'mex ', 'merida', 'mérida', 'yucatan', 'yucatán',
        'cargo', 'express', 'delivery', 'shop', 'store', 'market',
        'studio', 'studios', 'design', 'digital', 'tech', 'soluciones',
        'eventos', 'producciones', 'agencia', 'marketing', 'publicidad',
        'seguros', 'credito', 'crédito', 'prestamos', 'préstamos',
        'inmuebles', 'bienes raices', 'bienes raíces', 'renta ',
        'fotografia', 'fotografía', 'foto ', 'masajes', 'belleza',
        'uñas', 'cabello', 'cosmetica', 'cosmética',
    ]

    APELLIDOS_MX = [
        'garcia', 'garcía', 'martinez', 'martínez', 'lopez', 'lópez',
        'gonzalez', 'gonzález', 'rodriguez', 'rodríguez', 'hernandez',
        'hernández', 'perez', 'pérez', 'sanchez', 'sánchez', 'ramirez',
        'ramírez', 'flores', 'morales', 'jimenez', 'jiménez', 'diaz',
        'díaz', 'reyes', 'vargas', 'cruz', 'torres', 'gutierrez',
        'gutiérrez', 'ortiz', 'chavez', 'chávez', 'ramos', 'ruiz',
        'acosta', 'medina', 'aguilar', 'castro', 'mendoza', 'silva',
        # Yucatecos
        'caamal', 'dzul', 'pech', 'poot', 'may', 'uc', 'balam',
        'canul', 'chim', 'tun', 'cocom', 'pool', 'canche', 'cauich',
        'cetz', 'chan', 'che', 'chi', 'chuc', 'cutz', 'dzib', 'dzul',
        'euan', 'haas', 'keb', 'ku', 'mex', 'miss', 'nah', 'ox',
        'pat', 'puc', 'push', 'take', 'tamay', 'tzuc', 'ucan', 'xool',
        'yam', 'yeh',
    ]

    # Señal clara de empresa
    if any(s in n for s in EMPRESA_SIGNALS):
        return 'empresa'

    # Posible nombre de persona: 2-3 palabras con apellido conocido
    palabras = n.split()
    if 2 <= len(palabras) <= 3:
        if any(ap in palabras for ap in APELLIDOS_MX):
            return 'persona'

    return 'desconocido'


def calcular_ranking_score(autor_db_id):
    """
    Calcula el ranking_score de un autor basado en su actividad.
    Fórmula: (grupos distintos × 10) + (total posts) + (tiene teléfono × 5)
             - (días inactivo / 7)  + (es_cliente × 50)
    """
    if not autor_db_id:
        return 0

    conn   = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            COUNT(DISTINCT aa.group_id)                      AS grupos,
            COUNT(aa.id)                                     AS total_posts,
            DATEDIFF(NOW(), MAX(aa.fecha))                   AS dias_inactivo,
            (SELECT COUNT(*) FROM autor_telefonos
             WHERE autor_id = %s LIMIT 1)                   AS tiene_tel,
            a.es_cliente,
            COALESCE(SUM(n.repeticiones), 0)                AS total_repeticiones
        FROM autores a
        LEFT JOIN autor_actividad aa ON aa.autor_id = a.id
        LEFT JOIN negocios n ON n.autor_id = a.id AND n.repeticiones > 1
        WHERE a.id = %s
        GROUP BY a.id
    """, (autor_db_id, autor_db_id))

    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if not row:
        return 0

    grupos, total_posts, dias_inactivo, tiene_tel, es_cliente, total_repeticiones = row
    dias_inactivo = dias_inactivo or 0

    score = (
        (grupos or 0) * 10
        + (total_posts or 0)
        + (5 if tiene_tel else 0)
        - int(dias_inactivo / 7)
        + (50 if es_cliente else 0)
        + int(total_repeticiones or 0)   # repeticiones suman directamente al score
    )
    return max(score, 0)


def actualizar_ranking_autor(autor_db_id):
    """
    Recalcula el score y actualiza badge automático.
    Badges: NULL → 'destacado' (≥20) → 'premium' (si es_cliente)
    """
    if not autor_db_id:
        return

    score = calcular_ranking_score(autor_db_id)

    conn   = get_conn()
    cursor = conn.cursor()

    # Badge automático por score (no sobreescribe 'verificado' ni 'premium' manual)
    cursor.execute(
        "SELECT badge, es_cliente FROM autores WHERE id = %s LIMIT 1",
        (autor_db_id,)
    )
    row = cursor.fetchone()
    badge_actual = row[0] if row else None
    es_cliente   = row[1] if row else 0

    # Solo asignar badge automático si no tiene uno manual/pagado
    nuevo_badge = badge_actual
    if badge_actual not in ('verificado', 'premium'):
        if es_cliente:
            nuevo_badge = 'premium'
        elif score >= 20:
            nuevo_badge = 'destacado'
        else:
            nuevo_badge = None

    cursor.execute(
        """
        UPDATE autores
        SET ranking_score   = %s,
            ranking_updated = CURDATE(),
            badge           = %s
        WHERE id = %s
        """,
        (score, nuevo_badge, autor_db_id)
    )
    conn.commit()
    cursor.close()
    conn.close()


def upsert_autor_completo(autor_id_fb, nombre, autor_url=None):
    """
    Versión mejorada de upsert_autor que además:
    - Detecta tipo_nombre (empresa/persona) y tipo_perfil
    - Guarda autor_url para páginas FB sin ID numérico
    - Recalcula ranking y badge
    Retorna (autor_db_id, es_empresa) donde es_empresa es True/False.
    """
    if not autor_id_fb:
        return None, False

    conn   = get_conn()
    cursor = conn.cursor()

    tipo_nombre = detectar_tipo_nombre(nombre)
    tipo_perfil = tipo_nombre if tipo_nombre in ('empresa', 'persona') else 'desconocido'

    cursor.execute(
        "SELECT id, nombre, tipo_nombre, tipo_perfil FROM autores WHERE autor_id_fb = %s LIMIT 1",
        (str(autor_id_fb),)
    )
    row = cursor.fetchone()

    if row:
        autor_db_id    = row[0]
        nombre_actual  = row[1] or ''
        tipo_actual    = row[2] or 'desconocido'
        perfil_actual  = row[3] or 'desconocido'
        nombre_nuevo   = (nombre or '').strip()[:200]

        updates = []
        params  = []
        if nombre_nuevo and nombre_nuevo != nombre_actual:
            updates.append("nombre = %s")
            params.append(nombre_nuevo)
            tipo_nombre = detectar_tipo_nombre(nombre_nuevo)
            tipo_perfil = tipo_nombre if tipo_nombre in ('empresa', 'persona') else 'desconocido'

        if tipo_actual == 'desconocido' and tipo_nombre != 'desconocido':
            updates.append("tipo_nombre = %s")
            params.append(tipo_nombre)

        if perfil_actual == 'desconocido' and tipo_perfil != 'desconocido':
            updates.append("tipo_perfil = %s")
            params.append(tipo_perfil)

        if autor_url and _column_exists("autores", "autor_url"):
            updates.append("autor_url = %s")
            params.append(autor_url[:200])

        if updates:
            params.append(autor_db_id)
            cursor.execute(
                f"UPDATE autores SET {', '.join(updates)} WHERE id = %s",
                tuple(params)
            )
            conn.commit()

        tipo_perfil = tipo_perfil if tipo_perfil != 'desconocido' else perfil_actual
        tipo_nombre = tipo_nombre if tipo_nombre != 'desconocido' else tipo_actual
    else:
        if _column_exists("autores", "autor_url"):
            cursor.execute(
                """
                INSERT INTO autores (autor_id_fb, nombre, tipo_nombre, tipo_perfil, autor_url)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (str(autor_id_fb), (nombre or '').strip()[:200], tipo_nombre, tipo_perfil,
                 autor_url[:200] if autor_url else None)
            )
        else:
            cursor.execute(
                """
                INSERT INTO autores (autor_id_fb, nombre, tipo_nombre, tipo_perfil)
                VALUES (%s, %s, %s, %s)
                """,
                (str(autor_id_fb), (nombre or '').strip()[:200], tipo_nombre, tipo_perfil)
            )
        conn.commit()
        autor_db_id = cursor.lastrowid

    cursor.close()
    conn.close()

    actualizar_ranking_autor(autor_db_id)

    es_empresa = (tipo_nombre == 'empresa' or tipo_perfil == 'empresa')
    return autor_db_id, es_empresa


# ─── MASCOTAS ────────────────────────────────────────────────

def mascota_ya_existe(fbid_post):
    if not fbid_post:
        return None
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM mascotas WHERE fbid_post = %s LIMIT 1", (str(fbid_post),))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row[0] if row else None


def insertar_mascota(p, colonia_id):
    fbid_post = p.get("fbid_post")
    existing  = mascota_ya_existe(fbid_post)
    if existing:
        return existing, "duplicado"

    imgs    = p.get("imagenes_cloudinary") or []
    img_url = _img_url(imgs[0]) if imgs else None

    conn   = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO mascotas
          (tipo, nombre_mascota, especie, descripcion, colonia_id,
           telefono, imagen_url, url_post, fbid_post,
           autor, autor_id, fecha, fecha_post, fecha_post_dt,
           grupos_origen)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            p.get("tipo_mascota", "perdida"),
            (p.get("nombre_mascota") or "")[:100] or None,
            p.get("especie", "perro"),
            p.get("texto", ""),
            colonia_id,
            p.get("telefono") or None,
            img_url,
            p.get("url_post") or None,
            fbid_post,
            (p.get("autor") or "")[:200],
            p.get("_autor_db_id") or None,
            parsear_fecha_fb(p.get("fecha_post"), p.get("fecha_captura", "")),
            (p.get("fecha_post") or "")[:60] or None,
            parsear_fecha_fb(p.get("fecha_post"), p.get("fecha_captura", "")),
            _grupos_origen_json(p),
        ),
    )
    mascota_id = cursor.lastrowid
    conn.commit()
    cursor.close()
    conn.close()
    return mascota_id, "nuevo"


# ─── PERDIDOS ───────────────────────────────────────────────

def perdido_ya_existe(fbid_post):
    if not fbid_post:
        return None
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM perdidos WHERE fbid_post = %s LIMIT 1", (str(fbid_post),))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row[0] if row else None


def insertar_perdido(p, colonia_id):
    fbid_post = p.get("fbid_post")
    existing  = perdido_ya_existe(fbid_post)
    if existing:
        return existing, "duplicado"

    imgs    = p.get("imagenes_cloudinary") or []
    img_url = _img_url(imgs[0]) if imgs else None

    conn   = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO perdidos
          (estado, categoria_id, objeto, descripcion, ubicacion,
           fecha_evento, recompensa, telefono, imagen_cloudinary,
           url_post, fbid_post, autor, autor_id, colonia_id,
           fecha_captura, fecha_post, fecha_post_dt, grupos_origen)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            p.get("perdido_estado", "perdido"),
            p.get("perdido_categoria_id") or None,
            (p.get("objeto") or p.get("perdido_categoria") or "")[:150] or None,
            p.get("texto_limpio") or p.get("texto") or "",
            (p.get("ubicacion") or "")[:200] or None,
            (p.get("fecha_evento") or "")[:60] or None,
            1 if p.get("perdido_recompensa") else 0,
            p.get("telefono") or None,
            img_url,
            p.get("url_post") or None,
            fbid_post,
            (p.get("autor") or "")[:200],
            p.get("_autor_db_id") or None,
            colonia_id,
            parsear_fecha_fb(p.get("fecha_post"), p.get("fecha_captura", "")),
            (p.get("fecha_post") or "")[:60] or None,
            parsear_fecha_fb(p.get("fecha_post"), p.get("fecha_captura", "")),
            _grupos_origen_json(p),
        ),
    )
    perdido_id = cursor.lastrowid
    conn.commit()
    cursor.close()
    conn.close()
    return perdido_id, "nuevo"


def obtener_potenciales_clientes(limite=50, score_minimo=5):
    """
    Retorna autores tipo 'empresa' con alto ranking_score que aún no son clientes.
    Ordenados por ranking_score desc — los que más postean primero.
    """
    conn   = get_conn()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT
            a.id,
            a.autor_id_fb,
            a.nombre,
            a.tipo_nombre,
            a.tipo_perfil,
            a.ranking_score,
            a.badge,
            a.creado_en,
            COUNT(DISTINCT aa.group_id)  AS grupos_activos,
            COUNT(aa.id)                 AS total_posts,
            MAX(aa.fecha)                AS ultimo_post,
            (SELECT t.telefono FROM autor_telefonos t
             WHERE t.autor_id = a.id LIMIT 1) AS telefono
        FROM autores a
        LEFT JOIN autor_actividad aa ON aa.autor_id = a.id
        WHERE a.es_cliente = 0
          AND a.tipo_nombre = 'empresa'
          AND a.ranking_score >= %s
        GROUP BY a.id
        ORDER BY a.ranking_score DESC
        LIMIT %s
    """, (score_minimo, limite))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


