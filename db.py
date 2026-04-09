import os
from functools import lru_cache

import mysql.connector


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


def actualizar_grupo_stats(group_id, total_posts):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE grupos_facebook SET total_posts = total_posts + %s, ultimo_proceso = CURDATE() WHERE group_id = %s",
        (total_posts, group_id),
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
           facebook, colonia_id, fuente_autor, fecha_captura,
           activo, fbid_post)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,1,%s)
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
            p.get("fecha_captura"),
            fbid_post,
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
          (titulo, texto, categoria_id, colonia_id, autor,
           imagen_cloudinary, url_post, fbid_post, fecha_captura)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            (p.get("titulo") or "")[:200],
            p.get("texto", ""),
            p.get("categoria_id"),
            colonia_id,
            p.get("autor", "")[:200],
            imagen_principal,
            p.get("url_post") or None,
            fbid_post,
            p.get("fecha_captura"),
        ),
    )
    nid = cursor.lastrowid
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
          (texto_alerta, categoria_id, colonia_id, direccion_aprox,
           autor, imagen_cloudinary, url_post, fbid_post, fecha_captura)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            p.get("texto_alerta", "")[:500],
            p.get("categoria_id"),
            colonia_id,
            p.get("direccion_aprox"),
            p.get("autor", "")[:200],
            imagen_principal,
            p.get("url_post") or None,
            fbid_post,
            p.get("fecha_captura"),
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


def _get_area_id(area_nombre):
    """Retorna el id de empleo_areas por nombre, o None."""
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
    slug = SLUG_MAP.get(area_nombre or 'General', 'general')
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

    area_id   = _get_area_id(p.get("area"))
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
           autor, colonia_id, fbid_post, fecha_captura)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
            colonia_id,
            fbid_post,
            p.get("fecha_captura"),
        ),
    )
    empleo_id = cursor.lastrowid
    conn.commit()
    cursor.close()
    conn.close()
    return empleo_id, "nuevo"
