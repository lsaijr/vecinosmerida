"""
HostGator image uploader — drop-in replacement for cloudinary_service.subir_imagenes().

Downloads each image from url_temp, optimizes with Pillow (max 1200px, JPEG 82%),
extracts dominant_color, and uploads via FTP to /public_html/vecinosmerida/img/<tipo>/.
Returns the same (List[Dict], ok_count, fail_count) tuple as cloudinary_service.
"""
import ftplib
import io
import logging
import os
import re
import unicodedata
from typing import Dict, List, Tuple

import requests
from PIL import Image

logger = logging.getLogger(__name__)

FTP_HOST        = os.getenv("FTP_HOST",        "50.116.94.0")
FTP_PORT        = int(os.getenv("FTP_PORT",    "21"))
FTP_USER        = os.getenv("FTP_USER",        "claude@padmabymary.com")
FTP_PASS        = os.getenv("FTP_PASSWORD",    "")
FTP_REMOTE_BASE = os.getenv("FTP_REMOTE_PATH", "/vecinosmerida")
SITE_URL        = os.getenv("SITE_URL",        "https://vecinosmerida.com")
IMG_MAX_SIZE    = 1200
IMG_QUALITY     = 82


def _slugify(txt: str, max_len: int = 80) -> str:
    s = unicodedata.normalize("NFKD", str(txt or ""))
    s = s.encode("ascii", "ignore").decode()
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")[:max_len].rstrip("-")


def _dominant_color(img: Image.Image) -> str:
    thumb = img.convert("RGB").resize((50, 50), Image.LANCZOS)
    pixels = list(thumb.getdata())
    n = len(pixels)
    r = sum(p[0] for p in pixels) // n
    g = sum(p[1] for p in pixels) // n
    b = sum(p[2] for p in pixels) // n
    return f"#{r:02x}{g:02x}{b:02x}"


def _optimize(raw: bytes) -> Tuple[bytes, int, int, str, str]:
    """Returns (jpeg_bytes, width, height, 'jpg', dominant_color_hex)."""
    img = Image.open(io.BytesIO(raw))
    if img.mode not in ("RGB",):
        img = img.convert("RGB")
    dom = _dominant_color(img)
    if max(img.size) > IMG_MAX_SIZE:
        img.thumbnail((IMG_MAX_SIZE, IMG_MAX_SIZE), Image.LANCZOS)
    w, h = img.size
    buf = io.BytesIO()
    img.save(buf, "JPEG", quality=IMG_QUALITY, optimize=True)
    return buf.getvalue(), w, h, "jpg", dom


def _ftp_connect() -> ftplib.FTP:
    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, FTP_PORT, timeout=30)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.set_pasv(True)
    return ftp


def _ftp_mkdirs(ftp: ftplib.FTP, path: str) -> None:
    parts = [p for p in path.split("/") if p]
    cur = ""
    for part in parts:
        cur = f"{cur}/{part}"
        try:
            ftp.mkd(cur)
        except ftplib.error_perm:
            pass  # already exists


def _ftp_store(ftp: ftplib.FTP, data: bytes, remote_path: str) -> None:
    remote_dir = remote_path.rsplit("/", 1)[0]
    _ftp_mkdirs(ftp, remote_dir)
    ftp.storbinary(f"STOR {remote_path}", io.BytesIO(data))


MAX_WORKERS = int(os.getenv("IMG_UPLOAD_WORKERS", "4"))


def _procesar_imagen(idx, img, tipo, fbid_post, total, post, config_grupo):
    """Descarga, optimiza y sube una imagen. Devuelve (resultado_dict, success)."""
    try:
        from utils import generar_alt_imagen as _gen_alt
    except ImportError:
        _gen_alt = None

    url_origen = img.get("url_temp") if isinstance(img, dict) else (img if isinstance(img, str) else None)
    fbid_img   = img.get("fbid") if isinstance(img, dict) else None
    alt        = _gen_alt(post, config_grupo=config_grupo, idx=idx, total=total) if _gen_alt else ""

    if not url_origen:
        return {"url": None, "origen": "error", "fbid": fbid_img, "alt": alt, "error": "sin_url"}, False

    slug       = _slugify(f"{tipo}-{fbid_post}-{fbid_img or idx}")
    filename   = f"{slug}.jpg"
    remote     = f"{FTP_REMOTE_BASE}/img/{tipo}/{filename}"
    public_url = f"{SITE_URL}/img/{tipo}/{filename}"

    # Download — timeout corto para fallar rápido en URLs expiradas
    try:
        resp = requests.get(url_origen, timeout=8)
        resp.raise_for_status()
        raw = resp.content
    except Exception as e:
        logger.warning("Download FAIL %s: %s", url_origen, e)
        return {"url": None, "origen": "error", "fbid": fbid_img, "alt": alt, "error": f"dl:{e}"}, False

    # Optimize
    try:
        opt, w, h, fmt, dom = _optimize(raw)
    except Exception as e:
        logger.warning("Optimize FAIL: %s", e)
        return {"url": None, "origen": "error", "fbid": fbid_img, "alt": alt, "error": f"opt:{e}"}, False

    # Upload — cada thread abre su propia conexión FTP
    try:
        ftp = _ftp_connect()
        _ftp_store(ftp, opt, remote)
        try:
            ftp.quit()
        except Exception:
            pass
    except Exception as e:
        logger.warning("FTP FAIL %s: %s", remote, e)
        return {"url": None, "origen": "error", "fbid": fbid_img, "alt": alt, "error": f"ftp:{e}"}, False

    return {
        "url":            public_url,
        "origen":         "hostgator",
        "fbid":           fbid_img,
        "alt":            alt,
        "slug":           slug,
        "width":          w,
        "height":         h,
        "file_size":      len(opt),
        "format":         fmt,
        "dominant_color": dom,
    }, True


def subir_imagenes(post, meta=None, config_grupo=None) -> Tuple[List[Dict], int, int]:
    from concurrent.futures import ThreadPoolExecutor, as_completed

    imagenes = post.get("imagenes") or []
    if not imagenes:
        return [], 0, 0

    tipo      = (post.get("tipo") or post.get("_tipo_final") or "general").lower()
    fbid_post = post.get("fbid_post") or "x"
    total     = len(imagenes)
    workers   = min(MAX_WORKERS, total)

    resultados_map: dict = {}

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_procesar_imagen, idx, img, tipo, fbid_post, total, post, config_grupo): idx
            for idx, img in enumerate(imagenes)
        }
        for future in as_completed(futures):
            idx = futures[future]
            try:
                resultado, success = future.result()
            except Exception as e:
                img = imagenes[idx]
                fbid_img = img.get("fbid") if isinstance(img, dict) else None
                resultado = {"url": None, "origen": "error", "fbid": fbid_img, "error": str(e)}
                success = False
            resultados_map[idx] = (resultado, success)

    resultados: List[Dict] = []
    ok = fail = 0
    for idx in range(total):
        resultado, success = resultados_map[idx]
        resultados.append(resultado)
        if success:
            ok += 1
        else:
            fail += 1

    return resultados, ok, fail


