import cloudinary
import cloudinary.uploader
import os
import re
import logging

logger = logging.getLogger(__name__)

cloudinary.config(
    cloud_name=os.getenv("CLOUDINARY_CLOUD_NAME"),
    api_key=os.getenv("CLOUDINARY_API_KEY"),
    api_secret=os.getenv("CLOUDINARY_API_SECRET")
)

def _public_id_desde_fbid(fbid):
    """
    Genera un public_id determinista en Cloudinary a partir del fbid.
    Si se sube la misma imagen dos veces, Cloudinary devuelve la URL
    existente sin crear un duplicado ni consumir créditos.
    """
    fbid_limpio = re.sub(r'[^a-zA-Z0-9_-]', '_', str(fbid))
    return f"vecinosmerida/fb_{fbid_limpio}"

def subir_imagen(url_temp, fbid=None):
    """
    Sube una imagen a Cloudinary.
    - Si fbid está disponible, usa public_id determinista → no duplica.
    - Si Cloudinary falla, retorna la url_temp original como fallback.
    Retorna: (url_resultado, "cloudinary"|"fallback"|"error", mensaje_error)
    """
    kwargs = {
        "folder": None,           # la carpeta va en el public_id
        "unique_filename": False,
        "overwrite": True,
        "resource_type": "image",
    }

    if fbid:
        kwargs["public_id"] = _public_id_desde_fbid(fbid)

    try:
        res = cloudinary.uploader.upload(url_temp, **kwargs)
        url_cloud = res["secure_url"]
        logger.info(f"Cloudinary OK: {url_cloud}")
        return url_cloud, "cloudinary", None
    except Exception as e:
        err = str(e)
        logger.warning(f"Cloudinary FAIL para fbid={fbid}: {err}")
        # Fallback: devolver la URL temporal de FB para que el HTML sí muestre algo
        if url_temp and url_temp.startswith("http"):
            return url_temp, "fallback", err
        return None, "error", err

def subir_imagenes(imagenes):
    """
    Recibe lista de objetos imagen: {fbid, url_temp, alt}
    o lista de strings URL (compatibilidad hacia atrás).
    Retorna: (lista_resultados, count_ok, count_fail)
    Cada resultado: {"url": ..., "origen": "cloudinary"|"fallback"|"error", "fbid": ...}
    """
    resultados = []
    ok = 0
    fail = 0

    for img in imagenes:
        # Soportar tanto dicts {fbid, url_temp} como strings URL
        if isinstance(img, dict):
            url_temp = img.get("url_temp") or img.get("url", "")
            fbid = img.get("fbid")
        else:
            url_temp = img
            fbid = None

        if not url_temp:
            fail += 1
            resultados.append({"url": None, "origen": "error", "fbid": fbid})
            continue

        url_res, origen, err = subir_imagen(url_temp, fbid=fbid)

        if origen in ("cloudinary", "fallback"):
            ok += 1
            resultados.append({"url": url_res, "origen": origen, "fbid": fbid})
        else:
            fail += 1
            resultados.append({"url": None, "origen": "error", "fbid": fbid})

    return resultados, ok, fail
