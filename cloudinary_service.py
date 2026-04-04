import logging
import os
from typing import Dict, List, Tuple

import cloudinary
import cloudinary.uploader

from utils import construir_public_id, generar_alt_imagen, slugify

logger = logging.getLogger(__name__)

ASSET_FOLDER = os.getenv("CLOUDINARY_ASSET_FOLDER", "vecinosmerida")
TAG_BASE = os.getenv("CLOUDINARY_TAG_BASE", "vecinos-merida")


def _cloudinary_configured() -> bool:
    return bool(
        os.getenv("CLOUDINARY_URL")
        or (
            os.getenv("CLOUDINARY_CLOUD_NAME")
            and os.getenv("CLOUDINARY_API_KEY")
            and os.getenv("CLOUDINARY_API_SECRET")
        )
    )


def _configure_cloudinary() -> None:
    if os.getenv("CLOUDINARY_URL"):
        cloudinary.config(secure=True)
    else:
        cloudinary.config(
            cloud_name=os.getenv("CLOUDINARY_CLOUD_NAME"),
            api_key=os.getenv("CLOUDINARY_API_KEY"),
            api_secret=os.getenv("CLOUDINARY_API_SECRET"),
            secure=True,
        )


def _metadata_mapping() -> Dict[str, str]:
    mapping = {
        "post_tipo": os.getenv("CLOUDINARY_MD_POST_TIPO"),
        "subtipo": os.getenv("CLOUDINARY_MD_SUBTIPO"),
        "ciudad": os.getenv("CLOUDINARY_MD_CIUDAD"),
        "estado": os.getenv("CLOUDINARY_MD_ESTADO"),
        "colonia": os.getenv("CLOUDINARY_MD_COLONIA"),
        "fecha_captura": os.getenv("CLOUDINARY_MD_FECHA_CAPTURA"),
        "fbid_post": os.getenv("CLOUDINARY_MD_FBID_POST"),
        "fbid_image": os.getenv("CLOUDINARY_MD_FBID_IMAGE"),
        "headline_seo": os.getenv("CLOUDINARY_MD_HEADLINE_SEO"),
        "alt_status": os.getenv("CLOUDINARY_MD_ALT_STATUS"),
        "tiene_revision_manual": os.getenv("CLOUDINARY_MD_TIENE_REVISION"),
        "apto_noticia": os.getenv("CLOUDINARY_MD_APTO_NOTICIA"),
    }
    return {k: v for k, v in mapping.items() if v}


def _safe_context(post, meta=None, config_grupo=None, img=None) -> Dict[str, str]:
    alt = generar_alt_imagen(post, config_grupo=config_grupo)
    caption = (post.get("titulo") or post.get("nombre") or post.get("autor") or "")[:120]
    source_post_url = post.get("url_post") or ""
    return {
        "alt": alt,
        "caption_corta": caption,
        "source_post_url": source_post_url,
    }


def _build_tags(post, meta=None, config_grupo=None) -> List[str]:
    tipo = (post.get("tipo") or post.get("_tipo_final") or post.get("tipo_detectado") or "general").lower()
    tags = {TAG_BASE, tipo, slugify((meta or {}).get("group_id") or "grupo", 4, 30)}

    ciudad = slugify((meta or {}).get("city") or "merida", 3, 20)
    estado = slugify((meta or {}).get("state") or "yucatan", 3, 20)
    tags.update({ciudad, estado})

    colonia = slugify(
        (config_grupo or {}).get("colonia_nombres", ["general"])[0]
        if (config_grupo or {}).get("colonia_nombres")
        else "general",
        5,
        40,
    )
    if colonia:
        tags.add(colonia)

    categoria_id = post.get("categoria_id")
    if categoria_id:
        tags.add(f"cat-{categoria_id}")

    if tipo == "mascota":
        subtipo = {14: "perdido", 15: "encontrado", 16: "adopcion"}.get(categoria_id, "mascotas")
        tags.add(subtipo)
    elif tipo == "noticia":
        tags.add("seo-ready")
        if post.get("noticia_permitida"):
            tags.add("apto-noticia")

    if post.get("requiere_revision_manual"):
        tags.add("revision-manual")

    return sorted(t for t in tags if t)


def _build_structured_metadata(post, meta=None, config_grupo=None, img=None) -> Dict[str, str]:
    mapping = _metadata_mapping()
    if not mapping:
        return {}

    tipo = (post.get("tipo") or post.get("_tipo_final") or post.get("tipo_detectado") or "general").lower()
    subtipo = ""
    if tipo == "mascota":
        subtipo = {14: "perdido", 15: "encontrado", 16: "adopcion"}.get(post.get("categoria_id"), "general")

    values = {
        "post_tipo": tipo,
        "subtipo": subtipo,
        "ciudad": (meta or {}).get("city") or "Mérida",
        "estado": (meta or {}).get("state") or "Yucatán",
        "colonia": ((config_grupo or {}).get("colonia_nombres") or ["General"])[0],
        "fecha_captura": (meta or {}).get("fecha_captura") or "",
        "fbid_post": str(post.get("fbid_post") or ""),
        "fbid_image": str((img or {}).get("fbid") or ""),
        "headline_seo": (post.get("titulo") or "")[:120],
        "alt_status": "auto",
        "tiene_revision_manual": "true" if post.get("requiere_revision_manual") else "false",
        "apto_noticia": "true" if post.get("noticia_permitida") else "false",
    }
    out = {}
    for internal_name, external_id in mapping.items():
        value = values.get(internal_name)
        if value not in (None, ""):
            out[external_id] = value
    return out


def _context_to_string(context: Dict[str, str]) -> str:
    parts = []
    for k, v in context.items():
        if not v:
            continue
        safe = str(v).replace("|", " ").replace("=", "-")
        parts.append(f"{k}={safe}")
    return "|".join(parts)


def _metadata_to_string(metadata: Dict[str, str]) -> str:
    parts = []
    for k, v in metadata.items():
        if v in (None, ""):
            continue
        safe = str(v).replace("|", " ").replace("=", "-")
        parts.append(f"{k}={safe}")
    return "|".join(parts)


def subir_imagen(url_temp, *, public_id=None, tags=None, context=None, metadata=None):
    """
    Wrapper de subida individual para mantener un punto único de logging/fallback.
    """
    kwargs = {
        "folder": ASSET_FOLDER,
        "unique_filename": False,
        "overwrite": True,
        "use_filename": False,
        "resource_type": "image",
    }
    if public_id:
        kwargs["public_id"] = public_id
    if tags:
        kwargs["tags"] = tags
    if context:
        kwargs["context"] = _context_to_string(context)
    if metadata:
        metadata_str = _metadata_to_string(metadata)
        if metadata_str:
            kwargs["metadata"] = metadata_str

    try:
        res = cloudinary.uploader.upload(url_temp, **kwargs)
        url_cloud = res.get("secure_url") or res.get("url")
        logger.info("Cloudinary OK: %s", url_cloud)
        return res, "cloudinary", None
    except Exception as e:
        err = str(e)
        logger.warning("Cloudinary FAIL public_id=%s: %s", public_id, err)
        if url_temp and str(url_temp).startswith("http"):
            return {"secure_url": url_temp, "url": url_temp, "public_id": public_id}, "fallback", err
        return None, "error", err


def subir_imagenes(post, meta=None, config_grupo=None) -> Tuple[List[Dict], int, int]:
    """
    Sube imágenes a Cloudinary con:
    - public_id SEO-friendly y estable
    - context.alt
    - tags
    - structured metadata opcional

    Si Cloudinary no está configurado, devuelve las URLs originales como fallback seguro.
    """
    imagenes = post.get("imagenes") or []
    if not imagenes:
        return [], 0, 0

    resultados = []
    ok = fail = 0

    if _cloudinary_configured():
        _configure_cloudinary()

    for idx, img in enumerate(imagenes):
        origen = img.get("url_temp") if isinstance(img, dict) else img
        if not origen:
            fail += 1
            resultados.append({"url": None, "origen": "error", "fbid": (img or {}).get("fbid") if isinstance(img, dict) else None})
            continue

        public_id = construir_public_id(post, img if isinstance(img, dict) else {}, meta=meta, config_grupo=config_grupo, idx=idx)
        alt = generar_alt_imagen(post, config_grupo=config_grupo)
        context = _safe_context(post, meta=meta, config_grupo=config_grupo, img=img if isinstance(img, dict) else None)
        context["alt"] = alt
        tags = _build_tags(post, meta=meta, config_grupo=config_grupo)
        metadata = _build_structured_metadata(post, meta=meta, config_grupo=config_grupo, img=img if isinstance(img, dict) else None)

        if not _cloudinary_configured():
            resultados.append({
                "url": origen,
                "origen": "fallback",
                "fbid": img.get("fbid") if isinstance(img, dict) else None,
                "alt": alt,
                "public_id": public_id,
                "tags": tags,
                "fallback": True,
            })
            ok += 1
            continue

        response, origen_res, err = subir_imagen(
            origen,
            public_id=public_id,
            tags=tags,
            context=context,
            metadata=metadata,
        )

        if origen_res in ("cloudinary", "fallback") and response:
            resultados.append({
                "url": response.get("secure_url") or response.get("url"),
                "origen": origen_res,
                "fbid": img.get("fbid") if isinstance(img, dict) else None,
                "alt": alt,
                "public_id": response.get("public_id", public_id),
                "asset_id": response.get("asset_id"),
                "tags": tags,
                "error": err,
            })
            ok += 1
        else:
            resultados.append({
                "url": None,
                "origen": "error",
                "fbid": img.get("fbid") if isinstance(img, dict) else None,
                "alt": alt,
                "public_id": public_id,
                "error": err,
            })
            fail += 1

    return resultados, ok, fail
