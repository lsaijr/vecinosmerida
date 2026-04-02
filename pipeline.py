from utils import paso_1_limpieza, paso_2_clusters, extraer_telefono
from ia import clasificar_tipo, procesar_negocio, procesar_noticia, procesar_alerta, debe_usar_gemini
from db import (obtener_categorias_negocios, obtener_categorias_noticias,
                obtener_categorias_alertas, actualizar_grupo_stats)
from cloudinary_service import subir_imagenes
from generar_html import generar_html_resultados

def ejecutar_pipeline(posts, meta, config_grupo, estado):
    """
    posts: lista de posts del JSON
    meta: dict con group_id, group_name, fecha_captura, etc.
    config_grupo: dict con tipo, colonia_ids, colonia_nombres
    estado: dict compartido para tracking de progreso
    """
    resultados = {
        "negocios": [],
        "noticias": [],
        "alertas": [],
        "mascotas": [],
        "ignorados": [],
        "errores": []
    }

    grupo_tipo = config_grupo.get("tipo", "vecinos")

    # Cargar categorías una sola vez
    cats_negocios = obtener_categorias_negocios()
    cats_noticias = obtener_categorias_noticias()
    cats_alertas = obtener_categorias_alertas()

    # ── PASO 1: Limpieza ──────────────────────────────────────
    estado["paso"] = "Limpieza de textos"
    estado["progreso"] = 10
    posts_limpios = paso_1_limpieza(posts)

    for p in posts_limpios:
        p["telefono"] = extraer_telefono(p["texto_limpio"])

    # ── PASO 2: Deduplicación ────────────────────────────────
    estado["paso"] = "Eliminando duplicados"
    estado["progreso"] = 20
    clusters = paso_2_clusters(posts_limpios)
    unicos = [c[0] for c in clusters]
    duplicados = sum(len(c) - 1 for c in clusters if len(c) > 1)

    estado["detalles"] = f"{len(unicos)} únicos, {duplicados} duplicados eliminados"

    # ── PASO 3: Clasificación por tipo ───────────────────────
    estado["paso"] = "Clasificando posts"
    estado["progreso"] = 35

    clasificados = []
    for p in unicos:
        tipo, err = clasificar_tipo(p["texto_limpio"])
        p["tipo_detectado"] = tipo
        if err:
            p["error_clasificacion"] = err
        clasificados.append(p)

    # ── PASO 4: Subir imágenes a Cloudinary ──────────────────
    estado["paso"] = "Subiendo imágenes"
    estado["progreso"] = 50

    imgs_ok = 0
    imgs_fail = 0
    for p in clasificados:
        urls_temp = [img["url_temp"] for img in p.get("imagenes", []) if img.get("url_temp")]
        if urls_temp:
            urls_cloud, ok, fail = subir_imagenes(urls_temp)
            p["imagenes_cloudinary"] = urls_cloud
            imgs_ok += ok
            imgs_fail += fail
        else:
            p["imagenes_cloudinary"] = []

    estado["detalles"] = f"Imágenes: {imgs_ok} ok, {imgs_fail} fallidas"

    # ── PASO 5: Procesar con IA ──────────────────────────────
    estado["paso"] = "Procesando con IA"
    estado["progreso"] = 65

    total = len(clasificados)
    for i, p in enumerate(clasificados):
        estado["progreso"] = 65 + int((i / total) * 25)
        tipo = p["tipo_detectado"]

        if tipo == "ignorar":
            resultados["ignorados"].append(p)
            continue

        if tipo == "mascota":
            # Las mascotas van como negocio con categoría mascotas
            p["categoria_id"] = _detectar_cat_mascota(p["texto_limpio"])
            resultados["mascotas"].append(p)
            continue

        if tipo == "alerta":
            proc = procesar_alerta(p, cats_alertas)
            if proc.get("error_ia"):
                proc["_error_visible"] = proc["error_ia"]
            resultados["alertas"].append(proc)
            continue

        if tipo == "noticia" or grupo_tipo == "noticias":
            usar_gemini = debe_usar_gemini(p["texto_limpio"])
            proc = procesar_noticia(p, cats_noticias, usar_gemini=usar_gemini)
            if proc.get("error_ia"):
                proc["_error_visible"] = proc["error_ia"]
            resultados["noticias"].append(proc)
            continue

        if tipo == "negocio":
            proc = procesar_negocio(p, cats_negocios)
            if proc.get("error_ia"):
                proc["_error_visible"] = proc["error_ia"]
            resultados["negocios"].append(proc)
            continue

        # fallback
        resultados["ignorados"].append(p)

    # ── PASO 6: Generar HTML de resultados ───────────────────
    estado["paso"] = "Generando reporte HTML"
    estado["progreso"] = 92

    nombre_archivo = generar_html_resultados(
        resultados=resultados,
        meta=meta,
        config_grupo=config_grupo,
        cats_negocios=cats_negocios,
        cats_noticias=cats_noticias,
        cats_alertas=cats_alertas
    )

    # Actualizar estadísticas del grupo
    actualizar_grupo_stats(meta["group_id"], len(posts))

    estado["paso"] = "Completado"
    estado["progreso"] = 100
    estado["archivo_html"] = nombre_archivo
    estado["resumen"] = {
        "total_entrada": len(posts),
        "procesados": len(unicos),
        "duplicados": duplicados,
        "negocios": len(resultados["negocios"]),
        "noticias": len(resultados["noticias"]),
        "alertas": len(resultados["alertas"]),
        "mascotas": len(resultados["mascotas"]),
        "ignorados": len(resultados["ignorados"]),
        "imagenes_ok": imgs_ok,
        "imagenes_fail": imgs_fail
    }

    return nombre_archivo


def _detectar_cat_mascota(texto):
    """Detecta subcategoría de mascota: perdida, encontrada o adopción."""
    txt = texto.lower()
    if any(w in txt for w in ["perdid", "extravi", "se fue", "busco", "ayuda encontrar"]):
        return 14  # Mascotas - Perdidas
    if any(w in txt for w in ["encontr", "hallé", "hallad", "aparecio", "apareció"]):
        return 15  # Mascotas - Encontradas
    if any(w in txt for w in ["adopci", "adopta", "regalo", "regala", "busca hogar", "hogar"]):
        return 16  # Mascotas - Adopción
    return 11  # Mascotas general
