from utils import paso_1_limpieza, paso_2_clusters, extraer_telefono
from ia import clasificar_tipo, procesar_negocio, procesar_noticia, procesar_alerta, debe_usar_gemini, limpiar_texto_ia
from db import (obtener_categorias_negocios, obtener_categorias_noticias,
                obtener_categorias_alertas, actualizar_grupo_stats,
                insertar_negocio, insertar_noticia, insertar_alerta)
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
        imagenes = p.get("imagenes", [])
        if imagenes:
            resultados_img, ok, fail = subir_imagenes(imagenes)
            p["imagenes_cloudinary"] = [r["url"] for r in resultados_img if r["url"]]
            p["imagenes_origen"]     = [r["origen"] for r in resultados_img if r["url"]]
            imgs_ok  += ok
            imgs_fail += fail
        else:
            p["imagenes_cloudinary"] = []
            p["imagenes_origen"]     = []

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
            # Limpieza IA del texto (no reescritura)
            p["texto_limpio"] = limpiar_texto_ia(p["texto_limpio"])
            p["categoria_id"] = _detectar_cat_mascota(p["texto_limpio"])
            # nombre = autor directo
            p["nombre"] = p.get("autor", "")
            # descripcion = texto limpio completo
            p["descripcion"] = p["texto_limpio"]
            resultados["mascotas"].append(p)
            continue

        if tipo == "alerta":
            # La IA de alerta ya limpia el texto_alerta internamente
            proc = procesar_alerta(p, cats_alertas)
            if proc.get("error_ia"):
                proc["_error_visible"] = proc["error_ia"]
            # También limpiar el texto_limpio original por si se muestra
            proc["texto_limpio"] = limpiar_texto_ia(proc.get("texto_limpio", ""))
            resultados["alertas"].append(proc)
            continue

        if tipo == "noticia" or grupo_tipo == "noticias":
            # Noticias: reescritura completa con IA (Gemini para largas, Groq 70B para cortas)
            usar_gemini = debe_usar_gemini(p["texto_limpio"])
            proc = procesar_noticia(p, cats_noticias, usar_gemini=usar_gemini)
            if proc.get("error_ia"):
                proc["_error_visible"] = proc["error_ia"]
            resultados["noticias"].append(proc)
            continue

        if tipo == "negocio":
            # IA extrae categoria_id y telefono
            proc = procesar_negocio(p, cats_negocios)
            if proc.get("error_ia"):
                proc["_error_visible"] = proc["error_ia"]
            # nombre = autor directo del JSON (sin IA)
            proc["nombre"] = p.get("autor", "")
            # descripcion = texto limpiado con IA (sin reescribir)
            proc["descripcion"] = limpiar_texto_ia(p["texto_limpio"])
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

    # ── PASO 7: Guardar en DB ────────────────────────────────
    estado["paso"] = "Guardando en base de datos"
    estado["progreso"] = 95

    colonia_id = config_grupo.get("colonia_ids", [None])[0]
    db_nuevos = db_duplicados = 0

    for p in resultados["negocios"] + resultados["mascotas"]:
        try:
            _, estado_db = insertar_negocio(p, colonia_id)
            if estado_db == "nuevo":
                db_nuevos += 1
            else:
                db_duplicados += 1
        except Exception as e:
            resultados["errores"].append({"tipo": "db_negocio", "error": str(e), "autor": p.get("autor")})

    for p in resultados["noticias"]:
        try:
            _, estado_db = insertar_noticia(p, colonia_id)
            if estado_db == "nuevo":
                db_nuevos += 1
            else:
                db_duplicados += 1
        except Exception as e:
            resultados["errores"].append({"tipo": "db_noticia", "error": str(e), "autor": p.get("autor")})

    for p in resultados["alertas"]:
        try:
            _, estado_db = insertar_alerta(p, colonia_id)
            if estado_db == "nuevo":
                db_nuevos += 1
            else:
                db_duplicados += 1
        except Exception as e:
            resultados["errores"].append({"tipo": "db_alerta", "error": str(e), "autor": p.get("autor")})

    estado["detalles"] = f"DB: {db_nuevos} nuevos, {db_duplicados} ya existían"

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
        "imagenes_fail": imgs_fail,
        "db_nuevos": db_nuevos,
        "db_duplicados": db_duplicados
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
