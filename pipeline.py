from db import (
    actualizar_grupo_stats,
    insertar_alerta,
    insertar_negocio,
    insertar_noticia,
    obtener_categorias_alertas,
    obtener_categorias_negocios,
    obtener_categorias_noticias,
)
from generar_html import generar_html_resultados
from ia import clasificar_tipo, debe_usar_gemini, procesar_alerta, procesar_negocio, procesar_noticia
from utils import (
    NEWS_MIN_WORDS,
    generar_titulo_alerta,
    generar_titulo_mascota,
    generar_titulo_negocio,
    generar_titulo_noticia_fallback,
    paso_1_limpieza,
    paso_2_clusters,
    puede_ser_noticia_desde_json,
)
from cloudinary_service import subir_imagenes


POLITICAS = {
    "negocios": {
        "ia_clasificacion": "solo_ambiguos",
        "ia_categoria_negocio": "fallback",
        "noticia_ligera": False,
    },
    "mascotas": {
        "ia_clasificacion": "solo_ambiguos",
        "ia_categoria_negocio": False,
        "noticia_ligera": False,
    },
    "noticias": {
        "ia_clasificacion": "solo_ambiguos",
        "ia_categoria_negocio": False,
        "noticia_ligera": True,
    },
    "vecinos": {
        "ia_clasificacion": "solo_ambiguos",
        "ia_categoria_negocio": "fallback",
        "noticia_ligera": True,
    },
}


def ejecutar_pipeline(posts, meta, config_grupo, estado):
    resultados = {
        "negocios": [],
        "noticias": [],
        "alertas": [],
        "mascotas": [],
        "ignorados": [],
        "errores": [],
    }

    grupo_tipo = config_grupo.get("tipo", "vecinos")
    politicas = POLITICAS.get(grupo_tipo, POLITICAS["vecinos"])

    cats_negocios = obtener_categorias_negocios()
    cats_noticias = obtener_categorias_noticias()
    cats_alertas = obtener_categorias_alertas()

    cats_neg_map = {str(c["id"]): c for c in cats_negocios}
    cats_alert_map = {str(c["id"]): c for c in cats_alertas}

    # ── PASO 1: Limpieza y preclasificación ───────────────────────
    estado["paso"] = "Limpieza y pre-clasificación"
    estado["progreso"] = 10
    posts_limpios, descartados = paso_1_limpieza(posts, grupo_tipo=grupo_tipo)
    estado["detalles"] = f"{len(posts_limpios)} útiles, {len(descartados)} descartados sin IA"

    # ── PASO 2: Deduplicación ─────────────────────────────────────
    estado["paso"] = "Eliminando duplicados"
    estado["progreso"] = 20
    clusters = paso_2_clusters(posts_limpios)
    unicos = [c[0] for c in clusters]
    duplicados = sum(len(c) - 1 for c in clusters if len(c) > 1)
    estado["detalles"] = f"{len(unicos)} únicos, {duplicados} duplicados eliminados"

    # ── PASO 3: Clasificación ─────────────────────────────────────
    estado["paso"] = "Clasificando posts"
    estado["progreso"] = 35
    clasificados = []
    calls_ia_evitadas = 0

    for p in unicos:
        pre_tipo = p.get("pre_tipo", "ambiguo")
        p["noticia_permitida"] = puede_ser_noticia_desde_json(p.get("texto", ""))

        if pre_tipo != "ambiguo":
            p["tipo_detectado"] = pre_tipo
            calls_ia_evitadas += 1
            clasificados.append(p)
            continue

        tipo, err = clasificar_tipo(p.get("texto_limpio", ""))
        if tipo == "noticia" and not p["noticia_permitida"]:
            tipo = "negocio" if pre_tipo == "ambiguo" else pre_tipo
        p["tipo_detectado"] = tipo
        if err:
            p["error_clasificacion"] = err
        clasificados.append(p)

    estado["detalles"] = f"Clasificados: {calls_ia_evitadas} por reglas, {len(unicos)-calls_ia_evitadas} por IA"

    # ── PASO 4: Procesamiento ─────────────────────────────────────
    estado["paso"] = "Procesando posts"
    estado["progreso"] = 58

    total = max(len(clasificados), 1)
    aprobados = []
    for i, p in enumerate(clasificados):
        estado["progreso"] = 58 + int((i / total) * 22)
        tipo = p.get("tipo_detectado") or "ignorar"

        if tipo == "ignorar":
            resultados["ignorados"].append(p)
            continue

        if tipo == "noticia" and not p.get("noticia_permitida"):
            tipo = "negocio"

        if tipo == "mascota":
            p["tipo"] = "mascota"
            p["categoria_id"] = _detectar_cat_mascota(p.get("texto_limpio", ""))
            p["nombre"] = p.get("autor", "")
            p["descripcion"] = p.get("texto_limpio", "")
            p["titulo"] = generar_titulo_mascota(p, p.get("categoria_id", 11))
            resultados["mascotas"].append(p)
            aprobados.append(p)
            continue

        if tipo == "alerta":
            proc = procesar_alerta(p, cats_alertas)
            if proc.get("error_ia"):
                proc["_error_visible"] = proc["error_ia"]
            cat_nombre = cats_alert_map.get(str(proc.get("categoria_id")), {}).get("nombre", "Alerta")
            proc["titulo"] = generar_titulo_alerta(proc, cat_nombre=cat_nombre)
            resultados["alertas"].append(proc)
            aprobados.append(proc)
            continue

        if tipo == "noticia":
            palabras = len((p.get("texto_limpio") or "").split())
            modo = "ligera" if politicas.get("noticia_ligera") and palabras < 130 else "completa"
            usar_gemini = debe_usar_gemini(p.get("texto_limpio", ""))
            proc = procesar_noticia(p, cats_noticias, usar_gemini=usar_gemini, modo=modo)
            if proc.get("error_ia"):
                proc["_error_visible"] = proc["error_ia"]
            proc["titulo"] = proc.get("titulo") or generar_titulo_noticia_fallback(proc)
            resultados["noticias"].append(proc)
            aprobados.append(proc)
            continue

        # negocio (incluye downgrades desde noticia no permitida)
        proc = procesar_negocio(p, cats_negocios)
        if proc.get("error_ia"):
            proc["_error_visible"] = proc["error_ia"]
        proc["tipo"] = "negocio"
        proc["nombre"] = p.get("autor", "")
        proc["descripcion"] = p.get("texto_limpio", "")
        if not proc.get("telefono"):
            proc["telefono"] = p.get("telefono")
        categoria_nombre = cats_neg_map.get(str(proc.get("categoria_id")), {}).get("nombre", "General")
        proc["titulo"] = generar_titulo_negocio(proc, categoria_nombre=categoria_nombre)
        resultados["negocios"].append(proc)
        aprobados.append(proc)

    # ── PASO 5: Subir imágenes aprobadas a Cloudinary ──────────────
    estado["paso"] = "Subiendo imágenes"
    estado["progreso"] = 82
    imgs_ok = imgs_fail = 0

    for p in aprobados:
        imagenes = p.get("imagenes", []) or []
        if imagenes:
            res_imgs, ok, fail = subir_imagenes(p, meta=meta, config_grupo=config_grupo)
            p["imagenes_cloudinary"] = res_imgs
            imgs_ok += ok
            imgs_fail += fail
        else:
            p["imagenes_cloudinary"] = []

    estado["detalles"] = f"Imágenes: {imgs_ok} ok, {imgs_fail} fallidas"

    # ── PASO 6: Generar HTML ───────────────────────────────────────
    estado["paso"] = "Generando reporte HTML"
    estado["progreso"] = 92
    nombre_archivo = generar_html_resultados(
        resultados=resultados,
        meta=meta,
        config_grupo=config_grupo,
        cats_negocios=cats_negocios,
        cats_noticias=cats_noticias,
        cats_alertas=cats_alertas,
    )

    # ── PASO 7: Guardar en DB ──────────────────────────────────────
    estado["paso"] = "Guardando en base de datos"
    estado["progreso"] = 96

    colonia_id = config_grupo.get("colonia_ids", [None])[0]
    db_nuevos = db_duplicados = 0

    for p in resultados["negocios"] + resultados["mascotas"]:
        try:
            _, st = insertar_negocio(p, colonia_id)
            db_nuevos += 1 if st == "nuevo" else 0
            db_duplicados += 1 if st == "duplicado" else 0
        except Exception as e:
            resultados["errores"].append({"tipo": "db_negocio", "error": str(e), "autor": p.get("autor")})

    for p in resultados["noticias"]:
        try:
            _, st = insertar_noticia(p, colonia_id)
            db_nuevos += 1 if st == "nuevo" else 0
            db_duplicados += 1 if st == "duplicado" else 0
        except Exception as e:
            resultados["errores"].append({"tipo": "db_noticia", "error": str(e), "autor": p.get("autor")})

    for p in resultados["alertas"]:
        try:
            _, st = insertar_alerta(p, colonia_id)
            db_nuevos += 1 if st == "nuevo" else 0
            db_duplicados += 1 if st == "duplicado" else 0
        except Exception as e:
            resultados["errores"].append({"tipo": "db_alerta", "error": str(e), "autor": p.get("autor")})

    actualizar_grupo_stats(meta.get("group_id"), len(posts))

    estado["paso"] = "Completado"
    estado["progreso"] = 100
    estado["archivo_html"] = nombre_archivo
    estado["resumen"] = {
        "total_entrada": len(posts),
        "descartados_sin_ia": len(descartados),
        "procesados": len(unicos),
        "duplicados": duplicados,
        "keywords_sin_ia": calls_ia_evitadas,
        "negocios": len(resultados["negocios"]),
        "noticias": len(resultados["noticias"]),
        "alertas": len(resultados["alertas"]),
        "mascotas": len(resultados["mascotas"]),
        "ignorados": len(resultados["ignorados"]),
        "imagenes_ok": imgs_ok,
        "imagenes_fail": imgs_fail,
        "db_nuevos": db_nuevos,
        "db_duplicados": db_duplicados,
    }
    return nombre_archivo



def _detectar_cat_mascota(texto):
    txt = (texto or '').lower()

    if any(w in txt for w in [
        "se escapó", "se escapo", "se me escapó", "se me escapo", "perdid", "extravi",
        "si la ves", "si lo ves", "responde al nombre", "avísame", "avisame", "se salió", "se salio",
        "no la persigas", "no lo persigas", "ayuda encontrar"
    ]):
        return 14

    if any(w in txt for w in [
        "encontr", "hallé", "halle", "hallad", "aparecio", "apareció", "anda por", "anda en",
        "alguien lo reconoce", "alguien la reconoce", "trae collar", "lo encontré", "lo encontre",
        "la encontré", "la encontre"
    ]):
        return 15

    if any(w in txt for w in [
        "adopci", "adopta", "regalo", "regala", "busca hogar", "busca familia", "hogar",
        "la entregamos", "dar en adopción", "dar en adopcion", "necesita hogar",
        "esperando a alguien", "denle la oportunidad"
    ]):
        return 16

    return 11
