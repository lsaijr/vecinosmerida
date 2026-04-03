from utils import paso_1_limpieza, paso_2_clusters
from ia import (
    clasificar_tipo,
    procesar_negocio,
    procesar_noticia,
    procesar_noticia_ligera,
    procesar_alerta,
    debe_usar_gemini,
    debe_usar_noticia_ligera,
)
from db import (
    obtener_categorias_negocios,
    obtener_categorias_noticias,
    obtener_categorias_alertas,
    actualizar_grupo_stats,
    insertar_negocio,
    insertar_noticia,
    insertar_alerta,
)
from cloudinary_service import subir_imagenes
from generar_html import generar_html_resultados

POLITICAS = {
    "negocios": {
        "usar_ia_clasificacion": "solo_ambiguos",
        "usar_ia_categoria_negocio": "fallback",
        "usar_noticia_ligera": True,
        "forzar_noticias": False,
    },
    "mascotas": {
        "usar_ia_clasificacion": "solo_ambiguos",
        "usar_ia_categoria_negocio": False,
        "usar_noticia_ligera": True,
        "forzar_noticias": False,
    },
    "noticias": {
        "usar_ia_clasificacion": "solo_ambiguos",
        "usar_ia_categoria_negocio": "fallback",
        "usar_noticia_ligera": True,
        "forzar_noticias": False,
    },
    "vecinos": {
        "usar_ia_clasificacion": "solo_ambiguos",
        "usar_ia_categoria_negocio": "fallback",
        "usar_noticia_ligera": True,
        "forzar_noticias": False,
    },
    "alertas": {
        "usar_ia_clasificacion": "solo_ambiguos",
        "usar_ia_categoria_negocio": "fallback",
        "usar_noticia_ligera": True,
        "forzar_noticias": False,
    },
}


def _politica_para_grupo(grupo_tipo):
    return POLITICAS.get(grupo_tipo, POLITICAS["vecinos"])


def _detectar_cat_mascota(texto):
    txt = texto.lower()
    if any(w in txt for w in ["perdid", "extravi", "se fue", "busco", "ayuda encontrar"]):
        return 14
    if any(w in txt for w in ["encontr", "hallé", "hallad", "aparecio", "apareció"]):
        return 15
    if any(w in txt for w in ["adopci", "adopta", "regalo", "regala", "busca hogar", "hogar"]):
        return 16
    return 11


def _subir_imagenes_posts(posts):
    imgs_ok = 0
    imgs_fail = 0

    for p in posts:
        imagenes = p.get("imagenes", [])
        if imagenes:
            res_imgs, ok, fail = subir_imagenes(imagenes)
            p["imagenes_cloudinary"] = [r["url"] for r in res_imgs if r.get("url")]
            p["imagenes_origen"] = [r.get("origen") for r in res_imgs if r.get("url")]
            imgs_ok += ok
            imgs_fail += fail
        else:
            p["imagenes_cloudinary"] = []
            p["imagenes_origen"] = []

    return imgs_ok, imgs_fail


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
    politica = _politica_para_grupo(grupo_tipo)

    cats_negocios = obtener_categorias_negocios()
    cats_noticias = obtener_categorias_noticias()
    cats_alertas = obtener_categorias_alertas()

    # ── PASO 1: Limpieza + descarte + pre-clasificación ─────────────
    estado["paso"] = "Limpieza y pre-clasificación"
    estado["progreso"] = 10
    estado["error"] = None

    posts_limpios, descartados = paso_1_limpieza(posts)
    estado["detalles"] = f"{len(posts_limpios)} útiles, {len(descartados)} descartados sin IA"

    # ── PASO 2: Deduplicación ────────────────────────────────────────
    estado["paso"] = "Eliminando duplicados"
    estado["progreso"] = 20

    clusters = paso_2_clusters(posts_limpios)
    unicos = [c[0] for c in clusters]
    duplicados = sum(len(c) - 1 for c in clusters if len(c) > 1)
    estado["detalles"] = f"{len(unicos)} únicos, {duplicados} duplicados eliminados"

    # ── PASO 3: Clasificación híbrida ────────────────────────────────
    estado["paso"] = "Clasificando posts"
    estado["progreso"] = 35

    clasificados = []
    calls_ia_evitadas = 0
    calls_ia_clasificacion = 0

    for p in unicos:
        pre_tipo = p.get("pre_tipo", "ambiguo")
        tipo = None
        err = None

        # Para grupos de noticias, ya no se fuerza todo a noticia.
        if grupo_tipo == "noticias":
            if pre_tipo in {"negocio", "mascota", "alerta", "noticia"}:
                tipo = pre_tipo
                calls_ia_evitadas += 1
            else:
                tipo, err = clasificar_tipo(p["texto_limpio"])
                calls_ia_clasificacion += 1
        else:
            if pre_tipo != "ambiguo":
                tipo = pre_tipo
                calls_ia_evitadas += 1
            elif politica.get("usar_ia_clasificacion") == "solo_ambiguos":
                tipo, err = clasificar_tipo(p["texto_limpio"])
                calls_ia_clasificacion += 1
            else:
                tipo = "ignorar"

        p["tipo_detectado"] = tipo or "ignorar"
        if err:
            p["error_clasificacion"] = err
        clasificados.append(p)

    estado["detalles"] = (
        f"Clasificados: {calls_ia_evitadas} por keywords, "
        f"{calls_ia_clasificacion} por IA"
    )

    # ── PASO 4: Procesar texto / IA solo donde aporta ────────────────
    estado["paso"] = "Procesando contenidos"
    estado["progreso"] = 55

    noticias_ligeras = 0
    total = max(1, len(clasificados))
    aprobados_para_imagenes = []

    for i, p in enumerate(clasificados):
        estado["progreso"] = 55 + int((i / total) * 20)
        tipo = p["tipo_detectado"]

        if tipo == "ignorar":
            resultados["ignorados"].append(p)
            continue

        if tipo == "mascota":
            p["categoria_id"] = _detectar_cat_mascota(p["texto_limpio"])
            p["nombre"] = p.get("autor", "")
            p["descripcion"] = p["texto_limpio"]
            resultados["mascotas"].append(p)
            aprobados_para_imagenes.append(p)
            continue

        if tipo == "alerta":
            proc = procesar_alerta(p, cats_alertas)
            if proc.get("error_ia"):
                proc["_error_visible"] = proc["error_ia"]
            resultados["alertas"].append(proc)
            aprobados_para_imagenes.append(proc)
            continue

        if tipo == "noticia":
            if politica.get("usar_noticia_ligera") and debe_usar_noticia_ligera(p["texto_limpio"]):
                proc = procesar_noticia_ligera(p, cats_noticias)
                noticias_ligeras += 1
            else:
                usar_gemini = debe_usar_gemini(p["texto_limpio"])
                proc = procesar_noticia(p, cats_noticias, usar_gemini=usar_gemini)
            if proc.get("error_ia"):
                proc["_error_visible"] = proc["error_ia"]
            resultados["noticias"].append(proc)
            aprobados_para_imagenes.append(proc)
            continue

        if tipo == "negocio":
            proc = procesar_negocio(p, cats_negocios)
            if proc.get("error_ia"):
                proc["_error_visible"] = proc["error_ia"]
            proc["nombre"] = p.get("autor", "")
            proc["descripcion"] = p["texto_limpio"]
            if not proc.get("telefono"):
                proc["telefono"] = p.get("telefono")
            resultados["negocios"].append(proc)
            aprobados_para_imagenes.append(proc)
            continue

        resultados["ignorados"].append(p)

    # ── PASO 5: Subir imágenes solo de posts aprobados ───────────────
    estado["paso"] = "Subiendo imágenes"
    estado["progreso"] = 78

    imgs_ok, imgs_fail = _subir_imagenes_posts(aprobados_para_imagenes)
    estado["detalles"] = f"Imágenes: {imgs_ok} ok, {imgs_fail} fallidas"

    # ── PASO 6: Generar HTML ─────────────────────────────────────────
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

    # ── PASO 7: Guardar en DB ────────────────────────────────────────
    estado["paso"] = "Guardando en base de datos"
    estado["progreso"] = 95

    colonia_id = config_grupo.get("colonia_ids", [None])[0] if config_grupo.get("colonia_ids") else None
    db_nuevos = 0
    db_duplicados = 0

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
        "clasificaciones_ia": calls_ia_clasificacion,
        "noticias_ligeras": noticias_ligeras,
        "negocios": len(resultados["negocios"]),
        "noticias": len(resultados["noticias"]),
        "alertas": len(resultados["alertas"]),
        "mascotas": len(resultados["mascotas"]),
        "ignorados": len(resultados["ignorados"]),
        "imagenes_ok": imgs_ok,
        "imagenes_fail": imgs_fail,
        "db_nuevos": db_nuevos,
        "db_duplicados": db_duplicados,
        "errores": len(resultados["errores"]),
    }

    return nombre_archivo
