from db import (
    actualizar_grupo_stats,
    insertar_alerta,
    insertar_empleo,
    insertar_mascota,
    insertar_negocio,
    insertar_noticia,
    insertar_perdido,
    obtener_categorias_alertas,
    obtener_categorias_negocios,
    obtener_categorias_noticias,
    upsert_autor,
    upsert_autor_completo,
    registrar_actividad,
    actualizar_ranking_autor,
)
from generar_html import generar_html_resultados
from ia import clasificar_tipo, debe_usar_gemini, generar_titulo_negocio_ia, procesar_alerta, procesar_negocio, procesar_noticia, procesar_empleo, get_resumen_costo, reset_contadores, _titulo_pobre
from utils import (
    generar_alt_imagen,
    NEWS_MIN_WORDS,
    generar_titulo_alerta,
    generar_titulo_mascota,
    generar_titulo_negocio,
    generar_titulo_noticia_fallback,
    generar_titulo_perdido,
    generar_titulo_empleo,
    limpiar_titulo,
    paso_1_limpieza,
    paso_2_clusters,
    puede_ser_noticia_desde_json,
    get_config_grupo,
    tipo_permitido_en_grupo,
    requiere_imagen_en_grupo,
    es_noticia_geograficamente_valida,
    clasificar_tipo_empleo,
    OFERTA_KW,
    BUSQUEDA_KW,
)
from cloudinary_service import subir_imagenes
from collections import defaultdict


def _tiene_url_imagen_valida(post):
    """Verifica si el post tiene al menos una URL de imagen válida ANTES de llamar a la IA."""
    imgs = post.get("imagenes") or []
    return any(
        isinstance(img, dict) and (img.get("url_temp") or "").startswith("http")
        for img in imgs
    )


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
    "empleo": {
        "ia_clasificacion": "solo_ambiguos",
        "ia_categoria_negocio": False,
        "noticia_ligera": False,
    },
    "perdidos": {
        "ia_clasificacion": "solo_ambiguos",
        "ia_categoria_negocio": False,
        "noticia_ligera": False,
    },
}


def _set_estado(estado, paso=None, progreso=None, detalles=None, actividad=None, add_history=False):
    if paso is not None:
        estado["paso"] = paso
    if progreso is not None:
        estado["progreso"] = progreso
    if detalles is not None:
        estado["detalles"] = detalles
    if actividad is not None:
        estado["actividad"] = actividad
        if add_history:
            hist = estado.setdefault("historial", [])
            if not hist or hist[-1] != actividad:
                hist.append(actividad)
                if len(hist) > 20:
                    del hist[:-20]


def ejecutar_pipeline(posts, meta, config_grupo, estado):
    resultados = {
        "negocios": [],
        "noticias": [],
        "alertas": [],
        "mascotas": [],
        "empleos": [],
        "perdidos": [],
        "ignorados": [],
        "errores": [],
    }

    grupo_tipo = config_grupo.get("tipo", "vecinos")
    politicas  = POLITICAS.get(grupo_tipo, POLITICAS["vecinos"])
    cfg_grupo  = get_config_grupo(grupo_tipo)

    # Reiniciar contadores de tokens para esta corrida
    reset_contadores()

    cats_negocios = obtener_categorias_negocios()
    cats_noticias = obtener_categorias_noticias()
    cats_alertas = obtener_categorias_alertas()

    cats_neg_map = {str(c["id"]): c for c in cats_negocios}
    cats_alert_map = {str(c["id"]): c for c in cats_alertas}

    # ── PASO 1: Limpieza y preclasificación ───────────────────────
    _set_estado(estado, paso="Limpieza y pre-clasificación", progreso=10, actividad="Limpiando textos y preparando posts", add_history=True)
    posts_limpios, descartados = paso_1_limpieza(posts, grupo_tipo=grupo_tipo)
    _set_estado(estado, detalles=f"{len(posts_limpios)} útiles, {len(descartados)} descartados sin IA", actividad="Limpieza terminada", add_history=True)

    # ── PASO 2: Deduplicación ─────────────────────────────────────
    _set_estado(estado, paso="Eliminando duplicados", progreso=20, actividad="Comparando textos para eliminar duplicados", add_history=True)
    clusters = paso_2_clusters(posts_limpios)
    unicos = [c[0] for c in clusters]
    duplicados = sum(len(c) - 1 for c in clusters if len(c) > 1)
    _set_estado(estado, detalles=f"{len(unicos)} únicos, {duplicados} duplicados eliminados", actividad="Deduplicación terminada", add_history=True)

    # ── PASO 3: Clasificación ─────────────────────────────────────
    _set_estado(estado, paso="Clasificando posts", progreso=35, actividad="Clasificando posts por reglas e IA", add_history=True)
    clasificados = []
    calls_ia_evitadas = 0

    for idx, p in enumerate(unicos, 1):
        if idx == 1 or idx % 8 == 0:
            _set_estado(estado, actividad=f"Clasificando post {idx}/{len(unicos)}")
        pre_tipo = p.get("pre_tipo", "ambiguo")
        p["noticia_permitida"] = puede_ser_noticia_desde_json(p.get("texto", ""))

        # ── Detección directa de empleo (sin Groq) ───────────────
        if grupo_tipo == "empleo":
            tipo_emp = clasificar_tipo_empleo(p.get("texto_limpio", ""))

            # Si tiene imagen + teléfono y texto corto → verificar con IA
            if p.get("_empleo_verificar_ia") and not tipo_emp:
                tipo_ia, _ = clasificar_tipo(
                    p.get("texto_limpio", ""),
                    grupo_tipo="empleo",
                    grupo_nombre=meta.get("group_name", ""),
                )
                # Si IA dice que NO es empleo → ignorar
                if tipo_ia not in ("empleo", "negocio", "ambiguo"):
                    resultados["ignorados"].append(p)
                    continue

            # Si no matchea keywords específicas, clasificar como oferta por defecto
            # (en un grupo de empleo todo post válido es empleo)
            p["tipo_detectado"] = "empleo"
            p["tipo_empleo"]    = tipo_emp or "oferta"
            calls_ia_evitadas  += 1
            clasificados.append(p)
            continue

        if pre_tipo != "ambiguo":
            p["tipo_detectado"] = pre_tipo
            calls_ia_evitadas += 1
            clasificados.append(p)
            continue

        tipo, err = clasificar_tipo(
            p.get("texto_limpio", ""),
            grupo_tipo=grupo_tipo,
            grupo_nombre=meta.get("group_name", ""),
        )
        if tipo == "noticia" and not p["noticia_permitida"]:
            tipo = "negocio" if pre_tipo == "ambiguo" else pre_tipo
        p["tipo_detectado"] = tipo
        if err:
            p["error_clasificacion"] = err
        clasificados.append(p)

    _set_estado(estado, detalles=f"Clasificados: {calls_ia_evitadas} por reglas, {len(unicos)-calls_ia_evitadas} por IA", actividad="Clasificación terminada", add_history=True)

    # ── PASO 4: Procesamiento ─────────────────────────────────────
    _set_estado(estado, paso="Procesando posts", progreso=58, actividad="Procesando contenido por tipo de post", add_history=True)

    total = max(len(clasificados), 1)
    aprobados = []
    descartados_sin_imagen = 0
    for i, p in enumerate(clasificados):
        estado["progreso"] = 58 + int((i / total) * 22)
        autor = (p.get("autor") or "sin autor")[:40]
        tipo_prev = p.get("tipo_detectado") or "pendiente"
        if i == 0 or i % 6 == 0:
            _set_estado(estado, actividad=f"Procesando post {i+1}/{len(clasificados)} · {tipo_prev} · {autor}")
        tipo = p.get("tipo_detectado") or "ignorar"

        if tipo == "ignorar":
            resultados["ignorados"].append(p)
            continue

        # ── Filtro por tipo permitido en este grupo ───────────────
        if not tipo_permitido_en_grupo(tipo, grupo_tipo):
            p["_descartado"] = f"tipo_{tipo}_no_permitido_en_{grupo_tipo}"
            resultados["ignorados"].append(p)
            continue

        if tipo == "noticia" and not p.get("noticia_permitida"):
            tipo = "negocio"

        # ── Filtro geográfico para noticias en grupos de noticias ──
        if tipo == "noticia" and cfg_grupo.get("filtrar_geo_externa"):
            if not es_noticia_geograficamente_valida(p.get("texto_limpio", "")):
                p["_descartado"] = "noticia_geo_externa"
                resultados["ignorados"].append(p)
                continue

        tiene_imagen_origen = bool(p.get("imagenes") or [])
        if requiere_imagen_en_grupo(tipo, grupo_tipo) and not tiene_imagen_origen:
            p["_descartado"] = "sin_imagen"
            resultados["ignorados"].append(p)
            descartados_sin_imagen += 1
            continue

        # ── Validar URL real ANTES de gastar tokens de IA ─────────
        if requiere_imagen_en_grupo(tipo, grupo_tipo) and not _tiene_url_imagen_valida(p):
            p["_descartado"] = "sin_url_imagen_valida"
            resultados["ignorados"].append(p)
            descartados_sin_imagen += 1
            continue

        if tipo == "perdido":
            p["tipo"] = "perdido"
            p["titulo"] = generar_titulo_perdido(p)
            p["descripcion"] = p.get("texto_limpio", "")
            resultados["perdidos"].append(p)
            aprobados.append(p)
            continue

        if tipo == "mascota":
            p["tipo"] = "mascota"
            p["categoria_id"] = _detectar_cat_mascota(p.get("texto_limpio", ""))
            p["nombre"] = p.get("autor", "")
            p["descripcion"] = p.get("texto_limpio", "")
            p["titulo"] = generar_titulo_mascota(p, p.get("categoria_id", 11))
            resultados["mascotas"].append(p)
            aprobados.append(p)
            continue

        if tipo == "empleo":
            tipo_empleo = p.get("tipo_empleo", "oferta")
            proc = procesar_empleo(p, tipo_empleo=tipo_empleo)
            if proc.get("error_ia"):
                proc["_error_visible"] = proc["error_ia"]
            resultados["empleos"].append(proc)
            aprobados.append(proc)
            continue

        if tipo == "alerta":
            proc = procesar_alerta(p, cats_alertas)
            if proc.get("error_ia"):
                proc["_error_visible"] = proc["error_ia"]
            cat_nombre = cats_alert_map.get(str(proc.get("categoria_id")), {}).get("nombre", "Alerta")
            proc["titulo"] = generar_titulo_alerta(proc, categoria_nombre=cat_nombre)
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
            proc["titulo"] = limpiar_titulo(proc.get("titulo") or generar_titulo_noticia_fallback(proc), max_chars=88)
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
        # Regla primero — IA solo cuando el resultado es genérico/pobre
        titulo_regla = generar_titulo_negocio(proc, categoria_nombre=categoria_nombre)
        if _titulo_pobre(titulo_regla):
            titulo_ai = generar_titulo_negocio_ia(proc, categoria_nombre=categoria_nombre, prefer="groq")
            proc["titulo"] = titulo_ai or titulo_regla or "Negocio en Mérida"
        else:
            proc["titulo"] = titulo_regla
        resultados["negocios"].append(proc)
        aprobados.append(proc)

    for p in aprobados:
        if not (p.get("imagenes") or []):
            p["imagenes_cloudinary"] = []

    # ── PASO 5: Subir imágenes aprobadas a Cloudinary ──────────────
    _set_estado(estado, paso="Subiendo imágenes", progreso=82, actividad="Preparando subida de imágenes a Cloudinary", add_history=True)
    imgs_ok = imgs_fail = 0

    posts_con_imagen = [x for x in aprobados if (x.get("imagenes") or [])]
    total_con_imagen = max(len(posts_con_imagen), 1)
    for idx_img, p in enumerate(posts_con_imagen, 1):
        autor = (p.get("autor") or "sin autor")[:40]
        num_imgs_post = len(p.get("imagenes", []) or [])
        _set_estado(estado, actividad=f"Subiendo imágenes de {autor} · post {idx_img}/{total_con_imagen} · {num_imgs_post} imagen(es)")
        imagenes = p.get("imagenes", []) or []
        res_imgs, ok, fail = subir_imagenes(p, meta=meta, config_grupo=config_grupo)
        p["imagenes_cloudinary"] = res_imgs
        imgs_ok += ok
        imgs_fail += fail

    # Si un post no-noticia se quedó sin imágenes finales, descártalo del resultado
    for bucket in ['negocios', 'alertas', 'mascotas', 'empleos']:
        conservados = []
        for p in resultados[bucket]:
            if p.get('imagenes_cloudinary'):
                conservados.append(p)
            else:
                p['_descartado'] = 'sin_imagen_final'
                resultados['ignorados'].append(p)
                descartados_sin_imagen += 1
        resultados[bucket] = conservados

    _set_estado(estado, detalles=f"Imágenes: {imgs_ok} ok, {imgs_fail} fallidas · sin imagen: {descartados_sin_imagen}", actividad="Subida de imágenes terminada", add_history=True)

    # ── PASO 6: Generar HTML ───────────────────────────────────────
    _set_estado(estado, paso="Generando reporte HTML", progreso=92, actividad="Armando el reporte visual HTML", add_history=True)
    nombre_archivo = generar_html_resultados(
        resultados=resultados,
        meta=meta,
        config_grupo=config_grupo,
        cats_negocios=cats_negocios,
        cats_noticias=cats_noticias,
        cats_alertas=cats_alertas,
    )

    # ── PASO 7: Guardar en DB ──────────────────────────────────────
    _set_estado(estado, paso="Guardando en base de datos", progreso=96, actividad="Insertando registros en base de datos", add_history=True)

    colonia_ids = config_grupo.get("colonia_ids") or [None]
    colonia_id = colonia_ids[0]
    db_nuevos = db_duplicados = 0

    total_db = len(resultados["negocios"]) + len(resultados["mascotas"]) + len(resultados["noticias"]) + len(resultados["alertas"]) + len(resultados["perdidos"])
    db_idx = 0

    for p in resultados["negocios"]:
        db_idx += 1
        if db_idx == 1 or db_idx % 12 == 0:
            _set_estado(estado, actividad=f"Guardando en DB {db_idx}/{max(total_db,1)}")
        try:
            _, st = insertar_negocio(p, colonia_id)
            db_nuevos += 1 if st == "nuevo" else 0
            db_duplicados += 1 if st == "duplicado" else 0
        except Exception as e:
            resultados["errores"].append({"tipo": "db_negocio", "error": str(e), "autor": p.get("autor")})

    for p in resultados["mascotas"]:
        db_idx += 1
        if db_idx == 1 or db_idx % 12 == 0:
            _set_estado(estado, actividad=f"Guardando en DB {db_idx}/{max(total_db,1)}")
        try:
            _, st = insertar_mascota(p, colonia_id)
            db_nuevos += 1 if st == "nuevo" else 0
            db_duplicados += 1 if st == "duplicado" else 0
        except Exception as e:
            resultados["errores"].append({"tipo": "db_mascota", "error": str(e), "autor": p.get("autor")})

    for p in resultados["noticias"]:
        db_idx += 1
        if db_idx == 1 or db_idx % 12 == 0:
            _set_estado(estado, actividad=f"Guardando en DB {db_idx}/{max(total_db,1)}")
        try:
            _, st = insertar_noticia(p, colonia_id)
            db_nuevos += 1 if st == "nuevo" else 0
            db_duplicados += 1 if st == "duplicado" else 0
        except Exception as e:
            resultados["errores"].append({"tipo": "db_noticia", "error": str(e), "autor": p.get("autor")})

    for p in resultados["alertas"]:
        db_idx += 1
        if db_idx == 1 or db_idx % 12 == 0:
            _set_estado(estado, actividad=f"Guardando en DB {db_idx}/{max(total_db,1)}")
        try:
            _, st = insertar_alerta(p, colonia_id)
            db_nuevos += 1 if st == "nuevo" else 0
            db_duplicados += 1 if st == "duplicado" else 0
        except Exception as e:
            resultados["errores"].append({"tipo": "db_alerta", "error": str(e), "autor": p.get("autor")})

    for p in resultados["empleos"]:
        db_idx += 1
        if db_idx == 1 or db_idx % 12 == 0:
            _set_estado(estado, actividad=f"Guardando en DB {db_idx}/{max(total_db,1)}")
        try:
            _, st = insertar_empleo(p, colonia_id)
            db_nuevos += 1 if st == "nuevo" else 0
            db_duplicados += 1 if st == "duplicado" else 0
        except Exception as e:
            resultados["errores"].append({"tipo": "db_empleo", "error": str(e), "autor": p.get("autor")})

    for p in resultados["perdidos"]:
        db_idx += 1
        if db_idx == 1 or db_idx % 12 == 0:
            _set_estado(estado, actividad=f"Guardando en DB {db_idx}/{max(total_db,1)}")
        try:
            _, st = insertar_perdido(p, colonia_id)
            db_nuevos += 1 if st == "nuevo" else 0
            db_duplicados += 1 if st == "duplicado" else 0
        except Exception as e:
            resultados["errores"].append({"tipo": "db_perdido", "error": str(e), "autor": p.get("autor")})

    actualizar_grupo_stats(meta.get("group_id"), len(posts))

    # ── Registrar actividad de autores ────────────────────────
    group_id_meta   = meta.get("group_id", "")
    group_name_meta = meta.get("group_name", "")
    fecha_captura   = meta.get("fecha_captura")

    # Todos los posts aprobados con su tipo
    _tipo_map = {
        "negocios": "negocio",
        "mascotas": "mascota",
        "noticias": "noticia",
        "alertas":  "alerta",
        "empleos":  "empleo",
        "perdidos": "perdido",
    }
    for bucket, tipo_str in _tipo_map.items():
        for p in resultados.get(bucket, []):
            autor_id_fb = p.get("autor_id")
            if not autor_id_fb:
                continue
            try:
                autor_db_id, es_empresa = upsert_autor_completo(autor_id_fb, p.get("autor", ""))
                p["_autor_db_id"] = autor_db_id   # guardar para las inserciones
                p["_es_empresa"]  = es_empresa     # True si el autor es empresa
                registrar_actividad(
                    autor_db_id,
                    group_id_meta,
                    group_name_meta,
                    tipo_str,
                    fbid_post   = p.get("fbid_post"),
                    fecha       = fecha_captura,
                )
            except Exception as e:
                resultados["errores"].append({
                    "tipo":  "db_autor",
                    "error": str(e),
                    "autor": p.get("autor"),
                })

    # ── PASO 8: Reporte de clientes potenciales (autores repetidores) ──────────
    _set_estado(estado, paso="Analizando clientes potenciales", progreso=99,
                actividad="Identificando autores que publicaron múltiples veces", add_history=True)

    author_posts = defaultdict(list)
    for p in posts:
        autor = (p.get("autor") or "desconocido").strip()
        if autor not in ("desconocido", "sin autor"):
            author_posts[autor].append(p)

    clientes_potenciales = []
    for autor, aposts in author_posts.items():
        if len(aposts) < 2:
            continue
        from utils import extraer_telefono
        phones = []
        for ap in aposts:
            tel = extraer_telefono(ap.get("texto") or "")
            if tel and tel not in phones:
                phones.append(tel)

        tl_all = " ".join((ap.get("texto") or "").lower() for ap in aposts)
        if any(k in tl_all for k in ["oferta","promo","pedido","laborando","servicio","vendemos","disponible","taller","pizza","ropa"]):
            categoria = "COMERCIOS"
        elif any(k in tl_all for k in ["perro","gato","gatito","adopción","adopcion","rescate","perdido","mascota"]):
            categoria = "MASCOTAS"
        else:
            categoria = "OTRO"

        muestra = max(aposts, key=lambda x: len(x.get("texto") or ""))
        clientes_potenciales.append({
            "autor":     autor,
            "num_posts": len(aposts),
            "categoria": categoria,
            "telefonos": phones,
            "muestra":   (muestra.get("texto") or "")[:200],
        })

    clientes_potenciales.sort(key=lambda x: x["num_posts"], reverse=True)

    # Log de descartados con motivo
    log_descartados = [
        {
            "autor":   (p.get("autor") or "?")[:40],
            "motivo":  p.get("_descartado", "desconocido"),
            "palabras": len((p.get("texto") or "").split()),
            "texto":   (p.get("texto") or "")[:120],
        }
        for p in resultados["ignorados"]
    ]

    _set_estado(estado, paso="Completado", progreso=100, actividad="Proceso completado", add_history=True)
    estado["archivo_html"]  = nombre_archivo
    estado["_resultados"]   = resultados   # disponible para /guardar-db
    estado["_config_temp"]  = config_grupo # disponible para /guardar-db
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
        "empleos": len(resultados["empleos"]),
        "perdidos": len(resultados["perdidos"]),
        "perdidos_perdido": sum(1 for p in resultados["perdidos"] if p.get("perdido_estado") == "perdido"),
        "perdidos_encontrado": sum(1 for p in resultados["perdidos"] if p.get("perdido_estado") == "encontrado"),
        "perdidos_sin_estado": sum(1 for p in resultados["perdidos"] if not p.get("perdido_estado")),
        "ignorados": len(resultados["ignorados"]),
        "imagenes_ok": imgs_ok,
        "imagenes_fail": imgs_fail,
        "descartados_sin_imagen": descartados_sin_imagen,
        "db_nuevos": db_nuevos,
        "db_duplicados": db_duplicados,
        "clientes_potenciales": clientes_potenciales,
        "log_descartados": log_descartados,
        **get_resumen_costo(),
    }
    return nombre_archivo



def _detectar_cat_mascota(texto):
    txt = (texto or '').lower()

    if any(w in txt for w in [
        "se escapó", "se escapo", "se me escapó", "se me escapo", "perdid", "extravi",
        "si la ves", "si lo ves", "si la ven", "si lo ven", "responde al nombre", "avísame", "avisame", "se salió", "se salio",
        "no la persigas", "no lo persigas", "ayuda encontrar", "si la encuentran", "si lo encuentran"
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
