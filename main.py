from fastapi import FastAPI, UploadFile, File, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, Response
import json
import threading
import os
import time

from pipeline import ejecutar_pipeline
from utils import match_colonias, detectar_tipo_por_nombre
from db import buscar_grupo, registrar_grupo, obtener_colonias, obtener_potenciales_clientes

APP_VERSION = "2026-04-10-v3"
print(f"🚀 VecinosMérida Pipeline arrancando — versión {APP_VERSION}")

app = FastAPI()


@app.get("/api/version")
def version():
    return {"version": APP_VERSION, "ok": True}

# ── Lock: garantiza un solo pipeline activo a la vez ─────────────────────────
_pipeline_lock = threading.Lock()

estado = {
    "paso": "esperando",
    "progreso": 0,
    "archivo_html": None,
    "resumen": None,
    "detalles": "",
    "error": None,
    "inicio_ts": None,
    "elapsed_seconds": 0,
    "actividad": "",
    "historial": [],
}


@app.post("/analizar-grupo")
async def analizar_grupo(file: UploadFile = File(...)):
    contenido = await file.read()
    try:
        data = json.loads(contenido)
    except Exception:
        return JSONResponse({"error": "JSON inválido"}, status_code=400)

    meta = data.get("meta", {})
    group_id = meta.get("group_id", "")
    group_name = meta.get("group_name", "Grupo sin nombre")
    total_posts = meta.get("total_posts", len(data.get("posts", [])))

    estado["_posts_temp"] = data.get("posts", [])
    estado["_meta_temp"] = meta

    grupo_registrado = buscar_grupo(group_id)
    if grupo_registrado:
        return {
            "conocido": True,
            "group_id": group_id,
            "group_name": group_name,
            "tipo": grupo_registrado["tipo"],
            "total_posts": total_posts,
        }

    resultado_match, candidatas = match_colonias(group_name)
    tipo_sugerido = detectar_tipo_por_nombre(group_name)
    todas_colonias = obtener_colonias()

    return {
        "conocido": False,
        "group_id": group_id,
        "group_name": group_name,
        "total_posts": total_posts,
        "tipo_sugerido": tipo_sugerido,
        "match_colonias": resultado_match,
        "candidatas": candidatas,
        "todas_colonias": todas_colonias,
    }


@app.post("/procesar")
async def procesar(request: Request):
    global estado

    # Rechazar si ya hay un proceso corriendo
    if _pipeline_lock.locked():
        return JSONResponse(
            {"error": "Ya hay un proceso en curso. Espera a que termine antes de iniciar otro."},
            status_code=409
        )

    body = await request.json()
    posts = estado.get("_posts_temp", [])
    meta  = estado.get("_meta_temp", {})

    if not posts:
        return JSONResponse({"error": "No hay posts cargados. Sube el archivo primero."}, status_code=400)

    config_grupo = {
        "tipo": body.get("tipo", "vecinos"),
        "colonia_ids": body.get("colonia_ids", []),
        "colonia_nombres": body.get("colonia_nombres", ["General"]),
    }

    if body.get("guardar_grupo", False):
        registrar_grupo(
            group_id=meta.get("group_id"),
            nombre=meta.get("group_name"),
            tipo=config_grupo["tipo"],
            colonia_ids=config_grupo["colonia_ids"],
            notas=body.get("notas", ""),
        )

    estado.update({
        "paso": "Iniciando",
        "progreso": 5,
        "archivo_html": None,
        "resumen": None,
        "detalles": "",
        "error": None,
        "inicio_ts": time.time(),
        "elapsed_seconds": 0,
        "actividad": "Preparando procesamiento",
        "historial": ["Inicio del procesamiento"],
    })

    def run():
        global estado
        _pipeline_lock.acquire()
        try:
            ejecutar_pipeline(posts, meta, config_grupo, estado)
        except Exception as e:
            estado["paso"] = "Error"
            estado["progreso"] = 0
            estado["error"] = str(e)
            estado["actividad"] = "Proceso detenido por error"
            if estado.get("inicio_ts"):
                estado["elapsed_seconds"] = int(time.time() - estado["inicio_ts"])
        finally:
            _pipeline_lock.release()

    threading.Thread(target=run, daemon=True).start()
    return {"status": "procesando"}


@app.get("/status")
def status():
    inicio_ts = estado.get("inicio_ts")
    elapsed_seconds = 0
    if inicio_ts:
        elapsed_seconds = int(time.time() - inicio_ts)
        estado["elapsed_seconds"] = elapsed_seconds
    return {
        "paso": estado.get("paso"),
        "progreso": estado.get("progreso"),
        "detalles": estado.get("detalles", ""),
        "archivo_html": estado.get("archivo_html"),
        "resumen": estado.get("resumen"),
        "error": estado.get("error"),
        "elapsed_seconds": elapsed_seconds,
        "actividad": estado.get("actividad", ""),
        "historial": estado.get("historial", [])[-6:],
        "ocupado": _pipeline_lock.locked(),
    }


@app.post("/guardar-db")
async def guardar_db():
    """
    Guarda en DB los resultados del último pipeline procesado.
    Retorna un reporte de nuevos vs duplicados por tipo.
    """
    from db import (
        insertar_negocio, insertar_noticia, insertar_alerta,
        insertar_empleo, actualizar_grupo_stats,
        upsert_autor_completo, registrar_actividad,
    )

    resumen = estado.get("resumen")
    if not resumen:
        return JSONResponse({"error": "No hay resultados procesados. Ejecuta el pipeline primero."}, status_code=400)

    # Recuperar resultados del pipeline desde el estado
    # El pipeline los guarda en estado["_resultados"]
    resultados = estado.get("_resultados")
    meta        = estado.get("_meta_temp", {})
    config      = estado.get("_config_temp", {})
    if not resultados:
        return JSONResponse({"error": "Resultados no disponibles. Ejecuta el pipeline primero."}, status_code=400)

    colonia_ids = config.get("colonia_ids") or [None]
    colonia_id  = colonia_ids[0]
    group_id    = meta.get("group_id", "")
    group_name  = meta.get("group_name", "")
    fecha       = meta.get("fecha_captura")

    conteo = {
        "negocios_nuevos": 0, "negocios_dup": 0,
        "noticias_nuevas": 0, "noticias_dup": 0,
        "alertas_nuevas":  0, "alertas_dup":  0,
        "mascotas_nuevas": 0, "mascotas_dup": 0,
        "empleos_nuevos":  0, "empleos_dup":  0,
        "errores": [],
    }

    BUCKETS = [
        ("negocios",  insertar_negocio,  "negocios_nuevos",  "negocios_dup"),
        ("mascotas",  insertar_negocio,  "mascotas_nuevas",  "mascotas_dup"),
        ("noticias",  insertar_noticia,  "noticias_nuevas",  "noticias_dup"),
        ("alertas",   insertar_alerta,   "alertas_nuevas",   "alertas_dup"),
        ("empleos",   insertar_empleo,   "empleos_nuevos",   "empleos_dup"),
    ]

    for bucket, fn_insertar, key_nuevo, key_dup in BUCKETS:
        for p in resultados.get(bucket, []):
            try:
                _, st = fn_insertar(p, colonia_id)
                if st == "nuevo":
                    conteo[key_nuevo] += 1
                else:
                    conteo[key_dup] += 1
            except Exception as e:
                conteo["errores"].append({"tipo": bucket, "error": str(e)[:120]})

    # Registrar actividad de autores
    tipo_map = {
        "negocios": "negocio", "mascotas": "mascota",
        "noticias": "noticia", "alertas": "alerta", "empleos": "empleo",
    }
    for bucket, tipo_str in tipo_map.items():
        for p in resultados.get(bucket, []):
            autor_id_fb = p.get("autor_id")
            if not autor_id_fb:
                continue
            try:
                autor_db_id, es_empresa = upsert_autor_completo(autor_id_fb, p.get("autor", ""))
                p["_autor_db_id"] = autor_db_id
                p["_es_empresa"]  = es_empresa
                registrar_actividad(autor_db_id, group_id, group_name,
                                    tipo_str, p.get("fbid_post"), fecha)
            except Exception:
                pass

    try:
        actualizar_grupo_stats(group_id, len(estado.get("_posts_temp", [])))
    except Exception:
        pass

    conteo["total_nuevos"] = (
        conteo["negocios_nuevos"] + conteo["noticias_nuevas"] +
        conteo["alertas_nuevas"] + conteo["mascotas_nuevas"] +
        conteo["empleos_nuevos"]
    )

    return conteo



def descargar(nombre: str):
    ruta = os.path.join("static", "resultados", nombre)
    if not os.path.exists(ruta):
        return JSONResponse({"error": "Archivo no encontrado"}, status_code=404)
    with open(ruta, "rb") as f:
        data = f.read()
    return Response(
        content=data,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{nombre}"'},
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINTS PARA JSON LIMPIO (pre-clasificado por Claude)
# Agregar estos endpoints al main.py existente
# ─────────────────────────────────────────────────────────────────────────────
# Imports adicionales necesarios (ya deben estar en main.py):
# from fastapi import UploadFile, File, Request
# from db import buscar_grupo, registrar_grupo, obtener_colonias, obtener_potenciales_clientes,
#               insertar_negocio, insertar_alerta, upsert_autor_completo,
#               registrar_actividad, actualizar_grupo_stats
# from cloudinary_service import subir_imagen_cloudinary  (o el nombre correcto)

_estado_limpio = {
    "paso": "esperando",
    "progreso": 0,
    "total": 0,
    "procesados": 0,
    "nuevos": 0,
    "duplicados": 0,
    "errores": [],
    "error_fatal": None,
    "listo": False,
}
_lock_limpio = threading.Lock()


@app.post("/analizar-limpio")
async def analizar_limpio(file: UploadFile = File(...)):
    """
    Paso 1: Recibe el JSON limpio, identifica el grupo.
    Si el grupo ya está registrado → retorna config guardada.
    Si no → retorna sugerencias para que el usuario configure.
    """
    contenido = await file.read()
    try:
        data = json.loads(contenido)
    except Exception:
        return JSONResponse({"error": "JSON inválido"}, status_code=400)

    meta = data.get("meta", {})
    posts = data.get("posts", [])
    group_id = meta.get("group_id", "")
    group_name = meta.get("group_name", "Grupo sin nombre")

    if not posts:
        return JSONResponse({"error": "El JSON no contiene posts"}, status_code=400)

    # Guardar en estado para uso posterior
    _estado_limpio["_posts_temp"] = posts
    _estado_limpio["_meta_temp"] = meta
    _estado_limpio["listo"] = False

    try:
        grupo_registrado = buscar_grupo(group_id)
    except Exception:
        grupo_registrado = None

    if grupo_registrado:
        try:
            from db import obtener_colonias_de_grupo
            colonias_grupo = obtener_colonias_de_grupo(group_id)
            colonia_ids = [c["id"] for c in colonias_grupo]
        except Exception:
            colonia_ids = []
        return {
            "conocido": True,
            "group_id": group_id,
            "group_name": group_name,
            "tipo": grupo_registrado.get("tipo", "vecinos"),
            "colonia_ids": colonia_ids,
            "total_posts": len(posts),
        }

    # Grupo nuevo: sugerir colonia y tipo
    try:
        from utils import match_colonias, detectar_tipo_por_nombre
        resultado_match, candidatas = match_colonias(group_name)
        tipo_sugerido = detectar_tipo_por_nombre(group_name) or "mascotas"
    except Exception:
        resultado_match, candidatas, tipo_sugerido = [], [], "vecinos"

    try:
        todas_colonias = obtener_colonias()
    except Exception:
        todas_colonias = []

    return {
        "conocido": False,
        "group_id": group_id,
        "group_name": group_name,
        "total_posts": len(posts),
        "tipo_sugerido": tipo_sugerido,
        "match_colonias": resultado_match,
        "candidatas": [{"id": c["id"], "nombre": c["nombre"]} for c in (candidatas or [])],
        "todas_colonias": [{"id": c["id"], "nombre": c["nombre"]} for c in (todas_colonias or [])],
    }


@app.post("/procesar-limpio")
async def procesar_limpio(request: Request):
    """
    Paso 2: Procesa el JSON limpio.
    - Registra grupo si es nuevo
    - Sube imágenes a Cloudinary
    - Inserta en DB directamente (sin IA, sin filtros)
    """
    global _estado_limpio

    if _lock_limpio.locked():
        return JSONResponse(
            {"error": "Ya hay un proceso en curso."},
            status_code=409
        )

    body = await request.json()
    posts = _estado_limpio.get("_posts_temp", [])
    meta  = _estado_limpio.get("_meta_temp", {})

    if not posts:
        return JSONResponse(
            {"error": "No hay posts cargados. Sube el archivo primero."},
            status_code=400
        )

    config_grupo = {
        "tipo": body.get("tipo", "mascotas"),
        "colonia_ids": body.get("colonia_ids", []),
        "colonia_nombres": body.get("colonia_nombres", ["General"]),
    }

    # Registrar grupo si es nuevo o si el usuario pidió guardarlo
    if body.get("guardar_grupo", False):
        registrar_grupo(
            group_id=meta.get("group_id"),
            nombre=meta.get("group_name"),
            tipo=config_grupo["tipo"],
            colonia_ids=config_grupo["colonia_ids"],
            notas=body.get("notas", ""),
        )

    _estado_limpio.update({
        "paso": "Iniciando",
        "progreso": 0,
        "total": len(posts),
        "procesados": 0,
        "nuevos": 0,
        "duplicados": 0,
        "errores": [],
        "error_fatal": None,
        "listo": False,
        "inicio_ts": time.time(),
    })

    def run():
        global _estado_limpio
        _lock_limpio.acquire()
        try:
            from cloudinary_service import subir_imagenes
            from db import (
                insertar_negocio, insertar_alerta,
                upsert_autor_completo, registrar_actividad,
                actualizar_grupo_stats,
            )

            colonia_id  = (config_grupo["colonia_ids"] or [None])[0]
            group_id    = meta.get("group_id", "")
            group_name  = meta.get("group_name", "")
            fecha       = meta.get("fecha_captura")
            total       = len(posts)

            for i, p in enumerate(posts):
                _estado_limpio["paso"] = f"Procesando post {i+1} de {total}"
                _estado_limpio["progreso"] = int((i / total) * 90)
                _estado_limpio["procesados"] = i + 1

                try:
                    # 1. Subir imágenes a Cloudinary
                    res_imgs, ok, fail = subir_imagenes(p, meta=meta, config_grupo=config_grupo)
                    if res_imgs:
                        p["imagenes_cloudinary"] = res_imgs

                    # 2. Campos requeridos para inserción
                    p["fecha_captura"]  = fecha
                    p["tipo"]           = p.get("tipo", "mascota")
                    p["categoria_id"]   = p.get("categoria_id", 11)
                    p["descripcion"]    = p.get("texto", "")
                    p["nombre"]         = p.get("autor", "")

                    # 3. Upsert autor
                    autor_id_fb = p.get("autor_id")
                    if autor_id_fb:
                        autor_db_id, es_empresa = upsert_autor_completo(autor_id_fb, p.get("autor", ""))
                        p["_autor_db_id"] = autor_db_id
                        p["_es_empresa"]  = es_empresa
                    else:
                        autor_db_id = None

                    # 4. Insertar en DB según tipo
                    tipo = p.get("tipo", "mascota")
                    if tipo in ("mascota", "negocio"):
                        _, st = insertar_negocio(p, colonia_id)
                    elif tipo == "alerta":
                        _, st = insertar_alerta(p, colonia_id)
                    else:
                        _, st = insertar_negocio(p, colonia_id)  # fallback

                    if st == "nuevo":
                        _estado_limpio["nuevos"] += 1
                    else:
                        _estado_limpio["duplicados"] += 1

                    # 5. Registrar actividad del autor
                    if autor_id_fb and autor_db_id:
                        try:
                            registrar_actividad(
                                autor_db_id, group_id, group_name,
                                tipo, p.get("fbid_post"), fecha
                            )
                        except Exception:
                            pass

                except Exception as e:
                    _estado_limpio["errores"].append({
                        "autor": p.get("autor", ""),
                        "error": str(e)[:150],
                    })

            # Actualizar stats del grupo
            try:
                actualizar_grupo_stats(group_id, total)
            except Exception:
                pass

            _estado_limpio["paso"] = "Completado"
            _estado_limpio["progreso"] = 100
            _estado_limpio["listo"] = True

        except Exception as e:
            _estado_limpio["paso"] = "Error"
            _estado_limpio["error_fatal"] = str(e)
        finally:
            _lock_limpio.release()

    threading.Thread(target=run, daemon=True).start()
    return {"status": "procesando"}


@app.get("/status-limpio")
def status_limpio():
    inicio_ts = _estado_limpio.get("inicio_ts")
    elapsed = int(time.time() - inicio_ts) if inicio_ts else 0
    return {
        "paso":        _estado_limpio.get("paso"),
        "progreso":    _estado_limpio.get("progreso", 0),
        "total":       _estado_limpio.get("total", 0),
        "procesados":  _estado_limpio.get("procesados", 0),
        "nuevos":      _estado_limpio.get("nuevos", 0),
        "duplicados":  _estado_limpio.get("duplicados", 0),
        "errores":     _estado_limpio.get("errores", []),
        "error_fatal": _estado_limpio.get("error_fatal"),
        "listo":       _estado_limpio.get("listo", False),
        "ocupado":     _lock_limpio.locked(),
        "elapsed":     elapsed,
    }


@app.get("/potenciales-clientes")
def potenciales_clientes(limite: int = 50, score_minimo: int = 5):
    """
    Retorna empresas con alto ranking_score que no son clientes aún.
    Ordenadas por score desc — las que más postean primero.
    """
    try:
        rows = obtener_potenciales_clientes(limite=limite, score_minimo=score_minimo)
        # Serializar fechas
        for r in rows:
            for k, v in r.items():
                if hasattr(v, 'isoformat'):
                    r[k] = v.isoformat()
        return {"total": len(rows), "clientes": rows}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/groq-limpiar")
async def groq_limpiar(request: Request):
    """
    Llama a Groq con el JSON crudo y un modelo específico.
    La API key viene de la variable de entorno GROQ_API_KEY.
    Body: { "model": "llama-3.3-70b-versatile", "json_data": {...} }
    """
    import httpx

    groq_key = os.environ.get("GROQ_API_KEY", "")
    if not groq_key:
        return JSONResponse({"error": "GROQ_API_KEY no configurada en el servidor"}, status_code=500)

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Body inválido"}, status_code=400)

    model = body.get("model", "llama-3.3-70b-versatile")
    json_data = body.get("json_data")
    if not json_data:
        return JSONResponse({"error": "Falta json_data"}, status_code=400)

    MODELOS_PERMITIDOS = [
        "llama-3.3-70b-versatile",
        "llama-3.1-8b-instant",
        "mixtral-8x7b-32768",
    ]
    if model not in MODELOS_PERMITIDOS:
        return JSONResponse({"error": f"Modelo no permitido: {model}"}, status_code=400)

    SYSTEM_PROMPT = """Eres un asistente que limpia posts de Facebook para VecinosMérida.com (Mérida, Yucatán, México).

INSTRUCCIÓN CRÍTICA: Devuelve ÚNICAMENTE el JSON válido. Sin texto antes ni después. Sin "..." en arrays.

REGLAS DE DESCARTE — eliminar el post si:
- num_imgs = 0 y imagenes vacío
- palabra individual >28 chars mezclando letras y números
- menos de 5 palabras útiles
- actualización de cierre <35 palabras: "ya apareció", "ya está con sus dueños", "muchas gracias por compartir"
- autor contiene "Indicador de estado online"

MANEJO DE DUPLICADOS:
- Mismo autor + mismos primeros 120 chars = duplicado
- Conservar solo el primero, agregar campo "repeticiones" con el total
- Eliminar las copias

NORMALIZACIÓN DEL CAMPO "texto":
- Mayúsculas SOLO en: inicio del texto, después de punto/!/?, nombres propios, siglas
- NUNCA texto todo en MAYÚSCULAS → convertir a sentence case
- Corregir: perdio→perdió, telefono→teléfono, ke→que, xfa→por favor, tmb→también
- !!!! → ! y ???? → ?
- NO modificar: teléfonos, precios, URLs, nombres de colonias, hashtags

CAMPOS A AGREGAR a cada post válido:
- fbid_post: extraer de url_post si tiene /posts/XXXXXXXXX/, si no: "syn_" + 18 chars únicos
- repeticiones: número de veces publicado (mínimo 1)

FORMATO DE SALIDA:
- Mismo JSON de entrada, solo posts válidos, texto normalizado
- Campos fbid_post y repeticiones agregados
- meta.total_posts actualizado
- NUNCA uses "..." en arrays de imagenes o videos — copia los arrays completos"""

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Aquí está el JSON de Facebook para limpiar:\n\n{json.dumps(json_data, ensure_ascii=False)}"}
        ],
        "temperature": 0.1,
        "max_tokens": 32000,
        "response_format": {"type": "json_object"}
    }

    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {groq_key}"
                },
                json=payload
            )

        if resp.status_code != 200:
            err = resp.json()
            return JSONResponse({"error": err.get("error", {}).get("message", f"HTTP {resp.status_code}")}, status_code=500)

        data = resp.json()
        return JSONResponse({
            "content": data["choices"][0]["message"]["content"],
            "usage": data.get("usage", {}),
            "model": model
        })

    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ── Static files — debe ir AL FINAL para no interceptar endpoints ──
app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/", StaticFiles(directory="static", html=True), name="root")
