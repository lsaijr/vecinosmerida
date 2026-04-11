from fastapi import FastAPI, UploadFile, File, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, Response
import json
import threading
import os
import time

from pipeline import ejecutar_pipeline
from utils import match_colonias, detectar_tipo_por_nombre
from db import buscar_grupo, registrar_grupo, obtener_colonias

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
                autor_db_id = upsert_autor_completo(autor_id_fb, p.get("autor", ""))
                p["_autor_db_id"] = autor_db_id
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


app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/", StaticFiles(directory="static", html=True), name="root")
# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINTS PARA JSON LIMPIO (pre-clasificado por Claude)
# Agregar estos endpoints al main.py existente
# ─────────────────────────────────────────────────────────────────────────────
# Imports adicionales necesarios (ya deben estar en main.py):
# from fastapi import UploadFile, File, Request
# from db import buscar_grupo, registrar_grupo, obtener_colonias,
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

    grupo_registrado = buscar_grupo(group_id)
    if grupo_registrado:
        return {
            "conocido": True,
            "group_id": group_id,
            "group_name": group_name,
            "tipo": grupo_registrado["tipo"],
            "colonia_ids": grupo_registrado.get("colonia_ids", []),
            "total_posts": len(posts),
        }

    # Grupo nuevo: sugerir colonia y tipo
    from utils import match_colonias, detectar_tipo_por_nombre
    resultado_match, candidatas = match_colonias(group_name)
    tipo_sugerido = detectar_tipo_por_nombre(group_name) or "mascotas"
    todas_colonias = obtener_colonias()

    return {
        "conocido": False,
        "group_id": group_id,
        "group_name": group_name,
        "total_posts": len(posts),
        "tipo_sugerido": tipo_sugerido,
        "match_colonias": resultado_match,
        "candidatas": candidatas,
        "todas_colonias": todas_colonias,
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
                        autor_db_id = upsert_autor_completo(autor_id_fb, p.get("autor", ""))
                        p["_autor_db_id"] = autor_db_id
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
