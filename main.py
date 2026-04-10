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

APP_VERSION = "2026-04-10-v2"
print(f"🚀 VecinosMérida Pipeline arrancando — versión {APP_VERSION}")

app = FastAPI()


@app.get("/version")
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
