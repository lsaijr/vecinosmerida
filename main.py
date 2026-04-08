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

app = FastAPI()

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


@app.get("/descargar/{nombre}")
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
