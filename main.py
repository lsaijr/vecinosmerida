from fastapi import FastAPI, UploadFile, File, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, FileResponse
import json
import threading
import os

from pipeline import ejecutar_pipeline
from utils import match_colonias, detectar_tipo_por_nombre
from db import buscar_grupo, registrar_grupo, obtener_colonias

app = FastAPI()

# Estado global del pipeline (simple, uso interno)
estado = {
    "paso": "esperando",
    "progreso": 0,
    "archivo_html": None,
    "resumen": None,
    "detalles": ""
}

# ─── ANALIZAR GRUPO ANTES DE PROCESAR ───────────────────────
@app.post("/analizar-grupo")
async def analizar_grupo(file: UploadFile = File(...)):
    """
    Lee el JSON, detecta el grupo y retorna:
    - Si el grupo ya está registrado: info del grupo
    - Si es nuevo: candidatos de colonia + tipo sugerido
    """
    contenido = await file.read()
    try:
        data = json.loads(contenido)
    except Exception:
        return JSONResponse({"error": "JSON inválido"}, status_code=400)

    meta = data.get("meta", {})
    group_id = meta.get("group_id", "")
    group_name = meta.get("group_name", "Grupo sin nombre")
    total_posts = meta.get("total_posts", len(data.get("posts", [])))

    # Guardar temporalmente en estado para el siguiente paso
    estado["_posts_temp"] = data.get("posts", [])
    estado["_meta_temp"] = meta

    # ¿El grupo ya está registrado?
    grupo_registrado = buscar_grupo(group_id)
    if grupo_registrado:
        return {
            "conocido": True,
            "group_id": group_id,
            "group_name": group_name,
            "tipo": grupo_registrado["tipo"],
            "total_posts": total_posts
        }

    # Grupo nuevo: detectar colonia y tipo
    resultado_match, candidatas = match_colonias(group_name)
    tipo_sugerido = detectar_tipo_por_nombre(group_name)
    todas_colonias = obtener_colonias()

    return {
        "conocido": False,
        "group_id": group_id,
        "group_name": group_name,
        "total_posts": total_posts,
        "tipo_sugerido": tipo_sugerido,
        "match_colonias": resultado_match,  # "exacto", "multiple", "ninguno"
        "candidatas": candidatas,
        "todas_colonias": todas_colonias
    }


# ─── CONFIRMAR Y PROCESAR ────────────────────────────────────
@app.post("/procesar")
async def procesar(request: Request):
    """
    Recibe config del grupo (tipo, colonia_ids) y arranca el pipeline.
    Body JSON: { group_id, tipo, colonia_ids, colonia_nombres, guardar_grupo }
    """
    global estado
    body = await request.json()

    posts = estado.get("_posts_temp", [])
    meta = estado.get("_meta_temp", {})

    if not posts:
        return JSONResponse({"error": "No hay posts cargados. Sube el archivo primero."}, status_code=400)

    config_grupo = {
        "tipo": body.get("tipo", "vecinos"),
        "colonia_ids": body.get("colonia_ids", []),
        "colonia_nombres": body.get("colonia_nombres", ["General"])
    }

    # Registrar grupo nuevo si se indicó
    if body.get("guardar_grupo", False):
        registrar_grupo(
            group_id=meta.get("group_id"),
            nombre=meta.get("group_name"),
            tipo=config_grupo["tipo"],
            colonia_ids=config_grupo["colonia_ids"],
            notas=body.get("notas", "")
        )

    # Reset estado
    estado.update({
        "paso": "Iniciando",
        "progreso": 5,
        "archivo_html": None,
        "resumen": None,
        "detalles": ""
    })

    def run():
        global estado
        try:
            ejecutar_pipeline(posts, meta, config_grupo, estado)
        except Exception as e:
            estado["paso"] = "Error"
            estado["progreso"] = 0
            estado["error"] = str(e)

    threading.Thread(target=run).start()
    return {"status": "procesando"}


# ─── STATUS ──────────────────────────────────────────────────
@app.get("/status")
def status():
    return {
        "paso": estado.get("paso"),
        "progreso": estado.get("progreso"),
        "detalles": estado.get("detalles", ""),
        "archivo_html": estado.get("archivo_html"),
        "resumen": estado.get("resumen"),
        "error": estado.get("error")
    }


# ─── DESCARGAR HTML GENERADO ────────────────────────────────
@app.get("/descargar/{nombre}")
def descargar(nombre: str):
    ruta = os.path.join("static", "resultados", nombre)
    if not os.path.exists(ruta):
        return JSONResponse({"error": "Archivo no encontrado"}, status_code=404)
    return FileResponse(ruta, media_type="text/html", filename=nombre)


# ─── STATIC AL FINAL ─────────────────────────────────────────
app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/", StaticFiles(directory="static", html=True), name="root")
