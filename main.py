from fastapi import FastAPI, UploadFile, File
from fastapi.staticfiles import StaticFiles
import json
import threading

from pipeline import ejecutar_pipeline

app = FastAPI()

estado = {"paso": "esperando", "progreso": 0}

app.mount("/", StaticFiles(directory="static", html=True), name="static")

@app.post("/procesar")
async def procesar(file: UploadFile = File(...)):
    global estado

    contenido = await file.read()
    posts = json.loads(contenido)

    def run():
        global estado
        estado["paso"] = "Iniciando"
        estado["progreso"] = 5

        ejecutar_pipeline(posts, estado)

        estado["paso"] = "Finalizado"
        estado["progreso"] = 100

    threading.Thread(target=run).start()

    return {"status": "procesando"}

@app.get("/status")
def status():
    return estado
