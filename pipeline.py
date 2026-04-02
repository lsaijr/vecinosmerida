from utils import *
from ia import procesar_con_ia
from db import guardar_post
from cloudinary_service import subir_imagenes

def ejecutar_pipeline(posts, estado):

    estado["paso"] = "Limpieza"
    estado["progreso"] = 10
    posts_limpios = paso_1_limpieza(posts)

    for p in posts_limpios:
        p["telefono"] = extraer_telefono(p["texto_limpio"])

    estado["paso"] = "Duplicados"
    estado["progreso"] = 30
    clusters = paso_2_clusters(posts_limpios)

    unicos = [c[0] for c in clusters if len(c) == 1]

    estado["paso"] = "Clasificación"
    estado["progreso"] = 50
    for p in unicos:
        p["categoria"] = clasificar_post(p["texto_limpio"])

    estado["paso"] = "IA procesando"
    estado["progreso"] = 70
    procesados = procesar_con_ia(unicos)

    estado["paso"] = "Subiendo imágenes"
    estado["progreso"] = 85
    for p in procesados:
        p["imagenes"] = subir_imagenes(p.get("imagenes", []))

    estado["paso"] = "Guardando en DB"
    estado["progreso"] = 95
    for p in procesados:
        guardar_post(p)

    return True
