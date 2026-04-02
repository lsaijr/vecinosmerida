import re
from rapidfuzz import fuzz

def limpiar_texto(txt):
    if not txt:
        return ""
    txt = re.sub(r'http\S+', '', txt)
    txt = re.sub(r'\s+', ' ', txt).strip()
    return txt

def paso_1_limpieza(posts):
    limpios = []
    for post in posts:
        txt = post.get("texto", "").strip()
        txt_limpio = limpiar_texto(txt)
        if len(txt_limpio) < 15:
            continue
        post["texto_limpio"] = txt_limpio
        limpios.append(post)
    return limpios

def paso_2_clusters(posts):
    clusters = []
    usados = set()

    for i, p1 in enumerate(posts):
        if i in usados:
            continue

        cluster = [p1]
        usados.add(i)

        for j, p2 in enumerate(posts):
            if j in usados:
                continue

            sim = fuzz.token_set_ratio(p1["texto_limpio"], p2["texto_limpio"])
            if sim >= 85:
                cluster.append(p2)
                usados.add(j)

        clusters.append(cluster)

    return clusters

def clasificar_post(txt):
    txt = txt.lower()
    if "tacos" in txt:
        return 1
    return 12

def extraer_telefono(txt):
    match = re.search(r'\d{10}', txt)
    return match.group() if match else None
