import re
from rapidfuzz import fuzz
from db import obtener_colonias

def limpiar_texto(txt):
    if not txt:
        return ""
    txt = re.sub(r'http\S+', '', txt)
    txt = re.sub(r'\s+', ' ', txt).strip()
    return txt

def extraer_telefono(txt):
    match = re.search(r'\d{10}', txt)
    return match.group() if match else None

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

# ─── MATCH DE COLONIAS ───────────────────────────────────────

KEYWORDS_NOTICIAS = [
    "noticias", "ultimas", "última", "última hora", "hoy",
    "policia", "policía", "seguridad", "yucatan", "yucatán",
    "merida", "mérida", "informativo", "novedades", "prensa",
    "notificaciones", "alerta", "urgente"
]

def detectar_tipo_por_nombre(group_name):
    """Detecta si el grupo es de noticias o vecinos/negocios por palabras clave."""
    nombre_lower = group_name.lower()
    for kw in KEYWORDS_NOTICIAS:
        if kw in nombre_lower:
            return "noticias"
    return "vecinos"

def match_colonias(group_name):
    """
    Busca coincidencias entre el nombre del grupo y las colonias en DB.
    Retorna:
      - "exacto": una sola colonia con alta coincidencia
      - "multiple": varias colonias detectadas
      - "ninguno": no hay match
    + lista de colonias candidatas con sus scores
    """
    colonias = obtener_colonias()
    nombre_lower = group_name.lower()
    candidatas = []

    for col in colonias:
        col_lower = col["nombre"].lower()

        # Match exacto por substring
        if col_lower in nombre_lower:
            candidatas.append({"colonia": col, "score": 100, "tipo": "substring"})
            continue

        # Match fuzzy
        score = fuzz.partial_ratio(col_lower, nombre_lower)
        if score >= 75:
            candidatas.append({"colonia": col, "score": score, "tipo": "fuzzy"})

    # Ordenar por score desc
    candidatas.sort(key=lambda x: x["score"], reverse=True)

    # Quitar duplicados por id
    seen = set()
    unicas = []
    for c in candidatas:
        if c["colonia"]["id"] not in seen:
            seen.add(c["colonia"]["id"])
            unicas.append(c)

    if not unicas:
        return "ninguno", []
    elif len(unicas) == 1 and unicas[0]["score"] >= 85:
        return "exacto", unicas
    else:
        return "multiple", unicas[:5]
