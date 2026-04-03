import re
from rapidfuzz import fuzz

# db se importa solo cuando se necesita para evitar error en tests
# from db import obtener_colonias

# ═══════════════════════════════════════════════════════════════
# PASO 0 — DESCARTE PREVIO (sin IA, sin regex complejo)
# ═══════════════════════════════════════════════════════════════

_UI_FACEBOOK_TEXTOS = {
    "Más relevantes", "Ver más comentarios", "Por qué ves esto",
    "Compartir", "Me gusta", "Comentar", "Enviar", "Seguir",
}


def contar_imagenes(post):
    """Cuenta imágenes usando varios campos posibles del scraper."""
    if isinstance(post.get("num_imgs"), int):
        return post.get("num_imgs", 0)

    for key in ("imagenes", "images", "fotos"):
        valor = post.get(key)
        if isinstance(valor, list):
            return len(valor)

    return 0


def es_descartable(post):
    """
    Descarta antes de cualquier proceso.
    Retorna (True, razon) si se debe descartar, (False, None) si pasa.
    """
    txt = post.get("texto", "").strip()
    num_imgs = contar_imagenes(post)

    # 1. Scrambled — metadatos de FB mezclados con texto
    palabras = txt.split()
    if any(len(w) > 25 and re.search(r'\d', w) and re.search(r'[a-zA-Z]', w) for w in palabras):
        limpio = re.sub(r'\S{26,}', '', txt).strip()
        if len(limpio) < 20:
            return True, "scrambled"

    # 2. Muy corto (sin imagen tampoco)
    if len(txt) < 15 and num_imgs == 0:
        return True, "muy_corto"

    # 3. Solo un número de teléfono o basura
    if re.match(r'^[\d\s\-\.\(\)\+]{8,18}$', txt):
        return True, "solo_telefono_o_numero"

    # 4. Texto de UI de Facebook
    if txt in _UI_FACEBOOK_TEXTOS:
        return True, "ui_facebook"

    return False, None


# ═══════════════════════════════════════════════════════════════
# LIMPIEZA DE TEXTO — SIN IA
# ═══════════════════════════════════════════════════════════════

_RE_EMOJI = re.compile(
    r'[\U0001F300-\U0001F9FF'
    r'\U00002700-\U000027BF'
    r'\U0000FE00-\U0000FE0F'
    r'\u2600-\u26FF'
    r'\u2B50\u2B55\u231A\u231B'
    r'\u25AA-\u25FE'
    r'\u2614\u2615'
    r']+', flags=re.UNICODE
)
_RE_HASHTAG = re.compile(r'#\w+')
_RE_URL = re.compile(r'https?://\S+|wa\.me(?:/c)?/\S+')
_RE_SIGNOS_REP = re.compile(r'([!?])\1{1,}|\.{4,}')
_RE_ESPACIOS = re.compile(r'\s+')
_RE_MENCIONES = re.compile(r'@\[\d+:\d+:[^\]]*\]')


def limpiar_texto_regex(txt):
    """
    Limpieza completa sin IA. Maneja la mayoría de los casos del corpus.
    Orden importa: primero quitar estructuras, luego normalizar espacios.
    """
    if not txt:
        return ""

    txt = _RE_MENCIONES.sub('', txt)
    txt = _RE_URL.sub('', txt)
    txt = _RE_HASHTAG.sub('', txt)
    txt = _RE_EMOJI.sub('', txt)

    palabras = txt.split()
    txt = ' '.join(
        w for w in palabras
        if not (len(w) > 25 and re.search(r'\d', w) and re.search(r'[a-zA-Z]', w))
    )

    def _norm_signos(match):
        grp = match.group(0)
        if grp.startswith('!'):
            return '!'
        if grp.startswith('?'):
            return '?'
        return '...'

    txt = _RE_SIGNOS_REP.sub(_norm_signos, txt)
    txt = _RE_ESPACIOS.sub(' ', txt).strip()

    letras = re.findall(r'[a-zA-ZÁÉÍÓÚáéíóúÑñÜü]', txt)
    mayus = sum(1 for l in letras if l.isupper())
    if letras and mayus / len(letras) > 0.70 and len(txt) > 15:
        partes = re.split(r'(?<=[\.!?])\s+', txt)
        partes = [p.strip().capitalize() for p in partes if p.strip()]
        txt = ' '.join(partes)

    return txt


# ═══════════════════════════════════════════════════════════════
# EXTRACCIÓN DE TELÉFONO — MEJORADO
# ═══════════════════════════════════════════════════════════════

_PATRONES_TEL = [
    r'\b(\d{10})\b',
    r'\b(\d{3})[\s\-\.](\d{3})[\s\-\.](\d{4})\b',
    r'(?:\+52|52)[\s\-]?(\d{10})\b',
    r'(?:\+52|52)[\s\-]?(\d{3})[\s\-\.](\d{3})[\s\-\.](\d{4})\b',
    r'(?:cel|tel|whatsapp|wha|wa|llamar?\s*al?|marca\s*al?|al\s+num(?:ero)?)[:\s\-]*(\d[\d\s\-\.]{8,12}\d)',
    r'wa\.me(?:/c)?/(?:52)?(\d{10})',
]


def extraer_telefono(txt):
    """
    Extracción de teléfono mejorada. Retorna string de 10 dígitos o None.
    """
    if not txt:
        return None

    for pat in _PATRONES_TEL:
        m = re.search(pat, txt, re.IGNORECASE)
        if not m:
            continue

        numero = ''.join(g for g in m.groups() if g)
        numero = re.sub(r'\D', '', numero)
        if len(numero) >= 10:
            return numero[-10:]

    return None


# ═══════════════════════════════════════════════════════════════
# PRE-CLASIFICACIÓN POR KEYWORDS (antes de llamar a la IA)
# ═══════════════════════════════════════════════════════════════

_KW = {
    'negocio': {
        2: [
            'vendo', 'venta', 'se vende', 'precio', 'oferta', 'promoción', 'promocion',
            'servicio', 'domicilio', 'pedido', 'encargo', 'disponible', 'delivery',
            'envio', 'envío', 'whatsapp', 'cel', 'tel', 'pesos', 'mxn', 'repostería',
            'reposteria', 'taller', 'reparaci', 'instalaci', 'plomero', 'electricista',
            'carpintero', 'pintura', 'herreria', 'albañil', 'mantenimiento', 'cotiz',
            'renta', 'alquiler', 'instalación gratis', 'rifa'
        ],
        1: [
            'comida', 'tacos', 'pizza', 'hamburguesa', 'torta', 'sushi', 'mariscos',
            'pollo', 'carne', 'panadería', 'panaderia', 'pastel', 'refresco', 'agua',
            'café', 'cafe', 'ropa', 'calzado', 'zapato', 'accesorio', 'joyeria',
            'belleza', 'estética', 'estetica', 'uñas', 'cabello', 'maquillaje',
            'masaje', 'spa', 'gym', 'clases', 'curso', 'academia', 'asesoría',
            'asesoria', 'seguro', 'crédito', 'credito', 'préstamo', 'prestamo',
            'inmueble', 'departamento', 'casa en renta'
        ],
    },
    'alerta': {
        2: [
            'robo', 'robaron', 'ladrón', 'ladron', 'sospechoso', 'bache', 'fuga de agua',
            'accidente', 'choque', 'atropelló', 'atropello', 'herido', 'peligro',
            'alerta', 'cuidado', 'emergencia', 'sin luz', 'cable caído', 'cable caido',
            'fuga de gas', 'se metieron'
        ],
        1: ['incendio', 'inundación', 'inundacion', 'perro agresivo', 'persona sospechosa'],
    },
    'mascota': {
        3: [
            'perdí mi perro', 'perdi mi perro', 'perdí mi gato', 'perdi mi gato',
            'se escapó mi', 'se escapo mi', 'se perdió mi', 'se perdio mi',
            'en adopción', 'en adopcion', 'busca hogar', 'busco dueño', 'busco dueno',
            'encontré un perro', 'encontre un perro', 'encontré un gato', 'encontre un gato',
            'perro perdido', 'gato perdido'
        ],
        1: ['mascota', 'perro', 'gato', 'cachorro', 'gatito', 'canino', 'felino', 'adopción', 'adopcion'],
    },
    'noticia': {
        2: [
            'autoridades', 'municipio', 'alcalde', 'gobierno', 'obra pública', 'obra publica',
            'colonia informa', 'vecinos reportan', 'policía', 'policia', 'bomberos',
            'comunicado', 'protección civil', 'proteccion civil'
        ],
        1: ['reunión', 'reunion', 'asamblea', 'informan', 'reportan', 'confirman'],
    },
}

UMBRAL_KEYWORDS = 2


def pre_clasificar_keywords(txt, autor=""):
    """
    Intenta clasificar el post sin IA usando keywords ponderadas.
    Retorna: (tipo, score). Si score < umbral → ('ambiguo', 0).
    """
    texto_completo = (txt + ' ' + autor).lower()
    scores = {}

    for tipo, grupos in _KW.items():
        score = 0
        for peso, keywords in grupos.items():
            for kw in keywords:
                if kw in texto_completo:
                    score += peso
        scores[tipo] = score

    mejor_tipo = max(scores, key=scores.get)
    mejor_score = scores[mejor_tipo]

    if mejor_score >= UMBRAL_KEYWORDS:
        empatados = [t for t, s in scores.items() if s == mejor_score and s > 0]
        if len(empatados) == 1:
            return mejor_tipo, mejor_score

    return 'ambiguo', 0


# ═══════════════════════════════════════════════════════════════
# PASO 1 y 2 — LIMPIEZA Y DEDUPLICACIÓN
# ═══════════════════════════════════════════════════════════════

def paso_1_limpieza(posts):
    """
    Limpieza completa sin IA:
    1. Descarte previo (scrambled, muy corto, UI)
    2. Extracción de teléfono desde texto original y autor
    3. Limpieza de texto (regex)
    4. Pre-clasificación por keywords
    Marca cada post con: texto_limpio, telefono, pre_tipo, pre_score.
    """
    limpios = []
    descartados = []

    for post in posts:
        debe_descartar, razon = es_descartable(post)
        if debe_descartar:
            post['_descartado'] = razon
            descartados.append(post)
            continue

        txt_original = post.get("texto", "").strip()

        # Extraer teléfono ANTES de quitar URLs para no perder wa.me o formatos raros.
        tel = (
            extraer_telefono(txt_original)
            or extraer_telefono(post.get("autor", ""))
        )

        txt_limpio = limpiar_texto_regex(txt_original)
        if len(txt_limpio) < 10:
            post['_descartado'] = 'vacio_tras_limpieza'
            descartados.append(post)
            continue

        post["texto_limpio"] = txt_limpio
        post["telefono"] = tel or extraer_telefono(txt_limpio)

        pre_tipo, pre_score = pre_clasificar_keywords(txt_limpio, post.get("autor", ""))
        post["pre_tipo"] = pre_tipo
        post["pre_score"] = pre_score

        limpios.append(post)

    return limpios, descartados


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
    "noticias", "ultimas", "última", "última hora",
    "policia", "policía", "seguridad", "yucatan", "yucatán",
    "merida", "mérida", "informativo", "novedades", "prensa",
    "notificaciones", "alerta", "urgente"
]


def detectar_tipo_por_nombre(group_name):
    nombre_lower = group_name.lower()
    for kw in KEYWORDS_NOTICIAS:
        if kw in nombre_lower:
            return "noticias"
    return "vecinos"


def match_colonias(group_name):
    from db import obtener_colonias

    colonias = obtener_colonias()
    nombre_lower = group_name.lower()
    candidatas = []

    for col in colonias:
        col_lower = col["nombre"].lower()
        if col_lower in nombre_lower:
            candidatas.append({"colonia": col, "score": 100, "tipo": "substring"})
            continue
        score = fuzz.partial_ratio(col_lower, nombre_lower)
        if score >= 75:
            candidatas.append({"colonia": col, "score": score, "tipo": "fuzzy"})

    candidatas.sort(key=lambda x: x["score"], reverse=True)

    seen = set()
    unicas = []
    for c in candidatas:
        if c["colonia"]["id"] not in seen:
            seen.add(c["colonia"]["id"])
            unicas.append(c)

    if not unicas:
        return "ninguno", []
    if len(unicas) == 1 and unicas[0]["score"] >= 85:
        return "exacto", unicas
    return "multiple", unicas[:5]
