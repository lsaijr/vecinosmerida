import ast
import re
import unicodedata
from rapidfuzz import fuzz

# db se importa solo cuando se necesita para evitar errores en tests
# from db import obtener_colonias

NEWS_MIN_WORDS = 70

# ═══════════════════════════════════════════════════════════════
# DESCARTE PREVIO
# ═══════════════════════════════════════════════════════════════

def es_descartable(post):
    """
    Descarta antes de cualquier proceso pesado.
    Retorna (True, razon) si se debe descartar.
    """
    txt = (post.get("texto") or "").strip()
    num_imgs = post.get("num_imgs")
    if num_imgs is None:
        num_imgs = len(post.get("imagenes") or [])

    palabras = txt.split()
    if any(len(w) > 25 and re.search(r'\d', w) and re.search(r'[a-zA-Z]', w) for w in palabras):
        limpio = re.sub(r'\S{26,}', '', txt).strip()
        if len(limpio) < 20:
            return True, "scrambled"

    if len(txt) < 15 and num_imgs == 0:
        return True, "muy_corto"

    if re.match(r'^[\d\s\-\.\(\)\+]{8,18}$', txt):
        return True, "solo_telefono_o_numero"

    if txt in {
        "Más relevantes", "Ver más comentarios", "Por qué ves esto",
        "Compartir", "Me gusta", "Comentar"
    }:
        return True, "ui_facebook"

    return False, None


# ═══════════════════════════════════════════════════════════════
# LIMPIEZA DE TEXTO
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
_RE_SIGNOS_REP = re.compile(r'([!?])\1{1,}|\.{3,}')
_RE_ESPACIOS = re.compile(r'\s+')
_RE_MENCIONES = re.compile(r'@\[\d+:\d+:[^\]]*\]')


def limpiar_texto_regex(txt):
    if not txt:
        return ""

    txt = _RE_MENCIONES.sub('', txt)
    txt = _RE_HASHTAG.sub('', txt)
    txt = _RE_EMOJI.sub('', txt)

    palabras = txt.split()
    txt = ' '.join(
        w for w in palabras
        if not (len(w) > 25 and re.search(r'\d', w) and re.search(r'[a-zA-Z]', w))
    )

    txt = _RE_SIGNOS_REP.sub(lambda m: m.group(1) if m.group(1) else '.', txt)
    txt = _RE_ESPACIOS.sub(' ', txt).strip()

    letras = re.findall(r'[a-zA-ZÁÉÍÓÚÑáéíóúñ]', txt)
    mayus = sum(1 for l in letras if l.isupper())
    if letras and len(txt) > 15 and mayus / max(len(letras), 1) > 0.70:
        partes = [s.strip().capitalize() for s in re.split(r'(?<=[.!?])\s+|\n+', txt) if s.strip()]
        txt = ' '.join(partes)

    return txt


def remover_urls(txt):
    return _RE_URL.sub('', txt or '').strip()


def contar_palabras(txt):
    if not txt:
        return 0
    return len([w for w in re.split(r'\s+', txt.strip()) if w])


# ═══════════════════════════════════════════════════════════════
# TELÉFONO
# ═══════════════════════════════════════════════════════════════

_PATRONES_TEL = [
    r'\b(\d{10})\b',
    r'\b(\d{3})[\s\-\.](\d{3})[\s\-\.](\d{4})\b',
    r'(?:\+52|52)[\s\-]?(\d{10})\b',
    r'(?:\+52|52)[\s\-]?(\d{3})[\s\-\.](\d{3})[\s\-\.](\d{4})\b',
    r'(?:cel|tel|whatsapp|wha|llamar?\s*al?|marca\s*al?|al\s+num(?:ero)?)[:\s\-]*(\d[\d\s\-\.]{8,12}\d)',
    r'wa\.me(?:/c)?/(?:52)?(\d{10})',
]


def extraer_telefono(txt):
    if not txt:
        return None
    for pat in _PATRONES_TEL:
        m = re.search(pat, txt, re.IGNORECASE)
        if m:
            numero = ''.join(g for g in m.groups() if g)
            numero = re.sub(r'\D', '', numero)
            if len(numero) >= 10:
                return numero[-10:]
    return None


# ═══════════════════════════════════════════════════════════════
# REGLAS DE NOTICIAS
# ═══════════════════════════════════════════════════════════════

KW_BLOQUEAR_NOTICIA = [
    "ubicados en", "servicio a domicilio", "pedido", "pedidos", "menu", "menú",
    "whatsapp", "horario", "contamos con", "promo", "promoción", "precio",
    "pesos", "$", "frapp", "smoothie", "crepa", "fresas con", "mini hotcakes",
    "mini waffles", "entrega", "a domicilio", "cotiza", "costo", "reserva",
    "viernes de", "jueves de", "sabado de", "sábado de", "abrimos",
    "cerramos", "ubicada en", "ubicado en", "encargos", "encargo"
]

KW_NOTICIA_FUERTE = [
    "autoridades", "reportan", "se registra", "ocurrió", "informaron",
    "policía", "policia", "incendio", "accidente", "protección civil",
    "proteccion civil", "vecinos denuncian", "movilización", "movilizacion",
    "hechos", "comunicado", "ayuntamiento", "bomberos", "seguridad",
    "choque", "falleció", "fallecio", "detenido", "rescate"
]


def tiene_bloqueo_noticia(txt):
    t = (txt or "").lower()
    return any(kw in t for kw in KW_BLOQUEAR_NOTICIA)


def tiene_senales_noticia(txt):
    t = (txt or "").lower()
    return any(kw in t for kw in KW_NOTICIA_FUERTE)


def puede_ser_noticia_desde_json(txt):
    if contar_palabras(txt) < NEWS_MIN_WORDS:
        return False
    if tiene_bloqueo_noticia(txt):
        return False
    if not tiene_senales_noticia(txt):
        return False
    return True


# ═══════════════════════════════════════════════════════════════
# PRE-CLASIFICACIÓN POR KEYWORDS
# ═══════════════════════════════════════════════════════════════

_KW = {
    'negocio': {
        2: [
            'vendo', 'venta', 'precio', 'oferta', 'servicio', 'domicilio', 'pedido',
            'encargo', 'contamos con', 'disponible', 'delivery', 'envio', 'envío',
            'whatsapp', 'cel', 'tel', 'pesos', 'mxn', 'dlls', 'repostería', 'reposteria',
            'taller', 'reparaci', 'instalaci', 'plomero', 'electricista', 'carpintero',
            'pintura', 'herreria', 'albañil', 'mantenimiento', 'cotiz', 'ubicados en',
            'ubicado en', 'horario', 'frapp', 'smoothie', 'crepa', 'fresas con', 'menu', 'menú'
        ],
        1: [
            'comida', 'tacos', 'pizza', 'hamburguesa', 'torta', 'sushi', 'mariscos',
            'pollo', 'carne', 'panadería', 'panaderia', 'pastel', 'refresco', 'agua', 'café', 'cafe',
            'ropa', 'calzado', 'zapato', 'accesorio', 'joyeria', 'joyería', 'belleza', 'estética',
            'uñas', 'cabello', 'maquillaje', 'masaje', 'spa', 'gym', 'clases', 'curso',
            'academia', 'asesoría', 'asesoria', 'seguro', 'crédito', 'credito', 'préstamo', 'prestamo',
            'inmueble', 'renta', 'entrega'
        ],
    },
    'alerta': {
        2: [
            'robo', 'robaron', 'ladrón', 'ladron', 'sospechoso', 'bache', 'fuga de agua',
            'accidente', 'choque', 'atropelló', 'atropello', 'herido', 'peligro',
            'cuidado', 'alerta', 'auxilio', 'emergencia', 'sin luz'
        ],
        1: ['incendio', 'inundación', 'inundacion', 'poste caido', 'fuga', 'cables']
    },
    'mascota': {
        2: [
            'perdí mi perro', 'perdí mi gato', 'se escapó mi', 'se me escapó', 'se perdió mi',
            'en adopción', 'en adopcion', 'busca hogar', 'busca familia', 'dar en adopción',
            'encontré un perro', 'encontré un gato', 'perro perdido', 'gato perdido',
            'si la ves', 'responde al nombre', 'avísame', 'la entregamos'
        ],
        1: ['mascota', 'perro', 'gato', 'cachorro', 'gatito', 'canino', 'felino', 'adopción', 'adopcion']
    },
    'noticia': {
        3: ['autoridades', 'policía', 'policia', 'bomberos', 'protección civil', 'proteccion civil'],
        2: ['vecinos reportan', 'se registra', 'comunicado', 'ayuntamiento', 'movilización', 'movilizacion'],
        1: ['reportan', 'informan', 'accidente', 'incendio', 'hechos']
    },
}

UMBRAL_KEYWORDS = 2


def pre_clasificar_keywords(txt, autor="", grupo_tipo="vecinos"):
    texto_completo = ((txt or "") + ' ' + (autor or "")).lower()
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

    if mejor_tipo == 'noticia' and not puede_ser_noticia_desde_json(txt):
        return 'ambiguo', 0

    if mejor_score >= UMBRAL_KEYWORDS:
        return mejor_tipo, mejor_score

    return 'ambiguo', 0


# ═══════════════════════════════════════════════════════════════
# PASO 1: LIMPIEZA
# ═══════════════════════════════════════════════════════════════

def paso_1_limpieza(posts, grupo_tipo="vecinos"):
    limpios = []
    descartados = []

    for post in posts:
        debe_descartar, razon = es_descartable(post)
        if debe_descartar:
            post['_descartado'] = razon
            descartados.append(post)
            continue

        txt_original = (post.get("texto") or "").strip()
        txt_sin_urls = remover_urls(txt_original)
        txt_limpio = limpiar_texto_regex(txt_sin_urls)

        if len(txt_limpio) < 10:
            post['_descartado'] = 'vacio_tras_limpieza'
            descartados.append(post)
            continue

        post['texto_limpio'] = txt_limpio
        post['telefono'] = extraer_telefono(txt_original) or extraer_telefono(post.get("autor", ""))
        post['noticia_permitida'] = puede_ser_noticia_desde_json(txt_original)

        if es_post_consulta(txt_original) and not tiene_senal_comercial_fuerte(post, txt_original):
            post['_descartado'] = 'consulta_baja_prioridad'
            descartados.append(post)
            continue

        if contar_palabras(txt_limpio) < 5 and not tiene_senal_comercial_fuerte(post, txt_original):
            post['_descartado'] = 'post_demasiado_debil'
            descartados.append(post)
            continue

        pre_tipo, pre_score = pre_clasificar_keywords(txt_limpio, post.get("autor", ""), grupo_tipo=grupo_tipo)
        post['pre_tipo'] = pre_tipo
        post['pre_score'] = pre_score
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
            sim = fuzz.token_set_ratio(p1.get("texto_limpio", ""), p2.get("texto_limpio", ""))
            if sim >= 85:
                cluster.append(p2)
                usados.add(j)
        clusters.append(cluster)
    return clusters


# ═══════════════════════════════════════════════════════════════
# TÍTULOS / SEO / CLOUDINARY HELPERS
# ═══════════════════════════════════════════════════════════════

def _ascii(txt):
    txt = unicodedata.normalize('NFKD', txt or '').encode('ascii', 'ignore').decode('ascii')
    return txt


def _norm(txt):
    txt = _ascii(txt).lower()
    txt = re.sub(r'[^a-z0-9\s]', ' ', txt)
    return re.sub(r'\s+', ' ', txt).strip()


def slugify(txt, max_words=10, max_len=90):
    txt = _ascii(txt).lower()
    txt = re.sub(r'[^a-z0-9\s\-]', ' ', txt)
    txt = re.sub(r'\s+', ' ', txt).strip()
    words = txt.split()[:max_words]
    slug = '-'.join(words)
    slug = re.sub(r'-{2,}', '-', slug).strip('-')
    return slug[:max_len].strip('-') or 'imagen'


def primeras_palabras(txt, n=8):
    txt = re.sub(r'\s+', ' ', txt or '').strip()
    words = txt.split()
    return ' '.join(words[:n]).strip()


SALUDOS_TITULO = [
    'buenas noches', 'buenos dias', 'buen día', 'buen dia', 'hola', 'vecinos', 'amigos', 'amigas'
]
CONSULTA_HINTS = [
    'alguien vende', 'quien vende', 'quién vende', 'alguien sabe', 'saben donde', 'saben dónde',
    'recomienden', 'recomendacion', 'recomendación', 'sugerencias', 'donde venden', 'dónde venden',
    'donde hay', 'dónde hay', 'sopes o panuchos', 'panuchos o sopes', 'que me recomiendan',
    'qué me recomiendan', 'conocen a', 'alguna recomendacion', 'alguna recomendación'
]
COMERCIAL_FUERTE_HINTS = [
    'vendo', 'venta', 'precio', 'pesos', '$', 'whatsapp', 'telefono', 'tel', 'servicio',
    'domicilio', 'pedido', 'pedidos', 'menu', 'menú', 'horario', 'contamos con', 'ubicados en',
    'ubicado en', 'promo', 'promocion', 'promoción', 'cotiza', 'reserva', 'entrega', 'disponible'
]
STOPWORDS_TITULO = {'de', 'en', 'para', 'con', 'sin', 'por', 'y', 'a', 'al', 'del', 'la', 'el', 'los', 'las'}

NEGOCIO_TEMA_MAP = [
    (['fresas con crema', 'fresas'], 'Fresas con crema'),
    (['frappe', 'frappé', 'frappes', 'frappés'], 'Frappés'),
    (['smoothie', 'smoothies'], 'Smoothies'),
    (['crepa', 'crepas'], 'Crepas'),
    (['waffle', 'waffles'], 'Waffles'),
    (['hotcake', 'hotcakes'], 'Hotcakes'),
    (['sopes'], 'Sopes'),
    (['panuchos'], 'Panuchos'),
    (['tacos al pastor', 'pastor'], 'Tacos al pastor'),
    (['tacos'], 'Tacos'),
    (['pizza', 'pizzas'], 'Pizzas'),
    (['hamburguesa', 'hamburguesas'], 'Hamburguesas'),
    (['postre', 'postres'], 'Postres'),
    (['gomitas'], 'Gomitas'),
    (['reposteria', 'repostería', 'panes', 'pastel', 'pasteles'], 'Repostería casera'),
    (['masaje', 'masajes'], 'Masajes'),
    (['flete', 'fletes', 'mudanza', 'mudanzas'], 'Fletes y mudanzas'),
    (['clases', 'curso', 'academia', 'vacaciones'], 'Clases'),
    (['uñas', 'unas', 'cabello', 'maquillaje', 'lifting'], 'Servicios de belleza'),
    (['mueble', 'muebles', 'sala', 'colchon', 'colchón'], 'Lavado de muebles'),
    (['pintura', 'ceramica', 'cerámica', 'detallado'], 'Corrección de pintura'),
    (['bateria', 'batería', 'optima'], 'Baterías'),
    (['ropa', 'vestido', 'blusa', 'tenis', 'sandalia', 'calzado'], 'Ropa y accesorios'),
    (['envios', 'envíos', 'paqueterias', 'paqueterías', 'estafeta', 'fedex'], 'Envíos'),
]

ALERTA_TEMA_MAP = [
    (['fuga de agua', 'fuga'], 'Fuga de agua'),
    (['bache', 'baches'], 'Bache reportado'),
    (['robo', 'robaron', 'asalto'], 'Robo reportado'),
    (['choque', 'accidente'], 'Accidente reportado'),
    (['sin luz', 'cfe'], 'Falla eléctrica'),
    (['perro agresivo'], 'Perro agresivo reportado'),
]


def _dedupe_repeated_phrases(txt):
    txt = re.sub(r'\s+', ' ', txt or '').strip()
    for _ in range(3):
        nuevo = re.sub(r'(?i)\b([^,.!?]{3,60}?)\s+\1\b', r'\1', txt)
        if nuevo == txt:
            break
        txt = nuevo
    return txt


def _dedupe_consecutive_words(txt):
    words = (txt or '').split()
    out = []
    for w in words:
        nw = _norm(w)
        if out and _norm(out[-1]) == nw:
            continue
        out.append(w)
    return ' '.join(out)


def _strip_saludos(txt):
    t = re.sub(r'\s+', ' ', txt or '').strip()
    tn = _norm(t)
    for saludo in SALUDOS_TITULO:
        s = _norm(saludo)
        if tn.startswith(s):
            t = t[len(saludo):].strip(' .,:;-')
            tn = _norm(t)
    return t


def _smart_title_case(txt):
    words = (txt or '').split()
    out = []
    for i, w in enumerate(words):
        if not w:
            continue
        if i > 0 and _norm(w) in STOPWORDS_TITULO:
            out.append(w.lower())
        elif w.isupper() and len(w) <= 4:
            out.append(w)
        else:
            out.append(w[:1].upper() + w[1:].lower())
    return ' '.join(out)


def limpiar_titulo(txt, max_chars=72):
    txt = (txt or '').replace('*', ' ')
    txt = re.sub(r'[!?]{2,}', '?', txt)
    txt = re.sub(r'\.{2,}', '.', txt)
    txt = _strip_saludos(txt)
    txt = _dedupe_repeated_phrases(txt)
    txt = _dedupe_consecutive_words(txt)
    txt = re.sub(r'\s+', ' ', txt).strip(' .,:;-?')
    if len(txt) > max_chars:
        corte = txt[:max_chars].rstrip()
        if ' ' in corte:
            corte = corte.rsplit(' ', 1)[0]
        txt = corte.strip(' .,:;-?')
    return _smart_title_case(txt)


def extraer_ubicacion_simple(txt):
    t = txt or ''
    patrones = [
        r'\b(?:ubicados en|ubicado en|en|de)\s+([A-Za-zÁÉÍÓÚÑáéíóúñ0-9\-\s]{4,50})',
        r'\bcerca de\s+([A-Za-zÁÉÍÓÚÑáéíóúñ0-9\-\s]{4,50})',
    ]
    for pat in patrones:
        m = re.search(pat, t, re.IGNORECASE)
        if m:
            frag = re.split(r'[,.]|\s+y\s|\s+x\s', m.group(1))[0].strip()
            frag = re.sub(r'\s+', ' ', frag)
            frag = re.sub(r'(?i)^(la|el|los|las)\s+', '', frag)
            if len(frag) >= 4:
                return frag[:40]
    return ''


def es_post_consulta(txt):
    t = _norm(txt)
    if any(kw in t for kw in CONSULTA_HINTS):
        return True
    if ('?' in (txt or '') or '¿' in (txt or '')) and any(w in t for w in ['alguien', 'quien', 'donde', 'recomienden', 'saben']):
        return True
    return False


def tiene_senal_comercial_fuerte(post, txt):
    t = _norm(txt)
    num_imgs = post.get('num_imgs')
    if num_imgs is None:
        num_imgs = len(post.get('imagenes') or [])
    if num_imgs and num_imgs > 0:
        return True
    if post.get('url_post'):
        return True
    if extraer_telefono(txt):
        return True
    if re.search(r'\$\s?\d+|\d+\s*(pesos|mxn)', txt or '', re.IGNORECASE):
        return True
    return any(kw in t for kw in COMERCIAL_FUERTE_HINTS)




def _contains_kw(txt, kw):
    t = _norm(txt)
    k = _norm(kw)
    if not k:
        return False
    if ' ' in k:
        return k in t
    return re.search(rf'(?<!\w){re.escape(k)}(?!\w)', t) is not None


def _extraer_frase_comercial(txt):
    t = _strip_saludos(txt or '')
    t = _dedupe_repeated_phrases(t)
    t = _dedupe_consecutive_words(t)
    t = re.sub(r'[`*_#]+', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip(' .,:;!-?')
    patrones = [
        r'(?i)\b(?:a la venta|vendo|venta de|se vende[n]?|ofrezco|ofrezca)\s+(.+)',
        r'(?i)\b(?:servicio de|realizamos|contamos con|disponible)\s+(.+)',
        r'(?i)\b(?:clases de|clases)\s+(.+)',
        r'(?i)\b(?:promocion de|promoción de|promo de)\s+(.+)',
    ]
    for pat in patrones:
        m = re.search(pat, t)
        if m:
            frag = m.group(1).strip()
            frag = re.split(r'[.!?]|\s+pero\s+|\s+porque\s+|\s+para\s+', frag, 1)[0].strip()
            if len(frag.split()) >= 2:
                return frag
    frag = re.split(r'[.!?\n]', t)[0].strip()
    frag = re.sub(r'(?i)^vecinos\s+', '', frag).strip()
    return frag


def _frase_a_titulo_comercial(txt, max_words=5):
    frag = _extraer_frase_comercial(txt)
    frag = re.sub(r'(?i)\b(?:buenas noches|buenos dias|buen día|buen dia|hola|vecinos)\b', ' ', frag)
    frag = re.sub(r'[^A-Za-zÁÉÍÓÚÑáéíóúñ0-9\s/&+-]', ' ', frag)
    frag = re.sub(r'\s+', ' ', frag).strip()
    words = []
    for w in frag.split():
        wn = _norm(w)
        if not wn or wn in STOPWORDS_TITULO:
            continue
        words.append(w)
        if len(words) >= max_words:
            break
    return _smart_title_case(' '.join(words))

def inferir_tema_negocio(txt, categoria_nombre=''):
    t = _norm(txt)
    for kws, tema in NEGOCIO_TEMA_MAP:
        if any(_contains_kw(t, kw) for kw in kws):
            return tema

    frase = _frase_a_titulo_comercial(txt, max_words=5)
    if frase and len(frase.split()) >= 2:
        return frase

    cat = (categoria_nombre or '').strip()
    if cat and cat.lower() not in ['general', 'mascotas']:
        return cat
    return 'Negocio local'


def inferir_tema_alerta(txt, categoria_nombre='Alerta'):
    t = _norm(txt)
    for kws, tema in ALERTA_TEMA_MAP:
        if any(_norm(kw) in t for kw in kws):
            return tema
    cat = (categoria_nombre or '').strip()
    if cat and cat.lower() != 'alerta':
        return cat
    return 'Alerta vecinal'


def generar_titulo_negocio(post, categoria_nombre=''):
    txt = post.get('texto_limpio') or post.get('texto') or ''
    if es_post_consulta(txt):
        return ''
    ubic = extraer_ubicacion_simple(txt)
    tema = inferir_tema_negocio(txt, categoria_nombre=categoria_nombre)
    titulo = tema
    if ubic and _norm(ubic) not in _norm(titulo) and len(_norm(ubic).split()) <= 4:
        titulo = f"{titulo} en {ubic}"
    titulo = limpiar_titulo(titulo, max_chars=60)
    if titulo and titulo.lower() not in {'negocio local en merida', 'negocio local', 'general en merida'}:
        return titulo

    frase = _frase_a_titulo_comercial(txt, max_words=5)
    if frase:
        if ubic and _norm(ubic) not in _norm(frase):
            frase = f"{frase} en {ubic}"
        return limpiar_titulo(frase, max_chars=60)

    return 'Negocio en Mérida'


def generar_titulo_mascota(post, categoria_id=11):
    txt_raw = post.get('texto_limpio') or post.get('texto') or ''
    txt = txt_raw.lower()
    especie = 'Mascota'
    if any(x in txt for x in ['perro', 'perrita', 'perrito', 'cachorro']):
        especie = 'Perro'
    elif any(x in txt for x in ['gata', 'gato', 'gatita']):
        especie = 'Gato'

    subtipo = {14: 'perdido', 15: 'encontrado', 16: 'en adopción'}.get(categoria_id, 'reportado')
    if subtipo == 'reportado':
        if any(x in txt for x in ['si la ves', 'si lo ves', 'si la ven', 'si lo ven', 'responde al nombre', 'avísame', 'avisame']):
            subtipo = 'perdido'
        elif any(x in txt for x in ['la entregamos', 'dar en adopción', 'dar en adopcion', 'busca familia', 'necesita hogar', 'vacunada', 'bañadita', 'banadita']):
            subtipo = 'en adopción'
    ubic = extraer_ubicacion_simple(txt_raw)
    titulo = f"{especie} {subtipo}"
    if ubic:
        titulo += f" en {ubic}"
    elif subtipo == 'en adopción':
        titulo += ' en Mérida'
    return limpiar_titulo(titulo, max_chars=62) or 'Mascota reportada'


def generar_titulo_alerta(post, categoria_nombre='Alerta', cat_nombre=None):
    if cat_nombre and (not categoria_nombre or categoria_nombre == 'Alerta'):
        categoria_nombre = cat_nombre
    txt = post.get('texto_alerta') or post.get('texto_limpio') or post.get('texto') or ''
    ubic = extraer_ubicacion_simple(txt)
    tema = inferir_tema_alerta(txt, categoria_nombre=categoria_nombre)
    titulo = tema
    if ubic and _norm(ubic) not in _norm(titulo):
        titulo = f"{titulo} en {ubic}"
    return limpiar_titulo(titulo, max_chars=62) or 'Alerta vecinal'


def generar_titulo_noticia_fallback(post):
    txt = post.get('texto_limpio') or post.get('texto') or ''
    primera = re.split(r'[.!?\n]', txt)[0].strip()
    base = primera or primeras_palabras(txt, 12) or 'Noticia local en Mérida'
    return limpiar_titulo(base, max_chars=88) or 'Noticia local en Mérida'


def generar_alt_imagen(post, config_grupo=None):
    tipo = post.get('tipo') or post.get('_tipo_final') or post.get('tipo_detectado') or 'general'
    txt = post.get('texto_limpio') or post.get('texto') or post.get('descripcion') or post.get('texto_alerta') or ''
    ubic = extraer_ubicacion_simple(txt)

    if tipo == 'mascota':
        titulo = generar_titulo_mascota(post, post.get('categoria_id', 11))
        alt = f"Imagen de {titulo.lower()}"
    elif tipo == 'alerta':
        alt = "Imagen de alerta vecinal"
        if ubic:
            alt += f" en {ubic}"
    elif tipo == 'noticia':
        titulo = post.get('titulo') or generar_titulo_noticia_fallback(post)
        alt = f"Imagen relacionada con {titulo.lower()}"
    else:
        titulo = post.get('titulo') or generar_titulo_negocio(post, categoria_nombre='')
        alt = f"Imagen de {titulo.lower()}"
    return limpiar_titulo(alt, max_chars=125)


def construir_public_id(post, img, meta=None, config_grupo=None, idx=0):
    tipo = post.get('tipo') or post.get('_tipo_final') or post.get('tipo_detectado') or 'general'
    txt = post.get('texto_limpio') or post.get('texto') or post.get('descripcion') or post.get('texto_alerta') or ''
    tema = slugify(post.get('titulo') or generar_titulo_negocio(post, categoria_nombre=''), max_words=8, max_len=55)
    ciudad = slugify((meta or {}).get('city') or 'merida', max_words=3, max_len=20)
    estado = slugify((meta or {}).get('state') or 'yucatan', max_words=3, max_len=20)
    zona = slugify(extraer_ubicacion_simple(txt) or 'general', max_words=5, max_len=30)
    img_id = None
    if isinstance(img, dict):
        img_id = img.get('fbid') or img.get('id')
    img_id = img_id or post.get('fbid_post') or f"{idx+1}"
    return slugify(f"{tema} {tipo} {ciudad} {estado} {zona} {img_id}", max_words=20, max_len=120)


def parse_keywords(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(x).strip().lower() for x in value if str(x).strip()]
    if isinstance(value, str):
        txt = value.strip()
        if not txt:
            return []
        try:
            parsed = ast.literal_eval(txt)
            if isinstance(parsed, (list, tuple)):
                return [str(x).strip().lower() for x in parsed if str(x).strip()]
        except Exception:
            pass
        if '|' in txt:
            parts = txt.split('|')
        elif ',' in txt:
            parts = txt.split(',')
        else:
            parts = [txt]
        return [p.strip().lower() for p in parts if p.strip()]
    return []


# ─── MATCH DE COLONIAS ───────────────────────────────────────

KEYWORDS_NOTICIAS = [
    'noticias', 'ultimas', 'última', 'última hora', 'policia', 'policía',
    'seguridad', 'yucatan', 'yucatán', 'merida', 'mérida', 'informativo',
    'novedades', 'prensa', 'notificaciones', 'alerta', 'urgente'
]


def detectar_tipo_por_nombre(group_name):
    nombre_lower = (group_name or '').lower()
    for kw in KEYWORDS_NOTICIAS:
        if kw in nombre_lower:
            return 'noticias'
    return 'vecinos'


def match_colonias(group_name):
    from db import obtener_colonias
    colonias = obtener_colonias()
    nombre_lower = (group_name or '').lower()
    candidatas = []

    for col in colonias:
        col_lower = col['nombre'].lower()
        if col_lower in nombre_lower:
            candidatas.append({'colonia': col, 'score': 100, 'tipo': 'substring'})
            continue
        score = fuzz.partial_ratio(col_lower, nombre_lower)
        if score >= 75:
            candidatas.append({'colonia': col, 'score': score, 'tipo': 'fuzzy'})

    candidatas.sort(key=lambda x: x['score'], reverse=True)

    seen = set()
    unicas = []
    for c in candidatas:
        if c['colonia']['id'] not in seen:
            seen.add(c['colonia']['id'])
            unicas.append(c)

    if not unicas:
        return 'ninguno', []
    if len(unicas) == 1 and unicas[0]['score'] >= 85:
        return 'exacto', unicas
    return 'multiple', unicas[:5]
