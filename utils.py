import ast
import re
import unicodedata
from rapidfuzz import fuzz

# db se importa solo cuando se necesita para evitar errores en tests
# from db import obtener_colonias

NEWS_MIN_WORDS = 70

# ═══════════════════════════════════════════════════════════════
# CONFIGURACIÓN POR TIPO DE GRUPO
# Calibrada con datos reales de 5 grupos de Mérida (abril 2026)
# ═══════════════════════════════════════════════════════════════

CONFIG_GRUPO = {
    "vecinos": {
        "tipos_permitidos":    ["noticia", "negocio", "mascota", "alerta", "empleo", "perdido"],
        "tipos_prioritarios":  ["negocio", "alerta"],
        "min_palabras":        3,
        "min_palabras_noticia": 70,
        "min_palabras_mascota": 15,
        "requiere_imagen":     ["negocio", "alerta"],
        "no_requiere_imagen":  ["noticia", "mascota", "perdido"],
        "largo_es_negocio":    True,
        "validar_noticia_ia":  True,
        "filtrar_geo_externa": False,
    },
    "noticias": {
        "tipos_permitidos":    ["noticia", "alerta", "mascota", "negocio"],
        "tipos_prioritarios":  ["noticia"],
        "min_palabras":        5,
        "min_palabras_noticia": 70,
        "min_palabras_mascota": 15,
        "requiere_imagen":     [],
        "no_requiere_imagen":  ["noticia", "alerta"],
        "largo_es_negocio":    False,
        "validar_noticia_ia":  True,
        "filtrar_geo_externa": True,
    },
    "mascotas": {
        "tipos_permitidos":    ["mascota", "alerta", "negocio"],
        "tipos_prioritarios":  ["mascota"],
        "min_palabras":        7,
        "min_palabras_noticia": 9999,
        "min_palabras_mascota": 15,
        "requiere_imagen":     [],
        "no_requiere_imagen":  ["mascota", "alerta"],
        "largo_es_negocio":    False,
        "validar_noticia_ia":  False,
        "filtrar_geo_externa": False,
    },
    "negocios": {
        "tipos_permitidos":    ["negocio", "mascota"],
        "tipos_prioritarios":  ["negocio"],
        "min_palabras":        3,
        "min_palabras_noticia": 9999,
        "min_palabras_mascota": 15,
        "requiere_imagen":     ["negocio"],
        "no_requiere_imagen":  ["mascota"],
        "largo_es_negocio":    True,
        "validar_noticia_ia":  False,
        "filtrar_geo_externa": False,
    },
    "empleo": {
        "tipos_permitidos":    ["empleo"],
        "tipos_prioritarios":  ["empleo"],
        "min_palabras":        5,
        "min_palabras_noticia": 9999,
        "min_palabras_mascota": 9999,
        "requiere_imagen":     [],
        "no_requiere_imagen":  ["empleo"],
        "largo_es_negocio":    False,
        "validar_noticia_ia":  False,
        "filtrar_geo_externa": False,
    },
    "perdidos": {
        "tipos_permitidos":    ["perdido", "mascota", "negocio", "ignorar"],
        "tipos_prioritarios":  ["perdido"],
        "min_palabras":        5,
        "min_palabras_noticia": 9999,
        "min_palabras_mascota": 15,
        "requiere_imagen":     [],
        "no_requiere_imagen":  ["perdido", "mascota"],
        "largo_es_negocio":    False,
        "validar_noticia_ia":  False,
        "filtrar_geo_externa": False,
    },
}


def get_config_grupo(grupo_tipo):
    """Retorna la config del grupo, con fallback a 'vecinos'."""
    return CONFIG_GRUPO.get(grupo_tipo or "vecinos", CONFIG_GRUPO["vecinos"])


def tipo_permitido_en_grupo(tipo, grupo_tipo):
    """Verifica si un tipo de contenido está permitido en este grupo."""
    cfg = get_config_grupo(grupo_tipo)
    return tipo in cfg["tipos_permitidos"]


def requiere_imagen_en_grupo(tipo, grupo_tipo):
    """Verifica si este tipo requiere imagen en este grupo."""
    cfg = get_config_grupo(grupo_tipo)
    return tipo in cfg.get("requiere_imagen", [])


# ═══════════════════════════════════════════════════════════════
# FILTRO GEOGRÁFICO — URGENTE / DIFUNDAMOS
# Distingue alertas locales de virales externos
# ═══════════════════════════════════════════════════════════════

_SIGNALS_URGENTE = [
    'difundamos', 'difunde', 'urgente', 'comparte esto', '🆘', 'sos ',
    'necesito su ayuda', 'ayúdanos a compartir', 'ayudanos a compartir',
]

_GEO_YUCATAN = [
    'mérida', 'merida', 'yucatán', 'yucatan', 'colonia', 'fraccionamiento',
    'comisaría', 'comisaria', 'ticul', 'progreso', 'valladolid', 'izamal',
    'motul', 'umán', 'uman', 'kanasín', 'kanasin', 'celestún', 'celestun',
    'telchac', 'hunucmá', 'hunucma', 'tizimín', 'tizimin', 'oxkutzcab',
    'tekax', 'maxcanú', 'maxcanu', 'dzilam', 'sisal', 'chicxulub',
    'municipio de', 'ayuntamiento de', 'sspe', 'seye', 'conkal',
    # Sureste relevante para audiencia yucateca:
    'cancún', 'cancun', 'quintana roo', 'campeche', 'chetumal',
    'playa del carmen', 'tulum', 'holbox', 'isla mujeres',
]

_GEO_EXTERNA = [
    'california', 'estados unidos', 'eeuu', 'cdmx', 'ciudad de mexico',
    'guadalajara', 'monterrey', 'nuevo leon', 'jalisco', 'puebla',
    'veracruz', 'chiapas', 'oaxaca', 'tabasco', 'totonacapan',
    'huimilpan', 'coahuila', 'ahmsa', 'tamaulipas', 'sinaloa',
    'sonora', 'tijuana', 'juarez', 'acapulco', 'mazatlan',
    'torreon', 'torreón', 'saltillo', 'indonesia', 'tailandia',
    'irán', 'iran', 'israel', 'ucrania', 'rusia', 'cuba',
]


def clasificar_urgente_geo(texto):
    """
    Para posts con señales de urgente/difundamos determina si son:
    - 'alerta': tienen geo local Yucatán → pipeline de alertas
    - 'ignorar': tienen geo externa → descartar sin IA
    - 'ambiguo': sin geo clara → mandar a IA
    - None: el post no tiene señales de urgente
    """
    t = (texto or '').lower()
    if not any(s in t for s in _SIGNALS_URGENTE):
        return None

    tiene_local   = any(s in t for s in _GEO_YUCATAN)
    tiene_externa = any(s in t for s in _GEO_EXTERNA)

    if tiene_local and not tiene_externa:
        return 'alerta'
    if tiene_externa:
        return 'ignorar'
    return 'ambiguo'


def es_noticia_geograficamente_valida(texto):
    """
    Para grupos de noticias: verifica que la noticia sea de Yucatán.
    Retorna False si detecta geografía externa sin mención local.
    """
    t = (texto or '').lower()
    tiene_local   = any(s in t for s in _GEO_YUCATAN)
    tiene_externa = any(s in t for s in _GEO_EXTERNA)

    if tiene_externa and not tiene_local:
        return False
    return True

# ═══════════════════════════════════════════════════════════════
# DESCARTE PREVIO
# ═══════════════════════════════════════════════════════════════

# Patrones de spam / contenido no publicable
_RE_TIKTOK    = re.compile(r'tiktok\.com|vm\.tiktok|tiktok para descubrir|entra a tiktok', re.IGNORECASE)
_RE_LIVE      = re.compile(r'transmisi[oó]n en directo|en vivo ahora|live\s+ahora|unirte a la transmisi[oó]n', re.IGNORECASE)
_RE_SOLO_TEL  = re.compile(r'^[\d\s\-\.\(\)\+]{8,18}$')
_RE_SCRAMBLE  = re.compile(r'\S{26,}')

_UI_FB = {
    "Más relevantes", "Ver más comentarios", "Por qué ves esto",
    "Compartir", "Me gusta", "Comentar"
}


def es_descartable(post):
    """
    Descarta antes de cualquier proceso pesado.
    Retorna (True, razon) si se debe descartar.
    """
    txt = (post.get("texto") or "").strip()
    num_imgs = post.get("num_imgs")
    if num_imgs is None:
        num_imgs = len(post.get("imagenes") or [])

    # Texto scrambled (tokens hash-like sin sentido)
    palabras = txt.split()
    if any(len(w) > 25 and re.search(r'\d', w) and re.search(r'[a-zA-Z]', w) for w in palabras):
        limpio = _RE_SCRAMBLE.sub('', txt).strip()
        if len(limpio) < 20:
            return True, "scrambled"

    # Muy corto y sin imagen
    if len(txt) < 15 and num_imgs == 0:
        return True, "muy_corto"

    # Solo número de teléfono
    if _RE_SOLO_TEL.match(txt):
        return True, "solo_telefono_o_numero"

    # UI residual de Facebook
    if txt in _UI_FB:
        return True, "ui_facebook"

    # NUEVO: TikTok links / promoción de perfil externo
    if _RE_TIKTOK.search(txt):
        return True, "spam_tiktok"

    # NUEVO: Transmisiones en vivo (no son negocios ni noticias)
    if _RE_LIVE.search(txt):
        return True, "live_stream"

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
_RE_HASHTAG    = re.compile(r'#\w+')
_RE_URL        = re.compile(r'https?://\S+|wa\.me(?:/c)?/\S+')
_RE_SIGNOS_REP = re.compile(r'([!?])\1{1,}|\.{3,}')
_RE_ESPACIOS   = re.compile(r'\s+')
_RE_MENCIONES  = re.compile(r'@\[\d+:\d+:[^\]]*\]')
# Caracteres Unicode decorativos (bold/italic Facebook)
_RE_UNICODE_DECO = re.compile(
    r'[\U0001D400-\U0001D7FF'   # Mathematical Alphanumeric Symbols
    r'\U0001F100-\U0001F1FF'    # Enclosed Alphanumeric Supplement
    r']+', flags=re.UNICODE
)


def limpiar_texto_regex(txt):
    if not txt:
        return ""

    txt = _RE_MENCIONES.sub('', txt)
    txt = _RE_HASHTAG.sub('', txt)
    txt = _RE_EMOJI.sub('', txt)
    # NUEVO: quitar caracteres decorativos Unicode (negrita FB, etc.)
    txt = _RE_UNICODE_DECO.sub('', txt)

    palabras = txt.split()
    txt = ' '.join(
        w for w in palabras
        if not (len(w) > 25 and re.search(r'\d', w) and re.search(r'[a-zA-Z]', w))
    )

    txt = _RE_SIGNOS_REP.sub(lambda m: m.group(1) if m.group(1) else '.', txt)
    txt = _RE_ESPACIOS.sub(' ', txt).strip()

    def _normalizar_mayusculas(txt):
        """
        Normaliza mayúsculas a sentence case, oración por oración.
        Aplica a cualquier oración/segmento con >35% de letras mayúsculas.
        Texto normal (<=35%) no se toca.
        """
        def _sc(seg):
            seg = seg.strip()
            if not seg or len(seg) < 4:
                return seg
            lets = re.findall(r'[a-zA-ZÁÉÍÓÚÑáéíóúñ]', seg)
            if not lets:
                return seg
            pct = sum(1 for l in lets if l.isupper()) / max(len(lets), 1)
            if pct <= 0.35:
                return seg
            return seg[0].upper() + seg[1:].lower()

        lineas = txt.split('\n')
        resultado = []
        for linea in lineas:
            partes = re.split(r'(?<=[.!?¡])\s+', linea)
            resultado.append(' '.join(_sc(p) for p in partes))
        return '\n'.join(resultado)

    txt = _normalizar_mayusculas(txt)
    return txt


def remover_urls(txt):
    return _RE_URL.sub('', txt or '').strip()


def contar_palabras(txt):
    if not txt:
        return 0
    return len([w for w in re.split(r'\s+', txt.strip()) if w])


def contar_palabras_contenido(txt):
    """
    Cuenta solo palabras con contenido real:
    excluye números de teléfono, números solos, URLs, y palabras
    de acción sin sustancia ('inbox', 'whatsapp', 'info').
    """
    if not txt:
        return 0
    RUIDO = {'inbox', 'whatsapp', 'info', 'tel', 'cel', 'num', 'numero',
             'numero', 'al', 'llamar', 'llama', 'escribe', 'mensaje',
             'escribeme', 'contacto'}
    palabras = re.split(r'\s+', txt.strip())
    reales = []
    for w in palabras:
        if not w:
            continue
        # Solo dígitos o solo símbolos → ruido
        if re.match(r'^[\d\s\-\.\(\)\+]+$', w):
            continue
        # Palabra de 10 dígitos (teléfono) → ruido
        if re.match(r'^\d{10}$', re.sub(r'\D', '', w)) and len(re.sub(r'\D', '', w)) == 10:
            continue
        if w.lower() in RUIDO:
            continue
        reales.append(w)
    return len(reales)


# ═══════════════════════════════════════════════════════════════
# TELÉFONO
# ═══════════════════════════════════════════════════════════════

_PATRONES_TEL = [
    r'\b(\d{10})\b',
    r'\b(\d{3})[\s\-\.](\d{3})[\s\-\.](\d{4})\b',
    r'(?:\+52|52)[\s\-]?(\d{10})\b',
    r'(?:\+52|52)[\s\-]?(\d{3})[\s\-\.](\d{3})[\s\-\.](\d{4})\b',
    r'(?:cel|tel|whatsapp|wha|llamar?\s*al?|marca\s*al?|al\s+num(?:ero)?)[\:\s\-]*(\d[\d\s\-\.]{8,12}\d)',
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


_ANTI_NOTICIA_FUERTE = [
    'alguien sabe', 'me recomienda', 'dónde puedo', 'donde puedo',
    'me pueden recomendar', 'que me recomiendan', 'qué me recomiendan',
    'mi mamá', 'mi papá', 'mi familia', 'mi esposo', 'mi esposa',
    'cumpleaños', 'felicidades', 'gracias a todos', 'los quiero',
    'precios:', 'lista de precios', 'nuestros precios',
]


def puede_ser_noticia_desde_json(txt, min_palabras=None, grupo_tipo="vecinos"):
    """
    Verifica si un post puede ser noticia publicable.
    Aplica umbral configurable según tipo de grupo.
    """
    umbral = min_palabras or NEWS_MIN_WORDS
    if contar_palabras(txt) < umbral:
        return False
    if tiene_bloqueo_noticia(txt):
        return False
    # Pre-filtro anti-noticia: 2+ señales fuertes → no es noticia
    t = (txt or '').lower()
    if sum(1 for s in _ANTI_NOTICIA_FUERTE if s in t) >= 2:
        return False
    # En grupos de noticias aplicar filtro geográfico
    if grupo_tipo == "noticias" and not es_noticia_geograficamente_valida(txt):
        return False
    if not tiene_senales_noticia(txt):
        return False
    return True


# ═══════════════════════════════════════════════════════════════
# PRE-CLASIFICACIÓN POR KEYWORDS
# ═══════════════════════════════════════════════════════════════

_KW = {
    'negocio': {
        2: ['vendo','venta','precio','oferta','servicio','domicilio','pedido','encargo',
            'contamos con','disponible','delivery','envio','envío','whatsapp','cotiz',
            'ubicados en','ubicado en','horario','tenemos','pedidos','ingredientes',
            'estamos laborando','ven a visitarnos','nos ubicamos','sucursal',
            'hacemos','realizamos','ofrecemos','apartado','aparta el tuyo',
            'pesos','mxn','repostería','reposteria','taller','reparaci','instalaci',
            'plomero','electricista','carpintero','herreria','albañil',
            'frapp','smoothie','crepa','fresas con','menu','menú'],
        1: ['comida','tacos','pizza','hamburguesa','torta','sushi','mariscos','pollo','carne',
            'panadería','panaderia','pastel','café','cafe','ropa','calzado','zapato',
            'accesorio','joyeria','joyería','belleza','estética','uñas','cabello',
            'maquillaje','masaje','spa','gym','clases','curso','academia',
            'seguro','crédito','credito','préstamo','prestamo','inmueble','renta','entrega',
            'birria','burger','hot dog','pierna','pastor','tamales','pozole',
            'mochila','bolsa','vestido','blusa','pantalon','zapatos',
            'instalacion','mantenimiento','reparacion','fumigacion',
            'remato','entrega inmediata','modelos disponibles','keratina',
            'alaciado','permanente','corte','tinte','tratamiento','limpieza',
            'lavado','piscina','alberca','brincolines','inflable',
            'lentes','armazones','internet','fibra óptica','fibra optica',
            'megas','totalplay','pisos','azulejo','loseta','cemento',
            'block','varilla','material','construcción','construccion',
            'nueva colección','nueva coleccion','temporada','catálogo','catalogo'],
    },
    'alerta': {
        2: ['robo','robaron','ladrón','ladron','sospechoso','bache','fuga de agua','accidente',
            'choque','atropelló','atropello','herido','peligro','cuidado','alerta','auxilio',
            'emergencia','sin luz','maltrato','estafa','fraude','depósito','deposito',
            'timo','engaño','invasores','despojo','amenaza','violencia'],
        1: ['incendio','inundación','inundacion','poste caido','fuga','cables','denuncia'],
    },
    'mascota': {
        2: ['perdí mi perro','perdí mi gato','se escapó mi','se me escapó','se perdió mi',
            'en adopción','en adopcion','busca hogar','busca familia','dar en adopción',
            'encontré un perro','encontré un gato','perro perdido','gato perdido',
            'si la ves','responde al nombre','avísame','la entregamos'],
        1: ['mascota','perro','gato','cachorro','gatito','canino','felino','adopción','adopcion',
            'perrito','perrita','gatitos','rescate','hogar temporal','esteriliz',
            'desparasit','collar','correa','veterinari','abandonado','abandonada'],
    },
    'noticia': {
        3: ['autoridades','policía','policia','bomberos','protección civil','proteccion civil'],
        2: ['vecinos reportan','se registra','comunicado','ayuntamiento','movilización','movilizacion',
            'gobierno','secretaría','secretaria','programa','municipio','detenido',
            'operativo','volcadura','fallecio','falleció'],
        1: ['reportan','informan','hechos','seguridad'],
    },
    'empleo': {
        2: ['se solicita','se solicitan','solicitamos','estamos contratando','buscamos personal',
            'vacante','vacantes','contratación inmediata','únete a nuestro equipo',
            'estamos buscando','envía tu cv','manda tu cv','pago semanal','pago quincenal',
            'prestaciones','solicito personal','contratando','busco trabajo','busco empleo',
            'ofrezco mis servicios','busca empleo','en búsqueda de empleo'],
        1: ['sueldo','requisitos','experiencia','turno','interesados','curriculum',
            'tiempo completo','medio tiempo','plaza','puesto'],
    },
    'perdido': {
        3: ['se me perdió','se me perdio','perdí mi','perdi mi','se me extravió','se me extravio',
            'encontré un','encontre un','encontré una','encontre una','encontré este','encontre este',
            'encontré esta','encontre esta'],
        2: ['se perdió','se perdio','extravié','extravie','se me cayó','se me cayo',
            'se extraviaron','se extravió','se extravio','perdí','perdi','encontré','encontre',
            'si alguien encontró','si alguien encontro','alguien perdió','alguien perdio',
            'se le cayó','se le cayo','recompensa','gratificación','gratificacion'],
        1: ['perdido','perdida','extraviado','extraviada','encontrado','encontrada',
            'ine','credencial','identificación','identificacion','cartera','billetera',
            'llaves','llave','llavero','celular','iphone','samsung','mochila','bolsa',
            'pasaporte','licencia de conducir','placa','bicicleta'],
    },
}

UMBRAL_KEYWORDS = 2


# ── Detección de objetos perdidos/encontrados ─────────────────

_PERDIDO_CATEGORIAS = {
    'documento':    ['ine','credencial','identificación','identificacion','pasaporte','licencia',
                     'acta de nacimiento','curp','visa','tarjeta de circulación','tarjeta de circulacion',
                     'cartilla','titulo','cédula','cedula','constancia','certificado'],
    'electronico':  ['celular','iphone','samsung','xiaomi','motorola','teléfono','telefono','tablet',
                     'laptop','computadora','airpods','audífonos','audifonos','cámara','camara',
                     'reloj','smartwatch','bocina'],
    'llaves':       ['llaves','llave','llavero'],
    'cartera_bolsa':['cartera','billetera','bolsa','mochila','maletín','maletin','portafolio',
                     'riñonera','monedero','cangurera'],
    'vehiculo':     ['bicicleta','bici','moto','motocicleta','carro','auto','camioneta','placa',
                     'placas','patín','patin','scooter'],
    'mascota':      ['perro','perra','gato','gata','perrit','gatit','mascota','cachorro'],
    'lentes':       ['lentes','gafas','anteojos'],
    'ropa':         ['chamarra','sudadera','sombrero','gorra','zapato','tenis','suéter','sueter'],
}


def detectar_categoria_perdido(texto):
    t = (texto or '').lower()
    for cat, kws in _PERDIDO_CATEGORIAS.items():
        if any(k in t for k in kws):
            return cat
    return 'otro'


def detectar_estado_perdido(texto):
    t = (texto or '').lower()
    if any(k in t for k in ['encontré','encontre','encontramos','hallé','halle','apareció',
                              'aparecio','encontrado','encontrada']):
        return 'encontrado'
    if any(k in t for k in ['perdí','perdi','se perdió','se perdio','extravié','extravie',
                              'se me cayó','se me cayo','se me perdió','se me perdio','robaron',
                              'robó','robo mi','me robaron','perdido','perdida','extraviado',
                              'extraviada']):
        return 'perdido'
    return None


def detectar_recompensa(texto):
    return bool(re.search(r'recompensa|gratificaci[oó]n|reward|\$\s*\d+.*recompensa', texto or '', re.IGNORECASE))


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

    # Prioridad por tipo de grupo
    if grupo_tipo == 'empleo' and scores.get('empleo', 0) >= UMBRAL_KEYWORDS:
        return 'empleo', scores['empleo']
    if grupo_tipo == 'perdidos' and scores.get('perdido', 0) >= UMBRAL_KEYWORDS:
        return 'perdido', scores['perdido']

    if mejor_score >= UMBRAL_KEYWORDS:
        return mejor_tipo, mejor_score

    return 'ambiguo', 0
# ═══════════════════════════════════════════════════════════════

OFERTA_KW = [
    'se solicita', 'se solicitan', 'solicitamos', 'estamos contratando',
    'buscamos personal', 'vacante', 'vacantes', 'contratación inmediata',
    'únete a nuestro equipo', 'estamos buscando', 'requisitos', 'sueldo',
    'interesados presentarse', 'interesados comunicarse', 'envía tu cv',
    'manda tu cv', 'se busca', 'urge', 'pago semanal', 'pago quincenal',
    'prestaciones', 'solicito personal', 'solicita personal', 'contratando',
    'contrata', 'reclutamiento', 'talento', 'solicito', 'buscamos',
    'están solicitando', 'están contratando', 'no se requiere experiencia',
]

BUSQUEDA_KW = [
    'busco trabajo', 'busco empleo', 'ando en busca', 'estoy buscando trabajo',
    'estoy buscando empleo', 'algún trabajo', 'algún empleo', 'algun trabajo',
    'algun empleo', 'busco por las', 'necesito trabajo', 'ofrezco mis servicios',
    'en búsqueda de empleo', 'en busca de empleo', 'busco oportunidad',
    'ando buscando', 'busco por el rumbo',
]

EMPLEO_AREAS = {
    'Cocina':          {
        'icon': '🍳', 'color': '#f97316',
        'kws': ['cocina', 'cocinero', 'cocinera', 'chef', 'cocinar', 'lavaloza',
                'lava loza', 'mesero', 'mesera', 'bartender', 'ayudante de cocina',
                'panadero', 'repostería', 'cafetería', 'barista'],
    },
    'Ventas':          {
        'icon': '💼', 'color': '#10b981',
        'kws': ['ventas', 'vendedor', 'vendedora', 'cajero', 'cajera',
                'atención al cliente', 'servicio al cliente', 'promotor',
                'asesor de crédito', 'asesor comercial', 'ejecutivo de ventas',
                'módulo de crédito', 'tarjeta de crédito'],
    },
    'Seguridad':       {
        'icon': '🛡', 'color': '#3b82f6',
        'kws': ['seguridad', 'guardia', 'vigilante', 'velador', 'custodi',
                'protección activos', 'oficial de seguridad'],
    },
    'Transporte':      {
        'icon': '🚗', 'color': '#6366f1',
        'kws': ['chofer', 'chófer', 'conductor', 'operador', 'repartidor',
                'mensajero', 'cargador', 'montacarguista', 'paquetería',
                'reparto', 'ayudante de chofer', 'estibador', 'logística'],
    },
    'Administrativo':  {
        'icon': '📋', 'color': '#8b5cf6',
        'kws': ['administrativo', 'asistente', 'recepcionista', 'secretaria',
                'recursos humanos', 'contabilidad', 'contador', 'facturación',
                'auxiliar contable', 'digitalizador'],
    },
    'Limpieza':        {
        'icon': '🧹', 'color': '#06b6d4',
        'kws': ['limpieza', 'intendencia', 'mantenimiento', 'plomero',
                'electricista', 'pintor', 'jardinero', 'carpintero',
                'mozo', 'camarista', 'lavador', 'pulidor'],
    },
    'Construcción':    {
        'icon': '🏗', 'color': '#78716c',
        'kws': ['construcción', 'obra', 'albañil', 'soldador', 'instalador',
                'panel solar', 'herrería', 'llantera', 'llantero'],
    },
    'Salud':           {
        'icon': '🏥', 'color': '#ef4444',
        'kws': ['enfermera', 'enfermero', 'médico', 'doctor', 'psicólogo',
                'terapeuta', 'farmacia', 'clínica', 'hospital', 'veterinaria'],
    },
    'Tecnología':      {
        'icon': '💻', 'color': '#0ea5e9',
        'kws': ['programador', 'desarrollador', 'sistemas', 'software',
                'soporte técnico', 'analista de datos', 'social media',
                'diseño web', 'control de plagas'],
    },
    'Educación':       {
        'icon': '📚', 'color': '#7c3aed',
        'kws': ['maestro', 'maestra', 'profesor', 'profesora', 'docente',
                'tutor', 'guardería', 'preescolar', 'asistente educativo'],
    },
    'Almacén':         {
        'icon': '📦', 'color': '#f59e0b',
        'kws': ['almacén', 'almacen', 'bodega', 'surtidor', 'inventario',
                'ferreter', 'auxiliar de tienda', 'gomart', 'soriana',
                'encargado de tienda'],
    },
}

EMPLEO_HORARIO_KW = {
    'Tiempo completo': ['tiempo completo', '8 horas', 'jornada completa',
                        'lunes a viernes', 'lunes a sábado', 'lunes a sabado'],
    'Medio tiempo':    ['medio tiempo', '4 horas', 'part time'],
    'Fin de semana':   ['fin de semana', 'sábado y domingo', 'fines de semana'],
    'Nocturno':        ['turno nocturno', 'nocturno', 'noche'],
    'Vespertino':      ['turno vespertino', 'vespertino'],
    'Matutino':        ['turno matutino', 'matutino'],
    'Por día':         ['por día', 'por dia', 'días de juego'],
}

EMPLEO_ZONA_KW = [
    'zona norte', 'zona sur', 'zona oriente', 'zona poniente', 'zona centro',
    'francisco de montejo', 'fco. de montejo', 'altabrisa', 'pensiones',
    'las américas', 'las americas', 'caucel', 'chuburna', 'vergeles', 'mulsay',
    'kanasín', 'kanasin', 'polígono 108', 'ciudad caucel', 'cordemex',
    'galerías mérida', 'xcumpich', 'dzityá',
]

EMPLEO_SIGLAS = {
    'IMSS', 'INFONAVIT', 'FONACOT', 'RFC', 'INE', 'CURP', 'IFE', 'SAT',
    'CV', 'RH', 'CDMX', 'ISO', 'ISSSTE', 'IVA', 'AM', 'PM',
}

EMPLEO_SECCIONES = [
    'Requisitos', 'Ofrecemos', 'Horario', 'Zona', 'Ubicación', 'Ubicacion',
    'Sueldo', 'Pago', 'Actividades', 'Funciones', 'Beneficios', 'Contacto',
    'Interesados', 'Lo que ofrecemos', 'Lo que buscamos', 'Prestaciones',
    'Nota', 'Buscamos',
]


def get_empleo_area(txt):
    """Detecta el área laboral de un post de empleo por keywords."""
    t = (txt or '').lower()
    for area, cfg in EMPLEO_AREAS.items():
        if any(kw in t for kw in cfg['kws']):
            return area, cfg['icon'], cfg['color']
    return 'General', '💼', '#6b7280'


def extraer_horario_empleo(txt):
    """Extrae tipo de horario de un post de empleo."""
    t = (txt or '').lower()
    for label, kws in EMPLEO_HORARIO_KW.items():
        if any(k in t for k in kws):
            return label
    return None


def extraer_zona_empleo(txt):
    """Extrae zona geográfica de un post de empleo."""
    t = (txt or '').lower()
    for z in EMPLEO_ZONA_KW:
        if z in t:
            return z.title()
    m = re.search(
        r'(?:zona|ubicación|ubicacion)[:\s]+([A-ZÁÉÍÓÚÑ][a-záéíóúñA-ZÁÉÍÓÚÑ\s,]{3,30})',
        txt or '', re.I
    )
    if m:
        return m.group(1).strip().split('\n')[0][:30]
    return None


def _cap_primera(s):
    s = s.strip()
    if not s:
        return s
    for i, c in enumerate(s):
        if c.isalpha():
            return s[:i] + c.upper() + s[i + 1:]
    return s


def _pct_caps_empleo(texto):
    palabras = [re.sub(r'\W', '', w) for w in texto.split()]
    largas = [w for w in palabras if len(w) > 3 and w not in EMPLEO_SIGLAS]
    if not largas:
        return 0
    return sum(1 for w in largas if w.isupper()) / len(largas)


def _bajar_caps_empleo(texto):
    palabras = texto.split()
    resultado = []
    for w in palabras:
        nucleo = re.sub(r'\W', '', w)
        if nucleo.upper() in EMPLEO_SIGLAS:
            resultado.append(nucleo.upper())
        else:
            resultado.append(w.lower())
    return _cap_primera(' '.join(resultado))


def limpiar_texto_empleo(txt):
    """
    Limpia texto de posts de empleo:
    - Quita URLs, emojis, hashtags, menciones, teléfonos, emails
    - Inserta saltos antes de secciones (Requisitos:, Ofrecemos:, etc.)
    - Normaliza mayúsculas excesivas respetando siglas (IMSS, RFC, etc.)
    - Capitaliza correctamente inicio de oración
    """
    if not txt:
        return ''

    t = re.sub(r'https?://\S+|wa\.me\S*|rebrand\.ly\S*', '', txt)
    t = re.sub(r'@\[\d+:\d+:[^\]]*\]|@\w+', '', t)
    t = re.sub(r'#\w+', '', t)
    t = re.sub(r'[\U0001F300-\U0001FFFF\u2600-\u27BF]+', ' ', t, flags=re.UNICODE)
    t = re.sub(r'[\U0001D400-\U0001D7FF]+', '', t, flags=re.UNICODE)
    t = re.sub(r'[*_`~]', '', t)
    t = re.sub(r'(?m)^[•·\-–—]\s*', '', t)
    t = re.sub(r'\S+@\S+\.\S+', '', t)
    t = re.sub(r'(?<!\d)\d{3}[\s\-.]?\d{3}[\s\-.]?\d{4}(?!\d)', '', t)
    t = re.sub(r'(?i)\bTelf?\.\s*', '', t)
    t = re.sub(r'^#+\s*', '', t, flags=re.M)
    t = re.sub(r'\.{2,}', '.', t)
    t = re.sub(r'!{2,}', '!', t)
    t = re.sub(r'\?{2,}', '?', t)
    t = re.sub(r'\(\s*\)', '', t)

    # Insertar saltos antes de secciones conocidas
    for sec in EMPLEO_SECCIONES:
        t = re.sub(
            rf'(?<!\n)\s*\b({re.escape(sec)}s?:)',
            r'\n\1', t, flags=re.I
        )

    # Separar bullets pegados
    t = re.sub(r'-([A-ZÁÉÍÓÚÑ][a-záéíóúñ])', r'\n\1', t)
    t = re.sub(r'\n{3,}', '\n\n', t)

    # Normalizar caps por línea
    lines_out = []
    for linea in t.split('\n'):
        linea = re.sub(r' {2,}', ' ', linea).strip()
        if not linea:
            lines_out.append('')
            continue
        if _pct_caps_empleo(linea) >= 0.45:
            linea = _bajar_caps_empleo(linea)
            linea = re.sub(
                r'([.!?])\s+([a-záéíóúñ])',
                lambda m: m.group(1) + ' ' + m.group(2).upper(),
                linea
            )
        else:
            linea = _cap_primera(linea)
        lines_out.append(linea)

    # Quitar blancos consecutivos
    clean = []
    prev_blank = False
    for l in lines_out:
        if not l:
            if not prev_blank:
                clean.append('')
            prev_blank = True
        else:
            clean.append(l)
            prev_blank = False

    t = '\n'.join(clean).strip()
    if t and t[0].isalpha() and t[0].islower():
        t = t[0].upper() + t[1:]
    t = re.sub(r' ([,.:;])', r'\1', t)
    t = re.sub(r'([.:;,])([A-Za-záéíóúñÁÉÍÓÚÑ])', r'\1 \2', t)
    return t.strip()


def clasificar_tipo_empleo(txt):
    """
    Retorna 'oferta', 'busqueda', o None si no es un post de empleo.
    Se llama antes de Groq para evitar llamadas innecesarias.
    """
    t = (txt or '').lower()
    if any(k in t for k in OFERTA_KW):
        return 'oferta'
    if any(k in t for k in BUSQUEDA_KW):
        return 'busqueda'
    return None



# ═══════════════════════════════════════════════════════════════
# PASO 1: LIMPIEZA
# ═══════════════════════════════════════════════════════════════

# Mínimo de palabras con contenido real para que un negocio sea publicable
NEGOCIO_MIN_PALABRAS_CONTENIDO = 15


def paso_1_limpieza(posts, grupo_tipo="vecinos"):
    limpios = []
    descartados = []
    cfg = get_config_grupo(grupo_tipo)
    min_palabras_mascota = cfg.get("min_palabras_mascota", 15)
    min_palabras_noticia  = cfg.get("min_palabras_noticia", NEWS_MIN_WORDS)
    largo_es_negocio      = cfg.get("largo_es_negocio", False)

    # ── Contar frecuencia por autor ANTES de cualquier filtro ──
    _autor_freq = {}
    for p in posts:
        aid = p.get('autor_id') or p.get('autor_url')
        if aid:
            _autor_freq[aid] = _autor_freq.get(aid, 0) + 1

    for i, post in enumerate(posts):
        # Preservar fbid_post del scraper v3, o generar sintético
        if not post.get('fbid_post'):
            autor = (post.get('autor') or 'x')[:8].replace(' ','')
            texto_hash = abs(hash((post.get('texto') or '')[:50])) % 10**9
            post['fbid_post'] = f"syn_{autor}_{texto_hash}_{i}"

        debe_descartar, razon = es_descartable(post)
        if debe_descartar:
            post['_descartado'] = razon
            descartados.append(post)
            continue

        txt_original = (post.get("texto") or "").strip()
        txt_sin_urls = remover_urls(txt_original)
        txt_limpio   = limpiar_texto_regex(txt_sin_urls)

        if len(txt_limpio) < 10:
            post['_descartado'] = 'vacio_tras_limpieza'
            descartados.append(post)
            continue

        post['texto_limpio'] = txt_limpio
        post['telefono']     = extraer_telefono(txt_original) or extraer_telefono(post.get("autor", ""))
        post['noticia_permitida'] = puede_ser_noticia_desde_json(
            txt_original,
            min_palabras=min_palabras_noticia,
            grupo_tipo=grupo_tipo,
        )

        # ── Filtro urgente/difundamos por geografía ───────────────
        geo_urgente = clasificar_urgente_geo(txt_original)
        if geo_urgente == 'ignorar':
            post['_descartado'] = 'urgente_geo_externa'
            descartados.append(post)
            continue
        if geo_urgente == 'alerta':
            post['pre_tipo']  = 'alerta'
            post['pre_score'] = 3
            post['_urgente_local'] = True
            limpios.append(post)
            continue

        # ── Filtro de consulta sin señal comercial ────────────────
        # Empleo: no aplicar este filtro — "busco trabajo" es consulta válida
        if grupo_tipo != 'empleo' and es_post_consulta(txt_original) and not tiene_senal_comercial_fuerte(post, txt_original):
            post['_descartado'] = 'consulta_baja_prioridad'
            descartados.append(post)
            continue

        num_imgs = post.get("num_imgs") or len(post.get("imagenes") or [])
        tiene_tel = bool(post.get('telefono'))

        # ── Filtro estructural: sin imagen + sin tel + muy corto ──
        palabras_total = contar_palabras(txt_limpio)
        palabras_contenido = contar_palabras_contenido(txt_limpio)

        # Para mascotas el umbral mínimo es 15 palabras
        # Para el resto aplicar filtro de contenido normal
        es_mascota_probable = any(
            kw in txt_limpio.lower()
            for kw in ['adopci', 'perdid', 'extravi', 'encontr', 'rescate',
                       'hogar', 'gatito', 'perrito', 'cachorro', 'mascota',
                       'felino', 'canino', 'collar']
        )

        if es_mascota_probable and palabras_total >= min_palabras_mascota:
            pass  # mascota válida aunque sea corta
        elif grupo_tipo == 'empleo':
            # Lógica de filtro para empleo:
            # - Sin imagen: mínimo 8 palabras
            # - Con imagen: mínimo 4 palabras
            # - Con imagen + teléfono: mínimo 4 palabras, marcar para análisis IA profundo
            if num_imgs > 0:
                min_emp = 4
            else:
                min_emp = 8

            if palabras_total < min_emp:
                post['_descartado'] = 'empleo_muy_corto'
                descartados.append(post)
                continue

            # Si tiene imagen Y teléfono → marcar para análisis IA profundo
            if num_imgs > 0 and tiene_tel:
                post['_empleo_verificar_ia'] = True
        elif not num_imgs and not tiene_tel and palabras_total < 70:
            post['_descartado'] = 'sin_img_tel_y_corto'
            descartados.append(post)
            continue
        elif not es_mascota_probable and palabras_contenido < NEGOCIO_MIN_PALABRAS_CONTENIDO:
            if num_imgs > 0 and palabras_contenido >= 5:
                pass  # acepta con imagen aunque sea corto
            else:
                post['_descartado'] = 'contenido_insuficiente'
                post['_palabras_contenido'] = palabras_contenido
                descartados.append(post)
                continue

        if palabras_total < 5 and not tiene_senal_comercial_fuerte(post, txt_original):
            post['_descartado'] = 'post_demasiado_debil'
            descartados.append(post)
            continue

        # ── Pre-clasificación por keywords ───────────────────────
        pre_tipo, pre_score = pre_clasificar_keywords(
            txt_limpio, post.get("autor", ""), grupo_tipo=grupo_tipo
        )

        # Heurística: post largo en grupo vecinos → sospechar negocio
        if largo_es_negocio and palabras_total > 150 and pre_tipo == 'ambiguo':
            t = txt_limpio.lower()
            if sum(1 for kw in _KW['negocio'][2] if kw in t) >= 1:
                pre_tipo  = 'negocio'
                pre_score = 2

        post['pre_tipo']  = pre_tipo
        post['pre_score'] = pre_score

        # ── Frecuencia del autor ──────────────────────────────
        aid = post.get('autor_id') or post.get('autor_url')
        post['autor_frecuencia'] = _autor_freq.get(aid, 1) if aid else 1

        # ── Campos específicos de perdidos ────────────────────
        if grupo_tipo == 'perdidos' or pre_tipo == 'perdido':
            post['perdido_estado']    = detectar_estado_perdido(txt_original)
            post['perdido_categoria'] = detectar_categoria_perdido(txt_original)
            post['perdido_recompensa'] = detectar_recompensa(txt_original)
            # Boost: si es grupo perdidos y tiene estado pero quedó ambiguo, forzar perdido
            if grupo_tipo == 'perdidos' and pre_tipo == 'ambiguo' and post['perdido_estado']:
                post['pre_tipo'] = 'perdido'
                post['pre_score'] = 2

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

# ═══════════════════════════════════════════════════════════════
# NEGOCIO_TEMA_MAP — mapa de inferencia de tema para títulos SEO
# Orden importa: más específico primero.
# Cada entrada: ([keywords], 'Título SEO')
# ═══════════════════════════════════════════════════════════════
NEGOCIO_TEMA_MAP = [
    # ── COMIDA & BEBIDAS (específicos primero) ──────────────────
    (['fresas con crema'], 'Fresas con crema'),
    (['fresas'], 'Fresas con crema'),
    (['frappe', 'frappé', 'frappes', 'frappés'], 'Frappés'),
    (['smoothie', 'smoothies'], 'Smoothies'),
    (['crepa', 'crepas'], 'Crepas'),
    (['waffle', 'waffles'], 'Waffles'),
    (['hotcake', 'hotcakes'], 'Hotcakes'),
    (['sopes'], 'Sopes'),
    (['panuchos'], 'Panuchos'),
    (['tacos al pastor'], 'Tacos al pastor'),
    (['tacos de canasta'], 'Tacos de canasta'),
    (['tacos de cochinita', 'cochinita pibil', 'cochinita'], 'Cochinita pibil'),
    (['tacos'], 'Tacos'),
    (['burritos', 'burrito'], 'Burritos'),
    (['pizza', 'pizzas'], 'Pizzas'),
    (['hamburguesa', 'hamburguesas', 'burger', 'angus'], 'Hamburguesas'),
    (['hot dog', 'hotdog'], 'Hot dogs'),
    (['torta', 'tortas'], 'Tortas'),
    (['sushi'], 'Sushi'),
    (['mariscos', 'ceviche', 'camarones', 'pescado frito'], 'Mariscos'),
    (['pollo asado', 'pollo rostizado'], 'Pollo asado'),
    (['tamales', 'tamal'], 'Tamales'),
    (['empanadas', 'empanada'], 'Empanadas'),
    (['quesadillas', 'quesadilla'], 'Quesadillas'),
    (['gorditas', 'gordita'], 'Gorditas'),
    (['pozole'], 'Pozole'),
    (['enchiladas'], 'Enchiladas'),
    (['chilaquiles'], 'Chilaquiles'),
    (['coctel', 'cócteles'], 'Cócteles'),
    (['elotes', 'esquites'], 'Elotes y esquites'),
    (['churros'], 'Churros'),
    (['donas', 'dona'], 'Donas'),
    (['panaderia', 'panadería', 'pan dulce', 'conchas'], 'Panadería'),
    (['gomitas'], 'Gomitas'),
    (['chocolates', 'chocolate', 'trufas'], 'Chocolates y trufas'),
    (['paletas', 'helados', 'nieves'], 'Paletas y helados'),
    (['cafe de olla', 'café de olla', 'cafe', 'café'], 'Café'),
    (['jugos', 'licuados', 'agua de'], 'Jugos y licuados'),
    (['reposteria', 'repostería', 'panes', 'pastel', 'pasteles', 'cupcake', 'pays', 'cheesecake'], 'Repostería'),
    (['postre', 'postres'], 'Postres'),
    (['catering', 'banquete', 'banquetes', 'servicio de comida', 'servicio de buffet'], 'Catering y banquetes'),
    (['desayunos', 'desayuno', 'brunch'], 'Desayunos'),
    (['comida rapida', 'comida rápida', 'fast food'], 'Comida rápida'),
    (['comida vegetariana', 'vegana', 'vegano'], 'Comida vegetariana'),
    (['rosca', 'roscas'], 'Roscas'),
    (['loncheria', 'lonchería', 'almuerzo'], 'Lonchería'),

    # ── SALUD & BIENESTAR ───────────────────────────────────────
    (['masaje', 'masajes', 'masoterapia'], 'Masajes'),
    (['fisioterapia', 'fisioterapeuta', 'rehabilitacion fisica', 'rehabilitación física'], 'Fisioterapia'),
    (['terapia psicologica', 'psicólogo', 'psicologo', 'salud mental'], 'Psicología'),
    (['nutricion', 'nutrición', 'nutriólogo', 'nutriologo', 'dieta', 'plan alimenticio'], 'Nutrición'),
    (['acupuntura'], 'Acupuntura'),
    (['quiropraxia', 'quiropráctico'], 'Quiropraxia'),
    (['yoga'], 'Clases de yoga'),
    (['pilates'], 'Clases de pilates'),
    (['medicina alternativa', 'homeopatia', 'homeopatía'], 'Medicina alternativa'),
    (['dentista', 'dental', 'odontologia', 'odontología', 'ortodoncia', 'braces'], 'Odontología'),
    (['optometrista', 'optometría', 'lentes', 'anteojos'], 'Optometría'),
    (['enfermera', 'enfermero', 'cuidado de adulto mayor', 'cuidado de enfermos'], 'Cuidado de enfermos'),
    (['delivery de medicamentos', 'farmacia'], 'Farmacia y medicamentos'),

    # ── BELLEZA & ESTÉTICA ──────────────────────────────────────
    (['semipermanente', 'nail art', 'uñas acrilicas', 'uñas acrílicas', 'uñas gel'], 'Uñas acrílicas'),
    (['uñas', 'manicure', 'manicura', 'pedicure'], 'Manicure y pedicure'),
    (['corte de cabello', 'coloracion', 'coloración', 'tinte de cabello', 'keratina', 'alisado'], 'Estilismo de cabello'),
    (['barbería', 'barberia', 'barber', 'corte de barba', 'fade'], 'Barbería'),
    (['maquillaje profesional', 'maquillaje para bodas', 'maquillaje artístico'], 'Maquillaje profesional'),
    (['maquillaje', 'makeup'], 'Maquillaje'),
    (['depilacion', 'depilación', 'cera', 'laser'], 'Depilación'),
    (['lifting de pestañas', 'extensiones de pestañas', 'pestañas'], 'Extensiones de pestañas'),
    (['spa', 'relajacion', 'relajación', 'day spa'], 'Spa'),
    (['micropigmentacion', 'micropigmentación', 'microblading', 'cejas', 'diseño de cejas'], 'Micropigmentación'),
    (['tatuaje', 'tattoo', 'piercing'], 'Tatuajes y piercings'),
    (['estetica', 'estética', 'salon de belleza', 'salón de belleza'], 'Estética'),
    (['lifting', 'tratamiento facial', 'facial', 'limpieza facial'], 'Tratamientos faciales'),
    (['cabello', 'pelo'], 'Servicios de cabello'),

    # ── AUTOMOTRIZ ──────────────────────────────────────────────
    (['pintura automotriz', 'pintura de auto', 'pintura de carro', 'enderezado y pintura'], 'Pintura automotriz'),
    (['ceramica automotriz', 'cerámica automotriz', 'recubrimiento ceramico', 'recubrimiento cerámico', 'detallado automotriz', 'detailing'], 'Detailing automotriz'),
    (['mecanica', 'mecánica', 'taller mecanico', 'taller mecánico', 'motor', 'frenos', 'suspension'], 'Mecánica automotriz'),
    (['electrico automotriz', 'eléctrico automotriz', 'electricista automotriz'], 'Electricidad automotriz'),
    (['bateria', 'batería', 'optima', 'varta', 'bosch'], 'Baterías para auto'),
    (['llanta', 'llantas', 'neumatico', 'neumático', 'vulcanizadora', 'ponchadura'], 'Llantas y vulcanizadora'),
    (['alineacion', 'alineación', 'balanceo'], 'Alineación y balanceo'),
    (['vidrios automotrices', 'parabrisas', 'luneta'], 'Vidrios automotrices'),
    (['desabollado', 'hojalatería', 'hojalateria'], 'Hojalatería'),
    (['tapicería automotriz', 'tapiceria automotriz', 'tapizado de asientos'], 'Tapicería automotriz'),
    (['lubricentro', 'cambio de aceite', 'aceite automotriz'], 'Cambio de aceite'),
    (['venta de auto', 'venta de carro', 'vendo carro', 'vendo auto', 'se vende auto', 'se vende carro'], 'Venta de auto'),
    (['renta de auto', 'renta de carro', 'renta de vehiculo', 'renta de vehículo'], 'Renta de autos'),

    # ── SERVICIOS DEL HOGAR ─────────────────────────────────────
    (['plomero', 'plomería', 'plomeria', 'fontanero', 'fontanería', 'tuberias', 'tuberías', 'fugas de agua'], 'Plomería'),
    (['electricista', 'instalacion electrica', 'instalación eléctrica', 'cableado', 'tablero electrico'], 'Electricidad'),
    (['carpintero', 'carpinteria', 'carpintería', 'muebles de madera', 'closet', 'puertas de madera'], 'Carpintería'),
    (['herrero', 'herreria', 'herrería', 'soldadura', 'rejas', 'portones', 'puertas de herreria'], 'Herrería'),
    (['albanil', 'albañil', 'albanileria', 'albañilería', 'construccion', 'construcción', 'mamposteria'], 'Albañilería y construcción'),
    (['pintura de casas', 'pintura de interior', 'pintura de exterior', 'pintamos casas'], 'Pintura de casas'),
    (['impermeabilizacion', 'impermeabilización', 'impermeabilizante', 'techo', 'losa'], 'Impermeabilización'),
    (['techado', 'techos', 'lamina', 'lámina', 'teja', 'cubiertas'], 'Techados y cubiertas'),
    (['aires acondicionados', 'aire acondicionado', 'minisplit', 'refrigeracion', 'refrigeración', 'hvac'], 'Aire acondicionado'),
    (['jardineria', 'jardinería', 'poda', 'corte de cesped', 'césped', 'mantenimiento de jardines'], 'Jardinería'),
    (['fumigacion', 'fumigación', 'plagas', 'insectos', 'cucarachas', 'ratones', 'termitas'], 'Fumigación y control de plagas'),
    (['limpieza de casas', 'limpieza del hogar', 'limpieza profunda', 'intendencia'], 'Limpieza del hogar'),
    (['lavado de muebles', 'lavado de salas', 'lavado de colchones', 'lavado de alfombras'], 'Lavado de muebles'),
    (['instalacion de pisos', 'pisos laminados', 'pisos de madera', 'porcelanato', 'azulejos'], 'Instalación de pisos'),
    (['cortinas', 'persianas', 'toldos', 'mosquiteros', 'roller'], 'Cortinas y persianas'),
    (['cerrajero', 'cerrajeria', 'cerrajería', 'chapa', 'candado', 'llave', 'apertura de puertas'], 'Cerrajería'),
    (['mantenimiento del hogar', 'mantenimiento de casas', 'reparaciones del hogar'], 'Mantenimiento del hogar'),
    (['tapiceria', 'tapicería', 'tapizado de muebles', 'retapizado'], 'Tapicería'),
    (['gasero', 'instalacion de gas', 'instalación de gas', 'estufa', 'calentador'], 'Instalación de gas'),

    # ── MASCOTAS ────────────────────────────────────────────────
    (['veterinaria', 'veterinario', 'clinica veterinaria', 'clínica veterinaria'], 'Veterinaria'),
    (['grooming', 'estética canina', 'estetica canina', 'peluquería canina', 'baño de mascotas'], 'Estética canina'),
    (['alimento para mascotas', 'croquetas', 'comida para perros', 'comida para gatos'], 'Alimento para mascotas'),
    (['accesorios para mascotas', 'correa', 'collar para perro', 'juguetes para mascotas'], 'Accesorios para mascotas'),
    (['paseador de perros', 'paseo de perros', 'cuidado de mascotas'], 'Cuidado de mascotas'),

    # ── MUDANZAS & LOGÍSTICA ────────────────────────────────────
    (['flete', 'fletes', 'mudanza', 'mudanzas', 'camion de mudanza'], 'Fletes y mudanzas'),
    (['envios', 'envíos', 'paqueteria', 'paquetería', 'estafeta', 'fedex', 'redpack', 'dhl'], 'Envíos y paquetería'),
    (['mensajeria', 'mensajería', 'delivery'], 'Mensajería y delivery'),
    (['reparacion de celulares', 'reparación de celulares', 'pantalla de celular', 'servicio de celular'], 'Reparación de celulares'),
    (['reparacion de computadoras', 'reparación de computadoras', 'laptops', 'pc', 'formateo'], 'Reparación de computadoras'),
    (['instalacion de camaras', 'camaras de seguridad', 'cámaras de seguridad', 'cctv', 'videovigilancia'], 'Cámaras de seguridad'),
    (['alarmas', 'sistema de alarma', 'alarma para casa', 'alarma para negocio'], 'Sistemas de alarma'),
    (['redes', 'internet', 'wifi', 'router', 'cableado de red', 'instalacion de internet'], 'Redes e internet'),
    (['venta de celulares', 'venta de smartphones', 'celulares seminuevos'], 'Venta de celulares'),
    (['vendo laptop', 'vendo computadora', 'vendo pc', 'computadora en venta'], 'Venta de computadoras'),
    (['videojuegos', 'nintendo switch', 'playstation', 'xbox', 'ps5', 'ps4', 'mariokart'], 'Videojuegos'),
    (['electrónica', 'electronica', 'televisiones', 'pantallas'], 'Electrónica'),
    (['impresion', 'impresión', 'imprenta', 'diseño grafico', 'diseño gráfico', 'logo', 'branding', 'banner'], 'Diseño gráfico e impresión'),

    # ── EDUCACIÓN & CURSOS ──────────────────────────────────────
    (['clases de ingles', 'clases de inglés', 'ingles', 'inglés', 'idiomas', 'english'], 'Clases de inglés'),
    (['clases de matematicas', 'clases de matemáticas', 'matematicas', 'matemáticas', 'algebra', 'calculo'], 'Clases de matemáticas'),
    (['clases de musica', 'clases de música', 'piano', 'guitarra', 'violin', 'violín', 'bateria musical'], 'Clases de música'),
    (['clases de baile', 'baile', 'danza', 'salsa', 'merengue', 'reggaeton'], 'Clases de baile'),
    (['clases de natacion', 'clases de natación', 'natacion', 'natación', 'alberca'], 'Clases de natación'),
    (['clases de dibujo', 'clases de arte', 'pintura artistica', 'pintura artística'], 'Clases de arte'),
    (['clases de cocina', 'gastronomia', 'gastronomía', 'curso de cocina'], 'Clases de cocina'),
    (['taekwondo', 'karate', 'jiu jitsu', 'boxeo', 'artes marciales', 'kung fu'], 'Artes marciales'),
    (['clases de futbol', 'clases de fútbol', 'futbol soccer', 'academia de futbol'], 'Clases de fútbol'),
    (['clases de computacion', 'clases de computación', 'programacion', 'programación', 'coding'], 'Clases de computación'),
    (['clases particulares', 'clases a domicilio', 'tutoria', 'tutoría', 'apoyo escolar'], 'Clases particulares'),
    (['cursos en linea', 'cursos online', 'certificaciones'], 'Cursos en línea'),
    (['clases', 'curso', 'academia', 'taller de'], 'Clases y cursos'),

    # ── ROPA & ACCESORIOS ───────────────────────────────────────
    (['uniformes', 'uniforme escolar', 'uniforme de trabajo'], 'Uniformes'),
    (['ropa de bebe', 'ropa de bebé', 'ropa infantil', 'ropa para niños'], 'Ropa infantil'),
    (['ropa deportiva', 'playeras personalizadas', 'sublimacion', 'sublimación'], 'Ropa deportiva'),
    (['ropa', 'blusa', 'vestido', 'pantalon', 'pantalón', 'falda', 'traje'], 'Ropa'),
    (['bolsa', 'bolso', 'cartera', 'mochila'], 'Bolsas y accesorios'),
    (['joyeria', 'joyería', 'aretes', 'collares', 'pulseras', 'anillos', 'plata'], 'Joyería'),
    (['zapatos', 'zapatillas', 'sandalias', 'tenis', 'calzado', 'botas'], 'Calzado'),
    (['remate de ropa', 'ropa de segunda', 'ropa usada'], 'Ropa de segunda mano'),

    # ── INMOBILIARIA & CONSTRUCCIÓN ─────────────────────────────
    (['se renta departamento', 'renta de departamento', 'departamento en renta', 'depa en renta'], 'Departamento en renta'),
    (['se renta casa', 'renta de casa', 'casa en renta'], 'Casa en renta'),
    (['venta de casa', 'casa en venta', 'se vende casa'], 'Casa en venta'),
    (['venta de terreno', 'terreno en venta', 'se vende terreno', 'terreno'], 'Terreno en venta'),
    (['local comercial', 'local en renta', 'bodega en renta', 'oficina en renta'], 'Local comercial en renta'),
    (['departamento', 'condominio', 'inmueble'], 'Inmobiliaria'),
    (['materiales de construccion', 'materiales de construcción', 'block', 'cemento', 'varillas'], 'Materiales de construcción'),

    # ── EVENTOS & ENTRETENIMIENTO ───────────────────────────────
    (['fotografia', 'fotografía', 'fotografo', 'fotógrafo', 'sesion de fotos', 'sesión de fotos'], 'Fotografía'),
    (['video de bodas', 'video de xv', 'videografo', 'videógrafo', 'cinematic', 'filmacion'], 'Videografía'),
    (['dj', 'discjockey', 'musica para eventos', 'música para eventos', 'animacion de fiestas'], 'DJ y animación'),
    (['renta de inflables', 'inflables', 'brincolín', 'brincolin'], 'Renta de inflables'),
    (['renta de sillas', 'renta de mesas', 'renta de vajilla', 'renta de carpas', 'renta de mobiliario'], 'Renta de mobiliario para eventos'),
    (['decoracion para eventos', 'decoración para fiestas', 'globos', 'decoradora'], 'Decoración para eventos'),
    (['magos', 'mago', 'payaso', 'entretenimiento infantil', 'show infantil'], 'Entretenimiento infantil'),
    (['piñata', 'piñatas'], 'Piñatas'),
    (['bodas', 'boda', 'quinceañera', 'xv años', 'primera comunion', 'primera comunión'], 'Eventos sociales'),
    (['fiesta', 'fiestas', 'eventos', 'evento'], 'Organización de eventos'),

    # ── SERVICIOS PROFESIONALES ─────────────────────────────────
    (['contador', 'contaduria', 'contaduría', 'contabilidad', 'declaracion anual', 'sat', 'fiscal', 'facturas'], 'Contabilidad y fiscal'),
    (['abogado', 'asesor legal', 'derecho', 'juridico', 'jurídico', 'tramites legales', 'trámites legales'], 'Servicios legales'),
    (['seguro de vida', 'seguro de gastos medicos', 'seguro médico', 'seguros', 'aseguradora'], 'Seguros'),
    (['arquitecto', 'arquitectura', 'planos', 'proyecto arquitectonico'], 'Arquitectura'),
    (['agencia de viajes', 'paquetes de viaje', 'tours', 'excursiones'], 'Agencia de viajes'),
    (['traductor', 'traducción', 'traduccion', 'interprete'], 'Traducción e interpretación'),
    (['reclutamiento', 'recursos humanos', 'bolsa de trabajo', 'empleo'], 'Recursos humanos'),
    (['marketing digital', 'redes sociales', 'manejo de redes', 'community manager', 'publicidad digital'], 'Marketing digital'),
    (['desarrollo web', 'pagina web', 'página web', 'app movil', 'app móvil', 'software'], 'Desarrollo web y apps'),

    # ── COMPRA-VENTA & REMATES ──────────────────────────────────
    (['rifa', 'rifamos', 'boleto de rifa'], 'Rifa'),
    (['se vende', 'vendo', 'a la venta', 'en venta', 'en remate', 'ofrezca', 'remato'], 'Artículo en venta'),
    (['segunda mano', 'de segunda', 'usado', 'usados', 'seminuevo', 'seminuevos'], 'Artículo seminuevo'),

    # ── HOGAR & DECORACIÓN ──────────────────────────────────────
    (['muebles', 'muebleria', 'mueblería', 'sala', 'comedor', 'cama', 'colchon', 'colchón'], 'Muebles'),
    (['decoracion del hogar', 'decoración del hogar', 'plantas', 'macetas', 'arreglos florales'], 'Decoración del hogar'),
    (['electrodomesticos', 'electrodomésticos', 'refrigerador', 'lavadora', 'estufa', 'microondas'], 'Electrodomésticos'),
    (['pinturas', 'pintura vinilica', 'pintura vinílica', 'ferreteria', 'ferretería'], 'Ferretería y pinturas'),
]

# ═══════════════════════════════════════════════════════════════
# NEGOCIO_CATEGORIA_MAP — asigna categoría DB a partir de keywords
# Independiente de si la columna 'keywords' está llena en DB.
# Estructura: {nombre_categoria_normalizado: [keywords]}
# Se usa como fallback cuando cat.get('keywords') está vacío.
# ═══════════════════════════════════════════════════════════════
NEGOCIO_CATEGORIA_KEYWORDS = {
    'comida': [
        'tacos', 'pizza', 'hamburguesa', 'hamburguesas', 'burger', 'torta', 'sushi', 'mariscos',
        'ceviche', 'camarones', 'pollo', 'tamales', 'empanadas', 'quesadillas', 'gorditas',
        'pozole', 'enchiladas', 'chilaquiles', 'fresas', 'frappe', 'frappé', 'smoothie',
        'crepa', 'waffle', 'hotcake', 'sopes', 'panuchos', 'postre', 'pasteles', 'panaderia',
        'pan dulce', 'cafe', 'café', 'jugos', 'licuados', 'reposteria', 'repostería',
        'gomitas', 'chocolates', 'paletas', 'helados', 'desayunos', 'brunch', 'rosca',
        'loncheria', 'cochinita', 'menu', 'menú', 'comida', 'alimentos', 'antojo',
        'coctel', 'elotes', 'esquites', 'churros', 'donas', 'catering', 'banquete',
    ],
    'salud': [
        'masaje', 'masajes', 'fisioterapia', 'rehabilitacion', 'rehabilitación',
        'psicologo', 'psicólogo', 'nutricion', 'nutrición', 'nutriologo', 'nutriólogo',
        'acupuntura', 'quiropraxia', 'yoga', 'pilates', 'dentista', 'dental',
        'odontologia', 'odontología', 'optometrista', 'lentes', 'enfermera',
        'farmacia', 'medicamentos', 'medicina', 'terapia', 'consultorio', 'doctor',
        'salud', 'bienestar', 'clinica', 'clínica',
    ],
    'belleza': [
        'uñas', 'manicure', 'manicura', 'pedicure', 'cabello', 'coloracion', 'coloración',
        'tinte', 'keratina', 'alisado', 'barberia', 'barbería', 'barber', 'maquillaje',
        'depilacion', 'depilación', 'pestañas', 'lifting', 'micropigmentacion',
        'tatuaje', 'piercing', 'estetica', 'estética', 'spa', 'facial', 'tratamiento',
        'corte', 'peinado', 'extensiones',
    ],
    'ropa': [
        'ropa', 'blusa', 'vestido', 'pantalon', 'pantalón', 'falda', 'traje', 'uniforme',
        'tenis', 'zapatos', 'sandalias', 'calzado', 'botas', 'bolsa', 'bolso', 'cartera',
        'mochila', 'joyeria', 'joyería', 'aretes', 'collares', 'pulseras',
        'sublimacion', 'sublimación', 'playeras',
    ],
    'inmobiliaria': [
        'se renta', 'en renta', 'renta de', 'se vende casa', 'casa en venta',
        'departamento', 'depa', 'terreno', 'inmueble', 'propiedad', 'local comercial',
        'bodega', 'alquiler', 'condominio', 'fraccionamiento',
    ],
    'automotriz': [
        'pintura automotriz', 'ceramica automotriz', 'cerámica automotriz', 'detailing',
        'mecanica', 'mecánica', 'taller', 'motor', 'frenos', 'suspension', 'bateria',
        'batería', 'llanta', 'llantas', 'alineacion', 'alineación', 'balanceo',
        'parabrisas', 'hojalatería', 'tapiceria automotriz', 'cambio de aceite',
        'vendo carro', 'vendo auto', 'renta de auto',
    ],
    'servicios del hogar': [
        'plomero', 'plomería', 'plomeria', 'fontanero', 'electricista', 'carpintero',
        'carpinteria', 'carpintería', 'herrero', 'herreria', 'herrería', 'soldadura',
        'albanil', 'albañil', 'construccion', 'construcción', 'pintura de casas',
        'impermeabilizacion', 'impermeabilización', 'techado', 'techos', 'lamina',
        'aires acondicionados', 'aire acondicionado', 'minisplit', 'jardineria',
        'jardinería', 'poda', 'fumigacion', 'fumigación', 'plagas', 'limpieza',
        'lavado de muebles', 'pisos', 'cortinas', 'persianas', 'cerrajero',
        'cerrajeria', 'mantenimiento', 'tapiceria', 'tapicería', 'gas',
        'toldos', 'mosquiteros', 'reparacion', 'reparación',
    ],
    'tecnología': [
        'celulares', 'pantalla de celular', 'computadoras', 'laptops', 'formateo',
        'camaras de seguridad', 'cámaras de seguridad', 'cctv', 'alarmas', 'wifi',
        'redes', 'internet', 'router', 'videojuegos', 'nintendo', 'playstation',
        'xbox', 'electronica', 'electrónica', 'televisiones', 'pantallas',
        'diseño grafico', 'diseño gráfico', 'logo', 'branding', 'imprenta',
        'impresion', 'impresión', 'programacion', 'programación',
    ],
    'educación': [
        'clases', 'curso', 'cursos', 'academia', 'taller', 'inglés', 'ingles',
        'matematicas', 'matemáticas', 'musica', 'música', 'piano', 'guitarra',
        'baile', 'danza', 'natacion', 'natación', 'dibujo', 'arte', 'cocina',
        'taekwondo', 'karate', 'boxeo', 'artes marciales', 'futbol', 'fútbol',
        'computacion', 'computación', 'tutoria', 'tutoría', 'apoyo escolar',
    ],
    'eventos': [
        'fotografia', 'fotografía', 'fotografo', 'fotógrafo', 'video de bodas',
        'dj', 'musica para eventos', 'inflables', 'brincolín', 'sillas', 'mesas',
        'vajilla', 'carpas', 'globos', 'decoracion', 'decoración', 'mago', 'payaso',
        'piñata', 'bodas', 'boda', 'quinceañera', 'xv años', 'fiestas', 'eventos',
        'animacion', 'animación',
    ],
    'mascotas': [
        'veterinaria', 'veterinario', 'grooming', 'estética canina', 'peluquería canina',
        'croquetas', 'alimento para mascotas', 'accesorios para mascotas', 'paseador',
        'cuidado de mascotas',
    ],
    'servicios profesionales': [
        'contador', 'contabilidad', 'fiscal', 'sat', 'facturas', 'abogado', 'legal',
        'tramites', 'trámites', 'seguro', 'seguros', 'arquitecto', 'planos',
        'agencia de viajes', 'tours', 'traductor', 'traducción', 'marketing digital',
        'redes sociales', 'community manager', 'desarrollo web', 'pagina web',
    ],
    'mudanzas': [
        'flete', 'fletes', 'mudanza', 'mudanzas', 'envios', 'envíos',
        'paqueteria', 'paquetería', 'estafeta', 'fedex', 'redpack', 'dhl',
        'mensajeria', 'mensajería', 'delivery',
    ],
}

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
    txt = (txt or '')
    # NUEVO: eliminar bloques markdown y la palabra "json" / "ignorar" que se filtren mal
    txt = re.sub(r'```(?:json)?', '', txt, flags=re.IGNORECASE)
    txt = re.sub(r'```', '', txt)
    txt = re.sub(r'\bjson\b', '', txt, flags=re.IGNORECASE)
    txt = txt.replace('*', ' ')
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

    # Lista de zonas/colonias válidas de Mérida — evita tomar frases de contexto
    ZONAS_VALIDAS = {
        'altabrisa', 'caucel', 'chuburna', 'cholul', 'conkal', 'diaz ordaz',
        'dzitya', 'francisco de montejo', 'garcia gineres', 'garcia ginerés',
        'itzimna', 'jardines de mérida', 'miguel hidalgo', 'montecristo',
        'montejo', 'nueva sambula', 'nueva sambulá', 'pensiones', 'pedregal',
        'polígono 108', 'san antonio cinta', 'san bernardino', 'san damian',
        'san francisco', 'san jose', 'san josé', 'san pedro', 'san sebastian',
        'san sebastián', 'santa rosa', 'santa gertrudis', 'tanlum', 'vergel',
        'vista alegre', 'xcumpich', 'yucalpeten', 'centro', 'centro histórico',
        'dzibilchaltun', 'dzibilchaltún', 'residencial pensiones',
        # Municipios cercanos
        'merida', 'mérida', 'progreso', 'uman', 'umán', 'kanasin', 'kanasín',
        'conkal', 'tekax', 'valladolid', 'tizimin', 'tizimín',
    }

    # Palabras que NO son zonas aunque aparezcan después de "en"
    FALSOS_POSITIVOS = {
        'venta', 'renta', 'domicilio', 'linea', 'línea', 'tienda', 'efectivo',
        'credito', 'crédito', 'oferta', 'promocion', 'promoción', 'horario',
        'facebook', 'whatsapp', 'instagram', 'internet', 'general', 'todo',
        'almacenamiento', 'agua', 'luz', 'gas', 'servicio', 'obra', 'casa',
        'departamento', 'local', 'oficina', 'el trabajo', 'la ciudad',
        'toda la ciudad', 'tu hogar', 'tu negocio', 'el norte', 'el sur',
    }

    # Patrones con prioridad — los más específicos primero
    patrones = [
        r'\b(?:ubicados en|ubicado en|nos ubicamos en|estamos en)\s+([A-Za-zÁÉÍÓÚÑáéíóúñ0-9\-\s]{4,50})',
        r'\bcerca de\s+([A-Za-zÁÉÍÓÚÑáéíóúñ0-9\-\s]{4,50})',
        r'\bcolonia\s+([A-Za-zÁÉÍÓÚÑáéíóúñ0-9\-\s]{4,40})',
        r'\bfracc(?:ionamiento)?\s+([A-Za-zÁÉÍÓÚÑáéíóúñ0-9\-\s]{4,40})',
    ]

    for pat in patrones:
        m = re.search(pat, t, re.IGNORECASE)
        if m:
            frag = re.split(r'[,.]|\s+y\s|\s+x\s|\n', m.group(1))[0].strip()
            frag = re.sub(r'\s+', ' ', frag)
            frag = re.sub(r'(?i)^(la|el|los|las)\s+', '', frag)
            frag_norm = frag.lower().strip()
            if len(frag) < 4:
                continue
            # Rechazar falsos positivos
            if any(fp in frag_norm for fp in FALSOS_POSITIVOS):
                continue
            # Validar contra zonas conocidas
            if any(z in frag_norm for z in ZONAS_VALIDAS):
                return frag[:40]

    # Búsqueda directa de zona conocida en el texto completo
    t_norm = t.lower()
    for zona in sorted(ZONAS_VALIDAS, key=len, reverse=True):
        if zona in t_norm and len(zona) >= 6:
            return zona.title()

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
    if re.search(r'\$\s?\d+|\b\d+\s*(pesos|mxn)\b', txt or '', re.IGNORECASE):
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

    # Caso 8 — Actualización de cierre: descartar
    _frases_cierre = [
        'ya está con sus dueños', 'ya esta con sus dueños', 'ya apareció', 'ya aparecio',
        'ya fue encontrado', 'ya fue encontrada', 'ya está en casa', 'ya esta en casa',
        'gracias a todos', 'muchas gracias a todos', 'muchísimas gracias a todos',
    ]
    if any(f in txt for f in _frases_cierre) and len(txt.split()) < 40:
        return None  # señal para descartar el post

    # Caso 3 — Detectar especie correcta: priorizar la más mencionada
    _conteo = {
        'Perro': sum(txt.count(x) for x in ['perro', 'perrita', 'perrito', 'cachorro', 'can ']),
        'Gato': sum(txt.count(x) for x in ['gato', 'gata', 'gatito', 'gatita', 'michi']),
    }
    if _conteo['Perro'] > 0 or _conteo['Gato'] > 0:
        especie = max(_conteo, key=_conteo.get)
    else:
        especie = 'Mascota'

    # Caso 2 — Detectar subtipo con señales explícitas antes de usar categoria_id
    # Señales de PERDIDA (aunque aparezca la palabra "encontrar" como objetivo)
    _senales_perdida = [
        'se perdió', 'se perdio', 'se extravió', 'se extravio', 'desapareció', 'desaparecio',
        'se busca', 'seguimos buscando', 'ayuda para encontrar', 'pido ayuda para encontrar',
        'se ofrece recompensa', 'recompensa', 'no aparece', 'no ha aparecido',
        'si la ves', 'si lo ves', 'si la ven', 'si lo ven', 'responde al nombre',
        'avísame', 'avisame', 'por favor compartan',
    ]
    # Señales de ENCONTRADA (el animal ya está físicamente con alguien)
    _senales_encontrada = [
        'lo encontré', 'la encontré', 'lo encontre', 'la encontre',
        'encontré este', 'encontré esta', 'encontre este', 'encontre esta',
        'está aquí conmigo', 'esta aqui conmigo', 'lo tengo', 'la tengo',
        'apareció en', 'aparecio en', 'se metió a mi casa', 'se metio a mi casa',
        'anda en mi', 'está en mi casa', 'esta en mi casa',
        'si alguien conoce al dueño', 'si alguien conoce a su dueño',
        'si es de alguien', 'si es tuyo', 'si es tuya',
    ]
    # Señales de ADOPCIÓN
    _senales_adopcion = [
        'dar en adopción', 'dar en adopcion', 'en adopción', 'en adopcion',
        'busca familia', 'busca hogar', 'necesita hogar', 'necesita familia',
        'la entregamos', 'lo entregamos', 'vacunada', 'vacunado', 'esterilizada',
        'bañadita', 'banadita',
    ]

    if any(s in txt for s in _senales_perdida):
        subtipo = 'perdida'
    elif any(s in txt for s in _senales_encontrada):
        subtipo = 'encontrada'
    elif any(s in txt for s in _senales_adopcion):
        subtipo = 'en adopción'
    else:
        # Fallback a categoria_id
        _map = {14: 'perdida', 15: 'encontrada', 16: 'en adopción'}
        subtipo = _map.get(categoria_id, None)

    # Caso 8 — Si no hay subtipo claro y el texto es muy vago, descartar
    if not subtipo:
        palabras_utiles = [w for w in txt.split() if len(w) > 3]
        if len(palabras_utiles) < 12:
            return None  # demasiado vago para un título útil
        subtipo = 'reportada'

    # Concordancia de género correcta por especie
    _genero_fem = {'perdida': 'Perdida', 'encontrada': 'Encontrada',
                   'en adopción': 'en Adopción', 'reportada': 'Reportada'}
    _genero_mas = {'perdida': 'Perdido', 'encontrada': 'Encontrado',
                   'en adopción': 'en Adopción', 'reportada': 'Reportado'}
    if especie == 'Mascota':
        subtipo_display = _genero_fem.get(subtipo, subtipo.capitalize())
    else:
        # Perro y Gato son masculinos
        subtipo_display = _genero_mas.get(subtipo, subtipo.capitalize())

    ubic = extraer_ubicacion_simple(txt_raw)
    titulo = f"{especie} {subtipo_display}"
    if ubic:
        titulo += f" en {ubic}"
    elif subtipo == 'en adopción':
        titulo += ' en Mérida'
    return limpiar_titulo(titulo, max_chars=62) or None


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


def generar_titulo_perdido(post):
    """
    Genera título SEO para objetos perdidos/encontrados.
    Formato: "[Objeto] [perdido/encontrado] en [ubicación]"
    Ejemplos:
      "INE perdida en el centro de Mérida"
      "iPhone encontrado en Galerías Mérida"
      "Llaves perdidas por Pensiones — ofrece recompensa"
    """
    txt = post.get('texto_limpio') or post.get('texto') or ''
    estado = post.get('perdido_estado')  # 'perdido' | 'encontrado' | None
    categoria = post.get('perdido_categoria', 'otro')
    recompensa = post.get('perdido_recompensa', False)

    # Mapeo de categoría a nombre legible
    _cat_nombres = {
        'documento':     'Documento',
        'electronico':   'Celular',
        'llaves':        'Llaves',
        'cartera_bolsa': 'Cartera',
        'vehiculo':      'Vehículo',
        'mascota':       'Mascota',
        'lentes':        'Lentes',
        'ropa':          'Prenda de ropa',
        'otro':          'Objeto',
    }

    # Intentar extraer objeto específico del texto
    _objetos_especificos = [
        (r'\bINE\b', 'INE'),
        (r'\bcredencial\b', 'Credencial'),
        (r'\bpasaporte\b', 'Pasaporte'),
        (r'\blicencia\b', 'Licencia'),
        (r'\biphone\s*\d*', None),  # captura "iPhone 15"
        (r'\bsamsung\s*\w*', None),
        (r'\bcelular\b', 'Celular'),
        (r'\bcartera\b', 'Cartera'),
        (r'\bbilletera\b', 'Billetera'),
        (r'\bmochila\b', 'Mochila'),
        (r'\bbolsa\b', 'Bolsa'),
        (r'\bllaves?\b', 'Llaves'),
        (r'\bllavero\b', 'Llavero'),
        (r'\blentes\b', 'Lentes'),
        (r'\bgafas\b', 'Gafas'),
        (r'\bbicicleta\b', 'Bicicleta'),
        (r'\bmoto\b', 'Moto'),
        (r'\bplacas?\b', 'Placas'),
        (r'\blaptop\b', 'Laptop'),
        (r'\btablet\b', 'Tablet'),
    ]

    objeto = _cat_nombres.get(categoria, 'Objeto')
    for patron, nombre_fijo in _objetos_especificos:
        m = re.search(patron, txt, re.IGNORECASE)
        if m:
            objeto = nombre_fijo or m.group(0).strip().title()
            break

    # Estado con concordancia de género
    _femeninas_sing = ('cartera','billetera','mochila','bolsa','moto','bicicleta',
                       'ine','credencial','licencia','laptop','tablet','prenda de ropa',
                       'mascota','identificación','identificacion','placa')
    _femeninas_plur = ('llaves','gafas','placas')
    _masculinas_plur = ('lentes','audífonos','audifonos','airpods','documentos')

    def _concordar(base_masc, base_fem, obj):
        o = obj.lower()
        if o in _femeninas_sing:
            return base_fem + 'a'
        elif o in _femeninas_plur:
            return base_fem + 'as'
        elif o in _masculinas_plur:
            return base_masc + 's'
        else:
            return base_masc

    if estado == 'encontrado':
        estado_txt = _concordar('encontrado', 'encontrad', objeto)
    else:
        # perdido o None — usar "perdido" con concordancia
        estado_txt = _concordar('perdido', 'perdid', objeto)

    ubic = extraer_ubicacion_simple(txt)
    titulo = f"{objeto} {estado_txt}"
    if ubic:
        titulo += f" en {ubic}"
    else:
        titulo += " en Mérida"
    if recompensa:
        titulo += " — ofrece recompensa"

    return limpiar_titulo(titulo, max_chars=72) or 'Objeto perdido en Mérida'


def generar_titulo_empleo(post):
    """
    Genera título SEO para posts de empleo.
    Formato: "[Puesto] — [oferta/búsqueda] en Mérida"
    """
    txt = post.get('texto_limpio') or post.get('texto') or ''
    tipo_empleo = post.get('tipo_empleo', 'oferta')
    puesto = post.get('puesto') or ''

    if not puesto:
        # Intentar extraer puesto del texto
        _puestos = [
            r'\b(cocinero|cocinera|chef)\b',
            r'\b(mesero|mesera|camarero)\b',
            r'\b(chofer|conductor)\b',
            r'\b(cajero|cajera)\b',
            r'\b(vendedor|vendedora)\b',
            r'\b(guardia|seguridad|vigilante)\b',
            r'\b(limpieza|intendente)\b',
            r'\b(secretaria|recepcionista|administrativo)\b',
            r'\b(electricista|plomero|albañil|soldador)\b',
            r'\b(enfermero|enfermera|médico)\b',
            r'\b(programador|desarrollador|diseñador)\b',
            r'\b(maestro|maestra|profesor)\b',
        ]
        for pat in _puestos:
            m = re.search(pat, txt, re.IGNORECASE)
            if m:
                puesto = m.group(0).strip().title()
                break

    if tipo_empleo == 'busqueda':
        if puesto:
            titulo = f"Busco trabajo de {puesto} en Mérida"
        else:
            titulo = "Busco empleo en Mérida"
    else:
        if puesto:
            titulo = f"Vacante de {puesto} en Mérida"
        else:
            titulo = "Vacante de empleo en Mérida"

    return limpiar_titulo(titulo, max_chars=68) or 'Empleo en Mérida'


def generar_alt_imagen(post, config_grupo=None, idx=0, total=1):
    """
    Genera alt text para una imagen.
    idx/total permiten diferenciar cuando hay múltiples fotos en el mismo post.
    """
    tipo = post.get('tipo') or post.get('_tipo_final') or post.get('tipo_detectado') or 'general'
    txt = post.get('texto_limpio') or post.get('texto') or post.get('descripcion') or post.get('texto_alerta') or ''
    ubic = extraer_ubicacion_simple(txt)

    if tipo == 'mascota':
        titulo = generar_titulo_mascota(post, post.get('categoria_id', 11)) or 'Mascota en Mérida'
        alt = f"Imagen de {titulo.lower()}"
    elif tipo == 'alerta':
        alt = "Imagen de alerta vecinal"
        if ubic:
            alt += f" en {ubic}"
    elif tipo == 'noticia':
        titulo = post.get('titulo') or generar_titulo_noticia_fallback(post) or 'Noticia local'
        alt = f"Imagen relacionada con {titulo.lower()}"
    elif tipo == 'perdido':
        titulo = generar_titulo_perdido(post) or 'Objeto perdido en Mérida'
        alt = f"Imagen de {titulo.lower()}"
    elif tipo == 'empleo':
        titulo = generar_titulo_empleo(post) or 'Empleo en Mérida'
        alt = f"Imagen de {titulo.lower()}"
    else:
        titulo = post.get('titulo') or generar_titulo_negocio(post, categoria_nombre='') or 'Publicación en Mérida'
        alt = f"Imagen de {titulo.lower()}"

    # Numerar cuando hay más de una foto
    if total > 1:
        alt = f"Foto {idx + 1} de {total} — {alt}"

    return limpiar_titulo(alt, max_chars=125)


def construir_public_id(post, img, meta=None, config_grupo=None, idx=0):
    tipo = post.get('tipo') or post.get('_tipo_final') or post.get('tipo_detectado') or 'general'
    txt = post.get('texto_limpio') or post.get('texto') or post.get('descripcion') or post.get('texto_alerta') or ''

    # Título según tipo
    if tipo == 'perdido':
        tema = slugify(generar_titulo_perdido(post) or 'objeto-perdido', max_words=8, max_len=55)
    elif tipo == 'empleo':
        tema = slugify(generar_titulo_empleo(post) or 'empleo', max_words=8, max_len=55)
    elif tipo == 'mascota':
        tema = slugify(generar_titulo_mascota(post, post.get('categoria_id', 11)) or 'mascota', max_words=8, max_len=55)
    elif tipo == 'alerta':
        tema = slugify(generar_titulo_alerta(post) or 'alerta', max_words=8, max_len=55)
    else:
        tema = slugify(post.get('titulo') or generar_titulo_negocio(post, categoria_nombre='') or 'post', max_words=8, max_len=55)

    ciudad = slugify((meta or {}).get('city') or 'merida', max_words=3, max_len=20)
    estado = slugify((meta or {}).get('state') or 'yucatan', max_words=3, max_len=20)
    zona = slugify(extraer_ubicacion_simple(txt) or 'general', max_words=5, max_len=30)
    img_id = None
    if isinstance(img, dict):
        img_id = img.get('fbid') or img.get('id')
    img_id = img_id or post.get('fbid_post') or f"{idx+1}"
    nombre = slugify(f"{tema} {ciudad} {estado} {zona} {img_id}", max_words=20, max_len=120)
    fecha = ((meta or {}).get("fecha_captura") or "")[:10] or "sin-fecha"
    return f"{tipo}/{fecha}/{nombre}"


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

KEYWORDS_EMPLEO = [
    'empleo', 'empleos', 'trabajo', 'trabajos', 'vacante', 'vacantes',
    'bolsa de trabajo', 'oportunidad laboral', 'busco trabajo', 'busco empleo',
    'oferta de trabajo', 'reclutamiento', 'contratación',
]

KEYWORDS_MASCOTAS = [
    'mascotas', 'mascota', 'adopcion', 'adopción', 'perros perdidos',
    'gatos perdidos', 'rescate animal', 'animales',
]

KEYWORDS_NEGOCIOS = [
    'negocios', 'compra', 'venta', 'anuncios', 'marketplace',
    'directorio', 'comercios', 'servicios',
]

KEYWORDS_PERDIDOS = [
    'objetos perdidos', 'cosas perdidas', 'perdidos y encontrados',
    'lost and found', 'perdido', 'encontrado',
]


def detectar_tipo_por_nombre(group_name):
    nombre_lower = (group_name or '').lower()
    # Empleo primero — evita falsos positivos con "seguridad" en noticias
    for kw in KEYWORDS_EMPLEO:
        if kw in nombre_lower:
            return 'empleo'
    for kw in KEYWORDS_PERDIDOS:
        if kw in nombre_lower:
            return 'perdidos'
    for kw in KEYWORDS_MASCOTAS:
        if kw in nombre_lower:
            return 'mascotas'
    for kw in KEYWORDS_NEGOCIOS:
        if kw in nombre_lower:
            return 'negocios'
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
