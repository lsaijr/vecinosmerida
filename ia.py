import itertools
import json
import os
import re
import time

from utils import (
    NEWS_MIN_WORDS, contar_palabras, es_post_consulta,
    generar_titulo_noticia_fallback, limpiar_titulo,
    parse_keywords, tiene_senal_comercial_fuerte,
    generar_titulo_negocio,
)

# ═══════════════════════════════════════════════════════════════
# ROTACIÓN DE KEYS
# ═══════════════════════════════════════════════════════════════

def _get_groq_keys():
    keys = []
    for var in ["GROQ_API_KEY", "GROQ_API_KEY_2", "GROQ_API_KEY_3"]:
        k = os.getenv(var)
        if k:
            keys.append(k)
    return keys if keys else [None]


def _get_gemini_keys():
    keys = []
    for var in ["GEMINI_API_KEY", "GEMINI_API_KEY_2"]:
        k = os.getenv(var)
        if k:
            keys.append(k)
    return keys if keys else [None]


_groq_cycle = None
_gemini_cycle = None


def _next_groq_key():
    global _groq_cycle
    if _groq_cycle is None:
        _groq_cycle = itertools.cycle(_get_groq_keys())
    return next(_groq_cycle)


def _next_gemini_key():
    global _gemini_cycle
    if _gemini_cycle is None:
        _gemini_cycle = itertools.cycle(_get_gemini_keys())
    return next(_gemini_cycle)


# ═══════════════════════════════════════════════════════════════
# CONTADOR DE COSTO REAL
# ═══════════════════════════════════════════════════════════════

_TOKENS = {
    "groq_input": 0,
    "groq_output": 0,
    "gemini_input": 0,
    "gemini_output": 0,
    "groq_calls": 0,
    "gemini_calls": 0,
}

# Precios Groq 70B (USD por millón de tokens)
_PRECIO_GROQ_INPUT  = 0.59
_PRECIO_GROQ_OUTPUT = 0.79
# Precios Gemini 2.5 Flash (USD por millón de tokens)
_PRECIO_GEMINI_INPUT  = 0.15
_PRECIO_GEMINI_OUTPUT = 0.60


def get_resumen_costo():
    """Retorna dict con tokens usados y costo estimado en USD."""
    costo_groq = (
        _TOKENS["groq_input"]  * _PRECIO_GROQ_INPUT  / 1_000_000 +
        _TOKENS["groq_output"] * _PRECIO_GROQ_OUTPUT / 1_000_000
    )
    costo_gemini = (
        _TOKENS["gemini_input"]  * _PRECIO_GEMINI_INPUT  / 1_000_000 +
        _TOKENS["gemini_output"] * _PRECIO_GEMINI_OUTPUT / 1_000_000
    )
    return {
        "groq_input_tokens":  _TOKENS["groq_input"],
        "groq_output_tokens": _TOKENS["groq_output"],
        "groq_calls":         _TOKENS["groq_calls"],
        "gemini_input_tokens":  _TOKENS["gemini_input"],
        "gemini_output_tokens": _TOKENS["gemini_output"],
        "gemini_calls":         _TOKENS["gemini_calls"],
        "costo_groq_usd":   round(costo_groq, 5),
        "costo_gemini_usd": round(costo_gemini, 5),
        "costo_total_usd":  round(costo_groq + costo_gemini, 5),
    }


def reset_contadores():
    for k in _TOKENS:
        _TOKENS[k] = 0


# ═══════════════════════════════════════════════════════════════
# INTERPRETADOR DE ERRORES
# ═══════════════════════════════════════════════════════════════

def interpretar_error(proveedor, error):
    msg = str(error).lower()
    if "quota" in msg or "resource_exhausted" in msg or "429" in msg:
        return f"{proveedor}: Se agotó la cuota de la API."
    if "api_key" in msg or "invalid" in msg or "unauthorized" in msg or "401" in msg:
        return f"{proveedor}: API key inválida o no configurada."
    if "timeout" in msg or "deadline" in msg:
        return f"{proveedor}: Timeout — la solicitud tardó demasiado."
    if "safety" in msg or "blocked" in msg or "harm" in msg:
        return f"{proveedor}: Contenido bloqueado por filtros de seguridad."
    if "context" in msg or "tokens" in msg or "length" in msg:
        return f"{proveedor}: Texto demasiado largo."
    if "model" in msg or "not found" in msg or "404" in msg:
        return f"{proveedor}: Modelo no disponible."
    if "rate" in msg or "limit" in msg:
        return f"{proveedor}: Límite de solicitudes alcanzado."
    if "connect" in msg or "network" in msg or "503" in msg:
        return f"{proveedor}: Error de conexión."
    return f"{proveedor}: Error inesperado — {str(error)[:120]}"


# ═══════════════════════════════════════════════════════════════
# PROMPTS
# ═══════════════════════════════════════════════════════════════

def _prompt_clasificar_tipo(texto):
    return f"""Clasifica el siguiente texto de Facebook en UNA sola categoría.

TEXTO:
{texto}

CATEGORÍAS POSIBLES:
- "negocio": venta de productos, servicios, ofertas comerciales, anuncios de negocios
- "noticia": eventos, accidentes, política, deportes, cultura, información de interés general
- "alerta": incidentes locales inmediatos (baches, robos, personas sospechosas, perros agresivos, fugas de agua, problemas de infraestructura en una colonia específica)
- "mascota": mascotas perdidas, encontradas o en adopción
- "ignorar": spam, contenido irrelevante, texto sin sentido, menos de 20 palabras útiles

Responde ÚNICAMENTE con una de estas palabras exactas: negocio, noticia, alerta, mascota, ignorar"""


def _prompt_negocio(texto, categorias):
    cats_str = ", ".join([f"{c['id']}:{c['nombre']}" for c in categorias])
    return f"""Analiza este texto de una publicación de Facebook de vecinos en Mérida, Yucatán.

TEXTO:
{texto}

INSTRUCCIONES:
1. categoria_id: Elige el número que mejor corresponda de esta lista: {cats_str}. Solo devuelve el número.
2. telefono: Extrae el número de 10 dígitos si existe, sino null.

Responde ÚNICAMENTE con un JSON válido:
{{"categoria_id": 12, "telefono": null}}"""


def _prompt_noticia(texto, categorias, modo="completa"):
    cats_str = ", ".join([f"{c['id']}:{c['nombre']}" for c in categorias])
    if modo == "ligera":
        return f"""Eres un editor de noticias locales de Mérida, Yucatán, México.

TEXTO ORIGINAL:
{texto}

REGLAS ESTRICTAS:
1. NO inventes hechos, causas, delitos, autoridades, consecuencias ni contexto no presente en el texto.
2. NO agregues información externa.
3. titulo: crea un título SEO claro y directo de máximo 90 caracteres. Sin emojis ni markdown.
4. texto: reescribe el contenido en 1 párrafo breve y fiel al original.
5. categoria_id: elige el número más adecuado de: {cats_str}

Responde ÚNICAMENTE con JSON válido:
{{"titulo":"...","texto":"...","categoria_id":1}}"""

    return f"""Eres un editor de noticias locales de Mérida, Yucatán, México.

TEXTO ORIGINAL:
{texto}

REGLAS ESTRICTAS:
1. NO inventes hechos, causas, delitos, autoridades, consecuencias ni contexto no presente en el texto.
2. NO agregues información externa.
3. titulo: escribe un titular optimizado para SEO, claro y directo. Máximo 90 caracteres. Sin emojis ni markdown.
4. texto: redacta la noticia manteniendo exclusivamente los hechos presentes en el texto original.
5. categoria_id: elige el número más adecuado de: {cats_str}

Responde ÚNICAMENTE con JSON válido:
{{"titulo":"...","texto":"...","categoria_id":1}}"""


def _prompt_titulo_negocio(texto, categoria_nombre=""):
    return f"""Genera un título MUY corto y claro para un directorio local de Mérida.

TEXTO:
{texto}

CONTEXTO:
Categoría sugerida: {categoria_nombre or 'General'}

REGLAS:
1. Máximo 6 palabras o 60 caracteres.
2. Debe sonar natural, específico y útil para SEO local.
3. No repitas palabras ni frases.
4. No uses saludos, relleno, preguntas ni emojis.
5. No inventes datos no presentes en el texto.
6. Evita títulos genéricos como "Negocio local en Mérida".
7. Si el post es una consulta o sugerencia y NO una oferta clara, responde exactamente: IGNORAR
8. Responde solo con un JSON con la clave titulo. Sin markdown, sin bloques de código.

Ejemplos válidos:
{{"titulo":"Fresas con crema en Mérida"}}
{{"titulo":"Clases de baile en Mérida"}}
{{"titulo":"Baterías Optima en Mérida"}}

Responde SOLO con JSON válido:
{{"titulo":"..."}}"""


def _prompt_alerta(texto, cat_alertas):
    padres = [c for c in cat_alertas if c["parent_id"] is None]
    cats_str = ""
    for p in padres:
        hijos = [c for c in cat_alertas if c["parent_id"] == p["id"]]
        hijos_str = ", ".join([f"{h['id']}:{h['nombre']}" for h in hijos])
        cats_str += f"\n  {p['id']}:{p['nombre']} → subcategorías: {hijos_str}"

    return f"""Eres un sistema que procesa alertas ciudadanas de vecinos de Mérida, Yucatán.

TEXTO:
{texto}

INSTRUCCIONES:
1. texto_alerta: resume la alerta en 1-2 oraciones claras. Máximo 200 caracteres. Sin emojis.
2. categoria_id: elige el ID de la subcategoría más específica de esta lista:{cats_str}
3. direccion_aprox: si el texto menciona una calle, colonia o lugar específico, extráelo. Si no, null.

Responde ÚNICAMENTE con JSON válido:
{{"texto_alerta":"...","categoria_id":6,"direccion_aprox":null}}"""


# ═══════════════════════════════════════════════════════════════
# RATE LIMIT / CACHÉ
# ═══════════════════════════════════════════════════════════════

_groq_last_call   = {}
_gemini_last_call = {}

# Con Groq de pago: sin restricciones reales, mínimo simbólico
GROQ_MIN_INTERVAL   = 0.05
GEMINI_MIN_INTERVAL = 0.3
MAX_RETRIES = 3

_CACHE_CLASIFICAR = {}
_CACHE_NEGOCIO    = {}
_CACHE_ALERTA     = {}
_CACHE_NOTICIA    = {}
_CACHE_TITULOS    = {}


def _esperar_key(last_calls, key, intervalo):
    ultimo = last_calls.get(key, 0)
    transcurrido = time.time() - ultimo
    if transcurrido < intervalo:
        time.sleep(intervalo - transcurrido)
    last_calls[key] = time.time()


# ═══════════════════════════════════════════════════════════════
# GROQ — Llama 3.3 70B para todo (modo evaluación de costo real)
# ═══════════════════════════════════════════════════════════════

def _llamar_groq(prompt, temperatura=0.3, modelo="llama-3.3-70b-versatile"):
    ultimo_error = None
    for intento in range(MAX_RETRIES):
        key = _next_groq_key()
        _esperar_key(_groq_last_call, key, GROQ_MIN_INTERVAL)
        try:
            from groq import Groq
            resp = Groq(api_key=key).chat.completions.create(
                model=modelo,
                messages=[{"role": "user", "content": prompt}],
                temperature=temperatura,
                max_tokens=1200,
            )
            # Registrar tokens para cálculo de costo
            if resp.usage:
                _TOKENS["groq_input"]  += resp.usage.prompt_tokens
                _TOKENS["groq_output"] += resp.usage.completion_tokens
                _TOKENS["groq_calls"]  += 1
            return resp.choices[0].message.content.strip()
        except Exception as e:
            ultimo_error = e
            msg = str(e).lower()
            if any(x in msg for x in ["429", "quota", "rate", "limit", "exhausted"]):
                time.sleep(5 * (2 ** intento))
                continue
            raise e
    raise ultimo_error


# ═══════════════════════════════════════════════════════════════
# GEMINI — nuevo SDK google.genai (reemplaza google.generativeai deprecado)
# ═══════════════════════════════════════════════════════════════

def _llamar_gemini(prompt):
    ultimo_error = None
    for intento in range(MAX_RETRIES):
        key = _next_gemini_key()
        _esperar_key(_gemini_last_call, key, GEMINI_MIN_INTERVAL)
        try:
            from google import genai as google_genai
            client = google_genai.Client(api_key=key)
            resp = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
            )
            # Registrar tokens si están disponibles
            if hasattr(resp, 'usage_metadata') and resp.usage_metadata:
                _TOKENS["gemini_input"]  += getattr(resp.usage_metadata, 'prompt_token_count', 0) or 0
                _TOKENS["gemini_output"] += getattr(resp.usage_metadata, 'candidates_token_count', 0) or 0
                _TOKENS["gemini_calls"]  += 1
            return resp.text.strip()
        except Exception as e:
            ultimo_error = e
            msg = str(e).lower()
            if any(x in msg for x in ["429", "quota", "resource_exhausted", "rate"]):
                time.sleep(8 * (2 ** intento))
                continue
            raise e
    raise ultimo_error


# ═══════════════════════════════════════════════════════════════
# PARSEAR JSON SEGURO
# ═══════════════════════════════════════════════════════════════

def _parsear_json(texto):
    texto = (texto or '').strip()
    # Limpiar bloques markdown
    texto = re.sub(r"```(?:json)?", "", texto, flags=re.IGNORECASE)
    texto = re.sub(r"```", "", texto)
    texto = texto.strip()

    if not texto:
        return {}

    # Detectar respuesta IGNORAR directa
    if texto.upper().strip() == 'IGNORAR':
        return {'titulo': 'IGNORAR'}

    try:
        return json.loads(texto)
    except Exception:
        pass

    # Intentar extraer bloque JSON con regex
    m = re.search(r'\{.*\}', texto, re.S)
    if m:
        bloque = m.group(0)
        try:
            return json.loads(bloque)
        except Exception:
            pass

    # Fallback: extraer valor de titulo
    m = re.search(r'"titulo"\s*:\s*"([^"]+)"', texto, re.S)
    if m:
        return {'titulo': m.group(1)}

    return {'titulo': texto.strip()}


# ═══════════════════════════════════════════════════════════════
# CLASIFICAR TIPO DE POST
# ═══════════════════════════════════════════════════════════════

def clasificar_tipo(texto):
    key = (texto or '').strip().lower()
    if key in _CACHE_CLASIFICAR:
        return _CACHE_CLASIFICAR[key], None

    try:
        prompt = _prompt_clasificar_tipo(texto)
        resultado = _llamar_groq(prompt, temperatura=0.1)
        tipo = resultado.strip().lower()
        if tipo not in ["negocio", "noticia", "alerta", "mascota", "ignorar"]:
            tipo = "ignorar"
        _CACHE_CLASIFICAR[key] = tipo
        return tipo, None
    except Exception as e:
        return "ignorar", interpretar_error("Groq", e)


# ═══════════════════════════════════════════════════════════════
# DETECCIÓN DE CATEGORÍA POR KEYWORDS (sin IA)
# ═══════════════════════════════════════════════════════════════

def _contains_keyword_text(txt, kw):
    txt_n = (txt or '').lower()
    kw_n = (kw or '').lower().strip()
    if not kw_n:
        return False
    if ' ' in kw_n:
        return kw_n in txt_n
    return re.search(rf'(?<!\w){re.escape(kw_n)}(?!\w)', txt_n) is not None


def _detectar_categoria_negocio_keywords(texto, categorias):
    txt = (texto or '').lower()
    mejores = []
    mapa_por_nombre = {
        'comida': ['menu', 'menú', 'tacos', 'pizza', 'hamburguesa', 'frapp', 'smoothie',
                   'crepa', 'fresas con', 'postre', 'waffle', 'hotcake', 'panuchos', 'sopes',
                   'reposteria', 'antojo'],
        'salud': ['masaje', 'terapia', 'rehabilitacion', 'rehabilitación', 'psicolog',
                  'consultorio', 'doctor', 'nutricion', 'nutrición'],
        'ropa & accesorios': ['ropa', 'blusa', 'vestido', 'bolsa', 'accesorio',
                               'zapato', 'tenis', 'joyeria', 'joyería'],
        'inmobiliaria': ['se renta', 'renta casa', 'renta departamento', 'departamento',
                         'terreno', 'inmueble', 'alquiler', 'venta de casa', 'venta de terreno'],
        'belleza & estética': ['uñas', 'cabello', 'maquillaje', 'spa', 'estetica', 'estética', 'lifting'],
        'automotriz': ['auto', 'carro', 'pintura', 'ceramica', 'cerámica', 'mecanica',
                       'mecánica', 'bateria', 'llanta'],
        'servicios': ['fletes', 'mudanza', 'plomero', 'electricista', 'carpintero',
                      'reparacion', 'reparación', 'mantenimiento', 'envios', 'envíos'],
    }

    for cat in categorias:
        kws = parse_keywords(cat.get('keywords'))
        if not kws:
            kws = mapa_por_nombre.get((cat.get('nombre') or '').strip().lower(), [])
        score = sum(1 for kw in kws if kw and _contains_keyword_text(txt, kw))
        if score > 0:
            mejores.append((score, cat['id']))

    if mejores:
        mejores.sort(reverse=True)
        return mejores[0][1]
    return None


# ═══════════════════════════════════════════════════════════════
# TÍTULO DE NEGOCIO CON IA
# ═══════════════════════════════════════════════════════════════

def _titulo_pobre(titulo):
    """Detecta títulos genéricos o con artefactos que requieren reintento."""
    if not titulo:
        return True
    t = limpiar_titulo(titulo or '', max_chars=60)
    if not t:
        return True
    tn = t.lower().strip()
    # Títulos genéricos conocidos
    if tn in {
        'negocio local', 'negocio local en merida', 'negocio en merida',
        'negocio en merida', 'general', 'servicio', 'mascotas', 'alerta',
        'ignorar', 'negocio local en mérida', 'negocio en mérida'
    }:
        return True
    # Artefactos markdown / JSON que se filtraron mal
    if '```' in tn or re.search(r'\bjson\b', tn):
        return True
    # Demasiado largo
    if len(t.split()) > 8:
        return True
    return False


def generar_titulo_negocio_ia(post, categoria_nombre='', prefer='groq'):
    """
    Genera título con IA solo cuando el título rule-based es pobre.
    prefer='groq'  → intenta Groq 70B primero, Gemini como respaldo
    prefer='gemini' → intenta Gemini primero, Groq como respaldo
    """
    texto = post.get('texto_limpio') or post.get('texto') or ''
    if es_post_consulta(texto) and not tiene_senal_comercial_fuerte(post, texto):
        return None
    if contar_palabras(texto) < 5:
        return None

    cache_key = f"negocio::{(categoria_nombre or '').lower()}::{texto.strip().lower()}"
    if cache_key in _CACHE_TITULOS:
        return _CACHE_TITULOS[cache_key]

    prompt = _prompt_titulo_negocio(texto, categoria_nombre=categoria_nombre)
    proveedores = ['groq', 'gemini'] if prefer == 'groq' else ['gemini', 'groq']

    for proveedor in proveedores:
        try:
            raw = (_llamar_groq(prompt, temperatura=0.3)
                   if proveedor == 'groq'
                   else _llamar_gemini(prompt))
            if 'IGNORAR' in (raw or '').upper():
                return None
            datos = _parsear_json(raw)
            titulo_raw = datos.get('titulo', '') if isinstance(datos, dict) else str(datos)
            if 'IGNORAR' in (titulo_raw or '').upper():
                return None
            titulo = limpiar_titulo(titulo_raw, max_chars=60)
            if titulo and not _titulo_pobre(titulo):
                _CACHE_TITULOS[cache_key] = titulo
                return titulo
        except Exception:
            continue
    return None


# ═══════════════════════════════════════════════════════════════
# PROCESAR NEGOCIO
# ═══════════════════════════════════════════════════════════════

def procesar_negocio(post, categorias):
    texto = post["texto_limpio"]
    key = texto.strip().lower()
    if key in _CACHE_NEGOCIO:
        datos = dict(_CACHE_NEGOCIO[key])
        return {**post, **datos, "tipo": "negocio", "error_ia": None}

    # Intentar detectar categoría por keywords primero (sin IA)
    categoria_kw = _detectar_categoria_negocio_keywords(texto, categorias)
    if categoria_kw:
        datos = {"categoria_id": categoria_kw, "telefono": post.get("telefono")}
        _CACHE_NEGOCIO[key] = datos
        return {**post, **datos, "tipo": "negocio", "error_ia": None}

    # Fallback: IA con Groq 70B
    prompt = _prompt_negocio(texto, categorias)
    error_msg = None
    try:
        raw = _llamar_groq(prompt)
        datos = _parsear_json(raw)
    except Exception as e:
        error_msg = interpretar_error("Groq", e)
        datos = {"categoria_id": 12, "telefono": post.get("telefono")}

    _CACHE_NEGOCIO[key] = dict(datos)
    return {**post, **datos, "tipo": "negocio", "error_ia": error_msg}


# ═══════════════════════════════════════════════════════════════
# PROCESAR NOTICIA
# ═══════════════════════════════════════════════════════════════

def procesar_noticia(post, categorias, usar_gemini=False, modo="completa"):
    texto = post["texto_limpio"]
    cache_key = f"{modo}::{texto.strip().lower()}"
    if cache_key in _CACHE_NOTICIA:
        datos = dict(_CACHE_NOTICIA[cache_key])
        return {**post, **datos, "tipo": "noticia", "error_ia": None}

    prompt = _prompt_noticia(texto, categorias, modo=modo)
    error_msg = None

    if usar_gemini:
        try:
            raw = _llamar_gemini(prompt)
            datos = _parsear_json(raw)
        except Exception as e:
            error_msg = interpretar_error("Gemini", e)
            try:
                raw = _llamar_groq(prompt)
                datos = _parsear_json(raw)
                error_msg += " → Usando Groq 70B como respaldo."
            except Exception as e2:
                error_msg += f" | Groq también falló: {interpretar_error('Groq', e2)}"
                datos = {
                    "titulo": generar_titulo_noticia_fallback(post),
                    "texto": texto,
                    "categoria_id": 1,
                }
    else:
        try:
            raw = _llamar_groq(prompt)
            datos = _parsear_json(raw)
        except Exception as e:
            error_msg = interpretar_error("Groq", e)
            datos = {
                "titulo": generar_titulo_noticia_fallback(post),
                "texto": texto,
                "categoria_id": 1,
            }

    datos["titulo"] = datos.get("titulo") or generar_titulo_noticia_fallback(post)
    datos["texto"]  = datos.get("texto")  or texto
    if len(datos["texto"].split()) < min(NEWS_MIN_WORDS, 25):
        datos["texto"] = texto

    _CACHE_NOTICIA[cache_key] = dict(datos)
    return {**post, **datos, "tipo": "noticia", "error_ia": error_msg}


# ═══════════════════════════════════════════════════════════════
# PROCESAR ALERTA
# ═══════════════════════════════════════════════════════════════

def procesar_alerta(post, cat_alertas):
    texto = post["texto_limpio"]
    key = texto.strip().lower()
    if key in _CACHE_ALERTA:
        datos = dict(_CACHE_ALERTA[key])
        return {**post, **datos, "tipo": "alerta", "error_ia": None}

    prompt = _prompt_alerta(texto, cat_alertas)
    error_msg = None
    try:
        raw = _llamar_groq(prompt)
        datos = _parsear_json(raw)
    except Exception as e:
        error_msg = interpretar_error("Groq", e)
        datos = {"texto_alerta": texto[:200], "categoria_id": None, "direccion_aprox": None}

    _CACHE_ALERTA[key] = dict(datos)
    return {**post, **datos, "tipo": "alerta", "error_ia": error_msg}


# ═══════════════════════════════════════════════════════════════
# DECISOR GEMINI vs GROQ PARA NOTICIAS
# ═══════════════════════════════════════════════════════════════

def debe_usar_gemini(texto, categoria_id=None):
    """Usa Gemini para noticias largas o de alta importancia."""
    palabras = len((texto or '').split())
    if palabras >= 140:
        return True
    if categoria_id in [1, 2]:  # Política, Seguridad
        return True
    return False
