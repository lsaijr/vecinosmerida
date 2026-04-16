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
    NEGOCIO_CATEGORIA_KEYWORDS,
)

# ═══════════════════════════════════════════════════════════════
# ROTACIÓN DE KEYS
# ═══════════════════════════════════════════════════════════════

def _get_groq_keys():
    keys = []
    # Prioridad 1: fallback en código
    try:
        from config_keys import GROQ_FALLBACK_KEY
        if GROQ_FALLBACK_KEY:
            keys.append(GROQ_FALLBACK_KEY)
    except ImportError:
        pass
    # Prioridad 2: variables de entorno (sin GROQ_API_KEY — está corrupta en Railway)
    for var in ["GROQ_KEY_MAIN", "GROQ_API_KEY_VM", "GROQ_API_KEY_VM_2", "GROQ_API_KEY_VM_3"]:
        k = os.getenv(var)
        if k and k not in keys:
            keys.append(k)
    return keys if keys else [None]


def _get_sambanova_key():
    """Obtiene la key de SambaNova para usar como fallback."""
    k = os.getenv("SAMNV_API_KEY") or os.getenv("SAMBANOVA_API_KEY") or ""
    if not k:
        try:
            from config_keys import SAMBANOVA_FALLBACK_KEY
            k = SAMBANOVA_FALLBACK_KEY or ""
        except (ImportError, AttributeError):
            pass
    return k or None


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

# Precios Groq Llama 70B (USD por millón de tokens)
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

def _prompt_clasificar_tipo(texto, grupo_tipo="vecinos", grupo_nombre=""):
    contexto_grupo = {
        "vecinos": "grupo de vecinos de una colonia en Mérida, Yucatán. Mezcla de negocios, alertas, noticias locales y mascotas.",
        "noticias": "grupo de noticias locales de Mérida y Yucatán. La mayoría son noticias; pocos posts son negocios.",
        "mascotas": "grupo dedicado a mascotas perdidas, encontradas y en adopción en Mérida. La mayoría son mascotas, pero también hay denuncias de maltrato (→alerta) y ventas de accesorios (→negocio).",
        "negocios": "grupo de compra-venta y negocios locales de Mérida. La gran mayoría son negocios.",
        "empleo": "grupo de empleo y trabajo en Mérida. Todo post válido es empleo o ignorar.",
        "perdidos": "grupo de objetos perdidos y encontrados en Mérida. Documentos, celulares, llaves, carteras, vehículos. También mascotas perdidas.",
    }.get(grupo_tipo, "grupo de Facebook de vecinos en Mérida, Yucatán.")

    return f"""Clasifica el siguiente texto de Facebook en UNA sola categoría.

CONTEXTO: Es un post de un {contexto_grupo}{f' Nombre del grupo: {grupo_nombre}.' if grupo_nombre else ''}

TEXTO:
{texto}

CATEGORÍAS:
- "negocio": venta de productos/servicios, ofertas, anuncios de negocios, precios
- "noticia": eventos, hechos informativos de Mérida/Yucatán. ⚠ SOLO si tiene ≥70 palabras.
- "alerta": incidentes, denuncias, maltrato animal, estafas, fraudes, peligro. Incluye denuncias implícitas con indignación por un hecho concreto.
- "mascota": mascotas perdidas, encontradas, adopción, rescate, ayuda médica. ⚠ "Apoya a [animal] para cirugía" = mascota, NO negocio.
- "empleo": ofertas o búsquedas de trabajo, vacantes, solicitudes de personal.
- "perdido": objetos perdidos o encontrados (documentos, celulares, llaves, carteras, NO mascotas). Si es mascota perdida → "mascota".
- "ignorar": spam, reflexiones filosóficas sin denuncia concreta, posts sobre otro post, saludos.

REGLAS:
1. Texto < 70 palabras → JAMÁS "noticia"
2. Reflexión filosófica sobre animales SIN denuncia concreta → "ignorar", no "mascota"
3. Denuncia de maltrato (explícita o implícita) → "alerta", no "mascota"
4. "Apoya/ayuda a [animal] para cirugía/tratamiento" → "mascota", no "negocio"
5. Objeto perdido/encontrado → "perdido". Mascota perdida → "mascota"

Responde ÚNICAMENTE con una de estas palabras: negocio, noticia, alerta, mascota, empleo, perdido, ignorar"""


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

    ejemplos_titulos = """
EJEMPLOS DE TÍTULOS CORRECTOS:
✓ "Incendio en predio de la calle 64 moviliza a bomberos de Mérida"
✓ "Vecinos de Francisco de Montejo denuncian baches en avenida principal"
✓ "Joven es detenido tras robo en colonia Nueva Sambulá"
✓ "Ayuntamiento de Mérida anuncia corte de agua en 5 colonias"
✓ "Festival gratuito en el parque de San Sebastián este 25 de abril"

EJEMPLOS DE TÍTULOS INCORRECTOS (NO hagas esto):
✗ "Noticia importante de Mérida" — demasiado genérico
✗ "Vecinos reportan situación" — no dice qué pasó
✗ "Incidente ocurrido el día de hoy en la ciudad" — vago
✗ "URGENTE: Situación de emergencia" — sensacionalista y vacío"""

    if modo == "ligera":
        return f"""Eres un editor de noticias locales de Mérida, Yucatán, México.

TEXTO ORIGINAL:
{texto}
{ejemplos_titulos}

REGLAS:
1. titulo: titular SEO de máximo 90 caracteres. Incluye el QUIÉN, QUÉ y DÓNDE si están en el texto.
2. NO inventes hechos, causas ni consecuencias ausentes en el texto.
3. texto: 1 párrafo breve y fiel al original. No copies literalmente.
4. categoria_id: elige de: {cats_str}

Responde ÚNICAMENTE con JSON válido:
{{"titulo":"...","texto":"...","categoria_id":1}}"""

    return f"""Eres un editor de noticias locales de Mérida, Yucatán, México.

TEXTO ORIGINAL:
{texto}
{ejemplos_titulos}

REGLAS:
1. titulo: titular SEO de máximo 90 caracteres. Incluye el QUIÉN, QUÉ y DÓNDE presentes en el texto.
2. NO inventes hechos, causas, delitos, autoridades, consecuencias ni contexto ausentes en el texto.
3. texto: redacta la noticia con los hechos del texto original. Mínimo 2 oraciones. No copies literalmente.
4. categoria_id: elige de: {cats_str}

Responde ÚNICAMENTE con JSON válido:
{{"titulo":"...","texto":"...","categoria_id":1}}"""


def _prompt_titulo_negocio(texto, categoria_nombre=""):
    return f"""Eres un editor de directorios locales de Mérida, Yucatán. Tu tarea es generar un título corto, claro y útil para una publicación de Facebook de un negocio o servicio local.

TEXTO DEL POST:
{texto}

CATEGORÍA DETECTADA: {categoria_nombre or 'General'}

REGLAS:
1. Máximo 7 palabras. Máximo 65 caracteres.
2. Describe QUÉ ofrece el negocio. Nunca copies frases del texto — extrae el servicio.
3. El título debe tener sentido COMPLETO. Nunca termines con: el, la, los, las, un, una, de, en, a, con, por, para, que, y, o, su, tu, más, te, se.
4. No uses saludos, preguntas, emojis ni puntuación final.
5. No inventes datos ausentes en el texto.
6. Incluye zona o colonia SOLO si está explícitamente mencionada en el texto.
7. Si el post es una consulta o pregunta (no una oferta), responde exactamente: IGNORAR

EJEMPLOS CORRECTOS:
{{"titulo": "Plomería y fontanería en Mérida"}}
{{"titulo": "Instalación de cámaras de seguridad"}}
{{"titulo": "Clases de inglés en Francisco de Montejo"}}
{{"titulo": "Fumigación y control de plagas"}}
{{"titulo": "Repostería artesanal a domicilio"}}
{{"titulo": "Renta de inflables para fiestas"}}
{{"titulo": "Cerrajería 24 horas en Mérida"}}
{{"titulo": "Apoyo educativo para niños"}}
{{"titulo": "Venta de celulares a crédito"}}
{{"titulo": "Impermeabilización de techos"}}

EJEMPLOS INCORRECTOS — NO hagas esto:
{{"titulo": "Te Proporcionamos Un Espacio Que"}}  <- cortado, termina en pronombre
{{"titulo": "Vive Una Experiencia Única Tus"}}  <- cortado, termina en pronombre
{{"titulo": "Sabor Que Nos Caracteriza Contamos"}}  <- frase sin sentido
{{"titulo": "Negocio local en Mérida"}}  <- demasiado genérico
{{"titulo": "Servicio"}}  <- demasiado corto

Responde ÚNICAMENTE con JSON válido:
{{"titulo": "..."}}"""


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
# GROQ — llama-3.3-70b-versatile
# Kimi K2 (moonshotai/kimi-k2-instruct-0905) dado de baja 2026-04-14
# ═══════════════════════════════════════════════════════════════
_GROQ_MODEL_DEFAULT = "llama-3.3-70b-versatile"

def _llamar_groq_raw(prompt, temperatura=0.3, modelo=None):
    """Llama a Groq directamente (sin fallback)."""
    if modelo is None:
        modelo = _GROQ_MODEL_DEFAULT
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


def _llamar_groq(prompt, temperatura=0.3, modelo=None):
    """Llama a Groq, si falla intenta SambaNova como fallback."""
    try:
        return _llamar_groq_raw(prompt, temperatura=temperatura, modelo=modelo)
    except Exception as e_groq:
        sn_key = _get_sambanova_key()
        if sn_key:
            try:
                return _llamar_sambanova(prompt, temperatura=temperatura)
            except Exception as e_sn:
                raise Exception(f"Groq: {e_groq} | SambaNova: {e_sn}")
        raise e_groq


# ═══════════════════════════════════════════════════════════════
# SAMBANOVA — DeepSeek V3.1 (OpenAI-compatible, fallback de Groq)
# ═══════════════════════════════════════════════════════════════
_SAMBANOVA_MODEL = "DeepSeek-V3.1"
_sambanova_last_call = {}

def _llamar_sambanova(prompt, temperatura=0.3, modelo=None):
    """Llama a SambaNova como fallback cuando Groq falla."""
    import json as _json
    import urllib.request
    import urllib.error

    key = _get_sambanova_key()
    if not key:
        raise Exception("SambaNova API key no configurada (SAMNV_API_KEY)")

    if modelo is None:
        modelo = _SAMBANOVA_MODEL

    _esperar_key(_sambanova_last_call, key, 1.0)

    body = _json.dumps({
        "model": modelo,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": temperatura,
        "max_tokens": 1200,
    }).encode("utf-8")

    req = urllib.request.Request(
        "https://api.sambanova.ai/v1/chat/completions",
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {key}",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = _json.loads(resp.read().decode("utf-8"))
        return data["choices"][0]["message"]["content"].strip()
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8", errors="ignore")
        raise Exception(f"SambaNova HTTP {e.code}: {error_body[:200]}")


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

def clasificar_tipo(texto, grupo_tipo="vecinos", grupo_nombre=""):
    key = (texto or '').strip().lower()
    if key in _CACHE_CLASIFICAR:
        return _CACHE_CLASIFICAR[key], None

    try:
        prompt = _prompt_clasificar_tipo(texto, grupo_tipo=grupo_tipo, grupo_nombre=grupo_nombre)
        resultado = _llamar_groq(prompt, temperatura=0.1)
        tipo = resultado.strip().lower()
        if tipo not in ["negocio", "noticia", "alerta", "mascota", "empleo", "perdido", "ignorar"]:
            tipo = "ignorar"
        _CACHE_CLASIFICAR[key] = tipo
        return tipo, None
    except Exception as e:
        return "ignorar", interpretar_error("IA", e)


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
    """
    Detecta la categoría de un negocio por keywords sin usar IA.
    Prioridad:
    1. keywords de la DB (columna 'keywords' en cat_categorias)
    2. NEGOCIO_CATEGORIA_KEYWORDS — mapa robusto en código (fallback)
    3. None → la IA decidirá
    """
    txt = (texto or '').lower()
    mejores = []

    for cat in categorias:
        nombre_cat = (cat.get('nombre') or '').strip().lower()

        # 1. Keywords de la DB (mayor prioridad)
        kws_db = parse_keywords(cat.get('keywords'))

        # 2. Fallback: buscar por nombre normalizado en NEGOCIO_CATEGORIA_KEYWORDS
        kws_fallback = []
        if not kws_db:
            for clave, kws in NEGOCIO_CATEGORIA_KEYWORDS.items():
                # Match si el nombre de la categoría contiene la clave o viceversa
                if clave in nombre_cat or nombre_cat in clave:
                    kws_fallback = kws
                    break

        kws = kws_db or kws_fallback
        if not kws:
            continue

        score = sum(1 for kw in kws if kw and _contains_keyword_text(txt, kw))
        if score > 0:
            # Bonus: keywords de DB tienen más peso que fallback
            peso_extra = 10 if kws_db else 0
            mejores.append((score + peso_extra, cat['id']))

    if mejores:
        mejores.sort(reverse=True)
        return mejores[0][1]
    return None


# ═══════════════════════════════════════════════════════════════
# TÍTULO DE NEGOCIO CON IA
# ═══════════════════════════════════════════════════════════════

def _titulo_pobre(titulo):
    """Detecta títulos genéricos, cortados o con artefactos."""
    if not titulo:
        return True
    t = limpiar_titulo(titulo or '', max_chars=60)
    if not t:
        return True
    tn = t.lower().strip()

    # Títulos genéricos conocidos
    if tn in {
        'negocio local', 'negocio local en merida', 'negocio en merida',
        'general', 'servicio', 'mascotas', 'alerta', 'ignorar',
        'negocio local en mérida', 'negocio en mérida', 'servicio domicilio',
    }:
        return True

    # Artefactos markdown / JSON
    if '```' in tn or re.search(r'\bjson\b', tn):
        return True

    # Demasiado largo
    if len(t.split()) > 8:
        return True

    # Título cortado: termina en palabra vacía (artículo, prep, pronombre, conjunción)
    PALABRAS_VACIAS = {
        'el', 'la', 'los', 'las', 'un', 'una', 'unos', 'unas',
        'de', 'del', 'en', 'a', 'con', 'por', 'para', 'que', 'y',
        'o', 'e', 'ni', 'su', 'sus', 'tu', 'tus', 'mi', 'mis',
        'te', 'se', 'me', 'nos', 'le', 'les', 'más', 'mas',
        'es', 'son', 'al', 'ante', 'bajo', 'sin', 'sobre',
    }
    ultima_palabra = tn.split()[-1].rstrip('.,;:') if tn.split() else ''
    if ultima_palabra in PALABRAS_VACIAS:
        return True

    # Demasiado corto (1 sola palabra genérica)
    palabras = t.split()
    if len(palabras) == 1 and len(t) < 6:
        return True

    return False


def _debe_usar_gemini_titulo(texto):
    """
    Gemini para títulos cuando el texto es corto o tiene poca claridad.
    Groq cuando hay contexto suficiente para entender el servicio.
    """
    palabras = contar_palabras(texto or '')

    # Texto muy corto — poco contexto para Groq
    if palabras < 20:
        return True

    # Texto con muchos errores tipográficos o palabras raras
    # Detectar ratio de palabras "raras" (>12 chars sin ser URLs ni teléfonos)
    t = (texto or '').lower()
    tokens = [w for w in t.split() if not w.startswith('http') and not w.isdigit()]
    palabras_largas_raras = sum(
        1 for w in tokens
        if len(w) > 12 and not any(c.isdigit() for c in w)
    )
    if tokens and palabras_largas_raras / max(len(tokens), 1) > 0.15:
        return True

    # Texto con muchas mayúsculas sueltas (stickers, decoración)
    mayus_sueltas = sum(1 for w in (texto or '').split() if w.isupper() and len(w) <= 3)
    if mayus_sueltas >= 4:
        return True

    return False


def generar_titulo_negocio_ia(post, categoria_nombre='', prefer='groq'):
    """
    Genera título con IA.
    - Texto corto o poco claro → Gemini primero (mejor inferencia)
    - Texto normal → Groq primero (más rápido, sin rate limit)
    """
    texto = post.get('texto_limpio') or post.get('texto') or ''
    if es_post_consulta(texto) and not tiene_senal_comercial_fuerte(post, texto):
        return None
    if contar_palabras(texto) < 5:
        return None

    cache_key = f"negocio::{(categoria_nombre or '').lower()}::{texto.strip().lower()}"
    if cache_key in _CACHE_TITULOS:
        return _CACHE_TITULOS[cache_key]

    # Decidir proveedor según claridad del texto
    if prefer == 'groq' and _debe_usar_gemini_titulo(texto):
        prefer = 'gemini'

    prompt = _prompt_titulo_negocio(texto, categoria_nombre=categoria_nombre)
    proveedores = ['gemini', 'groq'] if prefer == 'gemini' else ['groq', 'gemini']

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
    """
    Usa Gemini para noticias largas o que requieren mejor redacción.
    categoria_id se reserva para uso futuro cuando se conozca antes del procesamiento.
    """
    palabras = len((texto or '').split())
    if palabras >= 140:
        return True
    # Señales de alta importancia que justifican Gemini
    t = (texto or '').lower()
    if any(kw in t for kw in ['homicidio', 'feminicidio', 'balacera', 'asesinato',
                                'gobernador', 'alcalde', 'congreso', 'reforma',
                                'corrupción', 'corrupcion', 'detención masiva']):
        return True
    return False


# ═══════════════════════════════════════════════════════════════
# EMPLEO — PROMPT Y PROCESAMIENTO
# ═══════════════════════════════════════════════════════════════

_CACHE_EMPLEO = {}


def _prompt_empleo(texto, tipo_empleo='oferta'):
    """Prompt para extraer puesto y área de un post de empleo."""
    if tipo_empleo == 'oferta':
        instruccion = (
            "Extrae el puesto de trabajo ofertado. "
            "Si hay varios puestos, únelos con coma (máximo 3)."
        )
    else:
        instruccion = (
            "Describe en pocas palabras el perfil laboral que busca esta persona. "
            "Ejemplo: 'Cajera con experiencia', 'Cocinero turno matutino'."
        )

    return f"""Eres un asistente que procesa anuncios de empleo en Mérida, Yucatán.

TEXTO DEL ANUNCIO:
{texto[:400]}

INSTRUCCIONES:
1. puesto: {instruccion} Máximo 6 palabras. Sin puntuación final.
   Si no puedes determinarlo con certeza, responde: GENERAL
2. area: Elige UNA de estas áreas según el puesto:
   Cocina, Ventas, Seguridad, Transporte, Administrativo,
   Limpieza, Construcción, Salud, Tecnología, Educación, Almacén, General

Responde ÚNICAMENTE con JSON válido:
{{"puesto": "...", "area": "..."}}"""


def procesar_empleo(post, tipo_empleo='oferta'):
    """
    Procesa un post de empleo:
    - Limpia el texto con limpiar_texto_empleo()
    - Extrae horario y zona por keywords (sin IA)
    - Extrae puesto y área con Groq (con caché)
    - Fallback a keywords si Groq falla
    """
    from utils import (
        limpiar_texto_empleo, get_empleo_area,
        extraer_horario_empleo, extraer_zona_empleo,
    )

    texto_orig  = post.get('texto_limpio') or post.get('texto') or ''
    texto_clean = limpiar_texto_empleo(texto_orig)
    cache_key   = f"empleo::{tipo_empleo}::{texto_orig.strip().lower()}"

    horario = extraer_horario_empleo(texto_orig)
    zona    = extraer_zona_empleo(texto_orig)
    area_kw, icon_kw, color_kw = get_empleo_area(texto_orig)

    if cache_key in _CACHE_EMPLEO:
        datos = dict(_CACHE_EMPLEO[cache_key])
        return {
            **post,
            'tipo':           'empleo',
            'tipo_empleo':    tipo_empleo,
            'descripcion':    texto_clean,
            'horario':        horario,
            'zona':           zona,
            'area':           datos.get('area', area_kw),
            'icon':           icon_kw,
            'color':          color_kw,
            'puesto':         datos.get('puesto'),
            'error_ia':       None,
        }

    # Intentar con Groq
    error_msg = None
    puesto    = None
    area      = area_kw

    try:
        prompt  = _prompt_empleo(texto_clean, tipo_empleo)
        raw     = _llamar_groq(prompt, temperatura=0.1)
        datos   = _parsear_json(raw)
        puesto_raw = (datos.get('puesto') or '').strip()
        area_raw   = (datos.get('area')   or '').strip()

        if puesto_raw.upper() != 'GENERAL' and len(puesto_raw.split()) <= 8:
            puesto = puesto_raw
        if area_raw in {
            'Cocina', 'Ventas', 'Seguridad', 'Transporte', 'Administrativo',
            'Limpieza', 'Construcción', 'Salud', 'Tecnología', 'Educación',
            'Almacén', 'General'
        }:
            area = area_raw

        _CACHE_EMPLEO[cache_key] = {'puesto': puesto, 'area': area}

    except Exception as e:
        error_msg = interpretar_error('Groq', e)

    # Actualizar icono/color según área final
    from utils import EMPLEO_AREAS
    cfg   = EMPLEO_AREAS.get(area, {})
    icon  = cfg.get('icon', icon_kw)
    color = cfg.get('color', color_kw)

    return {
        **post,
        'tipo':        'empleo',
        'tipo_empleo': tipo_empleo,
        'descripcion': texto_clean,
        'horario':     horario,
        'zona':        zona,
        'area':        area,
        'icon':        icon,
        'color':       color,
        'puesto':      puesto,
        'error_ia':    error_msg,
    }
