import itertools
import json
import os
import re
import time

from utils import NEWS_MIN_WORDS, contar_palabras, es_post_consulta, generar_titulo_noticia_fallback, limpiar_titulo, parse_keywords

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
# INTERPRETADOR DE ERRORES
# ═══════════════════════════════════════════════════════════════

def interpretar_error(proveedor, error):
    msg = str(error).lower()
    if "quota" in msg or "resource_exhausted" in msg or "429" in msg:
        return f"{proveedor}: Se agotó la cuota de la API. Intenta en unos minutos o revisa tu plan."
    if "api_key" in msg or "invalid" in msg or "unauthorized" in msg or "401" in msg:
        return f"{proveedor}: API key inválida o no configurada. Revisa la variable de entorno."
    if "timeout" in msg or "deadline" in msg:
        return f"{proveedor}: La solicitud tardó demasiado (timeout)."
    if "safety" in msg or "blocked" in msg or "harm" in msg:
        return f"{proveedor}: El contenido fue bloqueado por filtros de seguridad."
    if "context" in msg or "tokens" in msg or "length" in msg:
        return f"{proveedor}: El texto es demasiado largo para procesar."
    if "model" in msg or "not found" in msg or "404" in msg:
        return f"{proveedor}: Modelo no disponible. Verifica el nombre del modelo."
    if "rate" in msg or "limit" in msg:
        return f"{proveedor}: Límite de solicitudes alcanzado."
    if "connect" in msg or "network" in msg or "503" in msg:
        return f"{proveedor}: Error de conexión con el servidor de IA."
    return f"{proveedor}: Error inesperado — {str(error)[:120]}"


# ═══════════════════════════════════════════════════════════════
# PROMPTS
# ═══════════════════════════════════════════════════════════════

def _prompt_clasificar_tipo(texto):
    return f"""Clasifica el siguiente texto de Facebook en UNA sola categoría.

TEXTO:
{texto}

CATEGORÍAS POSIBLES:
- \"negocio\": venta de productos, servicios, ofertas comerciales, anuncios de negocios
- \"noticia\": eventos, accidentes, política, deportes, cultura, información de interés general
- \"alerta\": incidentes locales inmediatos (baches, robos, personas sospechosas, perros agresivos, fugas de agua, problemas de infraestructura en una colonia específica)
- \"mascota\": mascotas perdidas, encontradas o en adopción
- \"ignorar\": spam, contenido irrelevante, texto sin sentido, menos de 20 palabras útiles

Responde ÚNICAMENTE con una de estas palabras exactas: negocio, noticia, alerta, mascota, ignorar"""


def _prompt_limpiar_texto(texto):
    return f"""Limpia y corrige el siguiente texto respetando estas reglas ESTRICTAMENTE:

REGLAS:
1. Corrige ortografía y acentos.
2. Corrige mayúsculas: solo al inicio de oración y después de punto.
3. Elimina emojis, hashtags y signos repetidos.
4. NO reescribas ni cambies el significado.
5. NO agregues información nueva.
6. Devuelve el texto completo, solo limpio.

TEXTO:
{texto}

Responde ÚNICAMENTE con el texto limpio, sin comillas ni markdown."""


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
3. titulo: crea un título SEO claro y directo de máximo 90 caracteres.
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
3. titulo: escribe un titular optimizado para SEO, claro y directo. Máximo 90 caracteres. Sin clickbait.
4. texto: redacta la noticia manteniendo exclusivamente los hechos presentes en el texto original. Si falta contexto, mantén una nota breve y fiel; no rellenes huecos.
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
2. Debe sonar natural y específico.
3. No repitas palabras.
4. No uses saludos, relleno ni preguntas.
5. No inventes datos no presentes.
6. Si el texto es solo una consulta o recomendación y no una oferta real, responde exactamente: IGNORAR

Responde SOLO con JSON válido:
{{"titulo":"..."}}"""


def _titulo_pobre(titulo):
    t = (titulo or '').strip()
    if not t:
        return True
    tn = t.lower()
    if tn in {'negocio local', 'general', 'servicio', 'mascotas', 'alerta'}:
        return True
    if len(t.split()) > 8:
        return True
    return False


def generar_titulo_negocio_ia(post, categoria_nombre='', prefer='gemini'):
    texto = post.get('texto_limpio') or post.get('texto') or ''
    if es_post_consulta(texto):
        return None
    if contar_palabras(texto) < 12:
        return None

    cache_key = f"negocio::{(categoria_nombre or '').lower()}::{texto.strip().lower()}"
    if cache_key in _CACHE_TITULOS:
        return _CACHE_TITULOS[cache_key]

    prompt = _prompt_titulo_negocio(texto, categoria_nombre=categoria_nombre)
    proveedores = ['gemini', 'groq'] if prefer == 'gemini' else ['groq', 'gemini']

    for proveedor in proveedores:
        try:
            if proveedor == 'gemini':
                raw = _llamar_gemini(prompt)
            else:
                raw = _llamar_groq(prompt, temperatura=0.2, modelo='llama-3.1-8b-instant')
            if raw.strip().upper() == 'IGNORAR':
                return None
            try:
                titulo = _parsear_json(raw).get('titulo', '')
            except Exception:
                titulo = raw
            titulo = limpiar_titulo(titulo, max_chars=60)
            if titulo and not _titulo_pobre(titulo):
                _CACHE_TITULOS[cache_key] = titulo
                return titulo
        except Exception:
            continue
    return None


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
1. texto_alerta: resume la alerta en 1-2 oraciones claras. Máximo 200 caracteres.
2. categoria_id: elige el ID de la subcategoría más específica de esta lista:{cats_str}
3. direccion_aprox: si el texto menciona una calle, colonia o lugar específico, extráelo. Si no, null.

Responde ÚNICAMENTE con JSON válido:
{{"texto_alerta":"...","categoria_id":6,"direccion_aprox":null}}"""


# ═══════════════════════════════════════════════════════════════
# RATE LIMIT / CACHÉ
# ═══════════════════════════════════════════════════════════════

_groq_last_call = {}
_gemini_last_call = {}
GROQ_MIN_INTERVAL = 0.2
GEMINI_MIN_INTERVAL = 0.5
MAX_RETRIES = 3

_CACHE_CLASIFICAR = {}
_CACHE_NEGOCIO = {}
_CACHE_ALERTA = {}
_CACHE_NOTICIA = {}
_CACHE_LIMPIEZA = {}
_CACHE_TITULOS = {}


def _esperar_key(last_calls, key, intervalo):
    ultimo = last_calls.get(key, 0)
    transcurrido = time.time() - ultimo
    if transcurrido < intervalo:
        time.sleep(intervalo - transcurrido)
    last_calls[key] = time.time()


def _llamar_groq(prompt, temperatura=0.3, modelo="llama-3.1-8b-instant"):
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
                max_tokens=1000,
            )
            return resp.choices[0].message.content.strip()
        except Exception as e:
            ultimo_error = e
            msg = str(e).lower()
            if any(x in msg for x in ["429", "quota", "rate", "limit", "exhausted"]):
                time.sleep(5 * (2 ** intento))
                continue
            raise e
    raise ultimo_error


def _llamar_gemini(prompt):
    ultimo_error = None
    for intento in range(MAX_RETRIES):
        key = _next_gemini_key()
        _esperar_key(_gemini_last_call, key, GEMINI_MIN_INTERVAL)
        try:
            import google.generativeai as genai
            genai.configure(api_key=key)
            resp = genai.GenerativeModel("gemini-2.5-flash").generate_content(prompt)
            return resp.text.strip()
        except Exception as e:
            ultimo_error = e
            msg = str(e).lower()
            if any(x in msg for x in ["429", "quota", "resource_exhausted", "rate"]):
                time.sleep(8 * (2 ** intento))
                continue
            raise e
    raise ultimo_error


def _parsear_json(texto):
    texto = re.sub(r"```json|```", "", texto).strip()
    return json.loads(texto)


# ═══════════════════════════════════════════════════════════════
# FUNCIONES PÚBLICAS
# ═══════════════════════════════════════════════════════════════

def clasificar_tipo(texto):
    key = (texto or '').strip().lower()
    if key in _CACHE_CLASIFICAR:
        return _CACHE_CLASIFICAR[key], None

    try:
        prompt = _prompt_clasificar_tipo(texto)
        resultado = _llamar_groq(prompt, temperatura=0.1, modelo="llama-3.1-8b-instant")
        tipo = resultado.strip().lower()
        if tipo not in ["negocio", "noticia", "alerta", "mascota", "ignorar"]:
            tipo = "ignorar"
        _CACHE_CLASIFICAR[key] = tipo
        return tipo, None
    except Exception as e:
        return "ignorar", interpretar_error("Groq", e)


def limpiar_texto_ia(texto):
    if not texto or len(texto.strip()) < 5:
        return texto
    key = texto.strip().lower()
    if key in _CACHE_LIMPIEZA:
        return _CACHE_LIMPIEZA[key]
    try:
        prompt = _prompt_limpiar_texto(texto)
        resultado = _llamar_groq(prompt, temperatura=0.1, modelo="llama-3.1-8b-instant")
        _CACHE_LIMPIEZA[key] = resultado.strip()
        return resultado.strip()
    except Exception:
        return _limpiar_regex_fallback(texto)


def _limpiar_regex_fallback(texto):
    texto = re.sub(r'[\U00010000-\U0010ffff]', '', texto, flags=re.UNICODE)
    texto = re.sub(r'[\U00002702-\U000027B0]', '', texto, flags=re.UNICODE)
    texto = re.sub(r'#\w+', '', texto)
    texto = re.sub(r'!{2,}', '!', texto)
    texto = re.sub(r'\?{2,}', '?', texto)
    texto = re.sub(r'\.{3,}', '.', texto)
    texto = re.sub(r'\s+', ' ', texto).strip()
    return texto


def _detectar_categoria_negocio_keywords(texto, categorias):
    txt = (texto or '').lower()
    mejores = []
    mapa_por_nombre = {
        'comida': ['menu', 'menú', 'tacos', 'pizza', 'hamburguesa', 'frapp', 'smoothie', 'crepa', 'fresas con', 'postre', 'waffle', 'hotcake'],
        'salud': ['masaje', 'terapia', 'rehabilitacion', 'rehabilitación', 'psicolog', 'consultorio', 'doctor', 'nutricion', 'nutrición'],
        'ropa & accesorios': ['ropa', 'blusa', 'vestido', 'bolsa', 'accesorio', 'zapato', 'tenis', 'joyeria', 'joyería'],
        'inmobiliaria': ['renta', 'alquiler', 'casa', 'departamento', 'terreno', 'inmueble'],
        'belleza': ['uñas', 'cabello', 'maquillaje', 'spa', 'estetica', 'estética'],
        'automotriz': ['auto', 'carro', 'pintura', 'ceramica', 'cerámica', 'mecanica', 'mecánica', 'bateria', 'llanta'],
        'servicios': ['fletes', 'mudanza', 'plomero', 'electricista', 'carpintero', 'reparacion', 'reparación', 'mantenimiento'],
    }

    for cat in categorias:
        kws = parse_keywords(cat.get('keywords'))
        if not kws:
            kws = mapa_por_nombre.get((cat.get('nombre') or '').strip().lower(), [])
        score = sum(1 for kw in kws if kw and kw in txt)
        if score > 0:
            mejores.append((score, cat['id']))

    if mejores:
        mejores.sort(reverse=True)
        return mejores[0][1]
    return None


def procesar_negocio(post, categorias):
    texto = post["texto_limpio"]
    key = texto.strip().lower()
    if key in _CACHE_NEGOCIO:
        datos = dict(_CACHE_NEGOCIO[key])
        return {**post, **datos, "tipo": "negocio", "error_ia": None}

    categoria_kw = _detectar_categoria_negocio_keywords(texto, categorias)
    if categoria_kw:
        datos = {"categoria_id": categoria_kw, "telefono": post.get("telefono")}
        _CACHE_NEGOCIO[key] = datos
        return {**post, **datos, "tipo": "negocio", "error_ia": None}

    prompt = _prompt_negocio(texto, categorias)
    error_msg = None
    try:
        raw = _llamar_groq(prompt, modelo="llama-3.1-8b-instant")
        datos = _parsear_json(raw)
    except Exception as e:
        error_msg = interpretar_error("Groq", e)
        datos = {"categoria_id": 12, "telefono": post.get("telefono")}

    _CACHE_NEGOCIO[key] = dict(datos)
    return {**post, **datos, "tipo": "negocio", "error_ia": error_msg}


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
                raw = _llamar_groq(prompt, modelo="llama-3.3-70b-versatile")
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
            raw = _llamar_groq(prompt, modelo="llama-3.3-70b-versatile")
            datos = _parsear_json(raw)
        except Exception as e:
            error_msg = interpretar_error("Groq", e)
            datos = {
                "titulo": generar_titulo_noticia_fallback(post),
                "texto": texto,
                "categoria_id": 1,
            }

    datos["titulo"] = datos.get("titulo") or generar_titulo_noticia_fallback(post)
    datos["texto"] = datos.get("texto") or texto
    if len(datos["texto"].split()) < min(NEWS_MIN_WORDS, 25):
        # cuando el modelo falle o devuelva algo demasiado pobre, mejor conservar fiel al original
        datos["texto"] = texto

    _CACHE_NOTICIA[cache_key] = dict(datos)
    return {**post, **datos, "tipo": "noticia", "error_ia": error_msg}


def procesar_alerta(post, cat_alertas):
    texto = post["texto_limpio"]
    key = texto.strip().lower()
    if key in _CACHE_ALERTA:
        datos = dict(_CACHE_ALERTA[key])
        return {**post, **datos, "tipo": "alerta", "error_ia": None}

    prompt = _prompt_alerta(texto, cat_alertas)
    error_msg = None
    try:
        raw = _llamar_groq(prompt, modelo="llama-3.1-8b-instant")
        datos = _parsear_json(raw)
    except Exception as e:
        error_msg = interpretar_error("Groq", e)
        datos = {"texto_alerta": texto[:200], "categoria_id": None, "direccion_aprox": None}

    _CACHE_ALERTA[key] = dict(datos)
    return {**post, **datos, "tipo": "alerta", "error_ia": error_msg}


def debe_usar_gemini(texto, categoria_id=None):
    palabras = len((texto or '').split())
    if palabras >= 140:
        return True
    if categoria_id in [1, 2]:
        return True
    return False
