import os
import json
import re
import time
import itertools
import hashlib
from pathlib import Path

# ─── ROTACIÓN DE KEYS ────────────────────────────────────────
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


# ─── CONFIG DE MODELOS ───────────────────────────────────────
MODEL_SIMPLE = os.getenv("MODEL_SIMPLE", "llama-3.1-8b-instant")
MODEL_NEWS = os.getenv("MODEL_NEWS", "llama-3.3-70b-versatile")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")


# ─── INTERPRETADOR DE ERRORES ────────────────────────────────
def interpretar_error(proveedor, error):
    msg = str(error).lower()
    if "quota" in msg or "resource_exhausted" in msg or "429" in msg:
        return f"{proveedor}: Se agotó la cuota de la API. Intenta en unos minutos o revisa tu plan."
    if "api_key" in msg or "invalid" in msg or "unauthorized" in msg or "401" in msg:
        return f"{proveedor}: API key inválida o no configurada. Revisa la variable de entorno."
    if "timeout" in msg or "deadline" in msg:
        return f"{proveedor}: La solicitud tardó demasiado (timeout). Reintentando."
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


# ─── PROMPTS ─────────────────────────────────────────────────

def _prompt_clasificar_tipo(texto):
    return f'''Clasifica el siguiente texto de Facebook en UNA sola categoría.

TEXTO:
{texto}

CATEGORÍAS POSIBLES:
- "negocio": venta de productos, servicios, ofertas comerciales, anuncios de negocios
- "noticia": eventos, accidentes, política, deportes, cultura, información de interés general
- "alerta": incidentes locales inmediatos (baches, robos, personas sospechosas, perros agresivos, fugas de agua, problemas de infraestructura en una colonia específica)
- "mascota": mascotas perdidas, encontradas o en adopción
- "ignorar": spam, contenido irrelevante, texto sin sentido, menos de 20 palabras útiles

Responde ÚNICAMENTE con una de estas palabras exactas: negocio, noticia, alerta, mascota, ignorar'''


def _prompt_limpiar_texto(texto):
    return f'''Limpia y corrige el siguiente texto respetando estas reglas ESTRICTAMENTE:

REGLAS:
1. Corrige ortografía y acentos
2. Corrige mayúsculas: solo al inicio de oración y después de punto. No todo en mayúsculas.
3. Elimina todos los emojis
4. Elimina hashtags (palabras que empiezan con #)
5. Elimina signos repetidos: !!! → !, ??? → ?, ... → .
6. Elimina caracteres extraños o que no correspondan al texto
7. NO reescribas ni cambies el significado. NO agregues contenido nuevo.
8. NO resumas. Devuelve el texto completo, solo limpio.
9. Si el texto ya está limpio, devuélvelo igual.

TEXTO:
{texto}

Responde ÚNICAMENTE con el texto limpio, sin explicaciones, sin comillas, sin markdown.'''


def _prompt_negocio(texto, categorias):
    cats_str = ", ".join([f"{c['id']}:{c['nombre']}" for c in categorias])
    return f'''Analiza este texto de una publicación de Facebook de vecinos en Mérida, Yucatán.

TEXTO:
{texto}

INSTRUCCIONES:
1. categoria_id: Elige el número que mejor corresponda de esta lista: {cats_str}. Solo devuelve el número.
2. telefono: Extrae el número de 10 dígitos si existe, sino null.

Responde ÚNICAMENTE con un JSON válido, sin explicaciones, sin markdown:
{{"categoria_id": 12, "telefono": null}}'''


def _prompt_noticia(texto, categorias):
    cats_str = ", ".join([f"{c['id']}:{c['nombre']}" for c in categorias])
    return f'''Eres un editor de noticias locales de Mérida, Yucatán, México con experiencia en redacción periodística.

Tu tarea es transformar el siguiente texto de Facebook en una noticia bien redactada.

TEXTO ORIGINAL:
{texto}

INSTRUCCIONES ESTRICTAS:
1. titulo: Escribe un titular optimizado para SEO, claro y directo. Máximo 90 caracteres. Sin emojis. Sin hashtags. Sin signos repetidos. Sin clickbait.
2. texto: Redacta el contenido completo. Mantén los hechos, mejora la redacción, añade contexto si es necesario. Mínimo 3 párrafos. Lenguaje periodístico formal pero accesible. Sin emojis ni hashtags.
3. categoria_id: Elige el número más adecuado de: {cats_str}. Solo el número.

Responde ÚNICAMENTE con JSON válido, sin explicaciones, sin markdown:
{{"titulo": "...", "texto": "...", "categoria_id": 1}}'''


def _prompt_noticia_ligera(texto, categorias):
    cats_str = ", ".join([f"{c['id']}:{c['nombre']}" for c in categorias])
    return f'''Analiza este texto de Facebook con formato de noticia local.

TEXTO ORIGINAL:
{texto}

INSTRUCCIONES:
1. titulo: crea un titular claro y directo de máximo 90 caracteres.
2. categoria_id: elige el número más adecuado de: {cats_str}.

Responde ÚNICAMENTE con JSON válido:
{{"titulo": "...", "categoria_id": 1}}'''


def _prompt_alerta(texto, cat_alertas):
    padres = [c for c in cat_alertas if c["parent_id"] is None]
    cats_str = ""
    for p in padres:
        hijos = [c for c in cat_alertas if c["parent_id"] == p["id"]]
        hijos_str = ", ".join([f"{h['id']}:{h['nombre']}" for h in hijos])
        cats_str += f"\n  {p['id']}:{p['nombre']} → subcategorías: {hijos_str}"

    return f'''Eres un sistema que procesa alertas ciudadanas de vecinos de Mérida, Yucatán.

TEXTO:
{texto}

INSTRUCCIONES:
1. texto_alerta: Resume la alerta en 1-2 oraciones claras. Sin emojis. Sin hashtags. Máximo 200 caracteres.
2. categoria_id: Elige el ID de la SUBCATEGORÍA más específica de esta lista:{cats_str}
3. direccion_aprox: Si el texto menciona una calle, colonia o lugar específico, extráelo. Si no, null.

Responde ÚNICAMENTE con JSON válido:
{{"texto_alerta": "...", "categoria_id": 6, "direccion_aprox": null}}'''


# ─── CACHE LOCAL ─────────────────────────────────────────────
_CACHE_DIR = Path("static/cache")
_CACHE_FILE = _CACHE_DIR / "ia_cache.json"
_CACHE_MEM = None
_CACHE_LIMIT = 5000


def _cache_load():
    global _CACHE_MEM
    if _CACHE_MEM is not None:
        return _CACHE_MEM

    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if _CACHE_FILE.exists():
        try:
            _CACHE_MEM = json.loads(_CACHE_FILE.read_text(encoding="utf-8"))
        except Exception:
            _CACHE_MEM = {}
    else:
        _CACHE_MEM = {}
    return _CACHE_MEM


def _cache_save(cache):
    try:
        _CACHE_DIR.mkdir(parents=True, exist_ok=True)
        if len(cache) > _CACHE_LIMIT:
            items = list(cache.items())[-_CACHE_LIMIT:]
            cache = dict(items)
        _CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def _cache_key(stage, payload):
    if isinstance(payload, (dict, list)):
        raw = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    else:
        raw = str(payload)
    digest = hashlib.md5(raw.encode("utf-8")).hexdigest()
    return f"v2:{stage}:{digest}"


def _cache_get(stage, payload):
    cache = _cache_load()
    return cache.get(_cache_key(stage, payload))


def _cache_set(stage, payload, value):
    cache = _cache_load()
    cache[_cache_key(stage, payload)] = value
    _cache_save(cache)


# ─── RATE LIMIT TRACKER ──────────────────────────────────────
_groq_last_call = {}
_gemini_last_call = {}
GROQ_MIN_INTERVAL = 0.2
GEMINI_MIN_INTERVAL = 0.5
MAX_RETRIES = 3


def _esperar_key(last_calls, key, intervalo):
    ultimo = last_calls.get(key, 0)
    transcurrido = time.time() - ultimo
    if transcurrido < intervalo:
        time.sleep(intervalo - transcurrido)
    last_calls[key] = time.time()


# ─── GROQ / GEMINI ───────────────────────────────────────────
def _llamar_groq(prompt, temperatura=0.3, modelo=MODEL_SIMPLE):
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
            resp = genai.GenerativeModel(GEMINI_MODEL).generate_content(prompt)
            return resp.text.strip()
        except Exception as e:
            ultimo_error = e
            msg = str(e).lower()
            if any(x in msg for x in ["429", "quota", "resource_exhausted", "rate"]):
                time.sleep(8 * (2 ** intento))
                continue
            raise e
    raise ultimo_error


# ─── UTILIDADES DE PARSEO / LIMPIEZA ─────────────────────────
def _parsear_json(texto):
    texto = re.sub(r"```json|```", "", texto).strip()
    try:
        return json.loads(texto)
    except Exception:
        match = re.search(r'\{.*\}', texto, re.DOTALL)
        if match:
            return json.loads(match.group(0))
        raise


def _limpiar_regex_fallback(texto):
    texto = re.sub(r'[\U00010000-\U0010ffff]', '', texto, flags=re.UNICODE)
    texto = re.sub(r'[\U00002702-\U000027B0]', '', texto, flags=re.UNICODE)
    texto = re.sub(r'#\w+', '', texto)
    texto = re.sub(r'!{2,}', '!', texto)
    texto = re.sub(r'\?{2,}', '?', texto)
    texto = re.sub(r'\.{3,}', '.', texto)
    texto = re.sub(r'\s+', ' ', texto).strip()
    return texto


def limpiar_texto_ia(texto):
    """
    Limpieza inteligente, pero primero usa regex casi gratis.
    Solo llama IA si detecta señales de texto realmente sucio.
    """
    if not texto or len(texto.strip()) < 5:
        return texto

    texto = _limpiar_regex_fallback(texto)
    if len(texto) < 5:
        return texto

    necesita_ia = bool(
        re.search(r'[A-ZÁÉÍÓÚÑ]{8,}', texto)
        or re.search(r'[¡¿]', texto)
        or re.search(r'\bq\b|\bxq\b', texto.lower())
    )
    if not necesita_ia:
        return texto

    cached = _cache_get("limpiar_texto", texto)
    if cached:
        return cached

    try:
        prompt = _prompt_limpiar_texto(texto)
        resultado = _llamar_groq(prompt, temperatura=0.1, modelo=MODEL_SIMPLE).strip()
        _cache_set("limpiar_texto", texto, resultado)
        return resultado
    except Exception:
        return texto


# ─── CLASIFICAR TIPO DE POST ─────────────────────────────────
def clasificar_tipo(texto):
    """Determina si el post es negocio, noticia, alerta, mascota o ignorar."""
    cached = _cache_get("clasificar_tipo", texto)
    if cached:
        return cached.get("tipo", "ignorar"), cached.get("error")

    try:
        prompt = _prompt_clasificar_tipo(texto)
        resultado = _llamar_groq(prompt, temperatura=0.1, modelo=MODEL_SIMPLE)
        tipo = resultado.strip().lower()
        if tipo not in ["negocio", "noticia", "alerta", "mascota", "ignorar"]:
            tipo = "ignorar"
        data = {"tipo": tipo, "error": None}
        _cache_set("clasificar_tipo", texto, data)
        return tipo, None
    except Exception as e:
        error = interpretar_error("Groq", e)
        data = {"tipo": "ignorar", "error": error}
        _cache_set("clasificar_tipo", texto, data)
        return "ignorar", error


# ─── CATEGORIZACIÓN DE NEGOCIOS POR KEYWORDS ────────────────
_DEFAULT_CATEGORY_HINTS = {
    "comida": ["comida", "tacos", "pizza", "hamburguesa", "sushi", "mariscos", "pollo", "postre", "pastel", "repostería", "reposteria"],
    "inmobiliaria": ["renta", "alquiler", "departamento", "casa", "terreno", "inmueble", "asesor inmobiliario"],
    "belleza": ["uñas", "unas", "cabello", "maquillaje", "spa", "facial", "peinado", "barber", "barbería", "barberia"],
    "mascotas": ["veterin", "estética canina", "estetica canina", "croquetas", "grooming"],
    "salud": ["consulta", "médico", "medico", "dentista", "nutrió", "nutrio", "fisioterapia"],
    "servicios": ["plomero", "electricista", "carpintero", "mantenimiento", "reparaci", "instalaci", "fumigación", "fumigacion"],
    "ventas": ["vendo", "venta", "rifa", "remato", "liquidación", "liquidacion"],
    "educación": ["clases", "curso", "academia", "taller", "asesoría", "asesoria"],
    "tecnología": ["internet", "fibra óptica", "fibra optica", "celular", "laptop", "switch", "playstation"],
}


def _slugify(texto):
    txt = texto.lower()
    txt = (
        txt.replace('á', 'a').replace('é', 'e').replace('í', 'i')
        .replace('ó', 'o').replace('ú', 'u').replace('ñ', 'n')
    )
    return re.sub(r'[^a-z0-9]+', ' ', txt).strip()


def _keywords_from_category(cat):
    kws = []

    for field in ("keywords", "slug", "nombre"):
        val = cat.get(field)
        if not val:
            continue
        if isinstance(val, list):
            kws.extend([str(x).strip().lower() for x in val if str(x).strip()])
        elif isinstance(val, str):
            low = val.strip().lower()
            if low:
                kws.append(low)

    nombre_slug = _slugify(cat.get("nombre", ""))
    for hint_name, hint_keywords in _DEFAULT_CATEGORY_HINTS.items():
        if hint_name in nombre_slug:
            kws.extend(hint_keywords)

    return list(dict.fromkeys(kws))


def _detectar_categoria_negocio_keywords(texto, categorias):
    txt = _slugify(texto)
    mejor = None
    mejor_score = 0

    for cat in categorias:
        score = 0
        for kw in _keywords_from_category(cat):
            kw_slug = _slugify(kw)
            if kw_slug and kw_slug in txt:
                score += max(1, min(3, len(kw_slug.split())))
        if score > mejor_score:
            mejor_score = score
            mejor = cat.get("id")

    return mejor if mejor_score >= 2 else None


# ─── PROCESAR NEGOCIO ────────────────────────────────────────
def procesar_negocio(post, categorias):
    """
    Intenta resolver categoría por keywords y usar teléfono ya detectado.
    Solo usa IA como fallback si la categoría queda ambigua.
    """
    texto = post["texto_limpio"]
    telefono = post.get("telefono")
    categoria_keywords = _detectar_categoria_negocio_keywords(texto, categorias)

    if categoria_keywords is not None:
        return {
            **post,
            "categoria_id": categoria_keywords,
            "telefono": telefono,
            "tipo": "negocio",
            "error_ia": None,
            "categoria_fuente": "keywords",
        }

    cache_payload = {"texto": texto, "telefonoregex": telefono}
    cached = _cache_get("procesar_negocio", cache_payload)
    if cached:
        return {**post, **cached}

    prompt = _prompt_negocio(texto, categorias)
    error_msg = None
    try:
        raw = _llamar_groq(prompt, modelo=MODEL_SIMPLE)
        datos = _parsear_json(raw)
        datos.setdefault("telefono", telefono)
        datos.setdefault("categoria_id", 12)
        datos["tipo"] = "negocio"
        datos["error_ia"] = None
        datos["categoria_fuente"] = "ia"
        _cache_set("procesar_negocio", cache_payload, datos)
        return {**post, **datos}
    except Exception as e:
        error_msg = interpretar_error("Groq", e)
        datos = {
            "categoria_id": 12,
            "telefono": telefono,
            "tipo": "negocio",
            "error_ia": error_msg,
            "categoria_fuente": "fallback",
        }
        _cache_set("procesar_negocio", cache_payload, datos)
        return {**post, **datos}


# ─── PROCESAR NOTICIA ────────────────────────────────────────
def procesar_noticia_ligera(post, categorias):
    texto = post["texto_limpio"]
    cached = _cache_get("procesar_noticia_ligera", texto)
    if cached:
        return {**post, **cached}

    prompt = _prompt_noticia_ligera(texto, categorias)
    error_msg = None
    try:
        raw = _llamar_groq(prompt, temperatura=0.1, modelo=MODEL_SIMPLE)
        datos = _parsear_json(raw)
        titulo = datos.get("titulo") or texto[:90]
        categoria_id = datos.get("categoria_id") or 1
        salida = {
            "titulo": titulo,
            "texto": texto,
            "categoria_id": categoria_id,
            "tipo": "noticia",
            "error_ia": None,
            "modo_noticia": "ligera",
        }
        _cache_set("procesar_noticia_ligera", texto, salida)
        return {**post, **salida}
    except Exception as e:
        error_msg = interpretar_error("Groq", e)
        salida = {
            "titulo": texto[:90],
            "texto": texto,
            "categoria_id": 1,
            "tipo": "noticia",
            "error_ia": error_msg,
            "modo_noticia": "ligera_fallback",
        }
        _cache_set("procesar_noticia_ligera", texto, salida)
        return {**post, **salida}


def procesar_noticia(post, categorias, usar_gemini=False):
    texto = post["texto_limpio"]
    cache_payload = {"texto": texto, "usar_gemini": usar_gemini}
    cached = _cache_get("procesar_noticia", cache_payload)
    if cached:
        return {**post, **cached}

    prompt = _prompt_noticia(texto, categorias)
    error_msg = None

    if usar_gemini:
        try:
            raw = _llamar_gemini(prompt)
            datos = _parsear_json(raw)
        except Exception as e:
            error_msg = interpretar_error("Gemini", e)
            try:
                raw = _llamar_groq(prompt, modelo=MODEL_NEWS)
                datos = _parsear_json(raw)
                error_msg += " → Usando Groq 70B como respaldo."
            except Exception as e2:
                error_msg += f" | Groq también falló: {interpretar_error('Groq', e2)}"
                datos = {"titulo": texto[:90], "texto": texto, "categoria_id": 1}
    else:
        try:
            raw = _llamar_groq(prompt, modelo=MODEL_NEWS)
            datos = _parsear_json(raw)
        except Exception as e:
            error_msg = interpretar_error("Groq", e)
            datos = {"titulo": texto[:90], "texto": texto, "categoria_id": 1}

    salida = {**datos, "tipo": "noticia", "error_ia": error_msg, "modo_noticia": "completa"}
    _cache_set("procesar_noticia", cache_payload, salida)
    return {**post, **salida}


# ─── PROCESAR ALERTA ─────────────────────────────────────────
def procesar_alerta(post, cat_alertas):
    texto = post["texto_limpio"]
    cached = _cache_get("procesar_alerta", texto)
    if cached:
        return {**post, **cached}

    prompt = _prompt_alerta(texto, cat_alertas)
    error_msg = None
    try:
        raw = _llamar_groq(prompt, modelo=MODEL_SIMPLE)
        datos = _parsear_json(raw)
    except Exception as e:
        error_msg = interpretar_error("Groq", e)
        datos = {"texto_alerta": texto[:200], "categoria_id": None, "direccion_aprox": None}

    salida = {**datos, "tipo": "alerta", "error_ia": error_msg}
    _cache_set("procesar_alerta", texto, salida)
    return {**post, **salida}


# ─── DECISORES ───────────────────────────────────────────────
def debe_usar_gemini(texto, categoria_id=None):
    """Usa Gemini para noticias largas o de alta importancia."""
    palabras = len(texto.split())
    if palabras >= 75:
        return True
    if categoria_id in [1, 2]:
        return True
    return False


def debe_usar_noticia_ligera(texto):
    """
    Noticias cortas/simplemente informativas pueden ir por una ruta ligera.
    """
    palabras = len(texto.split())
    if palabras <= 45 and len(texto) <= 280:
        return True
    return False
