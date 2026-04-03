import os
import json
import re
import time
import itertools

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


def _prompt_limpiar_texto(texto):
    return f"""Limpia y corrige el siguiente texto respetando estas reglas ESTRICTAMENTE:

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

Responde ÚNICAMENTE con el texto limpio, sin explicaciones, sin comillas, sin markdown."""


def _prompt_negocio(texto, categorias):
    cats_str = ", ".join([f"{c['id']}:{c['nombre']}" for c in categorias])
    return f"""Analiza este texto de una publicación de Facebook de vecinos en Mérida, Yucatán.

TEXTO:
{texto}

INSTRUCCIONES:
1. categoria_id: Elige el número que mejor corresponda de esta lista: {cats_str}. Solo devuelve el número.
2. telefono: Extrae el número de 10 dígitos si existe, sino null.

Responde ÚNICAMENTE con un JSON válido, sin explicaciones, sin markdown:
{{"categoria_id": 12, "telefono": null}}"""


def _prompt_noticia(texto, categorias):
    cats_str = ", ".join([f"{c['id']}:{c['nombre']}" for c in categorias])
    return f"""Eres un editor de noticias locales de Mérida, Yucatán, México con experiencia en redacción periodística.

Tu tarea es transformar el siguiente texto de Facebook en una noticia bien redactada.

TEXTO ORIGINAL:
{texto}

INSTRUCCIONES ESTRICTAS:
1. titulo: Escribe un titular optimizado para SEO, claro y directo. Máximo 90 caracteres. Sin emojis. Sin hashtags. Sin signos repetidos. Sin clickbait.
2. texto: Redacta el contenido completo. Mantén los hechos, mejora la redacción, añade contexto si es necesario. Mínimo 3 párrafos. Lenguaje periodístico formal pero accesible. Sin emojis ni hashtags.
3. categoria_id: Elige el número más adecuado de: {cats_str}. Solo el número.

Responde ÚNICAMENTE con JSON válido, sin explicaciones, sin markdown:
{{"titulo": "...", "texto": "...", "categoria_id": 1}}"""


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
1. texto_alerta: Resume la alerta en 1-2 oraciones claras. Sin emojis. Sin hashtags. Máximo 200 caracteres.
2. categoria_id: Elige el ID de la SUBCATEGORÍA más específica de esta lista:{cats_str}
3. direccion_aprox: Si el texto menciona una calle, colonia o lugar específico, extráelo. Si no, null.

Responde ÚNICAMENTE con JSON válido:
{{"texto_alerta": "...", "categoria_id": 6, "direccion_aprox": null}}"""


# ─── RATE LIMIT TRACKER ──────────────────────────────────────
# Con plan de pago de Groq los intervalos son mínimos
_groq_last_call   = {}
_gemini_last_call = {}
GROQ_MIN_INTERVAL   = 0.2   # plan de pago — sin rate limit agresivo
GEMINI_MIN_INTERVAL = 0.5   # plan de pago
MAX_RETRIES = 3

def _esperar_key(last_calls, key, intervalo):
    ultimo = last_calls.get(key, 0)
    transcurrido = time.time() - ultimo
    if transcurrido < intervalo:
        time.sleep(intervalo - transcurrido)
    last_calls[key] = time.time()


# ─── GROQ — modelo según tarea ───────────────────────────────
# Tareas simples (clasificar, limpiar, alertas): llama-3.1-8b-instant  → más barato y rápido
# Tareas complejas (noticias fallback):          llama-3.3-70b-versatile → más calidad

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
                max_tokens=1000
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


# ─── GEMINI ──────────────────────────────────────────────────
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


# ─── PARSEAR JSON SEGURO ────────────────────────────────────
def _parsear_json(texto):
    texto = re.sub(r"```json|```", "", texto).strip()
    return json.loads(texto)


# ─── CLASIFICAR TIPO DE POST ─────────────────────────────────
def clasificar_tipo(texto):
    """Determina si el post es negocio, noticia, alerta, mascota o ignorar."""
    try:
        prompt = _prompt_clasificar_tipo(texto)
        # Tarea simple → modelo ligero
        resultado = _llamar_groq(prompt, temperatura=0.1, modelo="llama-3.1-8b-instant")
        tipo = resultado.strip().lower()
        if tipo in ["negocio", "noticia", "alerta", "mascota", "ignorar"]:
            return tipo, None
        return "ignorar", None
    except Exception as e:
        return "ignorar", interpretar_error("Groq", e)


# ─── LIMPIAR TEXTO (negocios, alertas, mascotas) ─────────────
def limpiar_texto_ia(texto):
    """
    Limpieza inteligente: ortografía, mayúsculas, sin emojis/hashtags/signos repetidos.
    NO reescribe ni agrega contenido. Solo limpia.
    Usa Groq 8B (tarea simple).
    """
    if not texto or len(texto.strip()) < 5:
        return texto
    try:
        prompt = _prompt_limpiar_texto(texto)
        resultado = _llamar_groq(prompt, temperatura=0.1, modelo="llama-3.1-8b-instant")
        return resultado.strip()
    except Exception:
        # Si falla la IA, devolver texto con limpieza básica por regex
        return _limpiar_regex_fallback(texto)


def _limpiar_regex_fallback(texto):
    """Limpieza básica sin IA como fallback."""
    # Eliminar emojis
    texto = re.sub(r'[\U00010000-\U0010ffff]', '', texto, flags=re.UNICODE)
    texto = re.sub(r'[\U00002702-\U000027B0]', '', texto, flags=re.UNICODE)
    # Eliminar hashtags
    texto = re.sub(r'#\w+', '', texto)
    # Signos repetidos
    texto = re.sub(r'!{2,}', '!', texto)
    texto = re.sub(r'\?{2,}', '?', texto)
    texto = re.sub(r'\.{3,}', '.', texto)
    # Espacios extra
    texto = re.sub(r'\s+', ' ', texto).strip()
    return texto


# ─── PROCESAR NEGOCIO ────────────────────────────────────────
def procesar_negocio(post, categorias):
    """
    Para negocios:
    - nombre → viene del campo autor (se asigna en pipeline.py)
    - descripcion → texto limpiado con IA (se asigna en pipeline.py)
    - categoria_id → IA clasifica
    - telefono → IA extrae (o regex de utils.py como respaldo)
    """
    texto = post["texto_limpio"]
    prompt = _prompt_negocio(texto, categorias)
    error_msg = None
    try:
        # Tarea simple → modelo ligero
        raw = _llamar_groq(prompt, modelo="llama-3.1-8b-instant")
        datos = _parsear_json(raw)
    except Exception as e:
        error_msg = interpretar_error("Groq", e)
        datos = {
            "categoria_id": 12,
            "telefono": post.get("telefono")
        }
    return {**post, **datos, "tipo": "negocio", "error_ia": error_msg}


# ─── PROCESAR NOTICIA ────────────────────────────────────────
def procesar_noticia(post, categorias, usar_gemini=False):
    texto = post["texto_limpio"]
    prompt = _prompt_noticia(texto, categorias)
    error_msg = None

    if usar_gemini:
        try:
            raw = _llamar_gemini(prompt)
            datos = _parsear_json(raw)
        except Exception as e:
            error_msg = interpretar_error("Gemini", e)
            # Fallback a Groq 70B para noticias (más calidad)
            try:
                raw = _llamar_groq(prompt, modelo="llama-3.3-70b-versatile")
                datos = _parsear_json(raw)
                error_msg += " → Usando Groq 70B como respaldo."
            except Exception as e2:
                error_msg += f" | Groq también falló: {interpretar_error('Groq', e2)}"
                datos = {"titulo": texto[:90], "texto": texto, "categoria_id": 1}
    else:
        try:
            # Noticias cortas → Groq 70B (más calidad que 8B para redacción)
            raw = _llamar_groq(prompt, modelo="llama-3.3-70b-versatile")
            datos = _parsear_json(raw)
        except Exception as e:
            error_msg = interpretar_error("Groq", e)
            datos = {"titulo": texto[:90], "texto": texto, "categoria_id": 1}

    return {**post, **datos, "tipo": "noticia", "error_ia": error_msg}


# ─── PROCESAR ALERTA ─────────────────────────────────────────
def procesar_alerta(post, cat_alertas):
    texto = post["texto_limpio"]
    prompt = _prompt_alerta(texto, cat_alertas)
    error_msg = None
    try:
        # Tarea simple → modelo ligero
        raw = _llamar_groq(prompt, modelo="llama-3.1-8b-instant")
        datos = _parsear_json(raw)
    except Exception as e:
        error_msg = interpretar_error("Groq", e)
        datos = {"texto_alerta": texto[:200], "categoria_id": None, "direccion_aprox": None}
    return {**post, **datos, "tipo": "alerta", "error_ia": error_msg}


# ─── DECISOR: USAR GEMINI O GROQ PARA NOTICIAS ──────────────
def debe_usar_gemini(texto, categoria_id=None):
    """Usa Gemini para noticias largas o de alta importancia."""
    palabras = len(texto.split())
    if palabras >= 75:
        return True
    if categoria_id in [1, 2]:  # Política, Seguridad
        return True
    return False
