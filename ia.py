import os
import json
import re

# ─── CLIENTES ────────────────────────────────────────────────
def _get_groq_client():
    from groq import Groq
    return Groq(api_key=os.getenv("GROQ_API_KEY"))

def _get_gemini_model():
    import google.generativeai as genai
    genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
    return genai.GenerativeModel("gemini-1.5-flash")

# ─── INTERPRETADOR DE ERRORES ────────────────────────────────
def interpretar_error(proveedor, error):
    msg = str(error).lower()
    if "quota" in msg or "resource_exhausted" in msg or "429" in msg:
        return f"{proveedor}: Se agotó la cuota de la API. Intenta en unos minutos o revisa tu plan."
    if "api_key" in msg or "invalid" in msg or "unauthorized" in msg or "401" in msg:
        return f"{proveedor}: API key inválida o no configurada. Revisa la variable de entorno."
    if "timeout" in msg or "deadline" in msg:
        return f"{proveedor}: La solicitud tardó demasiado (timeout). Reintentando con Groq."
    if "safety" in msg or "blocked" in msg or "harm" in msg:
        return f"{proveedor}: El contenido fue bloqueado por filtros de seguridad."
    if "context" in msg or "tokens" in msg or "length" in msg:
        return f"{proveedor}: El texto es demasiado largo para procesar. Se truncará."
    if "model" in msg or "not found" in msg or "404" in msg:
        return f"{proveedor}: Modelo no disponible. Verifica el nombre del modelo."
    if "rate" in msg or "limit" in msg:
        return f"{proveedor}: Límite de solicitudes alcanzado. Esperando antes de reintentar."
    if "connect" in msg or "network" in msg or "503" in msg:
        return f"{proveedor}: Error de conexión con el servidor de IA. Verifica tu red."
    return f"{proveedor}: Error inesperado — {str(error)[:120]}"

# ─── PROMPTS ─────────────────────────────────────────────────
def _prompt_negocio(texto, categorias):
    cats_str = ", ".join([f"{c['id']}:{c['nombre']}" for c in categorias])
    return f"""Eres un asistente que procesa publicaciones de grupos de Facebook de vecinos en Mérida, Yucatán, México.

Tu tarea es analizar el siguiente texto de una publicación y extraer información estructurada.

TEXTO:
{texto}

INSTRUCCIONES ESTRICTAS:
1. nombre: Extrae el nombre del negocio o servicio. Si no hay nombre claro, usa el tipo de servicio (ej: "Plomero a domicilio"). Máximo 60 caracteres.
2. descripcion: Resume el servicio o producto en 1-2 oraciones claras y directas. Sin emojis ni hashtags. Máximo 120 caracteres.
3. categoria_id: Elige el número que mejor corresponda de esta lista: {cats_str}. Solo devuelve el número.
4. telefono: Extrae el número de 10 dígitos si existe, sino null.

Responde ÚNICAMENTE con un JSON válido, sin explicaciones, sin markdown:
{{"nombre": "...", "descripcion": "...", "categoria_id": 12, "telefono": null}}"""

def _prompt_noticia(texto, categorias):
    cats_str = ", ".join([f"{c['id']}:{c['nombre']}" for c in categorias])
    return f"""Eres un editor de noticias locales de Mérida, Yucatán, México con experiencia en redacción periodística.

Tu tarea es transformar el siguiente texto de Facebook en una noticia bien redactada.

TEXTO ORIGINAL:
{texto}

INSTRUCCIONES ESTRICTAS:
1. titulo: Escribe un titular periodístico claro, directo e informativo. Máximo 90 caracteres. Sin clickbait.
2. texto: Redacta el contenido completo. Mantén los hechos, mejora la redacción, añade contexto si es necesario. Mínimo 3 párrafos. Lenguaje periodístico formal pero accesible.
3. categoria_id: Elige el número más adecuado de: {cats_str}. Solo el número.

Responde ÚNICAMENTE con JSON válido, sin explicaciones, sin markdown:
{{"titulo": "...", "texto": "...", "categoria_id": 1}}"""

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

def _prompt_alerta(texto, cat_alertas):
    # Construir árbol de categorías
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
1. texto_alerta: Resume la alerta en 1-2 oraciones claras y directas. Sin emojis. Máximo 200 caracteres.
2. categoria_id: Elige el ID de la SUBCATEGORÍA más específica de esta lista:{cats_str}
3. direccion_aprox: Si el texto menciona una calle, colonia o lugar específico, extráelo. Si no, null.

Responde ÚNICAMENTE con JSON válido:
{{"texto_alerta": "...", "categoria_id": 6, "direccion_aprox": null}}"""

# ─── GROQ ────────────────────────────────────────────────────
def _llamar_groq(prompt, temperatura=0.3):
    client = _get_groq_client()
    resp = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        temperature=temperatura,
        max_tokens=500
    )
    return resp.choices[0].message.content.strip()

# ─── GEMINI ──────────────────────────────────────────────────
def _llamar_gemini(prompt):
    model = _get_gemini_model()
    resp = model.generate_content(prompt)
    return resp.text.strip()

# ─── PARSEAR JSON SEGURO ────────────────────────────────────
def _parsear_json(texto):
    texto = re.sub(r"```json|```", "", texto).strip()
    return json.loads(texto)

# ─── CLASIFICAR TIPO DE POST ─────────────────────────────────
def clasificar_tipo(texto):
    """Determina si el post es negocio, noticia, alerta, mascota o ignorar."""
    try:
        prompt = _prompt_clasificar_tipo(texto)
        resultado = _llamar_groq(prompt, temperatura=0.1)
        tipo = resultado.strip().lower()
        if tipo in ["negocio", "noticia", "alerta", "mascota", "ignorar"]:
            return tipo, None
        return "ignorar", None
    except Exception as e:
        return "ignorar", interpretar_error("Groq", e)

# ─── PROCESAR NEGOCIO ────────────────────────────────────────
def procesar_negocio(post, categorias):
    texto = post["texto_limpio"]
    prompt = _prompt_negocio(texto, categorias)
    error_msg = None
    try:
        raw = _llamar_groq(prompt)
        datos = _parsear_json(raw)
    except Exception as e:
        error_msg = interpretar_error("Groq", e)
        datos = {
            "nombre": texto[:60],
            "descripcion": texto[:120],
            "categoria_id": 12,
            "telefono": None
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
            # Fallback a Groq
            try:
                raw = _llamar_groq(prompt)
                datos = _parsear_json(raw)
                error_msg += " → Usando Groq como respaldo."
            except Exception as e2:
                error_msg += f" | Groq también falló: {interpretar_error('Groq', e2)}"
                datos = {"titulo": texto[:90], "texto": texto, "categoria_id": 1}
    else:
        try:
            raw = _llamar_groq(prompt)
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
        raw = _llamar_groq(prompt)
        datos = _parsear_json(raw)
    except Exception as e:
        error_msg = interpretar_error("Groq", e)
        datos = {"texto_alerta": texto[:200], "categoria_id": None, "direccion_aprox": None}
    return {**post, **datos, "tipo": "alerta", "error_ia": error_msg}

# ─── DECISOR: USAR GEMINI O GROQ PARA NOTICIAS ──────────────
def debe_usar_gemini(texto, categoria_id=None):
    """Usa Gemini para noticias largas o de alta importancia (política, seguridad)."""
    palabras = len(texto.split())
    if palabras >= 75:
        return True
    if categoria_id in [1, 2]:  # Política, Seguridad
        return True
    return False
