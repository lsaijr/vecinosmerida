import os
import json
import re
import time
import itertools

# ─── ROTACIÓN DE KEYS (3 Groq, 2 Gemini) ─────────────────────
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

# ─── LLAMADAS CON PAUSA DE SEGURIDAD (2.0s) ──────────────────
def _llamar_groq(prompt, temperatura=0.3):
    # Pausa de 2s para no saturar el plan Free (cada key descansa 6s)
    time.sleep(2.0) 
    from groq import Groq
    client = Groq(api_key=_next_groq_key())
    
    resp = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        temperature=temperatura,
        max_tokens=500
    )
    return resp.choices[0].message.content.strip()

def _llamar_gemini(prompt):
    # Pausa de 2s para Gemini
    time.sleep(2.0)
    import google.generativeai as genai
    genai.configure(api_key=_next_gemini_key())
    model = genai.GenerativeModel("gemini-1.5-flash")
    resp = model.generate_content(prompt)
    return resp.text.strip()

# (El resto de tus funciones de procesamiento permanecen iguales)
def _parsear_json(texto):
    try:
        match = re.search(r"\{.*\}", texto, re.DOTALL)
        if match:
            return json.loads(match.group())
        return json.loads(texto)
    except:
        return {}

def interpretar_error(origen, e):
    msg = str(e)
    if "429" in msg or "quota" in msg.lower():
        return f"{origen}: Se agotó la cuota. Revisa tus llaves."
    return f"{origen}: {msg[:50]}"

# Mantengo tus funciones de procesamiento de Negocios, Noticias y Alertas intactas...
def procesar_negocio(post, cat_negocios):
    texto = post["texto_limpio"]
    prompt = f"Analiza este post y devuelve JSON con: nombre, descripcion (max 140 car), categoria_id (de la lista: {json.dumps(cat_negocios)}), whatsapp, telefono.\nPost: {texto}"
    error_msg = None
    try:
        raw = _llamar_groq(prompt)
        datos = _parsear_json(raw)
    except Exception as e:
        error_msg = interpretar_error("Groq", e)
        datos = {"nombre": texto[:50], "descripcion": texto[:140], "categoria_id": 1}
    return {**post, **datos, "tipo": "negocio", "error_ia": error_msg}

def procesar_noticia(post, cat_noticias, usar_gemini=False):
    texto = post["texto_limpio"]
    prompt = f"Resume esta noticia local en un JSON con: titulo, texto (max 250 car), categoria_id (de: {json.dumps(cat_noticias)}).\nNoticia: {texto}"
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
                error_msg += " → Usando Groq como respaldo."
            except Exception as e2:
                error_msg += f" | Groq también falló"
                datos = {"titulo": texto[:90], "texto": texto, "categoria_id": 1}
    else:
        try:
            raw = _llamar_groq(prompt)
            datos = _parsear_json(raw)
        except Exception as e:
            error_msg = interpretar_error("Groq", e)
            datos = {"titulo": texto[:90], "texto": texto, "categoria_id": 1}
    return {**post, **datos, "tipo": "noticia", "error_ia": error_msg}

def procesar_alerta(post, cat_alertas):
    texto = post["texto_limpio"]
    prompt = f"Analiza esta alerta ciudadana en JSON: texto_alerta, categoria_id (de: {json.dumps(cat_alertas)}), direccion_aprox.\nAlerta: {texto}"
    error_msg = None
    try:
        raw = _llamar_groq(prompt)
        datos = _parsear_json(raw)
    except Exception as e:
        error_msg = interpretar_error("Groq", e)
        datos = {"texto_alerta": texto[:200], "categoria_id": None, "direccion_aprox": None}
    return {**post, **datos, "tipo": "alerta", "error_ia": error_msg}
