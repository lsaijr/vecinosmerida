#!/usr/bin/env python3
"""
limpiar_json.py — Bloque 1 del pipeline VecinosMérida
Limpieza determinista antes de enviar a la IA.

Uso:
    python3 limpiar_json.py input.json
    python3 limpiar_json.py input.json -o output.json
"""

import json
import re
import hashlib
import sys
import argparse
from pathlib import Path


# ─────────────────────────────────────────────
# REGLAS DE ORTOGRAFÍA FIJAS
# ─────────────────────────────────────────────
ORTOGRAFIA = {
    # Palabras sin acento
    r'\btelefono\b':      'teléfono',
    r'\btelefono\b':      'teléfono',
    r'\binformacion\b':   'información',
    r'\bsituacion\b':     'situación',
    r'\batencion\b':      'atención',
    r'\batencion\b':      'atención',
    r'\bdirección\b':     'dirección',
    r'\bdireccíon\b':     'dirección',
    r'\bperdio\b':        'perdió',
    r'\bextravio\b':      'extravió',
    r'\baparecio\b':      'apareció',
    r'\bdomicilio\b':     'domicilio',
    r'\benvio\b':         'envío',
    r'\benvios\b':        'envíos',
    r'\bsegun\b':         'según',
    r'\btambien\b':       'también',
    r'\bdias\b':          'días',
    r'\bmas\b(?!\s+o\s)': 'más',  # "mas" pero no "más o menos"
    r'\bsolo\b':          'solo',
    r'\besta\b':          'está',
    r'\bestamos\b':       'estamos',
    r'\bnumero\b':        'número',
    r'\bpagina\b':        'página',
    r'\bpublico\b':       'público',
    r'\bpractica\b':      'práctica',
    r'\brapido\b':        'rápido',
    r'\bfacil\b':         'fácil',
    r'\butiles\b':        'útiles',
    r'\bcredito\b':       'crédito',
    r'\bproximo\b':       'próximo',
    r'\bultimo\b':        'último',
    r'\bdomingo\b':       'domingo',
    r'\bmiercoles\b':     'miércoles',
    r'\bsabado\b':        'sábado',
    r'\bviernes\b':       'viernes',
    # Errores tipográficos informales
    r'\bke\b':            'que',
    r'\bkiero\b':         'quiero',
    r'\bkieren\b':        'quieren',
    r'\bkomo\b':          'como',
    r'\bxfa\b':           'por favor',
    r'\bxfavor\b':        'por favor',
    r'\bx favor\b':       'por favor',
    r'\btmb\b':           'también',
    r'\btb\b(?=\s)':      'también',
    r'\bgrs\b':           'gracias',
    r'\bgrcias\b':        'gracias',
    r'\bpf\b(?=\s)':      'por favor',
    r'\bq\b(?=\s)':       'que',
    # Errores comunes
    r'\bala\s':           'a la ',
    r'\basta\b':          'hasta',
    r'\baver\b':          'a ver',
    r'\bhaber\b(?=\s+si)': 'a ver',
    r'\bmakina\b':        'máquina',
    r'\bcel\b':           'celular',
    r'\bwhats\b':         'WhatsApp',
    r'\bwsp\b':           'WhatsApp',
    r'\bwsap\b':          'WhatsApp',
}

# ─────────────────────────────────────────────
# FUNCIONES DE LIMPIEZA
# ─────────────────────────────────────────────

def limpiar_unicode_fb(txt: str) -> str:
    """Elimina caracteres Unicode decorativos de Facebook (negritas/cursivas matemáticas)."""
    if not txt:
        return txt
    resultado = []
    for ch in txt:
        cp = ord(ch)
        if 0x1D400 <= cp <= 0x1D7FF:
            resultado.append(' ')
        else:
            resultado.append(ch)
    return re.sub(r' {2,}', ' ', ''.join(resultado)).strip()


def sentence_case(seg: str) -> str:
    """Aplica sentence case a un segmento si tiene >35% mayúsculas."""
    seg = seg.strip()
    if not seg or len(seg) < 4:
        return seg
    letras = re.findall(r'[a-zA-ZÁÉÍÓÚÑáéíóúñ]', seg)
    if not letras:
        return seg
    pct = sum(1 for l in letras if l.isupper()) / len(letras)
    if pct <= 0.35:
        return seg
    return seg[0].upper() + seg[1:].lower()


def normalizar_mayusculas(txt: str) -> str:
    """Normaliza mayúsculas oración por oración."""
    if not txt:
        return txt
    lineas = txt.split('\n')
    resultado = []
    for linea in lineas:
        # Dividir por signos que terminan oración
        segmentos = re.split(r'(?<=[.!?¡¿])\s+', linea)
        resultado.append(' '.join(sentence_case(s) for s in segmentos))
    return '\n'.join(resultado)


def limpiar_puntuacion(txt: str) -> str:
    """Limpia puntuación repetida y espacios excesivos."""
    if not txt:
        return txt
    txt = re.sub(r'!{2,}', '!', txt)
    txt = re.sub(r'\?{2,}', '?', txt)
    txt = re.sub(r'\.{4,}', '...', txt)  # mantener ... pero no ....
    txt = re.sub(r' {2,}', ' ', txt)
    txt = re.sub(r'\t', ' ', txt)
    txt = re.sub(r'\n{3,}', '\n\n', txt)  # máximo 2 saltos de línea
    return txt.strip()


def corregir_ortografia(txt: str) -> str:
    """Aplica correcciones ortográficas del diccionario."""
    if not txt:
        return txt
    for patron, correccion in ORTOGRAFIA.items():
        txt = re.sub(patron, correccion, txt, flags=re.IGNORECASE)
    return txt


def generar_fbid(post: dict) -> str:
    """Genera fbid_post desde URL o crea uno sintético."""
    url = post.get('url_post') or ''
    m = re.search(r'/posts/(\d+)', url)
    if m:
        return m.group(1)
    clave = (post.get('autor', '') + '|' + (post.get('texto', '') or '')[:80])
    return 'syn_' + hashlib.md5(clave.encode('utf-8')).hexdigest()[:18]


def es_token_basura(txt: str) -> bool:
    """Detecta tokens basura de Facebook (strings largos alfanuméricos sin sentido)."""
    for w in (txt or '').split():
        if len(w) > 28 and re.search(r'\d', w) and re.search(r'[a-zA-Z]', w):
            return True
    return False


def limpiar_post(post: dict) -> dict | None:
    """
    Aplica todas las reglas de limpieza a un post.
    Retorna None si el post debe descartarse.
    """
    # 1. Descartar autor fantasma
    autor = post.get('autor', '')
    if 'Indicador de estado online' in autor or 'Activo' in autor.strip():
        return None

    # 2. Descartar sin imágenes
    num_imgs = post.get('num_imgs', 0) or 0
    imagenes = post.get('imagenes') or []
    if num_imgs == 0 and len(imagenes) == 0:
        return None

    # 3. Descartar token basura
    texto_raw = post.get('texto', '') or ''
    if es_token_basura(texto_raw):
        return None

    # 4. Descartar texto muy corto
    palabras = [w for w in texto_raw.split() if len(w) > 2]
    if len(palabras) < 3:
        return None

    # 5. Limpiar texto
    texto = texto_raw
    texto = limpiar_unicode_fb(texto)
    texto = normalizar_mayusculas(texto)
    texto = corregir_ortografia(texto)
    texto = limpiar_puntuacion(texto)

    # 6. Construir post limpio
    post_limpio = dict(post)
    post_limpio['texto'] = texto

    # 7. Asegurar fbid_post
    if not post_limpio.get('fbid_post'):
        post_limpio['fbid_post'] = generar_fbid(post)

    # 8. Asegurar repeticiones
    if 'repeticiones' not in post_limpio:
        post_limpio['repeticiones'] = 1

    return post_limpio


def detectar_duplicados(posts: list) -> list:
    """
    Consolida duplicados: mismo autor + mismos primeros 120 chars.
    Conserva el primero y suma repeticiones.
    """
    seen = {}
    resultado = []

    for post in posts:
        txt = (post.get('texto') or '').lower().strip()
        autor = (post.get('autor') or '').lower().strip()
        clave = autor + '|' + txt[:120]
        dk = hashlib.md5(clave.encode('utf-8')).hexdigest()

        if dk in seen:
            # Sumar repetición al post original
            seen[dk]['repeticiones'] = (seen[dk].get('repeticiones') or 1) + 1
        else:
            seen[dk] = post
            resultado.append(post)

    return resultado


# ─────────────────────────────────────────────
# FUNCIÓN PRINCIPAL
# ─────────────────────────────────────────────

def limpiar_json(input_path: str, output_path: str = None) -> dict:
    """Procesa el JSON y retorna el resultado limpio con estadísticas."""

    with open(input_path, encoding='utf-8') as f:
        data = json.load(f)

    posts_originales = data.get('posts', [])
    meta = data.get('meta', {})

    stats = {
        'total_original': len(posts_originales),
        'descartados_sin_imagen': 0,
        'descartados_autor_fantasma': 0,
        'descartados_token_basura': 0,
        'descartados_texto_corto': 0,
        'duplicados_consolidados': 0,
        'total_final': 0,
    }

    # Paso 1: limpiar cada post individualmente
    posts_limpios = []
    for post in posts_originales:
        # Contar razón de descarte
        autor = post.get('autor', '')
        num_imgs = post.get('num_imgs', 0) or 0
        imagenes = post.get('imagenes') or []
        texto_raw = post.get('texto', '') or ''
        palabras = [w for w in texto_raw.split() if len(w) > 2]

        if 'Indicador de estado online' in autor:
            stats['descartados_autor_fantasma'] += 1
            continue
        if num_imgs == 0 and len(imagenes) == 0:
            stats['descartados_sin_imagen'] += 1
            continue
        if es_token_basura(texto_raw):
            stats['descartados_token_basura'] += 1
            continue
        if len(palabras) < 3:
            stats['descartados_texto_corto'] += 1
            continue

        post_limpio = limpiar_post(post)
        if post_limpio:
            posts_limpios.append(post_limpio)

    # Paso 2: detectar y consolidar duplicados
    antes_dedup = len(posts_limpios)
    posts_limpios = detectar_duplicados(posts_limpios)
    stats['duplicados_consolidados'] = antes_dedup - len(posts_limpios)
    stats['total_final'] = len(posts_limpios)

    # Construir resultado
    resultado = {
        'meta': {
            **meta,
            'total_posts': len(posts_limpios),
            'procesado_bloque1': True,
        },
        'posts': posts_limpios,
    }

    # Guardar
    if output_path is None:
        p = Path(input_path)
        output_path = str(p.parent / (p.stem + '_b1' + p.suffix))

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(resultado, f, ensure_ascii=False, indent=2)

    return stats, output_path


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Bloque 1 — limpieza determinista de JSON de Facebook')
    parser.add_argument('input', help='Archivo JSON de entrada')
    parser.add_argument('-o', '--output', help='Archivo JSON de salida (default: input_b1.json)')
    args = parser.parse_args()

    print(f'\n🧹 Procesando: {args.input}')
    stats, out = limpiar_json(args.input, args.output)

    print(f'\n✅ Resultado guardado en: {out}')
    print(f'\n📊 Estadísticas:')
    print(f'   Total original:           {stats["total_original"]}')
    print(f'   Descartados sin imagen:   {stats["descartados_sin_imagen"]}')
    print(f'   Descartados autor fantasma:{stats["descartados_autor_fantasma"]}')
    print(f'   Descartados token basura: {stats["descartados_token_basura"]}')
    print(f'   Descartados texto corto:  {stats["descartados_texto_corto"]}')
    print(f'   Duplicados consolidados:  {stats["duplicados_consolidados"]}')
    print(f'   ─────────────────────────')
    print(f'   Total final:              {stats["total_final"]}')
    total_desc = stats["total_original"] - stats["total_final"]
    print(f'   Reducción:                {total_desc} posts ({total_desc/max(stats["total_original"],1)*100:.1f}%)')
