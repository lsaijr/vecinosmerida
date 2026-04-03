import json
import os
from datetime import datetime

OUTPUT_DIR = "static/resultados"

def _cat_map(categorias, por="id"):
    return {str(c[por]): c for c in categorias}

def _color_por_tipo(tipo):
    mapa = {
        "negocio": ("#b45309", "rgba(245,158,11,.12)", "🏪"),
        "noticia": ("#1d4ed8", "rgba(59,130,246,.12)", "📰"),
        "alerta":  ("#dc2626", "rgba(220,38,38,.12)",  "⚠️"),
        "mascota": ("#059669", "rgba(5,150,105,.12)",  "🐾"),
    }
    return mapa.get(tipo, ("#495057", "rgba(108,117,125,.1)", "📌"))

def generar_html_resultados(resultados, meta, config_grupo, cats_negocios, cats_noticias, cats_alertas):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    fecha = meta.get("fecha_captura", datetime.now().strftime("%d-%m-%Y"))
    group_name = meta.get("group_name", "Grupo")
    colonias_str = ", ".join(config_grupo.get("colonia_nombres", ["General"]))

    todos = []
    for tipo, lista in resultados.items():
        if tipo == "ignorados": continue
        for p in lista:
            p["_tipo_final"] = tipo[:-1] if tipo.endswith("s") else tipo
            todos.append(p)

    resumen = {
        "total": len(todos),
        "negocios": len(resultados.get("negocios", [])),
        "noticias": len(resultados.get("noticias", [])),
        "alertas": len(resultados.get("alertas", [])),
        "mascotas": len(resultados.get("mascotas", [])),
        "ignorados": len(resultados.get("ignorados", [])),
    }

    cats_neg_map = _cat_map(cats_negocios)
    cats_not_map = _cat_map(cats_noticias)
    cats_ale_map = _cat_map(cats_alertas)

    def cat_info(p):
        tipo = p.get("_tipo_final", "negocio")
        cat_id = str(p.get("categoria_id", ""))
        if tipo in ["negocio", "mascota"]:
            c = cats_neg_map.get(cat_id, {})
            return c.get("nombre", "General"), c.get("emoji", "⭐"), c.get("color_hex", "#495057")
        elif tipo == "noticia":
            c = cats_not_map.get(cat_id, {})
            return c.get("nombre", "Noticia"), c.get("icono", "📰"), c.get("color", "#1d4ed8")
        elif tipo == "alerta":
            c = cats_ale_map.get(cat_id, {})
            return c.get("nombre", "Alerta"), c.get("icono", "⚠️"), c.get("color", "#dc2626")
        return "General", "📌", "#495057"

    def render_card(p):
        tipo = p.get("_tipo_final", "negocio")
        color, bg, emoji_tipo = _color_por_tipo(tipo)
        cat_nombre, cat_emoji, cat_color = cat_info(p)

        # CORRECCIÓN DE IMÁGENES: Buscamos en múltiples campos
        imgs = p.get("imagenes_cloudinary") or p.get("imagenes") or p.get("fotos") or []
        if isinstance(imgs, str): imgs = [imgs]

        tel = p.get("telefono") or p.get("whatsapp", "")
        autor = p.get("autor", "")
        error = p.get("error_ia", "")

        if tipo == "noticia":
            titulo = p.get("titulo", p.get("texto_limpio", "")[:80])
            descripcion = p.get("texto", p.get("texto_limpio", ""))
        elif tipo == "alerta":
            titulo = cat_nombre
            descripcion = p.get("texto_alerta", p.get("texto_limpio", ""))
        else:
            titulo = p.get("nombre", p.get("texto_limpio", "")[:60])
            descripcion = p.get("descripcion", p.get("texto_limpio", ""))

        uid = abs(hash(str(p.get("id", titulo)))) % 999999
        
        gal_html = ""
        if imgs:
            imgs_js = json.dumps(imgs)
            gal_html = f"""<div class="gal" onclick='openLB({imgs_js}, 0, {json.dumps(titulo)})'>
                <div class="gal-track">{''.join(f'<img src="{u}" loading="lazy">' for u in imgs)}</div>
                <span class="gal-badge">{emoji_tipo} {tipo.upper()}</span>
            </div>"""
        else:
            gal_html = f'<div class="gal-ph">{emoji_tipo}<span class="gal-badge">{emoji_tipo} {tipo.upper()}</span></div>'

        return f"""
        <div class="card" data-tipo="{tipo}">
            {gal_html}
            <div class="cbody">
                <div style="display:flex;gap:6px;margin-bottom:8px">
                    <span style="font-size:.7rem;padding:3px 9px;border-radius:20px;background:{bg};color:{cat_color}">{cat_emoji} {cat_nombre}</span>
                </div>
                <div class="cname">{titulo}</div>
                <div class="cdesc" id="ds_{uid}">{descripcion[:150]}... <button class="btn-mas" onclick="toggleDesc({uid})">Ver más</button></div>
                <div class="cdesc" id="df_{uid}" style="display:none">{descripcion} <button class="btn-mas" onclick="toggleDesc({uid})">Ver menos</button></div>
                {f'<div class="error-badge">{error}</div>' if error else ''}
                <div class="cfoot">
                    <span class="ctel">📞 {tel if tel else 'N/A'}</span>
                    {f'<a class="bwa" href="https://wa.me/52{tel}" target="_blank">WhatsApp</a>' if tel else ''}
                </div>
            </div>
        </div>"""

    cards_html = "\n".join(render_card(p) for p in todos)
    
    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width,initial-scale=1.0">
    <title>{group_name}</title>
    <style>
        /* (Tus estilos CSS permanecen iguales) */
        :root{{--rojo:#e63946;--crema:#fdf6ec;--carbon:#1a1a2e;--gris:#6b7280;--borde:#e8e0d5;}}
        body{{font-family:sans-serif;background:var(--crema);margin:0;}}
        .hero{{background:var(--carbon);color:#fff;padding:40px;text-align:center;}}
        .grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:20px;padding:20px;}}
        .card{{background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 4px 10px rgba(0,0,0,.1);display:flex;flex-direction:column;}}
        .gal{{height:200px;position:relative;background:#eee;cursor:pointer;}}
        .gal img{{width:100%;height:100%;object-fit:cover;}}
        .gal-badge{{position:absolute;top:10px;left:10px;background:rgba(0,0,0,.7);color:#fff;padding:2px 8px;border-radius:10px;font-size:10px;}}
        .cbody{{padding:15px;flex:1;}}
        .cname{{font-weight:700;margin-bottom:8px;}}
        .cdesc{{font-size:13px;color:#555;line-height:1.4;}}
        .btn-mas{{color:var(--rojo);border:none;background:none;cursor:pointer;font-weight:600;}}
        .cfoot{{margin-top:15px;display:flex;justify-content:space-between;align-items:center;border-top:1px solid #eee;padding-top:10px;}}
        .bwa{{background:#25D366;color:#fff;text-decoration:none;padding:5px 10px;border-radius:5px;font-size:12px;}}
    </style>
</head>
<body>
    <div class="hero">
        <h1>{group_name}</h1>
        <p>{fecha} | {resumen['total']} resultados</p>
    </div>
    <div class="grid">{cards_html}</div>

<script>
function toggleDesc(uid){{
    var s = document.getElementById('ds_' + uid);
    var f = document.getElementById('df_' + uid);
    if(s.style.display === 'none'){{
        s.style.display = 'block';
        f.style.display = 'none';
    }} else {{
        s.style.display = 'none';
        f.style.display = 'block';
    }}
}}

function openLB(imgs, idx, name){{
    alert("Abriendo galería de " + name + " con " + imgs.length + " fotos.");
    // Aquí iría tu lógica de Lightbox completa
}}
</script>
</body>
</html>"""

    nombre_archivo = f"resultado_{meta.get('group_id', 'grupo')}.html"
    ruta = os.path.join(OUTPUT_DIR, nombre_archivo)
    with open(ruta, "w", encoding="utf-8") as f:
        f.write(html)
    return nombre_archivo
