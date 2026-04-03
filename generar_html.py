import json
import os
from datetime import datetime

OUTPUT_DIR = "static/resultados"

def _cat_map(categorias, por="id"):
    """Crea un dict de categorías indexado por id o slug."""
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
    tipo_grupo = config_grupo.get("tipo", "vecinos")

    # Aplanar todos los posts con su tipo
    todos = []
    for tipo, lista in resultados.items():
        if tipo == "ignorados":
            continue
        for p in lista:
            p["_tipo_final"] = tipo[:-1] if tipo.endswith("s") else tipo  # "negocios" → "negocio"
            todos.append(p)

    resumen = {
        "total": len(todos),
        "negocios": len(resultados.get("negocios", [])),
        "noticias": len(resultados.get("noticias", [])),
        "alertas": len(resultados.get("alertas", [])),
        "mascotas": len(resultados.get("mascotas", [])),
        "ignorados": len(resultados.get("ignorados", [])),
    }

    # Mapas de categorías
    cats_neg_map = _cat_map(cats_negocios)
    cats_not_map = _cat_map(cats_noticias)
    cats_ale_map = _cat_map(cats_alertas)

    def cat_info(p):
        tipo = p.get("_tipo_final", "negocio")
        cat_id = str(p.get("categoria_id", ""))
        if tipo == "negocio" or tipo == "mascota":
            c = cats_neg_map.get(cat_id, {})
            return c.get("nombre", "General"), c.get("emoji", "⭐"), c.get("color_hex", "#495057")
        elif tipo == "noticia":
            c = cats_not_map.get(cat_id, {})
            return c.get("nombre", "General"), c.get("icono", "📰"), c.get("color", "#1d4ed8")
        elif tipo == "alerta":
            c = cats_ale_map.get(cat_id, {})
            return c.get("nombre", "Alerta"), c.get("icono", "⚠️"), c.get("color", "#dc2626")
        return "General", "📌", "#495057"

    def render_card(p):
        tipo = p.get("_tipo_final", "negocio")
        color, bg, emoji_tipo = _color_por_tipo(tipo)
        cat_nombre, cat_emoji, cat_color = cat_info(p)

        imgs = p.get("imagenes_cloudinary", [])
        tel = p.get("telefono") or p.get("whatsapp", "")
        autor = p.get("autor", "")
        error = p.get("_error_visible", "")

        # Título/nombre según tipo
        if tipo == "noticia":
            titulo = p.get("titulo", p.get("texto_limpio", "")[:80])
            descripcion = p.get("texto", p.get("texto_limpio", ""))[:300] + "..."
        elif tipo == "alerta":
            titulo = cat_nombre
            descripcion = p.get("texto_alerta", p.get("texto_limpio", ""))[:200]
        elif tipo == "mascota":
            titulo = cat_nombre
            descripcion = p.get("texto_limpio", "")[:200]
        else:
            titulo = p.get("nombre", p.get("texto_limpio", "")[:60])
            descripcion = p.get("descripcion", p.get("texto_limpio", ""))[:150]

        direccion = p.get("direccion_aprox", "")

        # Galería
        gal_html = ""
        if imgs:
            imgs_js = json.dumps(imgs)
            gal_html = f"""
<div class="gal" onclick="openLB({imgs_js}, 0, {json.dumps(titulo)})">
  <div class="gal-track" id="gt_{hash(titulo) % 99999}">
    {''.join(f'<img src="{u}" alt="" loading="lazy">' for u in imgs)}
  </div>
  {'<button class="gal-btn prev" onclick="event.stopPropagation();slideGal(this,-1)">&#8249;</button><button class="gal-btn nxt" onclick="event.stopPropagation();slideGal(this,1)">&#8250;</button>' if len(imgs) > 1 else ''}
  <span class="gal-badge" style="background:rgba(0,0,0,.72)!important;color:#fff!important">{emoji_tipo} {tipo.upper()}</span>
  {f'<span class="gal-cnt">{len(imgs)} fotos</span>' if len(imgs) > 1 else ''}
</div>"""
        else:
            gal_html = f"""
<div class="gal-ph" style="height:140px;display:flex;align-items:center;justify-content:center;background:linear-gradient(135deg,#f0ebe3,#e8e0d5);font-size:3rem;position:relative">
  {emoji_tipo}
  <span class="gal-badge" style="position:absolute;top:10px;left:10px;background:rgba(0,0,0,.72)!important;color:#fff!important">{emoji_tipo} {tipo.upper()}</span>
</div>"""

        # Footer con teléfono y WhatsApp
        footer_html = ""
        if tel:
            wa_url = f"https://wa.me/52{tel}?text=Hola!%20Vi%20tu%20publicación%20en%20VecinosMerida.com"
            footer_html = f"""
<div class="cfoot">
  <span class="ctel">📞 <span>{tel}</span></span>
  <div class="btns">
    <a class="bwa" href="{wa_url}" target="_blank">WhatsApp</a>
  </div>
</div>"""

        # Badge de error IA
        error_html = ""
        if error:
            error_html = f'<div class="error-badge">⚠ {error}</div>'

        # Dirección para alertas
        dir_html = ""
        if direccion:
            dir_html = f'<div class="dir-badge">📍 {direccion}</div>'

        # ID único para el toggle
        uid = abs(hash(titulo + descripcion)) % 999999
        desc_corta = descripcion[:180] + "..." if len(descripcion) > 180 else descripcion
        tiene_mas = len(descripcion) > 180

        if tiene_mas:
            desc_html = f'''<div class="cdesc">
  <span class="desc-short" id="ds_{uid}">{desc_corta} <button class="btn-mas" onclick="toggleDesc({uid})">Ver más ↓</button></span>
  <span class="desc-full" id="df_{uid}" style="display:none">{descripcion} <button class="btn-mas" onclick="toggleDesc({uid})">Ver menos ↑</button></span>
</div>'''
        else:
            desc_html = f'<div class="cdesc">{descripcion}</div>'

        return f"""
<div class="card" data-tipo="{tipo}" data-cat="{cat_nombre}">
  {gal_html}
  <div class="cbody">
    <div style="display:flex;align-items:center;gap:6px;margin-bottom:8px">
      <span style="font-size:.7rem;font-weight:700;padding:3px 9px;border-radius:20px;background:{bg};color:{cat_color}">{cat_emoji} {cat_nombre}</span>
      {f'<span style="font-size:.7rem;color:#9ca3af">{autor}</span>' if autor else ''}
    </div>
    <div class="cname">{titulo}</div>
    {desc_html}
    {dir_html}
    {error_html}
    {footer_html}
  </div>
</div>"""

    # Construir cards
    cards_html = "\n".join(render_card(p) for p in todos)

    # Botones de filtro
    tipos_presentes = list({p["_tipo_final"] for p in todos})
    filtros_html = '<button class="fb on r" data-tipo="todos" onclick="filtrar(this)">Todos</button>\n'
    etiquetas = {"negocio": "🏪 Negocios", "noticia": "📰 Noticias",
                 "alerta": "⚠️ Alertas", "mascota": "🐾 Mascotas"}
    for t in ["negocio", "noticia", "alerta", "mascota"]:
        if t in tipos_presentes:
            cnt = resumen.get(t + "s", 0)
            filtros_html += f'<button class="fb" data-tipo="{t}" onclick="filtrar(this)">{etiquetas[t]} ({cnt})</button>\n'

    nombre_archivo = f"resultado_{meta.get('group_id', 'grupo')}_{fecha.replace('-','')}.html"
    ruta = os.path.join(OUTPUT_DIR, nombre_archivo)

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Resultados — {group_name}</title>
<link href="https://fonts.googleapis.com/css2?family=Fraunces:wght@700;900&family=DM+Sans:wght@400;500;600&display=swap" rel="stylesheet">
<style>
:root{{--rojo:#e63946;--crema:#fdf6ec;--carbon:#1a1a2e;--gris:#6b7280;--borde:#e8e0d5;}}
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{font-family:'DM Sans',sans-serif;background:var(--crema);color:var(--carbon);}}
.hero{{background:var(--carbon);padding:48px 24px 40px;text-align:center;position:relative;overflow:hidden;}}
.hero::before{{content:'';position:absolute;inset:0;background:radial-gradient(ellipse 80% 60% at 50% 0%,rgba(230,57,70,.18),transparent 70%);}}
.hero-badge{{display:inline-block;background:rgba(230,57,70,.15);border:1px solid rgba(230,57,70,.35);color:#ff8a94;font-size:11px;font-weight:600;letter-spacing:.12em;text-transform:uppercase;padding:5px 14px;border-radius:20px;margin-bottom:16px;}}
.hero h1{{font-family:'Fraunces',serif;font-size:clamp(1.8rem,5vw,3rem);font-weight:900;color:#fff;line-height:1.1;margin-bottom:8px;}}
.hero h1 span{{color:var(--rojo);}}
.hero p{{color:rgba(255,255,255,.55);font-size:.95rem;margin-bottom:28px;max-width:480px;margin:0 auto 28px;}}
.stats{{display:flex;gap:28px;justify-content:center;flex-wrap:wrap;}}
.stat-n{{font-family:'Fraunces',serif;font-size:1.8rem;font-weight:700;color:#fff;}}
.stat-l{{font-size:.7rem;color:rgba(255,255,255,.4);letter-spacing:.08em;text-transform:uppercase;margin-top:2px;}}
.ctrl{{background:#fff;border-bottom:1px solid var(--borde);padding:14px 24px;position:sticky;top:0;z-index:100;box-shadow:0 2px 12px rgba(0,0,0,.05);}}
.ctrl-in{{max-width:1100px;margin:0 auto;display:flex;gap:10px;align-items:center;flex-wrap:wrap;}}
.sw{{position:relative;flex:1;min-width:200px;}}
.sinput{{width:100%;padding:9px 14px 9px 36px;border:1.5px solid var(--borde);border-radius:10px;font-family:'DM Sans',sans-serif;font-size:.88rem;outline:none;background:var(--crema);transition:border-color .2s;}}
.sinput:focus{{border-color:var(--rojo);}}
.si{{position:absolute;left:11px;top:50%;transform:translateY(-50%);color:var(--gris);font-size:14px;pointer-events:none;}}
.filtros{{display:flex;gap:6px;flex-wrap:wrap;}}
.fb{{padding:6px 12px;border-radius:8px;border:1.5px solid var(--borde);background:transparent;font-family:'DM Sans',sans-serif;font-size:.78rem;font-weight:500;color:var(--gris);cursor:pointer;transition:all .2s;white-space:nowrap;}}
.fb:hover{{border-color:var(--carbon);color:var(--carbon);}}
.fb.on{{background:var(--carbon);border-color:var(--carbon);color:#fff;}}
.fb.on.r{{background:var(--rojo);border-color:var(--rojo);}}
.main{{max-width:1100px;margin:0 auto;padding:28px 20px 60px;}}
.cnt{{font-size:.83rem;color:var(--gris);margin-bottom:16px;}}
.cnt strong{{color:var(--carbon);}}
.grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:20px;}}
@keyframes up{{from{{opacity:0;transform:translateY(16px)}}to{{opacity:1;transform:translateY(0)}}}}
.card{{background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(26,26,46,.08);transition:transform .25s,box-shadow .25s;display:flex;flex-direction:column;animation:up .4s ease both;}}
.card:hover{{transform:translateY(-4px);box-shadow:0 10px 36px rgba(26,26,46,.13);}}
.gal{{position:relative;height:200px;overflow:hidden;background:#f0ebe3;cursor:pointer;flex-shrink:0;}}
.gal-track{{display:flex;height:100%;transition:transform .35s cubic-bezier(.4,0,.2,1);}}
.gal-track img{{min-width:100%;height:100%;object-fit:cover;flex-shrink:0;display:block;}}
.gal-ph{{width:100%;height:140px;display:flex;align-items:center;justify-content:center;font-size:3rem;background:linear-gradient(135deg,#f0ebe3,#e8e0d5);}}
.gal-btn{{position:absolute;top:50%;transform:translateY(-50%);background:rgba(0,0,0,.5);color:#fff;border:none;width:32px;height:32px;border-radius:50%;font-size:18px;cursor:pointer;display:flex;align-items:center;justify-content:center;opacity:0;transition:opacity .2s;z-index:5;}}
.gal:hover .gal-btn{{opacity:1;}}
@media(hover:none){{.gal-btn{{opacity:1;background:rgba(0,0,0,.4);}}}}
.gal-btn.prev{{left:8px;}}.gal-btn.nxt{{right:8px;}}
.gal-badge{{position:absolute;top:10px;left:10px;z-index:4;display:inline-flex;align-items:center;gap:5px;font-size:.65rem;font-weight:700;letter-spacing:.06em;text-transform:uppercase;padding:3px 10px;border-radius:20px;backdrop-filter:blur(6px);box-shadow:0 2px 10px rgba(0,0,0,.35);pointer-events:none;}}
.gal-cnt{{position:absolute;bottom:8px;right:10px;background:rgba(0,0,0,.6);color:#fff;font-size:.68rem;font-weight:600;padding:2px 7px;border-radius:10px;}}
.cbody{{padding:14px 16px 16px;flex:1;display:flex;flex-direction:column;}}
.cname{{font-family:'Fraunces',serif;font-size:1rem;font-weight:700;color:var(--carbon);margin-bottom:6px;line-height:1.3;}}
.cdesc{{font-size:.82rem;color:var(--gris);line-height:1.6;flex:1;margin-bottom:12px;}}
.cfoot{{display:flex;align-items:center;justify-content:space-between;gap:8px;border-top:1px solid var(--borde);padding-top:10px;margin-top:auto;}}
.ctel{{font-size:.75rem;color:var(--gris);font-weight:500;}}
.ctel span{{color:var(--carbon);font-weight:600;}}
.btns{{display:flex;gap:6px;}}
.bwa{{display:inline-flex;align-items:center;gap:4px;background:#25D366;color:#fff;text-decoration:none;padding:6px 10px;border-radius:8px;font-size:.75rem;font-weight:600;transition:background .2s;white-space:nowrap;}}
.bwa:hover{{background:#1da851;}}
.error-badge{{margin-top:8px;font-size:.72rem;color:#dc2626;background:#fef2f2;border:1px solid #fecaca;border-radius:6px;padding:5px 8px;line-height:1.4;}}
.btn-mas{{background:none;border:none;color:var(--rojo);font-size:.75rem;font-weight:600;cursor:pointer;padding:0;margin-left:4px;font-family:'DM Sans',sans-serif;}}
.btn-mas:hover{{text-decoration:underline;}}
.dir-badge{{font-size:.75rem;color:#6b7280;margin-bottom:6px;}}
.empty{{display:none;text-align:center;padding:60px 20px;color:var(--gris);}}
.empty-ico{{font-size:3rem;margin-bottom:10px;}}
.lb{{display:none;position:fixed;inset:0;z-index:9999;background:rgba(0,0,0,.93);align-items:center;justify-content:center;flex-direction:column;}}
.lb.open{{display:flex;}}
.lb-wrap{{position:relative;display:flex;align-items:center;justify-content:center;}}
.lb-img{{max-width:90vw;max-height:78vh;object-fit:contain;border-radius:6px;box-shadow:0 8px 60px rgba(0,0,0,.7);transition:opacity .2s;}}
.lb-nav{{position:absolute;top:50%;transform:translateY(-50%);background:rgba(255,255,255,.12);border:none;color:#fff;width:44px;height:44px;border-radius:50%;font-size:22px;cursor:pointer;display:flex;align-items:center;justify-content:center;transition:background .2s;}}
.lb-nav:hover{{background:rgba(255,255,255,.25);}}
.lb-prev{{left:-56px;}}.lb-next{{right:-56px;}}
.lb-x{{position:fixed;top:20px;right:24px;background:rgba(255,255,255,.12);border:none;color:#fff;width:40px;height:40px;border-radius:50%;font-size:20px;cursor:pointer;display:flex;align-items:center;justify-content:center;z-index:10;}}
.lb-info{{text-align:center;margin-top:12px;}}
.lb-name{{color:#fff;font-family:'Fraunces',serif;font-size:.95rem;margin-bottom:4px;}}
.lb-num{{color:rgba(255,255,255,.45);font-size:.78rem;}}
.lb-thumbs{{display:flex;gap:8px;margin-top:10px;overflow-x:auto;max-width:90vw;padding:4px;scrollbar-width:none;}}
.lb-thumbs::-webkit-scrollbar{{display:none;}}
.lb-thumb{{flex-shrink:0;width:48px;height:48px;border-radius:6px;overflow:hidden;cursor:pointer;border:2px solid transparent;opacity:.5;transition:opacity .15s,border-color .15s;}}
.lb-thumb.on{{border-color:#fff;opacity:1;}}
.lb-thumb img{{width:100%;height:100%;object-fit:cover;display:block;}}
@media(max-width:600px){{
  .hero{{padding:36px 16px 30px;}}
  .main{{padding:18px 12px 48px;}}
  .grid{{grid-template-columns:1fr;}}
  .filtros{{overflow-x:auto;flex-wrap:nowrap;padding-bottom:4px;}}
  .lb-prev{{left:-32px;}}.lb-next{{right:-32px;}}
}}
</style>
</head>
<body>
<section class="hero">
  <div class="hero-badge">VecinosMérida.com — Pipeline</div>
  <h1>Resultados<br><span>{group_name}</span></h1>
  <p>Colonia: {colonias_str} · {fecha} · {tipo_grupo}</p>
  <div class="stats">
    <div><div class="stat-n">{resumen['total']}</div><div class="stat-l">Total</div></div>
    <div><div class="stat-n">{resumen['negocios']}</div><div class="stat-l">Negocios</div></div>
    <div><div class="stat-n">{resumen['noticias']}</div><div class="stat-l">Noticias</div></div>
    <div><div class="stat-n">{resumen['alertas']}</div><div class="stat-l">Alertas</div></div>
    <div><div class="stat-n">{resumen['mascotas']}</div><div class="stat-l">Mascotas</div></div>
    <div><div class="stat-n">{resumen['ignorados']}</div><div class="stat-l">Ignorados</div></div>
  </div>
</section>

<div class="ctrl">
  <div class="ctrl-in">
    <div class="sw">
      <span class="si">🔍</span>
      <input id="buscador" class="sinput" placeholder="Buscar en resultados…" type="text" oninput="buscar(this.value)">
    </div>
    <div class="filtros">
      {filtros_html}
    </div>
  </div>
</div>

<main class="main">
  <p class="cnt" id="cnt"></p>
  <div class="grid" id="grid">
    {cards_html}
  </div>
  <div class="empty" id="empty"><div class="empty-ico">🔍</div><p>Sin resultados</p></div>
</main>

<div class="lb" id="lb">
  <button class="lb-x" onclick="closeLB()">✕</button>
  <div class="lb-wrap">
    <button class="lb-nav lb-prev" onclick="navLB(-1)">‹</button>
    <img class="lb-img" id="lb-img" src="" alt="">
    <button class="lb-nav lb-next" onclick="navLB(1)">›</button>
  </div>
  <div class="lb-info">
    <div class="lb-name" id="lb-name"></div>
    <div class="lb-num" id="lb-num"></div>
  </div>
  <div class="lb-thumbs" id="lb-thumbs"></div>
</div>

<script>
var LI=[],LX=0,LN='';
var tipoActivo='todos', busquedaActiva='';

function openLB(imgs,idx,name){{LI=imgs;LX=idx;LN=name;drawLB();document.getElementById('lb').classList.add('open');document.body.style.overflow='hidden';}}
function closeLB(){{document.getElementById('lb').classList.remove('open');document.body.style.overflow='';}}
function navLB(d){{LX=(LX+d+LI.length)%LI.length;drawLB();}}
function drawLB(){{
  var img=document.getElementById('lb-img');
  img.style.opacity='0';img.src=LI[LX];img.onload=function(){{img.style.opacity='1';}};
  document.getElementById('lb-name').textContent=LN;
  document.getElementById('lb-num').textContent=LI.length>1?(LX+1)+' / '+LI.length:'';
  var tb=document.getElementById('lb-thumbs');tb.innerHTML='';
  LI.forEach(function(u,i){{
    var d=document.createElement('div');d.className='lb-thumb'+(i===LX?' on':'');
    var im=document.createElement('img');im.src=u;im.alt='';
    d.appendChild(im);d.onclick=function(){{LX=i;drawLB();}};tb.appendChild(d);
  }});
}}
document.getElementById('lb').addEventListener('click',function(e){{if(e.target===this)closeLB();}});
document.addEventListener('keydown',function(e){{
  if(!document.getElementById('lb').classList.contains('open'))return;
  if(e.key==='ArrowRight')navLB(1);
  else if(e.key==='ArrowLeft')navLB(-1);
  else if(e.key==='Escape')closeLB();
}});

function slideGal(btn,dir){{
  var gal=btn.closest('.gal');
  var track=gal.querySelector('.gal-track');
  var imgs=track.querySelectorAll('img');
  var cur=parseInt(track.dataset.cur||'0');
  var next=(cur+dir+imgs.length)%imgs.length;
  track.dataset.cur=next;
  track.style.transform='translateX(-'+next+'00%)';
}}

function filtrar(btn){{
  document.querySelectorAll('.fb').forEach(function(b){{b.classList.remove('on','r');}});
  btn.classList.add('on');
  if(btn.dataset.tipo==='todos')btn.classList.add('r');
  tipoActivo=btn.dataset.tipo;
  aplicarFiltros();
}}

function buscar(val){{
  busquedaActiva=val.toLowerCase();
  aplicarFiltros();
}}

function aplicarFiltros(){{
  var cards=document.querySelectorAll('.card');
  var visible=0;
  cards.forEach(function(c){{
    var matchTipo=tipoActivo==='todos'||c.dataset.tipo===tipoActivo;
    var txt=c.textContent.toLowerCase();
    var matchBusq=!busquedaActiva||txt.includes(busquedaActiva);
    var show=matchTipo&&matchBusq;
    c.style.display=show?'':'none';
    if(show)visible++;
  }});
  document.getElementById('cnt').innerHTML='<strong>'+visible+'</strong> resultado'+(visible!==1?'s':'');
  document.getElementById('empty').style.display=visible===0?'block':'none';
}}

// Inicializar contador
aplicarFiltros();

function toggleDesc(uid){{
  var s=document.getElementById('ds_'+uid);
  var f=document.getElementById('df_'+uid);
  if(s.style.display==='none'){{
    s.style.display='';
    f.style.display='none';
  }} else {{
    s.style.display='none';
    f.style.display='';
  }}
}}
</script>
</body>
</html>"""

    with open(ruta, "w", encoding="utf-8") as f:
        f.write(html)

    return nombre_archivo
