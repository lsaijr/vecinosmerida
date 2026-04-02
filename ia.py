def procesar_con_ia(posts):
    resultados = []
    for p in posts:
        texto = p["texto_limpio"]
        titulo = texto[:60]

        resultados.append({
            **p,
            "titulo": titulo,
            "contenido": texto
        })
    return resultados
