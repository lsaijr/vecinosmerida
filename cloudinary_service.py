import cloudinary
import cloudinary.uploader
import os

cloudinary.config(
    cloud_name=os.getenv("CLOUDINARY_CLOUD_NAME"),
    api_key=os.getenv("CLOUDINARY_API_KEY"),
    api_secret=os.getenv("CLOUDINARY_API_SECRET")
)

def subir_imagenes(urls_temp):
    """
    Sube una lista de URLs temporales a Cloudinary.
    Retorna: (lista_urls_cloudinary, count_ok, count_fail)
    """
    urls = []
    ok = 0
    fail = 0
    for url in urls_temp:
        try:
            res = cloudinary.uploader.upload(url, folder="vecinosmerida")
            urls.append(res["secure_url"])
            ok += 1
        except Exception:
            fail += 1
            continue
    return urls, ok, fail
