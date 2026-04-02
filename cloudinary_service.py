import cloudinary
import cloudinary.uploader
import os

cloudinary.config(
    cloud_name=os.getenv("CLOUDINARY_CLOUD_NAME"),
    api_key=os.getenv("CLOUDINARY_API_KEY"),
    api_secret=os.getenv("CLOUDINARY_API_SECRET")
)

def subir_imagenes(imagenes):
    urls = []
    for img in imagenes:
        try:
            res = cloudinary.uploader.upload(img)
            urls.append(res["secure_url"])
        except:
            continue
    return urls
