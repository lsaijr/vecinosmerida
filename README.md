# 🚀 Pipeline de Procesamiento de Posts (JSON → IA → DB)

Sistema para procesar publicaciones desde archivos JSON, limpiarlas, clasificarlas, mejorarlas con IA y almacenarlas en base de datos, incluyendo gestión de imágenes.

---

## 🧠 ¿Qué hace este sistema?

Este pipeline automatiza el flujo completo:

1. 🧹 Limpieza de texto  
2. 🔁 Detección de duplicados  
3. 🧠 Clasificación por categoría  
4. 🤖 Mejora con IA (SEO, redacción, títulos)  
5. 🖼 Subida de imágenes a Cloudinary  
6. 💾 Guardado en MySQL  

---

## 🏗️ Arquitectura

Frontend (HTML)
↓
API (FastAPI)
↓
Pipeline (Python)
↓
IA (Gemini / Groq)
↓
Cloudinary (imágenes)
↓
MySQL (base de datos)

---

## 📁 Estructura del proyecto

project/
│
├── main.py
├── pipeline.py
├── db.py
├── ia.py
├── cloudinary_service.py
├── utils.py
│
├── static/
│   └── index.html
│
├── requirements.txt
├── Procfile
└── README.md

---

## ⚙️ Instalación local

pip install -r requirements.txt
uvicorn main:app --reload

Abrir en navegador:
http://localhost:8000

---

## 🔐 Variables de entorno

### 🗄️ Base de datos (MySQL)
DB_HOST=
DB_PORT=3306
DB_USER=
DB_PASSWORD=
DB_NAME=

---

### ☁️ Cloudinary
CLOUDINARY_CLOUD_NAME=
CLOUDINARY_API_KEY=
CLOUDINARY_API_SECRET=

---

### 🤖 IA
GEMINI_API_KEY=
GROQ_API_KEY=

---

## 🌐 Uso

1. Subir archivo JSON desde la interfaz  
2. Click en Procesar  
3. Ver progreso en tiempo real  

---

## 🔄 Flujo del pipeline

1. Limpieza  
2. Duplicados  
3. Clasificación  
4. IA  
5. Imágenes  
6. Base de datos  

---

## 🚀 Deploy en Railway

1. Subir a GitHub  
2. Conectar en Railway  
3. Configurar variables  
4. Deploy  

---

## 🚀 Estado

MVP funcional listo para escalar
