"""
Configuración central del proyecto - Tarea 3 Big Data (UNAD).
Si cambias de conjunto de datos, solo tienes que tocar este archivo.
"""
import os
import re
import unicodedata

# ---------- Fuente de datos (Datos Abiertos Colombia: casos positivos de COVID-19) ----------
DATA_URL = "https://www.datos.gov.co/resource/gt2j-8ykr.csv"
LIMITE_FILAS = int(os.getenv("LIMITE_FILAS", "500000"))  # bájalo si tu máquina virtual tiene poca RAM
RUTA_CSV = os.getenv("RUTA_CSV", "datos/covid_colombia.csv")
CARPETA_SALIDA = os.getenv("CARPETA_SALIDA", "salida")

# ---------- Spark ----------
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

# ---------- Kafka ----------
KAFKA_SERVIDOR = os.getenv("KAFKA_SERVIDOR", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "casos_covid")

# ---------- Columnas: nombres posibles (en orden de preferencia) ----------
# El código busca la primera que exista en el archivo, así no se rompe si
# el portal cambia un poco los nombres.
CANDIDATOS = {
    "departamento": ["departamento_nom", "nombre_departamento", "departamento"],
    "municipio": ["ciudad_municipio_nom", "nombre_municipio", "ciudad_municipio"],
    "sexo": ["sexo"],
    "edad": ["edad"],
    "unidad_edad": ["unidad_medida", "unidad_de_medida_de_edad"],
    "estado": ["estado"],
    "recuperado": ["recuperado"],
    "contagio": ["fuente_tipo_contagio", "tipo_de_contagio"],
    "fecha": ["fecha_diagnostico", "fecha_de_notificaci_n", "fecha_reporte_web"],
}


def normalizar_nombre(nombre):
    """'Fecha de Notificación' -> 'fecha_de_notificacion' (sin tildes, espacios ni símbolos)."""
    s = unicodedata.normalize("NFKD", str(nombre)).encode("ascii", "ignore").decode()
    return re.sub(r"[^0-9a-zA-Z]+", "_", s.strip().lower()).strip("_")


def elegir(columnas, clave):
    """Devuelve el nombre real de la columna para 'clave', o None si no existe."""
    for candidato in CANDIDATOS[clave]:
        if candidato in columnas:
            return candidato
    return None
