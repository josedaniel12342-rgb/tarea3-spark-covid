# Tarea 3 – Procesamiento de datos con Apache Spark (Big Data, UNAD)

Análisis de los **casos positivos de COVID-19 en Colombia** (Datos Abiertos Colombia, Instituto Nacional de Salud)
con Apache Spark (PySpark) en dos modalidades:

1. **Batch** (`batch_spark.py`): descarga, limpieza, EDA (DataFrames, RDDs y Spark SQL) y gráficos.
2. **Tiempo real** (`productor_kafka.py` + `streaming_spark.py`): un generador envía casos a un topic de Kafka y
   Spark Structured Streaming los consume, cuenta eventos y actualiza un gráfico en vivo.

**Fuente de datos:** https://www.datos.gov.co/Salud-y-Protecci-n-Social/Casos-positivos-de-COVID-19-en-Colombia/gt2j-8ykr

## Problema
¿Cómo se distribuyeron los casos por departamento, sexo y grupo de edad, cómo evolucionaron en el tiempo y qué
letalidad se observa en cada grupo?

## Estructura
| Archivo | Para qué sirve |
|---|---|
| `config.py` | Parámetros (URL, rutas, topic de Kafka, nombres de columnas). |
| `batch_spark.py` | Procesamiento batch + visualización (PNG/CSV en `salida/`). |
| `productor_kafka.py` | Generador de datos que publica eventos JSON en Kafka. |
| `streaming_spark.py` | Consumidor Spark Streaming: conteos por departamento y por ventana de 10 s. |

## Requisitos
- Python 3.8+ y Java compatible con tu versión de Spark (8, 11 o 17)
- Apache Spark (PySpark) y Apache Kafka funcionando (ver Anexo 3 del curso)
- `pip install pyspark pandas matplotlib kafka-python`

## Cómo ejecutar
```bash
# 1) Batch (descarga el CSV la primera vez; LIMITE_FILAS controla el tamaño)
LIMITE_FILAS=300000 python batch_spark.py

# 2) Tiempo real (con Kafka encendido y el topic creado)
#    Terminal A – productor
python productor_kafka.py --eventos-por-segundo 5
#    Terminal B – streaming (el paquete debe coincidir con tu versión de Spark y Scala)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:<VERSION_DE_SPARK> streaming_spark.py
```
Resultados: carpeta `salida/` (gráficos `grafico_*.png`, tablas `.csv` y `streaming_en_vivo.png`).

## Autor
José Daniel Sánchez Palomino – Ingeniería de Sistemas, UNAD.
