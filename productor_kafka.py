"""
Tarea 3 - Big Data (UNAD)
GENERADOR DE DATOS -> KAFKA (productor)

Simula la llegada de casos en tiempo real: toma filas del dataset real (si ya
descargaste el CSV con batch_spark.py) y las envía una a una al topic de Kafka.
Si no hay CSV, genera eventos sintéticos para no frenar la demostración.

Requisitos:  pip install kafka-python pandas
Uso:         python productor_kafka.py --eventos-por-segundo 5
"""
import argparse
import json
import os
import random
import time
from datetime import datetime

import config
from config import elegir, normalizar_nombre

DEPARTAMENTOS = ["BOGOTA D.C.", "ANTIOQUIA", "VALLE DEL CAUCA", "ATLANTICO", "MAGDALENA",
                 "SANTANDER", "CUNDINAMARCA", "BOLIVAR", "NORTE DE SANTANDER", "CORDOBA"]
ESTADOS = ["LEVE", "LEVE", "LEVE", "MODERADO", "GRAVE", "FALLECIDO"]


def eventos_desde_csv(max_filas=50000):
    """Lee el CSV real y devuelve una lista de eventos (diccionarios) ya simplificados."""
    import pandas as pd

    df = pd.read_csv(config.RUTA_CSV, nrows=max_filas, dtype=str)
    df.columns = [normalizar_nombre(c) for c in df.columns]
    c = {k: elegir(df.columns, k) for k in config.CANDIDATOS}
    eventos = []
    for _, fila in df.iterrows():
        try:
            edad = float(fila[c["edad"]])
            if c["unidad_edad"] and str(fila[c["unidad_edad"]]) == "2":
                edad /= 12
            elif c["unidad_edad"] and str(fila[c["unidad_edad"]]) == "3":
                edad /= 365
            edad = int(edad)
        except (TypeError, ValueError):
            continue
        if not 0 <= edad <= 110:
            continue
        estado = str(fila[c["estado"]]).strip().upper() if c["estado"] else "SIN DATO"
        recup = str(fila[c["recuperado"]]).strip().upper() if c["recuperado"] else ""
        eventos.append({
            "departamento": str(fila[c["departamento"]]).strip().upper(),
            "sexo": str(fila[c["sexo"]]).strip().upper(),
            "edad": edad,
            "estado": estado,
            "fallecido": int("FALLECIDO" in (estado, recup)),
        })
    return eventos


def evento_sintetico():
    estado = random.choice(ESTADOS)
    return {"departamento": random.choice(DEPARTAMENTOS), "sexo": random.choice(["F", "M"]),
            "edad": random.randint(0, 95), "estado": estado, "fallecido": int(estado == "FALLECIDO")}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--eventos-por-segundo", type=float, default=5)
    ap.add_argument("--total", type=int, default=0, help="0 = enviar sin parar (Ctrl+C para detener)")
    args = ap.parse_args()

    try:
        from kafka import KafkaProducer
    except ImportError:
        raise SystemExit("Falta la librería: pip install kafka-python   "
                         "(si da error en Python 3.12 prueba: pip install kafka-python-ng)")

    if os.path.exists(config.RUTA_CSV):
        base = eventos_desde_csv()
        print(f"Usando {len(base):,} eventos reales tomados de {config.RUTA_CSV}")
    else:
        base = []
        print("No encontré el CSV: se generarán eventos sintéticos.")

    productor = KafkaProducer(
        bootstrap_servers=config.KAFKA_SERVIDOR,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )
    print(f"Enviando al topic '{config.KAFKA_TOPIC}' en {config.KAFKA_SERVIDOR} (Ctrl+C para parar)\n")

    n = 0
    try:
        while args.total == 0 or n < args.total:
            evento = dict(random.choice(base)) if base else evento_sintetico()
            n += 1
            evento["id"] = n
            evento["evento_ts"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            productor.send(config.KAFKA_TOPIC, evento)
            print(f"[{n}] {evento}")
            time.sleep(1.0 / args.eventos_por_segundo)
    except KeyboardInterrupt:
        pass
    finally:
        productor.flush()
        productor.close()
        print(f"\nEventos enviados: {n}")


if __name__ == "__main__":
    main()
