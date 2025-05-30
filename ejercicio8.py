import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# Ruta de guardado
ruta_guardado = r"C:/Users/CTM40/Desktop/Ejercicios Curso Power Bi/Dia_3 Visualizaciones Avanzadas y Diseño de Dashboard/Ejemplo 8"
os.makedirs(ruta_guardado, exist_ok=True)

# Parámetros
num_rows = 300
productos = ["Madera", "Fertilizante", "Maquinaria", "Semillas", "Herbicidas", "Insecticidas"]
regiones = ["Norte", "Sur", "Centro", "Este", "Oeste"]
canales = ["Tienda Física", "Online", "Distribuidor"]
agentes = ["Agente_1", "Agente_2", "Agente_3", "Agente_4", "Agente_5"]
monedas = ["USD", "EUR"]
años = [2022, 2023, 2024]

# Rango de precios y costos específicos para cada producto
rango_precios = {
    "Madera": (100, 200),
    "Fertilizante": (150, 250),
    "Maquinaria": (500, 800),
    "Semillas": (20, 50),
    "Herbicidas": (30, 60),
    "Insecticidas": (40, 70)
}

rango_costos = {
    "Madera": (50, 100),
    "Fertilizante": (80, 120),
    "Maquinaria": (300, 500),
    "Semillas": (10, 25),
    "Herbicidas": (15, 35),
    "Insecticidas": (20, 45)
}

# Generar Ventas.csv
fechas = [datetime(2022, 1, 1) + timedelta(days=random.randint(1, 365)) for _ in range(num_rows)]
data_ventas = {
    "Fecha": [fecha.strftime("%Y-%m-%d") for fecha in fechas],
    "Canal": [random.choice(canales) for _ in range(num_rows)],
    "Agente": [random.choice(agentes) for _ in range(num_rows)],
    "Producto": [random.choice(productos) for _ in range(num_rows)],
    "Cantidad": [random.randint(1, 50) for _ in range(num_rows)],
    "Precio_Unitario": [],
    "Costo_Unitario": [],
    "Moneda": [random.choice(monedas) for _ in range(num_rows)]
}

for producto in data_ventas["Producto"]:
    min_precio, max_precio = rango_precios[producto]
    min_costo, max_costo = rango_costos[producto]
    data_ventas["Precio_Unitario"].append(round(random.uniform(min_precio, max_precio), 2))
    data_ventas["Costo_Unitario"].append(round(random.uniform(min_costo, max_costo), 2))

df_ventas = pd.DataFrame(data_ventas)
df_ventas.to_csv(os.path.join(ruta_guardado, "Ventas.csv"), index=False)

# Generar Costos.csv
data_costos = {
    "Producto": [random.choice(productos) for _ in range(num_rows)],
    "Tipo_Costo": [random.choice(["Transporte", "Almacenamiento", "Marketing"]) for _ in range(num_rows)],
    "Monto": [round(random.uniform(100, 1000), 2) for _ in range(num_rows)],
    "Moneda": [random.choice(monedas) for _ in range(num_rows)]
}

df_costos = pd.DataFrame(data_costos)
df_costos.to_csv(os.path.join(ruta_guardado, "Costos.csv"), index=False)

# Generar Cambio_Moneda.csv
data_cambio_moneda = {
    "Moneda": ["USD", "EUR"],
    "Tipo_Cambio": [1, 1.2]  # Por ejemplo, 1 USD = 1.2 EUR
}

df_cambio_moneda = pd.DataFrame(data_cambio_moneda)
df_cambio_moneda.to_csv(os.path.join(ruta_guardado, "Cambio_Moneda.csv"), index=False)

print("Archivos generados en la ruta especificada:")
print(f"{os.path.join(ruta_guardado, 'Ventas.csv')}")
print(f"{os.path.join(ruta_guardado, 'Costos.csv')}")
print(f"{os.path.join(ruta_guardado, 'Cambio_Moneda.csv')}")
