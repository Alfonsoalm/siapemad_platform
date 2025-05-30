import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Parámetros
num_rows = 300
productos = ["Maíz", "Trigo", "Frijol", "Soya", "Café", "Caña de Azúcar"]
regiones = ["Norte", "Sur", "Centro", "Este", "Oeste"]
mercados = ["Mercado Local", "Mercado Internacional"]
años = [2022, 2023, 2024]

# Definir rangos de precios diferentes para cada producto
rango_precios = {
    "Maíz": (100, 200),
    "Trigo": (150, 250),
    "Frijol": (80, 180),
    "Soya": (200, 350),
    "Café": (300, 500),
    "Caña de Azúcar": (50, 100)
}

# Generar Producción_Agropecuaria.csv con variabilidad
data_produccion = {
    "Region": [random.choice(regiones) for _ in range(num_rows)],
    "Producto": [random.choice(productos) for _ in range(num_rows)],
    "Toneladas": [random.randint(500, 2000) for _ in range(num_rows)],  # Variabilidad en toneladas
    "Hectareas": [random.randint(100, 500) for _ in range(num_rows)],    # Variabilidad en hectáreas
    "Costo": [random.randint(20000, 100000) for _ in range(num_rows)],    # Variabilidad en el costo
    "Año": [random.choice(años) for _ in range(num_rows)]
}
df_produccion = pd.DataFrame(data_produccion)

# Generar Precios_Mercado.csv con variabilidad en función del producto
fechas = [datetime(2022, 1, 1) + timedelta(days=random.randint(1, 365)) for _ in range(num_rows)]
data_precios = {
    "Producto": [],
    "Mercado": [],
    "Precio": [],
    "Fecha": []
}

for i in range(num_rows):
    producto = random.choice(productos)
    min_precio, max_precio = rango_precios[producto]
    data_precios["Producto"].append(producto)
    data_precios["Mercado"].append(random.choice(mercados))
    data_precios["Precio"].append(round(random.uniform(min_precio, max_precio), 2))  # Precio basado en el producto
    data_precios["Fecha"].append(fechas[i].strftime("%Y-%m-%d"))

df_precios = pd.DataFrame(data_precios)
df_precios.to_csv("C:/Users/CTM40/Desktop/Ejercicios Curso Power Bi/Dia_3 Visualizaciones Avanzadas y Diseño de Dashboard/Ejemplo 7/Precios_Mercado.csv", index=False)


# Generar Clima.csv con variabilidad
data_clima = {
    "Region": [random.choice(regiones) for _ in range(num_rows)],
    "Temperatura": [round(random.uniform(15, 35), 1) for _ in range(num_rows)],  # Variabilidad en temperatura
    "Precipitacion": [round(random.uniform(0, 300), 1) for _ in range(num_rows)],  # Variabilidad en precipitación
    "Humedad": [round(random.uniform(20, 100), 1) for _ in range(num_rows)],  # Variabilidad en humedad
    "Fecha": [fecha.strftime("%Y-%m-%d") for fecha in fechas]
}
df_clima = pd.DataFrame(data_clima)
print("Archivos generados: Producción_Agropecuaria.csv, Precios_Mercado.csv, Clima.csv")

