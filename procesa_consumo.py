import pandas as pd
import re

# Carga el archivo de entrada
# Ruta del archivo Excel original
archivo_entrada = "dataset_final/YH-00052886.xlsx"
# Ruta del archivo Excel de salida
archivo_salida = "consumos_final/YH-00052886.xlsx"
df = pd.read_excel(archivo_entrada)

# Renombrar columnas
df.columns = ['fecha', 'valor', 'event_id', 'dispositivo', 'ubicacion']

# Función para extraer solo los consumos en W (ignorar kWh)
def extraer_watts(valor):
    if isinstance(valor, str):
        if 'W' in valor and 'kWh' not in valor and not valor.startswith('Acción'):
            try:
                return float(re.findall(r"[\d.]+", valor)[0])
            except:
                return None
    return None

df['consumo'] = df['valor'].apply(extraer_watts)
df = df.dropna(subset=['consumo'])

# Mapeo de dispositivos a columnas de salida
mapa_columnas = {
    'Enchufe Cocina': 'Enchufe Cocina',
    'Enchufe Salon': 'Enchufe Salon',
    'Luz Salon': 'Luz Salon',
    'Luz Dormitorio': 'Luz Dormitorio',
    'Luz Bano': 'Luz Bano',
    'Persiana Salon': 'Persiana Salon'
}

# Columnas del Excel de salida
columnas = ['fecha'] + list(mapa_columnas.values()) + ['ConsumoTotal']
df_salida = pd.DataFrame(columns=columnas)

# Agrupar y estructurar
agrupado = df.groupby(['fecha', 'dispositivo']).agg({'consumo': 'max'}).reset_index()

for _, fila in agrupado.iterrows():
    fecha = fila['fecha']
    dispositivo = fila['dispositivo']
    consumo = fila['consumo']

    fila_output = dict.fromkeys(columnas, '')
    fila_output['fecha'] = fecha
    if dispositivo in mapa_columnas:
        col_dispositivo = mapa_columnas[dispositivo]
        fila_output[col_dispositivo] = consumo
        fila_output['ConsumoTotal'] = consumo  # Puedes sumar si lo deseas
        df_salida = pd.concat([df_salida, pd.DataFrame([fila_output])], ignore_index=True)

# Asegura formato numérico
for col in columnas[1:]:
    df_salida[col] = pd.to_numeric(df_salida[col], errors='coerce')

df_salida = df_salida.fillna('')

# Guardar archivo resultante
df_salida.to_excel(archivo_salida, index=False)
