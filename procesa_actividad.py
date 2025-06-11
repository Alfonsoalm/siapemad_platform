import pandas as pd

# Ruta del archivo Excel original
archivo_entrada = "dataset_final/YH-00052931.xlsx"
# Ruta del archivo Excel de salida
archivo_salida = "dataset_final/YH-00052931.xlsx"
# Leer el archivo Excel
df = pd.read_excel(archivo_entrada)

# Eliminar la palabra "Consumo" de la columna 'event-id'
df['event-id'] = df['event-id'].astype(str).str.replace(r'(?i)consumo', '', regex=True).str.strip()

# Convertir la columna 'event-id' a tipo entero
df['event-id'] = pd.to_numeric(df['event-id'], errors='coerce').dropna().astype(int)

# Eliminar columnas innecesarias
columnas_a_eliminar = ['icon src', 'event-source-type', 'event-time']
df.drop(columns=[col for col in columnas_a_eliminar if col in df.columns], inplace=True)

# Guardar el nuevo archivo Excel
df.to_excel(archivo_salida, index=False)

print(f"Archivo procesado y guardado como: {archivo_salida}")