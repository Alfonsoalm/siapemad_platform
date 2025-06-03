import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

# Define un número razonable máximo de filas a generar para prevenir errores de memoria.
# Puedes ajustar este valor según la memoria de tu sistema y tus necesidades.
MAX_ROWS_TO_GENERATE = 1_000_000 # 1 millón de filas como límite por defecto

def calcular_frecuencias(df):
    """
    Calcula las frecuencias relativas de las combinaciones únicas de columnas,
    excluyendo la columna 'event-time'.
    """
    cols_sin_fecha = df.columns.difference(['event-time'])
    # Asegura que las columnas de agrupación se traten como una lista para groupby
    counts = df.groupby(list(cols_sin_fecha)).size()
    freq = counts / counts.sum()
    return freq

def obtener_filas_unicas(df):
    """
    Obtiene las filas únicas del DataFrame, excluyendo la columna 'event-time'
    para la determinación de unicidad, pero manteniendo todas las columnas
    en las filas resultantes.
    """
    cols_sin_fecha = df.columns.difference(['event-time'])
    unique_rows = df.drop_duplicates(subset=list(cols_sin_fecha)).copy()
    return unique_rows

def calcular_num_rows_a_generar(df, fecha_inicial, fecha_actual):
    """
    Calcula el número de filas a generar basándose en la diferencia de tiempo
    entre la fecha actual y la fecha inicial, y el promedio de las diferencias
    de tiempo entre eventos en el DataFrame original.
    Aplica un límite máximo para evitar la generación de un número excesivo de filas.
    También asegura que avg_diff no sea extremadamente pequeño.
    """
    df_sorted = df.sort_values('event-time')
    time_diffs = df_sorted['event-time'].diff().dropna()

    # Define un umbral mínimo para avg_diff para evitar valores extremadamente pequeños
    MIN_AVG_DIFF = timedelta(milliseconds=1) # Mínimo 1 milisegundo

    if time_diffs.empty:
        print("Advertencia: No hay suficientes diferencias de tiempo para calcular el promedio. Se usará 1 milisegundo como avg_diff por defecto.")
        avg_diff = MIN_AVG_DIFF
    else:
        avg_diff = time_diffs.mean()
        # Si avg_diff es menor que el mínimo permitido, lo ajustamos
        if avg_diff < MIN_AVG_DIFF:
            print(f"Advertencia: avg_diff calculado ({avg_diff}) es muy pequeño. Se ajustará a {MIN_AVG_DIFF}.")
            avg_diff = MIN_AVG_DIFF

    print(f"Fecha inicial de los datos: {fecha_inicial}")
    print(f"Fecha actual de generación: {fecha_actual}")
    print(f"Diferencia de tiempo promedio (ajustada si es necesario): {avg_diff}")

    total_seconds = (fecha_actual - fecha_inicial).total_seconds()

    if avg_diff.total_seconds() > 0:
        num_rows = int(total_seconds / avg_diff.total_seconds())
    else:
        # Fallback si avg_diff.total_seconds() es cero o negativo (no debería ocurrir con el ajuste)
        print("Advertencia: avg_diff.total_seconds() no es positivo. Se generarán 1000 filas por defecto.")
        num_rows = 1000

    print(f"Número de filas calculado inicialmente: {num_rows}")

    # Aplica el límite máximo para evitar errores de memoria
    if num_rows > MAX_ROWS_TO_GENERATE:
        print(f"Advertencia: El número de filas calculado ({num_rows}) excede el límite máximo ({MAX_ROWS_TO_GENERATE}). Se generarán {MAX_ROWS_TO_GENERATE} filas.")
        num_rows = MAX_ROWS_TO_GENERATE
    elif num_rows <= 0: # Asegura que al menos 1 fila se genere si el cálculo resulta en 0 o menos
        print(f"Advertencia: El número de filas calculado es {num_rows}. Se generará al menos 1 fila.")
        num_rows = 1

    return num_rows, avg_diff

def generar_datos(freq, unique_rows, fecha_inicial, avg_diff, num_rows_to_generate):
    """
    Genera nuevas filas de datos basándose en las frecuencias y filas únicas
    del DataFrame original, asignando tiempos secuenciales.
    """
    indices = freq.index
    probabilities = freq.values

    # Pre-procesa unique_rows para una búsqueda más rápida.
    # Crea un mapeo de la combinación de clave sin fecha (tupla) a la fila única completa (Series).
    unique_rows_map = {
        tuple(row[unique_rows.columns.difference(['event-time'])]): row
        for _, row in unique_rows.iterrows()
    }

    sampled_indices = np.random.choice(len(indices), size=num_rows_to_generate, p=probabilities)
    rows_list = []
    current_time = fecha_inicial
    
    print(f"Iniciando la generación de {num_rows_to_generate} filas...")

    for i, idx in enumerate(sampled_indices):
        # Imprime el progreso cada X filas para evitar saturar la consola
        if num_rows_to_generate > 100000 and (i + 1) % 10000 == 0:
            print(f"Procesando fila {i + 1}/{num_rows_to_generate}...")
        elif num_rows_to_generate <= 100000 and (i + 1) % 1000 == 0:
            print(f"Procesando fila {i + 1}/{num_rows_to_generate}...")
        elif num_rows_to_generate <= 1000 and (i + 1) % 100 == 0:
            print(f"Procesando fila {i + 1}/{num_rows_to_generate}...")
        elif num_rows_to_generate < 100: # Para números muy pequeños, imprime cada fila
            # Solo imprime las primeras 10 filas y las últimas 10 para no saturar si es muy pequeño
            if i < 10 or i >= num_rows_to_generate - 10:
                print(f"Procesando fila {i + 1}/{num_rows_to_generate}. Hora: {current_time}")

        # Obtiene la tupla de clave muestreada
        sampled_key_tuple = indices[idx]
        
        # Recupera los datos de la fila base usando el mapa pre-construido
        base_row_data = unique_rows_map[sampled_key_tuple].copy()
        
        # Asigna el nuevo 'event-time'
        base_row_data['event-time'] = current_time
        rows_list.append(base_row_data)
        
        # Incrementa el tiempo para la siguiente fila
        current_time += avg_diff

    df_final = pd.DataFrame(rows_list)
    
    # Asegura que las columnas estén en el orden original
    cols_original = unique_rows.columns
    df_final = df_final[cols_original]

    return df_final

def main():
    """
    Función principal para cargar datos, calcular frecuencias, generar nuevas filas
    y guardar el DataFrame resultante en un nuevo archivo Excel.
    """
    archivo = 'YH-00052931.xlsx'
    input_path = f'./actividades/{archivo}'
    output_path = f'./actividades_actualizadas/{archivo}'

    # Crear el directorio de salida si no existe
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Cargar el DataFrame y convertir la columna 'event-time' a datetime
    try:
        df = pd.read_excel(input_path)
    except FileNotFoundError:
        print(f"Error: El archivo '{input_path}' no se encontró. Asegúrate de que la ruta y el nombre del archivo son correctos.")
        return
    except Exception as e:
        print(f"Error al leer el archivo Excel: {e}")
        return

    if 'event-time' not in df.columns:
        print("Error: La columna 'event-time' no se encontró en el archivo Excel.")
        return

    df['event-time'] = pd.to_datetime(df['event-time'])
    print(f"Fecha mínima ('event-time') en el archivo original: {df['event-time'].min()}")

    # Calcular la fecha inicial del DataFrame y la fecha actual objetivo
    fecha_inicial = df['event-time'].min()
    # Asegura que fecha_actual sea un objeto datetime, no solo una fecha
    fecha_actual = pd.to_datetime(datetime(2025, 6, 27))

    # Calcular frecuencias y obtener filas únicas
    freq = calcular_frecuencias(df)
    unique_rows = obtener_filas_unicas(df)

    # Calcular el número de filas a generar y la diferencia promedio de tiempo
    num_rows_to_generate, avg_diff = calcular_num_rows_a_generar(df, fecha_inicial, fecha_actual)

    # Generar los datos finales
    df_final = generar_datos(freq, unique_rows, fecha_inicial, avg_diff, num_rows_to_generate)

    # Guardar el DataFrame final en un archivo Excel
    try:
        df_final.to_excel(output_path, index=False)
        print(f"\nArchivo generado: {output_path} con {len(df_final)} filas desde {fecha_inicial} hasta {fecha_actual}")
    except Exception as e:
        print(f"Error al guardar el archivo Excel: {e}")

if __name__ == '__main__':
    main()