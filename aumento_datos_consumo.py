import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def cargar_datos(ruta_excel):
    df = pd.read_excel(ruta_excel)
    # Normalizar nombres de columnas quitando espacios extra y poner coma decimal a punto
    df.columns = [c.strip() for c in df.columns]
    for col in df.columns:
        if col != 'fecha' and df[col].dtype == object:
            df[col] = df[col].astype(str).str.replace(",", ".").replace("", np.nan)
            df[col] = pd.to_numeric(df[col], errors='coerce')
    df['fecha'] = pd.to_datetime(df['fecha'])
    return df

def generar_fecha_rng(fecha_inicio, fecha_fin, registros_por_dia=25):
    # Genera fechas cada ~30 minutos entre fecha_inicio y fecha_fin
    freq = '30T'  # 30 minutos
    fechas = pd.date_range(start=fecha_inicio, end=fecha_fin, freq=freq)
    # Ajustar para que sean aprox 25 registros diarios (24h*2 = 48, filtramos hora >22)
    fechas = fechas[(fechas.hour >= 6) & (fechas.hour <= 22)]
    return fechas

def generar_datos(df_original, fechas_generadas):
    cols = df_original.columns.drop('fecha')
    distribuciones = {}

    # Calcular distribuciones básicas: media y std para cada sensor, ignorando nans
    for col in cols:
        series = df_original[col].dropna()
        distribuciones[col] = {
            'mean': series.mean(),
            'std': series.std(),
            'min': series.min(),
            'max': series.max()
        }
    
    # Función para decidir si sensor está activo en esa hora (ejemplo simple)
    def sensor_activo(sensor, hora):
        # Por ejemplo, sensores de pasillo y entrada activos solo de 18 a 23h
        if sensor in ['Luz Pasillo', 'Luz Entrada']:
            return 18 <= hora <= 23
        # Luz cocina activa de 6 a 22
        if sensor == 'Luz Cocina':
            return 6 <= hora <= 22
        # Dormitorio y baño solo noche y madrugada
        if sensor in ['Luz Dormitorio', 'Luz Bano']:
            return hora >= 20 or hora <= 6
        # Enchufe salon activo de 8 a 20
        if sensor == 'Enchufe Salon':
            return 8 <= hora <= 20
        # ConsumoTotal siempre activo
        if sensor == 'ConsumoTotal':
            return True
        # Por defecto activo todo el día
        return True

    data_gen = []
    for fecha in fechas_generadas:
        fila = {'fecha': fecha}
        hora = fecha.hour
        for col in cols:
            if not sensor_activo(col, hora):
                fila[col] = np.nan
            else:
                # Generar valor aleatorio gaussiano limitado a min-max
                mean = distribuciones[col]['mean']
                std = distribuciones[col]['std'] if distribuciones[col]['std'] > 0 else mean * 0.1
                val = np.random.normal(mean, std)
                val = max(distribuciones[col]['min'], min(val, distribuciones[col]['max']))
                # Valores muy bajos o negativos a NaN (sensores apagados o sin consumo)
                if val < 0.05 * mean:
                    val = np.nan
                fila[col] = round(val, 2)
        # Ajustar ConsumoTotal para que sea >= suma de otros consumos y no inconsistente
        cols_sin_total = [c for c in cols if c != 'ConsumoTotal']
        suma_sensores = sum([fila[c] for c in cols_sin_total if pd.notna(fila[c])])
        if pd.isna(fila['ConsumoTotal']) or fila['ConsumoTotal'] < suma_sensores:
            fila['ConsumoTotal'] = round(suma_sensores * np.random.uniform(1.0, 1.15), 2)
        data_gen.append(fila)
    
    df_gen = pd.DataFrame(data_gen)
    # Formatear fecha como texto en formato requerido
    df_gen['fecha'] = df_gen['fecha'].dt.strftime('%Y-%m-%d %H:%M:%S')
    return df_gen

def main():
    casa = "dataset_consumo_YH-00052931.xlsx"
    ruta_entrada = "./consumos/"+casa
    df_original = cargar_datos(ruta_entrada)
    
    fecha_inicio = df_original['fecha'].min()
    fecha_fin = datetime(2025, 6, 27)

    fechas_generadas = generar_fecha_rng(fecha_inicio, fecha_fin, registros_por_dia=25)
    df_generado = generar_datos(df_original, fechas_generadas)
    
    ruta_salida = "./consumos_actualizados/"+casa
    df_generado.to_excel(ruta_salida, index=False)
    print(f"Archivo generado con {len(df_generado)} filas en '{ruta_salida}'")

if __name__ == "__main__":
    main()
