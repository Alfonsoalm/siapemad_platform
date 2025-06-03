import pandas as pd
import re
import numpy as np
from datetime import datetime, timedelta

def anadir_fecha_dataset(df):
    print("Inicio anadir_fecha_dataset")
    fecha = datetime(2024, 2, 2)
    hora_actual_str = datetime.now().strftime('%H:%M:%S')
    hora_actual = datetime.strptime(hora_actual_str, '%H:%M:%S').time()

    print(f"Hora actual: {hora_actual}")

    df['event-time'] = pd.to_datetime(df['event-time'], format='%H:%M:%S').dt.time
    print("Convertida columna 'event-time' a tipo datetime.time")

    hora_anterior = hora_actual
    lista = []
    lista_tuplas = []

    df.dropna(subset=['event-time'], inplace=True)
    print(f"Filas tras eliminar NaN en 'event-time': {len(df)}")

    for index, row in df.iterrows():
        print(f"Fila {index} - event-time: {row['event-time']} | hora_anterior: {hora_anterior}")
        if hora_anterior >= row['event-time']:
            fecha_evento = datetime.combine(fecha, row['event-time'])
            print(f"Fecha evento asignada: {fecha_evento}")
            # El siguiente código usa variable 'casa' que no está definida aquí, podría fallar:
            # lista_tuplas.append(('casa', casa))
        else:
            fecha -= timedelta(days=1)
            print(f"Fecha decrementada: {fecha}")
            fecha_evento = datetime.combine(fecha, row['event-time'])

            lista.append(lista_tuplas)
            lista_tuplas = []
            print("Lista de tuplas reiniciada")

        hora_anterior = row['event-time']
        df.at[index, 'event-time'] = fecha_evento

    print("Fin anadir_fecha_dataset")
    return df


def eliminar_columnas_innecesarias(df_casa):
    print("Inicio eliminar_columnas_innecesarias")
    cols_to_drop = ['ng-star-inserted src', 'event-source-type']
    unnamed_columns = [col for col in df_casa.columns if col.startswith('Unnamed')]
    cols_to_drop = cols_to_drop + unnamed_columns
    print(f"Columnas a eliminar: {cols_to_drop}")

    # df_casa = df_casa.drop(cols_to_drop, axis=1)
    print(f"Columnas eliminadas, columnas restantes: {df_casa.columns.tolist()}")

    df_casa['marquee-text'] = df_casa['marquee-text'].replace('Ã³', 'ó', regex=True)
    df_casa['marquee-text 2'] = df_casa['marquee-text 2'].replace(['salÃ³n','SalÃ³n'], 'salón', regex=True)
    df_casa['marquee-text 3'] = df_casa['marquee-text 3'].replace('SalÃ³n', 'salón', regex=True)

    df_casa.dropna(subset=['marquee-text'], inplace=True)
    print(f"Filas tras eliminar NaN en 'marquee-text': {len(df_casa)}")
    print("Fin eliminar_columnas_innecesarias")
    return df_casa


def renombrar_columna_eventos(df_casa):
    print("Inicio renombrar_columna_eventos")
    # (Resumí para no saturar, mantén tus reemplazos aquí)
    # ...
    print("Reemplazos realizados en 'marquee-text 2'")

    diccionario_de_eventos = {...}  # tu diccionario

    for index, row in df_casa.iterrows():
        valor_a_buscar = row['marquee-text 2']
        tipo_de_evento = row['marquee-text']

        if valor_a_buscar in diccionario_de_eventos:
            nuevo_valor = diccionario_de_eventos[valor_a_buscar]

            if tipo_de_evento in ['Encendido', 'Acción: Encender', 'AcciÃ³n: Encender', 'AcciÃ³n: Abrir']:
                df_casa.at[index, 'event-id'] = str(nuevo_valor)+'.1'
            elif tipo_de_evento in ['Apagado', 'Acción: Apagar', 'AcciÃ³n: Apagar', 'AcciÃ³n: Cerrar']:
                df_casa.at[index, 'event-id'] = str(nuevo_valor)+'.2'
            elif 'W' in str(tipo_de_evento):
                df_casa.at[index, 'event-id'] = 'Consumo'+str(nuevo_valor)

    print("Fin renombrar_columna_eventos")
    return df_casa


def eliminar_registros(df_casa):
    print("Inicio eliminar_registros")
    before_len = len(df_casa)
    df_casa = df_casa[~(df_casa['marquee-text'].str.contains('Reconfiguraci', na=False))]
    df_casa = df_casa[~(df_casa['marquee-text'].str.contains('Solicita', na=False))]
    df_casa = df_casa[~df_casa['marquee-text 2'].isin([...])]  # mantén tu lista
    after_len = len(df_casa)
    print(f"Filas antes: {before_len}, después de eliminar: {after_len}")
    print("Fin eliminar_registros")
    return df_casa


def extraer_valor_numerico(cadena):
    valor_numerico_str = ''.join(caracter for caracter in cadena if caracter.isdigit() or caracter == '.')
    print(f"Extraído valor numérico de '{cadena}': {valor_numerico_str}")
    if valor_numerico_str == '':
        return 0.0
    return float(valor_numerico_str)


def sintetizar_en_intervalos(df_casa):
    print('Sintetizando en intervalos de 5 minutos...')
    df_consumo = df_casa.copy()

    rango_5_minutos = pd.date_range(start=df_casa.index.min(), end=df_casa.index.max(), freq='5T')
    print(f"Rango de fechas desde {rango_5_minutos.min()} hasta {rango_5_minutos.max()}")

    df_consumo = df_consumo[df_consumo['marquee-text'].str.contains('W', na=False)]
    df_consumo = df_consumo[~df_consumo['marquee-text'].str.contains('kWh', na=False)]
    df_consumo['marquee-text'] = df_consumo['marquee-text'].apply(extraer_valor_numerico)

    df_casa = df_casa[~df_casa['marquee-text'].str.contains('W', na=False)]
    df_casa = df_casa[~df_casa['marquee-text'].str.contains('kWh', na=False)]

    lista_diccionarios = []
    lista_diccionarios_consumo = []

    for start_time in rango_5_minutos:
        diccionario = {'fecha': start_time}
        diccionario_consumo = {'fecha': start_time}
        end_time = start_time + pd.Timedelta(minutes=5)

        eventos_en_intervalo = df_casa.loc[(df_casa.index >= start_time) & (df_casa.index < end_time)]
        eventos_en_intervalo_consumo = df_consumo.loc[(df_consumo.index >= start_time) & (df_consumo.index < end_time)]

        lista_eventos = eventos_en_intervalo['event-id'].unique()
        lista_eventos_consumo = eventos_en_intervalo_consumo['event-id'].unique()

        if len(lista_eventos) != 0:
            grupos_por_id = eventos_en_intervalo_consumo.groupby('event-id')

            for evento in lista_eventos:
                if pd.notna(evento) and 'Consumo' not in str(evento):
                    diccionario[evento] = eventos_en_intervalo['event-id'].value_counts().get(evento, 0)

            for evento in lista_eventos_consumo:
                if pd.notna(evento) and 'Consumo' in str(evento):
                    diccionario_consumo[evento] = grupos_por_id.get_group(evento)['marquee-text'].sum()

            lista_diccionarios.append(diccionario)
            lista_diccionarios_consumo.append(diccionario_consumo)

    df_nuevo = pd.DataFrame(lista_diccionarios)
    df_nuevo_consumo = pd.DataFrame(lista_diccionarios_consumo)

    df_nuevo_consumo['ConsumoTotal'] = df_nuevo_consumo.filter(like='Consumo').fillna(0).astype(float).sum(axis=1)
    print("Columnas del df_nuevo:", df_nuevo.columns)
    print("Cantidad de filas en df_nuevo:", len(df_nuevo))
    print('Intervalos sintetizados.')
    return df_nuevo


if __name__ == '__main__':
    print("Inicio del script principal")
    casas = ['YH-00049797']

    df = pd.DataFrame()

    for i, casa in enumerate(casas):
        print(f"Cargando datos de la casa: {casa}")
        df_casa = pd.read_excel('./historial_fibaro.xlsx')
        df_casa['casa'] = casa

        df_casa = anadir_fecha_dataset(df_casa)

        df_casa.set_index('event-time', inplace=True)
        print("Índice establecido en 'event-time'")

        df_casa = eliminar_columnas_innecesarias(df_casa)

        df_casa = renombrar_columna_eventos(df_casa)

        df_casa = eliminar_registros(df_casa)
        print('Registros innecesarios eliminados')

        df = pd.concat([df, sintetizar_en_intervalos(df_casa)])

    sensores_activos = []
    for idx, row in df.iterrows():
        elementos_no_nan = row[row.notna()]
        sensores = [sensor for sensor in elementos_no_nan.index if str(sensor).endswith('.1')]
        if sensores:
            for s in sensores:
                if s not in sensores_activos:
                    sensores_activos.append(s)

        if sensores_activos:
            for col in sensores_activos:
                df.at[idx, col] = 1

        sensores = [sensor for sensor in elementos_no_nan.index if str(sensor).endswith('.2')]
        for sensor in sensores:
            sensor_a = str(sensor).split('.')[0] + '.1'
            if sensor_a in sensores_activos:
                sensores_activos.remove(sensor_a)
    print("Columnas disponibles en df justo antes de eliminar 'fecha':", df.columns)

    df_sin_fecha = df.drop(['fecha'], axis=1)
    df['Inactividad'] = df_sin_fecha.isna().all(axis=1).astype(int)

    print("Guardando dataset final")
    df.to_excel('DATASET_FINAL2.0_'+casa+'.xlsx')
    print("Proceso finalizado")
