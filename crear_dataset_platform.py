import pandas as pd
import re
import numpy as np
from datetime import datetime, timedelta

def anadir_fecha_dataset(df):
    print("Inicio anadir_fecha_dataset")
    fecha = datetime.now()
    print(f"Fecha inicial: {fecha}")
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


def renombrar_columna_eventos(df_casa):

    df_casa['marquee-text 2'] = df_casa['marquee-text 2'].replace(['Luz pasillo', 'Luz 2 pasillo', 'Luz 1 Pasillo', 'Luz 2 Pasillo'], 'Luz Pasillo', regex=False)
    df_casa['marquee-text 2'] = df_casa['marquee-text 2'].replace(['Luz cocina'], 'Luz Cocina', regex=False)
    df_casa['marquee-text 2'] = df_casa['marquee-text 2'].replace(['Luz salón', 'Luz Salon', 'Luz 2 Salón','Luz 1 Salón','Luz 1 salon','Luz 2 salon','Luz 1 salÃ³n', 'Luz 2 salón', 'Luz 1 salón'], 'Luz Salon', regex=False)
    df_casa['marquee-text 2'] = df_casa['marquee-text 2'].replace(['Luz baño', 'Luz 2 BaÃ±o', 'Luz 1 Baño', 'Luz 1 BaÃ±o', 'Luz 1 baño', 'Luz Baño', 'Luz 1 baÃ±o', 'Luz BaÃ±o', 'Luz 2 baño', 'Luz 2 Baño'], 'Luz Bano', regex=False)
    df_casa['marquee-text 2'] = df_casa['marquee-text 2'].replace(['Luz dormitorio', 'Luz 1 Dormitorio', 'Luz 1 dormitorio', 'Luz 2 dormitorio', 'Luz 2 Dormitorio', 'Luz Dormitorio'], 'Luz Dormitorio', regex=False)
    df_casa['marquee-text 2'] = df_casa['marquee-text 2'].replace(['Luz de la entrada', 'Luz 2 entrada'], 'Luz Entrada', regex=False)
    df_casa['marquee-text 2'] = df_casa['marquee-text 2'].replace(['Enchufe salón 2', 'Enchufe salón', 'Enchufe Salón','Enchufe salón', 'Enchufe salÃ³n 2', 'Enchufe SalÃ³n','Enchufe salon 2', 'Enchufe Salon'], 'Enchufe Salon', regex=False)
    df_casa['marquee-text 2'] = df_casa['marquee-text 2'].replace(['Persiana salon','Persiana 2 salon','Persiana salÃ³n','Persiana 2 salÃ³n','Persiana Salon Centro', 'Persiana Salon Derecha', 'Persiana Salon Izquierda', 'Persiana', 'Persiana Salon', 'Persiana 2 salón', 'Persiana salón'], 'Persiana Salon', regex=False)
    df_casa['marquee-text 2'] = df_casa['marquee-text 2'].replace(['Puerta','La puerta de la calle'], 'Puerta', regex=False)
    df_casa['marquee-text 2'] = df_casa['marquee-text 2'].replace(['Sensor movimiento salón'], 'Sensor Movimiento Salon', regex=False)
    df_casa['marquee-text 2'] = df_casa['marquee-text 2'].replace(['Sensor de movimiento del pasillo'], 'Sensor Movimiento Pasillo', regex=False)
    df_casa['marquee-text 2'] = df_casa['marquee-text 2'].replace(['Enchufe cocina', 'Enchufe Cocina'], 'Enchufe Cocina', regex=False)
    df_casa['marquee-text 2'] = df_casa['marquee-text 2'].replace(['Persiana Salida'], 'Persiana Salida', regex=False)
    df_casa['marquee-text 2'] = df_casa['marquee-text 2'].replace(['Botón', 'BotÃ³n'], 'Boton', regex=False)
    df_casa['marquee-text 2'] = df_casa['marquee-text 2'].replace(['Temperatura'], 'Temperatura', regex=False)
    df_casa['marquee-text 2'] = df_casa['marquee-text 2'].replace(['Temperatura Salon', 'Temperatura salón'], 'Temperatura Salon', regex=False)
    df_casa['marquee-text 2'] = df_casa['marquee-text 2'].replace(['Temperatura del pasillo'], 'Temperatura Pasillo', regex=False)
    df_casa['marquee-text 2'] = df_casa['marquee-text 2'].replace(['zwave'], 'Zwave', regex=False)
    df_casa['marquee-text 2'] = df_casa['marquee-text 2'].replace(['Porterillo'], 'Porterillo', regex=False)
    df_casa['marquee-text 2'] = df_casa['marquee-text 2'].replace(['SmartImplant_2'], 'SmartImplant', regex=False)
    df_casa['marquee-text 2'] = df_casa['marquee-text 2'].replace(['Cuarto lavadora'], 'Cuarto Lavadora' , regex=False)
    df_casa['marquee-text 2'] = df_casa['marquee-text 2'].replace(['Persiana dormitorio'], 'Persiana Dormitorio', regex=False)
    df_casa['marquee-text 2'] = df_casa['marquee-text 2'].replace(['Descolgar'], 'Descolgar', regex=False)
    df_casa['marquee-text 2'] = df_casa['marquee-text 2'].replace(['Luminosidad salón'], 'Luminosidad Salon', regex=False)
    df_casa['marquee-text 2'] = df_casa['marquee-text 2'].replace(['Luminosidad del pasillo'], 'Luminosidad Pasillo', regex=False)
    df_casa['marquee-text 3'] = df_casa['marquee-text 3'].str.replace('Casa/', '', regex=False).str.replace('Default Section/', '', regex=False)

    diccionario_de_eventos = {'Luz Pasillo':'00', 'Luz Cocina':'01', 'Luz Salon':'02', 'Luz Bano':'03', 'Luz Dormitorio':'04', 'Luz Entrada':'05', 
                              'Enchufe Salon':'06', 'Persiana Salon':'07', 'Puerta':'08', 'Sensor Movimiento Salon':'09', 'Sensor Movimiento Pasillo':'10', 'Enchufe Cocina':'11', 'Persiana Salida':'12', 
                              'Boton':'13', 'Temperatura':'14', 'Temperatura Salon':'15', 'Temperatura Pasillo':'16', 'Zwave':'17', 'Porterillo':'18', 'SmartImplant':'19', 'Cuarto Lavadora':'20', 'Persiana Dormitorio':'21', 
                              'Descolgar':'22', 'Luminosidad Salon':'23', 'Luminosidad Pasillo':'24'}
    
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

    return df_casa


def eliminar_registros(df_casa):

    df_casa = df_casa[~(df_casa['marquee-text'].str.contains('Reconfiguraci', na=False))]
    df_casa = df_casa[~(df_casa['marquee-text'].str.contains('Solicita', na=False))]

    df_casa = df_casa[~df_casa['marquee-text 2'].isin(['22.0.4', '25', '27', '27.0', '28', '30','30.0', '32', '32.0', '33', '34', '35', '35.0', '36', '36.0', '38', '38.0', '39', '41', '41.0', 
                                                    '42', '42.0', '43', '43.0', '44', '44.0', '47', '48', '48.0.4', '49.0','49', '54', '54.0', '56', '58', '60', '60.0', '64', '65', '65.0.1', '65.3', 
                                                    '65.4', '65.7', '66', '69', '69.0.1','69.3', '69.4', '69.6', '69.7.1',  '82.0.1', '82', '82.7', '96', '108', '52', '47.0', '91', '110', '102', '108','65', '67'])]

    
    return df_casa

# Función para extraer el valor numérico de una cadena
def extraer_valor_numerico(cadena):
    valor_numerico_str = ''.join(caracter for caracter in cadena if caracter.isdigit() or caracter == '.')
    #print(valor_numerico_str)

    if valor_numerico_str == '':
        return 0.0
    return float(valor_numerico_str)

def sintetizar_en_intervalos(df_casa):

    df_consumo = df_casa.copy()

    # Crea un rango de 5 en 5 minutos
    rango_5_minutos = pd.date_range(start=df_casa.index.min(), end=df_casa.index.max(), freq='5T')

    nuevo_df = pd.DataFrame()

    df_nuevo = pd.DataFrame()

    
    df_consumo = df_consumo[df_consumo['marquee-text'].str.contains('W', na=False)]
    df_consumo = df_consumo[~df_consumo['marquee-text'].str.contains('kWh', na=False)]
    df_consumo['marquee-text'] = df_consumo['marquee-text'].apply(lambda x: extraer_valor_numerico(x))

    df_casa = df_casa[~df_casa['marquee-text'].str.contains('W', na=False)]
    df_casa = df_casa[~df_casa['marquee-text'].str.contains('kWh', na=False)]


    #print(df_consumo)
    lista_diccionarios = []
    lista_diccionarios_consumo = []

    # Recorre el rango de 5 en 5 minutos
    for start_time in rango_5_minutos:
    
        diccionario = {}
        diccionario_consumo = {}


        diccionario['event-time'] = start_time
        diccionario_consumo['event-time'] = start_time


        end_time = start_time + pd.Timedelta(minutes=5)
        
        # Filtra los timestamps que están dentro del intervalo de 5 minutos
        eventos_en_intervalo = df_casa.loc[(df_casa.index >= start_time) & (df_casa.index < end_time)]
        eventos_en_intervalo_consumo = df_consumo.loc[(df_consumo.index >= start_time) & (df_consumo.index < end_time)]

        lista_eventos = eventos_en_intervalo['event-id'].unique()
        lista_eventos_consumo = eventos_en_intervalo_consumo['event-id'].unique()

        sensor_activo = {}

        if len(lista_eventos)!=0:

            grupos_por_id = eventos_en_intervalo_consumo.groupby('event-id')
            
            # Contar las veces que se produce el evento para cada evento            
            for evento in lista_eventos:
                    
                if pd.notna(evento) and 'Consumo' not in str(evento):
                   
                    if evento in lista_eventos:
                        
                        diccionario[evento] = eventos_en_intervalo['event-id'].value_counts()[evento]

                        lista_diccionarios.append(diccionario)
            

            for evento in lista_eventos_consumo:

                if pd.notna(evento) and 'Consumo' in str(evento):
                   
                    diccionario_consumo[evento] = grupos_por_id.get_group(evento)['marquee-text'].sum()

                    #Hasta aqui los float se conservan
                    #print(diccionario[str(evento)])
                    lista_diccionarios_consumo.append(diccionario_consumo)
                    
    print(lista_diccionarios)

    df_nuevo = pd.DataFrame(lista_diccionarios)
    df_nuevo_consumo = pd.DataFrame(lista_diccionarios_consumo)

    df_nuevo_consumo['ConsumoTotal'] = df_nuevo_consumo.filter(like='Consumo').fillna(0).astype(float).sum(axis=1)

    #df_nuevo = df_casa.nunique()[0]

    df_nuevo_consumo.to_excel('dataset_consumo_'+casa+'.xlsx')


    return df_nuevo




if __name__ == '__main__':

    casa = 'YH-00052886.xlsx'

    df_casa = pd.read_excel(casa)

    df_casa = anadir_fecha_dataset(df_casa)

    df_casa = renombrar_columna_eventos(df_casa)

    df_casa = eliminar_registros(df_casa)

    df_casa['fecha'] = pd.to_datetime(df_casa['event-time'])
    df_casa = df_casa.set_index('fecha')


    df_consumo =  sintetizar_en_intervalos(df_casa)

    df_casa.to_excel('./dataset_final/'+casa)
    
    df_consumo.to_excel('./consumos_final/'+casa)


