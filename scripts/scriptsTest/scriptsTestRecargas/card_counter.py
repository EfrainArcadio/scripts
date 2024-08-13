import pandas as pd
import os
## Define: mes ="Febrero" --- Nombre del mes
mes = "Junio"
## Define: m ="02"  ---- Numero del mes
m = "06"
## Define: y ="2024" --- Año a tomar en cuenta en el analisis
y = "2024"
## Rango de dias semanales L - V Agregando + 1 al viernes ** Leer documentacion metodo range

## ------- Definir datos de Entrada --------
ruta_trabajo = f"data/{y}/{m} {mes}/"
archivo = f'Full_{mes}.csv'
# archivo = f'Full_{mes}.csv'
archivo = os.path.join(ruta_trabajo, archivo)
# Lectura del archivo de Entrada
df = pd.read_csv(archivo, low_memory=False, encoding='latin-1')

df_cards_app = df[df['LOCATION_ID'] == '101801']
df_cards_rdd = df[df['LOCATION_ID'] == '101800']
df_cards_rrf = df[df['LOCATION_ID'] == '201A00']

print('Tarjetas unicas AppCDMX:',len(set(df_cards_app['NUMERO_SERIE_HEX'])))
print('Tarjetas unicas RRD:',len(set(df_cards_rdd['NUMERO_SERIE_HEX'])))
print('Tarjetas unicas RRF:',len(set(df_cards_rrf['NUMERO_SERIE_HEX'])))
print('Tarjetas unicas Totales:',len(set(df['NUMERO_SERIE_HEX'])))  