import os
import pandas as pd

## Definimos el mes con nombre
mes = "Junio"
## Definimos el mes con número
m = "06"
## Definimos el año
y = "2024"

## La ruta de trabaajo es la ruta donde se leen y se generan los archivos
ruta_trabajo = f"Transacciones/{y}/{m} {mes}"

## Archivo a subir 
file_to_upload = 'Full_Junio.csv'

## metodo para asignar la ruta al archivo
archivo = os.path.join(ruta_trabajo, file_to_upload)
df = pd.read_csv(archivo, low_memory=False, encoding='latin-1')

# df_duplicados = df[df[['NUMERO_SERIE_HEX','FECHA_HORA_TRANSACCION','SALDO_ANTES_TRANSACCION','MONTO_TRANSACCION']].duplicated()]
df_duplicados = df[df.duplicated(subset=['NUMERO_SERIE_HEX','FECHA_HORA_TRANSACCION','SALDO_ANTES_TRANSACCION','MONTO_TRANSACCION'])]
print(df_duplicados)
print(len(df_duplicados))

dates_dup = df_duplicados['FECHA_HORA_TRANSACCION'].to_list()
cards_dup = df_duplicados['NUMERO_SERIE_HEX'].to_list()

# print(dates_dup)
# print(cards_dup)
duplicados = []
for card,date in zip(cards_dup,dates_dup):
  # tr_va = df[df['NUMERO_SERIE_HEX'] == card]
  # print(tr_va[['ID_TRANSACCION_ORGANISMO','NUMERO_SERIE_HEX','FECHA_HORA_TRANSACCION','LOCATION_ID','TIPO_TRANSACCION','SALDO_ANTES_TRANSACCION','MONTO_TRANSACCION']])
  dup = df[df['FECHA_HORA_TRANSACCION'] == date]
  cdup = dup[dup['NUMERO_SERIE_HEX'] == card]
  # print(cdup[['ID_TRANSACCION_ORGANISMO','NUMERO_SERIE_HEX','FECHA_HORA_TRANSACCION','LOCATION_ID','TIPO_TRANSACCION','SALDO_ANTES_TRANSACCION','MONTO_TRANSACCION']])
  duplicados.append(cdup)
df_fnal = pd.concat(duplicados)
df_fnal = df_fnal[['ID_TRANSACCION_ORGANISMO','NUMERO_SERIE_HEX','FECHA_HORA_TRANSACCION','LOCATION_ID','TIPO_TRANSACCION','SALDO_ANTES_TRANSACCION','MONTO_TRANSACCION']]
print("Generando archivo de  Duplicados")
archivo_full = f"Duplicados_{mes}.csv"
ruta_full = os.path.join(ruta_trabajo, archivo_full)
df_fnal.to_csv(ruta_full, index=False)
print(df_fnal)
  
print("Generando archivo de  Duplicados")
archivo_full = f"Duplicados_short_{mes}.csv"
ruta_full = os.path.join(ruta_trabajo, archivo_full)
df_fnal.to_csv(ruta_full, index=False)
print(df_fnal)
  
  
  
  
  
  
  
# print(df[df[['FECHA_HORA_TRANSACCION']].duplicated()])
# # df_dup = df[df[['NUMERO_SERIE_HEX','FECHA_HORA_TRANSACCION','LOCATION_ID','TIPO_TRANSACCION','SALDO_ANTES_TRANSACCION','MONTO_TRANSACCION']].duplicated()]
# df_dup = df[df[['FECHA_HORA_TRANSACCION']].duplicated()]
# df_dup_short = df_duplicados[['ID_TRANSACCION_ORGANISMO','NUMERO_SERIE_HEX','FECHA_HORA_TRANSACCION','LOCATION_ID','TIPO_TRANSACCION','SALDO_ANTES_TRANSACCION','MONTO_TRANSACCION']]
# card = '000000007C899CC8'
# df_hex = df[df['NUMERO_SERIE_HEX'] == card] 
# print(df_dup_short)
# print(df_hex[['ID_TRANSACCION_ORGANISMO','NUMERO_SERIE_HEX','FECHA_HORA_TRANSACCION','LOCATION_ID','TIPO_TRANSACCION','SALDO_ANTES_TRANSACCION','MONTO_TRANSACCION']])