import pandas as pd
import os
import glob
##
y= "2021"
mes = "Noviembre"
m = "11"
# c = '2da quincena dic 2021'
name_file = '1ra_qna_noviembre'
##
tablaExtName = f"datos_val_{y}"
a = "-Transacciones.csv"
ae = "-Transacciones-extension.csv"
### DataInfo
pathStringInfo = f'dataFiles/validaciones/{y}/{m} {mes}/'
current_dir = os.getcwd()
parent_dir = os.path.dirname(current_dir)
pathInfo = os.path.join(parent_dir,pathStringInfo)
##
file_to_upload = f'{name_file}.csv'

## metodo para asignar la ruta al archivo
# archivo = os.path.join(pathInfo, file_to_upload) 
# df = pd.read_csv(archivo, low_memory=False, encoding='latin-1')

# print(pathInfo)
archivos = glob.glob(os.path.join(pathInfo, '*.csv'))
df = pd.concat((pd.read_csv(archivo, dtype={'LOCATION_ID': str}, encoding='latin-1') for archivo in archivos), ignore_index=True)
df['LOCATION_ID'] = df['LOCATION_ID'].astype(str).str.zfill(width=1)


df_bus = df[df['TIPO_TRANSACCION'] == '3'].copy() 

mto_bus = sum(df['MONTO_TRANSACCION']) / 100
df_bus['FECHA_HORA_TRANSACCION'] = pd.to_datetime(df_bus['FECHA_HORA_TRANSACCION'], format="mixed")
df_bus['FECHA_HORA_TRANSACCION'] = df_bus['FECHA_HORA_TRANSACCION'].dt.strftime('%Y/%m/%d')
# df_bus['FECHA_HORA_TRANSACCION'] = df_bus['FECHA_HORA_TRANSACCION'].dt.strftime('%Y-%m-%d')

fechas = sorted(set(df_bus['FECHA_HORA_TRANSACCION']))
# print(fechas  )
# print(len(df_bus))
# print(mto_bus)

for fecha in fechas:
  print(fecha)
  df_dia = df_bus[df_bus['FECHA_HORA_TRANSACCION'] == fecha]
  print(len(df_dia))
  print('$',sum(df_dia['MONTO_TRANSACCION']) / 100)