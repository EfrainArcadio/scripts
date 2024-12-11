import os
import pandas as pd
import glob
## Directorio donde se encuentran los archivos CSV y la cual sera la base de trabajo del Script
y = '2024'
mes= 'Febrero'
m = '02'
##
file = '2da_qna_Febrero_2024_v2'
current_dir = os.getcwd()
parent_dir = os.path.dirname(current_dir)
path_files = os.path.join(parent_dir,f'dataFiles/validaciones/{y}/{m} {mes}')
##
archivo_f = f'{file}.csv'
##
path_file = os.path.join(path_files,archivo_f)
##
df = pd.read_csv(path_file)
##
print(df['LINEA'].value_counts())


df_copy = df.copy()
df_copy['FECHA_HORA_TRANSACCION'] = pd.to_datetime(df_copy['FECHA_HORA_TRANSACCION'])
df_copy['FECHA_HORA_TRANSACCION'] = df_copy['FECHA_HORA_TRANSACCION'].dt.strftime('%Y-%m-%d')
fechas_unicas = sorted(df['FECHA_HORA_TRANSACCION'].unique())
print(fechas_unicas)
