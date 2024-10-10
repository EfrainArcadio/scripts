import os
import pandas as pd
import glob

## Directorio donde se encuentran los archivos CSV y la cual sera la base de trabajo del Script
y = '2024'
mes= 'Septiembre'
m = '09'
##
c = '2da'
# c = '1ra'
##
periodo = f'{c}_qna'
##
archivo_f = f'{periodo}_{mes}_{y}.csv'
##
ruta_guardado = f'data/{y}/{m} {mes}/{c}'
##
archivos = glob.glob(os.path.join(ruta_guardado, '*.csv'))
##
df = pd.concat((pd.read_csv(archivo, dtype={'LOCATION_ID': str}, encoding='latin-1') for archivo in archivos), ignore_index=True)
df['LOCATION_ID'] = df['LOCATION_ID'].astype(str).str.zfill(width=1)

ruta_out = os.path.join(ruta_guardado,archivo_f)
df.to_csv(ruta_out, index=False)