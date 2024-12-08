import os
import pandas as pd
import glob
import json
## Directorio donde se encuentran los archivos CSV y la cual sera la base de trabajo del Script
y = '2024'
mes= 'Noviembre'
m = '11'
##  
c = '2da'
# c = '1ra_v2'
##
## File Names
periodo = f'{c}_qna'
json_ed = "modeloDatos.json"
archivo_f = f'{periodo}_{mes}_{y}_v2.csv'
##
# archivo_f = f'{periodo}_{mes}_{y}.csv'
## paths
##
ruta_actual = os.getcwd()
parent_dir = os.path.dirname(ruta_actual)
##
ruta_test_data = f'/data'
ruta_datainfo = f'respaldos/Python Scripts/Validadores/data/{y}/{m} {mes}/{c}'

ruta_datadump = f'dataFiles/validaciones/{y}/{m} {mes}'
# ruta_datainfo = f'dataFiles/validaciones/{y}/semanas/46/Mpeso'

ruta_test_json = os.path.join(ruta_actual,'data')
ruta_info = os.path.join(parent_dir,ruta_datainfo)
dump_file = os.path.join(parent_dir,ruta_datadump)
path_dump_file = os.path.join(dump_file,archivo_f)
path_json_ed = os.path.join(ruta_test_json,json_ed)

##
archivos = glob.glob(os.path.join(ruta_info, '*.csv'))
##
with open(path_json_ed) as f:
    data_ed = json.load(f)
    
dtypes = {col['column']: col['type'] for col in data_ed}
##
dfs = []
for archivo in archivos:
    # print(archivo)
    df = pd.read_csv(archivo,encoding='latin-1',low_memory=False)
    df = df.rename(columns={'ï»¿ID_TRANSACCION_ORGANISMO': 'ID_TRANSACCION_ORGANISMO'})
    dfs.append(df)
df = pd.concat(dfs)
# print(df)
# df = pd.concat((pd.read_csv(archivo, encoding='latin-1',low_memory=False) for archivo in archivos), ignore_index=True)


# df = df.astype(dtypes)
df['LOCATION_ID'] = df['LOCATION_ID'].astype(str).str.zfill(width=1)

df.to_csv(path_dump_file, index=False)
print('Proceso finalizado con Exito')