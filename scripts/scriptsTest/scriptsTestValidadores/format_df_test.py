import os
import pandas as pd
import glob
import json
## Directorio donde se encuentran los archivos CSV y la cual sera la base de trabajo del Script
y = '2024'
mes= 'Octubre'
m = '10'
##  
# c = '2da_v1'
c = '1ra_cotaxo'
##
## File Names
periodo = f'{c}_qna'
json_ed = "modeloDatos.json"
# archivo_f = f'{periodo}_{mes}_{y}_v2.csv'
##
archivo_f = f'{periodo}_qna_aulsa_{mes}.csv'
## paths
##
ruta_actual = os.getcwd()
parent_dir = os.path.dirname(ruta_actual)
##
ruta_test_data = f'/data'
ruta_datainfo = f'scripts/scriptsTest/scriptsTestValidadores/data/{y}/{m} {mes}/{c}'
# ruta_datainfo = f'respaldos/Python Scripts/Validadores/data/{y}/{m} {mes}/{c}'
ruta_datadump = f'dataFiles/validaciones/{y}/{m} {mes}'

ruta_test_json = os.path.join(ruta_actual,'data')
ruta_info = os.path.join(ruta_actual,ruta_datainfo)
print(ruta_info)
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
###

# df['LOCATION_ID'] = df['LOCATION_ID'].astype(str).str.zfill(width=1)
# df['AUTOBUS'] = df['AUTOBUS'].astype(str)
# df['RUTA'] = df['RUTA'].astype(str)
# df['EQUIPO'] = df['EQUIPO'].astype(str)
# df['SALDO_ANTES_TRANSACCION'].fillna(0, inplace=True)
# df['SALDO_ANTES_TRANSACCION'] = df['SALDO_ANTES_TRANSACCION'].astype('int64')
# df['MONTO_TRANSACCION'].fillna(0, inplace=True)
# df['MONTO_TRANSACCION'] = df['MONTO_TRANSACCION'].astype('int64')
# df['SALDO_DESPUES_TRANSACCION'].fillna(0, inplace=True)
# df['SALDO_DESPUES_TRANSACCION'] = df['SALDO_DESPUES_TRANSACCION'].astype('int64')
# df['CONTADOR_RECARGAS'].fillna(0, inplace=True)
# df['CONTADOR_RECARGAS'] = df['CONTADOR_RECARGAS'].astype('int64')
# df['CONTADOR_VALIDACIONES'].fillna(0, inplace=True)
# df['CONTADOR_VALIDACIONES'] = df['CONTADOR_VALIDACIONES'].astype('int64')
# df['CONTRACT_VALIDITY_DURATION'].fillna(0, inplace=True)
# df['CONTRACT_VALIDITY_DURATION'] = df['CONTRACT_VALIDITY_DURATION'].astype('int64')
###

df = df[~df['FECHA_HORA_TRANSACCION'].between('2024-09-30', '2024-09-30 23:59:59')]
fechas_unicas = sorted(set(df['FECHA_HORA_TRANSACCION']))
# print(fechas_unicas)
print(df.info())
df.to_csv(path_dump_file, index=False)
print('Proceso finalizado con Exito')