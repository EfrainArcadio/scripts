import os
import pandas as pd
import json
## 
y = '2024'
mes= 'Febrero'
m = '02'
##
# qna = '1ra'
qna = '2da'
##
file = f'extemp_{qna}_qna_{mes}'
json_ed = "modeloDatos.json"
current_dir = os.getcwd()
# parent_dir = os.path.dirname(current_dir)
path_files = os.path.join(f'public/validaciones/quincenas/{y}/{m} {mes}')
##
archivo_f = f'{file}.csv'
##
path_file = os.path.join(path_files,archivo_f)
##
df = pd.read_csv(path_file)
##
ruta_test_json = os.path.join(current_dir,'data')
path_json_ed = os.path.join(ruta_test_json,json_ed)
with open(path_json_ed) as f:
    data_ed = json.load(f)
dtypes = {col['column']: col['type'] for col in data_ed}
##

df = df.dropna(subset=['TIPO_TRANSACCION','SALDO_ANTES_TRANSACCION','MONTO_TRANSACCION','SALDO_DESPUES_TRANSACCION'])
df = df[df['LINEA'] != 'AA']
# df[[
#   'SALDO_ANTES_TRANSACCION',
#   'MONTO_TRANSACCION',
#   'SALDO_DESPUES_TRANSACCION',
#   'CONTADOR_RECARGAS',
#   'CONTADOR_VALIDACIONES',
#   'CONTRACT_TARIFF',
#   'CONTRACT_VALIDITY_DURATION',
#   'CONTRACT_RESTRICT_TIME',
#   'COUNTER_VALUE',
#   'COUNTER_AMOUNT',
#   'SAM_COUNTER',
#   'MAC',
#   'PURCHASE_LOG',
#   'EQUIPO',
#   'ESTACION'
#   ]].fillna(0, inplace=True)
df['SALDO_ANTES_TRANSACCION'].fillna(0, inplace=True)
df['MONTO_TRANSACCION'].fillna(0, inplace=True)
df['SALDO_DESPUES_TRANSACCION'].fillna(0, inplace=True)
df['CONTADOR_RECARGAS'].fillna(0, inplace=True)
df['CONTADOR_VALIDACIONES'].fillna(0, inplace=True)
df['CONTRACT_TARIFF'].fillna(0, inplace=True)
df['CONTRACT_VALIDITY_DURATION'].fillna(0, inplace=True)
df['CONTRACT_RESTRICT_TIME'].fillna(0, inplace=True)
df['COUNTER_VALUE'].fillna(0, inplace=True)
df['COUNTER_AMOUNT'].fillna(0, inplace=True)
df['SAM_COUNTER'].fillna(0, inplace=True)
df['MAC'].fillna(0, inplace=True)
df['PURCHASE_LOG'].fillna(0, inplace=True)
df['EQUIPO'].fillna(1, inplace=True)
df['ESTACION'].fillna(0, inplace=True)
df['AUTOBUS'].fillna(0, inplace=True)
df['RUTA'].fillna(0, inplace=True)
df['LOAD_LOG'].fillna(0, inplace=True)
df['PERFIL1'].fillna(0, inplace=True)

print(df['RUTA'].value_counts())
df[['EQUIPO', 'PERFIL2', 'PERFIL3', 'COUNTER_VALUE', 'COUNTER_AMOUNT','CONTRACT_RESTRICT_TIME']] = df[['EQUIPO', 'PERFIL2', 'PERFIL3', 'COUNTER_VALUE', 'COUNTER_AMOUNT','CONTRACT_RESTRICT_TIME']].astype('Int64')
df = df.astype(dtypes)
df['SAM_COUNTER'] = df['SAM_COUNTER'].str.rstrip('.0')
df['AUTOBUS'] = df['AUTOBUS'].str.rstrip('.0')
df['RUTA'] = df['RUTA'].str.rstrip('.0')
df['ESTACION'] = df['ESTACION'].str.rstrip('.0')
df['PERFIL1'] = df['PERFIL1'].str.rstrip('.0')
df['LOAD_LOG'] = df['LOAD_LOG'].str.rstrip('.0')

df = df[~df['FECHA_HORA_TRANSACCION'].between('2024-10-01 00:00:00', '2024-10-15 23:59:59')]
df = df[~df['TIPO_TRANSACCION'].between('FFFFFFF5', 'FFFFFFF9')]
# df = df[~df['FECHA_HORA_TRANSACCION'].between('2023-02-28', '2023-02-28 23:59:59')]
# df = df.drop(['LATITUD', 'LONGITUD','Fecha Alta'], axis=1)
# print(df)
print(df.info())

# print(df.columns)
df.to_csv(path_file,index=False)

