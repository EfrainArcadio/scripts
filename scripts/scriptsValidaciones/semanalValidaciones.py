import pandas as pd
import os
#### files
y = '2024'
m = '03'
mes = 'Marzo'
name_file_v1 = 'ort_2da qna Julio'

######### paths #####
path = os.path.dirname(__file__)
path_scripts = os.path.dirname(path)
path_scripts_root = os.path.dirname(path_scripts)
path_root = os.path.dirname(path_scripts_root)

####### --- ChekLine
path_files = os.path.join(path_root,f'dataFiles/validaciones/{y}/{m} {mes}')
file_v1 = os.path.join(path_files,f'{name_file_v1}.csv')
df_file_v1 = pd.read_csv(file_v1,low_memory=False, encoding='latin-1')
print(df_file_v1['LINEA'].value_counts())


# ###### --- chekTypes
# path_file = os.path.join(path_scripts_root,f'public/validaciones/quincenas/{y}/{m} {mes}')
# file_v1 = os.path.join(path_file,f'{name_file_v1}.csv')
# df_file_v1 = pd.read_csv(file_v1,low_memory=False, encoding='latin-1')

# print(df_file_v1.columns)
# print(df_file_v1['ID_TRANSACCION_ORGANISMO'])
# print(df_file_v1['SALDO_ANTES_TRANSACCION'])
# print(df_file_v1['MONTO_TRANSACCION']) 
# print(df_file_v1['SALDO_DESPUES_TRANSACCION'])
# print(df_file_v1['CONTADOR_VALIDACIONES'])
# print(df_file_v1['CONTRACT_TARIFF'])
# print(df_file_v1['CONTRACT_VALIDITY_DURATION'])