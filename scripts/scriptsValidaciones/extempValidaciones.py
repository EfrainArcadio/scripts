import pandas as pd
import os
#### files
y = '2024'
m = '05'
mes = 'Mayo'
name_file_v1 = 'Validaciones_2da qna Mayo'
name_file_v2 = '2da_qna_Mayo_2024_v2'
# qna = '1ra'
qna = '2da'
######### paths #####
path = os.path.dirname(__file__)
path_scripts = os.path.dirname(path)
path_scripts_root = os.path.dirname(path_scripts)
path_root = os.path.dirname(path_scripts_root)
def path_verify(path):
  if not os.path.exists(path):
    os.makedirs(path)
    print(f'Directorio Creado: {path}')
  else:
    print(f'El Directorio ya existe: {path}')
###### paths comps
path_files = os.path.join(path_root,f'dataFiles/validaciones/{y}/{m} {mes}')
path_dumps = os.path.join(path_scripts_root,f'public/validaciones/quincenas/{y}/{m} {mes}')
path_verify(path_dumps)
###### file compose
file_v1 = os.path.join(path_files,f'{name_file_v1}.csv')
file_v2 = os.path.join(path_files,f'{name_file_v2}.csv')
#### Read Files
df_file_v1 = pd.read_csv(file_v1,low_memory=False, encoding='latin-1')
df_file_v2 = pd.read_csv(file_v2,low_memory=False, encoding='latin-1')

# df_file_v1.fillna({'ID_TRANSACCION_ORGANISMO':0},inplace=True)
# print(df_file_v2.columns)
#######################
print(df_file_v1['LINEA'].value_counts()) 
# print(df_file_v1['RUTA'].value_counts()) 
# print(df_file_v2['RUTA'].value_counts()) 
print(df_file_v2['LINEA'].value_counts())
####
id_tr1 = df_file_v1['ID_TRANSACCION_ORGANISMO'].to_list()
id_tr2 = df_file_v2['ID_TRANSACCION_ORGANISMO'].to_list()
## Buscar las nuevas transacciones
l1 = len(id_tr1)
print('Transacciones Primer Archivo',len(id_tr1))
## Buscar longitud de la segunda lista
l2 = len(id_tr2)
print('Transacciones Segundo Archivo',len(id_tr2))
## Diferencia entre listas
print('Diferencia',l2 - l1)

print('Buscado Nuevas Transacciones ...')
list_ext = [ e for e in id_tr2 if e not in id_tr1]

## Nuevos Resultados
print('Nuevos Resultados')
print(len(list_ext))
extemp = []
for tr in list_ext:
  df_new = df_file_v2[df_file_v2['ID_TRANSACCION_ORGANISMO'] == tr]
  extemp.append(df_new)
df_extemp = pd.concat(extemp)
print(df_extemp['LINEA'].value_counts())

file_ext = f'extemp_{qna}_qna_{mes}.csv'
ruta_ext = os.path.join(path_dumps,file_ext)
df_extemp.to_csv(ruta_ext,index=False)

# print(path_scripts)
# print(path_scripts_root)
# print(path_root)