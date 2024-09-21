import pandas as pd
import os
import glob
### Variables 
## fechas
y= "2024"
mes = "Agosto"
m = "08"
## Carpetas
r1 = 'oldScripts'
r2 = 'Validadores'
r3 = ''
r4 = ''

### Funciones
def path_verify(path):
  if not os.path.exists(path):
    os.makedirs(path)
    # print(f'No existe el Directorio : {path}')
    print(f'Directorio Creado: {path}')
  else:
    print(f'El Directorio si existe: {path}')
    # print(f'El Directorio ya existe: {path}')
## end path_verify

### Rutas test
folder = 'repositorioCorredores_Ene-Ago-2024'
## Nombre corto del archivo sin extencion .csv
# name_file = 'transactions_with_geolocalisation_1726864415'
### DataInfo
pathStringInfo = f'data/{y}/{m} {mes}' ###################### test line
## carpetas de archivos
qna_1 = f'{r1}/{r2}/{y}/{m} {mes}/1ra/gps'
qna_2 = f'{r1}/{r2}/{y}/{m} {mes}/2da/gps'
## archivo salida
archivo_f = f'val_cord_{m}_{mes}_{y}.csv'

### Rutas Produccion
## carpetas de archivos
# qna_1 = f'{r1}/{r2}/{r3}/{r4}/{y}/{m} {mes}/1ra/gps'
# qna_2 = f'{r1}/{r2}/{r3}/{r4}/{y}/{m} {mes}/2da/gps'
## 
current_dir = os.getcwd()
print(current_dir)
parent_dir = os.path.dirname(current_dir)
print(parent_dir)
parent_dir_2 = os.path.dirname(parent_dir)
print(parent_dir_2)
parent_dir_3 = os.path.dirname(parent_dir_2)
print(parent_dir_3)
parent_dir_4 = os.path.dirname(parent_dir_3)
print(parent_dir_4)
pathInfo = os.path.join(parent_dir,pathStringInfo)
print(pathInfo)
## Rutas Dumps
ruta_qna_1 = os.path.join( parent_dir_4,qna_1)
ruta_qna_2 = os.path.join( parent_dir_4,qna_2)

ruta_dumps = os.path.join( pathInfo,f'public/validaciones/{y}/{folder}')

## verifys
path_verify(ruta_dumps)
path_verify(ruta_qna_1)
path_verify(ruta_qna_2)

##

if ruta_qna_1 and ruta_qna_2:
  print('confirmando existencia 1:')
  archivos_1 = glob.glob(os.path.join(ruta_qna_1, '*.csv'))
  df_1 = pd.concat((pd.read_csv(archivo, dtype={'LOCATION_ID': str}, encoding='latin-1') for archivo in archivos_1), ignore_index=True)
  df_1['LOCATION_ID'] = df_1['LOCATION_ID'].astype(str).str.zfill(width=1)
  print('confirmando existencia 2:')
  archivos_2 = glob.glob(os.path.join(ruta_qna_2, '*.csv'))
  df_2 = pd.concat((pd.read_csv(archivo, dtype={'LOCATION_ID': str}, encoding='latin-1') for archivo in archivos_2), ignore_index=True)
  df_2['LOCATION_ID'] = df_2['LOCATION_ID'].astype(str).str.zfill(width=1)

  print(len(df_1))
  print(len(df_2))
  df_final = pd.concat([df_1, df_2])
  print(len(df_final))
  df_short = df_final[['ID_TRANSACCION_ORGANISMO','FECHA_HORA_TRANSACCION','LONGITUD','LATITUD']]# print(df_short.columns)
  ruta_out = os.path.join(ruta_dumps,archivo_f)
  df_short.to_csv(ruta_out, index=False)










## end path_verify

## nombre dinamico del archivo utilizado para subir informacion a la base de datos con ext .csv
# file_to_upload = f'{name_file}.csv'
## Lectura del archivo y transformacion a DataFrame
# archivo = os.path.join(pathStringInfo, file_to_upload)  ############ test line
# archivo = os.path.join(pathInfo, file_to_upload) 
# df = pd.read_csv(archivo, low_memory=False, encoding='latin-1')

# print(df.columns)


