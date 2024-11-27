import pandas as pd
import os
import glob
### Variables 
## fechas
y= "2024"
mes = "Octubre"
m = "10"
## Carpetas
r1 = 'respaldos'
r2 = 'Python Scripts'
r3 = 'Validadores'
r4 = 'data'

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

### folder dump
folder = 'repositorioCorredores'
## carpetas de archivos
qna_1 = f'{r1}/{r2}/{y}/{m} {mes}/1ra/gps'
qna_2 = f'{r1}/{r2}/{y}/{m} {mes}/2da/gps'
## archivo salida
archivo_f = f'val_cord_{m}_{mes}_{y}.csv'
### Rutas Produccion
## carpetas de archivos
qna_1 = f'{r1}/{r2}/{r3}/{r4}/{y}/{m} {mes}/1ra/gps'
qna_2 = f'{r1}/{r2}/{r3}/{r4}/{y}/{m} {mes}/2da/gps'
## Paths
current_dir = os.getcwd()
# print('D:\ORT\scripts',current_dir)
parent_dir = os.path.dirname(current_dir)
# print('D:\ORT',parent_dir)
## rutas de origen
ruta_qna_1 = os.path.join( parent_dir,qna_1)
ruta_qna_2 = os.path.join( parent_dir,qna_2)
## Rutas Dumps
ruta_dumps = os.path.join( current_dir,f'public/validaciones/{folder}/{y}')

## verifys
path_verify(ruta_dumps)
path_verify(ruta_qna_1)
path_verify(ruta_qna_2)

##

print('Buscando informacion...')
if ruta_qna_1 and ruta_qna_2:
  print('Informacion Encontrada ...')
  print('Procesando informacion ...')
  ## Mapings cols
  mapping = {
    'Id Transaccion Organismo': 'ID_TRANSACCION_ORGANISMO', 
    'ï»¿ID_TRANSACCION_ORGANISMO': 'ID_TRANSACCION_ORGANISMO', 
    'Numero Serie Hex': 'NUMERO_SERIE_HEX',
    'Fecha Hora Transaccion': 'FECHA_HORA_TRANSACCION',
    'Linea': 'LINEA',
    'Autobus_Hex': 'AUTOBUS',
    'Autobus_Dec': 'AUTOBUS',  # Si quieres eliminar esta columna, no la incluyas en el diccionario
    'Location Id': 'LOCATION_ID',
    'Tipo Transaccion': 'TIPO_TRANSACCION',
    'Monto Transaccion': 'MONTO_TRANSACCION',
    'Latitud': 'LATITUD',
    'Longitud': 'LONGITUD'
  }
  print(f'Trabajando con el  mes de {mes}')
  ## Carga de archivos
  archivos_1 = glob.glob(os.path.join(ruta_qna_1, '*.csv'))
  archivos_2 = glob.glob(os.path.join(ruta_qna_2, '*.csv'))
  ## concertir las columnas con el nombre correcto.
  df_col_rename_1 = []
  for archivo1,archivo2 in zip(archivos_1,archivos_2):
    df1 = pd.read_csv(archivo1,encoding='latin-1',low_memory=False).rename(columns=mapping)
    # print(f"1ra Quincena {len(df1)} datos")
    df1['TIPO_TRANSACCION'] = df1['TIPO_TRANSACCION'].astype(str)
    df1_valid = df1[df1['TIPO_TRANSACCION'] == '3']
    # print(f"1ra Quincena {df1.columns} datos")
    df2 = pd.read_csv(archivo2,encoding='latin-1',low_memory=False).rename(columns=mapping)

    df2['TIPO_TRANSACCION'] = df2['TIPO_TRANSACCION'].astype(str)
    df2_valid = df2[df2['TIPO_TRANSACCION'] == '3']
    
    # print(f"2da Quincena {df2.columns} datos")
    df_short1 = df1_valid[['ID_TRANSACCION_ORGANISMO','FECHA_HORA_TRANSACCION','LONGITUD','LATITUD']]
    df_short2 = df2_valid[['ID_TRANSACCION_ORGANISMO','FECHA_HORA_TRANSACCION','LONGITUD','LATITUD']]
    df_col_rename_1.append(df_short1)
    df_col_rename_1.append(df_short2)
  
df_final = pd.concat(df_col_rename_1)
print(f"Archivo Final: {len(df_final)} datos")
df_final = df_final[(df_final['LATITUD'] != '0' ) & (df_final['LONGITUD'] != '0' )]
df_final = df_final[(df_final['LATITUD'] != 0 ) & (df_final['LONGITUD'] != 0 )]
df_final = df_final[(df_final['LATITUD'] != 0.0 ) & (df_final['LONGITUD'] != 0.0 )]
print(f"Archivo Final: {len(df_final)} datos")
ruta_out = os.path.join(ruta_dumps,archivo_f)
df_final.to_csv(ruta_out, index=False)
print("Proceso Realizado con Exito..")












