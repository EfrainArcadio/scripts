import pandas as pd
import os
import numpy as np
#### Variables
## fechas
y = '2023'
m = '12'
mes = 'Diciembre'
## periodo
# qna = '1ra'
qna = '2da'
## archivos
name_file_v1 = 'ORT_Validaciones_2da_qna_diciembre_2023_gral'
name_file_v2 = '2da_qna_Diciembre_2023_v2'
######### Funciones #####     
def path_verify(path):
  if not os.path.exists(path):
    os.makedirs(path)
    print(f'Directorio Creado: {path}')
  else:
    print(f'El Directorio ya existe: {path}')
## end path_verify
def tr_verify(df,lista):
  extemp = []
  for tr in lista:
    df_tr = df[df['ID_TRANSACCION_ORGANISMO'] == tr]
    extemp.append(df_tr)
  df_extemp = pd.concat(extemp)
  return df_extemp
## end tr_verify
#### Root Paths    
## D:/ORT/scripts
current_dir = os.getcwd()
## D:/ORT
parent_dir = os.path.dirname(current_dir)
##### paths comps
path_files = os.path.join(parent_dir,f'dataFiles/validaciones/{y}/{m} {mes}')
path_dumps = os.path.join(current_dir,f'public/validaciones/quincenas/{y}/{m} {mes}')
## Verifys
path_verify(path_dumps)
###### path file full
file_v1 = os.path.join(path_files,f'{name_file_v1}.csv')
file_v2 = os.path.join(path_files,f'{name_file_v2}.csv')
#### Read Files
df_file_v1 = pd.read_csv(file_v1,low_memory=False, encoding='latin-1')
df_file_v2 = pd.read_csv(file_v2,low_memory=False, encoding='latin-1')
########### Procesamiento de datos
# df_file_v1['ID_TRANSACCION_ORGANISMO'] = df_file_v1['ID_TRANSACCION_ORGANISMO'].str.replace('.', '')
# df_file_v2['ID_TRANSACCION_ORGANISMO'] = df_file_v2['ID_TRANSACCION_ORGANISMO'].str.replace('.', '')
df_file_v1['ID_TRANSACCION_ORGANISMO'] = df_file_v1['ID_TRANSACCION_ORGANISMO'].astype('Int64')
df_file_v2['ID_TRANSACCION_ORGANISMO'] = df_file_v2['ID_TRANSACCION_ORGANISMO'].astype('Int64')
df_file_v2 = df_file_v2.dropna(subset=['ID_TRANSACCION_ORGANISMO'])
df_file_v1 = df_file_v1.dropna(subset=['ID_TRANSACCION_ORGANISMO'])
print('Vacios',df_file_v1['ID_TRANSACCION_ORGANISMO'].isnull().sum())
print('Vacios',df_file_v2['ID_TRANSACCION_ORGANISMO'].isnull().sum())
# print(df_file_v1.info())
# print(df_file_v2.info())
## Crear listas de los ID_TRANSACCION_ORGANISMO 
id_tr1 = df_file_v1['ID_TRANSACCION_ORGANISMO'].unique().tolist()
id_tr2 = df_file_v2['ID_TRANSACCION_ORGANISMO'].unique().tolist()
## Buscar las nuevas transacciones
id_tr1 = np.array(id_tr1)
id_tr2 = np.array(id_tr2)
l1 = len(id_tr1)
print('Transacciones Primer Archivo',len(id_tr1))
## Buscar longitud de la segunda lista
l2 = len(id_tr2)
print('Transacciones Segundo Archivo',len(id_tr2))
## Diferencia entre listas
print('Diferencia = # datos esperados',l2 - l1)
##
print('Buscado Nuevas Transacciones ...')
list_ext = id_tr2[~np.isin(id_tr2, id_tr1)].tolist()
# list_ext = ( e for e in id_tr2 if e not in id_tr1 )

# list_no_2 = ( e for e in id_tr1 if e not in id_tr2 )
list_no_2 = id_tr1[~np.isin(id_tr1, id_tr2)].tolist()
## Nuevos Resultados  
##
df_extemp = tr_verify(df_file_v2,list_ext)
print(f'Nuevos Resultados: {len(df_extemp)}')
## datos file extemp
file_ext = f'extemp_{qna}_qna_{mes}.csv'
print('Generando archivo extemporaneos')
print(f'Generando CSV: {file_ext}')
ruta_ext = os.path.join(path_dumps,file_ext)
df_extemp.to_csv(ruta_ext,index=False)
# print(df_extemp['LINEA'].value_counts())
lin_f = df_extemp['LINEA'].unique().tolist()
df_extemp['TIPO_TRANSACCION'] = df_extemp['TIPO_TRANSACCION'].astype(str)

res_f = []
for linea in lin_f:
  df_lf = df_extemp[df_extemp['LINEA'] == linea]
  df_valids = df_lf[(df_lf['TIPO_TRANSACCION'] == '5') | (df_lf['TIPO_TRANSACCION'] == '3')]
  monto_ext = sum(df_valids['MONTO_TRANSACCION']) / 100
  res_f.append({
    "Linea": linea,
    "Transacciones": len(df_valids),
    "Monto":monto_ext
  })

df_res_f = pd.DataFrame(res_f)
file_rext = f're_extemp_{qna}_qna_{mes}.csv'
ruta_rext = os.path.join(path_dumps,file_rext)
df_res_f.to_csv(ruta_rext,index=False)

print(f'Generando Resumen {file_rext}')

## condicional para faltantes
if any(list_no_2):
  print('Falta informacion por analizar...')
  df_faltante = tr_verify(df_file_v1,list_no_2)
  print('Faltan: ',len(df_faltante))
  file_falt = f'faltante_{qna}_qna_{mes}.csv'
  print(f'Generando CSV: {file_falt}')
  ruta_ext = os.path.join(path_dumps,file_falt)
  df_faltante.to_csv(ruta_ext,index=False)
else:
  print('No hay existe informacion por analizar...')
print('Proceso Realizado con exito')
