import pandas as pd
import os
#### Variables
## fechas
y = '2024'
m = '01'
mes = 'Enero'
## periodo
qna = '1ra'
# qna = '2da'
## archivos
name_file_v1 = 'Validaciones de la 1ra qna de enero 2024'
name_file_v2 = '1ra_qna_Enero_2024_v2'
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
## Crear listas de los ID_TRANSACCION_ORGANISMO 
id_tr1 = df_file_v1['ID_TRANSACCION_ORGANISMO'].unique().tolist()
id_tr2 = df_file_v2['ID_TRANSACCION_ORGANISMO'].unique().tolist()
## Buscar las nuevas transacciones

l1 = len(id_tr1)
print('Transacciones Primer Archivo',len(id_tr1))
## Buscar longitud de la segunda lista
l2 = len(id_tr2)
print('Transacciones Segundo Archivo',len(id_tr2))
## Diferencia entre listas
print('Diferencia = # datos esperados',l2 - l1)
##
print('Buscado Nuevas Transacciones ...')
list_ext = ( e for e in id_tr2 if e not in id_tr1 )
list_no_2 = ( e for e in id_tr1 if e not in id_tr2 )
## Nuevos Resultados  
print('Nuevos Resultados')
##
df_extemp = tr_verify(df_file_v2,list_ext)
# print(df_extemp)
## datos file extemp
file_ext = f'extemp_{qna}_qna_{mes}.csv'
ruta_ext = os.path.join(path_dumps,file_ext)
df_extemp.to_csv(ruta_ext,index=False)
print(df_extemp['LINEA'].value_counts())
print('Generando archivo extemporaneos')
## condicional para faltantes
if len(list(list_no_2)) > 0:
  df_faltante = tr_verify(df_file_v1,list_no_2)
  print(len(df_faltante))
  file_falt = f'faltante_{qna}_qna_{mes}.csv'
  ruta_ext = os.path.join(path_dumps,file_falt)
  df_faltante.to_csv(ruta_ext,index=False)
elif len(list(list_no_2) < 0):
  print('No existen faltantes se revizo el archivo completo')
