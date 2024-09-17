import os
import pandas as pd
##### Declaracion de nombres de los Archivos a trabajar
## Nombre del archivo que se subio a SEMOVI
nf1 = 'ORT_Validaciones_1ra_qna_junio_2024_Microsafe'
## Nombre del archivo actualizado
nf2 = 'transactions_2024_micro_jun_1ra_qna'
## Año 
y = '2024'
## Mes declarado en sus dos formatos
mes = 'Junio'
m = '06'
## 
qna = '1ra'
# qna = '2da'
## Ruta de Trabajo
path_work = f'data/{y}/{m} {mes}'
## Crear Primer DataFrame con el Primer Archivo
name_file_1 = f'{nf1}.csv'
file_1 = os.path.join(path_work,name_file_1)
df_f1 = pd.read_csv(file_1,low_memory=False, encoding='latin-1')
## Crear Primer DataFrame con el Segundo Archivo
name_file_2 = f'{nf2}.csv'
file_2 = os.path.join(path_work,name_file_2)
df_f2 = pd.read_csv(file_2,low_memory=False, encoding='latin-1')
## Crear Lista de los ID_TRANSACCION_ORGANISMO de cada DataFrame
id_tr1 = df_f1['ID_TRANSACCION_ORGANISMO'].to_list()
id_tr2 = df_f2['ID_TRANSACCION_ORGANISMO'].to_list()
## Buscar las nuevas transacciones
print('Buscado Nuevas Transacciones ...')
list_ext = [ e for e in id_tr2 if e not in id_tr1]
## Buscar longitud de la primer lista
l1 = len(id_tr1)
print('Transacciones Primer Archivo',l1)
## Buscar longitud de la segunda lista
l2 = len(id_tr2)
print('Transacciones Segundo Archivo',l2)
## Diferencia entre listas
print('Diferencia',l1 - l2)
## Nuevos Resultados
print('Nuevos Resultados')
print(len(list_ext))

extemp = []
for tr in list_ext:
  df_new = df_f2[df_f2['ID_TRANSACCION_ORGANISMO'] == tr]
  extemp.append(df_new)
df_extemp = pd.concat(extemp)
## 

# print(df_extemp)