import os
import pandas as pd

## Definimos el mes con nombre
mes = "octubre"
## Definimos el mes con número
m = "10"
## Definimos el año
y = "2024"

name_file = 'Indios Verdes 18oct24'
name_file_ou = 'Indios Verdes 18oct24_ou'
## La ruta de trabaajo es la ruta donde se leen y se generan los archivos

ruta_trabajo = f"scripts/scriptsTest/scriptsTestValidadores/data/{y}/{m} {mes}"

ruta_actual = os.getcwd()

parent_dir = os.path.dirname(ruta_actual)

path_files = os.path.join(ruta_actual,ruta_trabajo)
## Es el periodo en el que se realiza el analisis
periodo = "1ra_qna"

## Archivo a subir 
file_to_upload = f'{name_file}.csv'
file_to_dump = f'{name_file_ou}.csv'

## metodo para asignar la ruta al archivo
archivo = os.path.join(path_files, file_to_upload)
archivo_ou = os.path.join(path_files, file_to_dump)
df = pd.read_csv(archivo, low_memory=False, encoding='latin-1')


print(df['FECHA_HORA_TRANSACCION'].value_counts())
df['FECHA_HORA_TRANSACCION'] = df["FECHA_HORA_TRANSACCION"].astype('datetime64[ns]')
print(df['FECHA_HORA_TRANSACCION'].value_counts())

df.to_csv(archivo_ou,index=False)