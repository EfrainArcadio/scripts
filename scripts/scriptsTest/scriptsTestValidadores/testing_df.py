import os
import pandas as pd

## Definimos el mes con nombre
mes = "Julio"
## Definimos el mes con número
m = "07"
## Definimos el año
y = "2024"

name_file = '1ra_qna_Julio_2024'
## La ruta de trabaajo es la ruta donde se leen y se generan los archivos
ruta_trabajo = f"data/{y}/{m} {mes}"

## Es el periodo en el que se realiza el analisis
periodo = "1ra_qna"

## Archivo a subir 
file_to_upload = f'{name_file}.csv'

## metodo para asignar la ruta al archivo
archivo = os.path.join(ruta_trabajo, file_to_upload)
df = pd.read_csv(archivo, low_memory=False, encoding='latin-1')

print(df['FECHA_HORA_TRANSACCION'].value_counts())