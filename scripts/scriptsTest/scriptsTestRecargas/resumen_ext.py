##importar librearias necesarias para el funcionamiento del script
import os
import pandas as pd
from openpyxl import Workbook
import matplotlib.pyplot as plt
import numpy as np
## Nombre del mes con texto, se ocupara para leer la carpeta del mes y asignar el nombre a los archivos generados
## Modificar el contenido de m = "mes" * Para los meses que anteriores a octubre ocupar la sintaxis 09 = Septiembre 08 = Agosto
mes = "Marzo"
m = "03"

## Modificar el contenido de Y = "Año" 2023 / 2024 / 2025 
y = "2022"

## Nombre de las extenciones de los archivos que ocupara el script para realizar 
a = "-Transacciones.csv"


## Ruta de la cual se extraeran todos los archivos y en la misma se guardaran los archivos
ruta_guardado = f"data/{y}/{m} {mes}"

## Este es el rango de dias en el que se trabajara, para el tema del ultimo dia siempre se le sumara 1
## Ejemplo primera quincena dia_fn = 16 el metodo range trabaja de esa forma
dia_in = 1
dia_fn = 32
rango = dia_fn - dia_in

## Listado de los archvios -Transacciones.csv
## Listado de los archivo a leer segun el rango especificado 
archivo_tr = [os.path.join(ruta_guardado, f"{y}{m}{d:02d}{a}") for d in range(dia_in, dia_fn)]

## Areglo que se llenara con los archivos -Transacciones-extension.csv
transacciones = []
## Bucle para insertar todos los archivos en el DataFrame transacciones
for transaccion in archivo_tr:
  df = pd.read_csv(transaccion, low_memory=False)
  transacciones.append(df)

resumen = []

# for df in transacciones:
#   ## Filtrar las transacciones de tipo 0
#   df['TIPO_TRANSACCION'] = df['TIPO_TRANSACCION'].astype('str')
#   df_filtro = df[df['TIPO_TRANSACCION'] == '0'].copy()

#   ## Sacaremos el total de transacciones con el metodo count 
#   tr_totales = df_filtro['TIPO_TRANSACCION'].count()

#   ## Convertir la columna FECHA_HORA_TRANSACCION a datetime
#   df_filtro['FECHA_HORA_TRANSACCION'] = pd.to_datetime(df_filtro['FECHA_HORA_TRANSACCION'])
#   df_filtro['FECHA_HORA_TRANSACCION'] = df_filtro['FECHA_HORA_TRANSACCION'].dt.strftime('%Y-%m-%d')

#   ## Separar las transacciones en base al método
#   df_fisico = df_filtro[df_filtro['LOCATION_ID'] == '201A00']
#   df_digital = df_filtro[df_filtro['LOCATION_ID'] == '101800']
#   df_appcdmx = df_filtro[df_filtro['LOCATION_ID'] == '101801']

#   ## Calcular el monto total por transacción física y agregar al DataFrame correspondiente
#   monto_fisico = df_fisico['MONTO_TRANSACCION'].sum()

#   ## Calcular el monto total por transacción digital y agregar al DataFrame correspondiente
#   monto_digital = df_digital['MONTO_TRANSACCION'].sum()
#   ## 
#   monto_appcdmx = df_appcdmx['MONTO_TRANSACCION'].sum()

#   ## Obtener los valores únicos de la fecha de transacción
#   fechas_unicas = df_filtro['FECHA_HORA_TRANSACCION'].unique()

#   ## Agregar los resultados a la lista de resumen 
#   resumen.append({
#       'FECHA': ', '.join(fechas_unicas),
#       'TR Digitales': df_digital.shape[0],
#       'TR Fisicas': df_fisico.shape[0],
#       'TR AppCDMX': df_appcdmx.shape[0],
#       'TR Totales': tr_totales,
#       'Montos Digitales': monto_digital / 100,
#       'Montos Fisicos': monto_fisico / 100,
#       'Montos AppCDMX': monto_appcdmx / 100,
#       'Monto Total': (monto_digital + monto_fisico + monto_appcdmx)/100,
#   })
  
# ## Convertir el arreglo resumen en DataFrame
datos = pd.concat(transacciones)

print(f"Creando archivo FULL_{mes}_{dia_in}-{dia_fn-1}...")
archivo_full = f"FULL_{mes}_{dia_in}-{dia_fn-1}.csv"
ruta_datos = os.path.join(ruta_guardado, archivo_full)
datos.to_csv(ruta_datos, index=False)
# print(f"Creando archivo RES_{mes}_{dia_in}-{dia_fn-1}...")
# resultados = pd.DataFrame(resumen)
# archivo_res = f"RES_{mes}_{dia_in}-{dia_fn-1}.csv"
# ruta_res_sem = os.path.join(ruta_guardado, archivo_res)
# resultados.to_csv(ruta_res_sem, index=False)
print("Proceso realizado con Exito!!")