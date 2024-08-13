##importar librearias necesarias para el funcionamiento del script
import os
import pandas as pd
## Nombre del mes con texto, se ocupara para leer la carpeta del mes y asignar el nombre a los archivos generados
## Modificar el contenido de m = "mes" * Para los meses que anteriores a octubre ocupar la sintaxis 09 = Septiembre 08 = Agosto
mes = "Julio"
m = "07"
## Modificar el contenido de Y = "Año" 2023 / 2024 / 2025 
y = "2024"
## Nombre de las extenciones de los archivos que ocupara el script para realizar 
a = "-Transacciones.csv"
## Ruta de la cual se extraeran todos los archivos y en la misma se guardaran los archivos
ruta_guardado = f"data/{y}/{m} {mes}"
## Este es el rango de dias en el que se trabajara, para el tema del ultimo dia siempre se le sumara 1
## Ejemplo primera quincena dia_fn = 16 el metodo range trabaja de esa forma
dia_in = 16
dia_fn = 22
rango = dia_fn - dia_in
## Listado de los archvios -Transacciones.csv
## Listado de los archivo a leer segun el rango especificado 
archivo_tr = [os.path.join(ruta_guardado, f"{y}{m}{d:02d}{a}") for d in range(dia_in, dia_fn)]
# print(archivo_tr)
## Areglo que se llenara con los archivos -Transacciones-extension.csv
transacciones = []
## Bucle para insertar todos los archivos en el DataFrame transacciones

def impute_dates(df,dia):
  dia = dia + 1
  if dia < 10:
    dia = f'0{dia}'
  static_date = f'{y}-{m}-{dia} 00:00:00'
  default_date = f'2021-08-13 00:00:00'
  df['FECHA_HORA_TRANSACCION'] = df['FECHA_HORA_TRANSACCION'].fillna(static_date)
  df['CONTRACT_VALIDITY_START_DATE'] = df['CONTRACT_VALIDITY_START_DATE'].fillna(default_date)

for dia,transaccion in enumerate(archivo_tr):
    df = pd.read_csv(transaccion, low_memory=False)
    impute_dates(df,dia)
    transacciones.append(df)
##
datos = pd.concat(transacciones)
##
print(f"Creando archivo FULL_{mes}_{dia_in}-{dia_fn-1}...")
archivo_full = f"FULL_{mes}_{dia_in}-{dia_fn-1}.csv"
ruta_datos = os.path.join(ruta_guardado, archivo_full)
datos.to_csv(ruta_datos, index=False)
##
print("Proceso realizado con Exito!!")