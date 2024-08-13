##importar librearias necesarias para el funcionamiento del script
import os
import pandas as pd

## Nombre del mes con texto, se ocupara para leer la carpeta del mes y asignar el nombre a los archivos generados
mes_nombre = "Diciembre"

## Modificar el contenido de m = "mes" * Para los meses que anteriores a octubre ocupar la sintaxis 09 = Septiembre 08 = Agosto
## Modificar el contenido de Y = "Año" 2023 / 2024 / 2025 
m = "12"
y = "2023"

## Nombre de las extenciones de los archivos que ocupara el script para realizar 
a = "-Transacciones.csv"
ae = "-Transacciones-extension.csv"

## Ruta de la cual se extraeran todos los archivos y en la misma se guardaran los archivos
ruta_guardado = f"Transacciones/{y}/{m} {mes_nombre}"

## Este es el rango de dias en el que se trabajara, para el tema del ultimo dia siempre se le sumara 1
## Ejemplo primera quincena dia_fn = 16 el metodo range trabaja de esa forma
dia_in = 1
dia_fn = 32
rango = dia_fn - dia_in

## Listado de los archvios -Transacciones.csv
## Listado de los archivo a leer segun el rango especificado 
archivo_tr = [os.path.join(ruta_guardado, f"{y}{m}{d:02d}{a}") for d in range(dia_in, dia_fn)]

## Leer Archivos de Extencion para obtener la duracion de las transacciones
## Lista de nombres de archivo
archivo_ex = [os.path.join(ruta_guardado, f"{y}{m}{d:02d}{ae}") for d in range(dia_in, dia_fn)]

## Areglo que se llenara con los archivos -Transacciones-extension.csv
transacciones = []
## Bucle para insertar todos los archivos en el DataFrame transacciones
for transaccion in archivo_tr:
  df = pd.read_csv(transaccion, low_memory=False)
  transacciones.append(df)

## Concatenación de documentos extraídos del arreglo transacciones y creando un solo DataFrame con información de toda la quincena
## Arreglo que se llenara con los archivos -Transacciones.csv
extenciones = []
## Bucle para insertar todos los archivos en el DataFrame extenciones
for extencion in archivo_ex:
  df = pd.read_csv(extencion)
  extenciones.append(df)  

## Concatenación de documentos extraídos del arreglo extenciones y creando un solo DataFrame con información de todo el mes
df_extenciones = pd.concat(extenciones, ignore_index=True)
nan = 'nan'
sin_vacias = df_extenciones[df_extenciones['DEVICE_ID'] != nan]
smarpos_unique = sin_vacias['DEVICE_ID'].unique()

print(len(smarpos_unique))

print(smarpos_unique)