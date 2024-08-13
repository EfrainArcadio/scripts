##importar librearias necesarias para el funcionamiento del script
import os
import pandas as pd

## Nombre del mes con texto, se ocupara para leer la carpeta del mes y asignar el nombre a los archivos generados
mes_nombre = "Octubre"

## Modificar el contenido de m = "mes" * Para los meses que anteriores a octubre ocupar la sintaxis 09 = Septiembre 08 = Agosto
## Modificar el contenido de Y = "Año" 2023 / 2024 / 2025 
m = "10"
y = "2023"

## Nombre de las extenciones de los archivos que ocupara el script para realizar 
a = "-Transacciones.csv"

## Ruta de la cual se extraeran todos los archivos y en la misma se guardaran los archivos
ruta_guardado = f"Transacciones/{y}/{m} {mes_nombre}"

## Este es el rango de dias en el que se trabajara, para el tema del ultimo dia siempre se le sumara 1
## Ejemplo primera quincena dia_fn = 16 el metodo range trabaja de esa forma
dia_in =  1
dia_fn = 9

## Listado de los archvios -Transacciones.csv
## Listado de los archivo a leer segun el rango especificado 
archivo_tr = [os.path.join(ruta_guardado, f"{y}{m}{d:02d}{a}") for d in range(dia_in, dia_fn)]

## Arreglo que se llenara con los archivos -Transacciones.csv
transacciones = []

## Bucle para insertar todos los archivos en el DataFrame transacciones

for transaccion in archivo_tr:
    df = pd.read_csv(transaccion)
    transacciones.append(df)
    
## Concatenación de documentos extraídos del arreglo transacciones y creando un solo DataFrame con información de toda la quincena
mes_completo = f"{mes_nombre}_crudo_{y}.csv"
df_completo = pd.concat(transacciones, ignore_index=True)
ruta_completa = os.path.join(ruta_guardado, mes_completo)
df_completo.to_csv(ruta_completa, index=False)

print('Proceso finalizado..')