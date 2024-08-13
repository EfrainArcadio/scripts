##importar librearias necesarias para el funcionamiento del script
import os
import pandas as pd

## 
smartpos = 'SMARTPOS1494126447'
## Nombre del mes con texto, se ocupara para leer la carpeta del mes y asignar el nombre a los archivos generados
mes_nombre = "Julio"
## 
## Modificar el contenido de m = "mes" * Para los meses que anteriores a octubre ocupar la sintaxis 09 = Septiembre 08 = Agosto
## Modificar el contenido de Y = "Año" 2023 / 2024 / 2025 
m = "07"
y = "2024"

## Nombre de las extenciones de los archivos que ocupara el script para realizar 
a = "-Transacciones.csv"
ae = "-Transacciones-extension.csv"

## Ruta de la cual se extraeran todos los archivos y en la misma se guardaran los archivos
ruta_guardado = f"data/{y}/{m} {mes_nombre}"

## Este es el rango de dias en el que se trabajara, para el tema del ultimo dia siempre se le sumara 1
## Ejemplo primera quincena dia_fn = 16 el metodo range trabaja de esa forma
dia_in = 15
dia_fn = 22
rango = dia_fn - dia_in

periodo = f'{dia_in} - {dia_fn-1}'
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

df_smartpos = pd.concat(extenciones,ignore_index=True)
df_trmes = pd.concat(transacciones,ignore_index=True)

smartpos_filtered = df_smartpos[df_smartpos['DEVICE_ID'] == smartpos]
df_coincidencias = pd.merge(df_trmes, smartpos_filtered, on="ID_TRANSACCION_ORGANISMO", how="inner")
   ## Convertir la columna FECHA_HORA_TRANSACCION a datetime
df_coincidencias['FECHA_HORA_TRANSACCION'] = pd.to_datetime(df_coincidencias['FECHA_HORA_TRANSACCION'])
df_coincidencias['FECHA_HORA_TRANSACCION'] = df_coincidencias['FECHA_HORA_TRANSACCION'].dt.strftime('%Y-%m-%d')
# Obtener los valores únicos de la fecha de transacción
fechas_unicas = df_coincidencias['FECHA_HORA_TRANSACCION'].unique()
resumen = []
for fecha in fechas_unicas:
  print(fecha)
  df_fil = df_coincidencias.loc[df_coincidencias['FECHA_HORA_TRANSACCION'] == fecha]
  print(df_fil)
  monto = sum(df_fil['MONTO_TRANSACCION'] / 100)
  resumen.append({
      'FECHA': fecha,
      '# Transacciones': df_fil.shape[0],
      'Monto': monto,
  })
  
res = pd.DataFrame(resumen)
archivo_res = f"Resumen_AULSA_{periodo}_{mes_nombre}.csv"
ruta_resultados = os.path.join(ruta_guardado,archivo_res )
res.to_csv(ruta_resultados, index=False)
print('Analisis Finalizado con Exito!!')



