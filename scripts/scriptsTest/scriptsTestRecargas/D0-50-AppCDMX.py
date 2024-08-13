import os
import pandas as pd
## Nombre del mes con texto, se ocupara para leer la carpeta del mes y asignar el nombre a los archivos generados
mes_nombre = "Febrero"

## Modificar el contenido de m = "mes" * Para los meses que anteriores a octubre ocupar la sintaxis 09 = Septiembre 08 = Agosto
## Modificar el contenido de Y = "Año" 2023 / 2024 / 2025 
m = "02"
y = "2024"

## Nombre de las extenciones de los archivos que ocupara el script para realizar 
a = "-Transacciones.csv"
ae = "-Transacciones-extension.csv"

## Ruta de la cual se extraeran todos los archivos y en la misma se guardaran los archivos
ruta_guardado = f"Transacciones/{y}/{m} {mes_nombre}"

## Este es el rango de dias en el que se trabajara, para el tema del ultimo dia siempre se le sumara 1
## Ejemplo primera quincena dia_fn = 16 el metodo range trabaja de esa forma
dia_in = 1
dia_fn = 30
rango = dia_fn - dia_in

## Listado de los archvios -Transacciones.csv
## Listado de los archivo a leer segun el rango especificado 
archivo_tr = [os.path.join(ruta_guardado, f"{y}{m}{d:02d}{a}") for d in range(dia_in, dia_fn)]

transacciones = []
## Bucle para insertar todos los archivos en el DataFrame transacciones
for transaccion in archivo_tr:
  df = pd.read_csv(transaccion, low_memory=False)
  transacciones.append(df)
resumen = []

for df in transacciones:
    ## Filtrar las transacciones de tipo 0
    df['TIPO_TRANSACCION'] = df['TIPO_TRANSACCION'].astype('str')
    df_d0 = df[df['TIPO_TRANSACCION'] == 'D0'].copy()
    df_50 = df[df['TIPO_TRANSACCION'] == '50'].copy()

    ## Sacaremos el total de transacciones con el metodo count 
    tr_tot = df_d0['TIPO_TRANSACCION'].count()
    tr_total = df_50['TIPO_TRANSACCION'].count()

    ## Convertir la columna FECHA_HORA_TRANSACCION a datetime
    df['FECHA_HORA_TRANSACCION'] = pd.to_datetime(df['FECHA_HORA_TRANSACCION'])
    df['FECHA_HORA_TRANSACCION'] = df['FECHA_HORA_TRANSACCION'].dt.strftime('%Y-%m-%d')

    ## Separar las transacciones en base al método
    
    
    df_appcdmx1 = df_d0[df_d0['LOCATION_ID'] == '101801']
    df_appcdmx2 = df_50[df_50['LOCATION_ID'] == '101801']

    ## Calcular el monto total por transacción física y agregar al DataFrame correspondiente
    monto_d0 = df_appcdmx1['MONTO_TRANSACCION'].sum()

    ## Calcular el monto total por transacción digital y agregar al DataFrame correspondiente
    monto_50 = df_appcdmx2['MONTO_TRANSACCION'].sum()
    ## 
    

    ## Obtener los valores únicos de la fecha de transacción
    fechas_unicas = df['FECHA_HORA_TRANSACCION'].unique()

    ## Agregar los resultados a la lista de resumen 
    resumen.append({
        'FECHA': ', '.join(fechas_unicas),
        '# D0': tr_tot,
        '$ D0': monto_d0 / 100,
        '# 50': tr_total,
        '$ -50': monto_50 / 100,
    })
resultados = pd.DataFrame(resumen)
print(resultados)
archivo_sem = f"RRE_D0-50-AppCDMX.csv"
ruta_res_sem = os.path.join(ruta_guardado, archivo_sem)
resultados.to_csv(ruta_res_sem, index=False)