import pandas as pd
import os
## Define: mes ="Febrero" --- Nombre del mes
mes = "Julio"
## Define: m ="02"  ---- Numero del mes
m = "07"
## Define: y ="2024" --- Año a tomar en cuenta en el analisis
y = "2024"
## 
qna = '1ra_qna'
# qna = '2da_qna'
## Rango de dias semanales L - V Agregando + 1 al viernes ** Leer documentacion metodo range
dia_in = 1
dia_fn = 16
## Nombre de las extenciones de los archivos que ocupara el script para realizar 
a = "-Transacciones.csv"
a2 = "Transacciones"
## Ruta de la cual se extraeran todos los archivos y en la misma se guardaran los archivos
work_path = f"data/{y}/{m} {mes}"
archivo_tr = [os.path.join(work_path, f"{y}{m}{d:02d}{a}") for d in range(dia_in, dia_fn)]
## Areglo que se llenara con los archivos -Transacciones-extension.csv
transacciones = []
for transaccion in archivo_tr:
  df = pd.read_csv(transaccion, low_memory=False)
  transacciones.append(df)

df = pd.concat(transacciones)
df = df[[
  'ID_TRANSACCION_ORGANISMO', 
  'PROVIDER',
  'FECHA_HORA_TRANSACCION',
  'TIPO_TRANSACCION',
  'LOCATION_ID',
  'MONTO_TRANSACCION'
  ]]

arc_res_com = f'{a2}_{qna}_{mes}_{y}_short.csv'
ruta_res = os.path.join(work_path,arc_res_com)
df.to_csv(ruta_res,index=False)
print('Proceso Finalizado con exito!!')

