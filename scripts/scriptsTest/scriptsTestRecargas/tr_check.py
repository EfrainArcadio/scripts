import pandas as pd
import os
## Define: mes ="Febrero" --- Nombre del mes
mes = "Junio"
## Define: m ="02"  ---- Numero del mes
m = "06"
## Define: y ="2024" --- Año a tomar en cuenta en el analisis
y = "2024"
## Rango de dias semanales L - V Agregando + 1 al viernes ** Leer documentacion metodo range
d = "16"
## ------- Definir datos de Entrada --------
ruta_trabajo = f"Transacciones/{y}/{m} {mes}/"
# archivo = f'{mes}_crudo_2023.csv'
archivo = f'{y}{m}{d}-Transacciones.csv'
archivo_pt = os.path.join(ruta_trabajo, archivo)
# Lectura del archivo de Entrada
df = pd.read_csv(archivo_pt, low_memory=False, encoding='latin-1')

reemplazos = {
  '101800': 'RRE-Digital',
  '201A00': 'RRE-Fisica',
  '101801': 'APPCDMX'
}
df = df.assign(TIPO_RED='')
df['MONTO_TRANSACCION'] = df['MONTO_TRANSACCION']/100
df['TIPO_RED'] = df['LOCATION_ID'].map(reemplazos)
# print(df.columns)
df = df[['ID_TRANSACCION_ORGANISMO', 'FECHA_HORA_TRANSACCION','LOCATION_ID','TIPO_RED','NUMERO_SERIE_HEX','TIPO_TRANSACCION','MONTO_TRANSACCION']]

arc_res_com = f'RES_{archivo}'
ruta_res = os.path.join(ruta_trabajo,arc_res_com)
df.to_csv(ruta_res,index=False)
print('Proceso Finalizado con exito!!')


          