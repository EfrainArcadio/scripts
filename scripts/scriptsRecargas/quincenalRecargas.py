import os
import pandas as pd
## Primer fecha compuesta de la consulta 
y= "2024"
mes = "Octubre"
m = "10"
dia_in = 16
dia_fn = 32
# qna = '1ra'
qna = '2da'
a = "-Transacciones.csv"
ae = "-Transacciones-extension.csv"
###############################################################################
#                           Carga de archivos                                 #
###############################################################################
def path_verify(path):
  if not os.path.exists(path):
    os.makedirs(path)
    print(f'Directorio Creado: {path}')
  else:
    print(f'El Directorio ya existe: {path}')

## Obtener la ruta del directorio actual
ruta_actual = os.path.dirname(__file__)
## Subir un nivel en el directorio
ruta_superior = os.path.dirname(ruta_actual)
ruta_sup_2 = os.path.dirname(ruta_superior)
ruta_sup = os.path.dirname(ruta_sup_2)
##
path_files = os.path.join(ruta_sup,f'dataFiles/recargas/{y}/{m} {mes}')
ruta_dumps = os.path.join( ruta_sup_2,f'public/recargas/quincenas/{y}/{m} {mes}/{qna} qna')
path_verify(ruta_dumps)

##
archivo_tr = [os.path.join(path_files, f"{y}{m}{d:02d}{a}") for d in range(dia_in, dia_fn)]
## Leer Archivos de Extencion para obtener la duracion de las transacciones
## Lista de nombres de archivo
archivo_ex = [os.path.join(path_files, f"{y}{m}{d:02d}{ae}") for d in range(dia_in, dia_fn)]
##
transacciones = []
## Bucle para insertar todos los archivos en el DataFrame transacciones
for transaccion in archivo_tr:
  df = pd.read_csv(transaccion)
  transacciones.append(df)
df_transacciones = pd.concat(transacciones)
print('Creando archivo Transacciones...')
tr_file = f'Full_{qna}_qna_{mes}_{y}.csv'
ruta_res = os.path.join(ruta_dumps,tr_file)
df_transacciones.to_csv(ruta_res,index=False)
## Concatenación de documentos extraídos del arreglo transacciones y creando un solo DataFrame con información de toda la quincena
## Arreglo que se llenara con los archivos -Transacciones.csv
extenciones = []
## Bucle para insertar todos los archivos en el DataFrame extenciones
for extencion in archivo_ex:
  df = pd.read_csv(extencion)
  extenciones.append(df)
print('Creando archivo Extenciones...')
df_extenciones = pd.concat(extenciones)
ext_file = f'Full_ext_{qna}_qna_{mes}_{y}.csv'
ruta_res = os.path.join(ruta_dumps,ext_file)
df_extenciones.to_csv(ruta_res,index=False)
##
df_short_tr = df_transacciones[[
  'ID_TRANSACCION_ORGANISMO', 
  'PROVIDER',
  'FECHA_HORA_TRANSACCION',
  'TIPO_TRANSACCION',
  'LOCATION_ID',
  'MONTO_TRANSACCION'
]]
##
print('Creando archivo Short Transacciones...')
short_file = f'{qna}_qna_{mes}_{y}{a}'
ruta_res = os.path.join(ruta_dumps,short_file)
df_short_tr.to_csv(ruta_res,index=False)
##
tr_suc = df_transacciones[df_transacciones['TIPO_TRANSACCION'] == '0']
df_appcdmx = tr_suc[tr_suc['LOCATION_ID'] == '101801']    
df_digital = tr_suc[tr_suc['LOCATION_ID'] == '101800']    
df_fisicas = tr_suc[tr_suc['LOCATION_ID'] == '201A00']    
# Obtener el monto total
montoAppcdmx = df_appcdmx['MONTO_TRANSACCION']  /100
montoDigital = df_digital['MONTO_TRANSACCION'] /100
montoFisico = df_fisicas['MONTO_TRANSACCION'] /100

res_cards = []

res_cards.append({
  'Tarjetas': len(set(tr_suc['NUMERO_SERIE_HEX'])),
  'Transacciones': len(tr_suc),
  '$ Promedio Digital': montoDigital.mean(),
  '$ Promedio Fisico': montoFisico.mean(),
  '$ Promedio AppCDMX': montoAppcdmx.mean(),
})
print("Generando resumen de las tarjetas...")
res_tarjetas = pd.DataFrame(res_cards)
archivo_tar = f"Tarjetas_{qna}_qna_{mes}.csv"
ruta_resultados = os.path.join(ruta_dumps, archivo_tar)
res_tarjetas.to_csv(ruta_resultados, index=False)
print('Proceso Finalizado con exito!!')