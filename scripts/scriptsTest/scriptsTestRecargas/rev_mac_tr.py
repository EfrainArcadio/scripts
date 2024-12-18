import os
import pandas as pd
## Nombre del mes con texto, se ocupara para leer la carpeta del mes y asignar el nombre a los archivos generados
mes = "Noviembre"

## Modificar el contenido de m = "mes" * Para los meses que anteriores a octubre ocupar la sintaxis 09 = Septiembre 08 = Agosto
## Modificar el contenido de Y = "Año" 2023 / 2024 / 2025 
m = "11"
y = "2024"

## Nombre de las extenciones de los archivos que ocupara el script para realizar 
a = "-Transacciones.csv"

## Este es el rango de dias en el que se trabajara, para el tema del ultimo dia siempre se le sumara 1
## Ejemplo primera quincena dia_fn = 16 el metodo range trabaja de esa forma
dia_in = 1
dia_fn = 31
rango = dia_fn - dia_in

## Ruta de la cual se extraeran todos los archivos y en la misma se guardaran los archivos
path_scripts = os.getcwd()
path_root = os.path.dirname(path_scripts)

path_files = f'dataFiles/recargas/{y}/{m} {mes}'

pathInfo = os.path.join(path_root,path_files)
path_dumps = os.path.join( path_scripts,f'public/recargas/renovContract')

def path_verify(path):
  if not os.path.exists(path):
    os.makedirs(path)
    print(f'Directorio Creado: {path}')
  else:
    print(f'El Directorio ya existe: {path}')
    
path_verify(path_dumps)
## Listado de los archvios -Transacciones.csv
## Listado de los archivo a leer segun el rango especificado 
archivo_tr = [os.path.join(pathInfo, f"{y}{m}{d:02d}{a}") for d in range(dia_in, dia_fn)]

transacciones = []
## Bucle para insertar todos los archivos en el DataFrame transacciones
for transaccion in archivo_tr:
  df = pd.read_csv(transaccion, low_memory=False)
  transacciones.append(df)

df = pd.concat(transacciones)

print(df['TIPO_TRANSACCION'].value_counts())

df_dig = df[df['LOCATION_ID'] == '101800']
df_dig_ex = df_dig[df_dig['TIPO_TRANSACCION'] == '0']
df_renov = df_dig_ex[(df_dig_ex['SALDO_ANTES_TRANSACCION'] == 0) & (df_dig_ex['MONTO_TRANSACCION'] == 0) & (df_dig_ex['SALDO_DESPUES_TRANSACCION'] == 0)]
df_renov_9 = df_dig[df_dig['TIPO_TRANSACCION'] == '9']

tr_valids = ['0','9']
df_suc = df_dig[df_dig['TIPO_TRANSACCION'].isin(tr_valids)]
print(df_suc)
print('Tarjetas Orig',len(df_renov['NUMERO_SERIE_HEX']))
print('Tarjetas uniq',len(set(df_renov['NUMERO_SERIE_HEX'])))
print('Tarjetas Orig_9',len(df_renov_9['NUMERO_SERIE_HEX']))
print('Tarjetas uniq_9',len(set(df_renov_9['NUMERO_SERIE_HEX'])))

print('T9',len(df_renov_9))
print('3C',len(df_renov))

if len(df_renov) > 0:
  file_3c = f'3C_{mes}.csv'
  path_file_3c = os.path.join(path_dumps,file_3c)
  df_renov.to_csv(path_file_3c, index=False)
  print(f'{file_3c} Generado con exito!')
if len(df_renov_9) > 0:
  file_t9 = f'T9_{mes}.csv'
  path_file_t9 = os.path.join(path_dumps,file_t9)
  df_renov_9.to_csv(path_file_t9, index=False)
  print(f'{file_t9} Generado con exito!')

