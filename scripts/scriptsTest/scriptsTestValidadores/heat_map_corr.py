import pandas as pd
import os
import glob
import json
## Define: semana 
semana = '40'
## Define: mes ="Febrero" --- Nombre del mes
mes = "Septiembre"
## Define: m ="02"  ---- Numero del mes
m = "09"
## Define: y ="2024" --- Año a tomar en cuenta en el analisis
y = "2024"
## Rango de dias semanales L - V Agregando + 1 al viernes ** Leer documentacion metodo range
## ------- Definir datos de Entrada --------
### File Names
json_int = "integradores.json"

## Nombre del archivo y ruta de salida
fn = f'RE_BUS_{semana}_{mes}.xlsx'
###############################################################################
#                                Funciones                                    #
###############################################################################
def path_verify(path):
  if not os.path.exists(path):
    os.makedirs(path)
    print(f'Directorio Creado: {path}')
  else:
    print(f'El Directorio ya existe: {path}')
## end path_verify
def df_day(df,dias,array):
  for dia in dias:
    # print(dia)
    df_day = df[df['FECHA_HORA_TRANSACCION'] == dia]
    array.append(df_day)
  return array
def sheet_gen(df,fechas,array_out):
  lineas = set(df['LINEA'])
  lineas.remove('CETRAM ZAPATA')
  lineas.remove('37')
  lineas.remove('CETRAM TACUBAYA')
  lineas.remove('CETRAM BUENAVISTA')
  df_val = df[df['TIPO_TRANSACCION'] == '3']
  for linea in lineas:
    df_cons = df_val[df_val['LINEA'] == linea]
    data = {'Concesionario': linea} 
    val_por_hora = {f"{hora:02d}:00": 0 for hora in range(24)}
    for dia in fechas:
      for hora in range(0,24):
        inicio = f"{dia} {hora:02d}:00:00"
        fin = f"{dia} {(hora+1):02d}:00:00"
        df_hora = df_cons[(df_cons['FECHA_HORA_TRANSACCION'] >= inicio) & (df_cons['FECHA_HORA_TRANSACCION'] < fin)]
        val_por_hora[f"{hora:02d}:00"] += len(df_hora)
    for hora in val_por_hora:
      val_por_hora[hora] /= len(fechas)
      data[hora] = val_por_hora[hora]
    array_out.append(data)
  return array_out
###############################################################################
#                                  Paths                                      #
###############################################################################

ruta_actual = os.getcwd()
parent_dir = os.path.dirname(ruta_actual)
ruta_trabajo = f"dataFiles/validaciones/{y}/OCT_NOV"
ruta_json = f"data"
ruta_info = os.path.join(parent_dir,ruta_trabajo)
# print(ruta_info)
archivos = glob.glob(os.path.join(ruta_info, '*.csv'))
path_json_int = os.path.join(ruta_json,json_int)
###############################################################################
#                                  Read Files                                 #
###############################################################################
# Lectura de archivos de Entrada
# df = pd.read_csv(archivo, low_memory=False, encoding='latin-1')

with open(path_json_int) as f:
  data_int = json.load(f)
## ------- Definir datos de Salida --------
ruta_doc = os.path.join(ruta_info, fn)
###############################################################################
#                             Transformar datos                               #
###############################################################################
dfs = []
for archivo in archivos:
    # print(archivo)
    df = pd.read_csv(archivo,encoding='latin-1',low_memory=False)
    df = df.rename(columns={'ï»¿ID_TRANSACCION_ORGANISMO': 'ID_TRANSACCION_ORGANISMO'})
    dfs.append(df)
df = pd.concat(dfs)
# print(df)
## Convertir el TIPO_TRANSACCION a 
# print(set(df['LINEA']))
df['TIPO_TRANSACCION'] = df['TIPO_TRANSACCION'].astype(str)
df['LINEA'] = df['LINEA'].astype(str)
for origin,info_int in data_int.items():

  # print(info_int)
  for inte in info_int:
    key = inte['key']
    name = inte['name']
    df.replace({'LINEA': key}, name, inplace=True)
# print(set(df['LINEA']))

df_mdf = df.copy()
df_mdf['FECHA_HORA_TRANSACCION'] = pd.to_datetime(df_mdf['FECHA_HORA_TRANSACCION'],format='mixed')  
df_mdf['FECHA_HORA_TRANSACCION'] = df_mdf['FECHA_HORA_TRANSACCION'].dt.strftime('%d/%m/%Y')
fechas = sorted(df_mdf['FECHA_HORA_TRANSACCION'].unique().tolist() ) 
# print(fechas)

l_v = [
  '2024-10-01',
  '2024-10-02',
  '2024-10-03',
  '2024-10-04',
  '2024-10-07',
  '2024-10-08',
  '2024-10-09',
  '2024-10-10',
  '2024-10-11',
  '2024-10-14',
  '2024-10-15',
  '2024-10-16',
  '2024-10-17',
  '2024-10-18',
  '2024-10-21',
  '2024-10-22',
  '2024-10-23',
  '2024-10-24',
  '2024-10-25',
  '2024-10-28',
  '2024-10-29',
  '2024-10-30',
  '2024-10-31',
  '2024-11-01',
  '2024-11-04',
  '2024-11-05',
  '2024-11-06',
  '2024-11-07',
  '2024-11-08',
  '2024-11-11',
  '2024-11-12',
  '2024-11-13',
  '2024-11-14',
  '2024-11-15',
]

s = [
  '2024-10-05',
  '2024-10-12',
  '2024-10-19',
  '2024-10-26',
  '2024-11-02',
  '2024-11-09',
]

d = [
  '2024-10-06',
  '2024-10-13',
  '2024-10-20',
  '2024-10-27',
  '2024-11-03',
  '2024-11-10',
]

re_l_v = []
prom_lv = pd.DataFrame(sheet_gen(df,l_v,re_l_v))  
re_s = []
prom_s = pd.DataFrame(sheet_gen(df,s,re_s))  
re_d= []
prom_d = pd.DataFrame(sheet_gen(df,d,re_d))  
   
# print(prom_lv)

# # # ## Crear XLSX
with pd.ExcelWriter(ruta_doc) as writer:
    prom_lv.to_excel(writer, index=False, sheet_name=f'PROM_AUTOBUS_L-V')
    prom_s.to_excel(writer, index=False, sheet_name=f'PROM_AUTOBUS_S')
    prom_d.to_excel(writer, index=False, sheet_name=f'PROM_AUTOBUS_D')


print('Archivo creado con exito')
