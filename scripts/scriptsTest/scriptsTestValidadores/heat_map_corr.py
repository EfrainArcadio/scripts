import pandas as pd
import os
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib as mpl
import json
## Define: semana 
semana = '39'
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
archivo = 'Validaciones semana 39.csv'
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
def dia_hora(dias,data,df_cons):
  if len(dias) < 5:
    for dia in dias:
      for hora in range(0,24):
        inicio = f"{dia} {hora:02d}:00:00"
        fin = f"{dia} {(hora+1):02d}:00:00"
        df_hora = df_cons[(df_cons['FECHA_HORA_TRANSACCION'] >= inicio) & (df_cons['FECHA_HORA_TRANSACCION'] < fin)]
        data[f"{hora:02d}:00"] = len(df_hora['AUTOBUS'].unique())
        
  else:
    for dia in dias:
      for hora in range(0,24):
        inicio = f"{dia} {hora:02d}:00:00"
        fin = f"{dia} {(hora+1):02d}:00:00"
        df_hora = df_cons[(df_cons['FECHA_HORA_TRANSACCION'] >= inicio) & (df_cons['FECHA_HORA_TRANSACCION'] < fin)]
        data[f"{hora:02d}:00"] = len(df_hora['AUTOBUS'].unique())

    # for dia in dias:
    #   for hora in range(0,24):
    #     inicio = f"{dia} {hora:02d}:00:00"
    #     # print(inicio)
    #     fin = f"{dia} {(hora+1):02d}:00:00"
    #     # print(fin)
    #     df_hora = df_cons[(df_cons['FECHA_HORA_TRANSACCION'] >= inicio) & (df_cons['FECHA_HORA_TRANSACCION'] < fin)]
    #     # print(df_hora)
    #     autobuses_por_hora[f"{hora:02d}:00"] += len(df_hora['AUTOBUS'].unique())
    #   for hora in autobuses_por_hora:
    #     autobuses_por_hora[hora] /= 5
    #     data[hora] = autobuses_por_hora[hora]
    #   resem.append(data)

# end diasUnicos

###############################################################################
#                                  Paths                                      #
###############################################################################

current_dir = os.getcwd()
ruta_trabajo = f"scripts/scriptsTest/scriptsTestValidadores/data/{y}/{m} {mes}/"
ruta_json = f"scripts/scriptsTest/scriptsTestValidadores/data"
archivo = os.path.join(ruta_trabajo, archivo)
path_json_int = os.path.join(ruta_json,json_int)
###############################################################################
#                                  Read Files                                 #
###############################################################################
# Lectura de archivos de Entrada
df = pd.read_csv(archivo, low_memory=False, encoding='latin-1')
with open(path_json_int) as f:
  data_int = json.load(f)
## ------- Definir datos de Salida --------
ruta_doc = os.path.join(ruta_trabajo, fn)
###############################################################################
#                             Transformar datos                               #
###############################################################################
## Convertir el TIPO_TRANSACCION a 
df['TIPO_TRANSACCION'] = df['TIPO_TRANSACCION'].astype(str)
# print(df['FECHA_HORA_TRANSACCION'].value_counts())
##
df_mdf = df.copy()
df_mdf['FECHA_HORA_TRANSACCION'] = pd.to_datetime(df_mdf['FECHA_HORA_TRANSACCION'],format='mixed')  
df_mdf['FECHA_HORA_TRANSACCION'] = df_mdf['FECHA_HORA_TRANSACCION'].dt.strftime('%d/%m/%Y')
fechas = sorted(df_mdf['FECHA_HORA_TRANSACCION'].unique().tolist() ) 

l_v = fechas[:5]
s = fechas[5:-1]
d = fechas[6:]

adma = []
for dia in l_v:
  df_day = df_mdf[df_mdf['FECHA_HORA_TRANSACCION'] == dia]
  size = len(df_day)
  adma.append({
    "dia":dia,
    "size": size
  })
df_l_v_s = pd.DataFrame(adma)

l_dma = df_l_v_s['size'].unique().tolist()
s_dma = max(l_dma)
df_dma = df_l_v_s[df_l_v_s['size'] == s_dma]
f_dma = df_dma['dia'].unique()

# print(df_mdf['FECHA_HORA_TRANSACCION'].value_counts())
resem = []
for origin,info_int in data_int.items():
  for inte in info_int:
    key = inte['key']
    name = inte['name']
    pv = inte['pv']
    ##
    df.replace({'LINEA': key}, name, inplace=True)
    df_val = df[df['TIPO_TRANSACCION'] == '3']
    df_cons = df_val[df_val['LINEA'] == name]
    # print(df_cons['FECHA_HORA_TRANSACCION'])
    data = {'Concesionario': name, 'Parque Vehicular Total': pv} 
    autobuses_por_hora = {f"{hora:02d}:00": 0 for hora in range(24)}
    if pv != '0':
      # print(dia_hora(l_v,data,df_cons))      
      # dia_hora(s,data,df_cons)      
      # dia_hora(d,data,df_cons)
      # dia_hora(f_dma,data,df_cons )
      for dia in fechas:
        for hora in range(0,24):
          inicio = f"{dia} {hora:02d}:00:00"
          # print(inicio)
          fin = f"{dia} {(hora+1):02d}:00:00"
          # print(fin)
          df_hora = df_cons[(df_cons['FECHA_HORA_TRANSACCION'] >= inicio) & (df_cons['FECHA_HORA_TRANSACCION'] < fin)]
          # print(df_hora)
          autobuses_por_hora[f"{hora:02d}:00"] += len(df_hora['AUTOBUS'].unique())
      for hora in autobuses_por_hora:
        autobuses_por_hora[hora] /= 5
        data[hora] = autobuses_por_hora[hora]
      resem.append(data)

prom_lv = pd.DataFrame(resem)      
print(prom_lv)

print()
# print(df['LINEA'].value_counts())
# print(heat_map_info)
# for info_l in heat_map_info:
#   print(info_l)
  # print(pv)
# resem = []
## Crear
# for codigo, info in json_info.items():
#     ## Dataframe por Linea
#     print(codigo)
#     print(info)
#     df_cons = df_val[df_val['LINEA'] == info['nombre']]
    ## Data BASE
#     data = {'Concesionario': info['nombre'], 'Parque Vehicular Total': info['pv']} 
#     autobuses_por_hora = {f"{hora:02d}:00": 0 for hora in range(24)}
#     for dia in sem:
#         # print(dia)
#         if dia < 10:
#           dia = f'0{dia}'
#         for hora in range(0, 24):
#             inicio = f"{dia}/{m}/{y} {hora:02d}:00:00"
#             fin = f"{dia}/{m}/{y} {(hora+1):02d}:00:00"
#             df_hora = df_cons[(df_cons['FECHA_HORA_TRANSACCION'] >= inicio) & (df_cons['FECHA_HORA_TRANSACCION'] < fin)]
#             autobuses_por_hora[f"{hora:02d}:00"] += len(df_hora['AUTOBUS'].unique())
#     # Calcular el promedio de autobuses por hora
#     for hora in autobuses_por_hora:
#         autobuses_por_hora[hora] /= 5
#         data[hora] = autobuses_por_hora[hora]
#     # Append data for current line to the results
#     resem.append(data)
# # Create Pandas DataFrame from results
# prom_lv = pd.DataFrame(resem)

## Crear

# redma = []
# diasUnicos(redma,dma)
# resumen_dma = pd.DataFrame(redma)
# resb = []
# diasUnicos(resb,sb)
# resumen_sb = pd.DataFrame(resb)
# redm = []
# diasUnicos(redm,dm)
# resumen_dm = pd.DataFrame(redm)


# # ## Crear XLSX
# with pd.ExcelWriter(ruta_doc) as writer:
#     prom_lv.to_excel(writer, index=False, sheet_name=f'PROM_AUTOBUS_L-V')
#     resumen_dma.to_excel(writer, index=False, sheet_name=f'AUTOBUS_{dma}')
#     resumen_sb.to_excel(writer, index=False, sheet_name=f'AUTOBUS_S')
#     resumen_dm.to_excel(writer, index=False, sheet_name=f'AUTOBUS_D')


