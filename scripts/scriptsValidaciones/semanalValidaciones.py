import os
import pandas as pd
import json
###############################################################################
#                           Carga de archivos                                 #
###############################################################################
## Definimos el mes con nombre
mes = "Octubre"
## Definimos el mes con número
m = "10"
## Definimos el año
y = "2024"
folder_p = 'semanas'
# folder_p = 'quincenas'
periodo = '40'
# periodo = '1ra qna'
# periodo = '2da qna'
## Nombres de archivos
name_file = "Validaciones semana 40"
file = f'{name_file}.csv'
json_int = "integradores.json"
json_tot = "transacciones.json"
#
col_n_l = 'Linea'
col_n_f = 'Fecha'
##
linea = 'LINEA'
dato_fec = 'FECHA_HORA_TRANSACCION'    
tot_v = 'TIPO_TRANSACCION' 
####################################################################################################
def path_verify(path):
  if not os.path.exists(path):
    os.makedirs(path)
    print(f'Directorio Creado: {path}')
  else:
    print(f'El Directorio ya existe: {path}')
## end path_verify
## Modificar Lineas con base al JSON de Integradores
def remplazo_valores(data_json,df,column):
  for origen,replacements in data_json.items():
    for origen_dict in replacements:
      # print(origen_dict)
      for codigo, replacement in origen_dict.items():
        # print(replacement)
        df.replace({f'{column}':codigo},replacement,inplace=True)
        # print(replacement)
## end remplazo_valores    
def resumen(lista,df,n_col,lista_tp):
  dic_el = []
  for element in lista:
    df_element = df[df[f'{n_col}'] == element]
    df_bus = df_element[df_element['TIPO_TRANSACCION'] == 'Debito en Bus']
    df_ban = df_element[df_element['TIPO_TRANSACCION'] == 'Debito en bano']
    mto_bus = df_bus['MONTO_TRANSACCION'].sum() / 100
    mto_ban = df_ban['MONTO_TRANSACCION'].sum() / 100
    dic = []
    dic.append({'Monto Autobus': mto_bus} )
    dic.append({'Monto Bano': mto_ban} )
    for tot in lista_tp:
      df_tot =df_element[df_element['TIPO_TRANSACCION'] == tot]
      z = len(df_tot)
      dic.append({tot: z})
    dic_el.append({element:dic})
  return dic_el
## end resumen
def read_list(lista,col_name):
  data = []
  for key_l in lista:
    for key_d in key_l:
      data_key_e = key_l[f'{key_d}'] 
      for element in data_key_e:
        label = list(element.keys())[0]
        cantidad = element[label]
        data.append({
        f'{col_name}': key_d,
        f'{label}': cantidad    
        })
  df_list = pd.DataFrame(data)
  df_grouped = df_list.groupby(col_name).sum()
  df_grouped = df_grouped.reset_index()
# Aplicar la solución
  columnas_numericas = df_grouped.select_dtypes(include=['number']).columns
  total_row = pd.DataFrame(df_grouped[columnas_numericas].sum()).T
  total_row[f'{col_name}'] = 'Total'
  df_final = pd.concat([df_grouped, total_row], ignore_index=True)
  return df_final
## end read_list
def heat_sheet(df,array,dias):
  if len(dias) == 5:
    # print("dias_mayor a 5")
    for origin,info_int in data_int.items():
      for inte in info_int:
        name = inte['name']
        pv = inte['pv']
        key = inte['key']
        if pv != '0':
          df.replace({'LINEA': key}, name, inplace=True)
          # print(df['LINEA'].value_counts())
          df_linea = df[df['LINEA'] == name]
          # print(df_linea)
          data = {'Concesionario': name, 'Parque Vehicular Total': pv} 
          autobuses_por_hora = {f"{hora:02d}:00": 0 for hora in range(24)}
          for dia in dias:
            for hora in range(0, 24):
              inicio = f"{dia} {hora:02d}:00"
              # print(inicio)
              fin = f"{dia} {(hora+1):02d}:00"
              # print(inicio)
              # print(fin)
              df_hora = df_linea[(df_linea['FECHA_HORA_TRANSACCION'] >= inicio) & (df_linea['FECHA_HORA_TRANSACCION'] < fin)]
              # print(df_hora['FECHA_HORA_TRANSACCION'].value_counts())
              autobuses_por_hora[f"{hora:02d}:00"] += len(df_hora['AUTOBUS'].unique())
          for hora in autobuses_por_hora:
            autobuses_por_hora[hora] /= 5
            data[hora] = autobuses_por_hora[hora]
          array.append(data)
  else:
    for origin,info_int in data_int.items():
      for inte in info_int:
        name = inte['name']
        pv = inte['pv']
        key = inte['key']
        if pv != '0':
          df.replace({'LINEA': key}, name, inplace=True)
          df_linea = df[df['LINEA'] == name]
          # print(df_linea['FECHA_HORA_TRANSACCION'].va)
          data = {'Concesionario': name, 'Parque Vehicular Total': pv} 
          for dia in dias:
            # print(dia)
            for hora in range(0, 24):
              inicio = f"{dia} {hora:02d}:00"
              fin = f"{dia} {(hora+1):02d}:00"
              df_hora = df_linea[(df_linea['FECHA_HORA_TRANSACCION'] >= inicio) & (df_linea['FECHA_HORA_TRANSACCION'] < fin)]
              # print(df_hora['FECHA_HORA_TRANSACCION'].value_counts())
              data[f"{hora:02d}:00"] = len(df_hora['AUTOBUS'].unique())
        # Append data for current line to the results
            array.append(data)
## end heat_sheet
####################################################################################################
ruta_actual = os.getcwd()
# print(ruta_actual)
parent_dir = os.path.dirname(ruta_actual)
# ## Subir un nivel en el directorio
# ## Produccion
ruta_test_json = os.path.join(ruta_actual,'scripts/scriptsValidaciones/data')
# path_files = os.path.join(ruta_actual,f'scripts/scriptsTest/scriptsTestValidadores/data/{y}/{m} {mes}')
path_files = os.path.join(parent_dir,f'dataFiles/validaciones/{y}/{folder_p}')
path_dumps = os.path.join(ruta_actual,f'public/validaciones/{folder_p}/{y}/{periodo}')
path_verify(path_dumps)

## Ruta donde se leen los archivos json
path_json_int = os.path.join(ruta_test_json,json_int)
path_json_tot = os.path.join(ruta_test_json,json_tot)
path_file = os.path.join(path_files, file)
## Carga de archivo csv para analisis 
df = pd.read_csv(path_file,encoding='latin-1', low_memory=False)
## Load JSON data integradores
with open(path_json_int) as f:
    data_int = json.load(f)
## Load JSON data integradores
with open(path_json_tot) as f:
    data_tot = json.load(f)
df['TIPO_TRANSACCION'] = df['TIPO_TRANSACCION'].astype('str')
###############################################################################
#                           Modificacion de Formatos                          #
###############################################################################
## Funciones de modificacion de datos
remplazo_valores(data_tot,df,tot_v)
df_bus = df[df['TIPO_TRANSACCION'] == 'Debito en Bus'].copy()
for origin,info_int in data_int.items():
  for inte in info_int:
    key = inte['key']
    name = inte['name']
    ##
    df.replace({'LINEA': key}, name, inplace=True)
## Crear Listas a utilizar
lineas = df['LINEA'].unique().tolist()
# print(lineas)
tipos = df['TIPO_TRANSACCION'].unique().tolist()
## Modificar formato de las fechas
df['FECHA_HORA_TRANSACCION'] = pd.to_datetime(df['FECHA_HORA_TRANSACCION'], format='%d/%m/%Y %H:%M')
df['FECHA_HORA_TRANSACCION'] = df['FECHA_HORA_TRANSACCION'].dt.strftime('%d/%m/%Y')
## Obtener fechas unicas
fechas_unicas = df['FECHA_HORA_TRANSACCION'].unique().tolist()
print(fechas_unicas)
fechas_unicas = fechas_unicas[::-1]
print(fechas_unicas)
##  Aplicar Resumen
print("Generando Resumen")
fechas = resumen(fechas_unicas,df,dato_fec,tipos)
lineas = resumen(lineas,df,linea,tipos)
## Leer listas de resumen
df_fechas = read_list(fechas,col_n_f)
df_lineas = read_list(lineas,col_n_l)
## Datos archivo Final
lista = f"res_validaciones_{periodo}_{m}_{y}.xlsx"
ruta_lista = os.path.join(path_dumps,lista)
## Generacion de archivo de resultados

with pd.ExcelWriter(ruta_lista) as writer:
  df_fechas.to_excel(writer, index=False ,sheet_name=f'Resumen Validaciones ')
  df_lineas.to_excel(writer, index=False ,sheet_name=f'Resumen Linea ')
## HEATMAP
print("Generando Mapa de Calor")
l_v = fechas_unicas[:5]
s = fechas_unicas[5:-1]
d = fechas_unicas[6:]

adma = []
for dia in l_v:
  df_day = df[df['FECHA_HORA_TRANSACCION'] == dia]
  size = len(df_day)
  adma.append({
    "dia":dia,
    "size": size
  })
df_l_v_s = pd.DataFrame(adma)

df_dma = df_l_v_s[df_l_v_s['size'] == max(df_l_v_s['size']) ]

dma_s = df_dma['dia'].unique()
print("dia más alto:",dma_s)
array_l_v = []
heat_sheet(df_bus,array_l_v,l_v)
res_l_v = pd.DataFrame(array_l_v)

array_dma = []
heat_sheet(df_bus,array_dma,dma_s)
res_dma = pd.DataFrame(array_dma)

array_s = []
heat_sheet(df_bus,array_s,s)
res_s = pd.DataFrame(array_s)

array_d = []
heat_sheet(df_bus,array_d,d)
res_d = pd.DataFrame(array_d)


file = f"res_bus_{periodo}_{m}_{y}.xlsx"
ruta_file = os.path.join(path_dumps,file)
## Generacion de archivo de resultados
df_ordenado_l_v= res_l_v.sort_values(by='Concesionario')
df_ordenado_dma= res_dma.sort_values(by='Concesionario')
df_ordenado_s= res_s.sort_values(by='Concesionario')
df_ordenado_d= res_d.sort_values(by='Concesionario')
with pd.ExcelWriter(ruta_file) as writer:
  df_ordenado_l_v.to_excel(writer, index=False ,sheet_name=f'Promedio Semanal')
  df_ordenado_dma.to_excel(writer, index=False ,sheet_name=f'Resumen DMA')
  df_ordenado_s.to_excel(writer, index=False ,sheet_name=f'Resumen Sabado ')
  df_ordenado_d.to_excel(writer, index=False ,sheet_name=f'Resumen Domingo')
print('Proceso realizado con exito')