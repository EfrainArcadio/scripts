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
folder_p = 'quincenas'
# folder_p = 'quincenas'
# periodo = '40'
periodo = '1ra qna'
# periodo = '2da qna'
## Nombres de archivos
name_file = "1ra_qna_Octubre_2024"
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

####################################################################################################
ruta_actual = os.getcwd()
# print(ruta_actual)
parent_dir = os.path.dirname(ruta_actual)
# ## Subir un nivel en el directorio
# ## Produccion
ruta_test_json = os.path.join(ruta_actual,'scripts/scriptsValidaciones/data')
# path_files = os.path.join(ruta_actual,f'scripts/scriptsTest/scriptsTestValidadores/data/{y}/{m} {mes}')
path_files = os.path.join(parent_dir,f'dataFiles/validaciones/{y}/{m} {mes}/')

path_dumps = os.path.join(ruta_actual,f'public/validaciones/{folder_p}/{y}')
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

for origin,info_int in data_int.items():
  for inte in info_int:
    key = inte['key']
    name = inte['name']
    ##
    df.replace({'LINEA': key}, name, inplace=True)
## Crear Listas a utilizar
lineas = df['LINEA'].unique().tolist()
print(lineas)
tipos = df['TIPO_TRANSACCION'].unique().tolist()
## Modificar formato de las fechas
df['FECHA_HORA_TRANSACCION'] = pd.to_datetime(df['FECHA_HORA_TRANSACCION'], format='%Y-%m-%d %H:%M:%S')
df['FECHA_HORA_TRANSACCION'] = df['FECHA_HORA_TRANSACCION'].dt.strftime('%Y-%m-%d')
## Obtener fechas unicas
fechas_unicas = df['FECHA_HORA_TRANSACCION'].unique().tolist()
print(fechas_unicas)
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
