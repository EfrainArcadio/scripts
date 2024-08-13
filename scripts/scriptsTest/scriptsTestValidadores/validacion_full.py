import os
import pandas as pd
import json
###############################################################################
#                           Carga de archivos                                 #
###############################################################################
## Definimos el mes con nombre
mes = "Julio"
## Definimos el mes con número
m = "07"
## Definimos el año
y = "2024"
## Nombres de archivos
name_file = "ort_2da qna Julio"

ruta_actual = os.path.dirname(__file__)
## Subir un nivel en el directorio
ruta_superior = os.path.dirname(ruta_actual)
ruta_sup_2 = os.path.dirname(ruta_superior)
ruta_sup_3 = os.path.dirname(ruta_sup_2)
ruta_sup_4 = os.path.dirname(ruta_sup_3)

## Ruta donde se leen y se guardan los archivos csv
## Ruta donde se leen los archivos json
path_dics = "data"
json_int = "integradores.json"
path_json_int = os.path.join(path_dics,json_int)
json_tot = "transacciones.json"
path_json_tot = os.path.join(path_dics,json_tot)
## Carga de archivo csv para analisis 
path_files = os.path.join(ruta_sup_4,f'dataFiles/validaciones/{y}/{m} {mes}')
file = f'{name_file}.csv'
path_file = os.path.join(path_files, file)
df = pd.read_csv(path_file, low_memory=False)
## Load JSON data integradores
with open(path_json_int) as f:
    data_int = json.load(f)
## Load JSON data integradores
with open(path_json_tot) as f:
    data_tot = json.load(f)
###############################################################################
#                           Modificacion de Formatos                          #
###############################################################################
## Modificar Lineas con base al JSON de Integradores
for company, replacements in data_int.items():
  for company_dict in replacements:
    for codigo,replacement in company_dict.items():
      df.replace({'LINEA':codigo}, replacement, inplace=True)
## Crear Lista de las LINEAS
lineas = set(df['LINEA'])  
# print(lineas)   
## Modificar formato de las fechas
df['TIPO_TRANSACCION'] = df['TIPO_TRANSACCION'].astype('str')
df['FECHA_HORA_TRANSACCION'] = pd.to_datetime(df['FECHA_HORA_TRANSACCION'],format="mixed")  
df['FECHA_HORA_TRANSACCION'] = df['FECHA_HORA_TRANSACCION'].dt.strftime('%Y-%m-%d')
# ## Obtener fechas unicas
fechas_unicas = sorted(set(df['FECHA_HORA_TRANSACCION']))
# print(fechas_unicas)
###############################################################################
#                 Definir Funciones utiles para el resumen                    #
###############################################################################
## funcion para realizar 
def process_transactions(tot, df_dato, dia):
  for transaccion in data_tot[tot]:
    print(transaccion)
    for codigo,details in transaccion.items():
      print(codigo)
      print(details)
      df_tot = df_dato[df_dato['TIPO_TRANSACCION'] == codigo]
      size = len(df_tot)
      dia.append({
        f'{details}': size
      })
    
def resumen(lista,df,datos):
  tr = [] 
  for dato in lista:
    df_dato = df[df[f'{datos}'] == dato]
    dia = []
    process_transactions('exitosas',df_dato,dia)
    process_transactions('erroneas',df_dato,dia)
    df_day = pd.DataFrame(dia)
    print(df_day)
    # tr.append({
    #   f'{datos}': dato,
    #     {dia[0]}: dia
    # })
    print(dato,dia)
  # return tr
        # print(dato,details,len(df_tot))
    # print(dato)
    # print(df_dato)
    
  #   process_transactions("exitosas",df_dato,tr,datos,dato)
  #   # process_transactions("erroneas",df_dato,tr,datos,dato)
  # df_res = pd.DataFrame(tr)
  # return df_res    
###############################################################################
#                            Uso de funciones                                 #
###############################################################################   
dato_lin = 'LINEA'    
dato_fec = 'FECHA_HORA_TRANSACCION'    
# print(resumen(lineas,df,dato_lin))
# print('')
res_c = pd.DataFrame(resumen(fechas_unicas,df,dato_fec))
print(res_c)