import pandas as pd
import os
import json
import psycopg2
import numpy as np
###############################################################################
#                           Carga de archivos                                 #
###############################################################################
## Variables 
y= "2024"
mes = "Abril"
m = "04"
## Nombre corto del archivo sin extencion .csv
name_file = 'Mpeso_1ra qna Abril'
vt = 'v1'

###############################################################################
#                           Carga de archivos                                 #
###############################################################################
def impute_dates(df):
  default_date = f'13-08-2023 00:00:00'
  df['CONTRACT_VALIDITY_START_DATE'] = df['CONTRACT_VALIDITY_START_DATE'].fillna(default_date)
## end inpute_dates
def convertir_fecha_estandar(fecha, formatos_posibles=['%d-%m-%Y %H:%M','%d/%m/%Y %H:%M','%d-%m-%Y %H:%M:%S']):
  for formato in formatos_posibles:
    try:
      fecha_datetime = pd.to_datetime(fecha, format=formato)
      # Agregar segundos si no están presentes
      return fecha_datetime.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
      continue  # Pasar al siguiente formato si falla
  # Si ninguno de los formatos funciona, intentar inferir
  try:
    return pd.to_datetime(fecha).strftime('%Y-%m-%d %H:%M:%S')
  except ValueError:
    print(f"No se pudo convertir la fecha '{fecha}' a ningún formato válido.")
    return None
## End convertir_fecha
def format_info(df,dtypes_a):
  print('Llenando CONTRACT_VALIDITY_START_DATE con fecha default')
  impute_dates(df)
  print('Realizando conversión de FECHA_HORA_TRANSACCION')
  df['FECHA_HORA_TRANSACCION'] = df['FECHA_HORA_TRANSACCION'].apply(convertir_fecha_estandar)
  print('Realizando conversión de CONTRACT_VALIDITY_START_DATE')  
  df['CONTRACT_VALIDITY_START_DATE'] = df['CONTRACT_VALIDITY_START_DATE'].apply(convertir_fecha_estandar)
  print('Rellenando campos vacios con 0')  
  df.fillna(0, inplace=True)
  # df['ID_TRANSACCION_ORGANISMO'] = df['ID_TRANSACCION_ORGANISMO'].str.replace('.', '')
  dtypes = {col['column']: col['type'] for col in dtypes_a}
  print('Formateando Tipos de Dato a Columna')  
  df = df.astype(dtypes)
  return df
## end format_info
def uploadTra(df, table_name, connection):
  try:
    columns = list(df.columns)
    id_tr = columns[0]
    lowercase_columns = [ name.lower() for name in columns ]
    cursor = connection.cursor()
    datos = df.values
    querry = f"""
      INSERT INTO {table_name} ({", ".join(lowercase_columns)})
      VALUES ({", ".join(["%s"] * len(lowercase_columns))})
      ON CONFLICT ({id_tr}) DO NOTHING;
    """
    cursor.executemany(querry, datos.tolist())
    connection.commit()
    # Identificar filas que no se insertaron
    print(f"Se ha cargado {name_file} a la tabla {table_name} del {mes}")
  except Exception as e:
    print(e.__class__.__name__, ":", e)
  finally:
    cursor.close()
## end function uploadTra

###############################################################################
#                           Carga de archivos                                 #
###############################################################################
## Nombre dinamico de la tabla a la que se le cargaran los datos nuevos
tablaExtName = f"datos_val_{y}_{vt}"
file_to_upload = f'{name_file}.csv'
json_db = "db.json" 
json_data = "modeloDatos.json" 
## definicion y creacion de Rutas de trabajo
current_dir = os.getcwd()
parent_dir = os.path.dirname(current_dir)
pathStringInfo = f'dataFiles/validaciones/{y}/{m} {mes}'
path_db = "config/"
path_data = "data/"
##
pathInfo = os.path.join(parent_dir,pathStringInfo)
archivo = os.path.join(pathInfo, file_to_upload) 
path_json_db = os.path.join(path_db,json_db)
path_json_data = os.path.join(path_data,json_data)
##
df = pd.read_csv(archivo, low_memory=False, encoding='latin-1')
##
with open(path_json_db) as f:
  data_conn = json.load(f)
##
with open(path_json_data) as f:
  data_types = json.load(f)

###############################################################################
#                           Carga de archivos                                 #
###############################################################################
## coneccion
connection = psycopg2.connect(**data_conn)
if connection:
#   print("La Conexión con PostgreSQL es exitosa")
#   cursor = connection.cursor()
#   cursor.execute(f"SELECT  id_transaccion_organismo,numero_serie_hex,fecha_hora_transaccion,location_id FROM {tablaExtName}")
#   transacciones = cursor.fetchall()
# list_tr = []
# for fila in transacciones:
#   list_tr.append({ 
#     'id_transaccion_organismo':fila[0],
#     'numero_serie_hex': fila[1],
#     'location_id': fila[3], 
#     })

# df_db = pd.DataFrame(list_tr)

# ids_db = df_db['id_transaccion_organismo'].unique().tolist()
# # print(ids_db)
# print('ID_TR_EXIST: ',len(list_tr))

# array_up_list = np.array(up_list)
# array_list_tr = np.array(ids_db)
# list_no_val = array_up_list[np.isin(array_up_list, array_list_tr)].tolist()
# print('No Valids',len(list_no_val))

# array_b = []
# for element in list_no_val:
#   df_inv = df[df['ID_TRANSACCION_ORGANISMO'] == element]
#   df_db_short = df_db[df_db['id_transaccion_organismo'] == element]
#   ##
#   df_inv_array = df_inv[['ID_TRANSACCION_ORGANISMO','NUMERO_SERIE_HEX', 'LOCATION_ID']].values
#   df_db_short_array = df_db_short[['id_transaccion_organismo','numero_serie_hex', 'location_id']].values
#   listd = df_inv_array[np.isin(df_inv_array,df_db_short_array)]
#   if any(listd):
#     array_b.append(element)

# print('Eliminate',len(array_b))
# # print(len(lista_bad))
# lista_final = list(filter(lambda x: x not in list(array_b), up_list))
# print('Finales',len(lista_final))
# # print(f'No validas no analis: {len(list_no_val)}')


# valids = []
# for id_val in lista_final:
#   val_valid = df[df['ID_TRANSACCION_ORGANISMO'] == id_val ]
#   valids.append(val_valid)

# print(valids)
# df_valids = pd.concat(valids)
  df_formated =  format_info(df,data_types)
  print('Recortando información')
  # print(len(df_formated['ID_TRANSACCION_ORGANISMO'].unique().tolist()))

  df_short = df_formated[['ID_TRANSACCION_ORGANISMO','PROVIDER','TIPO_TARJETA','NUMERO_SERIE_HEX','FECHA_HORA_TRANSACCION','LINEA','ESTACION','AUTOBUS','RUTA','TIPO_EQUIPO','LOCATION_ID','TIPO_TRANSACCION','SALDO_ANTES_TRANSACCION','MONTO_TRANSACCION','SALDO_DESPUES_TRANSACCION','SAM_SERIAL_HEX_ULTIMA_RECARGA','SAM_SERIAL_HEX','CONTADOR_VALIDACIONES','EVENT_LOG','PURCHASE_LOG','MAC','ENVIRONMENT','ENVIRONMENT_ISSUER_ID','CONTRACT','CONTRACT_TARIFF','CONTRACT_SALE_SAM','CONTRACT_VALIDITY_START_DATE','CONTRACT_VALIDITY_DURATION']].copy()

  uploadTra(df_short,tablaExtName,connection)

  # print(df_short)






















