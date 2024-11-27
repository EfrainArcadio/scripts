import pandas as pd
import os
import json
import psycopg2
###############################################################################
#                           Carga de archivos                                 #
###############################################################################
## Variables 
y= "2024"
mes = "Julio"
m = "07"
# p= "1ra"
p= "2da"
## Nombre corto del archivo sin extencion .csv
name_file = f'{p}_qna_{mes}_{y}_v2'
vt = 'v2'
###############################################################################
#                           Carga de archivos                                 #
###############################################################################

def format_info(df,dtypes_a):
  df.fillna(0, inplace=True)
  dtypes = {col['column']: col['type'] for col in dtypes_a}
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
df_formated =  format_info(df,data_types)
## Se recorta el DataFrame a solo las columnas que utiliza este sistema
df_short = df_formated[['ID_TRANSACCION_ORGANISMO','PROVIDER','TIPO_TARJETA','NUMERO_SERIE_HEX','FECHA_HORA_TRANSACCION','LINEA','ESTACION','AUTOBUS','RUTA','TIPO_EQUIPO','LOCATION_ID','TIPO_TRANSACCION','SALDO_ANTES_TRANSACCION','MONTO_TRANSACCION','SALDO_DESPUES_TRANSACCION','SAM_SERIAL_HEX_ULTIMA_RECARGA','SAM_SERIAL_HEX','CONTADOR_VALIDACIONES','EVENT_LOG','PURCHASE_LOG','MAC','ENVIRONMENT','ENVIRONMENT_ISSUER_ID','CONTRACT','CONTRACT_TARIFF','CONTRACT_SALE_SAM','CONTRACT_VALIDITY_START_DATE','CONTRACT_VALIDITY_DURATION']].copy()
## Ejecucion de las inserciones
if connection:
  print("Conexión exitosa")
  print(f"Llenando tabla {tablaExtName} ...")
  uploadTra(df_short,tablaExtName,connection)





















