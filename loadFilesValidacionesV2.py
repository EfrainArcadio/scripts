import pandas as pd
import os
import json
import psycopg2
###############################################################################
#                           Carga de archivos                                 #
###############################################################################
## Variables 
y= "2024"
mes = "Enero"
m = "01"
## Nombre corto del archivo sin extencion .csv
name_file = '2da_qna_Enero_2024_v2'
vt = 'v2'
###############################################################################
#                           Carga de archivos                                 #
###############################################################################
# def impute_dates(df):
#   default_date = f'13-08-2021 00:00:00'
#   df['CONTRACT_VALIDITY_START_DATE'] = df['CONTRACT_VALIDITY_START_DATE'].fillna(default_date)
# ## end inpute_dates

def format_info(df,dtypes_a):
  # impute_dates(df)
  # df['FECHA_HORA_TRANSACCION'] = df['FECHA_HORA_TRANSACCION'].apply(convertir_fecha_estandar)
  # df['CONTRACT_VALIDITY_START_DATE'] = df['CONTRACT_VALIDITY_START_DATE'].apply(convertir_fecha_estandar)
  df.fillna(0, inplace=True)
  # df['ID_TRANSACCION_ORGANISMO'] = df['ID_TRANSACCION_ORGANISMO'].str.replace('.', '')
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
        ON CONFLICT ({id_tr}) DO NOTHING
         RETURNING *;
    """
    cursor.executemany(querry, datos.tolist())
    print(cursor.fetchall())
    # results = cursor.fetchall()
    # filas_no_insertadas = df[~df[id_tr].isin([row[0] for row in results])]
    # print(f"Filas no insertadas debido a conflictos: {filas_no_insertadas}")
    connection.commit()
    cursor.close()
  except Exception as e:
    print(e.__class__.__name__, ":", e)
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
# print('Vacios:',{df['ID_TRANSACCION_ORGANISMO'].isnull().sum()}) 
# print('Tamaño archivo:',{df.info()})
up_list = df['ID_TRANSACCION_ORGANISMO'].unique().tolist()
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
cursor = connection.cursor()
##
cursor.execute(f"SELECT DISTINCT id_transaccion_organismo FROM {tablaExtName}")
transacciones = cursor.fetchall()
list_tr = []
for idt in transacciones:
  list_tr.append(idt[0])

list_val = ( e for e in up_list if e not in list_tr)

vals = []
for val in list_val:
  print(val)
  vals.append(val)
print(vals)
###

df_formated =  format_info(df,data_types)
# print('Vacios:',{df_formated['ID_TRANSACCION_ORGANISMO'].isnull().sum()}) 

## Se recorta el DataFrame a solo las columnas que utiliza este sistema
df_short = df_formated[['ID_TRANSACCION_ORGANISMO','PROVIDER','TIPO_TARJETA','NUMERO_SERIE_HEX','FECHA_HORA_TRANSACCION','LINEA','ESTACION','AUTOBUS','RUTA','TIPO_EQUIPO','LOCATION_ID','TIPO_TRANSACCION','SALDO_ANTES_TRANSACCION','MONTO_TRANSACCION','SALDO_DESPUES_TRANSACCION','SAM_SERIAL_HEX_ULTIMA_RECARGA','SAM_SERIAL_HEX','CONTADOR_VALIDACIONES','EVENT_LOG','PURCHASE_LOG','MAC','ENVIRONMENT','ENVIRONMENT_ISSUER_ID','CONTRACT','CONTRACT_TARIFF','CONTRACT_SALE_SAM','CONTRACT_VALIDITY_START_DATE','CONTRACT_VALIDITY_DURATION']].copy()
# print('Tamaño archivo:',{df_short.info()}) 
## Ejecucion de las inserciones
if connection:
  print("Conexión exitosa")
  print(f"Llenando tabla {tablaExtName} ...")
  uploadTra(df_short,tablaExtName,connection)





















