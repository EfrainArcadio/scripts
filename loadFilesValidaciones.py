import pandas as pd
import os
import json
import psycopg2
## Variables 
y= "2024"
mes = "Enero"
m = "01"
## Nombre corto del archivo sin extencion .csv
name_file = 'Validaciones 2da qna enero 2024'
## Nombre dinamico de la tabla a la que se le cargaran los datos nuevos
tablaExtName = f"datos_val_{y}"
### definicion y creacion de Rutas de trabajo
pathStringInfo = f'dataFiles/validaciones/{y}/{m} {mes}'
current_dir = os.getcwd()
parent_dir = os.path.dirname(current_dir)
##
pathInfo = os.path.join(parent_dir,pathStringInfo)
## nombre dinamico del archivo utilizado para subir informacion a la base de datos con ext .csv
file_to_upload = f'{name_file}.csv'
## Lectura del archivo y transformacion a DataFrame
archivo = os.path.join(pathInfo, file_to_upload) 
df = pd.read_csv(archivo, low_memory=False, encoding='latin-1')
### 
def convertir_fecha(fecha):
  try:
    # Intentar convertir directamente al formato deseado
    return pd.to_datetime(fecha, format='%Y-%m-%d %H:%M:%S')
  except ValueError:
    # Si falla, intentar inferir el formato
    return pd.to_datetime(fecha,  format='%Y-%m-%d %H:%M:%S')
## End convertir_fecha
def impute_dates(df):
  default_date = f'2021-08-13 00:00:00'
  df['CONTRACT_VALIDITY_START_DATE'] = df['CONTRACT_VALIDITY_START_DATE'].fillna(default_date)
## end inpute_dates
impute_dates(df)
## Funcion de Subida de Archivos
def uploadTra(df, table_name, connection):
  try:
    columns = list(df.columns)
    unique_col = columns[0]
    lowercase_columns = [ name.lower() for name in columns ]
    cursor = connection.cursor()
    datos = df.values
    querry = f"""
        INSERT INTO {table_name} ({", ".join(lowercase_columns)})
        VALUES ({", ".join(["%s"] * len(lowercase_columns))})
        ON CONFLICT ({unique_col}) DO NOTHING;
    """
    cursor.executemany(querry, datos.tolist())
    connection.commit()
    cursor.close()
    print(f"Se han cargado {len(df)} filas a la tabla {table_name} del {mes}")
  except Exception as e:
    print(e.__class__.__name__, ":", e)
## end function uploadTra
## Database 
json_db = "db.json" 
path_db = "config/"
path_json_db = os.path.join(path_db,json_db)
# print(path_json_db) 
with open(path_json_db) as f:
  data_conn = json.load(f)
## coneccion
connection = psycopg2.connect(**data_conn)
cursor = connection.cursor()
## Se recorta el DataFrame a solo las columnas que utiliza este sistema
df_short = df[['ID_TRANSACCION_ORGANISMO','PROVIDER','TIPO_TARJETA','NUMERO_SERIE_HEX','FECHA_HORA_TRANSACCION','LINEA','ESTACION','AUTOBUS','RUTA','TIPO_EQUIPO','LOCATION_ID','TIPO_TRANSACCION','SALDO_ANTES_TRANSACCION','MONTO_TRANSACCION','SALDO_DESPUES_TRANSACCION','SAM_SERIAL_HEX_ULTIMA_RECARGA','SAM_SERIAL_HEX','CONTADOR_VALIDACIONES','EVENT_LOG','PURCHASE_LOG','MAC','ENVIRONMENT','ENVIRONMENT_ISSUER_ID','CONTRACT','CONTRACT_TARIFF','CONTRACT_SALE_SAM','CONTRACT_VALIDITY_START_DATE','CONTRACT_VALIDITY_DURATION']].copy()

# # Aplicar la función al DataFrame
df_short['FECHA_HORA_TRANSACCION'] = df_short['FECHA_HORA_TRANSACCION'].apply(convertir_fecha)
df_short['CONTRACT_VALIDITY_START_DATE'] = df_short['CONTRACT_VALIDITY_START_DATE'].apply(convertir_fecha)

## Ejecucion de las inserciones
if connection:
  print("Conexión exitosa")
  print(f"Llenando tabla {tablaExtName} ...")
  uploadTra(df_short,tablaExtName,connection)





















