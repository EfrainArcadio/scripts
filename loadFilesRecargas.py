import pandas as pd
import os
import json
import psycopg2
y= "2024"
mes = "enero"
m = "01"
dia_in = 1
dia_fn = 32
##
tablaExtName = f"datos_ext_rre_{y}"
tablaTraName = f"datos_rre_{y}"
a = "-Transacciones.csv"
ae = "-Transacciones-extension.csv"
### DataInfo
pathStringInfo = f'dataFiles/recargas/{y}/{m} {mes}'
current_dir = os.getcwd()
parent_dir = os.path.dirname(current_dir)
pathInfo = os.path.join(parent_dir,pathStringInfo)

## Database
json_db = "db.json"
path_db = "config/"
path_json_db = os.path.join(path_db,json_db)
with open(path_json_db) as f:
    data_conn = json.load(f)
## coneaccion
connection = psycopg2.connect(**data_conn)
cursor = connection.cursor()

archivo_tr = [os.path.join(pathInfo, f"{y}{m}{d:02d}{a}") for d in range(dia_in, dia_fn)]
## Leer Archivos de Extencion para obtener la duracion de las transacciones
## Lista de nombres de archivo
archivo_ex = [os.path.join(pathInfo, f"{y}{m}{d:02d}{ae}") for d in range(dia_in, dia_fn)]

transacciones = []
## Bucle para insertar todos los archivos en el DataFrame transacciones
for transaccion in archivo_tr:
  df = pd.read_csv(transaccion)
  transacciones.append(df)
## Concatenación de documentos extraídos del arreglo transacciones y creando un solo DataFrame con información de toda la quincena
## Arreglo que se llenara con los archivos -Transacciones.csv
extenciones = []
## Bucle para insertar todos los archivos en el DataFrame extenciones
for extencion in archivo_ex:
  df = pd.read_csv(extencion)
  extenciones.append(df)   

def uploadExt(df, table_name, connection):
    try:
        default_date = f'{y}-{m}-{dia_fn-1} 00:00:00'
        df['START_DATE'] = df['START_DATE'].fillna(default_date)
        df['END_DATE'] = df['END_DATE'].fillna(default_date)
        columns = list(df.columns)
        unique_col = columns[0]
        lowercase_columns = [name.lower() for name in columns]
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
##
def uploadTra(df, table_name, connection):
    try:
        columns = list(df.columns)
        unique_col = columns[0]
        lowercase_columns = [name.lower() for name in columns]
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
##
df_ext = pd.concat(extenciones)  
df_tra = pd.concat(transacciones)
df_tra = df_tra[['ID_TRANSACCION_ORGANISMO','PROVIDER','TIPO_TARJETA','NUMERO_SERIE_HEX','FECHA_HORA_TRANSACCION','TIPO_EQUIPO','LOCATION_ID','TIPO_TRANSACCION','SALDO_ANTES_TRANSACCION','MONTO_TRANSACCION','SALDO_DESPUES_TRANSACCION','SAM_SERIAL_HEX_ULTIMA_RECARGA','SAM_SERIAL_HEX','CONTADOR_RECARGAS','EVENT_LOG','LOAD_LOG','MAC','SAM_COUNTER','ENVIRONMENT','ENVIRONMET_ISSUER_ID','CONTRACT','CONTRACT_TARIFF','CONTRACT_SALE_SAM','CONTRACT_RESTRICT_TIME','CONTRACT_VALIDITY_START_DATE','CONTRACT_VALIDITY_DURATION']]

if connection:
  print("Conexión exitosa")
  print(f"Llenando tabla {tablaExtName} ...")
  uploadExt(df_ext,tablaExtName,connection)
  print(f"Llenando tabla {tablaTraName} ...")
  uploadTra(df_tra,tablaTraName,connection)