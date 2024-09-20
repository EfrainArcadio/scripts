import pandas as pd
import os
## Variables 
y= "2024"
mes = "Enero"
m = "01"
## Nombre corto del archivo sin extencion .csv
name_file = 'Validaciones de la 1ra qna de enero 2024'
## Nombre dinamico de la tabla a la que se le cargaran los datos nuevos
tablaExtName = f"datos_val_{y}"
### definicion y creacion de Rutas de trabajo
pathStringInfo = f'dataFiles/validaciones/{y}/{m} {mes}'
current_dir = os.getcwd()
parent_dir = os.path.dirname(current_dir)
pathInfo = os.path.join(parent_dir,pathStringInfo)
## nombre dinamico del archivo utilizado para subir informacion a la base de datos con ext .csv
file_to_upload = f'{name_file}.csv'
## Lectura del archivo y transformacion a DataFrame
archivo = os.path.join(pathInfo, file_to_upload) 
df = pd.read_csv(archivo, low_memory=False, encoding='latin-1')
## Funcion de Subida de Archivos
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


print(len(df.columns))

df_short = df[['ID_TRANSACCION_ORGANISMO','PROVIDER','TIPO_TARJETA','NUMERO_SERIE_HEX','FECHA_HORA_TRANSACCION','LINEA','ESTACION','AUTOBUS','RUTA','TIPO_EQUIPO','LOCATION_ID','TIPO_TRANSACCION','SALDO_ANTES_TRANSACCION','MONTO_TRANSACCION','SALDO_DESPUES_TRANSACCION','SAM_SERIAL_HEX_ULTIMA_RECARGA','SAM_SERIAL_HEX','CONTADOR_VALIDACIONES','EVENT_LOG','PURCHASE_LOG','MAC','ENVIRONMENT','ENVIRONMENT_ISSUER_ID','CONTRACT','CONTRACT_TARIFF','CONTRACT_SALE_SAM','CONTRACT_VALIDITY_START_DATE','CONTRACT_VALIDITY_DURATION']]

print(len(df_short.columns))

























