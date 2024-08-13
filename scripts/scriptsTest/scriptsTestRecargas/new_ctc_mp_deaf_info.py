import os
import pandas as pd
import psycopg2

##
## nombre de la tabla
mes = "Julio"
tabla = "contracargos"
root = 'data'
ruta_trabajo = f'{root}/Contracargos'
file_to_upload = f'contracargos_15_21_jul.csv'

# Parámetros de conexion
connection_string = {
    "host": "localhost",
    "database": "ORT",
    "user": "postgres",
    "password": "Hj2c#y8", ## ORT-PC
    # "password": "8YSH58pX3", ## PC-Home
    "port": 5432,
}

connection = psycopg2.connect(**connection_string)

# Función para cargar los datos del DataFrame a la tabla sin conversiones
def load_data_to_table_direct(df, table_name, connection):
    try:
        df = df.fillna(0) 
        # Obtener nombres de las columnas
        columns = list(df.columns)
        
        lowercase_columns = [name.lower() for name in columns]
        cursor = connection.cursor()
        datos = df.values

        querry = f"""
            INSERT INTO {table_name} ({", ".join(lowercase_columns)})
            VALUES ({", ".join(["%s"] * len(lowercase_columns))})
        """
        cursor.executemany(querry,datos.tolist())
        connection.commit()
        cursor.close()
        print(f"Se han cargado {df.shape[0]} filas a la tabla {table_name} del mes de {mes}")
    except Exception as e:
        print(e.__class__.__name__, ":", e)

ruta_archivo = os.path.join(ruta_trabajo,file_to_upload)
df = pd.read_csv(ruta_archivo)
df.rename(
  columns={
    'Fecha de Creación del contracargo (date_created)': 'date_created', 
    'Número de contracargo (chargeback_id)': 'chargeback_id', 
    'Estado del contracargo (status)': 'status', 
    'Detalle del estado del contracargo (status_detail)': 'status_detail',
    'Fecha límite para presentar la documentación (documentation_deadline)': 'documentation_deadline',
    'Monto del contracargo (amount)': 'amount',
    'Fecha de creación de la operación de Mercado Pago (operation_date_created)': 'operation_date_created',
    'Número de operación de Mercado Pago (operation_id)': 'operation_id',
    'Tipo de oeración (operation_type)': 'operation_type',
    'Código de referencia de la operación (operation_external_reference)': 'operation_external_reference',
    'Monto de la Operacion (operation_amount)': 'operation_amount',
    'Plataforma (operation_marketplace)': 'operation_marketplace',
    }, inplace=True)
## Recortar dataframe
df = df[[
  'date_created',
  'chargeback_id',
  'status',
  'status_detail',
  'documentation_deadline',
  'amount',
  'operation_date_created',
  'operation_id',
  'operation_type',
  'operation_external_reference',
  'operation_amount',
  'operation_marketplace'
  ]]

if connection:
    print("Conexión exitosa")
    print(f"Llenando tabla {tabla} ...")
    load_data_to_table_direct(df, tabla, connection)
