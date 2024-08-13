import pandas as pd
import psycopg2
import os

##
## nombre de la tabla
mes_nombre = "Julio"
di= '16'
df= '21'
m = "07"
##
y = "2024"
root = 'Recargas/data'
tabla = f"datos_rre_{y}"
ruta_trabajo = f'{root}/{y}/{m} {mes_nombre}'
# name_file = f"Full_{mes_nombre}.csv"
name_file = f"FULL_{mes_nombre}_{di}-{df}.csv"
# Parámetros de conexion

connection_string = {
    "host": "localhost",
    "database": "testing",
    "user": "postgres",
    "password": "Hj2c#y8",
    # "password": "8YSH58pX3", ## PC-Home
    "port": 5432,
}

connection = psycopg2.connect(**connection_string)

# Función para cargar los datos del DataFrame a la tabla sin conversiones
def load_data_to_table_direct(df, table_name, connection):
    try:
        columns = list(df.columns)
        lowercase_columns = [name.lower() for name in columns]
        cursor = connection.cursor()
        datos = df.values

        # Assuming a unique constraint on 'column1' and 'column2'
        querry = f"""
            INSERT INTO {table_name} ({", ".join(lowercase_columns)})
            VALUES ({", ".join(["%s"] * len(lowercase_columns))})
            ON CONFLICT (id_transaccion_organismo) DO NOTHING;
        """
        cursor.executemany(querry, datos.tolist())
        connection.commit()
        cursor.close()
        print(f"Se han cargado {len(df)} filas a la tabla {table_name} del {mes_nombre}")
    except Exception as e:
        print(e.__class__.__name__, ":", e)


ruta_archivo = os.path.join(ruta_trabajo,name_file)
df = pd.read_csv(ruta_archivo,encoding='latin-1',low_memory=False)

if connection:
    print("Conexión exitosa")
    print(f"Llenando tabla {tabla} ...")
    load_data_to_table_direct(df, tabla, connection)

