import pandas as pd
import psycopg2
import os
## nombre de la tabla
mes_nombre = "Abril"
m = "04"
y = "2024"
tabla = f"datos_{y}"
root = 'Validadores'
ruta_trabajo = f'{root}/{y}/{m} {mes_nombre}'
name_file = f"Mpeso_1ra qna Abril.csv"
##
connection_string = {
    "host": "localhost",
    "database": "ORT",
    "user": "postgres",
    "password": "Hj2c#y8",
    "port": 5432,
}

connection = psycopg2.connect(**connection_string)

cursor = connection.cursor()
cursor.execute(f"SELECT id_transaccion_organismo,numero_serie_hex,fecha_hora_transaccion,monto_transaccion FROM datos_2024 WHERE  location_id='101801' AND fecha_hora_transaccion > '2024-04-15'")

# Ejemplo de procesamiento de resultados
resultados = cursor.fetchall()
newDatos = []
for fila in resultados:
    newDatos.append({
        'id_transaccion_organismo':fila[0],
        'numero_serie_hex': fila[1],
        'fecha_hora_transaccion': fila[2],
        'monto_transaccion': fila[3]
    })
    # print(fila)
connection.close()
# Assuming newDatos is a list of dictionaries
df_res = pd.DataFrame(newDatos)
print(df_res['fecha_hora_transaccion'])
