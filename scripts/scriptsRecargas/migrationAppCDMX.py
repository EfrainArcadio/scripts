### importaciones
import os
import pandas as pd
import psycopg2
import json

## Fechas

# Periodo 1     

### Rutas
## Obtener la ruta del directorio actual
ruta_actual = os.path.dirname(__file__)
## Subir un nivel en el directorio
ruta_superior = os.path.dirname(ruta_actual)
ruta_sup_2 = os.path.dirname(ruta_superior)
ruta_cats = os.path.join(ruta_actual,'data/')

def path_verify(path):
  if not os.path.exists(path):
    os.makedirs(path)
    print(f'Directorio Creado: {path}')
  else:
    print(f'El Directorio ya existe: {path}')

### Diccionarios

json_conn = os.path.join(ruta_sup_2,f'config/db.json')
with open(json_conn, 'r') as f_conn:
  data_conn = json.load(f_conn)

### Consultar base para extraer 
## Conexion
connection = psycopg2.connect(**data_conn)
##
tu1 = connection.cursor()
tu1.execute("SELECT DISTINCT numero_serie_hex FROM datos_rre_2023 WHERE fecha_hora_transaccion >= '2023-11-01 00:00:00' AND fecha_hora_transaccion <= '2023-12-31 23:59:59'  AND tipo_transaccion = '0' AND location_id IN ('101800','201A00') AND tipo_transaccion IS NOT NULL UNION SELECT DISTINCT numero_serie_hex FROM datos_rre_2024 WHERE fecha_hora_transaccion >= '2024-01-01 00:00:00' AND fecha_hora_transaccion <= '2024-10-31 23:59:59' AND tipo_transaccion = '0' AND location_id IN ('101800','201A00') AND tipo_transaccion IS NOT NULL ")
tup1 = tu1.fetchall()

tu2 = connection.cursor()
tu2.execute("SELECT DISTINCT numero_serie_hex FROM datos_rre_2024 WHERE fecha_hora_transaccion >= '2024-07-01 00:00:00' AND fecha_hora_transaccion <= '2024-10-31 23:59:59' AND tipo_transaccion = '0' AND location_id IN ('101801') AND tipo_transaccion IS NOT NULL")
tup2 = tu2.fetchall()
##
connection.close()
## 
cards_pe1 = []
for card in tup1:
  cards_pe1.append(card[0])
# print(cards_pe1)
print(f'Total de Tarjetas unicas RRD y RRF P1: {len(cards_pe1)}')
##
cards_pe2 = []
for card in tup2:
  cards_pe2.append(card[0])
# print(cards_pe2)
  
print(f'Total de Tarjetas unicas AppCDMX P2: {len(cards_pe2)}')

lista_app_in_rre = (  e for e in cards_pe2 if e in cards_pe1 )
lista_app_in_rre = list(lista_app_in_rre)
print(f'Total de tarjetas de App en RRD y RRF {len(lista_app_in_rre)}')

lista_app_not_in_rre = (  e for e in cards_pe2 if not e in cards_pe1 )
lista_app_not_in_rre = list(lista_app_not_in_rre)
print(f'Total de tarjetas Solo de App {len(lista_app_not_in_rre)}')
  


print('Proceso Finalizado con Exito!! Wenas Noches :v')