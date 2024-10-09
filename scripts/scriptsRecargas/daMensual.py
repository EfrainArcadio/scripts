import os
import pandas as pd
import psycopg2
import json
## Primer fecha compuesta de la consulta 
y= "2024"
mes = "Septiembre"
m = "08"
dia_in = "1"
dia_fn= "31"
###
## Obtener la ruta del directorio actual
ruta_actual = os.path.dirname(__file__)
## Subir un nivel en el directorio
ruta_superior = os.path.dirname(ruta_actual)
ruta_sup_2 = os.path.dirname(ruta_superior)
##
ruta_dumps = os.path.join(ruta_sup_2,f'public/recargas/meses/{y}/{m} {mes}')
file_d = f'recargas_desglosadas_rre_{y}_{m}'
path_file_d = os.path.join(ruta_dumps,f'{file_d}.csv')
def path_verify(path):
  if not os.path.exists(path):
    os.makedirs(path)
    print(f'Directorio Creado: {path}')
  else:
    print(f'El Directorio ya existe: {path}')
path_verify(ruta_dumps)
###
json_conn = os.path.join(ruta_sup_2,f'config/db.json')
with open(json_conn, 'r') as f_conn:
  data_conn = json.load(f_conn)
connection = psycopg2.connect(**data_conn)
## Consulta para extraer todos los datos del periodo de tiempo seleccionado
cursor = connection.cursor()
cursor.execute(f"SELECT * FROM datos_rre_{y} WHERE fecha_hora_transaccion >= '{y}-{m}-{dia_in} 00:00:00' AND fecha_hora_transaccion <= '{y}-{m}-{dia_fn} 23:59:59'")
transacciones = cursor.fetchall()
##
newDatos = []
for fila in transacciones:
  newDatos.append({
    'id_transaccion_organismo':fila[0],
    'provider': fila[1],
    'tipo_tarjeta': fila[2],
    'numero_serie_hex': fila[3],
    'fecha_hora_transaccion': fila[4],
    'tipo_equipo' : fila[5],
    'location_id' : fila[6],
    'tipo_transaccion' : fila[7],
    'saldo_antes_transaccion' : fila[8],
    'monto_transaccion' : fila[9],
    'saldo_despues_transaccion' : fila[10],
    'sam_serial_hex_ultima_recarga' : fila[11],
    'sam_serial_hex' : fila[12],
    'contador_recargas' : fila[13],
    'event_log' : fila[14],
    'load_log' : fila[15],
    'mac' : fila[16],
    'sam_counter' : fila[17],
    'environment' : fila[18],
    'environmet_issuer_id' : fila[19],
    'contract' : fila[20],
    'contract_tariff' : fila[21],
    'contract_sale_sam' : fila[22],
    'contract_restrict_time' : fila[23],
    'contract_validity_start_date' : fila[24],
    'contract_validity_duration' : fila[25]
  })
connection.close()

df_transacciones = pd.DataFrame(newDatos)

print(df_transacciones)

df_suc = df_transacciones[df_transacciones['tipo_transaccion'] == '0'].copy()
df_suc['fecha_hora_transaccion'] = df_suc['fecha_hora_transaccion'].dt.strftime('%Y-%m-%d')
fechas_unicas = sorted(set(df_suc['fecha_hora_transaccion']))

reemplazos = {
    '101800': 'Red Digital',
    '201A00': 'Red Comercios',
    '101801': 'APPCDMX',
 }
data_short = []
for fecha in fechas_unicas:
  df_dia = df_suc[df_suc['fecha_hora_transaccion'] == fecha]
  for codigo, reemplazo in reemplazos.items():
      df_dia.replace({'location_id':codigo}, reemplazo, inplace=True)
  tipos = df_dia['location_id'].unique().tolist()
  for tipo in tipos:
    df_red = df_dia[df_dia['location_id'] == tipo]
    recargas = len(df_red)
    data_short.append({
      'FECHA': fecha,
      'TIPO_RED': tipo,
      'RECARGAS': recargas
    })

df_short = pd.DataFrame(data_short)
df_short.to_csv(path_file_d,index=False)
print(df_short)