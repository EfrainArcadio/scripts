import os
import pandas as pd
import psycopg2
import json
from scipy import stats
y = '2024'

mi = '01'
di = '01'
## Segunda fecha compuesta de la consulta
mf = '11'
df = '24'

sp = 'SMARTPOS1494575822'

file = f'data-{sp}-ext'
file_ful = f'{file}.csv'

def path_verify(path):
  if not os.path.exists(path):
    os.makedirs(path)
    print(f'Directorio Creado: {path}')
  else:
    print(f'El Directorio ya existe: {path}')

ruta_actual = os.getcwd()
parent_dir = os.path.dirname(ruta_actual)
datos = 'dataFiles/recargas/2024'
path_info = os.path.join(parent_dir,datos)
df_files = os.path.join(path_info,file_ful)
path_verify(path_info)
##
json_conn = os.path.join(ruta_actual,f'config/db.json')
with open(json_conn, 'r') as f_conn:
  data_conn = json.load(f_conn)
###
connection = psycopg2.connect(**data_conn)
cursor = connection.cursor()
cursor.execute(f"SELECT * FROM datos_rre_{y} WHERE fecha_hora_transaccion >= '{y}-{mi}-{di} 00:00:00' AND fecha_hora_transaccion <= '{y}-{mf}-{df} 23:59:59' AND location_id = '201A00'")
transacciones = cursor.fetchall()
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
df = pd.read_csv(df_files,low_memory=False, encoding='latin-1')
idto = df['id_transaccion_organismo'].to_list()
# print(len(idto))
df_ori = pd.DataFrame(newDatos)
df_ori_val = df_ori[df_ori['tipo_transaccion'] == '0']

df_pos = []
for idt in idto:
  df_idt = df_ori_val[df_ori_val['id_transaccion_organismo'] == f'{idt}']
  if(len(df_idt) != 0 ):
    # print(idt)
    # print(len(df_idt))
    df_pos.append(df_idt)

df_post = pd.concat(df_pos)
file_resumen = f'datos_tr_{sp}.csv'
path_resumen = os.path.join(path_info,file_resumen)
df_post.to_csv(path_resumen, index=False)

df_post['fecha_hora_transaccion'] = df_post['fecha_hora_transaccion'].dt.strftime('%Y-%m-%d')
fechas_unicas = sorted(set(df_post['fecha_hora_transaccion']))
df_post['monto_transaccion'] = df_post['monto_transaccion'] / 100
# print(fechas_unicas)
resumen = []
for fecha in fechas_unicas:
  df_dia = df_post[df_post['fecha_hora_transaccion'] == fecha]
  # print(df_dia['monto_transaccion'].value_counts())
  mto_rec_dia = sum(df_dia['monto_transaccion'])
 
  resumen.append({
        'FECHA': fecha,
        'Transacciones': len(df_dia),
        'Monto': mto_rec_dia,
        'Minima': min(df_dia['monto_transaccion']),
        'Maxima': max(df_dia['monto_transaccion']),
        'Moda': stats.mode(df_dia['monto_transaccion'])
    })

res = pd.DataFrame(resumen)
file_resumen = f'resumen_{sp}.csv'
path_resumen = os.path.join(path_info,file_resumen)
res.to_csv(path_resumen, index=False)

print(res)