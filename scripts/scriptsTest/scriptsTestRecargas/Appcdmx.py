import os
import pandas as pd
import json
import numpy as np
## Asignar el nombre del mes con texto
mes_nombre = "Febrero"
## Modificar el contenido de m = "mes" * Para los meses que anteriores a octubre ocupar la sintaxis 09 = Septiembre 08 = Agosto
## Modificar el contenido de Y = "Año" 2023 / 2024 / 2025 
m = "02"
y = "2024"
## Ruta de Archivos ADIP
ruta_adip = f"Transacciones/ADIP/{m} {mes_nombre}"
## Ruta de Archivos MP
ruta_mp = f"Transacciones/{y}/{m} {mes_nombre}"
## Ruta JSON Contracargos
ruta_json_ctc = "Transacciones/ADIP"
## Archivo de UsuarioTarjetas ADIP
archivo_tarjetas = 'card_numbers_feb_24.csv'
file_card = os.path.join(ruta_adip, archivo_tarjetas)
df_cards = pd.read_csv(file_card, low_memory=False, encoding='latin-1')
## Convertir card_number a mayusculas
df_cards['card_number'] = df_cards['card_number'].str.upper()
## Rellenar con '0' a la izquierda para acompletar las 16 posiciones
mask = df_cards['card_number'].str.len() != 16
## 

df_cards['card_number'] = np.where(mask, df_cards['card_number'].str.zfill(16), df_cards['card_number'])

## 

## Archivo de transacciones de ADIP
archivo_datos = 'mp_info_01-29_02_2024.csv'
file_info = os.path.join(ruta_adip, archivo_datos)
df_adip = pd.read_csv(file_info, low_memory=False, encoding='latin-1')
## Convertir campo id_transaccion a string
df_adip['id_transacion'] = df_adip['id_transacion'].astype(str)
## Eliminar el .0 que trae por defecto el campo id_transaccion, Ej. '71588127332.0' to '71588127332'
df_adip['id_transacion'] = df_adip['id_transacion'].apply(lambda x: x[:-2])
## Cambiar a mayusculas los hexid de las tarjetas
## Crear una lista de los id_transaccion del df_adip
transactions = df_adip['id_transacion'].to_list()
## Archivo de transacciones mensual de MP
archivo_mp = f"Full_{mes_nombre}.csv"
file_mp = os.path.join(ruta_mp, archivo_mp)
df_mp = pd.read_csv(file_mp, low_memory=False, encoding='latin-1')
# Lista de id_transaccion de contracargos aprovados
nArchivo_json_idTr = 'id_transacion_ctc_appcdmx.json'
json_id_tr = os.path.join(ruta_json_ctc,nArchivo_json_idTr)
with open(json_id_tr, 'r') as archivo_json:
  contracargoId = json.load(archivo_json)
## Convierte cada elemento del JSON a String
string_ctc = [str(number) for number in contracargoId]
## Crear lista de coincidencias satisfactorias entre la lista de contracargos aprovados y los id_transaccion de adip
lista_id_ctc_successfull = [e for e in string_ctc if e in transactions]
## Crear lista de coincidencias insatisfactorias entre la lista de contracargos aprovados y los id_transaccion de adip
lista_id_ctc_unsuccessfull = [e for e in string_ctc if e not in transactions]
## Crear archivo JSON de las insatisfactorias para analizar a futuro
print('Creando archvio JSON id_transaccion not found in id_transacion_ctc_appcdmx.json')
archivo_unsuccessfull = f'id_transaction_ctc_pendientes_rev_{mes_nombre}.json'
with open(f'{ruta_adip}/{archivo_unsuccessfull}', 'w') as archivo:
    json.dump(lista_id_ctc_unsuccessfull, archivo,indent=2)

## Inicio de Analisis 
print("Iniciando Analisis")
print("Ajuste de datos...")
## Convertimos el id_transacion en tipo cadena y elimina los decimales en caso de generar
## Lista para transacciones
transactions = []
## Obtener la lista de las transacciones que fueron identificadas como contracargo
for ctc_id in lista_id_ctc_successfull:
  # Filtrar DataFrame por ID de transacción
  tr = df_adip[df_adip['id_transacion'] == ctc_id]
  # Agregar transaccion a la lista
  transactions.append(tr)
## Crear el dataframe de Transaciones de contracargo
trs = pd.concat(transactions, ignore_index=True)
## Obtener los id de usuarios de la lista de transacciones con contracargo
usuario_tra_uni = set(trs['user_id'])
## Almacenar Usuarios y sus tarjetas segun la base de datos de ADIP
user_c = []
## Buscar los Usuarios y crear un DF para obtener sus tarjetas
for us in usuario_tra_uni:
    id_us = df_cards[df_cards['user_id'] == us]
    user_c.append(id_us)
## Crear DF de Users con sus cards
usuarios_ctc = pd.concat(user_c, ignore_index=True)
## Extraemos las tarjetas unicas de los usuarios que realizaron contracargo
cards_unique = set(usuarios_ctc['card_number'])

# Create a dictionary to store user IDs and their associated card numbers
print("Creando JSON usuario-tarjetas...")
user_cards = {}
##
for user_id, card_number in zip(usuarios_ctc['user_id'], usuarios_ctc['card_number']):
  if user_id not in user_cards:
    user_cards[user_id] = set()  # Inicializa un conjunto vacío
  user_cards[user_id].add(card_number.zfill(16))
# Convierte los conjuntos a listas antes de la serialización
for user_id, card_numbers in user_cards.items():
  user_cards[user_id] = list(card_numbers)
#
with open(f'{ruta_adip}/usuario-tarjetas-{mes_nombre}.json', 'w') as archivo:
  json.dump(user_cards, archivo,indent=2)
##
print("Creando JSON tarjeta-usuarios...")
card_users = {}

for user_id, card_number in zip(df_cards['user_id'], usuarios_ctc['card_number']):
  if card_number not in card_users:
      card_users[card_number] = set()  
  card_users[card_number].add(user_id)  
# print(card_users)
for card_number,user_id in card_users.items():
  card_users[card_number] = list(user_id)


with open(f'{ruta_adip}/tarjeta-usuarios-{mes_nombre}.json', 'w') as archivo:
  json.dump(card_users, archivo,indent=2)

trs['fecha_recarga_segundos'] = pd.to_datetime(trs['fecha_recarga'], format='%d/%m/%Y %H:%M').astype(np.int64) // 10**9
trs['fecha_recarga_segundos_mas_hora'] = trs['fecha_recarga_segundos'] + 3600

adip = trs[['id_transacion', 'user_id', 'monto_recarga', 'fecha_recarga', 'email', 'fecha_recarga_segundos_mas_hora']]


transa_mp = []

for card in cards_unique:
    df_mp['TIPO_TRANSACCION'] = df_mp['TIPO_TRANSACCION'].astype('str')
    df_filtro = df_mp[df_mp['TIPO_TRANSACCION'] == '0'].copy()
    df_appcdmx = df_filtro[df_filtro['LOCATION_ID'] == '101801']
    tra_mp = df_appcdmx[df_appcdmx['NUMERO_SERIE_HEX'] == str(card.upper())]
    transa_mp.append(tra_mp)
        
fnal = pd.concat(transa_mp,ignore_index=True)
# print(fnal)
fnal['FECHA_HORA_TRANSACCION_SEG'] = pd.to_datetime(fnal['FECHA_HORA_TRANSACCION']).astype(np.int64) // 10**9
mp = fnal[['ID_TRANSACCION_ORGANISMO', 'NUMERO_SERIE_HEX', 'FECHA_HORA_TRANSACCION', 'MONTO_TRANSACCION', 'FECHA_HORA_TRANSACCION_SEG']]
mp['MONTO_TRANSACCION'] = mp['MONTO_TRANSACCION'] / 100
# Initialize empty dictionary for results
resultados = []

# Define a threshold for considering a match
max_diferencia_permitida = 2400  # Adjust this value as needed
relacion_fechas = {}
# Main loop to iterate through 'fsg' dates
for i, fecha_fsg in enumerate(adip['fecha_recarga_segundos_mas_hora']):
  # Reset variables for each 'fsg' date
  minima_diferencia = float('inf')
  indice_minimo = None
  monto_fsg = adip['monto_recarga'].iloc[i]  # 
  id_usuario_fsg = adip['user_id'].iloc[i]  # 
  id_tr = adip['id_transacion'].iloc[i] 
  lista_diferencias = {}
  # Nested loop to compare with 'fmp' dates
  for tr_org,fecha_fmp, monto_fmp,card_hex,fecha_mp in zip(mp['ID_TRANSACCION_ORGANISMO'],mp['FECHA_HORA_TRANSACCION_SEG'], mp['MONTO_TRANSACCION'],mp['NUMERO_SERIE_HEX'], mp['FECHA_HORA_TRANSACCION']):
    diferencia_actual = fecha_fmp - fecha_fsg
    if(diferencia_actual > 0):
      if(diferencia_actual <= max_diferencia_permitida):
        if monto_fsg == monto_fmp:
          if diferencia_actual < minima_diferencia:
            lista_diferencias= (id_tr,tr_org,fecha_fsg,fecha_fmp,diferencia_actual,monto_fsg,monto_fmp)
            minima_diferencia = diferencia_actual
            indice_minimo = i
            t_card = card_hex.zfill(16)
            id_user = adip['user_id'].iloc[i]
            userCardsFn = user_cards[id_user]
            email = adip['email'].iloc[i] 
            fecha_adp = adip['fecha_recarga'].iloc[i] 
            resultados.append( {
                        'id_transacion':id_tr,
                        'monto_fsg': monto_fsg,
                        'monto_fmp': monto_fmp,
                        'id_transacion_org': tr_org,
                        'card': t_card,
                        'email':email,
                        'fecha_adp': fecha_adp,
                        'fecha_mp': fecha_mp,
                        'userCards': userCardsFn,
                        'diferencia': diferencia_actual,
                        'fecha_fsg': fecha_fsg,
                        'fecha_fmp': fecha_fmp,
                        'user_id':id_usuario_fsg,

                        })                 
  relacion_fechas[fecha_fsg] = lista_diferencias
# print(relacion_fechas)
resumen = pd.DataFrame(resultados)
print(resumen)
# # # ## Extenciones    
# # print("Generando datos extenciones")
# # archivo_tr_adip = f"Adip_CTC_{mes_nombre}.csv"
# # ruta_adip = os.path.join(ruta_adip, archivo_tr_adip)
# # trs.to_csv(ruta_adip, index=False)
# # # ## Extenciones    
# # print("Creando Relacion con MP")
# # archivo_tr_mp = f"MP_CTC_{mes_nombre}.csv"
# # ruta_mp = os.path.join(ruta_adip, archivo_tr_mp)
# # fnal.to_csv(ruta_mp, index=False)
# # ##
print("Creando Resumen General")
archivo_tr_ctc = f"Resumen_CTC_{mes_nombre}.csv"
ruta_re = os.path.join(ruta_adip, archivo_tr_ctc)
resumen.to_csv(ruta_re, index=False)


print("Proceso Realizado con Exito!!")

