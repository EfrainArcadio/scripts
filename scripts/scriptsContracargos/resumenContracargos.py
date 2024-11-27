import json
import pandas as pd
import os
from fuzzywuzzy import fuzz
## localizacion de archivos
sem = '45'
y = '2024'
file = 'sem_45_04-10_nov'
## archivos json correos baneados
file_bans_v1 = 'banned_mails_v1.json'
file_bans_v2 = 'banned_mails_v2.json'
file_bans_v3 = 'banned_mails_v3.json'
## archivos dumps
file_dump = f'{sem}_AppCDMX_Analisis_Contracargos.xlsx'
file_won = f'{sem}_won_contracargo.json'
file_lost = f'{sem}_lost_contracargo.json'
file_dispute = f'{sem}_dispute_contracargo.json'
file_nb = f'{sem}_correos_desbaneo.json'
## Fechas
fecha_limit_1 = '13-08-2024 23:59:59'
fecha_limit_1 = pd.to_datetime(fecha_limit_1, format='%d-%m-%Y %H:%M:%S')
fecha_limit_2 = '18-09-2024 23:59:59'
fecha_limit_2 = pd.to_datetime(fecha_limit_2, format='%d-%m-%Y %H:%M:%S')
## Utilidades
won = 'Ganados: '
lost = 'Perdidos: '
disputa = 'Disputa: '

###############################################################################
#                                Funciones                                    #
###############################################################################

def path_verify(path):
  if not os.path.exists(path):
    os.makedirs(path)
    print(f'Directorio Creado: {path}')
  else:
    print(f'El Directorio ya existe: {path}')
## end path_verify
def gen_report_by_status(df):
  list_user_set = set(df['operation_external_reference'])
  resultados = []
  for mail in list_user_set:
    df_user = df[df['operation_external_reference'] == mail]
    rein = len(df_user)
    mto_ctc = sum(df_user['operation_amount'])
    resultados.append({
      'Usuario': mail,
      'Reincidencias': rein,
      'Monto Total': mto_ctc
    })
  return resultados
## end gen_report_by_status
def no_ban(row,correos_baneados):
  correo = row['operation_external_reference']
  if correo in correos_baneados:
    return True
  else:
    return False
## end no_ban
def es_sospechoso(row, correos_baneados, umbral_similitud=80):
  correo = row['operation_external_reference']
  usuario = correo.split('@')[0]
  if correo in correos_baneados:
    return True
  for correo_baneado in correos_baneados:
    usuario_baneado = correo_baneado.split('@')[0]
    if fuzz.ratio(usuario, usuario_baneado) >= umbral_similitud:
      return True
  return False
## end es_sospechoso
def df_by_status(df,file,detail):
  print(f'Creando JSON {detail}')
  lista_operation_id = df['operation_id'].tolist()
  monto = sum(df['operation_amount'])
  print(detail,len(lista_operation_id),'Monto Total: $',monto)
  with open(file, 'w') as f:
      json.dump(lista_operation_id, f)
## end df_by_status
def search_date(df,fechas,fecha_limit,array):
  for fecha in fechas:
    if fecha >= fecha_limit:
      df_dia = df[df['operation_date_created'] == fecha]
      array.append(df_dia)
## end search_date
###############################################################################
#                                   Paths                                     #
###############################################################################

## Obtener la ruta del directorio actual
ruta_actual = os.getcwd()
## Subir un nivel en el directorio
ruta_superior = os.path.dirname(ruta_actual)
## archivos json correos baneados
ruta_bans = os.path.join(ruta_actual,'scripts/scriptsContracargos/data')
## archivo csv contracargos semanal
path_files = os.path.join(ruta_superior,f'dataFiles/contracargos/{y}')
## salida de archivos
ruta_dumps = os.path.join( ruta_actual,f'public/recargas/semanas/{y}/{sem}')
## Rutas de archivos de salida
dump_file = os.path.join(ruta_dumps,file_dump)
dump_file_won = os.path.join(ruta_dumps,file_won)
dump_file_lost = os.path.join(ruta_dumps,file_lost)
dump_file_dispute = os.path.join(ruta_dumps,file_dispute)
dump_file_nb = os.path.join(ruta_dumps,file_nb)
### Ruta json
## json emails bans
json_bans_v1 = os.path.join(ruta_bans,file_bans_v1)
with open(json_bans_v1, 'r') as f:
    data_bans_1 = json.load(f)
## json emails bans
json_bans_v2 = os.path.join(ruta_bans,file_bans_v2)
with open(json_bans_v2, 'r') as f:
    data_bans_2 = json.load(f)
## json emails bans
json_bans_v3 = os.path.join(ruta_bans,file_bans_v3)
with open(json_bans_v3, 'r') as f:
    data_bans_3 = json.load(f)
## archivo contracargos
path_file = os.path.join(path_files,f'{file}.csv')
df = pd.read_csv(path_file)
######
path_verify(ruta_dumps)

###############################################################################
#                       Formateo de informacion                               #
###############################################################################
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
# Ensure date_created is datetime format
df['date_created'] = pd.to_datetime(df['date_created'], format='%d/%m/%Y %H:%M:%S')
df['operation_date_created'] = pd.to_datetime(df['operation_date_created'], format='%d/%m/%Y %H:%M:%S')
###############################################################################
#                       Formateo de informacion                               #
###############################################################################
print('Analizando correos baneados...')
## busca correos unicos
lista_first = list(set(data_bans_1) | set(data_bans_2))
lista_final = list(lista_first + data_bans_3)
## aplica funcion de no_ban al DF
df['No_ban'] = df.apply(no_ban, axis=1 , args=(lista_final,))
## buscar correos no baneados y crear un DataFrame de estos
nobans = df[df['No_ban'] == False]
## extraer correos unicos de los no baneados
corr_nb = set(nobans['operation_external_reference'])
## verificar que no existan correos no baneados en la lista final de baneos
## agregar nuevos baneos
new_bans = []
print('Buscando nuevos correos a banear...')
for user in corr_nb:
  df_user = nobans[nobans['operation_external_reference'] == user]
  reincidencias = len(df_user)
  monto = sum(df_user['operation_amount'])
  if reincidencias >= 1 and monto >= 15:
    new_bans.append({
      'Usuario': user,
      'Reincidencias': reincidencias,
      'Monto': monto
    })
## crear DF de correos a banear
df_to_ban = pd.DataFrame(new_bans)

df_to_ban_list = df_to_ban['Usuario'].unique().tolist()
print("Correos a Banear:",len(df_to_ban_list))
lista_x = [e for e in df_to_ban_list if e in lista_final]
if len(lista_x) > 0:
  print("Correos a banear en baneados",len(lista_x))
print('Buscando operaciones recientes..')
##
fechas = set(df['operation_date_created'])
##
re1 = []
search_date(df,fechas,fecha_limit_1,re1)
re2 = []
search_date(df,fechas,fecha_limit_2,re2)
print('Buscando correos sospechosos en nuevas operaciones...')
##
df_news_1 = pd.concat(re1)
## 
df_news_1['sospechoso'] = df_news_1.apply(es_sospechoso, axis=1, args=(data_bans_1,))
# Filtrar los registros sospechosos
# print(df_news_1['sospechoso'].value_counts())
sospechosos1 = df_news_1[df_news_1['sospechoso'] == True]
# print(sospechosos1)
if len(re2) > 0:
  df_news_2 = pd.concat(re2)
  df_news_2['sospechoso'] = df_news_2.apply(es_sospechoso, axis=1, args=(data_bans_2,))
  sospechosos2 = df_news_2[df_news_2['sospechoso'] == True]

# print(sospechosos)
print('Creando resumen General...')
##
df['operation_amount'] = df['operation_amount'].astype(float)
## Lista de montos
lista_montos = df['operation_amount'].unique().tolist()
lista_montos.sort(reverse=True)
montos = []
for monto in lista_montos:
  df_mto = df[df['operation_amount'] == monto]
  num = len(df_mto)
  tt = monto * num
  montos.append({
      'Monto': monto,
      'N° Transacciones': num, 
      'Total': tt
  })
df_montos = pd.DataFrame(montos)    

# print(df_montos)

# Cread DataFrames con informacion dsegun el status del contracargo
df_rei = df[df["status"] == 'reimbursed']
df_set = df[df["status"] == 'settled']
df_dis = df[df["status"] == 'dispute']
df_cov = df[df["status"] == 'covered']
df_dpe = df[df["status"] == 'documentation_pending']
## Utilizamos la funcion para crear un DataFrame con la informacion Resumida
df_gen = pd.DataFrame(gen_report_by_status(df))
df_rei_fn = pd.DataFrame(gen_report_by_status(df_rei))
df_set_fn = pd.DataFrame(gen_report_by_status(df_set))
df_dis_fn = pd.DataFrame(gen_report_by_status(df_dis))
df_cov_fn = pd.DataFrame(gen_report_by_status(df_cov))
df_dpe_fn = pd.DataFrame(gen_report_by_status(df_dpe))

df_gen_ord = df_gen.sort_values(by='Reincidencias',ascending=False)
df_set_ord = df_set_fn.sort_values(by='Reincidencias',ascending=False)
df_dis_ord = df_dis_fn.sort_values(by='Reincidencias',ascending=False)
df_rei_ord = df_rei_fn.sort_values(by='Reincidencias',ascending=False)
df_cov_ord = df_cov_fn.sort_values(by='Reincidencias',ascending=False)
df_dpe_ord = df_cov_fn.sort_values(by='Reincidencias',ascending=False)

with pd.ExcelWriter(dump_file) as writer:
  df_montos.to_excel(writer, index=False ,sheet_name=f'Resumen Montos')
  df_gen_ord.to_excel(writer, index=False ,sheet_name=f'Resumen General')
  df_rei_ord.to_excel(writer, index=False ,sheet_name=f'Resumen reimbursed')
  df_set_ord.to_excel(writer, index=False ,sheet_name=f'Resumen settled')
  df_cov_ord.to_excel(writer, index=False ,sheet_name=f'Resumen covered')
  df_dis_ord.to_excel(writer, index=False ,sheet_name=f'Resumen dispute')
  df_dpe_ord.to_excel(writer, index=False ,sheet_name=f'Resumen documentation_pending')
  df_to_ban.to_excel(writer,index=False,sheet_name='Reincidentes')
  if len(sospechosos1) > 0:
    sospechosos1.to_excel(writer,index=False,sheet_name='Baneos 60')
  if len(sospechosos2) > 0:
      sospechosos2.to_excel(writer,index=False,sheet_name='Sospechosos 397')

##
df_won = df[df['status'] == 'reimbursed']
df_lost = df[(df['status'] == 'settled') | (df['status'] == 'covered') ]
df_dispute = df[(df['status'] == 'dispute') | (df['status'] == 'documentation_pending') ]

email_won = df_won['operation_external_reference'].unique().tolist()
email_lost = df_lost['operation_external_reference'].unique().tolist()
email_dispute = df_dispute['operation_external_reference'].unique().tolist()

lista_nb = [e for e in email_won if not e in email_lost or email_dispute]
print('Correos para Desbanear',len(lista_nb))

with open(dump_file_nb, 'w') as f:
    json.dump(lista_nb, f)

df_by_status(df_won,dump_file_won,won)
df_by_status(df_lost,dump_file_lost,lost)
df_by_status(df_dispute,dump_file_dispute,disputa)

print('Proceso Finalizado con Exito!')