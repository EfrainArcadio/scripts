import json
import pandas as pd
import os

##
sem = '33'
y = '2024'
file = 'sem_33_12-18_ago'
file_bans = 'banned_mails.json'
##
won = 'Ganados: '
lost = 'Perdidos: '
disputa = 'Disputa: '
file_dump = f'{sem}_AppCDMX_Analisis_Contracargos.xlsx'
file_won = f'{sem}_won_contracargo.json'
file_lost = f'{sem}_lost_contracargo.json'
file_dispute = f'{sem}_dispute_contracargo.json'
fecha_limit = '13-08-2024 23:59:59'
fecha_limit = pd.to_datetime(fecha_limit, format='%d-%m-%Y %H:%M:%S')
###############################################################################
#                           Carga de archivos                                 #
###############################################################################
## Obtener la ruta del directorio actual
ruta_actual = os.path.dirname(__file__)
ruta_bans = os.path.join(ruta_actual,'data')

## Subir un nivel en el directorio
ruta_superior = os.path.dirname(ruta_actual)
ruta_sup_2 = os.path.dirname(ruta_superior)
print(ruta_sup_2)
def path_verify(path):
  if not os.path.exists(path):
    os.makedirs(path)
    print(f'Directorio Creado: {path}')
  else:
    print(f'El Directorio ya existe: {path}')
##
###
ruta_dumps = os.path.join( ruta_sup_2,f'public/recargas/semanas/{y}/{sem}')
path_verify(ruta_dumps)
dump_file = os.path.join(ruta_dumps,file_dump)
dump_file_won = os.path.join(ruta_dumps,file_won)
dump_file_lost = os.path.join(ruta_dumps,file_lost)
dump_file_dispute = os.path.join(ruta_dumps,file_dispute)
ruta_sup_root = os.path.dirname(ruta_sup_2)
path_files = os.path.join(ruta_sup_root,f'dataFiles/contracargos/{y}')
json_bans = os.path.join(ruta_bans,file_bans)
####
path_file = os.path.join(path_files,f'{file}.csv')
df = pd.read_csv(path_file)
######

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
######

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

##

with open(json_bans, 'r') as f:
    data_bans = json.load(f)
    print(data_bans)  # Imprime la lista cargada desde el archivo
reincidents = []
for user in data_bans:
  df_user = df[df['operation_external_reference'] == user]
  fechas = set(df_user['operation_date_created'])
  print(set(df_user['operation_date_created']))
  for fecha in fechas:
    if fecha >= fecha_limit:
      print('valio vrg')
      reincidents.append(df_user)
print(reincidents)

print(sum(df['operation_amount']))

##
df['operation_amount'] = df['operation_amount'].astype(float)
## Lista de montos
lista_montos = df['operation_amount'].unique().tolist()
lista_montos.sort(reverse=True)
montos = []
for monto in lista_montos:
    df_mto = df[df['operation_amount'] == monto]
    print(df_mto)
    num = len(df_mto)
    tt = monto * num
    montos.append({
        'Monto': monto,
        'N° Transacciones': num, 
        'Total': tt
    })
df_montos = pd.DataFrame(montos)    

# Cread DataFrames con informacion dsegun el status del contracargo
df_set = df[df["status"] == 'settled']
df_dis = df[df["status"] == 'dispute']
df_rei = df[df["status"] == 'reimbursed']
df_cov = df[df["status"] == 'covered']
## Utilizamos la funcion para crear un DataFrame con la informacion Resumida
df_gen = pd.DataFrame(gen_report_by_status(df))
df_set_fn = pd.DataFrame(gen_report_by_status(df_set))
df_dis_fn = pd.DataFrame(gen_report_by_status(df_dis))
df_rei_fn = pd.DataFrame(gen_report_by_status(df_rei))
df_cov_fn = pd.DataFrame(gen_report_by_status(df_cov))

df_gen_ord = df_gen.sort_values(by='Reincidencias',ascending=False)
df_set_ord = df_set_fn.sort_values(by='Reincidencias',ascending=False)
df_dis_ord = df_dis_fn.sort_values(by='Reincidencias',ascending=False)
df_rei_ord = df_rei_fn.sort_values(by='Reincidencias',ascending=False)
df_cov_ord = df_cov_fn.sort_values(by='Reincidencias',ascending=False)

with pd.ExcelWriter(dump_file) as writer:
    df_montos.to_excel(writer, index=False ,sheet_name=f'Resumen Montos')
    df_gen_ord.to_excel(writer, index=False ,sheet_name=f'Resumen General')
    df_set_ord.to_excel(writer, index=False ,sheet_name=f'Resumen settled')
    df_dis_ord.to_excel(writer, index=False ,sheet_name=f'Resumen dispute')
    df_rei_ord.to_excel(writer, index=False ,sheet_name=f'Resumen reimbursed')
    df_cov_ord.to_excel(writer, index=False ,sheet_name=f'Resumen covered')

##
df_won = df[df['status'] == 'reimbursed']
df_lost = df[(df['status'] == 'settled') | (df['status'] == 'covered') ]
df_dispute = df[(df['status'] == 'dispute') | (df['status'] == 'documentation_pending') ]

def df_by_status(df,file,detail):
  print(f'Creando JSON {detail}')
  lista_operation_id = df['operation_id'].tolist()
  monto = sum(df['operation_amount'])
  print(detail,len(lista_operation_id),'Monto Total: $',monto)
  with open(file, 'w') as f:
      json.dump(lista_operation_id, f)

df_by_status(df_won,dump_file_won,won)
df_by_status(df_lost,dump_file_lost,lost)
df_by_status(df_dispute,dump_file_dispute,disputa)
