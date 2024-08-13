import pandas as pd
import psycopg2
import os
## Donde se depositara el archivo final Nombre del Archivo y ruta compuesta
ruta_trabajo = 'data/Contracargos'
lista = f"Analisis Contracargos.xlsx"
ruta_lista = os.path.join(ruta_trabajo,lista)
## Parámetros de conexion
tabla = "contracargos"
connection_string = {
    "host": "localhost",
    "database": "ORT",
    "user": "postgres",
    "password": "Hj2c#y8", ## ORT-PC
    # "password": "8YSH58pX3", ## PC-Home
    "port": 5432,
}

connection = psycopg2.connect(**connection_string)

try:
    connection = psycopg2.connect(**connection_string)
    print("Connection established successfully!")
except Exception as e:
    print("Connection failed:", e)
    exit()  # Terminate script on connection error

sql_query = f"""
SELECT *
FROM {tabla};
"""
cursor = connection.cursor()
cursor.execute(sql_query)
column_names = [col.name for col in cursor.description]  # Extract column names as strings
data = cursor.fetchall()
## Funcion de Resumen
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
## Crear DataFrame con informacion de la base de datos
df = pd.DataFrame(data, columns=column_names)
df['operation_amount'] = df['operation_amount'].astype(float)
###################################### Por usuario

# list_user_bads = {
# 'velengiobor@gmail.com',
# 'z2803daniel@hotmail.com',
# 'pe.pejose2371@gmail.com',
# 'tmbados@gmail.com',
# 'erjiva@gmail.com',
# 'victorhugal4@gmail.com',
# 'erickvo10203040@gmail.com',
# '99.yra.bh@gmail.com',
# 'jhoon5174@gmail.com',
# 'victorzarate741@gmail.com',
# 'monrroydaniel099@gmail.com',
# 'angelgot80@gmail.com',
# 'adanarriola4@gmail.com',
# 'ypsy1522@gmail.com',
# 'yaelgutierrez124@gmail.com',
# 'j2812dani.el@gmail.com',
# 'r.medinha@gmail.com',
# 'ferbellstar@gmail.com',
# 'l2803daniel@hotmail.com',
# }

# with pd.ExcelWriter(ruta_lista) as writer:
#   for usuario in list_user_bads:
#     df_user = df[df['operation_external_reference'] == usuario]
#     print(usuario)
#     df_short_user = df_user[['date_created','operation_id','operation_amount','operation_external_reference']]
#     print()
#     df_short_user.to_excel(writer, index=False ,sheet_name=f'{usuario}')

  # print(df_user['operation_id'])
  # print(df_user['operation_amount'])
  

################################################ consulta Semana - Mensual
# diai = '26'
# diaf = '29'
# #
# m = '02'
# m2 = '02'
# ##
# y = '2024'
# inicio = f"{diai}/{m}/{y} 00:00:00"
# fin = f"{diaf}/{m2}/{y} 00:00:00"
# print(inicio)
# print(fin)
# df_sem = df[(df['date_created'] >= inicio) & (df['date_created'] < fin)]

# print(len(df_sem))
# print(df_sem['date_created'].value_counts())
# print('$',sum(df_sem['operation_amount']))

# print('$',df_sem['operation_amount'].value_counts())

# print(df)

################################################################################### Reporte Contracargos

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
## Crear Archivo
with pd.ExcelWriter(ruta_lista) as writer:
    df_montos.to_excel(writer, index=False ,sheet_name=f'Resumen Montos')
    df_gen_ord.to_excel(writer, index=False ,sheet_name=f'Resumen General')
    df_set_ord.to_excel(writer, index=False ,sheet_name=f'Resumen settled')
    df_dis_ord.to_excel(writer, index=False ,sheet_name=f'Resumen dispute')
    df_rei_ord.to_excel(writer, index=False ,sheet_name=f'Resumen reimbursed')
    df_cov_ord.to_excel(writer, index=False ,sheet_name=f'Resumen covered')

print('Proceso Finalizado!!')

