import pandas as pd
import os
ruta_actual = os.path.dirname(__file__)
file = os.path.join(ruta_actual,'data-contracargos-2024.csv')
file_k = os.path.join(ruta_actual,'chargeback-20240802193627-e8a6.csv')
file_1 = os.path.join(ruta_actual,'01-07.csv')
file_2 = os.path.join(ruta_actual,'08-14.csv')
file_3 = os.path.join(ruta_actual,'15-21.csv')
file_4 = os.path.join(ruta_actual,'22-28.csv')
file_5 = os.path.join(ruta_actual,'29-31.csv')
file_v2 = os.path.join(ruta_actual,'contracargos_v2.csv')

df = pd.read_csv(file)
df_k = pd.read_csv(file_k)
df_1 = pd.read_csv(file_1)
df_2 = pd.read_csv(file_2)
df_3 = pd.read_csv(file_3)
df_4 = pd.read_csv(file_4)
df_5 = pd.read_csv(file_5)
df_v2 = pd.read_csv(file_v2)



print(len(set(df_v2['operation_id'])))
# df_mes_db = df[(df['date_created'] >= '2024-07-01 00:00:00') & (df['date_created'] <= '2024-07-31 23:59:59')]
# print(df_mes_db['date_created'])

# # print(df_k.columns)
# lau = df_k[df_k['Código de referencia de la operación (operation_external_reference)'] == 'LauraOrizaga@outlook.com']
# print(lau)
# mes = [
#   df_1,df_2,df_3,df_4,df_5
# ]

# df_mes = pd.concat(mes,ignore_index=True)

# list_j = set(df_mes_db['chargeback_id'])
# list_k = set(df_k['Número de contracargo (chargeback_id)'])
# print(len(list_j))
# print(len(list_k))

# lista_sin_coincidencias = [e for e in list_k if not e  in list_j]

# print(len(lista_sin_coincidencias))

# print(df_mes)
# print(len(set(df_k['Número de contracargo (chargeback_id)'])))
# print(len(set(df_1['Número de contracargo (chargeback_id)'])))
# print(len(set(df_2['Número de contracargo (chargeback_id)'])))
# print(len(set(df_3['Número de contracargo (chargeback_id)'])))
# print(len(set(df_4['Número de contracargo (chargeback_id)'])))
# print(len(set(df_5['Número de contracargo (chargeback_id)'])))
# print(len(set(df_mes['Número de contracargo (chargeback_id)'])))
