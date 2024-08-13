import os
import pandas as pd
import psycopg2
diai = '24'
diaf = '30'
#
m = '06'
m2 = '06'
##
y = '2024'
mes = "junio"
tabla = "contracargos"
root = 'Transacciones'
ruta_trabajo = f'{root}/Contracargos'
file_to_upload = f'{mes}.csv'
ruta_archivo = os.path.join(ruta_trabajo,file_to_upload)
df = pd.read_csv(ruta_archivo)
print(df.columns)
inicio = f"{diai}/{m}/{y} 00:00:00"
fin = f"{diaf}/{m2}/{y} 00:00:00"
df_sem = df[(df['Fecha de Creación del contracargo (date_created)'] >= inicio) & (df['Fecha de Creación del contracargo (date_created)'] < fin)]

print(len(df_sem))
print('$',sum(df_sem['Monto de la Operacion (operation_amount)']))

