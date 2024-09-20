import pandas as pd
import os
##
y= "2024"
mes = "Enero"
m = "01"
##
name_file = 'Validaciones de la 1ra qna de enero 2024'
##
tablaExtName = f"datos_val_{y}"
### DataInfo
pathStringInfo = f'dataFiles/validaciones/{y}/{m} {mes}'
current_dir = os.getcwd()
parent_dir = os.path.dirname(current_dir)
pathInfo = os.path.join(parent_dir,pathStringInfo)
##
file_to_upload = f'{name_file}.csv'

## metodo para asignar la ruta al archivo
archivo = os.path.join(pathInfo, file_to_upload) 
df = pd.read_csv(archivo, low_memory=False, encoding='latin-1')
print(len(df.columns))
df_short = df[['ID_TRANSACCION_ORGANISMO','PROVIDER','TIPO_TARJETA','NUMERO_SERIE_HEX','FECHA_HORA_TRANSACCION','LINEA','ESTACION','AUTOBUS','RUTA','TIPO_EQUIPO','LOCATION_ID','TIPO_TRANSACCION','SALDO_ANTES_TRANSACCION','MONTO_TRANSACCION','SALDO_DESPUES_TRANSACCION','SAM_SERIAL_HEX_ULTIMA_RECARGA','SAM_SERIAL_HEX','CONTADOR_VALIDACIONES','EVENT_LOG','PURCHASE_LOG','MAC','ENVIRONMENT','ENVIRONMENT_ISSUER_ID','CONTRACT','CONTRACT_TARIFF','CONTRACT_SALE_SAM','CONTRACT_VALIDITY_START_DATE','CONTRACT_VALIDITY_DURATION']]
print(len(df_short.columns))
print(df['LINEA'].value_counts())
























