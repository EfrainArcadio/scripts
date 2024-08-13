# imporatr las librerias necesarias para le funcionamiento
import os
import pandas as pd
# Directorio donde se encuentran los archivos CSV y la cual sera la base de trabajo del Script
ruta_guardado = "Transacciones/anual"

# Definir los nombres de los archivos que se van a leer
archivos_a_leer = [
    # "Marzo_crudo.csv",
    # "Abril_crudo.csv",
    # "Mayo_crudo.csv",
    # "Junio_crudo.csv",
    # "Julio_crudo.csv",
    # "Agosto_crudo.csv",
    # "Septiembre_crudo.csv",
    "Octubre_crudo.csv",
    "Noviembre_crudo.csv",
    "Diciembre_crudo.csv",

]

# Lista para almacenar los DataFrames de los archivos
dataframes = []

# Leer los archivos y almacenarlos en la lista de DataFrames
for archivo in archivos_a_leer:
    archivo_path = os.path.join(ruta_guardado, archivo)
    df = pd.read_csv(archivo_path)
    dataframes.append(df)

df = pd.concat(dataframes,ignore_index=True)
df['TIPO_TRANSACCION'] = df['TIPO_TRANSACCION'].astype('str')
df_filtered = df[df['TIPO_TRANSACCION'] == '0']

## Separar las transacciones en base al método
df_fisico = df_filtered[df_filtered['LOCATION_ID'] == '201A00']
df_digital = df_filtered[df_filtered['LOCATION_ID'] == '101800']

## Calcular el monto total por transacción física y agregar al DataFrame correspondiente
monto_fisico = df_fisico['MONTO_TRANSACCION'].sum()

## Calcular el monto total por transacción digital y agregar al DataFrame correspondiente
monto_digital = df_digital['MONTO_TRANSACCION'].sum()


mto_fis = monto_fisico / 100
mto_dig = monto_digital / 100

print("fisicas",df_fisico.shape[0], "$",mto_fis)
print("digitales",df_digital.shape[0],"$",mto_dig)

print(df_filtered['NUMERO_SERIE_HEX'].value_counts())
print(df_filtered['NUMERO_SERIE_HEX'].shape[0])

print(len(set(df_filtered['NUMERO_SERIE_HEX'])))
# ruta_validadores = os.path.join(ruta_guardado,validadores)
# df_valid.to_csv(ruta_validadores,index=False)