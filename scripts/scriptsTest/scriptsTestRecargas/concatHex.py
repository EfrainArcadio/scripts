# imporatr las librerias necesarias para le funcionamiento
import os
import pandas as pd
# Directorio donde se encuentran los archivos CSV y la cual sera la base de trabajo del Script
ruta_guardado = "Transacciones/2024/02 Febrero"

# Definir el nombre del archivo que contendra la union de todos
validadores = "Febrero-2024.csv"
# Definir los nombres de los archivos que se van a leer
archivos_a_leer = [
    "20240216-Transacciones.csv",
    "20240217-Transacciones.csv",
    "20240218-Transacciones.csv",

]

# Lista para almacenar los DataFrames de los archivos
dataframes = []

# Leer los archivos y almacenarlos en la lista de DataFrames
for archivo in archivos_a_leer:
    archivo_path = os.path.join(ruta_guardado, archivo)
    df = pd.read_csv(archivo_path)
    dataframes.append(df)

df_valid = pd.concat(dataframes,ignore_index=True)
ruta_validadores = os.path.join(ruta_guardado,validadores)
df_valid.to_csv(ruta_validadores,index=False)

