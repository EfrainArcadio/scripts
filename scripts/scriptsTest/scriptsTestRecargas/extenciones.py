# imporatr las librerias necesarias para le funcionamiento
import os
import pandas as pd

mes_nombre = '2da_qna_Jul-2da_qna_Nov'
mes_n = '11'
# Directorio donde se encuentran los archivos CSV y la cual sera la base de trabajo del Script
ruta_guardado = f"Extenciones/"

# Definir los nombres de los archivos que se van a leer
archivos_a_leer = [
    "2da_qna_Julio-extencion.csv",
    "2da_qna_Agosto-extencion.csv",
    "1ra_qna_Agosto-extencion.csv",
    "1ra_qna_Septiembre-extencion.csv",
    "2da_qna_Septiembre-extencion.csv",
    "2da_qna_Octubre-extencion.csv",
    "1ra_qna_Octubre-extencion.csv",
    "2da_qna_Noviembre-extencion.csv",
    "1ra_qna_Noviembre-extencion.csv",
]
# Lista para almacenar los DataFrames de los archivos
dataframes = []
## 

for archivo in archivos_a_leer:
    archivo_path = os.path.join(ruta_guardado, archivo)
    df = pd.read_csv(archivo_path,encoding="latin-1")
    dataframes.append(df)
    
df_all = pd.concat(dataframes,ignore_index=True)

df_ext = pd.DataFrame(df_all)
archivo_ext = f"Extenciones_{mes_nombre}.csv"
ruta_hex = os.path.join(ruta_guardado, archivo_ext)
df_ext.to_csv(ruta_hex, index=False)