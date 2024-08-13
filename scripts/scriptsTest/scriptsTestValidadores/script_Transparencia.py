# imporatr las librerias necesarias para le funcionamiento
import os
import pandas as pd

mes_nombre = 'Noviembre'
mes_n = '11'
# Directorio donde se encuentran los archivos CSV y la cual sera la base de trabajo del Script
ruta_guardado = f"Validadores/{mes_n} {mes_nombre}"

# Definir los nombres de los archivos que se van a leer
archivos_a_leer = [
    "ORT_Validaciones_2da_qna_noviembre_2023_Conduent_Buenavista.csv",
    "ORT_Validaciones_2da_qna_noviembre_2023_Conduent_Tacubaya.csv",
    "ORT_Validaciones_2da_qna_noviembre_2023_Microsafe.csv",
    "ORT_Validaciones_1ra_qna_noviembre_2023_Conduent_WC_Buenavista.csv",
    "ORT_Validaciones_1ra_qna_noviembre_2023_Conduent_WC_Tacubaya.csv",
    "ORT_Validaciones_1ra_qna_noviembre_2023_Microsafe.csv",
]
# Lista para almacenar los DataFrames de los archivos
dataframes = []
## 
hex_valids = []
# Leer los archivos y almacenarlos en la lista de DataFrames
for archivo in archivos_a_leer:
    archivo_path = os.path.join(ruta_guardado, archivo)
    df = pd.read_csv(archivo_path,encoding="latin-1")
    dataframes.append(df)
    
df_all = pd.concat(dataframes,ignore_index=True)
df_baños = df_all[df_all['TIPO_TRANSACCION'] == '5'].copy()
df_baños.LINEA.replace('E', 'Zapata', inplace=True)
df_baños.LINEA.replace('34', 'Buenavista', inplace=True)
df_baños.LINEA.replace('36', 'Tacubaya', inplace=True)
hex_baños = df_baños['NUMERO_SERIE_HEX']

df_hex_baños = pd.DataFrame(hex_baños)
archivo_hex = f"Hex_baños_{mes_nombre}.csv"
ruta_hex = os.path.join(ruta_guardado, archivo_hex)
df_hex_baños.to_csv(ruta_hex, index=False)

mto_baños = df_baños['MONTO_TRANSACCION']
date = df_baños['FECHA_HORA_TRANSACCION']
baño = df_baños['LINEA']

hex_valids.append({
    'NUMERO_SERIE_HEX': hex_baños,
    'LINEA': baño,
    'MONTO_TRANSACCION': mto_baños,
    'FECHA_HORA_TRANSACCION': date,
})

print(type(hex_valids))
hex_valids_df = [pd.DataFrame(d) for d in hex_valids]
df_valid_fin = pd.concat(hex_valids_df, ignore_index=True)
archivo_valid = f"Hex_montos_{mes_nombre}.csv"
ruta_valid = os.path.join(ruta_guardado,archivo_valid)
df_valid_fin.to_csv(ruta_valid, index=False)

print("Proceso Finalizado con Exito!!")