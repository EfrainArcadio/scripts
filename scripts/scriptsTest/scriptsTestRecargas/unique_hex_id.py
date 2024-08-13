import os
import pandas as pd
###############################################################################
#                           Carga de archivos                                 #
###############################################################################
## Definimos el mes con nombre
mes = "Junio"
## Definimos el mes con número
m = "06"
## Definimos el año
y = "2024"
## Nombres de archivos
name_file = "1er_Trimestre_2024"
file_out = f"Unique_cards_{name_file}.csv"
## Ruta donde se leen y se guardan los archivos csv
path_work = f"Transacciones/{y}/Trimestres"

file = f'{name_file}.csv'
path_file = os.path.join(path_work, file)
df = pd.read_csv(path_file, low_memory=False, encoding='latin-1')

unique_hex_card = set(df['NUMERO_SERIE_HEX'])
print(len(unique_hex_card))
data = {"unique_hex_cards": list(unique_hex_card)}
data = pd.DataFrame(data)
ruta_data = os.path.join(path_work,file_out)
data.to_csv(ruta_data, index=False)

