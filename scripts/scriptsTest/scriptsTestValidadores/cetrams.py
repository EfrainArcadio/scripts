##importar librearias necesarias para el funcionamiento del script
import os
import pandas as pd

# Nombre de documentos y carpetas
carpeta = "Cetrams"
cetrambd = "BD_CETRAM_09112023.csv"
ctrlgpsdb = "BD_ControlGPS_09112023.csv"

# Crear rutas con carpeta y archivo
cetram_path = os.path.join(carpeta,cetrambd)
gps_path = os.path.join(carpeta,ctrlgpsdb)

# DF de los archivos leidos
bd_cetram = pd.read_csv(cetram_path,encoding='latin1')
bd_gps = pd.read_csv(gps_path)

bd_cetram_clean = bd_cetram[bd_cetram['PLACA'] != "S/P"]

coincidencias = bd_gps['Placa'].isin(bd_cetram_clean['PLACA'])
# Suponiendo que 'bd_gps' y 'bd_cetram_clean' son tus DataFrames
merged_df = pd.merge(bd_cetram, bd_gps, left_on='PLACA', right_on='Placa', how='inner')

# 'merged_df' ahora contiene las filas de ambos DataFrames donde las placas coinciden
print(merged_df)
print(bd_gps['Estatus Actual'].value_counts())
ruta_filtrado = os.path.join(carpeta,"SinPlacas.csv")
ruta_coincidencias = os.path.join(carpeta,"Coincidencias.csv")
merged_df.to_csv(ruta_coincidencias, index=False)
bd_cetram_clean.to_csv(ruta_filtrado, index=False)