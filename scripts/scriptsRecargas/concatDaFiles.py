import os
import pandas as pd
import glob
## Datos Fnal
y= "2024"
m = "09"
## File name dump
archivo_f = f'recargas_desglosadas_rre_{y}_{m}.csv'
### Paths
## Obtener la ruta del directorio actual
path_root = os.getcwd()
## Obtener la ruta superior del directorio actual
path_sup =  os.path.dirname(path_root)
## path load files
path_files = os.path.join(path_sup,f'dataFiles/recargas/datosAbiertos')
## path dump 
path_dump = os.path.join(path_root,f'public/recargas/datosAbiertos')
## path dump file
path_file_dump = os.path.join(path_dump,archivo_f)
## read all files
archivos = glob.glob(os.path.join(path_files, '*.csv'))
## create new df
df = pd.concat((pd.read_csv(archivo, encoding='latin-1') for archivo in archivos), ignore_index=True)
## save df to csv
df.to_csv(path_file_dump, index=False)
