import os
import dask.dataframe as dd
import json
import pandas as pd
# import multiprocessing

# if __name__ == '__main__':
#     multiprocessing.freeze_support()
    
##########
y= "2024"
mes = "Febrero"
m = "02"
## Nombre corto del archivo sin extencion .csv
name_file = 'Validaciones 1ra qna febrero 2024'
vt = 'v1'
###############################################################################
#                           Carga de archivos                                 #
###############################################################################
## Nombre dinamico de la tabla a la que se le cargaran los datos nuevos
tablaExtName = f"datos_val_{y}_{vt}"
file_to_upload = f'{name_file}.csv'
json_db = "db.json" 
json_data = "modeloDatos.json" 
## definicion y creacion de Rutas de trabajo
current_dir = os.getcwd()
parent_dir = os.path.dirname(current_dir)
pathStringInfo = f'dataFiles/validaciones/{y}/{m} {mes}'
path_db = "config/"
path_data = "data/"
##
pathInfo = os.path.join(parent_dir,pathStringInfo)
archivo = os.path.join(pathInfo, file_to_upload) 
path_json_db = os.path.join(path_db,json_db)
path_json_data = os.path.join(path_data,json_data)
##
with open(path_json_data) as f:
  data_types = json.load(f)
column_names = [item["column"] for item in data_types]
data_types = [item["type"] for item in data_types]

dtypes = dict(zip(column_names, data_types))
# Create a dictionary mapping column names to data types

# client = Client(n_workers=10, threads_per_worker=1)
print(archivo)
df = pd.read_csv(archivo,encoding='latin-1',low_memory=False)
print(df.dtypes)
# df['FECHA_HORA_TRANSACCION'] = pd.to_datetime(df['FECHA_HORA_TRANSACCION'])
print(df.info())