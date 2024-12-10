import os
import glob
import pandas as pd
def path_verify(path):
  if not os.path.exists(path):
    # os.makedirs(path)
    print(f'No existe: {path}')
    
  else:
    print(f'El Directorio ya existe: {path}')
    
    
###
ruta_actual = os.getcwd()
path_root = os.path.dirname(ruta_actual)
path_files = f'dataFiles/validaciones/2024/extemporaneasFull'
ruta_dumps = os.path.join( ruta_actual,f'public/validaciones')
file = 'resumen_extemporaneas_2024.csv'
path_work_files = os.path.join(path_root,path_files)
print(ruta_actual)
path_verify(path_work_files)

archivos = glob.glob(os.path.join(path_work_files, '*.csv'))

files = []
for archivo in archivos:
    # print(archivo)
    df = pd.read_csv(archivo,encoding='latin-1',low_memory=False)
    # df = df.rename(columns={'ï»¿ID_TRANSACCION_ORGANISMO': 'ID_TRANSACCION_ORGANISMO'})
    files.append(df)
df = pd.concat(files)
# print(df.columns)
df['FECHA_HORA_TRANSACCION'] = pd.to_datetime(df['FECHA_HORA_TRANSACCION'])
df['FECHA_HORA_TRANSACCION'] = df['FECHA_HORA_TRANSACCION'].dt.strftime('%Y-%m-%d')
fechas_unicas = sorted(df['FECHA_HORA_TRANSACCION'].unique())
# print(fechas_unicas)
df['TIPO_TRANSACCION'] = df['TIPO_TRANSACCION'].astype('str')
res = []
for fecha in fechas_unicas:
  df_day = df[df['FECHA_HORA_TRANSACCION'] == fecha]
  bus = df_day[df_day['TIPO_TRANSACCION'] == '3']
  ban = df_day[df_day['TIPO_TRANSACCION'] == '5']
  monto_bus = sum(bus['MONTO_TRANSACCION']) /100
  monto_ban = sum(ban['MONTO_TRANSACCION']) /100
  res.append({
    'Fecha:': fecha,
    'Autobus': monto_bus, 
    'Baños': monto_ban, 
  })
  # print(sum(df_day['MONTO_TRANSACCION']) / 100)

resumen = pd.DataFrame(res)

path_flecha = os.path.join(ruta_dumps,file)
resumen.to_csv(path_flecha, index=False)

print(resumen)
# print(df)
# print(path_work_files)