import os
import pandas as pd

file = 'ORT_validaciones_2da_qna_octubre_2024_Insitra'
file_ful = f'{file}.csv'

def path_verify(path):
  if not os.path.exists(path):
    os.makedirs(path)
    print(f'Directorio Creado: {path}')
  else:
    print(f'El Directorio ya existe: {path}')

current_dir = os.getcwd()

datos = 'scripts/scriptsTest/scriptsTestValidadores/data/MAC'
path_info = os.path.join(current_dir,datos)
df_files = os.path.join(path_info,file_ful)

path_verify(path_info)

df = pd.read_csv(df_files,low_memory=False, encoding='latin-1')

macs = df['MAC'].to_list()
g_mac = []
for mac in macs:
  if(len(mac) > 68):
    print(len(mac))
  elif(len(mac) == 68):
    g_mac.append(mac)
print(f'{len(macs)} - {len(g_mac)}')


