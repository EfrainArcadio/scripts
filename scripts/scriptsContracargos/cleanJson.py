import os
import json

file1 = 'banned_mails_v1'
file2 = 'banned_mails_v2'
file3 = 'banned_mails_v3'

def path_verify(path):
  if not os.path.exists(path):
    os.makedirs(path)
    print(f'Directorio Creado: {path}')
  else:
    print(f'El Directorio ya existe: {path}')
    
current_dir = os.getcwd()
path_jsons = 'scripts/scriptsContracargos/data'

path_files = os.path.join(current_dir,path_jsons)

json_bans_v1 = os.path.join(path_files,f'{file1}.json')
with open(json_bans_v1, 'r') as f:
  data_bans_1 = json.load(f)

json_bans_v2 = os.path.join(path_files,f'{file2}.json')
with open(json_bans_v2, 'r') as f:
  data_bans_2 = json.load(f)

json_bans_v3 = os.path.join(path_files,f'{file3}.json')
with open(json_bans_v3, 'r') as f:
  data_bans_3 = json.load(f)

set_bans_1 = set(data_bans_1)
set_bans_2 = set(data_bans_2)
set_bans_3 = set(data_bans_3)

# Encontrar los usuarios que están en ambas listas 1 y 2
usuarios_en_ambas_1y2 = set_bans_1.intersection(set_bans_2)

# Eliminar los usuarios duplicados de la lista 2
set_bans_2 -= usuarios_en_ambas_1y2

# Realizar la misma operación para la lista 3
usuarios_en_ambas_1y3 = set_bans_1.intersection(set_bans_3)
set_bans_3 -= usuarios_en_ambas_1y3

# Convertir los conjuntos de nuevo a listas si es necesario
print('Antes')
print(len(data_bans_1))
print(len(data_bans_2))
print(len(data_bans_3))
print(len(data_bans_1)+len(data_bans_2)+len(data_bans_3))

data_bans_2 = list(set_bans_2)
data_bans_3 = list(set_bans_3)

print('Despues')
print(len(set_bans_1))
print(len(data_bans_2))
print(len(data_bans_3))

print(len(set_bans_1)+len(set_bans_2)+len(set_bans_3))

with open(json_bans_v2, 'w') as f:
  json.dump(data_bans_2, f)
with open(json_bans_v3, 'w') as f:
  json.dump(data_bans_3, f)


