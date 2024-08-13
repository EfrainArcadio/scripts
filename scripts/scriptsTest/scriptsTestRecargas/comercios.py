##importar librearias necesarias para el funcionamiento del script
import os
import pandas as pd

## 
ruta_file_com = f"Transacciones/"
arc_com = 'comercios.csv'
file_com = os.path.join(ruta_file_com, arc_com)
df_com = pd.read_csv(file_com, low_memory=False, encoding='latin-1')

# print(df_com['CUST_ID'])

## Nombre del mes con texto, se ocupara para leer la carpeta del mes y asignar el nombre a los archivos generados
mes_nombre = "Abril"
## 
## Modificar el contenido de m = "mes" * Para los meses que anteriores a octubre ocupar la sintaxis 09 = Septiembre 08 = Agosto
## Modificar el contenido de Y = "Año" 2023 / 2024 / 2025 
m = "04"
y = "2024"

## Ruta de la cual se extraeran todos los archivos y en la misma se guardaran los archivos
ruta_guardado = f"Transacciones/{y}/{m} {mes_nombre}"

arc_tr = f'Full_{mes_nombre}.csv'
file_tr = os.path.join(ruta_guardado, arc_tr)
df_tr = pd.read_csv(file_tr, low_memory=False, encoding='latin-1')

arc_ext = f'Full_ext_{mes_nombre}.csv'
file_ext = os.path.join(ruta_guardado, arc_ext)
df_ext = pd.read_csv(file_ext, low_memory=False, encoding='latin-1')


df_tr_com = df_tr[df_tr['LOCATION_ID'] == '201A00']
df_tr_com_ex = df_tr_com[df_tr_com['TIPO_TRANSACCION'] == '0']

list_tr_com = list(df_tr_com_ex['ID_TRANSACCION_ORGANISMO'])

res_ext_com = []
for tr in list_tr_com:
    df_ext_com = df_ext[df_ext['ID_TRANSACCION_ORGANISMO'] == tr]
    res_ext_com.append(df_ext_com)

fn_com = pd.concat(res_ext_com, ignore_index=True)
uniq_com = set(fn_com['USER_ID'])

res_final = []
for com in uniq_com:
    df_comr = fn_com[fn_com['USER_ID'] == com]
    cstm = df_com[df_com['CUST_ID'] == com]
    
    desc = cstm['DESCRIPCION']

    print(desc)
    dire = cstm['DIRECCION']

    print(dire)
    smpos = df_comr['DEVICE_ID'].unique()
    trs = list(df_comr['ID_TRANSACCION_ORGANISMO'])

    monto = []
    for tr in trs:
        df = df_tr_com_ex[df_tr_com_ex['ID_TRANSACCION_ORGANISMO'] == tr]
        mto = df['MONTO_TRANSACCION'] / 100

        print(mto)
        monto.append(mto)
      
    res_final.append({
        'CUST_ID': com,
        'SMARTPOS': smpos,
        'DIRECCION': dire,
        'DESCRIPCION': desc,
        'MONTO': sum(monto)
    }) 
    
# res_com_final_df = pd.DataFrame(res_final)
# print(res_com_final_df)
# arc_res_com = f'RE_COM_{mes_nombre}.csv'
# ruta_res = os.path.join(ruta_guardado,arc_res_com)
# res_com_final_df.to_csv(ruta_res,index=False)
# print('Proceso Finalizado con exito!!')

