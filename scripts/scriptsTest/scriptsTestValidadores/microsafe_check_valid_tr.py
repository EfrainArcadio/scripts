import os
import pandas as pd
mes = 'Mayo'
m = '05'
qna = '1ra'
# qna = '2da'
ruta_guardado = f"Validadores/2024/{m} {mes}"
#

archivo1 = os.path.join(ruta_guardado, f'Extemporaneas_{qna}_qna_{mes}.csv')
df_rev1 = pd.read_csv(archivo1, low_memory=False,encoding='latin-1')

df_rev1['TIPO_TRANSACCION'] = df_rev1['TIPO_TRANSACCION'].astype('str')
cont = df_rev1['LINEA'].value_counts()
print(cont)
df_rev1 = df_rev1[df_rev1['TIPO_TRANSACCION'] == '3']

df_miit = df_rev1[df_rev1['LINEA'] == '1']
# print(len(set(df_miit['AUTOBUS'])))
# print(len(set(df_miit['LOCATION_ID'])))
df_saus = df_rev1[df_rev1['LINEA'] == '2']
# print(len(set(df_saus['AUTOBUS'])))
# print(len(set(df_saus['LOCATION_ID'])))
df_atro = df_rev1[df_rev1['LINEA'] == '3']
# print(len(set(df_atro['AUTOBUS'])))
# print(len(set(df_atro['LOCATION_ID'])))
df_ceus = df_rev1[df_rev1['LINEA'] == '4']
# print(len(set(df_ceus['AUTOBUS'])))
# print(len(set(df_ceus['LOCATION_ID'])))

def analisis_bus (df):
  print(df['LINEA'].value_counts())
  list_bus = set(df['LOCATION_ID'])
  print(len(list_bus))
  # print(list_bus)
  df_autobuses = []
  for bus in list_bus:
    df_bus = df[df['LOCATION_ID'] == bus]
    tra = len(df_bus)
    mto = sum(df_bus['MONTO_TRANSACCION']) / 100
    df_autobuses.append({
      'Economico': bus,
      'Transacciones': tra,
      'Monto': f'$ {mto}',
    })
  return df_autobuses
  # print(df_autobuses)

# print(analisis_bus(df_atro))
miit =  pd.DataFrame(analisis_bus(df_miit))
sausa =  pd.DataFrame(analisis_bus(df_saus))
atrolsa =  pd.DataFrame(analisis_bus(df_atro))
ceusa =  pd.DataFrame(analisis_bus(df_ceus))

lista = f"analisis_corredores_{qna}_{mes}.xlsx"
ruta_lista = os.path.join(ruta_guardado,lista)

with pd.ExcelWriter(ruta_lista) as writer:
    miit.to_excel(writer, index=False ,sheet_name=f'Resumen MIIT')
    sausa.to_excel(writer, index=False ,sheet_name=f'Resumen SAUSA')
    atrolsa.to_excel(writer, index=False ,sheet_name=f'Resumen ATROLSA')
    ceusa.to_excel(writer, index=False ,sheet_name=f'Resumen CEUSA')

# print(miit)
# print(sausa)
# print(atrolsa)
# print(ceusa)

# df_fn_bus = pd.concat(df_autobuses)
# print(df_fn_bus['AUTOBUS'].value_counts())