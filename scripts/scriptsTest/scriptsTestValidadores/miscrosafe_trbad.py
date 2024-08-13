import os
import pandas as pd

ruta_guardado = "Validadores/Diciembre"

documento = []

archivo = os.path.join(ruta_guardado, 'ORT_Validaciones_2da_qna_diciembre_2023_gral.csv')
df = pd.read_csv(archivo, low_memory=False, encoding='latin-1')
# eliminar todas las transacciones exitosas
df = df[df['TIPO_TRANSACCION'] != 3]
df = df[df['TIPO_TRANSACCION'] != 5]

# Convertir la columna FECHA_HORA_TRANSACCION a datetime
df['FECHA_HORA_TRANSACCION'] = pd.to_datetime(df['FECHA_HORA_TRANSACCION'])
df['FECHA_HORA_TRANSACCION'] = df['FECHA_HORA_TRANSACCION'].dt.strftime('%Y-%m-%d')

fechas_unicas = df['FECHA_HORA_TRANSACCION'].unique()
# print(fechas_unicas)
df_autobuses = df['AUTOBUS'].unique()

for autobus in df_autobuses:
     df_trbad = df[df['AUTOBUS'] == autobus]
     documento.append(df_trbad)

autobuses = pd.concat(documento,ignore_index=True)
archivo_val = "Autobuses.csv"
ruta_autobuses = os.path.join(ruta_guardado, archivo_val)
autobuses.to_csv(ruta_autobuses, index=False)

##########
news = []
for fecha in fechas_unicas:
     df_new = autobuses[autobuses['FECHA_HORA_TRANSACCION'] == fecha]
     lin_gra = df_new[df_new['TIPO_TRANSACCION'] == 4]
     lin_tra = df_new[df_new['TIPO_TRANSACCION'] == 7]
     lin_sal = df_new[df_new['TIPO_TRANSACCION'] == 11]
     lin_rfw = df_new[df_new['TIPO_TRANSACCION'] == 13]
     lin_sss = df_new[df_new['TIPO_TRANSACCION'] == 51]
     lin_asm = df_new[df_new['TIPO_TRANSACCION'] == 53]
     lin_tai = df_new[df_new['TIPO_TRANSACCION'] == 55]
     lin_tax = df_new[df_new['TIPO_TRANSACCION'] == 61]
     lin_pgo = df_new[df_new['TIPO_TRANSACCION'] == 70]
     # print(lin_tax)
     news.append({
          'Fecha':fecha,
          'Gratuidad': lin_gra.shape[0],
          'Buses Grtuitidad': lin_gra['AUTOBUS'].unique(),
          'Transbordo': lin_tra.shape[0],
          'Buses Transbordo': lin_tra['AUTOBUS'].unique(),
          'Salida': lin_sal.shape[0],
          'Buses Salida': lin_sal['AUTOBUS'].unique(),
          'Rechazo de tarjeta por recarga fuera de lista blanca': lin_rfw.shape[0],
          'Buses RTRFLB': lin_rfw['AUTOBUS'].unique(),
          'Sin saldo suficiente': lin_sss.shape[0],
          'Buses Sin saldo': lin_sss['AUTOBUS'].unique(),
          'Transaccion abortada por saldo mayor': lin_asm.shape[0],
          'Buses ASM': lin_asm['AUTOBUS'].unique(),
          'Transaccion abortada por contrato invalido (firma erronea)': lin_tai.shape[0],
          'Buses TAI': lin_tai['AUTOBUS'].unique(),
          'Transaccion abortada (cualquier otro caso)': lin_tax.shape[0],
          'Buses TAX': lin_tax['AUTOBUS'].unique(),
          'Pase gratuito por operacion': lin_pgo.shape[0],
          'Buses PGO': lin_pgo['AUTOBUS'].unique(),
     })
     # print(news)
     
resultados = pd.DataFrame(news)
archivo_val = "Resumen.csv"
ruta_resultados = os.path.join(ruta_guardado, archivo_val)
resultados.to_csv(ruta_resultados, index=False)

    
print('Proceso Finalizado!!')

