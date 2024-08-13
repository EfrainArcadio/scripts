import os
import pandas as pd
import json

ruta_trabajo = 'Transacciones/migration'

pe1 = [
    "Full_Noviembre.csv",
    "Full_Diciembre.csv",
    "Full_Enero.csv",
]
pe2 = [
    "Full_21-29_Febrero.csv",
    "Full_Marzo.csv",
    "Full_Abril.csv",
    "Full_Mayo.csv",
    "Full_Junio.csv"
]

per1 = []
per2 = []

def lectura_archivos(array_l,array_s):
    for archivo in array_l:
        archivo_path = os.path.join(ruta_trabajo,archivo)
        df = pd.read_csv(archivo_path)
        array_s.append(df)
##  
lectura_archivos(pe1,per1)
lectura_archivos(pe2,per2)
##
df_dc = pd.concat(per1)
df_ac = pd.concat(per2)
##
df_dc['TIPO_TRANSACCION'] = df_dc['TIPO_TRANSACCION'].astype('str')
df_fil_dc = df_dc[df_dc['TIPO_TRANSACCION'] == '0']
   
df_bth = df_fil_dc[df_fil_dc['LOCATION_ID'] != '101801']

turPe1 = df_bth['NUMERO_SERIE_HEX'].unique().tolist()
##
print('Generando NUMERO_SERIE_HEX RRE Periodo 1')
path_json_pe1 = os.path.join(ruta_trabajo,'hexCardUnique_RRE_P1.json')
with open(path_json_pe1, 'w') as f:
    json.dump(turPe1, f)

df_ac['TIPO_TRANSACCION'] = df_ac['TIPO_TRANSACCION'].astype('str')
df_fil_ac = df_ac[df_ac['TIPO_TRANSACCION'] == '0']
df_app = df_fil_ac[df_fil_ac['LOCATION_ID'] == '101801']
df_rre = df_fil_ac[df_fil_ac['LOCATION_ID'] != '101801']    
##

turPe2 = df_rre['NUMERO_SERIE_HEX'].unique().tolist()
tuaPe2= df_app['NUMERO_SERIE_HEX'].unique().tolist()
print('Tarjetas RRE PE2',len(df_rre['NUMERO_SERIE_HEX'].unique().tolist()))
print('Tarjetas AppCDMX PE2',len(df_app['NUMERO_SERIE_HEX'].unique().tolist()))
##
print('Generando NUMERO_SERIE_HEX APP Periodo 2')
path_json_app_pe2 = os.path.join(ruta_trabajo,'hexCardUnique_APP_P2.json')
with open(path_json_app_pe2, 'w') as f:
    json.dump(tuaPe2, f)
##
print('Generando NUMERO_SERIE_HEX RRE Periodo 2')
path_json_pe2 = os.path.join(ruta_trabajo,'hexCardUnique_RRE_P2.json')
with open(path_json_pe2, 'w') as f:
    json.dump(turPe2, f)
##

print('Analisando HEX_PE1 in HEX_APP_PE2')
lista_rre_to_app = [e for e in turPe1 if e  in tuaPe2]
print('Migraciones',len(lista_rre_to_app))
print('Generando Migraciones AppToRRE Periodo 2')
path_json_mig_app = os.path.join(ruta_trabajo,'hexCardUnique_migration_app_to_rre_P2.json')
with open(path_json_mig_app, 'w') as f:
    json.dump(lista_rre_to_app, f)

lista_app_to_rre = [e for e in lista_rre_to_app if e  in turPe2]
print('Migraciones APP To RRE',len(lista_app_to_rre))
print('Generando Migraciones AppToRRE Periodo 2')
path_json_mig_rre = os.path.join(ruta_trabajo,'hexCardUnique_migration_app_to_rre_P2.json')
with open(path_json_mig_rre, 'w') as f:
    json.dump(lista_app_to_rre, f)





# with open('lista_hex_pe2_in_pe1.json', 'w') as f:
#     json.dump(lista_hex_pe2_in_pe1, f)

# print( 'Suma x Sep',(len(list_com) + len(list_dig) ) )
# print('Sin AppCDMX',len(list_bth))
# print('AppCDMX',len(list_app))


# lista_sin_coincidencias = [e for e in list_bth if e  in list_app]
# print('Coinci',len(lista_sin_coincidencias)) 
# #
# res_com = []
# for card in list_bth:
   
#     cardInfo = df_bth[df_bth['NUMERO_SERIE_HEX'] == card]
#     tipo = cardInfo['LOCATION_ID'].values
#     fnt = set(tipo)
#     monto = cardInfo['MONTO_TRANSACCION'].sum()
    
#     res_com.append({
#         'cshex': card,
#         'tipoRed':fnt,
#         '# Recargas': cardInfo.shape[0],
#         '$ Recargas': monto / 100
#     })
    
# resultados_comercios = pd.DataFrame(res_com)
# print(resultados_comercios)
# res_app = []
# for card in list_app:
    
#     cardInfo = df_app[df_app['NUMERO_SERIE_HEX'] == card]
#     tipo = cardInfo['LOCATION_ID'].values
#     fnt = set(tipo)
#     monto = cardInfo['MONTO_TRANSACCION'].sum()
    
#     res_app.append({
#         'cshex': card,
#         'tipoRed':fnt,
#         '# Recargas': cardInfo.shape[0],
#         '$ Recargas': monto / 100
#     })
   
# resultados_appcdmx = pd.DataFrame(res_app)
# print(resultados_appcdmx)
# res_fna = []
# for card in lista_sin_coincidencias:
    
#     cardInfo = df_app[df_app['NUMERO_SERIE_HEX'] == card]
#     tipo = cardInfo['LOCATION_ID'].values
#     fnt = set(tipo)
#     monto = cardInfo['MONTO_TRANSACCION'].sum()
    
#     res_fna.append({
#         'cshex': card,
#         'tipoRed':fnt,
#         '# Recargas': cardInfo.shape[0],
#         '$ Recargas': monto / 100
#     })
    
# resultados_finales = pd.DataFrame(res_fna)
# print(resultados_finales)

# rre_fix = []
# for card in lista_sin_coincidencias:
#     cardInfo = df_rre[df_rre['NUMERO_SERIE_HEX'] == card]
#     tipo = cardInfo['LOCATION_ID'].values
#     fnt = set(tipo)
#     monto = cardInfo['MONTO_TRANSACCION'].sum()
#     rre_fix.append({
#         'cshex': card,
#         'tipoRed':fnt,
#         '# Recargas': cardInfo.shape[0],
#         '$ Recargas': monto / 100
#     })
    
# rre_res_act = pd.DataFrame(rre_fix)
# print(rre_res_act)
# print('Creando Documento Final')
# doc_fna = f'Migracion_RRF_RRD_TO_APPCDMX.xlsx'
# ruta_doc_fna = os.path.join(ruta_trabajo,doc_fna)
# with pd.ExcelWriter(ruta_doc_fna, mode='a') as writer:
#     resultados_comercios.to_excel(writer,index=False,sheetname='RE_Nov-Ene_RRD-RRF')
#     resultados_appcdmx.to_excel(writer,index=False,sheetname='RE_Feb-Abril-App')
#     resultados_finales.to_excel(writer,index=False,sheetname='Resumen-Migracion')
#     rre_res_act.to_excel(writer,index=False,sheetname='Resumen-Migracion-RRE')

# print('Proceso Finalizado con Exito!! Wenas Noches :v')