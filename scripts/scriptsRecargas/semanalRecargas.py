import os
import pandas as pd
import psycopg2
import json
import matplotlib.pyplot as plt
import numpy as np
###############################################################################
#                           Parametros de Analisis                            #
###############################################################################
## Primer fecha compuesta de la consulta 
yi = '2024'
mi = '09'
di = '09'
## Segunda fecha compuesta de la consulta
yf = '2024'
mf = '09'
df = '15'
## Datos Extra
mes = 'Septiembre'
sem = '37'
##
app = '101801'
dig = '101800'
com = '201A00'
##
###############################################################################
#                           Carga de archivos                                 #
###############################################################################
## Obtener la ruta del directorio actual
ruta_actual = os.path.dirname(__file__)
## Subir un nivel en el directorio
ruta_superior = os.path.dirname(ruta_actual)
ruta_sup_2 = os.path.dirname(ruta_superior)
ruta_cats = os.path.join(ruta_actual,'data/')
# print(ruta_sup_2)
def path_verify(path):
  if not os.path.exists(path):
    os.makedirs(path)
    print(f'Directorio Creado: {path}')
  else:
    print(f'El Directorio ya existe: {path}')
##
###
ruta_dumps = os.path.join( ruta_sup_2,f'public/recargas/semanas/{yi}/{sem}')
ruta_dumps_mac = os.path.join(ruta_sup_2,f'public/recargas/semanas/{yi}/{sem}/MAC' )
## 
path_verify(ruta_dumps)
path_verify(ruta_dumps_mac)

## Unir la ruta superior con el nombre del archivo
json_fr = os.path.join(ruta_cats,'smartposFR.json')
with open(json_fr, 'r') as f_fr:
  data_fr = json.load(f_fr)
##
json_au = os.path.join(ruta_cats,'smartposAulsa.json')
with open(json_au, 'r') as f_au:
  data_au = json.load(f_au)
## 
json_conn = os.path.join(ruta_sup_2,f'config/db.json')
with open(json_conn, 'r') as f_conn:
  data_conn = json.load(f_conn)
##
connection = psycopg2.connect(**data_conn)
## Consulta para extraer todos los datos del periodo de tiempo seleccionado
cursor = connection.cursor()
cursor.execute(f"SELECT * FROM datos_rre_{yi} WHERE fecha_hora_transaccion >= '{yi}-{mi}-{di} 00:00:00' AND fecha_hora_transaccion <= '{yf}-{mf}-{df} 23:59:59'")
transacciones = cursor.fetchall()
##
cursor2 = connection.cursor()
cursor2.execute(f"SELECT * FROM datos_ext_rre_{yi} WHERE start_date >= '{yi}-{mi}-{di} 00:00:00' AND start_date <= '{yf}-{mf}-{df} 23:59:59'")
extenciones = cursor2.fetchall()
newExtenciones = []
for fila in extenciones:
  newExtenciones.append({
    'id_transaccion_organismo':fila[0],
    'user_id': fila[1],
    'device_id': fila[2],
    'latitude': fila[3],
    'longitude': fila[4],
    'start_date' : fila[5],
    'end_date' : fila[6],
    'duration' : fila[7],
  })
  
##
newDatos = []
##
for fila in transacciones:
  newDatos.append({
    'id_transaccion_organismo':fila[0],
    'provider': fila[1],
    'tipo_tarjeta': fila[2],
    'numero_serie_hex': fila[3],
    'fecha_hora_transaccion': fila[4],
    'tipo_equipo' : fila[5],
    'location_id' : fila[6],
    'tipo_transaccion' : fila[7],
    'saldo_antes_transaccion' : fila[8],
    'monto_transaccion' : fila[9],
    'saldo_despues_transaccion' : fila[10],
    'sam_serial_hex_ultima_recarga' : fila[11],
    'sam_serial_hex' : fila[12],
    'contador_recargas' : fila[13],
    'event_log' : fila[14],
    'load_log' : fila[15],
    'mac' : fila[16],
    'sam_counter' : fila[17],
    'environment' : fila[18],
    'environmet_issuer_id' : fila[19],
    'contract' : fila[20],
    'contract_tariff' : fila[21],
    'contract_sale_sam' : fila[22],
    'contract_restrict_time' : fila[23],
    'contract_validity_start_date' : fila[24],
    'contract_validity_duration' : fila[25]
  })

connection.close()
###############################################################################
#                           Funciones                                         #
###############################################################################
## DataFrame por tipo de red
def df_tipored(df,location_id):
  df_tipo = df[df['location_id'] == location_id]
  return df_tipo
## Resumen de Transacciones
def resumen_transacciones(df,fechas_unicas):
  resumen = []  
  ## Analizar dia por dia
  for fecha in fechas_unicas:
    df_dia = df[df['fecha_hora_transaccion'] == fecha]
    ## Crear DataFrame por tipo de red por dia
    df_app = df_tipored(df_dia,app)
    df_dig = df_tipored(df_dia,dig)
    df_com = df_tipored(df_dia,com)
    ## extraer montos por tipo de red
    monto_app = sum(df_app['monto_transaccion']) / 100
    monto_dig = sum(df_dig['monto_transaccion']) / 100
    monto_com = sum(df_com['monto_transaccion']) / 100
    ## Sumar los montos
    monto_total = monto_app + monto_dig + monto_com
    ## Transacciones totales
    tr_totales = len(df_dig) + len(df_app) + len(df_com)  
    ##
    resumen.append({
        'FECHA': fecha,
        'TR Digitales': len(df_dig),
        'TR Fisicas': len(df_com),
        'TR AppCDMX': len(df_app),
        'TR Totales': tr_totales,
        'Montos Digitales': monto_dig,
        'Montos Fisicos': monto_com,
        'Montos AppCDMX': monto_app,
        'Monto Total': monto_total
    })
  return resumen
## Definir funcion para crear graficos
def grafico(dias,mt,tr,title,name,periodo,colorb,colorl,colort):
  ## Declaracion del Grafico
  plt.style.use('seaborn-v0_8-paper')
  ## Definimos el objeto grafico (ancho,alto)
  fig,ax = plt.subplots(figsize=(18,8))
  ## Definimos el Area que ocupara el grafico
  plt.subplots_adjust(left=0.05, right=0.94, bottom=0.148, top=0.94)
  ## Valores base p
  ax2 = ax.twinx()
  p = ax.bar(dias,tr,label='Transacciones',color=f'#{colorb}')
  for i in enumerate(tr):
    ax.bar_label(p, label_type='center', color='#fff',fontsize=11,**{'fmt': '{:,.0f}'})
  for i, m in enumerate(mt):
    ax2.annotate(f'${m:,.0f}', (dias[i], m), xytext=(-20,10), textcoords='offset points', fontsize=12, fontweight=600,color=f'#{colort}')  
  ax2.plot(dias,mt,label='Montos',color=f'#{colorl}', marker='o',linestyle='solid')
  ## Creamos una legenda fuera del grafico
  fig.legend(loc='lower center', ncols=2, fontsize=12)
  # Ajusta el formato de los valores en el eje Y
  ax.yaxis.set_major_formatter(lambda x, pos: f'{x:,.0f}')
  ax2.yaxis.set_label_position("right")
  ax2.yaxis.set_major_formatter(lambda x, pos: f'${x:,.0f}')
  ##
  ax.tick_params(axis='x', labelsize=10)
  ax.tick_params(axis='y', labelsize=10)
  ax2.tick_params(axis='y', labelsize=10)
  ax.set_title(title,fontsize=14,fontweight=600)
  ax.set_xlabel(periodo,fontsize=12,fontweight=600)
  ax.set_ylabel('Transacciones',fontsize=12,fontweight=600)
  ax2.set_ylabel('Montos',fontsize=12,fontweight=600)
  ####################### Guarda el gráfico en alta resolución
  ruta_grafico = os.path.join(ruta_dumps, name)
  plt.savefig(ruta_grafico,format='png',dpi=980,bbox_inches='tight')
## 
def grafico_rre(df,title,name,periodo):
  ## Grafico
  dias = df['FECHA']
  digitales_tr = df['TR Digitales']  
  appcdmx_tr = df['TR AppCDMX']
  comercios_tr = df['TR Fisicas']
  digitales_mt = df['Montos Digitales']  
  appcdmx_mt = df['Montos AppCDMX']
  comercios_mt = df['Montos Fisicos']
    
    ## Definir el estilo del grafico
  plt.style.use('seaborn-v0_8-paper')
    ## Definimos el objeto grafico (ancho,alto)
  fig,ax = plt.subplots(figsize=(18,8))
  plt.subplots_adjust(left=0.05, right=0.926, bottom=0.148, top=0.94)
    ## Valores base para las barras apiladas
    ## Colores para las barras
# Create stacked bars using bar_stack
  # Create a dataframe to simplify plotting
  tipos = {
      'AppCDMX': appcdmx_mt,
      'Comercios': comercios_mt,
      'Digitales': digitales_mt
  }

  x = np.arange(len(dias))  # the label locations
  width = 0.25  # the width of the bars
  multiplier = 0

  colors = {  # Dictionary of colors for each attribute
      'AppCDMX': '#b81532',
      'Comercios': '#af38c1',
      'Digitales': '#08acec'
  }
  lColors = {
    'AppCDMX': '#4b0615',
    'Comercios': '#420c46',
    'Digitales': '#06314b'
  }
  for attribute, measurement in tipos.items():
      offset = width * multiplier
      color = colors.get(attribute)  # Get color from dictionary
      lcolor = lColors.get(attribute)  
      rects = ax.bar(x + offset, measurement, width, label=attribute, color=color)
      ax.bar_label(rects,label_type='center',padding=2,color=lcolor,fontweight=600,fontsize=9, labels=[f'${value:,.0f}' for value in measurement])
      multiplier += 1
  ax2 = ax.twinx()
      
  for i, tr in enumerate(digitales_tr):
    ax2.annotate(f'{tr:,.0f}', (dias[i], tr), xytext=(-20,10), textcoords='offset points', fontsize=9, color='#06314b')
  ax2.plot(dias,digitales_tr,label='Transacciones Digitales',color='#006fa5', marker='o',linestyle='solid')
  
  for i, tr in enumerate(appcdmx_tr):
    ax2.annotate(f'{tr:,.0f}', (dias[i], tr), xytext=(-20,10), textcoords='offset points', fontsize=9, color='#4b0615')
  ax2.plot(dias,appcdmx_tr,label='Transacciones AppCDMX',color='#f8747e', marker='o',linestyle='solid')
  
  for i, tr in enumerate(comercios_tr):
    ax2.annotate(f'{tr:,.0f}', (dias[i], tr), xytext=(-20,10), textcoords='offset points', fontsize=9, color='#420c46')
  ax2.plot(dias,comercios_tr,label='Transacciones Comercios',color='#66246b', marker='o',linestyle='solid')
  
    ## Creamos una legenda fuera del grafico
  fig.legend(loc='lower center', ncols=6, fontsize=12)
    # Ajusta el formato de los valores en el eje Y
  ax2.yaxis.set_label_position("right")
  ax.yaxis.set_major_formatter(lambda x, pos: f'${x:,.0f}')
  ax2.yaxis.set_major_formatter(lambda x, pos: f'{x:,.0f}')
    # Ajusta el título del gráfico
  ax.tick_params(axis='x', labelsize=10)
  ax.tick_params(axis='y', labelsize=10)
  ax2.tick_params(axis='y', labelsize=10)
  ax.set_title(title,fontsize=14,fontweight=600)
  ax.set_xlabel(periodo,fontsize=12,fontweight=600)
  ax.set_ylabel('Transacciones',fontsize=12,fontweight=600)
  ax2.set_ylabel('Montos',fontsize=12,fontweight=600)
    # Guarda el gráfico en alta resolución
  ruta_grafico = os.path.join(ruta_dumps, name)
  plt.savefig(ruta_grafico,format='png',dpi=980,bbox_inches='tight')
## 
def macFile(df,fechas,tipo):
  mac_sem = []
  for fecha in fechas:
    df_dia = df[(df['fecha_hora_transaccion'] >= f'{fecha} 00:00:00') & (df['fecha_hora_transaccion'] <= f'{fecha} 23:59:59')]
    df_type = df_tipored(df_dia,tipo)
    df_type.columns = df_type.columns.str.upper()
    mac_sem.append(df_type.head(3000))
  return mac_sem
##
def smartposDf(df_tr,df_ex,smartposList):
  coincidencias = []
  for smartpos in smartposList:
    df_smart = df_ex[df_ex['device_id'] == f'SMARTPOS{smartpos}']
    match = pd.merge(df_tr, df_smart, on="id_transaccion_organismo", how="inner")
    coincidencias.append(match)
  return coincidencias
##
def resSmartpos(df,fechas_unicas):
  reSmart = []
  for fecha in fechas_unicas:
    df_dia_sm = df[df['fecha_hora_transaccion'] == fecha]
    transacciones = len(df_dia_sm)
    monto = sum(df_dia_sm['monto_transaccion']) / 100
    reSmart.append({
      'Fecha': fecha,
      'Transacciones': transacciones,
      'Monto': monto
    })
  return reSmart
##
###############################################################################
#                           Inicio de Analisis                                #
###############################################################################
df_ori = pd.DataFrame(newDatos)
df_ext = pd.DataFrame(newExtenciones) 
##
df_suc = df_ori[df_ori['tipo_transaccion'] == '0'].copy()
df_suc_m = df_ori[df_ori['tipo_transaccion'] == '0'].copy()
df_suc_m['fecha_hora_transaccion'] = df_suc_m['fecha_hora_transaccion'].dt.strftime('%Y-%m-%d')
fechas_unicas = sorted(set(df_suc_m['fecha_hora_transaccion']))

## Smartpos Resumen
print('Resumen Smartpos')
aulsa = pd.concat(smartposDf(df_suc_m,df_ext,data_au),ignore_index=True)
flecha = pd.concat(smartposDf(df_suc_m,df_ext,data_fr),ignore_index=True)
##
df_aulsa = pd.DataFrame(resSmartpos(aulsa,fechas_unicas))
df_flecha = pd.DataFrame(resSmartpos(flecha,fechas_unicas))
#
print('Resumen Aulsa')
file_aulsa = f'resumen_aulsa_semana_{sem}_{yi}.csv'
path_aulsa = os.path.join(ruta_dumps,file_aulsa )
df_aulsa.to_csv(path_aulsa, index=False)
print('Resumen Flecha Roja')
#
file_flecha = f'resumen_flecha_semana_{sem}_{yi}.csv'
path_flecha = os.path.join(ruta_dumps,file_flecha)
df_flecha.to_csv(path_flecha, index=False)

########

fileApp = f'APP_MAC_rev_sem_{sem}.csv'
fileRRF = f'RRF_MAC_rev_sem_{sem}.csv'
fileRRD = f'RRD_MAC_rev_sem_{sem}.csv'
##
rutaApp = os.path.join(ruta_dumps_mac,fileApp)
rutaRRF = os.path.join(ruta_dumps_mac,fileRRF)
rutaRRD = os.path.join(ruta_dumps_mac,fileRRD)
##
macApp = macFile(df_suc,fechas_unicas,app)
macAppC = pd.concat(macApp, ignore_index=True)
macDig = macFile(df_suc,fechas_unicas,dig)
macDigC = pd.concat(macDig, ignore_index=True)
macCom = macFile(df_suc,fechas_unicas,com)
macComC = pd.concat(macCom, ignore_index=True)
##
print('Creando Archivos para revision MAC')
print('Generando Archivo RRF')
macComC.to_csv(rutaRRF,index=False)
print('Generando Archivo RRD')
macDigC.to_csv(rutaRRD,index=False)
print('Generando Archivo APP')
macAppC.to_csv(rutaApp,index=False)
##
resumen = resumen_transacciones(df_suc_m,fechas_unicas)
resultados = pd.DataFrame(resumen)
###################
print('Creando Resumen Semanal')
archivo_mens = f"RRE_res_semana_{sem}.csv"
ruta_res = os.path.join(ruta_dumps, archivo_mens)
resultados.to_csv(ruta_res, index=False)

print(f"Creando Grafico AppCDMX..")
grafico( resultados['FECHA'], resultados['Montos AppCDMX'], resultados['TR AppCDMX'],
  f'Comportamiento AppCDMX semana {sem}', f'AppCDMX_Grafico_Semana_{sem}.png', f'Semana {sem}',
  'b81532', 'f8747e', '4b0615' )
## Digital
print(f"Creando Grafico Digitales..")
grafico( resultados['FECHA'], resultados['Montos Digitales'], resultados['TR Digitales'],
  f'Comportamiento Digitales semana {sem}', f'Dig_Grafico_Semana_{sem}.png', f'Semana {sem}',
  '08acec', '006fa5', '06314b' )
## Comercios
print(f"Creando Grafico Comercios..")
grafico( resultados['FECHA'], resultados['Montos Fisicos'], resultados['TR Fisicas'],
  f'Comportamiento Comercios semana {sem}', f'Com_Grafico_Semana_{sem}.png', f'Semana {sem}',
  'af38c1', '66246b', '420c46' )
##
nameGrRRE = f'RRE_Grafico_Semana_{sem}.png'
titleRRE = f'Comportamiento RRE semana {sem}'
periodoRRE = f'Semana {sem}'
grafico_rre(resultados,titleRRE,nameGrRRE,periodoRRE)
print('Proceso Finalizado con Exito!!')











