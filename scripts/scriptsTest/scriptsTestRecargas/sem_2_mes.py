# imporatr las librerias necesarias para le funcionamiento
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
# Directorio donde se encuentran los archivos CSV y la cual sera la base de trabajo del Script
ruta_guardado = "Transacciones/2024/Semanas/Semana 22"
semana = '22'
# Definir los nombres de los archivos que se van a leer
## Agregar los archivos en base a la susecion de fechas confome al calendario
archivos_a_leer = [
    "20240527-Transacciones.csv",
    "20240528-Transacciones.csv",
    "20240529-Transacciones.csv",
    "20240530-Transacciones.csv",
    "20240531-Transacciones.csv",
    "20240601-Transacciones.csv",
    "20240602-Transacciones.csv",
]

# Lista para almacenar los DataFrames de los archivos
transacciones = []

resumen =[]

# Leer los archivos y almacenarlos en la lista de DataFrames
for archivo in archivos_a_leer:
    archivo_path = os.path.join(ruta_guardado, archivo)
    df = pd.read_csv(archivo_path)
    transacciones.append(df)
 ## Funcion para el analisis general
def resumen_transacciones(transacciones):
  """
  Función para generar un resumen de transacciones por tipo y método.
  Args:
    transacciones: Lista de DataFrames de transacciones.
  Returns:
    Lista de diccionarios con el resumen de transacciones.
  """
  for df in transacciones:
    ## Filtrar las transacciones de tipo 0
    df['TIPO_TRANSACCION'] = df['TIPO_TRANSACCION'].astype('str')
    df_filtro = df[df['TIPO_TRANSACCION'] == '0'].copy()

    ## Sacaremos el total de transacciones con el metodo count 
    tr_totales = df_filtro['TIPO_TRANSACCION'].count()

    ## Convertir la columna FECHA_HORA_TRANSACCION a datetime
    df_filtro['FECHA_HORA_TRANSACCION'] = pd.to_datetime(df_filtro['FECHA_HORA_TRANSACCION'])
    df_filtro['FECHA_HORA_TRANSACCION'] = df_filtro['FECHA_HORA_TRANSACCION'].dt.strftime('%Y-%m-%d')

    ## Separar las transacciones en base al método
    df_fisico = df_filtro[df_filtro['LOCATION_ID'] == '201A00']
    df_digital = df_filtro[df_filtro['LOCATION_ID'] == '101800']
    df_appcdmx = df_filtro[df_filtro['LOCATION_ID'] == '101801']

    ## Calcular el monto total por transacción física y agregar al DataFrame correspondiente
    monto_fisico = df_fisico['MONTO_TRANSACCION'].sum()

    ## Calcular el monto total por transacción digital y agregar al DataFrame correspondiente
    monto_digital = df_digital['MONTO_TRANSACCION'].sum()
    ## 
    monto_appcdmx = df_appcdmx['MONTO_TRANSACCION'].sum()

    ## Obtener los valores únicos de la fecha de transacción
    fechas_unicas = df_filtro['FECHA_HORA_TRANSACCION'].unique()

    ## Agregar los resultados a la lista de resumen 
    resumen.append({
        'FECHA': ', '.join(fechas_unicas),
        'TR Digitales': df_digital.shape[0],
        'TR Fisicas': df_fisico.shape[0],
        'TR AppCDMX': df_appcdmx.shape[0],
        'TR Totales': tr_totales,
        'Montos Digitales': monto_digital / 100,
        'Montos Fisicos': monto_fisico / 100,
        'Montos AppCDMX': monto_appcdmx / 100,
        'Monto Total': (monto_digital + monto_fisico + monto_appcdmx)/100,
    })
  return resumen
## Definir funcion para crear graficos:
def grafico_appCDMX(df,title,name,periodo):
  print(f"Creando Grafico AppCDMX..")
  ## Obtener los valores para el grafico
  dias = df['FECHA']
  appcdmx_tr = df['TR AppCDMX']
  appcdmx_mt = df['Montos AppCDMX']

  ## Declaracion del Grafico
  plt.style.use('seaborn-v0_8-paper')
  ## Definimos el objeto grafico (ancho,alto)
  fig,ax = plt.subplots(figsize=(18,8))
  ## Definimos el Area que ocupara el grafico
  plt.subplots_adjust(left=0.05, right=0.94, bottom=0.148, top=0.94)
  ## Valores base p
  ax2 = ax.twinx()
  p = ax.bar(dias,appcdmx_tr,label='Transacciones',color='#b81532')
  for i in enumerate(appcdmx_tr):
    ax.bar_label(p, label_type='center', color='#fff',fontsize=11,**{'fmt': '{:,.0f}'})
  for i, mt in enumerate(appcdmx_mt):
    ax2.annotate(f'${mt:,.0f}', (dias[i], mt), xytext=(-20,10), textcoords='offset points', fontsize=12, fontweight=600,color='#4b0615')  
  ax2.plot(dias,appcdmx_mt,label='Montos',color='#f8747e', marker='o',linestyle='solid')
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
  # Guarda el gráfico en alta resolución
  ruta_grafico = os.path.join(ruta_guardado, name)
  plt.savefig(ruta_grafico,format='png',dpi=980,bbox_inches='tight')
  
def grafico_digitales(df,title,name,periodo):
  print(f"Creando Grafico Digitales...")
  ##
  dias = df['FECHA']
  digitales_tr = df['TR Digitales'] 
  digitales_mt = df['Montos Digitales'] 
    ## Definir el estilo del grafico
  plt.style.use('seaborn-v0_8-paper')
    ## Definimos el objeto grafico (ancho,alto)
  fig,ax = plt.subplots(figsize=(18,8))
  plt.subplots_adjust(left=0.05, right=0.94, bottom=0.148, top=0.94)
    ## Valores base para las barras apiladas
    ## Colores para las barras
  ax2 = ax.twinx()
  p = ax.bar(dias,digitales_tr,label='Transacciones',color='#08acec')
  for i in enumerate(digitales_tr):
    ax.bar_label(p, label_type='center', color='#fff',fontsize=11,**{'fmt': '{:,.0f}'})
  for i, mt in enumerate(digitales_mt):
    ax2.annotate(f'${mt:,.0f}', (dias[i], mt), xytext=(-20,10), textcoords='offset points', fontsize=12, fontweight=600,color='#06314b')

  ax2.plot(dias,digitales_mt,label='Montos',color='#006fa5', marker='o',linestyle='solid')

    ## Creamos una legenda fuera del grafico
  fig.legend(loc='lower center', ncols=2, fontsize=12)
    # Ajusta el formato de los valores en el eje Y
  ax.yaxis.set_major_formatter(lambda x, pos: f'{x:,.0f}')
  ax2.yaxis.set_label_position("right")
  ax2.yaxis.set_major_formatter(lambda x, pos: f'${x:,.0f}')
    # Ajusta el título del gráfico
      # Ajusta el título del gráfico
  ##
  ax.tick_params(axis='x', labelsize=10)
  ax.tick_params(axis='y', labelsize=10)
  ax2.tick_params(axis='y', labelsize=10)
  ax.set_title(title,fontsize=14,fontweight=600)
  ax.set_xlabel(periodo,fontsize=12,fontweight=600)
  ax.set_ylabel('Transacciones',fontsize=12,fontweight=600)
  ax2.set_ylabel('Montos',fontsize=12,fontweight=600)
    # Guarda el gráfico en alta resolución
  ruta_grafico = os.path.join(ruta_guardado, name)
  plt.savefig(ruta_grafico,format='png',dpi=980,bbox_inches='tight')

  

def grafico_comercios(df,title,name,periodo):
  print(f"Creando Grafico Comercios..")
    ## Grafico
  dias = df['FECHA']
  comercios_tr = df['TR Fisicas']
  comercios_mt = df['Montos Fisicos'] 

    ## Definir el estilo del grafico
  plt.style.use('seaborn-v0_8-paper')
    ## Definimos el objeto grafico (ancho,alto)
  fig,ax = plt.subplots(figsize=(18,8))
  plt.subplots_adjust(left=0.05, right=0.94, bottom=0.148, top=0.94)
    ## Valores base para las barras apiladas
    ## Colores para las barras
  ax2 = ax.twinx()
  p = ax.bar(dias,comercios_tr,label='Transacciones',color='#af38c1')
  for i in enumerate(comercios_tr):
    ax.bar_label(p, label_type='center', color='#fff',fontsize=11, **{'fmt': '{:,.0f}'})
  for i, mt in enumerate(comercios_mt):
    ax2.annotate(f'${mt:,.0f}', (dias[i], mt), xytext=(-20,10), textcoords='offset points', fontsize=12, fontweight=600,color='#420c46')

  ax2.plot(dias,comercios_mt,label='Montos',color='#66246b', marker='o',linestyle='solid')

    ## Creamos una legenda fuera del grafico
  fig.legend(loc='lower center', ncols=2, fontsize=12)
    # Ajusta el formato de los valores en el eje Y
  ax2.yaxis.set_label_position("right")
  ax.yaxis.set_major_formatter(lambda x, pos: f'{x:,.0f}')
  ax2.yaxis.set_major_formatter(lambda x, pos: f'${x:,.0f}')
  # Ajusta el título del gráfico
  ##
  ax.tick_params(axis='x', labelsize=10)
  ax.tick_params(axis='y', labelsize=10)
  ax2.tick_params(axis='y', labelsize=10)
  ax.set_title(title,fontsize=14,fontweight=600)
  ax.set_xlabel(periodo,fontsize=12,fontweight=600)
  ax.set_ylabel('Transacciones',fontsize=12,fontweight=600)
  ax2.set_ylabel('Montos',fontsize=12,fontweight=600)
    # Guarda el gráfico en alta resolución
  ruta_grafico = os.path.join(ruta_guardado, name)
  plt.savefig(ruta_grafico,format='png',dpi=980,bbox_inches='tight')

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
      'AppCDMx': appcdmx_mt,
      'Comercios': comercios_mt,
      'Digitales': digitales_mt
  }

  x = np.arange(len(dias))  # the label locations
  width = 0.25  # the width of the bars
  multiplier = 0

  colors = {  # Dictionary of colors for each attribute
      'AppCDMx': '#b81532',
      'Comercios': '#af38c1',
      'Digitales': '#08acec'
  }
  lColors = {
    'AppCDMx': '#4b0615',
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
  for i, tr in enumerate(appcdmx_tr):
    ax2.annotate(f'{tr:,.0f}', (dias[i], tr), xytext=(-20,10), textcoords='offset points', fontsize=9, color='#4b0615')
  ax2.plot(dias,appcdmx_tr,label='Transacciones Appcdmx',color='#f8747e', marker='o',linestyle='solid')
  
  for i, tr in enumerate(comercios_tr):
    ax2.annotate(f'{tr:,.0f}', (dias[i], tr), xytext=(-20,10), textcoords='offset points', fontsize=9, color='#420c46')
  ax2.plot(dias,comercios_tr,label='Transacciones Comercios',color='#66246b', marker='o',linestyle='solid')
  
  for i, tr in enumerate(digitales_tr):
    ax2.annotate(f'{tr:,.0f}', (dias[i], tr), xytext=(-20,10), textcoords='offset points', fontsize=9, color='#06314b')
  ax2.plot(dias,digitales_tr,label='Transacciones Digitales',color='#006fa5', marker='o',linestyle='solid')
  
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
  ruta_grafico = os.path.join(ruta_guardado, name)
  plt.savefig(ruta_grafico,format='png',dpi=980,bbox_inches='tight')

resumen_transacciones(transacciones)
## Convertir el arreglo resumen en DataFrame
resultados = pd.DataFrame(resumen)
 
  
print(f"Trabajando con la semana {semana}")
df_transacciones = pd.concat(transacciones, ignore_index=True)
print("Generando datos en crudo")
archivo_full = f"Full__semana_{semana}.csv"
ruta_full = os.path.join(ruta_guardado, archivo_full)
df_transacciones.to_csv(ruta_full, index=False)
##   Extenciones
archivo_sem = f"RRE_{semana}.csv"
ruta_res_sem = os.path.join(ruta_guardado, archivo_sem)
resultados.to_csv(ruta_res_sem, index=False)
##
nameGrApp = f'AppCDMX_Grafico_Semana_{semana}.png'
titleApp = f'Comportamiento AppCDMX semana {semana}'
periodoApp = f'Semana {semana}'
grafico_appCDMX(resultados,titleApp,nameGrApp,periodoApp)
##
nameGrDig = f'Dig_Grafico_Semana_{semana}.png'
titleDig = f'Comportamiento Digitales semana {semana}'
periodoDig = f'Semana {semana}'
grafico_digitales(resultados,titleDig,nameGrDig,periodoDig)
##
nameGrCom = f'Com_Grafico_Semana_{semana}.png'
titleCom = f'Comportamiento Comercios semana {semana}'
periodoCom = f'Semana {semana}'
grafico_comercios(resultados,titleCom,nameGrCom,periodoCom)
##
nameGrRRE = f'RRE_Grafico_Semana_{semana}.png'
titleRRE = f'Comportamiento RRE semana {semana}'
periodoRRE = f'Semana {semana}'
grafico_rre(resultados,titleRRE,nameGrRRE,periodoRRE)
