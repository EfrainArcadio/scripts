import pandas as pd
import os

## Define: mes ="Febrero" --- Nombre del mes
mes = "Julio"
## Define: m ="02"  ---- Numero del mes
m = "07"
## Define: y ="2024" --- Año a tomar en cuenta en el analisis
y = "2024"
## Rango de dias semanales L - V Agregando + 1 al viernes ** Leer documentacion metodo range
diaI = 15
diaF = 20
sem = range(diaI,diaF)
## Dia más afluente de la semana
dma = '17'
## Sabado
sb = '20'
## Domingo
dm = '21'
## ------- Definir datos de Entrada --------
ruta_trabajo = f"data/{y}/{m} {mes}/"
archivo = 'Validaciones del 15 al 21 de julio 2024.csv'
archivo = os.path.join(ruta_trabajo, archivo)
# Lectura del archivo de Entrada
df = pd.read_csv(archivo, low_memory=False, encoding='latin-1')
## ------- Definir datos de Salida --------
## Semana completa
semana = '15 al 21'
## Nombre del archivo y ruta de salida
fn = f'RE_BUS_{semana}_{mes}.xlsx'
ruta_doc = os.path.join(ruta_trabajo, fn)
## Diccionario de Corredores el numero corresponde al numero de LINEA de la estructura de datos
json_info = {
    '1': {'nombre': 'MIIT', 'pv': 142},
    '2': {'nombre': 'SAUSA', 'pv': 66},
    '3': {'nombre': 'ATROLSA', 'pv': 57},
    '4': {'nombre': 'CEUSA', 'pv': 65},
    '7': {'nombre': 'AULSA', 'pv': 62},
    '11': {'nombre': 'CODIVERSA', 'pv': 54},
    '14': {'nombre': 'COTAXOMIL', 'pv': 129},
    '15': {'nombre': 'ABC', 'pv': 76},
    '16': {'nombre': 'MOVIN', 'pv': 86},
}
## Convertir el TIPO_TRANSACCION a 
df['TIPO_TRANSACCION'] = df['TIPO_TRANSACCION'].astype(str)
df_val = df[df['TIPO_TRANSACCION'] == '3']
## Remplazar El Codigo de la linea por nombre
for codigo, info in json_info.items():
    df_val.replace({'LINEA': codigo}, info['nombre'], inplace=True)
## Ordenar json_info alfabeticamente por nombre
json_info = dict(sorted(json_info.items(), key=lambda item: item[1]['nombre']))

resem = []
## Crear
for codigo, info in json_info.items():
    ## Dataframe por Linea
    df_cons = df_val[df_val['LINEA'] == info['nombre']]
    ## Data BASE
    data = {'Concesionario': info['nombre'], 'Parque Vehicular Total': info['pv']} 
    autobuses_por_hora = {f"{hora:02d}:00": 0 for hora in range(24)}
    for dia in sem:
        # print(dia)
        if dia < 10:
          dia = f'0{dia}'
        for hora in range(0, 24):
            inicio = f"{dia}/{m}/{y} {hora:02d}:00:00"
            fin = f"{dia}/{m}/{y} {(hora+1):02d}:00:00"
            df_hora = df_cons[(df_cons['FECHA_HORA_TRANSACCION'] >= inicio) & (df_cons['FECHA_HORA_TRANSACCION'] < fin)]
            autobuses_por_hora[f"{hora:02d}:00"] += len(df_hora['AUTOBUS'].unique())
    # Calcular el promedio de autobuses por hora
    for hora in autobuses_por_hora:
        autobuses_por_hora[hora] /= 5
        data[hora] = autobuses_por_hora[hora]
    # Append data for current line to the results
    resem.append(data)
# Create Pandas DataFrame from results
prom_lv = pd.DataFrame(resem)

## Estructura SEMANAL

## Crear
def diasUnicos(array,dia):
    for codigo, info in json_info.items():
        ## Dataframe por Linea
        df_cons = df_val[df_val['LINEA'] == info['nombre']]
        ## Data BASE
        data = {'Concesionario': info['nombre'], 'Parque Vehicular Total': info['pv']} 
        for hora in range(0, 24):
            inicio = f"{dia}/{m}/{y} {hora:02d}:00:00"
            fin = f"{dia}/{m}/{y} {(hora+1):02d}:00:00"
            df_hora = df_cons[(df_cons['FECHA_HORA_TRANSACCION'] >= inicio) & (df_cons['FECHA_HORA_TRANSACCION'] < fin)]
            data[f"{hora:02d}:00"] = len(df_hora['AUTOBUS'].unique())
        # Append data for current line to the results
        array.append(data)
    # Create Pandas DataFrame from results

redma = []
diasUnicos(redma,dma)
resumen_dma = pd.DataFrame(redma)
resb = []
diasUnicos(resb,sb)
resumen_sb = pd.DataFrame(resb)
redm = []
diasUnicos(redm,dm)
resumen_dm = pd.DataFrame(redm)


# ## Crear XLSX
with pd.ExcelWriter(ruta_doc) as writer:
    prom_lv.to_excel(writer, index=False, sheet_name=f'PROM_AUTOBUS_L-V')
    resumen_dma.to_excel(writer, index=False, sheet_name=f'AUTOBUS_{dma}')
    resumen_sb.to_excel(writer, index=False, sheet_name=f'AUTOBUS_S')
    resumen_dm.to_excel(writer, index=False, sheet_name=f'AUTOBUS_D')


