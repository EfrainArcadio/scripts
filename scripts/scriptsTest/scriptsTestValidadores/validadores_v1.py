import os
import pandas as pd

## Definimos el mes con nombre
mes = "Julio"
## Definimos el mes con número
m = "07"
## Definimos el año
y = "2024"

name_file = '1ra_qna_Julio_2024'
## La ruta de trabaajo es la ruta donde se leen y se generan los archivos
ruta_trabajo = f"data/{y}/{m} {mes}"

## Es el periodo en el que se realiza el analisis
periodo = "1ra_qna"

## Archivo a subir 
file_to_upload = f'{name_file}.csv'

## metodo para asignar la ruta al archivo
archivo = os.path.join(ruta_trabajo, file_to_upload) 
df = pd.read_csv(archivo, low_memory=False, encoding='latin-1')

reemplazos = {
    '1': 'MIIT',
    '2': 'SAUSA',
    '3': 'ATROLSA',
    '4': 'CEUSA',
    '5': 'TRIOXA',
    '6': 'ACASA',
    '7': 'AULSA',
    '9': 'COPATTSA',
    '11': 'CODIVERSA',
    '12': 'COPESA',
    '13': 'TVO',
    '14':'COTAXOMIL',
    '15': 'ABC',
    '16': 'MOVIN',
    '17': 'COAVEO',
    'D': 'AMOPSA',
    'E': 'CETRAM ZAPATA',
    '34': 'CETRAM BUENAVISTA',
    '36': 'CETRAM TACUBAYA'
 }

for codigo, reemplazo in reemplazos.items():
    df.replace({'LINEA':codigo}, reemplazo, inplace=True)
lineas = set(df['LINEA'])

## Convetir la fecha a aun formato donde se puedan leer de forma unica
df['FECHA_HORA_TRANSACCION'] = pd.to_datetime(df['FECHA_HORA_TRANSACCION'], format="mixed")
df['FECHA_HORA_TRANSACCION'] = df['FECHA_HORA_TRANSACCION'].dt.strftime('%Y-%m-%d')

# ## Obtener fechas unicas
fechas_unicas = set(df['FECHA_HORA_TRANSACCION'])

## definimos un arreglo documento para almancenar los resultados del primer analisis
documento = []
for fecha in fechas_unicas:
    ## Filtrar el DataFrame por la fecha actual
    df['TIPO_TRANSACCION'] = df['TIPO_TRANSACCION'].astype('str')
    df_fecha = df[df['FECHA_HORA_TRANSACCION'] == fecha]
    
    ## Filtrar por tipo de transaccion
    df_bus = df_fecha[df_fecha['TIPO_TRANSACCION'] == '3'] # Debito en Bus
    df_ban = df_fecha[df_fecha['TIPO_TRANSACCION'] == '5'] # debito en baño
    df_pgo = df_fecha[df_fecha['TIPO_TRANSACCION'] == '70'] # Gratuidad de operacion
    df_ctf = df_fecha[df_fecha['TIPO_TRANSACCION'] == '71'] # Cambio de Tarifa
    df_tra = df_fecha[df_fecha['TIPO_TRANSACCION'] == '7'] # Transbordo
    df_sal = df_fecha[df_fecha['TIPO_TRANSACCION'] == '11'] # Salida
    df_gsp = df_fecha[df_fecha['TIPO_TRANSACCION'] == '4'] # Salida
    
    ## transacciones no exitosas, no permiten acceso
    df_gra = df_fecha[df_fecha['TIPO_TRANSACCION'] == '0D'] # rechazo de tarjeta en lista negra 
    df_rfw = df_fecha[df_fecha['TIPO_TRANSACCION'] == '13'] # rechazo de tarjeta fuera de la lista blanca
    df_sss = df_fecha[df_fecha['TIPO_TRANSACCION'] == '51'] # transaccio abortada por saldo insuficiente
    df_asm = df_fecha[df_fecha['TIPO_TRANSACCION'] == '53'] # Transaccion abortada por saldo mayor al maximo permitido
    df_ati = df_fecha[df_fecha['TIPO_TRANSACCION'] == '54'] # transaccion abortada por aplicacion de transporte invalido
    df_tai = df_fecha[df_fecha['TIPO_TRANSACCION'] == '55'] # Transaccion abortada por contrato invalido
    df_tax = df_fecha[df_fecha['TIPO_TRANSACCION'] == '61'] # Transaccion abortada por cualquier otro caso
    df_tve = df_fecha[df_fecha['TIPO_TRANSACCION'] == '60'] # Transaccion abortada por vigencia expirada
    df_ff = df_fecha[df_fecha['TIPO_TRANSACCION'] == 'FF'] # Transaccion sin descripcion

    # Montos
    monto_cetrams = sum(df_ban['MONTO_TRANSACCION']) / 100
    monto_buses = sum(df_bus['MONTO_TRANSACCION']) / 100

    # Transacciones
    tr_cetrams = df_ban.shape[0]
    tr_buses = df_bus.shape[0]
 
    documento.append({
        'Fecha': fecha,
        'Debitos en autobus (E)': df_bus.shape[0],
        'Debitos en baño (E)': df_ban.shape[0],
        'Valor de Debitos en autobus': monto_buses,
        'Valor de Debitos en baños': monto_cetrams,
        'Gratuidad por Operacion': df_pgo.shape[0],
        'Gratuidad Supervisor': df_gsp.shape[0],
        'Transbordo (E)': df_tra.shape[0],
        'Salida (E)': df_sal.shape[0],
        'Rechazo de tarjeta por lista negra': df_gra.shape[0],
        'Rechazo de tarjeta por recarga fuera de lista blanca': df_rfw.shape[0],
        'Sin saldo suficiente': df_sss.shape[0],
        'Transaccion abortada por saldo mayor': df_asm.shape[0],
        'Transaccion abortada por aplicacion de transporte invalido': df_ati.shape[0],
        'Transaccion abortada por contrato invalido (firma erronea)': df_tai.shape[0],
        'Transaccion abortada (cualquier otro caso)': df_tax.shape[0],
        'Transaccion abortada por vigencia expirada': df_tve.shape[0],
        'FF': df_ff.shape[0],
        'Cambio de Tarifa': df_ctf.shape[0]
    })
# Create a DataFrame from the list of dictionaries
resultados = pd.DataFrame(documento)
# orena el DataFrame por 'Fecha' en modo ascendente 
resultados = resultados.sort_values(by='Fecha')
# Calcula todos los totales
sum_row = {
    'Fecha': 'Total',
    'Debitos en autobus (E)': resultados['Debitos en autobus (E)'].sum(),
    'Debitos en baño (E)': resultados['Debitos en baño (E)'].sum(),
    'Valor de Debitos en autobus': resultados['Valor de Debitos en autobus'].sum(),
    'Valor de Debitos en baños': resultados['Valor de Debitos en baños'].sum(),
    'Gratuidad por Operacion': resultados['Gratuidad por Operacion'].sum(),
    'Gratuidad Supervisor': resultados['Gratuidad Supervisor'].sum(),
    'Transbordo (E)': resultados['Transbordo (E)'].sum(),
    'Salida (E)': resultados['Salida (E)'].sum(),
    'Rechazo de tarjeta por lista negra': resultados['Rechazo de tarjeta por lista negra'].sum(),
    'Rechazo de tarjeta por recarga fuera de lista blanca': resultados['Rechazo de tarjeta por recarga fuera de lista blanca'].sum(),
    'Sin saldo suficiente': resultados['Sin saldo suficiente'].sum(),
    'Transaccion abortada por saldo mayor': resultados['Transaccion abortada por saldo mayor'].sum(),
    'Transaccion abortada por aplicacion de transporte invalido': resultados['Transaccion abortada por aplicacion de transporte invalido'].sum(),
    'Transaccion abortada por contrato invalido (firma erronea)': resultados['Transaccion abortada por contrato invalido (firma erronea)'].sum(),
    'Transaccion abortada (cualquier otro caso)': resultados['Transaccion abortada (cualquier otro caso)'].sum(),
    'Transaccion abortada por vigencia expirada': resultados['Transaccion abortada por vigencia expirada'].sum(),
    'FF': resultados['FF'].sum(),
    'Cambio de Tarifa': resultados['Cambio de Tarifa'].sum()
}

resultados = pd.concat([resultados, pd.DataFrame([sum_row])], ignore_index=True)


documento_linea = []
for linea in lineas:
    # Filtrar el DataFrame por la línea actual
    df_linea = df[df['LINEA'] == str(linea)]
    ## transacciones exitosas, permiten el acceso
    lin_bus = df_linea[df_linea['TIPO_TRANSACCION'] == '3'] # Debito en Bus
    lin_ban = df_linea[df_linea['TIPO_TRANSACCION'] == '5'] # debito en baño
    lin_pgo = df_linea[df_linea['TIPO_TRANSACCION'] == '70'] # Gratuidad de operacion
    lin_ctf = df_linea[df_linea['TIPO_TRANSACCION'] == '71'] # Cambio de Tarifa
    lin_tra = df_linea[df_linea['TIPO_TRANSACCION'] == '7'] # Transbordo
    lin_sal = df_linea[df_linea['TIPO_TRANSACCION'] == '11'] # Salida
    lin_gsp = df_linea[df_linea['TIPO_TRANSACCION'] == '4'] # Salida
    ## transacciones no exitosas, no permiten acceso
    lin_gra = df_linea[df_linea['TIPO_TRANSACCION'] == '0D'] # rechazo de tarjeta en lista negra 
    lin_rfw = df_linea[df_linea['TIPO_TRANSACCION'] == '13'] # rechazo de tarjeta fuera de la lista blanca
    lin_sss = df_linea[df_linea['TIPO_TRANSACCION'] == '51'] # transaccio abortada por saldo insuficiente
    lin_asm = df_linea[df_linea['TIPO_TRANSACCION'] == '53'] # Transaccion abortada por saldo mayor al maximo permitido
    lin_ati = df_linea[df_linea['TIPO_TRANSACCION'] == '54'] # transaccion abortada por aplicacion de transporte invalido
    lin_tai = df_linea[df_linea['TIPO_TRANSACCION'] == '55'] # Transaccion abortada por contrato invalido
    lin_tax = df_linea[df_linea['TIPO_TRANSACCION'] == '61'] # Transaccion abortada por cualquier otro caso
    lin_tve = df_linea[df_linea['TIPO_TRANSACCION'] == '60'] # Transaccion abortada por vigencia expirada
    lin_ff = df_linea[df_linea['TIPO_TRANSACCION'] == 'FF'] # Tramsaccion sin descripcion
    ## montos
    lin_mbu = sum(lin_bus['MONTO_TRANSACCION']) / 100
    lin_mba = sum(lin_ban['MONTO_TRANSACCION']) / 100
    documento_linea.append({
        'Linea': linea,
        'Debitos en autobus': lin_bus.shape[0],
        'Debitos en baño': lin_ban.shape[0],
        'Valor de Debitos en autobus': lin_mbu,
        'Valor de Debitos en baños': lin_mba,
        'Gratuidad': lin_pgo.shape[0],
        'Gratuidad Supervisor': lin_gsp.shape[0],
        'Transbordo': lin_tra.shape[0],
        'Salida': lin_sal.shape[0],
        'Rechazo de tarjeta en lista negra': lin_gra.shape[0],
        'Rechazo de tarjeta por recarga fuera de lista blanca': lin_rfw.shape[0],
        'Sin saldo suficiente': lin_sss.shape[0],
        'Transaccion abortada por saldo mayor': lin_asm.shape[0],
        'Transaccion abortada por aplicacion de transporte invalido': lin_ati.shape[0],
        'Transaccion abortada por contrato invalido (firma erronea)': lin_tai.shape[0],
        'Transaccion abortada (cualquier otro caso)': lin_tax.shape[0],
        'Transaccion abortada por vigencia expirada': lin_tve.shape[0],
        'FF': lin_ff.shape[0],
        'Cambio de Tarifa': lin_ctf.shape[0]
    })

    
resultados_lin = pd.DataFrame(documento_linea)
# Calcula los totales
sum_rows = {
    'Linea': 'Total',
    'Debitos en autobus': resultados_lin['Debitos en autobus'].sum(),
    'Debitos en baño': resultados_lin['Debitos en baño'].sum(),
    'Valor de Debitos en autobus': resultados_lin['Valor de Debitos en autobus'].sum(),
    'Valor de Debitos en baños': resultados_lin['Valor de Debitos en baños'].sum(),
    'Gratuidad': resultados_lin['Gratuidad'].sum(),
    'Gratuidad Supervisor': resultados_lin['Gratuidad Supervisor'].sum(),
    'Transbordo': resultados_lin['Transbordo'].sum(),
    'Salida': resultados_lin['Salida'].sum(),
    'Rechazo de tarjeta en lista negra': resultados_lin['Rechazo de tarjeta en lista negra'].sum(),
    'Rechazo de tarjeta por recarga fuera de lista blanca': resultados_lin['Rechazo de tarjeta por recarga fuera de lista blanca'].sum(),
    'Sin saldo suficiente': resultados_lin['Sin saldo suficiente'].sum(),
    'Transaccion abortada por saldo mayor': resultados_lin['Transaccion abortada por saldo mayor'].sum(),
    'Transaccion abortada por aplicacion de transporte invalido': resultados_lin['Transaccion abortada por aplicacion de transporte invalido'].sum(),
    'Transaccion abortada por contrato invalido (firma erronea)': resultados_lin['Transaccion abortada por contrato invalido (firma erronea)'].sum(),
    'Transaccion abortada (cualquier otro caso)': resultados_lin['Transaccion abortada (cualquier otro caso)'].sum(),
    'Transaccion abortada por vigencia expirada': resultados_lin['Transaccion abortada por vigencia expirada'].sum(),
    'FF': resultados_lin['FF'].sum(),
    'Cambio de Tarifa': resultados_lin['Cambio de Tarifa'].sum()
}

resultados_lin = pd.concat([resultados_lin, pd.DataFrame([sum_rows])], ignore_index=True)


## Creamos un arreglo que contenga la estructura que tienen o a quien pertenecen segun el integrador
integradores = [
    [
        'MIIT',
        'SAUSA',
        'ATROLSA',
        'CEUSA',
        'COPATTSA',
        'AMOPSA',
        'CETRAM ZAPATA',
    ],
    [
        'AULSA',
        'CODIVERSA',
        'TVO',
        'CETRAM BUENAVISTA',
        'CETRAM TACUBAYA',
    ],
    [
        'ABC',
        'MOVIN'
    ],
     [
        'TRIOXA',
    ],
    [
        'ACASA',
    ],
    [
        'COPESA',
    ],
    [
        'COAVEO',
        'COTAXOMIL',
    ],
        
]

## Definimos el integrador segun su posicion en el arreglo anterior
microsafe = integradores[0]
conduent = integradores[1]
jm = integradores[2]
bea = integradores[3]
mpeso= integradores[4]
insitra= integradores[5]
scsoft= integradores[6]

## Creamos una funcion para realizar el analisis por integrador
def resumen_integrador(df_integrador,df_final):
    """ 
    Args:
        df_integrador: ejemplo microsafe 
        df_final: Es el frame donde seguardara el resultado del analisis 
    """
    ## ciclo for que itera sobre la lista de integradores
    for linea in df_integrador:
        ## obtenemos un nuevo dataframe que contenga solamente la linea igual al integrador
        df_int = df[df['LINEA'] == str(linea)]
        ## Obtenemos los autobuses unicos para crear una lista
        autobuses = df_int['AUTOBUS'].unique()
        ## Ciclo for que itera sobre la lista de autobuses 
        for bus in autobuses:
            df_buses = df_int[df_int['AUTOBUS'] == bus]
            ##
            df_bus = df_buses[df_buses['TIPO_TRANSACCION'] == '3'] # Debito en Bus
            df_ban = df_buses[df_buses['TIPO_TRANSACCION'] == '5'] # debito en baño
            df_pgo = df_buses[df_buses['TIPO_TRANSACCION'] == '70'] # Gratuidad de operacion
            df_ctf = df_buses[df_buses['TIPO_TRANSACCION'] == '71'] # Cambio de Tarifa
            df_tra = df_buses[df_buses['TIPO_TRANSACCION'] == '7'] # Transbordo
            df_sal = df_buses[df_buses['TIPO_TRANSACCION'] == '11'] # Salida
            df_gsp = df_buses[df_buses['TIPO_TRANSACCION'] == '4'] # Gratuidad Supervisor
            ## transacciones no exitosas, no permiten acceso
            df_gra = df_buses[df_buses['TIPO_TRANSACCION'] == '0D'] # rechazo de tarjeta en lista negra 
            df_rfw = df_buses[df_buses['TIPO_TRANSACCION'] == '13'] # rechazo de tarjeta fuera de la lista blanca
            df_sss = df_buses[df_buses['TIPO_TRANSACCION'] == '51'] # transaccio abortada por saldo insuficiente
            df_asm = df_buses[df_buses['TIPO_TRANSACCION'] == '53'] # Transaccion abortada por saldo mayor al maximo permitido
            df_ati = df_buses[df_buses['TIPO_TRANSACCION'] == '54'] # transaccion abortada por aplicacion de transporte invalido
            df_tai = df_buses[df_buses['TIPO_TRANSACCION'] == '55'] # Transaccion abortada por contrato invalido
            df_tax = df_buses[df_buses['TIPO_TRANSACCION'] == '61'] # Transaccion abortada por cualquier otro caso
            df_tve = df_buses[df_buses['TIPO_TRANSACCION'] == '60'] # Transaccion abortada por vigencia expirada
            df_ff = df_buses[df_buses['TIPO_TRANSACCION'] == 'FF'] # Transaccion sin descripcion
            ## montos
            df_final.append({
                'Autobus': bus,
                'Linea': linea,
                'Debitos en autobus': df_bus.shape[0],
                'Debitos en baño': df_ban.shape[0],
                'Gratuidad': df_pgo.shape[0],
                'Gratuidad Supervisor': df_gsp.shape[0],
                'Transbordo': df_tra.shape[0],
                'Salida': df_sal.shape[0],
                'Rechazo de tarjeta en lista negra': df_gra.shape[0],
                'Rechazo de tarjeta por recarga fuera de lista blanca': df_rfw.shape[0],
                'Sin saldo suficiente': df_sss.shape[0],
                'Transaccion abortada por saldo mayor': df_asm.shape[0],
                'Transaccion abortada por aplicacion de transporte invalido': df_ati.shape[0],
                'Transaccion abortada por contrato invalido (firma erronea)': df_tai.shape[0],
                'Transaccion abortada (cualquier otro caso)': df_tax.shape[0],
                'Transaccion abortada por vigencia expirada': df_tve.shape[0],
                'FF': df_ff.shape[0],
                'Cambio de Tarifa':df_ctf.shape[0]
            })

        res_microsafe = pd.DataFrame(df_final)

        # Calcula los totales
        sum_row_mi = {
            'Autobus': "#",
            'Linea': 'Total',
            'Debitos en autobus': res_microsafe['Debitos en autobus'].sum(),
            'Debitos en baño': res_microsafe['Debitos en baño'].sum(),
            'Gratuidad Supervisor': res_microsafe['Gratuidad Supervisor'].sum(),
            'Transbordo': res_microsafe['Transbordo'].sum(),
            'Salida': res_microsafe['Salida'].sum(),
            'Rechazo de tarjeta en lista negra': res_microsafe['Rechazo de tarjeta en lista negra'].sum(),
            'Rechazo de tarjeta por recarga fuera de lista blanca': res_microsafe['Rechazo de tarjeta por recarga fuera de lista blanca'].sum(),
            'Sin saldo suficiente': res_microsafe['Sin saldo suficiente'].sum(),
            'Transaccion abortada por saldo mayor': res_microsafe['Transaccion abortada por saldo mayor'].sum(),
            'Transaccion abortada por aplicacion de transporte invalido': res_microsafe['Transaccion abortada por aplicacion de transporte invalido'].sum(),
            'Transaccion abortada por contrato invalido (firma erronea)': res_microsafe['Transaccion abortada por contrato invalido (firma erronea)'].sum(),
            'Transaccion abortada (cualquier otro caso)': res_microsafe['Transaccion abortada (cualquier otro caso)'].sum(),
            'Transaccion abortada por vigencia expirada': res_microsafe['Transaccion abortada por vigencia expirada'].sum(),
            'FF': res_microsafe['FF'].sum(),
            'Cambio de Tarifa': res_microsafe['Cambio de Tarifa'].sum()
        }
        ## Cambiar metodo de guardado XLSX con hojas por integrador
        res_microsafe_fn = pd.concat([res_microsafe, pd.DataFrame([sum_row_mi])], ignore_index=True)
    return res_microsafe_fn


df_microsafe = []    
resumen_integrador(microsafe,df_microsafe)
df_micro = pd.DataFrame(df_microsafe)
bus_micro = len(df_micro)


df_conduent = []
resumen_integrador(conduent,df_conduent)
df_cond = pd.DataFrame(df_conduent)
bus_cond = len(df_cond)


df_jm = []
resumen_integrador(jm,df_jm)
df_jotaeme = pd.DataFrame(df_jm)
bus_jotaeme = len(df_jotaeme)


df_bea = []
resumen_integrador(bea,df_bea)
df_be = pd.DataFrame(df_bea)
bus_be = len(df_be)


df_mpeso = []
resumen_integrador(mpeso,df_mpeso)
df_mpe = pd.DataFrame(df_mpeso)
bus_mpe = len(df_mpe)


df_insitra = []
resumen_integrador(insitra,df_insitra)
df_insi = pd.DataFrame(df_insitra)
bus_insi = len(df_insi)

df_scsoft = []
resumen_integrador(scsoft,df_scsoft)
df_scsoft = pd.DataFrame(df_scsoft)
bus_scsoft = len(df_scsoft)


def totalTran_no_validas(df):
    tb1 = df['Rechazo de tarjeta en lista negra'].sum()
    tb2 = df['Rechazo de tarjeta por recarga fuera de lista blanca'].sum()
    tb3 = df['Sin saldo suficiente'].sum()
    tb4 = df['Transaccion abortada por saldo mayor'].sum()
    tb5 = df['Transaccion abortada por aplicacion de transporte invalido'].sum()
    tb6 = df['Transaccion abortada por contrato invalido (firma erronea)'].sum()
    tb7 = df['Transaccion abortada (cualquier otro caso)'].sum()
    tb8 = df['Transaccion abortada por vigencia expirada'].sum()
    tb9 = df['FF'].sum()
    total = tb1 + tb2 + tb3 + tb4 + tb5 + tb6 + tb7 + tb8 + tb9
    return total

def totalTran_validas(df):
    tt1 = df['Debitos en autobus'].sum()
    tt2 = df['Debitos en baño'].sum()
    tt3 = df['Gratuidad'].sum()
    tt6 = df['Gratuidad Supervisor'].sum()
    tt4 = df['Transbordo'].sum()
    tt5 = df['Salida'].sum()
    tt7 = df['Cambio de Tarifa'].sum()
    total = tt1 + tt2 + tt3 + tt4 + tt5 +tt6 +tt7
    return total

## 
ttnv_insi = totalTran_no_validas(df_insi)
ttnv_mpe = totalTran_no_validas(df_mpe)
ttnv_be = totalTran_no_validas(df_be)
ttnv_jotaeme = totalTran_no_validas(df_jotaeme)
ttnv_cond = totalTran_no_validas(df_cond)
ttnv_micro = totalTran_no_validas(df_micro)
ttnv_scsoft = totalTran_no_validas(df_scsoft)
ttnv = ttnv_insi + ttnv_mpe + ttnv_be + ttnv_jotaeme + ttnv_cond + ttnv_micro + ttnv_scsoft

## 
ttv_insi = totalTran_validas(df_insi)
ttv_mpe = totalTran_validas(df_mpe)
ttv_be = totalTran_validas(df_be)
ttv_jotaeme = totalTran_validas(df_jotaeme)
ttv_cond = totalTran_validas(df_cond)
ttv_micro = totalTran_validas(df_micro)
ttv_scsoft = totalTran_validas(df_scsoft)
ttv = ttv_insi + ttv_mpe +ttv_be + ttv_jotaeme + ttv_cond + ttv_micro + ttv_scsoft


resumen = {
    'Integrador': ['Microsafe','Conduent','Insitra','Mpeso','Bea','JM','ScSoft','Totales'],
    '# No Exitosas': [ttnv_micro,ttnv_cond,ttnv_insi,ttnv_mpe,ttnv_be,ttnv_jotaeme,ttnv_scsoft,ttnv],
    '% No Exitosas': [ttnv_micro/ttnv,ttnv_cond/ttnv,ttnv_insi/ttnv,ttnv_mpe/ttnv,ttnv_be/ttnv,ttnv_jotaeme/ttnv,ttnv_scsoft/ttnv,''],
    'No Exitosas x Unidad': [ttnv_micro/bus_micro,ttnv_cond/bus_cond,ttnv_insi/bus_insi,ttnv_mpe/bus_mpe,ttnv_be/bus_be,ttnv_jotaeme/bus_jotaeme,ttnv_scsoft/bus_scsoft,'Maxima: '],
    '# Exitosas': [ttv_micro,ttv_cond,ttv_insi,ttv_mpe,ttv_be,ttv_jotaeme,ttnv_scsoft,ttv],
    '% Exitosas': [ttv_micro/ttv,ttv_cond/ttv,ttv_insi/ttv,ttv_mpe/ttv,ttv_be/ttv,ttv_jotaeme/ttv,ttv_scsoft/ttv,''],
}

res_int = pd.DataFrame(resumen)

lista = f"Analisis_validaciones_{periodo}_{mes}.xlsx"
ruta_lista = os.path.join(ruta_trabajo,lista)
with pd.ExcelWriter(ruta_lista) as writer:
    res_int.to_excel(writer, index=False ,sheet_name=f'Resumen Integrador {mes}')
    resultados.to_excel(writer, index=False ,sheet_name=f'Resumen Validaciones {mes}')
    resultados_lin.to_excel(writer, index=False ,sheet_name=f'Resumen Linea {mes}')
    df_micro.to_excel(writer, index=False ,sheet_name=f'Transacciones Microsafe {mes}')
    df_cond.to_excel(writer, index=False ,sheet_name=f'Transacciones  Conduent {mes}')
    df_insi.to_excel(writer, index=False ,sheet_name=f'Transacciones Insitra {mes}')
    df_mpe.to_excel(writer, index=False ,sheet_name=f'Transacciones Mpeso {mes}')
    df_be.to_excel(writer, index=False ,sheet_name=f'Transacciones Bea {mes}')
    df_jotaeme.to_excel(writer, index=False ,sheet_name=f'Transacciones JM {mes}')
    df_scsoft.to_excel(writer, index=False ,sheet_name=f'Transacciones ScSoft {mes}')
    
print('Proceso Finalizado!!')