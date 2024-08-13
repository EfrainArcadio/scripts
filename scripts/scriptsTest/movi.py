import pandas as pd
import os
ruta_trabajo = 'Transacciones/Transacciones_Limpias_2Quin_Marzo_2024'

def leer_csv(archivo_o):
    archivo_l = 'Cat Estaciones STC MB STE-TL STE CB.csv'
    archivo_path_l = os.path.join(ruta_trabajo,archivo_l)
    df_l= pd.read_csv(archivo_path_l,low_memory=False)
    #
    archivo_path = os.path.join(ruta_trabajo,archivo_o)
    df = pd.read_csv(archivo_path,low_memory=False)
    # Agregar encabezados estandarizados
    df.columns = ["organismo", "csdec", "fecha_hora", "operacion", "monto", "saldo_final", "loc_id"]
    ##
    df['fecha_hora'] = pd.to_datetime(df['fecha_hora'])
    df['fecha_hora'] = df['fecha_hora'].dt.strftime('%Y-%m-%d')
    ##
    organismo = df['organismo'].unique()
    id_org = organismo[0]
    ##
    df = df[df['operacion'] == 0]
    dias = ['2024-03-19','2024-03-20','2024-03-21']
    ## 
    new_ids = []
    for lid in df['loc_id']:
        nid = str(lid)
        new_ids.append(nid[:-3])
    df['loc_id_m'] = new_ids
    locs = set(df['loc_id_m'])
    locs_lista = list(locs)
    locs_lista.sort()
    df_org = df_l[df_l['provider'] == id_org]
    org = df_org['organismo'].values
    # print(df_org)
    resumen = []
    resumenl = []
    for dia in dias:
        df_d = df[df['fecha_hora'] == dia]
        # print(df_d)
        resumen.append({
            'Dia': dia,
            'Transacciones': len(df_d),
            'Montos': sum(df_d['monto'])
        })
        for nloc in locs_lista:
            est = df_d[df_d['loc_id_m'] == nloc]
            
            nest = df_org[df_org['location_id'] == nloc]
            if not nest.empty:
                nest_n = nest['estacion_nom']
                # print(nest_n.values)
                st = nest_n.values
                resumenl.append({
                    'Estacion': st[0],
                    'loc_id': nloc,
                    'Monto': sum(est['monto']),
                    'Transacciones': len(est),
                    'dia': dia
                                })
    # print(resumen)
    df_final = pd.DataFrame(resumenl)
    df_fina = pd.DataFrame(resumen)
    # print(df_fina)
    # print(df_final)
    lista = f"Analisis_{org[0]}.xlsx"
    ruta_xlsx = os.path.join(ruta_trabajo,lista)
    with pd.ExcelWriter(ruta_xlsx) as writer:
        df_fina.to_excel(writer, index=False ,sheet_name=f'Resumen Dia {org[0]}')
        df_final.to_excel(writer, index=False ,sheet_name=f'Resumen Linea {org[0]}')
    print(f'Archivo {org[0]} Finalizado')

leer_csv('STE_2024-03-Q2.csv')
leer_csv('STC_2024-03-Q2.csv')
leer_csv('CB_2024-03-Q2.csv')
leer_csv('MB_2024-03-Q2.csv')



