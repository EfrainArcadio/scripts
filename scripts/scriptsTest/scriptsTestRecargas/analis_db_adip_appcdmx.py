import os
import pandas as pd

## Definimos el mes con nombre
mes = "Febrero"
## Definimos el mes con número
m = "02"
## Definimos el año
y = "2024"

## La ruta de trabaajo es la ruta donde se leen y se generan los archivos
ruta_trabajo = f"Transacciones/ADIP"

## Es el periodo en el que se realiza el analisis
periodo = "Febrero"

## Archivo a subir 
file_to_upload = 'mp_info_01-29_02_2024.csv'
archivo = os.path.join(ruta_trabajo, file_to_upload)
df = pd.read_csv(archivo, low_memory=False, encoding='latin-1')


ror = df[df['detalle'] == 'cc_rejected_other_reason']
ria = df[df['detalle'] == 'cc_rejected_insufficient_amount']
rhr = df[df['detalle'] == 'cc_rejected_high_risk']
rcfa = df[df['detalle'] == 'cc_rejected_call_for_authorize']
rbfsc = df[df['detalle'] == 'cc_rejected_bad_filled_security_code ']
## Fuera de Cat
pc = df[df['detalle'] == 'pending_challenge']
rbfd = df[df['detalle'] == 'cc_rejected_bad_filled_date']
rbfcn = df[df['detalle'] == 'cc_rejected_bad_filled_card_number']
rbl = df[df['detalle'] == 'cc_rejected_blacklist']
pcy = df[df['detalle'] == 'pending_contingency']
rcd = df[df['detalle'] == 'cc_rejected_card_disabled']
arle = df[df['detalle'] == 'cc_amount_rate_limit_exceeded']
rma = df[df['detalle'] == 'cc_rejected_max_attempts']

doc = []
doc.append({
    'Otras razones Bancarias': ror.shape[0],
    'Fondos Insuficientes': ria.shape[0],
    'Alto Riesgo de Fraude': rhr.shape[0],
    'Comunicarse con el banco para Autorizacion': rcfa.shape[0],
    'CVV incorrecto': rbfsc.shape[0],
    'Fuera de ': 'Catalogo',
    'pending_challenge': pc.shape[0],
    'cc_rejected_bad_filled_date': rbfd.shape[0],
    'cc_rejected_bad_filled_card_number': rbfcn.shape[0],
    'cc_rejected_blacklist': rbl.shape[0],
    'pending_contingency': pcy.shape[0],
    'cc_rejected_card_disabled': rcd.shape[0],
    'cc_amount_rate_limit_exceeded': arle.shape[0],
    'cc_rejected_max_attempts': rma.shape[0]
})                  

final = pd.DataFrame(doc)
archivo_mens = f"RE_BAD_TR_ADIP.csv"
ruta_res_mens = os.path.join(ruta_trabajo, archivo_mens)
final.to_excel(ruta_res_mens, index=False)
print('Proceso Finalizado con Exito..!!')
     
                    
                        
              
        
                      
                      
                  
           
             
  