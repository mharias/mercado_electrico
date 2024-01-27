import os.path as pth
import time
import pandas as pd
import numpy as np
import requests
import json
from datetime import datetime,timedelta,date
import sys

sys.path.append("/Users/mharias/Documents/proyectos/mylibs") # directorio de acceso a librerías auxiliares
sys.path.append('/home/waly00/claves')

from pass_esios import token_esios


# https://api.esios.ree.es/doc/indicator/getting_a_disaggregated_indicator_filtering_values_by_a_date_range_and_geo_ids,_grouped_by_geo_id_and_month,_using_avg_aggregation_for_geo_and_avg_for_time_without_time_trunc.html


def download_esios(token,indicadores,fecha_inicio,fecha_fin,time_trunc='day'):
    headers = {'Accept':'application/json; application/vnd.esios-api-v2+json',
           'Content-Type':'application/json',
           'Host':'api.esios.ree.es',
           'Cookie' : '',
           'Authorization':f'Token token={token}',
           'x-api-key': f'{token}', 
           'Cache-Control': 'no-cache',
           'Pragma': 'no-cache'
          }
    url = 'https://api.esios.ree.es/indicators'
    #url = 'https://apip.esios.ree.es/indicators'
    lista=[]

    for indicador in indicadores:
        
        url_ = f'{url}/{indicador}?start_date={fecha_inicio}T00:00&end_date={fecha_fin}T23:59&time_trunc={time_trunc}&geo_limit=peninsular'
        response = requests.get(url_, headers=headers).json()
        lista.append(pd.json_normalize(data=response['indicator'], record_path=['values'], meta=['name','short_name'], errors='ignore'))


    return pd.concat(lista, ignore_index=True )


def download_ree(indicador,fecha_inicio,fecha_fin,geo_limit='peninsular',geo_ids='8741',time_trunc='day'):
    headers = {'Accept': 'application/json',
               'Content-Type': 'applic<ation/json',
               'Host': 'apidatos.ree.es'}
    end_point = 'https://apidatos.ree.es/es/datos/'
    lista=[]
    
    
    url_ = f'{end_point}{indicador}?start_date={fecha_inicio}T00:00&end_date={fecha_fin}T23:59&geo_trunc=electric_system&geo_limit={geo_limit}&geo_ids={geo_ids}&time_trunc={time_trunc}'
    print (url_)
    response = requests.get(url_, headers=headers).json()
    
    lista.append(pd.json_normalize(data=response['included'], record_path=['attributes','values'], meta=['type',['attributes','type' ]], errors='ignore'))

    return pd.concat(lista, ignore_index=True )

def download_gas(year):
    path = f'https://www.mibgas.es/en/file-access/MIBGAS_Data_{year}.xlsx?path=AGNO_{year}/XLS'
    return (pd.
                  read_excel(path,sheet_name='Trading Data PVB&VTP',usecols=['Trading day','Product','Daily Reference Price\n[EUR/MWh]']).
       query("Product=='GDAES_D+1'").
       pipe(renombra_columnas,['Fecha','Producto','Precio']).
       sort_values('Fecha',ascending=True).
       reset_index(drop=True)
      )

def download_gas_rd(year):
    path = f'https://www.mibgas.es/en/file-access/MIBGAS_Data_{year}.xlsx?path=AGNO_{year}/XLS'
    return (pd.
                  read_excel(path,sheet_name='PGN_RD_10_2022',usecols=['Date','PGN Price\n[EUR/MWh]']).
       rename(columns={'Date':'fecha','PGN Price\n[EUR/MWh]':'precio'}).
       sort_values('fecha',ascending=True).
       reset_index(drop=True)
      )


def renombra_columnas(df,columnas):
    df.columns = columnas
    return df