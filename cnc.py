import requests
import json
from datetime import datetime
import os
import re
from time import sleep
import pandas  as pd
import pg
import notification
from dotenv import load_dotenv

load_dotenv()

cnc_data = [os.getenv('cnc_api_3'), os.getenv('cnc_api_5')]


def read_cnc_api_data(api_data):
    global prevs
    result = list()
    timestamp = datetime.now()
    for item in api_data.get('data', []):
        if len(item) > 1:
            entity = item[0]
            param_list = item[1]
            for param in param_list:
                if len(param) > 1:
                    param_name = param[0]
                    param_val = param[1]

                    result.append({
                        'entity': entity,
                        'param_name': param_name,
                        'param_val_str': param_val,
                        'timestamp': timestamp
                    })
    return pd.DataFrame(result)


def get_api_data(table):
    df = pd.DataFrame()
    for url in cnc_data:
        r = requests.get(url)
        json_data = json.loads(r.content)
        df_add = read_cnc_api_data(json_data)
        if(df_add.size == 0):
            continue
        df_add.insert(0, "url", url[-1])
        df = pd.concat([df,df_add ], ignore_index=True, sort=False).drop_duplicates()

    if(df.size > 0):
        pg.insert_table(df, table)
    return df

def start_cnc(list):
    error = {}
    while True:
        sleep(10)
        try:
            df = get_api_data('public.cnc')
            if(len(list) > 0):
                break
            if(df.size == 0):
                continue

            for _, row in df[df['param_name'] == 'Статус канала'].iterrows():
                if (row.get('param_val_str') in ['Ошибка'] or row.get('param_val_str') is None) and error.get(row.get('url')+row.get('entity')) is None:
                    notification.send_message(f"""CNC Машина {row.get('url')}, {row.get('entity')} вышла из работы с {row.get('param_name')} - {row.get('param_val_str')} """)
                    error[row.get('url')+row.get('entity')] = True
                elif row.get('param_val_str') == 'Работа' and error.get(row.get('url')+row.get('entity')) == True:
                    notification.send_message(f"""CNC Машина {row.get('url')}, {row.get('entity')} снова в работе""")
                    del error[row.get('url')+row.get('entity')]
        except Exception as e:
            print(e)
            print('Mistake on cnc')