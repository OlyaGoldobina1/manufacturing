import requests
import json
from datetime import datetime
import os
import re
from time import sleep
import pandas as pd
import pg
import notification

cnc_data = ['https://cnc.kovalev.team/get/3', 'https://cnc.kovalev.team/get/5']
login = '{"dbname": "postgres","user": "postgres","password": "posrgres","host": "212.233.99.38","port": "5432","client_encoding": "utf-8"}'
def param_val_to_float(val):
    val = str(val)
    result = re.sub('[^\d,-\.]', '', val).replace(',', '.')
    try:
        return float(result)
    except Exception as e:
        return None
    
prev = {}

def read_cnc_api_data(api_data, url):
    global prevs
    result = list()
    timestamp = datetime.now()
    ret = False
    for item in api_data.get('data', []):
        if len(item) > 1:
            entity = item[0]
            param_list = item[1]
            for param in param_list:
                if len(param) > 1:
                    param_name = param[0]
                    param_val = param[1]
                    key = entity+param_name + url
                    if(key not in prev):
                        prev[key] = ''
                    
                    if(prev[key]!=param_val):
                        ret = True
                        prev[key]=param_val

                    result.append({
                        'entity': entity,
                        'param_name': param_name,
                        'param_val_str': param_val,
                        #'param_val_float': param_val_to_float(param_val),
                        'timestamp': timestamp
                    })
    if(not ret):
        return pd.DataFrame()
    return pd.DataFrame(result)


def get_api_data(table):
    #global prev
    df = pd.DataFrame()
    for url in cnc_data:
        r = requests.get(url)
        json_data = json.loads(r.content)
        df_add = read_cnc_api_data(json_data, url)
        if(df_add.size == 0):
            continue

        # if(len(prev.columns) == 0 or prev.compare(df_add.drop(['timestamp'], axis=1)) is not None):
        #     prev = df_add.drop(['timestamp'], axis=1)
        # else:
        #     return df_add
# 
        df_add.insert(0, "url", url[-1])
        df = pd.concat([df,df_add ], ignore_index=True, sort=False).drop_duplicates()

    if(df.size > 0):
        pg.insert_table(df, table, login=login)
    return df

def start_cnc(list):
    sent = False
    while True:
        sleep(1)
        try:
            df = get_api_data('public.cnc')
 
            if(len(list) > 0):
                break
            if(df.size == 0):
                continue
            row = df[df['param_name'] == 'Статус канала'].iloc[0]
            status = row.get('param_val_str')
            if not sent and status == 'Ошибка':
                entity = row.get('entity')
                param = row.get('param_name')
                machine = row.get('URL')
                sent = True
                usrids = pg.query_to_df('select chat_id from users', login=login)
                usrids.drop_duplicates()
                for chat_id in usrids:
                    notification.send_message(chat_id, f"""<b>{machine}</b>
    {entity} -> {param} -> {status} ❌""")
            sent = status == 'Ошибка'
        except Exception as e:
            print(e)
            print('Mistake on cnc')