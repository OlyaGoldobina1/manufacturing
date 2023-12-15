import requests
import json
from datetime import datetime
import os
import re
from time import sleep
import pandas as pd
import pg
import notification
from dotenv import load_dotenv

load_dotenv()

cnc_data = [os.getenv('cnc_api_3'), os.getenv('cnc_api_5')]
login = json.loads(os.getenv('db_login'))
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
    while True:
        sleep(10)
        try:
            df = get_api_data('public.cnc')
 
            if(len(list) > 0):
                break
            if(df.size == 0):
                continue
            row = df[df['param_name'] == 'Статус канала']
            if row.param_val_str[row.param_val_str == 'Ошибка'].any(axis=None):
                row.apply(lambda x: notification.send_message(f"""{x.get('url')} {x.get('entity')} -> {x.get('param_name')} -> {x.get('param_val_str')} X"""), axis = 1)
        except Exception as e:
            print(e)
            print('Mistake on cnc')