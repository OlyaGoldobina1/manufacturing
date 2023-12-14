import requests
import json
from datetime import datetime
import os

cnc_data = [os.environ['cnc_api_3'], os.environ['cnc_api_5']]
api_data = os.environ['api_data']


def read_cnc_api_data(api_data: dict) -> pd.DataFrame:
    result = list()
    timestamp = datetime.now()
    for item in api_data.get('data', []):
        if len(item) > 1:
            entity = item[0]
            param_list = item[1]
            print(entity)
            for param in param_list:
                if len(param) > 1:
                    param_name = param[0]
                    param_val = param[1]
                    result.append({
                        'entity': entity,
                        'param_name': param_name,
                        'param_val_str': param_val,
                        'param_val_float': param_val_to_float(param_val),
                        'timestamp': timestamp
                    })
    return pd.DataFrame(result)


def get_api_data(cnc_data: List[str], table: str) -> pd.DataFrame:
    df = pd.DataFrame()
    for url in cnc_data:
        r = requests.get(url)
        json_data = json.loads(r.content)
        df_add = read_cnc_api_data(json_data)
        df_add.insert(0, "URL", url[-1])
        df = pd.concat([df,df_add ], ignore_index=True, sort=False).drop_duplicates()
        db.insert_table(df, table, login=os.environ['db_login'])
    return df