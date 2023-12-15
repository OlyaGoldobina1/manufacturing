import telebot
from telebot import types
import requests
import os
from dotenv import load_dotenv
import pg
import json

load_dotenv()
login = json.loads(os.getenv('db_login'))
TOKEN = os.getenv('TOKEN')

def send_message(text) -> None:
    method = 'SendMessage'
    url = f'https://api.telegram.org/bot{TOKEN}/{method}'
    usrids = pg.query_to_df('select chat_id from users', login=login)
    usrids.drop_duplicates()
    usrids = usrids[usrids.columns[0]].values.tolist()
    for chat_id in usrids:
        data = {'chat_id': chat_id, 'text': text, 'parse_mode': 'Markdown'}
        requests.post(url, data=data)