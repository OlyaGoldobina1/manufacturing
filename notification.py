import telebot
from telebot import types
import requests
import os
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv('TOKEN')

def send_message(chat_id, text) -> None:
    method = 'SendMessage'
    url = f'https://api.telegram.org/bot{TOKEN}/{method}'
    data = {'chat_id': chat_id, 'text': text, 'parse_mode': 'Markdown'}
    requests.post(url, data=data)