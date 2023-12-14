import telebot
from telebot import types
import requests


TOKEN = os.environ['TOKEN']
bot = telebot.TeleBot(TOKEN);


def send_message(chat_id: int, text: str) -> None:
    method = 'SendMessage'
    url = f'https://api.telegram.org/bot{TOKEN}/{method}'
    data = {'chat_id': chat_id, 'text': text, 'parse_mode': 'Markdown'}
    requests.post(url, data=data)