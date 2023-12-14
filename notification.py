import telebot
from telebot import types
import requests
import os

TOKEN = "6712983152:AAGLrXDOURlFTXHOs2j8TBMakXx64sfX7Ek"
bot = telebot.TeleBot(TOKEN)


def send_message(chat_id, text) -> None:
    method = 'SendMessage'
    url = f'https://api.telegram.org/bot{TOKEN}/{method}'
    data = {'chat_id': chat_id, 'text': text, 'parse_mode': 'Markdown'}
    requests.post(url, data=data)