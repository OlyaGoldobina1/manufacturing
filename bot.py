import telebot
import pandas as pd
from telebot import types
import requests
import os

TOKEN = os.environ['TOKEN']
bot = telebot.TeleBot(TOKEN)

@bot.message_handler(commands=['reg'])
def sign_handler(message):
    if message.text.replace('/reg ', '') == os.environ['tg_password']:
      users_data = {"chat_id": message.chat.id, "user_id": message.from_user.id, "username": message.from_user.username, "user_first_name": message.from_user.first_name, "user_last_name": message.from_user.last_name}
      df = pd.DataFrame(users_data)
      db.insert_table(df, 'users', login=os.environ['db_login'])
      bot.send_message(message.chat.id, 'Регистрация прошла успешно', parse_mode="Markdown")
    else:
      bot.send_message(message.chat.id, 'Апи не корректен', parse_mode="Markdown")

bot.infinity_polling()
