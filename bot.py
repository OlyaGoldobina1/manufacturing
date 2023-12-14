import telebot
import pandas as pd
from telebot import types
import requests
import os
from threading import Thread
from cnc import start_cnc
from mqtt import start_mqtt
from opc import start_opc
import pg

TOKEN = os.environ['TOKEN']
bot = telebot.TeleBot(TOKEN)

@bot.message_handler(commands=['reg'])
def sign_handler(message):
    if message.text.replace('/reg ', '') == os.environ['tg_password']:
      users_data = {"chat_id": message.chat.id, "user_id": message.from_user.id, "username": message.from_user.username, "user_first_name": message.from_user.first_name, "user_last_name": message.from_user.last_name}
      df = pd.DataFrame(users_data)
      pg.insert_table(df, 'users', login=os.environ['db_login'])
      bot.send_message(message.chat.id, 'Регистрация прошла успешно', parse_mode="Markdown")
    else:
      bot.send_message(message.chat.id, 'Апи не корректен', parse_mode="Markdown")

@bot.message_handler(commands=['cnc-start'])
def sign_handler(message):
    usrids = pg.query_to_df('select chat_id from users', login=os.environ['db_login'])
    usrids.drop_duplicates()
    if message.chat.id in usrids:
       global thread_cnc
       thread_cnc = Thread(target=start_cnc)
       thread_cnc.start()

@bot.message_handler(commands=['cnc-stop'])
def sign_handler(message):
    usrids = pg.query_to_df('select chat_id from users', login=os.environ['db_login'])
    usrids.drop_duplicates()
    if message.chat.id in usrids:
        try:
          thread_cnc.stop()
        except Exception as e:
          return bot.send_message(message.chat.id, 'Остановка не была произведена', parse_mode="Markdown")
              

@bot.message_handler(commands=['mqtt-start'])
def sign_handler(message):
    usrids = pg.query_to_df('select chat_id from users', login=os.environ['db_login'])
    usrids.drop_duplicates()
    if message.chat.id in usrids:
       global thread_mqtt
       thread_mqtt = Thread(target=start_mqtt)
       thread_mqtt.start()

@bot.message_handler(commands=['mqtt-stop'])
def sign_handler(message):
    usrids = pg.query_to_df('select chat_id from users', login=os.environ['db_login'])
    usrids.drop_duplicates()
    if message.chat.id in usrids:
        try:
          thread_mqtt.stop()
        except Exception as e:
          return bot.send_message(message.chat.id, 'Остановка не была произведена', parse_mode="Markdown")
        
@bot.message_handler(commands=['opc-start'])
def sign_handler(message):
    usrids = pg.query_to_df('select chat_id from users', login=os.environ['db_login'])
    usrids.drop_duplicates()
    if message.chat.id in usrids:
       global thread_opc
       thread_opc = Thread(target=start_opc)
       thread_opc.start()

@bot.message_handler(commands=['opc-stop'])
def sign_handler(message):
    usrids = pg.query_to_df('select chat_id from users', login=os.environ['db_login'])
    usrids.drop_duplicates()
    if message.chat.id in usrids:
        try:
          thread_opc.stop()
        except Exception as e:
          return bot.send_message(message.chat.id, 'Остановка не была произведена', parse_mode="Markdown")

bot.infinity_polling()
