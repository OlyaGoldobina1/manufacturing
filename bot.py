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
import json

from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv('TOKEN')
bot = telebot.TeleBot(TOKEN)
login_str = os.getenv('db_login')
login = json.loads(os.getenv('db_login'))

def start_cnc_2():
   global thread_cnc_list
   global thread_cnc
   start_cnc(thread_cnc_list)

def start_mqtt_2():
    global thread_mqtt_list
    global thread_mqtt
    start_mqtt(thread_mqtt_list)

def start_opc_2():
   global thread_opc_list
   global thread_opc
   start_opc(thread_opc_list)


thread_cnc = 1
thread_mqtt = 1
thread_opc = 1

@bot.message_handler(commands=['reg'])
def sign_handler(message):
    global thread_cnc
    if message.text.replace('/reg ', '') ==  "123":
      users_data = {"chat_id": [message.chat.id], "user_id": [message.from_user.id], "username": [message.from_user.username], "user_first_name": [message.from_user.first_name], "user_last_name": [message.from_user.last_name]}
      df = pd.DataFrame.from_dict(users_data)
      pg.insert_table(df, 'users', login=login)
      bot.send_message(message.chat.id, 'Регистрация прошла успешно', parse_mode="Markdown")
    else:
      bot.send_message(message.chat.id, 'Апи не корректен', parse_mode="Markdown")

@bot.message_handler(commands=['cnc-start'])
def sign_handler1(message):
    global thread_cnc
    global thread_cnc_list
    usrids = pg.query_to_df('select chat_id from users', login=login)
    usrids.drop_duplicates()
    usrids = usrids[usrids.columns[0]].values.tolist()
    if message.chat.id in usrids:
      if(thread_cnc == 1):
          thread_cnc = Thread(target=start_cnc_2)
          thread_cnc_list=[]
          thread_cnc.start()
          bot.send_message(message.chat.id, 'Сборщик успешно запущен', parse_mode="Markdown")
      else:
          bot.send_message(message.chat.id, 'Сборщик уже был запущен', parse_mode="Markdown")

       

@bot.message_handler(commands=['cnc-stop'])
def sign_handler2(message):
    global thread_cnc
    global thread_cnc_list
    usrids = pg.query_to_df('select chat_id from users', login=login)
    usrids.drop_duplicates()
    usrids = usrids[usrids.columns[0]].values.tolist()
    if message.chat.id in usrids:
        try:
          if(thread_cnc != 1):
            l_thread = thread_cnc
            thread_cnc_list.append("Very special string")
            bot.send_message(message.chat.id, 'Сборщик успешно остановлен', parse_mode="Markdown")
            l_thread.join()

            thread_cnc = 1
          else:
            bot.send_message(message.chat.id, 'Нет запущенных сборщиков', parse_mode="Markdown") 
        except Exception as e:
          return bot.send_message(message.chat.id, 'Остановка не была произведена', parse_mode="Markdown")
              

@bot.message_handler(commands=['mqtt-start'])
def sign_handler3(message):
    global thread_mqtt_list
    global thread_mqtt
    usrids = pg.query_to_df('select chat_id from users', login=login)
    usrids.drop_duplicates()
    usrids = usrids[usrids.columns[0]].values.tolist()
    if message.chat.id in usrids:

      if(thread_mqtt == 1):
          thread_mqtt_list=[]
          thread_mqtt = Thread(target=start_mqtt_2)
          thread_mqtt.start()
          bot.send_message(message.chat.id, 'Сборщик успешно запущен', parse_mode="Markdown")
      else:
          bot.send_message(message.chat.id, 'Сборщик уже был запущен', parse_mode="Markdown")

@bot.message_handler(commands=['mqtt-stop'])
def sign_handler4(message):
    global thread_mqtt_list
    global thread_mqtt
    usrids = pg.query_to_df('select chat_id from users', login=login)
    usrids.drop_duplicates()
    usrids = usrids[usrids.columns[0]].values.tolist()
    if message.chat.id in usrids:
        try:
          if(thread_mqtt != 1):
            l_thread = thread_mqtt
            thread_mqtt_list.append("Very special string")
            bot.send_message(message.chat.id, 'Сборщик успешно остановлен', parse_mode="Markdown")
            l_thread.join()

            thread_mqtt = 1
          else:
            bot.send_message(message.chat.id, 'Нет запущенных сборщиков', parse_mode="Markdown")
        except Exception as e:
          return bot.send_message(message.chat.id, 'Остановка не была произведена', parse_mode="Markdown")
        
@bot.message_handler(commands=['opc-start'])
def sign_handler5(message):
    global thread_opc
    global thread_opc_list
    usrids = pg.query_to_df('select chat_id from users', login=login)
    usrids.drop_duplicates()
    usrids = usrids[usrids.columns[0]].values.tolist()
    if message.chat.id in usrids:
      global thread_opc
      if(thread_opc == 1):
          thread_opc_list = []
          thread_opc = Thread(target=start_opc_2)
          thread_opc.start()
          bot.send_message(message.chat.id, 'Сборщик успешно запущен', parse_mode="Markdown")
      else:
          bot.send_message(message.chat.id, 'Сборщик уже был запущен', parse_mode="Markdown")

@bot.message_handler(commands=['opc-stop'])
def sign_handler6(message):
    global thread_opc
    global thread_opc_list
    usrids = pg.query_to_df('select chat_id from users', login=login)
    usrids.drop_duplicates()
    usrids = usrids[usrids.columns[0]].values.tolist()
    if message.chat.id in usrids:
        try:
          if(thread_opc != 1):
            l_thread = thread_opc
            thread_opc_list.append("Very special string")
            bot.send_message(message.chat.id, 'Сборщик успешно остановлен', parse_mode="Markdown")
            l_thread.join()

            thread_opc = 1
          else:
            bot.send_message(message.chat.id, 'Нет запущенных сборщиков', parse_mode="Markdown")
        except Exception as e:
          return bot.send_message(message.chat.id, 'Остановка не была произведена', parse_mode="Markdown")

bot.infinity_polling()
