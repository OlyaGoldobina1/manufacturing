import telebot
from telebot import types
import requests
TOKEN = os.environ['TOKEN']
bot = telebot.TeleBot(TOKEN);
all_chat_ids = []

@bot.message_handler(commands=['reg'])
def sign_handler(message):
    if message.text.replace('/reg ', '') == os.environ['tg_password']:
      all_chat_ids.append(message.chat.id)
      print(message.chat.id)
      bot.send_message(message.chat.id, 'Регистрация прошла успешно', parse_mode="Markdown")
    else:
      bot.send_message(message.chat.id, 'Апи не корректен', parse_mode="Markdown")

bot.infinity_polling()
