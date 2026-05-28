from telethon.sync import TelegramClient
from telethon.sessions import StringSession
import os
import dotenv

dotenv.load_dotenv()

TELEGRAM_API_ID   = int(os.getenv("TELEGRAM_API_ID",   "0"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH",     "")
TELEGRAM_PHONE    = os.getenv("TELEGRAM_PHONE",        "")
SESSION_STRING = os.getenv("TELEGRAM_SESSION_STRING", "")


with TelegramClient(StringSession(), TELEGRAM_API_ID, TELEGRAM_API_HASH) as client:
    client.start(phone=TELEGRAM_PHONE)
    print(client.session.save())  # copy this string