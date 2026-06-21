"""
Read-only feasibility check: can a Telegram BOT account (via @BotFather token)
read message history from the channels AI-SCRAPER scrapes?

This does NOT touch the live pipeline or the existing user-session auth in
main.py. It exists to answer one question before committing to a bot-token
migration: bots are not always allowed to read history in a channel they
haven't been added to, even if the channel is public. Run this once a
TELEGRAM_BOT_TOKEN exists (create one via @BotFather -> /newbot).

Usage:
    python check_telegram_bot_access.py
"""

import asyncio
import os

import dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import RPCError

dotenv.load_dotenv()

TELEGRAM_API_ID   = int(os.getenv("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

# Kept in sync with TELEGRAM_CHANNELS_SEED in main.py.
CHANNELS = [
    "jobnetworkng", "remotejobss", "ingressive4good",
    "jbtoday", "nigeriatechjobs", "lagostechjobs",
    "techJobsNG", "devjobsng", "africatechjobs", "remotejobsafrica",
]


async def main() -> None:
    if not (TELEGRAM_API_ID and TELEGRAM_API_HASH):
        print("✗ TELEGRAM_API_ID / TELEGRAM_API_HASH not set — aborting.")
        return
    if not TELEGRAM_BOT_TOKEN:
        print("✗ TELEGRAM_BOT_TOKEN not set — create one via @BotFather (/newbot) first.")
        return

    client = TelegramClient(StringSession(), TELEGRAM_API_ID, TELEGRAM_API_HASH)
    await client.start(bot_token=TELEGRAM_BOT_TOKEN)
    print("✓ Bot authenticated\n")

    passed = 0
    try:
        for channel in CHANNELS:
            try:
                entity   = await client.get_entity(channel)
                messages = await client.get_messages(entity, limit=5)
                print(f"✓ PASS  @{channel}  ({len(messages)} messages readable)")
                passed += 1
            except RPCError as e:
                print(f"✗ FAIL  @{channel}  {type(e).__name__}: {e}")
            except Exception as e:
                print(f"✗ FAIL  @{channel}  {type(e).__name__}: {e}")
    finally:
        await client.disconnect()

    print(f"\n{passed}/{len(CHANNELS)} channels readable as a bot.")
    if passed == len(CHANNELS):
        print("→ Safe to migrate fetch_telegram() fully to TELEGRAM_BOT_TOKEN.")
    elif passed == 0:
        print("→ Bot token cannot read these channels at all — stay on user-session auth.")
    else:
        print("→ Partial access — bot-token migration would need a per-channel fallback.")


if __name__ == "__main__":
    asyncio.run(main())
