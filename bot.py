"""
Media -> Admin relay bot.

Anyone sends a photo or video to the bot. The bot:
  1. Checks it against everything it's seen before (Telegram's
     file_unique_id).
  2. Replies to the sender right away saying whether it was a
     duplicate or is being sent, including a running total of how many
     unique items have been sent so far.
  3. If it's new, queues it for relay to the admin.

Anyone sends a plain text message (not a command) -> it's forwarded
to the admin as-is, no duplicate check or count involved.

A single background worker drains the media queue and forwards items
to ADMIN_ID one at a time, waiting RELAY_DELAY_SECONDS between each
send, so a burst of incoming media arrives as a steady, rate-limit-safe
stream rather than all at once.

No setup needed from the sender beyond /start -- just send media or
text and the bot handles the rest.
"""

import asyncio
import logging

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

import config
import storage

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# Queue of {"type": "photo"|"video", "file_id": str, "count": int}
# waiting to be relayed to the admin.
relay_queue: "asyncio.Queue[dict]" = asyncio.Queue()

WELCOME_MESSAGE = (
    "👋 Welcome! Send me any photo or video and I'll pass it along to "
    "the admin — duplicates are automatically detected and skipped.\n"
    "Go ahead and send your media whenever you're ready."
)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if message:
        await message.reply_text(WELCOME_MESSAGE)


async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if not message:
        return

    if message.photo:
        media_type = "photo"
        # photo is a list of sizes, largest last
        file_id = message.photo[-1].file_id
        unique_id = message.photo[-1].file_unique_id
    elif message.video:
        media_type = "video"
        file_id = message.video.file_id
        unique_id = message.video.file_unique_id
    else:
        return  # ignore documents/stickers/etc. (text is handled separately)

    is_new, total_count = await storage.record_media(unique_id)

    if not is_new:
        logger.info(f"Duplicate {media_type} ({unique_id})")
        await message.reply_text(
            f"⚠️ This {media_type} looks like a duplicate — already sent "
            f"before, so I skipped it. (Total sent so far: {total_count})"
        )
        return

    await relay_queue.put({"type": media_type, "file_id": file_id, "count": total_count})
    logger.info(f"Queued {media_type} for relay ({unique_id}), total={total_count}")
    await message.reply_text(
        f"✅ Got it — new {media_type}, sending to the admin now. "
        f"(Total sent so far: {total_count})"
    )


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if not message or not message.text:
        return

    try:
        await context.bot.forward_message(
            chat_id=config.ADMIN_ID,
            from_chat_id=message.chat_id,
            message_id=message.message_id,
        )
        logger.info(f"Forwarded text message from {message.chat_id} to admin")
    except Exception as e:
        logger.error(f"Failed to forward text message to admin: {e}")


async def relay_worker(app: Application):
    """Drains relay_queue forever, sending one item to the admin every
    RELAY_DELAY_SECONDS. Runs as a background task for the app's
    lifetime."""
    bot = app.bot
    while True:
        item = await relay_queue.get()
        caption = f"📥 New {item['type']} (Total sent: {item['count']})"
        try:
            if item["type"] == "photo":
                await bot.send_photo(config.ADMIN_ID, item["file_id"], caption=caption)
            else:
                await bot.send_video(config.ADMIN_ID, item["file_id"], caption=caption)
        except Exception as e:
            logger.error(f"Failed to relay {item['type']} to admin: {e}")
        finally:
            relay_queue.task_done()
        await asyncio.sleep(config.RELAY_DELAY_SECONDS)


async def post_init(app: Application):
    app.create_task(relay_worker(app))
    logger.info("🤖 Bot running...")


def main():
    app = Application.builder().token(config.BOT_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.PHOTO | filters.VIDEO, handle_media))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.run_polling()


if __name__ == "__main__":
    main()
