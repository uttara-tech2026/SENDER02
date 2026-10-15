import os
from dotenv import load_dotenv

load_dotenv()

# =========================
# REQUIRED SETTINGS
# =========================

BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN is missing in .env file")

ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

if not ADMIN_ID:
    raise ValueError("❌ ADMIN_ID is missing in .env file")

# =========================
# RELAY
# =========================

# Seconds to wait between each item sent to the admin, so a burst of
# incoming media doesn't hit Telegram's rate limits and arrives as a
# readable stream rather than a flood.
RELAY_DELAY_SECONDS = float(os.getenv("RELAY_DELAY_SECONDS", "2"))

# =========================
# STORAGE
# =========================

# Where the duplicate-detection memory (seen file_unique_ids) is kept.
DATA_FILE = os.getenv("DATA_FILE", "data.json")
