# Telegram Media-to-Admin Relay Bot

Anyone sends the bot a photo or video, it checks whether it's a
duplicate, tells the sender the result, and — if it's new — relays it
to a single admin user, one item every 2 seconds. Plain text messages
get forwarded straight to the admin too.

## How it works

1. **`/start`** → the bot replies with a short welcome explaining that
   duplicates get skipped, and asks the sender to go ahead and send
   media.
2. Someone sends a photo or video to the bot.
   - The bot checks Telegram's `file_unique_id` for that item against
     everything it's relayed before.
   - It immediately replies to the sender either
     `⚠️ ... duplicate ... skipped` or `✅ ... sending to the admin now`,
     including a running total of how many unique items have been sent
     overall.
   - If it's new, the item is added to an internal relay queue.
3. A background worker drains that queue and sends items to `ADMIN_ID`
   one at a time, waiting `RELAY_DELAY_SECONDS` (default 2s) between
   each send — so a burst of incoming media doesn't hit Telegram's
   rate limits and arrives as a readable stream. Each item the admin
   receives is captioned with the running total too.
4. Someone sends a plain text message (anything that isn't a command
   like `/start`) → it's forwarded to the admin as-is. No duplicate
   check or count applies to text.

Anything else sent (documents, stickers, etc.) is ignored.

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env
# edit .env: set BOT_TOKEN (from @BotFather) and ADMIN_ID (your own
# Telegram user id — get it from a bot like @userinfobot)
python bot.py
```

**Important:** the admin must have started a DM with the bot at least
once (e.g. sent it `/start` or any message) before the bot can send
messages to them — this is a Telegram restriction, not a bot bug.

State (the list of already-seen media for duplicate detection, and the
running sent-count) persists to `data.json` next to the script, so
restarting the bot doesn't forget what it's already relayed or reset
the counter — **except on Railway and similar platforms, see below.**

## Deploying on Railway

1. Push this folder to a GitHub repo. **Do not commit `.env`** — it's
   in `.gitignore`. Set `BOT_TOKEN` and `ADMIN_ID` as environment
   variables in the Railway dashboard instead (Variables tab).
2. Railway will detect Python via `requirements.txt` and use the
   `Procfile` (`worker: python bot.py`) to start it as a background
   worker — it does **not** need a public port, since `run_polling()`
   isn't an HTTP server. If Railway shows a "no open port detected"
   warning, ignore it or set the service type to "Worker" explicitly
   in settings.
3. **Attach a Volume** if you want duplicate-detection memory to
   survive redeploys. Railway's container filesystem resets on every
   redeploy — without a volume, `data.json` disappears each time you
   push, and the bot will treat previously-relayed media as new again.
   In Railway: your service → Settings → Volumes → add one, mount it
   at e.g. `/data`, then set `DATA_FILE=/data/data.json` as an
   environment variable. If you don't care about that edge case (e.g.
   you rarely redeploy), you can skip this.
4. Deploy. Check the Railway logs for `🤖 Bot running...` — if the
   token or admin id is wrong you'll see the error there immediately.

## Files

- `config.py` — loads `.env` / environment variables
- `storage.py` — tiny JSON persistence of seen `file_unique_id`s and
  the running sent-count, for duplicate detection + counting
- `bot.py` — the entire bot: `/start`, media handler, text-forward
  handler, relay worker, and entrypoint

## Known limitations

- Duplicate detection is based on Telegram's `file_unique_id` — it
  won't catch a pixel-identical image re-uploaded as a genuinely new
  file (e.g. re-saved/re-compressed), since Telegram assigns that a
  different id.
- The "total sent" count increments the moment an item is confirmed
  new and queued, not the moment it actually reaches the admin a few
  seconds later — in the rare case the admin-side send fails, the
  count will be very slightly ahead of what the admin actually
  received (the failure is logged either way).
- `ADMIN_ID` is a single fixed user — there's no way to relay to
  multiple admins or a channel without code changes.
- The relay queue lives in memory only; if the bot restarts with
  unsent items still queued, those specific items are lost (though
  they were already recorded as "seen," so they won't be re-queued if
  re-sent — resending them if that happens is easiest).
- Text forwarding uses Telegram's native forward, so the admin will
  see it tagged "Forwarded from [sender]" rather than as an anonymous
  copy (unlike media, which is sent fresh with no sender attribution).
- Not live-tested against Telegram's servers. Please run it against
  your real bot token and watch the console/logs for errors.
