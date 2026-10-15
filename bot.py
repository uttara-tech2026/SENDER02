"""
Media relay bot with roles, routing and an admin panel.

Flow
  * Unknown user taps /start -> admin is notified with buttons to make
    them a SENDER or a RECEIVER (or reject). Multiple of each allowed.
  * Senders send photos/videos. Duplicates (file_unique_id) are skipped.
    New media is routed, per the admin's setting, to:
        receivers | destination (a channel/group/chat) | both
  * Every target (each receiver + the destination) has its OWN queue,
    worker and delay, so they are rate-limited independently.
  * When all queued work is finished, the admin gets a completion
    message with stats.
  * Admin panel (/admin): users & roles, destination & route, delays,
    caption on/off + text, stats, running processes.
  * Plain text from a sender is forwarded to the admin.
"""

import asyncio
import html
import logging
import time
from collections import Counter

from telegram import InlineKeyboardButton as Btn
from telegram import InlineKeyboardMarkup as Markup
from telegram import Update
from telegram.constants import ParseMode
from telegram.error import BadRequest, RetryAfter, TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
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

HTML = ParseMode.HTML
DEFAULT_CAPTION = "📥 New {type} (Total sent: {count})"
ROLE_EMOJI = {"sender": "📤", "receiver": "📥", "pending": "⏳", "rejected": "🚫"}

# ----------------------------------------------------------------- state
queues: dict[str, asyncio.Queue] = {}        # target key -> queue
workers: dict[str, asyncio.Task] = {}        # target key -> worker task
worker_state: dict[str, dict] = {}           # target key -> {"status", "last"}
pending_jobs = 0                             # jobs queued or in flight


def new_batch() -> dict:
    return {"started": None, "unique": 0, "dups": 0, "sent": {}, "failed": {}}


batch = new_batch()

# Target keys: "dest" for the destination chat, "r:<user_id>" for a receiver.


# --------------------------------------------------------------- helpers
def esc(s) -> str:
    return html.escape(str(s))


def is_admin_id(uid) -> bool:
    return uid == config.ADMIN_ID


def user_label(u: dict) -> str:
    name = u.get("name") or "User"
    handle = f" @{u['username']}" if u.get("username") else ""
    return f"{name}{handle} ({u['user_id']})"


async def target_label(key: str) -> str:
    """Plain-text label (escape before putting into HTML)."""
    if key == "dest":
        title = await storage.get_setting("dest_title")
        chat = await storage.get_setting("dest_chat", "?")
        return f"📢 Destination ({title or chat})"
    u = await storage.get_user(int(key[2:]))
    return "👤 " + (user_label(u) if u else key)


async def configured_targets() -> list[str]:
    """Every target that currently exists, regardless of route mode."""
    keys = [f"r:{u['user_id']}" for u in await storage.list_users("receiver")]
    if await storage.get_setting("dest_chat"):
        keys.append("dest")
    return keys


async def active_targets() -> list[str]:
    """Targets that should receive new media, per the admin's route mode."""
    route = await storage.get_setting("route", "receivers")
    keys = []
    if route in ("receivers", "both"):
        keys += [f"r:{u['user_id']}" for u in await storage.list_users("receiver")]
    if route in ("destination", "both") and await storage.get_setting("dest_chat"):
        keys.append("dest")
    return keys


async def default_delay() -> float:
    v = await storage.get_setting("delay:default")
    return float(v) if v is not None else float(config.RELAY_DELAY_SECONDS)


async def get_delay(key: str) -> float:
    v = await storage.get_setting(f"delay:{key}")
    return float(v) if v is not None else await default_delay()


async def resolve_chat(key: str):
    if key == "dest":
        v = await storage.get_setting("dest_chat")
        if v is None:
            return None
        return int(v) if v.lstrip("-").isdigit() else v
    return int(key[2:])


def ago(ts) -> str:
    if not ts:
        return "never"
    s = int(time.time() - ts)
    if s < 60:
        return f"{s}s ago"
    if s < 3600:
        return f"{s // 60}m ago"
    return f"{s // 3600}h {(s % 3600) // 60}m ago"


def duration(seconds: float) -> str:
    seconds = int(seconds)
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


async def build_caption(job: dict):
    if await storage.get_setting("caption_on", "0") != "1":
        return None
    template = await storage.get_setting("caption_text") or DEFAULT_CAPTION
    text = (
        template.replace("{type}", job["type"])
        .replace("{count}", str(job["count"]))
        .replace("{sender}", job.get("sender", ""))
    )
    return text[:1024]


# --------------------------------------------------------------- workers
def ensure_worker(app: Application, key: str):
    if key not in queues:
        queues[key] = asyncio.Queue()
    task = workers.get(key)
    if task is None or task.done():
        worker_state[key] = {"status": "idle", "last": None}
        workers[key] = asyncio.create_task(target_worker(app, key))


async def send_job(bot, key: str, job: dict) -> bool:
    chat_id = await resolve_chat(key)
    if chat_id is None:
        return False
    caption = await build_caption(job)
    for _ in range(3):
        try:
            if job["type"] == "photo":
                await bot.send_photo(chat_id, job["file_id"], caption=caption)
            else:
                await bot.send_video(chat_id, job["file_id"], caption=caption)
            return True
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
        except TelegramError as e:
            logger.error(f"Relay to {key} failed: {e}")
            return False
    return False


async def notify_complete(bot):
    global batch
    done, batch = batch, new_batch()  # swap first so new work starts a fresh batch
    total_sent = sum(done["sent"].values())
    total_failed = sum(done["failed"].values())
    if not (total_sent or total_failed):
        return
    elapsed = time.time() - (done["started"] or time.time())
    lines = [
        "✅ <b>Relay task complete</b>",
        "",
        f"New unique media: <b>{done['unique']}</b>",
        f"Duplicates skipped: <b>{done['dups']}</b>",
        f"Deliveries: <b>{total_sent}</b> sent, <b>{total_failed}</b> failed",
        f"Time taken: {duration(elapsed)}",
        "",
        "<b>Per target</b>",
    ]
    for key in sorted(set(done["sent"]) | set(done["failed"])):
        lines.append(
            f"• {esc(await target_label(key))} — "
            f"{done['sent'].get(key, 0)} sent, {done['failed'].get(key, 0)} failed"
        )
    try:
        await bot.send_message(config.ADMIN_ID, "\n".join(lines), parse_mode=HTML)
    except TelegramError as e:
        logger.error(f"Could not send completion message: {e}")


async def target_worker(app: Application, key: str):
    """One worker per target; drains that target's queue with its own delay."""
    global pending_jobs
    q = queues[key]
    state = worker_state[key]
    while True:
        state["status"] = "idle"
        job = await q.get()
        state["status"] = "sending"
        try:
            ok = await send_job(app.bot, key, job)
            await storage.incr(("sent:" if ok else "failed:") + key)
            bucket = batch["sent" if ok else "failed"]
            bucket[key] = bucket.get(key, 0) + 1
        except Exception:
            logger.exception(f"Worker {key} crashed on a job")
        finally:
            q.task_done()
            state["last"] = time.time()
            pending_jobs = max(0, pending_jobs - 1)
        if pending_jobs == 0:
            await notify_complete(app.bot)
        delay = await get_delay(key)
        if delay > 0:
            state["status"] = f"cooldown ({delay:g}s)"
            await asyncio.sleep(delay)


# ------------------------------------------------------- user-facing side
WELCOME_SENDER = (
    "👋 Welcome! Send me any photo or video and I'll pass it along — "
    "duplicates are automatically detected and skipped."
)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg, user = update.effective_message, update.effective_user
    if not msg or not user:
        return

    if is_admin_id(user.id):
        await msg.reply_text("👑 Hi admin! Open the control panel with /admin")
        return

    created = await storage.upsert_user(user.id, user.full_name, user.username)
    u = await storage.get_user(user.id)
    role = u["role"]

    if role == "sender":
        await msg.reply_text(WELCOME_SENDER)
    elif role == "receiver":
        await msg.reply_text("📥 You're set up as a receiver. Media will arrive here automatically.")
    elif role == "rejected":
        await msg.reply_text("🚫 Your access request was not approved.")
    else:
        await msg.reply_text("⏳ Your request has been sent to the admin. You'll be notified once approved.")
        if created:
            kb = Markup(
                [
                    [
                        Btn("📤 Sender", callback_data=f"role:{user.id}:sender"),
                        Btn("📥 Receiver", callback_data=f"role:{user.id}:receiver"),
                    ],
                    [Btn("🚫 Reject", callback_data=f"role:{user.id}:rejected")],
                ]
            )
            try:
                await context.bot.send_message(
                    config.ADMIN_ID,
                    f"🔔 <b>New user pressed /start</b>\n\n{esc(user_label(u))}\n\nAssign a role:",
                    parse_mode=HTML,
                    reply_markup=kb,
                )
            except TelegramError as e:
                logger.error(f"Could not notify admin about new user: {e}")


async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message, user = update.effective_message, update.effective_user
    if not message or not user:
        return
    if is_admin_id(user.id):
        await message.reply_text("ℹ️ Admin media isn't relayed. Use /admin for the control panel.")
        return

    u = await storage.get_user(user.id)
    if not u:
        await message.reply_text("Please press /start first.")
        return
    if u["role"] == "pending":
        await message.reply_text("⏳ Still waiting for admin approval.")
        return
    if u["role"] != "sender":
        await message.reply_text("⛔ You're not allowed to send media.")
        return

    if message.photo:
        media_type = "photo"
        file_id = message.photo[-1].file_id
        unique_id = message.photo[-1].file_unique_id
    elif message.video:
        media_type = "video"
        file_id = message.video.file_id
        unique_id = message.video.file_unique_id
    else:
        return

    targets = await active_targets()
    if not targets:
        await message.reply_text("⚠️ No destination or receiver is configured right now. Please try again later.")
        return

    is_new, total_count = await storage.record_media(unique_id, user.id)
    if not is_new:
        await storage.incr("dup")
        batch["dups"] += 1
        logger.info(f"Duplicate {media_type} ({unique_id})")
        await message.reply_text(
            f"⚠️ This {media_type} looks like a duplicate — already sent "
            f"before, so I skipped it. (Total sent so far: {total_count})"
        )
        return

    global pending_jobs
    if batch["started"] is None:
        batch["started"] = time.time()
    batch["unique"] += 1
    await storage.incr(f"sender:{user.id}")

    job = {"type": media_type, "file_id": file_id, "count": total_count, "sender": user.full_name}
    for key in targets:
        ensure_worker(context.application, key)
        queues[key].put_nowait(dict(job))
        pending_jobs += 1

    logger.info(f"Queued {media_type} ({unique_id}) for {len(targets)} target(s), total={total_count}")
    await message.reply_text(
        f"✅ Got it — new {media_type}, sending now. (Total sent so far: {total_count})"
    )


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message, user = update.effective_message, update.effective_user
    if not message or not message.text or not user:
        return

    if is_admin_id(user.id):
        awaiting = context.user_data.pop("awaiting", None)
        if awaiting:
            await handle_admin_input(update, context, awaiting)
        else:
            await message.reply_text("Use /admin to open the control panel.")
        return

    u = await storage.get_user(user.id)
    if not u:
        await message.reply_text("Please press /start first.")
        return
    if u["role"] == "pending":
        await message.reply_text("⏳ Still waiting for admin approval.")
        return
    if u["role"] != "sender":
        return

    try:
        await context.bot.forward_message(
            chat_id=config.ADMIN_ID,
            from_chat_id=message.chat_id,
            message_id=message.message_id,
        )
        await storage.incr("text")
    except TelegramError as e:
        logger.error(f"Failed to forward text message to admin: {e}")


# ------------------------------------------------------------ admin views
def back_row(target="menu:main"):
    return [Btn("⬅️ Back", callback_data=target)]


def main_menu():
    text = "🛠 <b>Admin panel</b>\nChoose a section:"
    kb = Markup(
        [
            [Btn("👥 Users", callback_data="menu:users"), Btn("🎯 Destination", callback_data="menu:dest")],
            [Btn("⏱ Delays", callback_data="menu:delay"), Btn("📝 Caption", callback_data="menu:caption")],
            [Btn("📊 Stats", callback_data="menu:stats"), Btn("⚙️ Processes", callback_data="menu:proc")],
        ]
    )
    return text, kb


async def view_users():
    users = await storage.list_users()
    rows = [
        [Btn(f"{ROLE_EMOJI.get(u['role'], '')} {user_label(u)}"[:60], callback_data=f"user:{u['user_id']}")]
        for u in users[:40]
    ]
    rows.append(back_row())
    text = "👥 <b>Users</b>\nTap a user to change their role."
    if not users:
        text += "\n\nNo users yet."
    return text, Markup(rows)


async def view_user(uid: int):
    u = await storage.get_user(uid)
    if not u:
        return "User not found.", Markup([back_row("menu:users")])
    text = f"{ROLE_EMOJI.get(u['role'], '')} <b>{esc(user_label(u))}</b>\nCurrent role: <b>{esc(u['role'])}</b>"
    kb = Markup(
        [
            [
                Btn("📤 Sender", callback_data=f"role:{uid}:sender"),
                Btn("📥 Receiver", callback_data=f"role:{uid}:receiver"),
            ],
            [Btn("🚫 Reject / Remove", callback_data=f"role:{uid}:rejected")],
            back_row("menu:users"),
        ]
    )
    return text, kb


async def view_dest():
    route = await storage.get_setting("route", "receivers")
    dest = await storage.get_setting("dest_chat")
    title = await storage.get_setting("dest_title")
    receivers = await storage.list_users("receiver")
    mark = lambda m: "✅ " if route == m else ""
    text = (
        "🎯 <b>Destination & routing</b>\n\n"
        f"Destination: <b>{esc(title or dest) if dest else 'not set'}</b>\n"
        f"Receivers: <b>{len(receivers)}</b>\n"
        f"Route: <b>{esc(route)}</b>\n\n"
        "Media from senders goes to the selected target(s)."
    )
    kb = Markup(
        [
            [Btn(f"{mark('receivers')}Receivers", callback_data="route:receivers")],
            [Btn(f"{mark('destination')}Destination", callback_data="route:destination")],
            [Btn(f"{mark('both')}Both", callback_data="route:both")],
            [Btn("✏️ Set destination", callback_data="dest:set"), Btn("🗑 Clear", callback_data="dest:clear")],
            back_row(),
        ]
    )
    return text, kb


async def view_delay():
    default = await default_delay()
    rows = [[Btn(f"🌐 Default: {default:g}s", callback_data="delay:default")]]
    for key in await configured_targets():
        v = await storage.get_setting(f"delay:{key}")
        label = await target_label(key)
        rows.append(
            [Btn(f"{label} — {float(v):g}s"[:60] if v is not None else f"{label} — default"[:60],
                 callback_data=f"delay:{key}")]
        )
    rows.append(back_row())
    text = (
        "⏱ <b>Delays</b>\nDelay between sends, set per receiver / destination. "
        "Targets without their own value use the default."
    )
    return text, Markup(rows)


async def view_caption():
    on = await storage.get_setting("caption_on", "0") == "1"
    custom = await storage.get_setting("caption_text")
    text = (
        "📝 <b>Caption</b>\n\n"
        f"Status: <b>{'enabled ✅' if on else 'disabled ❌'}</b>\n"
        f"Text: <code>{esc(custom or DEFAULT_CAPTION)}</code>"
        f"{'' if custom else ' (default)'}\n\n"
        "Placeholders: <code>{type}</code> <code>{count}</code> <code>{sender}</code>"
    )
    kb = Markup(
        [
            [Btn("🔴 Disable" if on else "🟢 Enable", callback_data="cap:toggle")],
            [Btn("✏️ Set caption text", callback_data="cap:set"), Btn("♻️ Reset text", callback_data="cap:clear")],
            back_row(),
        ]
    )
    return text, kb


async def build_stats() -> str:
    stats = await storage.all_stats()
    users = await storage.list_users()
    roles = Counter(u["role"] for u in users)
    route = await storage.get_setting("route", "receivers")
    cap_on = await storage.get_setting("caption_on", "0") == "1"
    lines = [
        "📊 <b>Stats</b>",
        "",
        f"Unique media: <b>{await storage.count_media()}</b>",
        f"Duplicates blocked: <b>{stats.get('dup', 0)}</b>",
        f"Texts forwarded to admin: <b>{stats.get('text', 0)}</b>",
        f"Queued right now: <b>{pending_jobs}</b>",
        "",
        f"Route: <b>{esc(route)}</b> · Caption: <b>{'on' if cap_on else 'off'}</b>",
        f"Users — senders: {roles['sender']}, receivers: {roles['receiver']}, "
        f"pending: {roles['pending']}, rejected: {roles['rejected']}",
    ]
    senders = [u for u in users if u["role"] == "sender"]
    if senders:
        lines += ["", "<b>Per sender (unique media)</b>"]
        for u in senders:
            n = stats.get("sender:" + str(u["user_id"]), 0)
            lines.append(f"• {esc(user_label(u))} — {n}")

    keys = set(await configured_targets()) | set(queues)
    keys |= {k.split(":", 1)[1] for k in stats if k.startswith(("sent:", "failed:"))}
    if keys:
        lines += ["", "<b>Per target</b>"]
        for key in sorted(keys):
            queued = queues[key].qsize() if key in queues else 0
            lines.append(
                f"• {esc(await target_label(key))} — "
                f"{stats.get('sent:' + key, 0)} sent, {stats.get('failed:' + key, 0)} failed, {queued} queued"
            )
    return "\n".join(lines)


async def build_processes() -> str:
    lines = [
        "⚙️ <b>Running processes</b>",
        "",
        "🟢 Bot polling: running",
        f"🧾 Current task: {'running since ' + ago(batch['started']) if batch['started'] else 'idle'}"
        f" · jobs left: <b>{pending_jobs}</b>",
        "",
    ]
    if not workers:
        lines.append("No relay workers yet (they start when media is queued).")
    for key, task in workers.items():
        alive = not task.done()
        st = worker_state.get(key, {})
        q = queues.get(key)
        lines.append(
            f"{'🟢' if alive else '🔴'} {esc(await target_label(key))}\n"
            f"    status: {esc(st.get('status', '?'))} · queue: {q.qsize() if q else 0} · "
            f"delay: {await get_delay(key):g}s · last send: {ago(st.get('last'))}"
        )
    return "\n".join(lines)


async def view_stats():
    return await build_stats(), Markup([[Btn("🔄 Refresh", callback_data="menu:stats")], back_row()])


async def view_proc():
    return await build_processes(), Markup([[Btn("🔄 Refresh", callback_data="menu:proc")], back_row()])


# ---------------------------------------------------------- admin handlers
async def show(query, text, kb=None):
    try:
        await query.edit_message_text(text, reply_markup=kb, parse_mode=HTML)
    except BadRequest as e:
        if "not modified" not in str(e).lower():
            raise


async def admin_only(update: Update) -> bool:
    if is_admin_id(update.effective_user.id if update.effective_user else None):
        return True
    return False


async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await admin_only(update):
        return
    text, kb = main_menu()
    await update.effective_message.reply_text(text, reply_markup=kb, parse_mode=HTML)


async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await admin_only(update):
        return
    text, kb = await view_stats()
    await update.effective_message.reply_text(text, reply_markup=kb, parse_mode=HTML)


async def cmd_process(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await admin_only(update):
        return
    text, kb = await view_proc()
    await update.effective_message.reply_text(text, reply_markup=kb, parse_mode=HTML)


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await admin_only(update):
        return
    context.user_data.pop("awaiting", None)
    await update.effective_message.reply_text("Cancelled. /admin to reopen the panel.")


async def prompt(query, context, awaiting: str, text: str):
    context.user_data["awaiting"] = awaiting
    await show(query, text + "\n\nSend /cancel to abort.", Markup([back_row()]))


async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not is_admin_id(q.from_user.id):
        await q.answer("Admins only.", show_alert=True)
        return
    await q.answer()
    data = q.data
    context.user_data.pop("awaiting", None)  # leaving any pending prompt

    if data == "menu:main":
        await show(q, *main_menu())
    elif data == "menu:users":
        await show(q, *await view_users())
    elif data == "menu:dest":
        await show(q, *await view_dest())
    elif data == "menu:delay":
        await show(q, *await view_delay())
    elif data == "menu:caption":
        await show(q, *await view_caption())
    elif data == "menu:stats":
        await show(q, *await view_stats())
    elif data == "menu:proc":
        await show(q, *await view_proc())

    elif data.startswith("user:"):
        await show(q, *await view_user(int(data.split(":")[1])))

    elif data.startswith("role:"):
        _, uid, role = data.split(":")
        uid = int(uid)
        if role not in ("sender", "receiver", "rejected"):
            return
        await storage.set_role(uid, role)
        notes = {
            "sender": "✅ You're approved as a sender. Send me photos or videos any time.",
            "receiver": "✅ You're set up as a receiver. Media will arrive here automatically.",
            "rejected": "🚫 Your access has been removed by the admin.",
        }
        try:
            await context.bot.send_message(uid, notes[role])
        except TelegramError as e:
            logger.warning(f"Could not notify user {uid} of new role: {e}")
        await show(q, *await view_user(uid))

    elif data.startswith("route:"):
        await storage.set_setting("route", data.split(":")[1])
        await show(q, *await view_dest())

    elif data == "dest:set":
        await prompt(
            q, context, "dest",
            "🎯 Send the destination chat ID (e.g. <code>-1001234567890</code>) or "
            "<code>@channelusername</code>.\nThe bot must be an admin of that channel/group.",
        )
    elif data == "dest:clear":
        await storage.del_setting("dest_chat")
        await storage.del_setting("dest_title")
        await show(q, *await view_dest())

    elif data.startswith("delay:"):
        key = data[len("delay:"):]
        label = "the default delay" if key == "default" else await target_label(key)
        current = await default_delay() if key == "default" else await get_delay(key)
        extra = "" if key == "default" else "\nSend <code>reset</code> to use the default again."
        await prompt(
            q, context, f"delay:{key}",
            f"⏱ Send the delay in seconds for <b>{esc(label)}</b> (now {current:g}s). "
            f"Example: <code>2.5</code>{extra}",
        )

    elif data == "cap:toggle":
        on = await storage.get_setting("caption_on", "0") == "1"
        await storage.set_setting("caption_on", "0" if on else "1")
        await show(q, *await view_caption())
    elif data == "cap:set":
        await prompt(
            q, context, "caption",
            "📝 Send the caption text. Placeholders: <code>{type}</code> <code>{count}</code> <code>{sender}</code>",
        )
    elif data == "cap:clear":
        await storage.del_setting("caption_text")
        await show(q, *await view_caption())


async def handle_admin_input(update: Update, context: ContextTypes.DEFAULT_TYPE, awaiting: str):
    msg = update.effective_message
    text = msg.text.strip()
    panel = Markup([[Btn("🛠 Open panel", callback_data="menu:main")]])

    if awaiting == "dest":
        target = int(text) if text.lstrip("-").isdigit() else (text if text.startswith("@") else "@" + text)
        try:
            chat = await context.bot.get_chat(target)
            if chat.type == "channel":
                member = await context.bot.get_chat_member(chat.id, context.bot.id)
                if member.status not in ("administrator", "creator"):
                    await msg.reply_text("⚠️ I'm not an admin in that channel. Add me as admin and try again.")
                    return
        except TelegramError as e:
            await msg.reply_text(f"❌ Couldn't access that chat: {e}\nCheck the ID and that the bot is added.")
            return
        await storage.set_setting("dest_chat", chat.id)
        await storage.set_setting("dest_title", chat.title or chat.username or chat.id)
        await msg.reply_text(f"✅ Destination set to {chat.title or chat.id}.", reply_markup=panel)

    elif awaiting.startswith("delay:"):
        key = awaiting[len("delay:"):]
        setting_key = f"delay:{key}"
        if text.lower() == "reset" and key != "default":
            await storage.del_setting(setting_key)
            await msg.reply_text("✅ Delay reset to default.", reply_markup=panel)
            return
        try:
            value = float(text)
            if not 0 <= value <= 3600:
                raise ValueError
        except ValueError:
            context.user_data["awaiting"] = awaiting  # keep waiting
            await msg.reply_text("❌ Send a number of seconds between 0 and 3600 (or /cancel).")
            return
        await storage.set_setting(setting_key, value)
        await msg.reply_text(f"✅ Delay set to {value:g}s.", reply_markup=panel)

    elif awaiting == "caption":
        await storage.set_setting("caption_text", text[:1024])
        await storage.set_setting("caption_on", "1")
        await msg.reply_text("✅ Caption saved and enabled.", reply_markup=panel)


# ------------------------------------------------------------------ main
async def post_init(app: Application):
    logger.info("🤖 Bot running...")


async def post_shutdown(app: Application):
    for task in workers.values():
        task.cancel()


def main():
    app = (
        Application.builder()
        .token(config.BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    private = filters.ChatType.PRIVATE
    app.add_handler(CommandHandler("start", start, filters=private))
    app.add_handler(CommandHandler(["admin", "panel"], cmd_admin, filters=private))
    app.add_handler(CommandHandler("stats", cmd_stats, filters=private))
    app.add_handler(CommandHandler("process", cmd_process, filters=private))
    app.add_handler(CommandHandler("cancel", cmd_cancel, filters=private))
    app.add_handler(CallbackQueryHandler(on_button))
    app.add_handler(MessageHandler(private & (filters.PHOTO | filters.VIDEO), handle_media))
    app.add_handler(MessageHandler(private & filters.TEXT & ~filters.COMMAND, handle_text))
    app.run_polling()


if __name__ == "__main__":
    main()
