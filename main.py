import os
import re
import time
import random
import logging
import unicodedata
from datetime import datetime, timedelta, timezone

import asyncpg
from dotenv import load_dotenv
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ChatPermissions,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ChatMemberHandler,
    ContextTypes,
    filters,
)

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("promo-guard-bot")

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
GROUP_ID = int(os.getenv("GROUP_ID", "0"))
ADMIN_IDS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()}
REWARD_IMAGE_URL = os.getenv("REWARD_IMAGE_URL", "https://picsum.photos/1200/800")
REWARD_REQUIRED_JOINS = int(os.getenv("REWARD_REQUIRED_JOINS", "6"))

URL_RE = re.compile(r"(https?://|www\.|t\.me/|telegram\.me/|bit\.ly/|gofile\.io/|discord\.gg/)", re.I)
AT_RE = re.compile(r"(^|\s)@[a-zA-Z0-9_]{3,32}\b")

DB: asyncpg.Pool | None = None
USER_STATE: dict[int, str] = {}


def norm(text: str) -> str:
    text = unicodedata.normalize("NFD", text or "")
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    return text.lower()


def admin_panel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🟢 ON", callback_data="toggle_on"), InlineKeyboardButton("🔴 OFF", callback_data="toggle_off")],
        [InlineKeyboardButton("➕ Ajouter mot", callback_data="word_add"), InlineKeyboardButton("🗑 Supprimer mot", callback_data="word_del")],
        [InlineKeyboardButton("📋 Liste mots", callback_data="word_list")],
        [InlineKeyboardButton("📢 Broadcast", callback_data="broadcast")],
        [InlineKeyboardButton("🎁 Modifier lien Gofile", callback_data="set_gofile"), InlineKeyboardButton("🚀 Publish", callback_data="publish")],
    ])


async def init_db():
    global DB
    DB = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with DB.acquire() as con:
        await con.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            chat_id BIGINT PRIMARY KEY,
            messages_open BOOLEAN DEFAULT TRUE,
            gofile_link TEXT DEFAULT '',
            reward_image_url TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS forbidden_words (
            chat_id BIGINT NOT NULL,
            word TEXT NOT NULL,
            normalized TEXT NOT NULL,
            PRIMARY KEY(chat_id, normalized)
        );
        CREATE TABLE IF NOT EXISTS joins (
            chat_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            joined_at BIGINT NOT NULL,
            PRIMARY KEY(chat_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS violations (
            chat_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            count INT DEFAULT 0,
            PRIMARY KEY(chat_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS reward_links (
            chat_id BIGINT NOT NULL,
            owner_id BIGINT NOT NULL,
            invite_link TEXT UNIQUE NOT NULL,
            joins_count INT DEFAULT 0,
            delivered BOOLEAN DEFAULT FALSE,
            PRIMARY KEY(chat_id, owner_id)
        );
        CREATE TABLE IF NOT EXISTS reward_joined (
            chat_id BIGINT NOT NULL,
            joined_user_id BIGINT NOT NULL,
            owner_id BIGINT NOT NULL,
            PRIMARY KEY(chat_id, joined_user_id)
        );
        """)
        await con.execute("""
            INSERT INTO settings(chat_id, reward_image_url)
            VALUES($1, $2)
            ON CONFLICT(chat_id) DO NOTHING
        """, GROUP_ID, REWARD_IMAGE_URL)


async def is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int | None = None, chat_id: int | None = None) -> bool:
    user_id = user_id or (update.effective_user.id if update.effective_user else 0)
    chat_id = chat_id or GROUP_ID
    if user_id in ADMIN_IDS:
        return True
    try:
        m = await context.bot.get_chat_member(chat_id, user_id)
        return m.status in ("administrator", "creator")
    except Exception:
        return False


async def ensure_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not await is_admin(update, context):
        if update.callback_query:
            await update.callback_query.answer("Admin uniquement", show_alert=True)
        elif update.effective_message:
            await update.effective_message.reply_text("Admin uniquement.")
        return False
    return True


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Bot actif. Les admins peuvent utiliser /admin.")


async def admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_admin(update, context):
        return
    row = await DB.fetchrow("SELECT messages_open FROM settings WHERE chat_id=$1", GROUP_ID)
    status = "ON 🟢" if row and row["messages_open"] else "OFF 🔴"
    await update.message.reply_text(f"Panel admin\nMessages utilisateurs : {status}", reply_markup=admin_panel())


async def delete_safely(context, chat_id, message_id):
    try:
        await context.bot.delete_message(chat_id, message_id)
    except Exception as e:
        log.info("delete failed: %s", e)


async def mute_user(context, chat_id: int, user_id: int, days: int):
    until = datetime.now(timezone.utc) + timedelta(days=days)
    perms = ChatPermissions(can_send_messages=False)
    await context.bot.restrict_chat_member(chat_id, user_id, permissions=perms, until_date=until)


async def punish(context, chat_id: int, user_id: int) -> int:
    count = await DB.fetchval("""
        INSERT INTO violations(chat_id,user_id,count) VALUES($1,$2,1)
        ON CONFLICT(chat_id,user_id) DO UPDATE SET count=violations.count+1
        RETURNING count
    """, chat_id, user_id)
    days = 7 if count and count >= 2 else 1
    await mute_user(context, chat_id, user_id, days)
    return days


def has_media(msg) -> bool:
    return any([msg.photo, msg.video, msg.animation, msg.document, msg.sticker, msg.audio, msg.voice, msg.video_note, msg.contact, msg.location, msg.venue, msg.poll])


async def check_forbidden(chat_id: int, text: str) -> str | None:
    rows = await DB.fetch("SELECT normalized FROM forbidden_words WHERE chat_id=$1", chat_id)
    ntext = norm(text)
    for r in rows:
        if r["normalized"] in ntext:
            return r["normalized"]
    return None


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    user = update.effective_user
    if not msg or not user or msg.chat_id != GROUP_ID:
        return

    if msg.new_chat_members or msg.left_chat_member:
        for u in msg.new_chat_members or []:
            await DB.execute("""
                INSERT INTO joins(chat_id,user_id,joined_at) VALUES($1,$2,$3)
                ON CONFLICT(chat_id,user_id) DO UPDATE SET joined_at=$3
            """, msg.chat_id, u.id, int(time.time()))
        await delete_safely(context, msg.chat_id, msg.message_id)
        return

    if await is_admin(update, context, user.id, msg.chat_id):
        return

    row = await DB.fetchrow("SELECT messages_open FROM settings WHERE chat_id=$1", msg.chat_id)
    messages_open = bool(row["messages_open"]) if row else True

    if not messages_open:
        await delete_safely(context, msg.chat_id, msg.message_id)
        return

    if has_media(msg):
        await delete_safely(context, msg.chat_id, msg.message_id)
        joined_at = await DB.fetchval("SELECT joined_at FROM joins WHERE chat_id=$1 AND user_id=$2", msg.chat_id, user.id)
        if joined_at and int(time.time()) - int(joined_at) <= 120:
            await mute_user(context, msg.chat_id, user.id, 1)
        return

    text = msg.text or msg.caption or ""
    if not text:
        await delete_safely(context, msg.chat_id, msg.message_id)
        return

    bad_word = await check_forbidden(msg.chat_id, text)
    if bad_word or URL_RE.search(text) or AT_RE.search(text):
        await delete_safely(context, msg.chat_id, msg.message_id)
        days = await punish(context, msg.chat_id, user.id)
        warn = await context.bot.send_message(msg.chat_id, f"Utilisateur muté {days} jour(s) pour contenu interdit.")
        context.job_queue.run_once(delete_later, 20, data={"chat_id": msg.chat_id, "message_id": warn.message_id})


async def on_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cmu = update.chat_member
    if not cmu or cmu.chat.id != GROUP_ID:
        return
    old, new = cmu.old_chat_member.status, cmu.new_chat_member.status
    if old in ("left", "kicked") and new in ("member", "restricted"):
        user_id = cmu.new_chat_member.user.id
        await DB.execute("""
            INSERT INTO joins(chat_id,user_id,joined_at) VALUES($1,$2,$3)
            ON CONFLICT(chat_id,user_id) DO UPDATE SET joined_at=$3
        """, GROUP_ID, user_id, int(time.time()))
        inv = cmu.invite_link.invite_link if cmu.invite_link else None
        if inv:
            owner = await DB.fetchval("SELECT owner_id FROM reward_links WHERE chat_id=$1 AND invite_link=$2", GROUP_ID, inv)
            if owner and owner != user_id:
                inserted = await DB.execute("""
                    INSERT INTO reward_joined(chat_id, joined_user_id, owner_id) VALUES($1,$2,$3)
                    ON CONFLICT DO NOTHING
                """, GROUP_ID, user_id, owner)
                if inserted.endswith("1"):
                    count = await DB.fetchval("""
                        UPDATE reward_links SET joins_count=joins_count+1 WHERE chat_id=$1 AND owner_id=$2 RETURNING joins_count
                    """, GROUP_ID, owner)
                    if count >= REWARD_REQUIRED_JOINS:
                        delivered = await DB.fetchval("SELECT delivered FROM reward_links WHERE chat_id=$1 AND owner_id=$2", GROUP_ID, owner)
                        if not delivered:
                            gofile = await DB.fetchval("SELECT gofile_link FROM settings WHERE chat_id=$1", GROUP_ID)
                            if gofile:
                                try:
                                    await context.bot.send_message(owner, f"🎁 Bravo ! Voici ton lien :\n{gofile}")
                                    await DB.execute("UPDATE reward_links SET delivered=TRUE WHERE chat_id=$1 AND owner_id=$2", GROUP_ID, owner)
                                except Exception:
                                    pass


async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if q.data == "share":
        user = q.from_user
        row = await DB.fetchrow("SELECT invite_link, joins_count, delivered FROM reward_links WHERE chat_id=$1 AND owner_id=$2", GROUP_ID, user.id)
        if not row:
            link = await context.bot.create_chat_invite_link(GROUP_ID, name=f"reward_{user.id}_{int(time.time())}")
            await DB.execute("INSERT INTO reward_links(chat_id,owner_id,invite_link) VALUES($1,$2,$3)", GROUP_ID, user.id, link.invite_link)
            invite = link.invite_link
            count = 0
        else:
            invite, count = row["invite_link"], row["joins_count"]
        try:
            await context.bot.send_message(user.id, f"Partage ce lien. Quand {REWARD_REQUIRED_JOINS} personnes rejoignent, tu reçois le lien privé.\n\n{invite}\n\nProgression : {count}/{REWARD_REQUIRED_JOINS}")
            await q.answer("Lien envoyé en privé.", show_alert=True)
        except Exception:
            await q.answer("Ouvre d’abord le bot en privé avec /start, puis reclique.", show_alert=True)
        return

    if not await ensure_admin(update, context):
        return

    if q.data in ("toggle_on", "toggle_off"):
        val = q.data == "toggle_on"
        await DB.execute("UPDATE settings SET messages_open=$1 WHERE chat_id=$2", val, GROUP_ID)
        await q.edit_message_text(f"Messages utilisateurs : {'ON 🟢' if val else 'OFF 🔴'}", reply_markup=admin_panel())
    elif q.data in ("word_add", "word_del", "broadcast", "set_gofile"):
        USER_STATE[q.from_user.id] = q.data
        prompts = {
            "word_add": "Envoie le mot interdit à ajouter.",
            "word_del": "Envoie le mot interdit à supprimer.",
            "broadcast": "Envoie le message à broadcaster dans le groupe.",
            "set_gofile": "Envoie le nouveau lien Gofile.",
        }
        await q.message.reply_text(prompts[q.data])
    elif q.data == "word_list":
        rows = await DB.fetch("SELECT word FROM forbidden_words WHERE chat_id=$1 ORDER BY word", GROUP_ID)
        txt = "Mots interdits :\n" + ("\n".join(f"- {r['word']}" for r in rows) or "Aucun")
        await q.message.reply_text(txt)
    elif q.data == "publish":
        gofile = await DB.fetchval("SELECT gofile_link FROM settings WHERE chat_id=$1", GROUP_ID)
        if not gofile:
            await q.message.reply_text("Ajoute d’abord un lien Gofile.")
            return
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("📤 Je partage", callback_data="share")]])
        caption = f"🎁 Récompense disponible !\n\nPartage le groupe avec ton lien personnalisé. Dès que {REWARD_REQUIRED_JOINS} personnes rejoignent, tu reçois le lien privé."
        await context.bot.send_photo(GROUP_ID, photo=REWARD_IMAGE_URL, caption=caption, reply_markup=kb)
        await q.message.reply_text("Publication envoyée dans le groupe.")


async def private_admin_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    state = USER_STATE.get(user_id)
    if not state or not await is_admin(update, context, user_id, GROUP_ID):
        return
    txt = update.message.text.strip()
    if state == "word_add":
        await DB.execute("""
            INSERT INTO forbidden_words(chat_id,word,normalized) VALUES($1,$2,$3)
            ON CONFLICT(chat_id,normalized) DO UPDATE SET word=$2
        """, GROUP_ID, txt, norm(txt))
        await update.message.reply_text("Mot ajouté.")
    elif state == "word_del":
        await DB.execute("DELETE FROM forbidden_words WHERE chat_id=$1 AND normalized=$2", GROUP_ID, norm(txt))
        await update.message.reply_text("Mot supprimé si présent.")
    elif state == "broadcast":
        await context.bot.send_message(GROUP_ID, txt, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        await update.message.reply_text("Broadcast envoyé.")
    elif state == "set_gofile":
        await DB.execute("UPDATE settings SET gofile_link=$1 WHERE chat_id=$2", txt, GROUP_ID)
        await update.message.reply_text("Lien Gofile enregistré.")
    USER_STATE.pop(user_id, None)


async def delete_later(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    try:
        await context.bot.delete_message(data["chat_id"], data["message_id"])
    except Exception:
        pass


async def deterrence(context: ContextTypes.DEFAULT_TYPE):
    messages = [
        "⚠️ Détection des comportements inhabituels…",
        "🔍 Analyse automatique du groupe en cours…",
        "🛡 Système anti-spam actif : comportements suspects surveillés…",
    ]
    try:
        m = await context.bot.send_message(GROUP_ID, random.choice(messages))
        context.job_queue.run_once(delete_later, 120, data={"chat_id": GROUP_ID, "message_id": m.message_id})
    except Exception as e:
        log.info("deterrence failed: %s", e)


async def post_init(app: Application):
    await init_db()
    app.job_queue.run_repeating(deterrence, interval=7200, first=random.randint(120, 900))
    log.info("Bot ready")


def main():
    if not BOT_TOKEN or not DATABASE_URL or not GROUP_ID:
        raise RuntimeError("BOT_TOKEN, DATABASE_URL et GROUP_ID sont obligatoires")
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", admin))
    app.add_handler(CallbackQueryHandler(buttons))
    app.add_handler(ChatMemberHandler(on_chat_member, ChatMemberHandler.CHAT_MEMBER))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT, private_admin_text))
    app.add_handler(MessageHandler(filters.Chat(GROUP_ID), handle_message))
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
