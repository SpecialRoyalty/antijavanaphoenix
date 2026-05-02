import os
import re
import time
import random
import logging
import unicodedata
from urllib.parse import quote_plus
from datetime import datetime, timedelta, timezone

import asyncpg
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatPermissions
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ChatMemberHandler, ContextTypes, filters

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("promo-guard-bot")

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
GROUP_ID = int(os.getenv("GROUP_ID", "0"))
ADMIN_IDS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()}
REWARD_IMAGE_URL = os.getenv("REWARD_IMAGE_URL", "https://postimg.cc/3W2rzWCs")
REWARD_REQUIRED_JOINS = int(os.getenv("REWARD_REQUIRED_JOINS", "6"))
DEFAULT_REWARD_TEXT = f"🎁 Récompense disponible !\n\nClique sur “Je partage” pour recevoir ton lien personnalisé. Dès que {REWARD_REQUIRED_JOINS} personnes rejoignent avec ton lien, tu reçois le lien ici. Tu peux également suivre ta progression."

URL_RE = re.compile(r"(https?://|www\.|t\.me/|telegram\.me/|bit\.ly/|gofile\.io/|discord\.gg/)", re.I)
AT_RE = re.compile(r"(^|\s)@[a-zA-Z0-9_]{3,32}\b")
DB: asyncpg.Pool | None = None
USER_STATE: dict[int, str] = {}
BOT_USERNAME = ""


def norm(text: str) -> str:
    text = unicodedata.normalize("NFD", text or "")
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    return text.lower()


def admin_panel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🟢 ON", callback_data="toggle_on"), InlineKeyboardButton("🔴 OFF", callback_data="toggle_off")],
        [InlineKeyboardButton("➕ Ajouter mot", callback_data="word_add"), InlineKeyboardButton("🗑 Supprimer mot", callback_data="word_del")],
        [InlineKeyboardButton("📋 Liste mots", callback_data="word_list"), InlineKeyboardButton("ℹ️ Info bot", callback_data="bot_info")],
        [InlineKeyboardButton("📢 Broadcast groupe", callback_data="broadcast_group"), InlineKeyboardButton("📨 Broadcast PV", callback_data="broadcast_private")],
        [InlineKeyboardButton("🎁 Lien Gofile", callback_data="set_gofile"), InlineKeyboardButton("✍️ Texte pub", callback_data="set_reward_text")],
        [InlineKeyboardButton("🚀 Publish nouvelle récompense", callback_data="publish")],
    ])


def user_home_panel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🎁 Voir les récompenses actives", callback_data="rewards_list")]])


def share_panel(campaign_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("📤 Je partage", callback_data=f"share:{campaign_id}")]])


async def init_db():
    global DB
    DB = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with DB.acquire() as con:
        await con.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            chat_id BIGINT PRIMARY KEY,
            messages_open BOOLEAN DEFAULT TRUE,
            gofile_link TEXT DEFAULT '',
            reward_image_url TEXT DEFAULT '',
            reward_text TEXT DEFAULT ''
        );
        ALTER TABLE settings ADD COLUMN IF NOT EXISTS reward_text TEXT DEFAULT '';

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
        CREATE TABLE IF NOT EXISTS bot_users (
            user_id BIGINT PRIMARY KEY,
            username TEXT DEFAULT '',
            first_name TEXT DEFAULT '',
            started_private BOOLEAN DEFAULT FALSE,
            updated_at BIGINT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS reward_campaigns (
            id BIGSERIAL PRIMARY KEY,
            chat_id BIGINT NOT NULL,
            gofile_link TEXT NOT NULL,
            image_url TEXT DEFAULT '',
            promo_text TEXT NOT NULL,
            required_joins INT NOT NULL DEFAULT 6,
            active BOOLEAN DEFAULT TRUE,
            created_at BIGINT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS reward_links (
            campaign_id BIGINT NOT NULL REFERENCES reward_campaigns(id) ON DELETE CASCADE,
            chat_id BIGINT NOT NULL,
            owner_id BIGINT NOT NULL,
            invite_link TEXT UNIQUE NOT NULL,
            joins_count INT DEFAULT 0,
            delivered BOOLEAN DEFAULT FALSE,
            created_at BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY(campaign_id, owner_id)
        );
        CREATE TABLE IF NOT EXISTS reward_joined (
            campaign_id BIGINT NOT NULL REFERENCES reward_campaigns(id) ON DELETE CASCADE,
            chat_id BIGINT NOT NULL,
            joined_user_id BIGINT NOT NULL,
            owner_id BIGINT NOT NULL,
            PRIMARY KEY(campaign_id, joined_user_id)
        );
        """)
        await con.execute("""
            INSERT INTO settings(chat_id, reward_image_url, reward_text)
            VALUES($1, $2, $3)
            ON CONFLICT(chat_id) DO NOTHING
        """, GROUP_ID, REWARD_IMAGE_URL, DEFAULT_REWARD_TEXT)


async def save_user(user, started_private: bool = False):
    if not user:
        return
    await DB.execute("""
        INSERT INTO bot_users(user_id,username,first_name,started_private,updated_at)
        VALUES($1,$2,$3,$4,$5)
        ON CONFLICT(user_id) DO UPDATE SET
            username=$2,
            first_name=$3,
            started_private=bot_users.started_private OR $4,
            updated_at=$5
    """, user.id, user.username or "", user.first_name or "", started_private, int(time.time()))


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


async def get_admin_panel_text() -> str:
    row = await DB.fetchrow("SELECT messages_open FROM settings WHERE chat_id=$1", GROUP_ID)
    status = "ON 🟢" if row and row["messages_open"] else "OFF 🔴"
    return f"Panel admin\nMessages utilisateurs : {status}"


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await save_user(update.effective_user, started_private=(update.effective_chat.type == "private"))
    if update.effective_chat.type != "private":
        await delete_safely(context, update.effective_chat.id, update.effective_message.message_id)
        return

    args = context.args or []
    if args and args[0].startswith("reward_"):
        try:
            campaign_id = int(args[0].split("_", 1)[1])
            await send_personal_share(update.effective_user.id, context, campaign_id)
            return
        except Exception:
            pass

    if await is_admin(update, context):
        await update.message.reply_text(await get_admin_panel_text(), reply_markup=admin_panel())
    else:
        await update.message.reply_text(
            "Bienvenue. Clique pour choisir la récompense et recevoir ton lien personnalisé de partage.",
            reply_markup=user_home_panel(),
        )


async def admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        await delete_safely(context, update.effective_chat.id, update.effective_message.message_id)
        return
    if not await ensure_admin(update, context):
        return
    await update.message.reply_text(await get_admin_panel_text(), reply_markup=admin_panel())


async def delete_safely(context, chat_id, message_id):
    try:
        await context.bot.delete_message(chat_id, message_id)
    except Exception as e:
        log.info("delete failed: %s", e)


async def mute_user(context, chat_id: int, user_id: int, days: int):
    until = datetime.now(timezone.utc) + timedelta(days=days)
    await context.bot.restrict_chat_member(chat_id, user_id, permissions=ChatPermissions(can_send_messages=False), until_date=until)


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
    await save_user(user, started_private=False)

    if msg.new_chat_members or msg.left_chat_member:
        for u in msg.new_chat_members or []:
            await save_user(u, started_private=False)
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

    if await check_forbidden(msg.chat_id, text) or URL_RE.search(text) or AT_RE.search(text):
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
        await save_user(cmu.new_chat_member.user, started_private=False)
        await DB.execute("""
            INSERT INTO joins(chat_id,user_id,joined_at) VALUES($1,$2,$3)
            ON CONFLICT(chat_id,user_id) DO UPDATE SET joined_at=$3
        """, GROUP_ID, user_id, int(time.time()))
        inv = cmu.invite_link.invite_link if cmu.invite_link else None
        if inv:
            row = await DB.fetchrow("SELECT campaign_id, owner_id, joins_count FROM reward_links WHERE chat_id=$1 AND invite_link=$2", GROUP_ID, inv)
            if row and row["owner_id"] != user_id:
                inserted = await DB.execute("""
                    INSERT INTO reward_joined(campaign_id, chat_id, joined_user_id, owner_id) VALUES($1,$2,$3,$4)
                    ON CONFLICT DO NOTHING
                """, row["campaign_id"], GROUP_ID, user_id, row["owner_id"])
                if inserted.endswith("1"):
                    count = await DB.fetchval("""
                        UPDATE reward_links SET joins_count=joins_count+1
                        WHERE campaign_id=$1 AND owner_id=$2 RETURNING joins_count
                    """, row["campaign_id"], row["owner_id"])
                    campaign = await DB.fetchrow("SELECT gofile_link, required_joins FROM reward_campaigns WHERE id=$1", row["campaign_id"])
                    if campaign and count >= campaign["required_joins"]:
                        delivered = await DB.fetchval("SELECT delivered FROM reward_links WHERE campaign_id=$1 AND owner_id=$2", row["campaign_id"], row["owner_id"])
                        if not delivered:
                            try:
                                await context.bot.send_message(row["owner_id"], f"🎁 Bravo ! Voici ton lien :\n{campaign['gofile_link']}")
                                await DB.execute("UPDATE reward_links SET delivered=TRUE WHERE campaign_id=$1 AND owner_id=$2", row["campaign_id"], row["owner_id"])
                            except Exception:
                                pass


async def list_active_rewards(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    rows = await DB.fetch("""
        SELECT id, promo_text, required_joins, created_at
        FROM reward_campaigns
        WHERE chat_id=$1 AND active=TRUE
        ORDER BY id DESC
        LIMIT 10
    """, GROUP_ID)
    if not rows:
        await context.bot.send_message(user_id, "Aucune récompense active pour le moment.")
        return
    buttons = []
    lines = ["🎁 Récompenses actives :"]
    for r in rows:
        text = re.sub(r"\s+", " ", r["promo_text"]).strip()[:55]
        lines.append(f"\n#{r['id']} — objectif {r['required_joins']} partages")
        buttons.append([InlineKeyboardButton(f"🎁 Recevoir mon lien — #{r['id']}", callback_data=f"share:{r['id']}")])
    await context.bot.send_message(user_id, "\n".join(lines), reply_markup=InlineKeyboardMarkup(buttons))


async def send_personal_share(user_id: int, context: ContextTypes.DEFAULT_TYPE, campaign_id: int):
    campaign = await DB.fetchrow("SELECT id, gofile_link, required_joins, active FROM reward_campaigns WHERE id=$1 AND chat_id=$2", campaign_id, GROUP_ID)
    if not campaign or not campaign["active"]:
        await context.bot.send_message(user_id, "Cette récompense n’est plus active.", reply_markup=user_home_panel())
        return

    row = await DB.fetchrow("SELECT invite_link, joins_count, delivered FROM reward_links WHERE campaign_id=$1 AND owner_id=$2", campaign_id, user_id)
    if not row:
        link = await context.bot.create_chat_invite_link(GROUP_ID, name=f"c{campaign_id}_u{user_id}"[:32])
        await DB.execute("""
            INSERT INTO reward_links(campaign_id,chat_id,owner_id,invite_link,created_at)
            VALUES($1,$2,$3,$4,$5)
        """, campaign_id, GROUP_ID, user_id, link.invite_link, int(time.time()))
        invite, count, delivered = link.invite_link, 0, False
    else:
        invite, count, delivered = row["invite_link"], row["joins_count"], row["delivered"]

    if delivered:
        await context.bot.send_message(user_id, f"✅ Récompense déjà débloquée. Voici ton lien :\n{campaign['gofile_link']}")
        return

    share_text = f"Rejoins ce groupe pour débloquer une récompense : {invite}"
    share_url = f"https://t.me/share/url?url={quote_plus(invite)}&text={quote_plus(share_text)}"
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("📤 Envoyer le lien", url=share_url)]])
    await context.bot.send_message(
        user_id,
        f"Partage ce lien personnalisé. Quand {campaign['required_joins']} personnes rejoignent avec TON lien, tu reçois le lien ici. Tu peux également suivre ta progression.\n\n{invite}\n\nProgression : {count}/{campaign['required_joins']}",
        reply_markup=kb,
    )


async def bot_info_text(context: ContextTypes.DEFAULT_TYPE) -> str:
    lines = ["ℹ️ Info bot"]
    try:
        await DB.fetchval("SELECT 1")
        lines.append("Base de données : ✅ branchée")
    except Exception as e:
        lines.append(f"Base de données : ❌ {e}")
    try:
        chat = await context.bot.get_chat(GROUP_ID)
        me = await context.bot.get_me()
        member = await context.bot.get_chat_member(GROUP_ID, me.id)
        lines.append(f"Groupe : ✅ {chat.title or GROUP_ID}")
        lines.append(f"Bot admin : {'✅' if member.status in ('administrator','creator') else '❌'}")
        lines.append(f"Supprimer messages : {'✅' if getattr(member, 'can_delete_messages', False) else '❌'}")
        lines.append(f"Mute/restrictions : {'✅' if getattr(member, 'can_restrict_members', False) else '❌'}")
        lines.append(f"Créer liens invitation : {'✅' if getattr(member, 'can_invite_users', False) else '❌'}")
    except Exception as e:
        lines.append(f"Groupe : ❌ erreur avec GROUP_ID / droits bot : {e}")
    users = await DB.fetchval("SELECT COUNT(*) FROM bot_users WHERE started_private=TRUE")
    campaigns = await DB.fetchval("SELECT COUNT(*) FROM reward_campaigns WHERE chat_id=$1 AND active=TRUE", GROUP_ID)
    lines.append(f"Utilisateurs PV enregistrés : {users}")
    lines.append(f"Récompenses actives : {campaigns}")
    lines.append(f"Bot username : @{BOT_USERNAME}" if BOT_USERNAME else "Bot username : inconnu")
    return "\n".join(lines)


async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await save_user(q.from_user, started_private=(q.message and q.message.chat.type == "private"))

    if q.data == "rewards_list":
        try:
            await list_active_rewards(q.from_user.id, context)
            await q.answer("Liste envoyée ici.", show_alert=False)
        except Exception:
            await q.answer("Ouvre d’abord le bot en privé avec /start.", show_alert=True)
        return

    if q.data.startswith("share:"):
        campaign_id = int(q.data.split(":", 1)[1])
        try:
            await send_personal_share(q.from_user.id, context, campaign_id)
            await q.answer("Lien personnalisé envoyé ici/en privé.", show_alert=True)
        except Exception:
            start_link = f"https://t.me/{BOT_USERNAME}?start=reward_{campaign_id}" if BOT_USERNAME else "le bot en privé"
            await q.answer(f"Ouvre d’abord le bot en privé : {start_link}", show_alert=True)
        return

    if not await ensure_admin(update, context):
        return

    if q.data in ("toggle_on", "toggle_off"):
        val = q.data == "toggle_on"
        await DB.execute("UPDATE settings SET messages_open=$1 WHERE chat_id=$2", val, GROUP_ID)
        await q.edit_message_text(await get_admin_panel_text(), reply_markup=admin_panel())
    elif q.data in ("word_add", "word_del", "broadcast_group", "broadcast_private", "set_gofile", "set_reward_text"):
        USER_STATE[q.from_user.id] = q.data
        prompts = {
            "word_add": "Envoie le mot interdit à ajouter.",
            "word_del": "Envoie le mot interdit à supprimer.",
            "broadcast_group": "Envoie le message à broadcaster dans le groupe.",
            "broadcast_private": "Envoie le message à broadcaster en privé aux utilisateurs qui ont déjà démarré le bot.",
            "set_gofile": "Envoie le nouveau lien Gofile. Il sera utilisé pour la PROCHAINE récompense publiée.",
            "set_reward_text": "Envoie le texte de la prochaine publication. Tu peux changer ce texte à chaque fichier/récompense.",
        }
        await q.message.reply_text(prompts[q.data])
    elif q.data == "word_list":
        rows = await DB.fetch("SELECT word FROM forbidden_words WHERE chat_id=$1 ORDER BY word", GROUP_ID)
        txt = "Mots interdits :\n" + ("\n".join(f"- {r['word']}" for r in rows) or "Aucun")
        await q.message.reply_text(txt)
    elif q.data == "bot_info":
        await q.message.reply_text(await bot_info_text(context))
    elif q.data == "publish":
        row = await DB.fetchrow("SELECT gofile_link, reward_image_url, reward_text FROM settings WHERE chat_id=$1", GROUP_ID)
        gofile = row["gofile_link"] if row else ""
        if not gofile:
            await q.message.reply_text("Ajoute d’abord un lien Gofile.")
            return
        promo_text = (row["reward_text"] or DEFAULT_REWARD_TEXT).replace("{required}", str(REWARD_REQUIRED_JOINS))
        image_url = row["reward_image_url"] or REWARD_IMAGE_URL
        campaign_id = await DB.fetchval("""
            INSERT INTO reward_campaigns(chat_id,gofile_link,image_url,promo_text,required_joins,created_at)
            VALUES($1,$2,$3,$4,$5,$6)
            RETURNING id
        """, GROUP_ID, gofile, image_url, promo_text, REWARD_REQUIRED_JOINS, int(time.time()))
        await context.bot.send_photo(GROUP_ID, photo=image_url, caption=promo_text, reply_markup=share_panel(campaign_id))
        await q.message.reply_text(f"Publication envoyée dans le groupe. Récompense #{campaign_id} créée.")


async def private_admin_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await save_user(update.effective_user, started_private=True)
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
    elif state == "broadcast_group":
        await context.bot.send_message(GROUP_ID, txt, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        await update.message.reply_text("Broadcast groupe envoyé.")
    elif state == "broadcast_private":
        rows = await DB.fetch("SELECT user_id FROM bot_users WHERE started_private=TRUE")
        ok = fail = 0
        for r in rows:
            try:
                await context.bot.send_message(r["user_id"], txt, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                ok += 1
            except Exception:
                fail += 1
        await update.message.reply_text(f"Broadcast PV terminé. Envoyés : {ok}. Échecs : {fail}.")
    elif state == "set_gofile":
        await DB.execute("UPDATE settings SET gofile_link=$1 WHERE chat_id=$2", txt, GROUP_ID)
        await update.message.reply_text("Lien Gofile enregistré pour la prochaine publication.")
    elif state == "set_reward_text":
        await DB.execute("UPDATE settings SET reward_text=$1 WHERE chat_id=$2", txt, GROUP_ID)
        await update.message.reply_text("Texte de publication enregistré pour la prochaine récompense.")
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
    global BOT_USERNAME
    await init_db()
    me = await app.bot.get_me()
    BOT_USERNAME = me.username or ""
    app.job_queue.run_repeating(deterrence, interval=7200, first=random.randint(120, 900))
    log.info("Bot ready as @%s", BOT_USERNAME)


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
