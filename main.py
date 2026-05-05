# V17 FULL CLEAN — Promo Guard Bot Railway
# Contient : rewards, gestion admin, notifications anciens gagnants, join validé 5 min,
# modération liens/@/forward/langue/médias, slash mute groupe.
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

try:
    from langdetect import detect_langs, LangDetectException
except Exception:
    detect_langs = None
    LangDetectException = Exception

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatPermissions
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

REWARD_IMAGE_URL = os.getenv(
    "REWARD_IMAGE_URL",
    "https://i.postimg.cc/XNzZGCZY/5475f4b9-b4f6-4fc1-b072-a9be4132adb4.jpg",
)
REWARD_REQUIRED_JOINS = int(os.getenv("REWARD_REQUIRED_JOINS", "6"))

DEFAULT_REWARD_TEXT = (
    "🎁 Récompense disponible !\n\n"
    "Clique sur “Recevoir mon lien” pour obtenir ton lien personnalisé.\n\n"
    f"Quand {REWARD_REQUIRED_JOINS} personnes rejoignent avec TON lien, "
    "tu reçois le lien ici.\n\n"
    "Tu peux également suivre ta progression."
)

URL_RE = re.compile(r"(https?://|www\.|t\.me/|telegram\.me/|bit\.ly/|gofile\.io/|discord\.gg/)", re.I)
AT_RE = re.compile(r"(^|\s)@[a-zA-Z0-9_]{3,32}\b")
FR_EXTRA_RE = re.compile(r"[àâäçéèêëîïôöùûüÿœæ]", re.I)
LANG_MIN_LETTERS = int(os.getenv("LANG_MIN_LETTERS", "3"))
LANG_CONFIDENCE = float(os.getenv("LANG_CONFIDENCE", "0.85"))

DB: asyncpg.Pool | None = None
USER_STATE: dict[int, str] = {}
CREATE_FLOW: dict[int, dict] = {}
BOT_USERNAME = ""


def norm(text: str) -> str:
    text = unicodedata.normalize("NFD", text or "")
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    return text.lower()


def admin_panel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🟢 ON", callback_data="toggle_on"),
            InlineKeyboardButton("🔴 OFF", callback_data="toggle_off"),
        ],
        [
            InlineKeyboardButton("➕ Ajouter mot", callback_data="word_add"),
            InlineKeyboardButton("🗑 Supprimer mot", callback_data="word_del"),
        ],
        [
            InlineKeyboardButton("📋 Liste mots", callback_data="word_list"),
            InlineKeyboardButton("ℹ️ Info bot", callback_data="bot_info"),
        ],
        [
            InlineKeyboardButton("📢 Broadcast groupe", callback_data="broadcast_group"),
            InlineKeyboardButton("📨 Broadcast PV", callback_data="broadcast_private"),
        ],
        [InlineKeyboardButton("✍️ Texte pub", callback_data="set_reward_text")],
        [InlineKeyboardButton("🖼 Image pub", callback_data="set_reward_image")],
        [InlineKeyboardButton("🎛 Gérer récompenses", callback_data="manage_rewards")],
        [InlineKeyboardButton("🚀 Publish nouvelle récompense", callback_data="publish")],
    ])


def user_home_panel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎁 Voir les récompenses actives", callback_data="rewards_list")]
    ])


def share_panel(campaign_id: int) -> InlineKeyboardMarkup:
    """
    Bouton affiché dans le groupe sous la publication.
    Important :
    - Telegram ne permet pas au bot d’envoyer un PV si l’utilisateur n’a jamais ouvert le bot.
    - Donc le bouton ouvre le bot en privé avec /start reward_ID.
    """
    if BOT_USERNAME:
        url = f"https://t.me/{BOT_USERNAME}?start=reward_{campaign_id}"
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🔐 Recevoir le mot de passe", url=url)]
        ])

    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔐 Recevoir le mot de passe", callback_data=f"share:{campaign_id}")]
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
            reward_image_url TEXT DEFAULT '',
            reward_text TEXT DEFAULT ''
        );

        ALTER TABLE settings ADD COLUMN IF NOT EXISTS reward_text TEXT DEFAULT '';
        ALTER TABLE settings ADD COLUMN IF NOT EXISTS reward_image_url TEXT DEFAULT '';
        ALTER TABLE settings ADD COLUMN IF NOT EXISTS gofile_link TEXT DEFAULT '';

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
            seen_at BIGINT DEFAULT 0,
            PRIMARY KEY(chat_id, user_id)
        );

        ALTER TABLE joins ADD COLUMN IF NOT EXISTS seen_at BIGINT DEFAULT 0;

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
            password TEXT DEFAULT '',
            is_free BOOLEAN DEFAULT FALSE,
            image_url TEXT DEFAULT '',
            promo_text TEXT NOT NULL,
            required_joins INT NOT NULL DEFAULT 6,
            active BOOLEAN DEFAULT TRUE,
            created_at BIGINT NOT NULL
        );

        ALTER TABLE reward_campaigns ADD COLUMN IF NOT EXISTS password TEXT DEFAULT '';
        ALTER TABLE reward_campaigns ADD COLUMN IF NOT EXISTS is_free BOOLEAN DEFAULT FALSE;

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

        CREATE TABLE IF NOT EXISTS pending_reward_joins (
            campaign_id BIGINT NOT NULL REFERENCES reward_campaigns(id) ON DELETE CASCADE,
            chat_id BIGINT NOT NULL,
            joined_user_id BIGINT NOT NULL,
            owner_id BIGINT NOT NULL,
            invite_link TEXT NOT NULL,
            joined_at BIGINT NOT NULL,
            PRIMARY KEY(campaign_id, joined_user_id)
        );
        """)

        await con.execute("""
            INSERT INTO settings(chat_id, reward_image_url, reward_text)
            VALUES($1, $2, $3)
            ON CONFLICT(chat_id) DO NOTHING
        """, GROUP_ID, REWARD_IMAGE_URL, DEFAULT_REWARD_TEXT)

        await con.execute("""
            UPDATE settings
            SET reward_image_url=$2
            WHERE chat_id=$1
              AND (reward_image_url='' OR reward_image_url IS NULL OR reward_image_url LIKE 'https://picsum.photos/%')
        """, GROUP_ID, REWARD_IMAGE_URL)


async def save_user(user, started_private: bool = False):
    if not user:
        return

    await DB.execute("""
        INSERT INTO bot_users(user_id, username, first_name, started_private, updated_at)
        VALUES($1, $2, $3, $4, $5)
        ON CONFLICT(user_id) DO UPDATE SET
            username=$2,
            first_name=$3,
            started_private=bot_users.started_private OR $4,
            updated_at=$5
    """, user.id, user.username or "", user.first_name or "", started_private, int(time.time()))


async def is_admin(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int | None = None,
    chat_id: int | None = None,
) -> bool:
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


async def delete_safely(context, chat_id, message_id):
    try:
        await context.bot.delete_message(chat_id, message_id)
    except Exception as e:
        log.info("delete failed: %s", e)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await save_user(update.effective_user, started_private=(update.effective_chat.type == "private"))

    if update.effective_chat.type != "private":
        # /start dans le groupe = mute 1 mois pour les non-admins.
        if not await is_admin(update, context, update.effective_user.id, update.effective_chat.id):
            await mute_user(context, update.effective_chat.id, update.effective_user.id, 30)
        await delete_safely(context, update.effective_chat.id, update.effective_message.message_id)
        return

    args = context.args or []

    if args and args[0].startswith("reward_"):
        try:
            campaign_id = int(args[0].split("_", 1)[1])
            await send_personal_share(update.effective_user.id, context, campaign_id)
            return
        except Exception as e:
            log.exception("start reward failed: %s", e)
            await update.message.reply_text("Erreur pendant la récupération du lien. Réessaie.")
            return

    if await is_admin(update, context):
        await update.message.reply_text(await get_admin_panel_text(), reply_markup=admin_panel())
    else:
        await update.message.reply_text(
            "Bienvenue. Clique pour choisir une récompense et recevoir ton lien personnalisé de partage.",
            reply_markup=user_home_panel(),
        )


async def admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        # /admin dans le groupe = mute 1 mois pour les non-admins.
        if not await is_admin(update, context, update.effective_user.id, update.effective_chat.id):
            await mute_user(context, update.effective_chat.id, update.effective_user.id, 30)
        await delete_safely(context, update.effective_chat.id, update.effective_message.message_id)
        return

    if not await ensure_admin(update, context):
        return

    await update.message.reply_text(await get_admin_panel_text(), reply_markup=admin_panel())


async def mute_user(context, chat_id: int, user_id: int, days: int):
    until = datetime.now(timezone.utc) + timedelta(days=days)
    await context.bot.restrict_chat_member(
        chat_id,
        user_id,
        permissions=ChatPermissions(can_send_messages=False),
        until_date=until,
    )


async def ban_user(context, chat_id: int, user_id: int):
    """
    Ban direct, sans message public.
    """
    try:
        await context.bot.ban_chat_member(chat_id, user_id)
    except Exception as e:
        log.warning("ban failed for %s: %s", user_id, e)


async def punish_forbidden_word(context, chat_id: int, user_id: int) -> int:
    """
    Mot interdit :
    - 1ère fois : mute 1 jour
    - 2ème fois et suivantes : mute 30 jours
    L'utilisateur peut lire mais ne peut plus envoyer.
    """
    count = await DB.fetchval("""
        INSERT INTO violations(chat_id, user_id, count)
        VALUES($1, $2, 1)
        ON CONFLICT(chat_id, user_id)
        DO UPDATE SET count=violations.count+1
        RETURNING count
    """, chat_id, user_id)

    days = 30 if count and count >= 2 else 1
    await mute_user(context, chat_id, user_id, days)
    return days


def has_media(msg) -> bool:
    return any([
        msg.photo,
        msg.video,
        msg.animation,
        msg.document,
        msg.sticker,
        msg.audio,
        msg.voice,
        msg.video_note,
        msg.contact,
        msg.location,
        msg.venue,
        msg.poll,
    ])


async def check_forbidden(chat_id: int, text: str) -> str | None:
    rows = await DB.fetch("SELECT normalized FROM forbidden_words WHERE chat_id=$1", chat_id)
    ntext = norm(text)

    for r in rows:
        if r["normalized"] in ntext:
            return r["normalized"]

    return None


def should_ban_non_french(text: str) -> tuple[bool, str]:
    """
    Détection langue via langdetect.
    Retourne (True, langue) si le message semble clairement non-français.
    Sécurité anti-faux positifs :
    - ignore les messages trop courts
    - ignore si langdetect n'est pas installé
    - demande une confiance suffisante
    """
    if not text or not detect_langs:
        return False, ""

    clean = re.sub(r"https?://\S+|www\.\S+|@\w+", " ", text)
    letters = re.findall(r"[A-Za-zÀ-ÖØ-öø-ÿ]", clean)

    if len(letters) < LANG_MIN_LETTERS:
        return False, ""

    # Beaucoup de français courts sont mal détectés ; les accents aident à confirmer.
    try:
        langs = detect_langs(clean)
    except LangDetectException:
        return False, ""

    if not langs:
        return False, ""

    top = langs[0]
    lang = top.lang
    prob = float(top.prob)

    if lang == "fr":
        return False, lang

    # Si le texte contient des accents français, on évite de ban sauf confiance très forte.
    if FR_EXTRA_RE.search(clean) and prob < 0.95:
        return False, lang

    return prob >= LANG_CONFIDENCE, lang


def is_forwarded_message(msg) -> bool:
    """
    Détecte les messages transférés/forwardés.
    Compatible avec plusieurs versions python-telegram-bot.
    """
    return any([
        getattr(msg, "forward_date", None),
        getattr(msg, "forward_from", None),
        getattr(msg, "forward_from_chat", None),
        getattr(msg, "forward_sender_name", None),
        getattr(msg, "forward_origin", None),
    ])


async def get_or_create_join_time(chat_id: int, user_id: int) -> int:
    """
    Telegram peut parfois rater/retarder l'événement de join.
    Pour éviter le bypass :
    - si joined_at existe, on l'utilise.
    - sinon, on enregistre le premier message vu comme seen_at/joined_at fallback.
    """
    now = int(time.time())
    row = await DB.fetchrow(
        "SELECT joined_at, seen_at FROM joins WHERE chat_id=$1 AND user_id=$2",
        chat_id,
        user_id,
    )

    if row:
        joined_at = int(row["joined_at"] or 0)
        seen_at = int(row["seen_at"] or 0)

        if not seen_at:
            await DB.execute(
                "UPDATE joins SET seen_at=$3 WHERE chat_id=$1 AND user_id=$2",
                chat_id,
                user_id,
                now,
            )

        return joined_at or seen_at or now

    await DB.execute("""
        INSERT INTO joins(chat_id, user_id, joined_at, seen_at)
        VALUES($1, $2, $3, $3)
        ON CONFLICT(chat_id, user_id) DO NOTHING
    """, chat_id, user_id, now)

    return now


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    user = update.effective_user

    if not msg or not user or msg.chat_id != GROUP_ID:
        return

    await save_user(user, started_private=False)

    # Service messages : entrée/sortie, ajout bot.
    if msg.new_chat_members or msg.left_chat_member:
        for u in msg.new_chat_members or []:
            await save_user(u, started_private=False)
            await DB.execute("""
                INSERT INTO joins(chat_id, user_id, joined_at, seen_at)
                VALUES($1, $2, $3, 0)
                ON CONFLICT(chat_id, user_id)
                DO UPDATE SET joined_at=$3, seen_at=0
            """, msg.chat_id, u.id, int(time.time()))

            # Si un non-admin ajoute un bot, ban direct de l'utilisateur + du bot ajouté.
            if getattr(u, "is_bot", False) and not await is_admin(update, context, user.id, msg.chat_id):
                await ban_user(context, msg.chat_id, user.id)
                await ban_user(context, msg.chat_id, u.id)

        await delete_safely(context, msg.chat_id, msg.message_id)
        return

    # Piège permissions ouvertes :
    # si un non-admin change nom/photo/supprime photo du groupe => ban direct.
    if msg.new_chat_title or msg.new_chat_photo or msg.delete_chat_photo:
        if not await is_admin(update, context, user.id, msg.chat_id):
            await ban_user(context, msg.chat_id, user.id)
        await delete_safely(context, msg.chat_id, msg.message_id)
        return

    if await is_admin(update, context, user.id, msg.chat_id):
        return

    text = msg.text or msg.caption or ""

    # Toute commande "/" dans le groupe = mute 1 mois pour les non-admins.
    # L'utilisateur peut lire, mais ne peut plus écrire.
    if msg.text and msg.text.strip().startswith("/"):
        await delete_safely(context, msg.chat_id, msg.message_id)
        await mute_user(context, msg.chat_id, user.id, 30)
        return

    # Enregistre un fallback d'arrivée au premier message vu.
    # Ça rend la règle média 2 minutes beaucoup plus fiable si Telegram rate le join.
    joined_at = await get_or_create_join_time(msg.chat_id, user.id)

    # Forward :
    # - forward normal = supprimé
    # - forward avec lien/@ = ban direct
    # - forward dans les 2 minutes après arrivée/rejoin = ban direct
    if is_forwarded_message(msg):
        await delete_safely(context, msg.chat_id, msg.message_id)

        if (text and (URL_RE.search(text) or AT_RE.search(text))) or int(time.time()) - int(joined_at) <= 120:
            await ban_user(context, msg.chat_id, user.id)

        return

    # Règle prioritaire, même si le groupe est ON ou OFF :
    # média interdit pour utilisateurs normaux.
    # Si média envoyé dans les 2 minutes après arrivée/rejoin => ban direct.
    # Après 2 minutes => suppression seulement.
    if has_media(msg):
        await delete_safely(context, msg.chat_id, msg.message_id)

        if int(time.time()) - int(joined_at) <= 120:
            await ban_user(context, msg.chat_id, user.id)

        return

    # Règle prioritaire, même si le groupe est ON ou OFF :
    # lien ou @ = ban direct, sans message public.
    if text and (URL_RE.search(text) or AT_RE.search(text)):
        await delete_safely(context, msg.chat_id, msg.message_id)
        await ban_user(context, msg.chat_id, user.id)
        return

    # Mot interdit, même si le groupe est ON ou OFF :
    # 1ère fois mute 1 jour, récidive mute 30 jours.
    if text and await check_forbidden(msg.chat_id, text):
        await delete_safely(context, msg.chat_id, msg.message_id)
        await punish_forbidden_word(context, msg.chat_id, user.id)
        return

    # Langue non française détectée clairement = ban direct.
    # Fonctionne ON ou OFF. Pas de message public.
    ban_lang, detected_lang = should_ban_non_french(text)
    if text and ban_lang:
        await delete_safely(context, msg.chat_id, msg.message_id)
        await ban_user(context, msg.chat_id, user.id)
        log.info("Banned non-French message from %s detected=%s text=%r", user.id, detected_lang, text[:80])
        return

    # Message texte envoyé dans les 2 minutes après arrivée/rejoin => mute 1 jour.
    # Pas de message public.
    if text and int(time.time()) - int(joined_at) <= 120:
        await delete_safely(context, msg.chat_id, msg.message_id)
        await mute_user(context, msg.chat_id, user.id, 1)
        return

    row = await DB.fetchrow("SELECT messages_open FROM settings WHERE chat_id=$1", msg.chat_id)
    messages_open = bool(row["messages_open"]) if row else True

    # Si OFF, tout message utilisateur normal est supprimé,
    # mais les règles média/lien/@/mots interdits ont déjà été appliquées avant.
    if not messages_open:
        await delete_safely(context, msg.chat_id, msg.message_id)
        return

    if not text:
        await delete_safely(context, msg.chat_id, msg.message_id)
        return


def progress_text(count: int, required: int) -> str:
    remaining = max(required - count, 0)

    if remaining <= 0:
        return "🎁 C’EST BON !"
    if remaining == 1:
        return "🚨 DERNIÈRE ÉTAPE !\n\nPlus qu’1 personne et tu débloques tout 😈"
    if remaining == 2:
        return "🔥 Encore 2 personnes !\nTu es proche du but…"

    return f"🔥 Encore {remaining} personnes !\nContinue, tu avances bien."


async def notify_progress(context: ContextTypes.DEFAULT_TYPE, owner_id: int, campaign_id: int, count: int, required: int):
    try:
        await context.bot.send_message(
            owner_id,
            "🔔 +1 validé !\n\n"
            f"Progression : {count}/{required}\n\n"
            f"{progress_text(count, required)}",
        )
    except Exception as e:
        log.info("progress notification failed for %s: %s", owner_id, e)


async def validate_pending_join(context: ContextTypes.DEFAULT_TYPE):
    """
    Le join ne compte que si la personne reste au moins 5 minutes.
    """
    data = context.job.data or {}
    campaign_id = int(data["campaign_id"])
    joined_user_id = int(data["joined_user_id"])

    pending = await DB.fetchrow("""
        SELECT campaign_id, chat_id, joined_user_id, owner_id
        FROM pending_reward_joins
        WHERE campaign_id=$1 AND joined_user_id=$2
    """, campaign_id, joined_user_id)

    if not pending:
        return

    chat_id = int(pending["chat_id"])
    owner_id = int(pending["owner_id"])

    try:
        member = await context.bot.get_chat_member(chat_id, joined_user_id)
        if member.status in ("left", "kicked"):
            await DB.execute(
                "DELETE FROM pending_reward_joins WHERE campaign_id=$1 AND joined_user_id=$2",
                campaign_id,
                joined_user_id,
            )
            return
    except Exception as e:
        log.info("validate pending join failed: %s", e)
        return

    inserted = await DB.execute("""
        INSERT INTO reward_joined(campaign_id, chat_id, joined_user_id, owner_id)
        VALUES($1, $2, $3, $4)
        ON CONFLICT DO NOTHING
    """, campaign_id, chat_id, joined_user_id, owner_id)

    await DB.execute(
        "DELETE FROM pending_reward_joins WHERE campaign_id=$1 AND joined_user_id=$2",
        campaign_id,
        joined_user_id,
    )

    if not inserted.endswith("1"):
        return

    count = await DB.fetchval("""
        UPDATE reward_links
        SET joins_count=joins_count+1
        WHERE campaign_id=$1 AND owner_id=$2
        RETURNING joins_count
    """, campaign_id, owner_id)

    campaign = await DB.fetchrow("""
        SELECT gofile_link, password, required_joins
        FROM reward_campaigns
        WHERE id=$1
    """, campaign_id)

    if not campaign:
        return

    required = int(campaign["required_joins"])

    if int(count) >= required:
        delivered = await DB.fetchval("""
            SELECT delivered
            FROM reward_links
            WHERE campaign_id=$1 AND owner_id=$2
        """, campaign_id, owner_id)

        if not delivered:
            try:
                await context.bot.send_message(
                    owner_id,
                    "🎁 C’EST BON !\n\n"
                    "Tu as débloqué le mot de passe 🔥\n\n"
                    f"🔗 Lien : {campaign['gofile_link']}\n"
                    f"🔐 Mot de passe : {campaign['password'] or 'Aucun'}",
                )
                await DB.execute("""
                    UPDATE reward_links
                    SET delivered=TRUE
                    WHERE campaign_id=$1 AND owner_id=$2
                """, campaign_id, owner_id)
            except Exception as e:
                log.warning("Impossible d’envoyer le Gofile en PV à %s: %s", owner_id, e)
    else:
        await notify_progress(context, owner_id, campaign_id, int(count), required)


async def on_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cmu = update.chat_member

    if not cmu or cmu.chat.id != GROUP_ID:
        return

    old = cmu.old_chat_member.status
    new = cmu.new_chat_member.status

    if old in ("left", "kicked") and new in ("member", "restricted"):
        user_id = cmu.new_chat_member.user.id

        await save_user(cmu.new_chat_member.user, started_private=False)

        await DB.execute("""
            INSERT INTO joins(chat_id, user_id, joined_at, seen_at)
            VALUES($1, $2, $3, 0)
            ON CONFLICT(chat_id, user_id)
            DO UPDATE SET joined_at=$3, seen_at=0
        """, GROUP_ID, user_id, int(time.time()))

        inv = cmu.invite_link.invite_link if cmu.invite_link else None

        if not inv:
            return

        row = await DB.fetchrow("""
            SELECT campaign_id, owner_id, joins_count
            FROM reward_links
            WHERE chat_id=$1 AND invite_link=$2
        """, GROUP_ID, inv)

        if not row:
            return

        if row["owner_id"] == user_id:
            return

        already_counted = await DB.fetchval("""
            SELECT 1
            FROM reward_joined
            WHERE campaign_id=$1 AND joined_user_id=$2
        """, row["campaign_id"], user_id)

        if already_counted:
            return

        inserted = await DB.execute("""
            INSERT INTO pending_reward_joins(campaign_id, chat_id, joined_user_id, owner_id, invite_link, joined_at)
            VALUES($1, $2, $3, $4, $5, $6)
            ON CONFLICT DO NOTHING
        """, row["campaign_id"], GROUP_ID, user_id, row["owner_id"], inv, int(time.time()))

        if inserted.endswith("1"):
            context.job_queue.run_once(
                validate_pending_join,
                300,
                data={
                    "campaign_id": int(row["campaign_id"]),
                    "joined_user_id": int(user_id),
                },
            )


async def list_active_rewards(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    active_challenge = await DB.fetchrow("""
        SELECT rl.campaign_id, rl.invite_link, rl.joins_count, rc.required_joins
        FROM reward_links rl
        JOIN reward_campaigns rc ON rc.id = rl.campaign_id
        WHERE rl.owner_id=$1
          AND rl.delivered=FALSE
          AND rc.active=TRUE
        ORDER BY rl.created_at DESC
        LIMIT 1
    """, user_id)

    if active_challenge:
        invite = active_challenge["invite_link"]
        count = active_challenge["joins_count"]
        required = active_challenge["required_joins"]
        share_text = f"Rejoins ce groupe pour débloquer une récompense : {invite}"
        share_url = f"https://t.me/share/url?url={quote_plus(invite)}&text={quote_plus(share_text)}"

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📤 Envoyer mon lien en cours", url=share_url)],
            [InlineKeyboardButton("🔄 Rafraîchir progression", callback_data=f"refresh:{active_challenge['campaign_id']}")],
        ])

        await context.bot.send_message(
            user_id,
            "Tu as déjà un challenge en cours. Termine-le avant d’en ouvrir un autre.\n\n"
            "Attention : chaque récompense a son propre lien.\n"
            "Continue avec CE lien jusqu’à débloquer la récompense.\n\n"
            f"Ton lien en cours :\n{invite}\n\n"
            f"Progression : {count}/{required}\n"
            f"{progress_text(int(count), int(required))}",
            reply_markup=kb,
        )
        return

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
        lines.append(f"\n#{r['id']} — objectif {r['required_joins']} invitations")
        buttons.append([
            InlineKeyboardButton(
                f"🔐 Mot de passe — #{r['id']}",
                callback_data=f"share:{r['id']}",
            )
        ])

    await context.bot.send_message(
        user_id,
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def send_personal_share(user_id: int, context: ContextTypes.DEFAULT_TYPE, campaign_id: int):
    campaign = await DB.fetchrow("""
        SELECT id, gofile_link, password, required_joins, is_free, active
        FROM reward_campaigns
        WHERE id=$1 AND chat_id=$2
    """, campaign_id, GROUP_ID)

    if not campaign or not campaign["active"]:
        await context.bot.send_message(
            user_id,
            "Cette récompense n’est plus active.",
            reply_markup=user_home_panel(),
        )
        return

    if campaign["is_free"]:
        await context.bot.send_message(
            user_id,
            "🆓 Récompense gratuite accessible maintenant.\n\n"
            f"🔗 Lien : {campaign['gofile_link']}\n"
            f"🔐 Mot de passe : {campaign['password'] or 'Aucun'}",
        )
        return

    active_challenge = await DB.fetchrow("""
        SELECT rl.campaign_id, rl.invite_link, rl.joins_count, rc.required_joins
        FROM reward_links rl
        JOIN reward_campaigns rc ON rc.id = rl.campaign_id
        WHERE rl.owner_id=$1
          AND rl.delivered=FALSE
          AND rc.active=TRUE
        ORDER BY rl.created_at DESC
        LIMIT 1
    """, user_id)

    if active_challenge and int(active_challenge["campaign_id"]) != int(campaign_id):
        invite = active_challenge["invite_link"]
        count = active_challenge["joins_count"]
        required = active_challenge["required_joins"]

        share_text = f"Rejoins ce groupe pour débloquer une récompense : {invite}"
        share_url = f"https://t.me/share/url?url={quote_plus(invite)}&text={quote_plus(share_text)}"

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📤 Envoyer mon lien en cours", url=share_url)],
            [InlineKeyboardButton("🔄 Rafraîchir progression", callback_data=f"refresh:{active_challenge['campaign_id']}")],
        ])

        await context.bot.send_message(
            user_id,
            "Tu as déjà un challenge en cours. Termine-le avant d’en ouvrir un autre.\n\n"
            "Attention : chaque récompense a son propre lien.\n"
            "Continue avec CE lien jusqu’à débloquer la récompense.\n\n"
            f"Ton lien en cours :\n{invite}\n\n"
            f"Progression : {count}/{required}\n"
            f"{progress_text(int(count), int(required))}",
            reply_markup=kb,
        )
        return

    row = await DB.fetchrow("""
        SELECT invite_link, joins_count, delivered
        FROM reward_links
        WHERE campaign_id=$1 AND owner_id=$2
    """, campaign_id, user_id)

    if not row:
        link = await context.bot.create_chat_invite_link(
            GROUP_ID,
            name=f"c{campaign_id}_u{user_id}"[:32],
        )

        await DB.execute("""
            INSERT INTO reward_links(campaign_id, chat_id, owner_id, invite_link, created_at)
            VALUES($1, $2, $3, $4, $5)
        """, campaign_id, GROUP_ID, user_id, link.invite_link, int(time.time()))

        invite = link.invite_link
        count = 0
        delivered = False
    else:
        invite = row["invite_link"]
        count = row["joins_count"]
        delivered = row["delivered"]

    if delivered:
        await context.bot.send_message(
            user_id,
            "✅ Mot de passe déjà débloqué.\n\n"
            f"🔗 Lien : {campaign['gofile_link']}\n"
            f"🔐 Mot de passe : {campaign['password'] or 'Aucun'}",
        )
        return

    share_text = f"Rejoins ce groupe pour débloquer une récompense : {invite}"
    share_url = f"https://t.me/share/url?url={quote_plus(invite)}&text={quote_plus(share_text)}"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📤 Envoyer le lien", url=share_url)],
        [InlineKeyboardButton("🔄 Rafraîchir progression", callback_data=f"refresh:{campaign_id}")],
    ])

    required = int(campaign["required_joins"])
    await context.bot.send_message(
        user_id,
        "🔥 Mot de passe verrouillé\n\n"
        f"🔗 Lien Gofile :\n{campaign['gofile_link']}\n\n"
        "Chaque récompense a son propre défi et son propre lien d’invitation.\n"
        "Utilise uniquement le lien affiché ici pour cette récompense.\n\n"
        f"Invite {required} personnes avec TON lien pour débloquer le mot de passe.\n\n"
        f"Ton lien d’invitation :\n{invite}\n\n"
        f"Progression : {count}/{required}\n"
        f"{progress_text(int(count), required)}",
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
        lines.append(f"Bot admin : {'✅' if member.status in ('administrator', 'creator') else '❌'}")
        lines.append(f"Supprimer messages : {'✅' if getattr(member, 'can_delete_messages', False) else '❌'}")
        lines.append(f"Mute/restrictions : {'✅' if getattr(member, 'can_restrict_members', False) else '❌'}")
        lines.append(f"Créer liens invitation : {'✅' if getattr(member, 'can_invite_users', False) else '❌'}")
    except Exception as e:
        lines.append(f"Groupe : ❌ erreur avec GROUP_ID / droits bot : {e}")

    users = await DB.fetchval("SELECT COUNT(*) FROM bot_users WHERE started_private=TRUE")
    campaigns = await DB.fetchval(
        "SELECT COUNT(*) FROM reward_campaigns WHERE chat_id=$1 AND active=TRUE",
        GROUP_ID,
    )

    lines.append(f"Utilisateurs PV enregistrés : {users}")
    lines.append(f"Récompenses actives : {campaigns}")
    lines.append(f"Bot username : @{BOT_USERNAME}" if BOT_USERNAME else "Bot username : inconnu")

    return "\n".join(lines)



async def admin_rewards_list_text() -> tuple[str, InlineKeyboardMarkup]:
    rows = await DB.fetch("""
        SELECT id, active, required_joins, gofile_link
        FROM reward_campaigns
        WHERE chat_id=$1
        ORDER BY id DESC
        LIMIT 15
    """, GROUP_ID)

    if not rows:
        return "Aucune récompense créée.", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Retour", callback_data="admin_back")]])

    lines = ["🎛 Récompenses"]
    buttons = []

    for r in rows:
        status = "✅ active" if r["active"] else "🛑 désactivée"
        gofile_status = "🔗 lien OK" if r["gofile_link"] else "⚠️ lien vide"
        lines.append(f"#{r['id']} — {status} — objectif {r['required_joins']} — {gofile_status}")
        buttons.append([InlineKeyboardButton(f"🎁 Gérer récompense #{r['id']}", callback_data=f"reward_detail:{r['id']}")])

    buttons.append([InlineKeyboardButton("⬅️ Retour panel", callback_data="admin_back")])
    return "\n".join(lines), InlineKeyboardMarkup(buttons)


async def reward_detail_text(campaign_id: int) -> tuple[str, InlineKeyboardMarkup]:
    r = await DB.fetchrow("""
        SELECT id, active, required_joins, gofile_link, password, is_free, image_url, promo_text
        FROM reward_campaigns
        WHERE id=$1 AND chat_id=$2
    """, campaign_id, GROUP_ID)

    if not r:
        return "Récompense introuvable.", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Retour", callback_data="manage_rewards")]])

    unlocked = await DB.fetchval("SELECT COUNT(*) FROM reward_links WHERE campaign_id=$1 AND delivered=TRUE", campaign_id)
    progress = await DB.fetchval("SELECT COUNT(*) FROM reward_links WHERE campaign_id=$1 AND delivered=FALSE", campaign_id)
    pending = await DB.fetchval("SELECT COUNT(*) FROM pending_reward_joins WHERE campaign_id=$1", campaign_id)

    status = "✅ active" if r["active"] else "🛑 désactivée"

    txt = (
        f"🎁 Récompense #{campaign_id}\n\n"
        f"Statut : {status}\n"
        f"Objectif : {r['required_joins']}\n"
        f"Type : {'🆓 gratuit' if r['is_free'] else '🔒 mot de passe verrouillé'}\n"
        f"Débloqués : {unlocked}\n"
        f"En cours : {progress}\n"
        f"Joins en attente validation : {pending}\n\n"
        f"Lien Gofile actuel :\n{r['gofile_link'] or '⚠️ Aucun lien'}\n\n"
        f"Mot de passe actuel :\n{r['password'] or '⚠️ Aucun'}\n\n"
        "Actions disponibles :"
    )

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Modifier lien Gofile", callback_data=f"reward_edit_gofile:{campaign_id}")],
        [InlineKeyboardButton("🔐 Modifier mot de passe", callback_data=f"reward_edit_password:{campaign_id}")],
        [InlineKeyboardButton("🎯 Modifier objectif", callback_data=f"reward_edit_required:{campaign_id}")],
        [InlineKeyboardButton("🆓 Gratuit ON/OFF", callback_data=f"reward_toggle_free:{campaign_id}")],
        [InlineKeyboardButton("🔁 Republier cette récompense", callback_data=f"reward_republish:{campaign_id}")],
        [InlineKeyboardButton("📊 Voir stats/users", callback_data=f"reward_stats:{campaign_id}")],
        [InlineKeyboardButton("🛑 Désactiver/Supprimer", callback_data=f"reward_delete:{campaign_id}")],
        [InlineKeyboardButton("⬅️ Retour liste", callback_data="manage_rewards")],
    ])

    return txt, kb


async def reward_stats_text(campaign_id: int) -> str:
    rows_done = await DB.fetch("""
        SELECT rl.owner_id, rl.joins_count, bu.username, bu.first_name
        FROM reward_links rl
        LEFT JOIN bot_users bu ON bu.user_id=rl.owner_id
        WHERE rl.campaign_id=$1 AND rl.delivered=TRUE
        ORDER BY rl.joins_count DESC, rl.owner_id
        LIMIT 50
    """, campaign_id)

    rows_progress = await DB.fetch("""
        SELECT rl.owner_id, rl.joins_count, bu.username, bu.first_name
        FROM reward_links rl
        LEFT JOIN bot_users bu ON bu.user_id=rl.owner_id
        WHERE rl.campaign_id=$1 AND rl.delivered=FALSE
        ORDER BY rl.joins_count DESC, rl.owner_id
        LIMIT 50
    """, campaign_id)

    def user_label(r):
        if r["username"]:
            return f"@{r['username']} ({r['owner_id']})"
        if r["first_name"]:
            return f"{r['first_name']} ({r['owner_id']})"
        return str(r["owner_id"])

    lines = [f"📊 Stats récompense #{campaign_id}"]

    lines.append("\n✅ Ont débloqué :")
    if rows_done:
        for r in rows_done:
            lines.append(f"- {user_label(r)} — {r['joins_count']} joins")
    else:
        lines.append("- Aucun")

    lines.append("\n⏳ En cours :")
    if rows_progress:
        for r in rows_progress:
            lines.append(f"- {user_label(r)} — {r['joins_count']} joins")
    else:
        lines.append("- Aucun")

    return "\n".join(lines)


async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await save_user(q.from_user, started_private=(q.message and q.message.chat.type == "private"))

    if q.data == "rewards_list":
        try:
            await list_active_rewards(q.from_user.id, context)
            await q.answer("Liste envoyée ici.", show_alert=False)
        except Exception as e:
            log.warning("rewards_list failed: %s", e)
            await q.answer("Ouvre d’abord le bot en privé avec /start.", show_alert=True)
        return

    if q.data.startswith("share:"):
        campaign_id = int(q.data.split(":", 1)[1])

        try:
            await send_personal_share(q.from_user.id, context, campaign_id)
            await q.answer("Lien personnalisé envoyé ici.", show_alert=True)
        except Exception as e:
            log.warning("share callback failed: %s", e)
            start_link = f"https://t.me/{BOT_USERNAME}?start=reward_{campaign_id}" if BOT_USERNAME else "le bot en privé"
            await q.answer(f"Ouvre d’abord le bot en privé : {start_link}", show_alert=True)

        return

    if q.data.startswith("refresh:"):
        campaign_id = int(q.data.split(":", 1)[1])
        try:
            await send_personal_share(q.from_user.id, context, campaign_id)
            await q.answer("Progression rafraîchie.", show_alert=False)
        except Exception as e:
            log.warning("refresh failed: %s", e)
            await q.answer("Impossible de rafraîchir maintenant.", show_alert=True)
        return

    if not await ensure_admin(update, context):
        return

    if q.data == "admin_back":
        await q.edit_message_text(await get_admin_panel_text(), reply_markup=admin_panel())
        return

    if q.data == "manage_rewards":
        txt, kb = await admin_rewards_list_text()
        await q.message.reply_text(txt, reply_markup=kb)
        return

    if q.data.startswith("reward_detail:"):
        campaign_id = int(q.data.split(":", 1)[1])
        txt, kb = await reward_detail_text(campaign_id)
        await q.message.reply_text(txt, reply_markup=kb)
        return

    if q.data.startswith("reward_edit_gofile:"):
        campaign_id = int(q.data.split(":", 1)[1])
        USER_STATE[q.from_user.id] = f"edit_reward_gofile:{campaign_id}"
        await q.message.reply_text(f"Envoie le nouveau lien Gofile pour la récompense #{campaign_id}.")
        return

    if q.data.startswith("reward_edit_password:"):
        campaign_id = int(q.data.split(":", 1)[1])
        USER_STATE[q.from_user.id] = f"edit_reward_password:{campaign_id}"
        await q.message.reply_text(f"Envoie le nouveau mot de passe pour la récompense #{campaign_id}.")
        return

    if q.data.startswith("reward_edit_required:"):
        campaign_id = int(q.data.split(":", 1)[1])
        USER_STATE[q.from_user.id] = f"edit_reward_required:{campaign_id}"
        await q.message.reply_text(f"Envoie le nouvel objectif d’invitations pour la récompense #{campaign_id}. Mets 0 pour gratuit.")
        return

    if q.data.startswith("reward_toggle_free:"):
        campaign_id = int(q.data.split(":", 1)[1])
        new_val = await DB.fetchval("""
            UPDATE reward_campaigns
            SET is_free = NOT is_free
            WHERE id=$1 AND chat_id=$2
            RETURNING is_free
        """, campaign_id, GROUP_ID)
        await q.message.reply_text(f"Récompense #{campaign_id} : gratuit = {'ON' if new_val else 'OFF'}.")
        return

    if q.data.startswith("reward_republish:"):
        campaign_id = int(q.data.split(":", 1)[1])
        r = await DB.fetchrow("""
            SELECT id, active, image_url, promo_text
            FROM reward_campaigns
            WHERE id=$1 AND chat_id=$2
        """, campaign_id, GROUP_ID)

        if not r:
            await q.message.reply_text("Récompense introuvable.")
            return

        if not r["active"]:
            await DB.execute("UPDATE reward_campaigns SET active=TRUE WHERE id=$1 AND chat_id=$2", campaign_id, GROUP_ID)

        image_url = r["image_url"] or REWARD_IMAGE_URL
        await context.bot.send_photo(
            GROUP_ID,
            photo=image_url,
            caption=r["promo_text"],
            reply_markup=share_panel(campaign_id),
        )
        await q.message.reply_text(f"Récompense #{campaign_id} republiée dans le groupe.")
        return

    if q.data.startswith("reward_delete:"):
        campaign_id = int(q.data.split(":", 1)[1])
        await DB.execute("UPDATE reward_campaigns SET active=FALSE WHERE id=$1 AND chat_id=$2", campaign_id, GROUP_ID)
        await q.message.reply_text(
            f"Récompense #{campaign_id} désactivée.\n"
            "Les stats restent conservées. Tu peux la republier plus tard depuis le menu."
        )
        return

    if q.data.startswith("reward_stats:"):
        campaign_id = int(q.data.split(":", 1)[1])
        await q.message.reply_text(await reward_stats_text(campaign_id))
        return

    if q.data in ("toggle_on", "toggle_off"):
        val = q.data == "toggle_on"
        await DB.execute("UPDATE settings SET messages_open=$1 WHERE chat_id=$2", val, GROUP_ID)
        await q.edit_message_text(await get_admin_panel_text(), reply_markup=admin_panel())

    elif q.data in (
        "word_add",
        "word_del",
        "broadcast_group",
        "broadcast_private",
                "set_reward_text",
        "set_reward_image",
    ):
        USER_STATE[q.from_user.id] = q.data

        prompts = {
            "word_add": "Envoie le mot interdit à ajouter.",
            "word_del": "Envoie le mot interdit à supprimer.",
            "broadcast_group": "Envoie le message à broadcaster dans le groupe.",
            "broadcast_private": "Envoie le message à broadcaster en privé aux utilisateurs qui ont déjà démarré le bot.",
            "set_reward_text": "Envoie le texte de la prochaine publication. Tu peux changer ce texte à chaque fichier/récompense.",
            "set_reward_image": "Envoie le lien direct de l’image pour les prochaines publications.",
        }

        await q.message.reply_text(prompts[q.data])

    elif q.data == "word_list":
        rows = await DB.fetch("SELECT word FROM forbidden_words WHERE chat_id=$1 ORDER BY word", GROUP_ID)
        txt = "Mots interdits :\n" + ("\n".join(f"- {r['word']}" for r in rows) or "Aucun")
        await q.message.reply_text(txt)

    elif q.data == "bot_info":
        await q.message.reply_text(await bot_info_text(context))

    elif q.data == "publish":
        CREATE_FLOW[q.from_user.id] = {}
        USER_STATE[q.from_user.id] = "create_reward_gofile"
        await q.message.reply_text(
            "Création d’une nouvelle récompense.\n\n"
            "1/3 — Envoie le lien Gofile visible publiquement."
        )
        return


async def notify_previous_winners_new_challenge(context: ContextTypes.DEFAULT_TYPE, campaign_id: int) -> tuple[int, int]:
    """
    Préviens uniquement les anciens gagnants quand une nouvelle récompense est publiée.
    """
    rows = await DB.fetch("""
        SELECT DISTINCT rl.owner_id
        FROM reward_links rl
        JOIN bot_users bu ON bu.user_id = rl.owner_id
        WHERE rl.delivered=TRUE
          AND bu.started_private=TRUE
    """)

    ok = 0
    fail = 0

    if BOT_USERNAME:
        url = f"https://t.me/{BOT_USERNAME}?start=reward_{campaign_id}"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔐 Recevoir le mot de passe", url=url)]])
    else:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔐 Recevoir le mot de passe", callback_data=f"share:{campaign_id}")]])

    for r in rows:
        try:
            await context.bot.send_message(
                r["owner_id"],
                "🔥 Nouveau challenge dispo !\n\n"
                "Tu as déjà débloqué une récompense avant.\n"
                "Un nouveau contenu est disponible maintenant.\n\n"
                "Clique pour débloquer le mot de passe du nouveau contenu 👇",
                reply_markup=kb,
            )
            ok += 1
        except Exception:
            fail += 1

    return ok, fail


async def private_admin_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await save_user(update.effective_user, started_private=True)

    user_id = update.effective_user.id
    state = USER_STATE.get(user_id)

    if not state or not await is_admin(update, context, user_id, GROUP_ID):
        return

    txt = update.message.text.strip()

    if state == "create_reward_gofile":
        CREATE_FLOW[user_id] = {"gofile_link": txt}
        USER_STATE[user_id] = "create_reward_password"
        await update.message.reply_text("2/3 — Envoie le mot de passe à débloquer. Mets '-' s’il n’y en a pas.")
        return

    if state == "create_reward_password":
        CREATE_FLOW.setdefault(user_id, {})["password"] = "" if txt == "-" else txt
        USER_STATE[user_id] = "create_reward_required"
        await update.message.reply_text("3/3 — Envoie l’objectif d’invitations. Mets 0 pour une récompense gratuite.")
        return

    if state == "create_reward_required":
        try:
            required = max(0, int(txt))
        except ValueError:
            await update.message.reply_text("Envoie un nombre valide. Exemple : 6 ou 0 pour gratuit.")
            return

        flow = CREATE_FLOW.get(user_id, {})
        if not flow.get("gofile_link"):
            USER_STATE.pop(user_id, None)
            CREATE_FLOW.pop(user_id, None)
            await update.message.reply_text("Création annulée : lien Gofile manquant.")
            return

        row = await DB.fetchrow("SELECT reward_image_url, reward_text FROM settings WHERE chat_id=$1", GROUP_ID)
        image_url = (row["reward_image_url"] if row else "") or REWARD_IMAGE_URL
        promo_text = ((row["reward_text"] if row else "") or DEFAULT_REWARD_TEXT).replace("{required}", str(required or REWARD_REQUIRED_JOINS))

        is_free = required == 0
        campaign_id = await DB.fetchval("""
            INSERT INTO reward_campaigns(chat_id, gofile_link, password, is_free, image_url, promo_text, required_joins, created_at)
            VALUES($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id
        """, GROUP_ID, flow["gofile_link"], flow.get("password", ""), is_free, image_url, promo_text, required or REWARD_REQUIRED_JOINS, int(time.time()))

        await context.bot.send_photo(
            GROUP_ID,
            photo=image_url,
            caption=promo_text,
            reply_markup=share_panel(campaign_id),
        )

        ok, fail = await notify_previous_winners_new_challenge(context, campaign_id)

        USER_STATE.pop(user_id, None)
        CREATE_FLOW.pop(user_id, None)

        await update.message.reply_text(
            f"Récompense #{campaign_id} créée et publiée.\n"
            f"Type : {'gratuite' if is_free else 'verrouillée'}\n"
            f"Anciens gagnants prévenus en PV : {ok}. Échecs : {fail}."
        )
        return

    if state.startswith("edit_reward_gofile:"):
        campaign_id = int(state.split(":", 1)[1])
        await DB.execute(
            "UPDATE reward_campaigns SET gofile_link=$1 WHERE id=$2 AND chat_id=$3",
            txt,
            campaign_id,
            GROUP_ID,
        )
        await update.message.reply_text(f"Lien Gofile modifié pour la récompense #{campaign_id}.")
        USER_STATE.pop(user_id, None)
        return

    if state.startswith("edit_reward_password:"):
        campaign_id = int(state.split(":", 1)[1])
        await DB.execute(
            "UPDATE reward_campaigns SET password=$1 WHERE id=$2 AND chat_id=$3",
            "" if txt == "-" else txt,
            campaign_id,
            GROUP_ID,
        )
        await update.message.reply_text(f"Mot de passe modifié pour la récompense #{campaign_id}.")
        USER_STATE.pop(user_id, None)
        return

    if state.startswith("edit_reward_required:"):
        campaign_id = int(state.split(":", 1)[1])
        try:
            required = max(0, int(txt))
        except ValueError:
            await update.message.reply_text("Envoie un nombre valide.")
            return
        await DB.execute(
            "UPDATE reward_campaigns SET required_joins=$1, is_free=$2 WHERE id=$3 AND chat_id=$4",
            required or REWARD_REQUIRED_JOINS,
            required == 0,
            campaign_id,
            GROUP_ID,
        )
        await update.message.reply_text(f"Objectif modifié pour la récompense #{campaign_id}. Gratuit = {'ON' if required == 0 else 'OFF'}.")
        USER_STATE.pop(user_id, None)
        return

    if state == "word_add":
        await DB.execute("""
            INSERT INTO forbidden_words(chat_id, word, normalized)
            VALUES($1, $2, $3)
            ON CONFLICT(chat_id, normalized)
            DO UPDATE SET word=$2
        """, GROUP_ID, txt, norm(txt))

        await update.message.reply_text("Mot ajouté.")

    elif state == "word_del":
        await DB.execute(
            "DELETE FROM forbidden_words WHERE chat_id=$1 AND normalized=$2",
            GROUP_ID,
            norm(txt),
        )
        await update.message.reply_text("Mot supprimé si présent.")

    elif state == "broadcast_group":
        await context.bot.send_message(
            GROUP_ID,
            txt,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        await update.message.reply_text("Broadcast groupe envoyé.")

    elif state == "broadcast_private":
        rows = await DB.fetch("SELECT user_id FROM bot_users WHERE started_private=TRUE")

        ok = 0
        fail = 0

        for r in rows:
            try:
                await context.bot.send_message(
                    r["user_id"],
                    txt,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                ok += 1
            except Exception:
                fail += 1

        await update.message.reply_text(f"Broadcast PV terminé. Envoyés : {ok}. Échecs : {fail}.")


    elif state == "set_reward_text":
        await DB.execute("UPDATE settings SET reward_text=$1 WHERE chat_id=$2", txt, GROUP_ID)
        await update.message.reply_text("Texte de publication enregistré pour la prochaine récompense.")

    elif state == "set_reward_image":
        await DB.execute("UPDATE settings SET reward_image_url=$1 WHERE chat_id=$2", txt, GROUP_ID)
        await update.message.reply_text("Image enregistrée pour les prochaines publications.")

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
        context.job_queue.run_once(
            delete_later,
            120,
            data={"chat_id": GROUP_ID, "message_id": m.message_id},
        )
    except Exception as e:
        log.info("deterrence failed: %s", e)


async def post_init(app: Application):
    global BOT_USERNAME

    await init_db()

    me = await app.bot.get_me()
    BOT_USERNAME = me.username or ""

    app.job_queue.run_repeating(
        deterrence,
        interval=7200,
        first=random.randint(120, 900),
    )

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
