# otp_bot_final_v7_admin_users_ban_v5.py
# Versión: El botón de broadcast ahora permite elegir si enviar a Premium o No Premium.

import time
import re
import threading
import imaplib
import email
from email.header import decode_header
import html
import random
import urllib.parse
import urllib.request
from typing import Dict, Any, Optional
import datetime
import json
import os
import asyncio # Necesario para la pausa en el broadcast

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message
)
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
    CallbackQueryHandler,
)

# ===========================
# CONFIGURACIÓN - EDITA ESTO
# ===========================
BOT_TOKEN = "8202877262:AAHglQByO4rVGxb0jFKbN7CVtATlyiII-GE"
MY_CHAT_ID = "7590578210"  # Tu ID como administrador
ADMIN_USERNAME = "@PAUBLITE_GT" # Tu username para mensajes a usuarios
DATA_FILE = "premium_keys.json" # Archivo para guardar las claves (PERSISTENCIA)

# Aquí defines las cuentas Gmail que quieres vigilar.
GMAIL_ACCOUNTS = [
    {"email": "paudronixpro@gmail.com", "app_password": "moyxztlqmgzjkwnq"},
    {"email": "amentos562@gmail.com", "app_password": "jlygznadzvtrnjqj"},
    {"email": "dluxevulucion@gmail.com", "app_password": "btyliqzmpmrqmyjo"},
]

TARGET_SENDERS = [] # Si deseas filtrar, pon fragmentos aquí.

GIFS = [
    "https://images.squarespace-cdn.com/content/v1/545a70e0e4b0f1f91509cf05/08a31d15-bb80-4a97-acb9-7e4841badeeb/emmatest.GIF",
    "https://i.pinimg.com/originals/0c/95/d6/0c95d675e0216ea464942f2e6f971bfe.gif",
    "https://i.pinimg.com/originals/3a/a1/7a/3aa17aba045b0bd889b372c4df3bdd95.gif",
    "https://i.pinimg.com/originals/7d/f4/9d/7df49d0ac94d08407ab69aec7fa7234d.gif",
    "https://i.pinimg.com/originals/97/36/63/97366317a733cc7507f194461c8c6c77.gif",
]

# ===========================
# COMPORTAMIENTO
# ===========================
ACCEPT_ANY_SENDER = True
IMAP_CHECK_INTERVAL_SECONDS = 10 # ⚡ Velocidad de revisión

# ===========================
# DEBUG / MODO PRUEBAS
# ===========================
DEBUG_SEND_ALL_UNSEEN = False
DEBUG_SEND_TO_ADMIN_ONLY = True

# ===========================
# ESTADO Y SUSCRIPCIONES (GLOBAL)
# ===========================
SUBSCRIPTIONS = set()
SUBSCRIPTIONS_LOCK = threading.Lock()
IS_SUBSCRIBED_GLOBAL = True

# { "paublte-genX-CÓDIGO": {"chat_id": 12345, "expires_at": datetime_obj, "level": "Bronce 1"} }
PREMIUM_KEYS: Dict[str, Dict[str, Any]] = {}
PREMIUM_KEYS_LOCK = threading.Lock()

# { chat_id: "paublte-genX-CODIGO" }
USER_ACTIVE_KEYS: Dict[int, str] = {}
USER_ACTIVE_KEYS_LOCK = threading.Lock()

# { chat_id: {"name": "User Name", "username": "@username"} }
USER_CONTACTS: Dict[int, Dict[str, str]] = {}
USER_CONTACTS_LOCK = threading.Lock()

# { chat_id } conjunto de IDs baneados
BANNED_USERS: set[int] = set()
BANNED_USERS_LOCK = threading.Lock()

# Estado temporal para la generación de claves multinivel y broadcast
# Valores posibles para el estado de broadcast:
# "AWAITING_BROADCAST_TARGET" (Esperando a quién enviar)
# "AWAITING_BROADCAST_CONTENT_PREMIUM" (Esperando contenido para Premium)
# "AWAITING_BROADCAST_CONTENT_NON_PREMIUM" (Esperando contenido para No Premium)
ADMIN_STATE: Dict[int, str] = {}
ADMIN_BROADCAST_TARGET: Dict[int, str] = {} # chat_id -> "PREMIUM" o "NON_PREMIUM"

# ===========================
# UTILIDADES DE PERSISTENCIA
# ===========================

def load_keys():
    """Carga las claves premium, contactos y baneados desde el archivo JSON si existe."""
    global PREMIUM_KEYS, USER_ACTIVE_KEYS, SUBSCRIPTIONS, USER_CONTACTS, BANNED_USERS
    if not os.path.exists(DATA_FILE):
        print("💾 Archivo de datos no encontrado. Iniciando sin claves guardadas.")
        return

    try:
        with open(DATA_FILE, 'r') as f:
            data = json.load(f)
    except Exception as e:
        print(f"❌ ERROR al leer el archivo de datos: {e}")
        return

    try:
        keys_data = data.get("keys", {})
        user_active = data.get("user_active_keys", {})
        subs = data.get("subscriptions", [])
        contacts = data.get("user_contacts", {})
        banned = data.get("banned_users", [])

        with PREMIUM_KEYS_LOCK:
            PREMIUM_KEYS = {}
            for key, details in keys_data.items():
                if isinstance(details.get("expires_at"), str):
                    try:
                        details["expires_at"] = datetime.datetime.fromisoformat(details["expires_at"])
                    except Exception:
                        details["expires_at"] = datetime.datetime.now()
                PREMIUM_KEYS[key] = details

        with USER_ACTIVE_KEYS_LOCK:
            USER_ACTIVE_KEYS = {int(k): v for k, v in user_active.items()}

        with SUBSCRIPTIONS_LOCK:
            SUBSCRIPTIONS = set(subs)

        with USER_CONTACTS_LOCK:
            USER_CONTACTS = {int(k): v for k, v in contacts.items()}

        with BANNED_USERS_LOCK:
             BANNED_USERS = set(banned)

        print(f"✅ Claves, estados y baneados cargados de {DATA_FILE}. Total de claves: {len(PREMIUM_KEYS)}")
    except Exception as e:
        print(f"❌ ERROR al cargar las claves desde JSON: {e}")

def save_keys():
    """Guarda las claves premium, contactos y baneados en el archivo JSON."""
    global PREMIUM_KEYS, USER_ACTIVE_KEYS, SUBSCRIPTIONS, USER_CONTACTS, BANNED_USERS
    try:
        data_to_save = {
            "keys": {},
            "user_active_keys": {},
            "subscriptions": list(SUBSCRIPTIONS),
            "user_contacts": {},
            "banned_users": list(BANNED_USERS)
        }

        with PREMIUM_KEYS_LOCK:
            for key, details in PREMIUM_KEYS.items():
                details_copy = details.copy()
                if isinstance(details_copy.get("expires_at"), datetime.datetime):
                    details_copy["expires_at"] = details_copy["expires_at"].isoformat()
                data_to_save["keys"][key] = details_copy

        with USER_ACTIVE_KEYS_LOCK:
            data_to_save["user_active_keys"] = {str(k): v for k, v in USER_ACTIVE_KEYS.items()}

        with USER_CONTACTS_LOCK:
            data_to_save["user_contacts"] = {str(k): v for k, v in USER_CONTACTS.items()}


        with open(DATA_FILE, 'w') as f:
            json.dump(data_to_save, f, indent=4)
    except Exception as e:
        print(f"❌ ERROR al guardar las claves en JSON: {e}")

# ===========================
# UTILIDADES GENERALES
# ===========================

def decode_mime_words(s):
    if not s: return ""
    parts = decode_header(s)
    decoded = ""
    for part, encoding in parts:
        if isinstance(part, bytes):
            try: decoded += part.decode(encoding or "utf-8", errors="ignore")
            except: decoded += part.decode("utf-8", errors="ignore")
        else:
            decoded += part
    return decoded

def strip_html_tags(text: str) -> str:
    if not text: return ""
    text = re.sub(r'(?is)<(script|style).*?>.*?(</\1>)', ' ', text)
    text = re.sub(r'(?s)<.*?>', ' ', text)
    text = html.unescape(text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def extract_sender_email(sender_raw: str) -> str:
    match = re.search(r'<(.*?)>', sender_raw)
    if match: return match.group(1).lower()
    if '@' in sender_raw: return sender_raw.lower()
    return ""

def get_time_remaining(expires_at: datetime.datetime) -> str:
    now = datetime.datetime.now()
    time_left = expires_at - now
    if time_left.total_seconds() <= 0: return "EXPIRADA"

    days = time_left.days
    hours = time_left.seconds // 3600
    minutes = (time_left.seconds % 3600) // 60
    seconds = time_left.seconds % 60

    parts = []
    if days > 0: parts.append(f"{days}d")
    if hours > 0: parts.append(f"{hours}h")
    if minutes > 0: parts.append(f"{minutes}m")
    if seconds > 0 or not parts: parts.append(f"{seconds}s")

    return " ".join(parts)

def update_user_contacts(user: Optional[Any], chat_id: int):
    """Actualiza la información de nombre y username de un usuario."""
    if not user: return

    user_name = user.first_name if user and user.first_name else "Usuario Desconocido"
    # Aseguramos que tenga el @ si tiene username
    user_username = f"@{user.username}" if user and user.username else "sin_username"

    with USER_CONTACTS_LOCK:
        USER_CONTACTS[chat_id] = {
            "name": user_name,
            "username": user_username
        }

# ===========================
# LÓGICA DE OTP (Sin cambios)
# ===========================
def is_login_otp(subject: str, body: str) -> bool:
    subject_low = (subject or "").lower()
    body_low = (body or "").lower()
    DENY_KEYWORDS = ["restablecer", "recuperar contraseña", "cambio de contraseña", "reset password"]
    ALLOW_KEYWORDS = ["iniciar sesión", "codigo para iniciar", "código de verificación", "login code", "otp", "spotify", "netflix"]

    for kw in DENY_KEYWORDS:
        if kw in subject_low or kw in body_low: return False
    for kw in ALLOW_KEYWORDS:
        if kw in subject_low or kw in body_low: return True

    return bool(re.search(r"(\d{4,8})", body or ""))

def extract_otp_code(text: str, subject: str = ""):
    """
    Intenta extraer el código OTP del texto. Prioriza códigos numéricos de 4-8 dígitos.
    """
    if not text: return None
    full_text = text.replace("-", "").replace(" ", "").lower() + subject.replace("-", "").replace(" ", "").lower()

    # 1. Patrones numéricos fuertes (ej. 6 dígitos juntos)
    match_digits = re.search(r'(\d{4,8})', full_text)
    if match_digits: return match_digits.group(1)

    # 2. Patrones alfanuméricos junto a palabras clave (ej. 4-8 caracteres)
    pattern_keyword = re.compile(
        r"""
        (?:c[óo]digo(?:s)? | code(?:s)? | otp | pass(?:word)? | key | token)
        \s*[:=\-]?\s* ([A-Z0-9]{4,8})
        \b
        """,
        re.IGNORECASE | re.VERBOSE
    )
    match = pattern_keyword.search(full_text)
    if match: return match.group(1)

    # 3. Patrón general alfanumérico (ej. 6 caracteres)
    match_general = re.search(r'([A-Z0-9]{6})', full_text, re.IGNORECASE)
    if match_general: return match.group(1)

    return None

def get_email_body(msg):
    """Extrae el cuerpo del mensaje y devuelve texto plano (sin HTML)."""
    raw = ""
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            cdisp = str(part.get("Content-Disposition") or "")
            if ctype == 'text/plain' and 'attachment' not in cdisp:
                try: raw = part.get_payload(decode=True).decode(part.get_content_charset() or 'utf-8', errors='ignore')
                except: raw = part.get_payload(decode=True).decode(errors='ignore')
                return strip_html_tags(raw)
        for part in msg.walk():
            if part.get_content_type() == 'text/html':
                try: raw = part.get_payload(decode=True).decode(part.get_content_charset() or 'utf-8', errors='ignore')
                except: raw = part.get_payload(decode=True).decode(errors='ignore')
                return strip_html_tags(raw)
    else:
        try: raw = msg.get_payload(decode=True).decode(msg.get_content_charset() or 'utf-8', errors='ignore')
        except: raw = msg.get_payload(decode=True).decode(errors='ignore')
        return strip_html_tags(raw)
    return ""

def send_telegram_message(text: str):
    """Envía un mensaje a todos los chat_ids en SUBSCRIPTIONS o a MY_CHAT_ID como fallback."""

    # Filtrar suscripciones por IDs baneados
    with SUBSCRIPTIONS_LOCK: 
        targets = list(SUBSCRIPTIONS)
    with BANNED_USERS_LOCK:
        targets = [cid for cid in targets if cid not in BANNED_USERS]

    if not targets: 
        targets = [int(MY_CHAT_ID)]

    try:        
        for chat_id in targets:
            try:
                url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
                data = urllib.parse.urlencode({"chat_id": int(chat_id), "text": text, "parse_mode": "HTML"}).encode("utf-8")
                req = urllib.request.Request(url, data=data)
                with urllib.request.urlopen(req, timeout=10) as response: _ = response.read().decode("utf-8")
            except Exception as inner_e:
                print(f"❌ Error al enviar mensaje a chat {chat_id}: {inner_e}")
    except Exception as e:
        print(f"❌ Error general al enviar mensajes a Telegram: {e}")

def check_for_otp_emails_for_account(account):
    email_addr = account.get("email")
    app_pass = account.get("app_password")

    while True:
        mail = None
        try:
            clean_expired_keys()
            save_keys()

            if not IS_SUBSCRIBED_GLOBAL:
                time.sleep(IMAP_CHECK_INTERVAL_SECONDS)
                continue

            try:
                mail = imaplib.IMAP4_SSL("imap.gmail.com")
                mail.login(email_addr, app_pass)
            except Exception as e_login:
                print(f"❌ ERROR login IMAP ({email_addr}): {e_login}")
                time.sleep(60)
                continue

            mail.select("INBOX")
            # Usar "UNSEEN" para solo obtener correos no leídos
            status, email_ids = mail.search(None, "UNSEEN")
            email_id_list = email_ids[0].split()

            if email_id_list:
                uids_to_process = email_id_list[-50:]
                for uid in uids_to_process:
                    try:
                        status, msg_data = mail.fetch(uid, "(RFC822)")
                        if status != 'OK' or not msg_data or not msg_data[0]: continue
                        raw_email = msg_data[0][1]
                        msg = email.message_from_bytes(raw_email)

                        sender_raw = msg.get("from") or ""
                        subject_raw = msg.get("subject") or "(Sin asunto)"
                        subject = decode_mime_words(subject_raw)
                        sender_full = decode_mime_words(sender_raw)

                        body = get_email_body(msg) or "" 
                        otp_code = extract_otp_code(body, subject)

                        if otp_code:
                            if not is_login_otp(subject, body):
                                mail.store(uid, "+FLAGS", "\\Seen")
                                continue

                            telegram_message = (
                                f"📣 <b>NUEVO CÓDIGO OTP</b> 📣\n\n"
                                f"De: <b>{html.escape(sender_full)}</b>\n"
                                f"Cuenta: <code>{html.escape(email_addr)}</code>\n"
                                f"Asunto: {html.escape(subject)}\n\n"
                                f"🔑 <b>CÓDIGO: {otp_code}</b>"
                            )
                            send_telegram_message(telegram_message)

                        mail.store(uid, "+FLAGS", "\\Seen")
                    except Exception as e_proc:
                        print(f"❌ Error procesando uid {uid} en {email_addr}: {e_proc}")
        except Exception as e:
            print(f"❌ Error general en hilo {email_addr}: {e}")
        finally:
            if mail:
                try: mail.logout()
                except: pass
            time.sleep(IMAP_CHECK_INTERVAL_SECONDS)

# ===========================
# UTILIDADES PREMIUM KEY
# ===========================
def clean_expired_keys():
    """Limpia claves expiradas y desuscribe a los usuarios afectados y guarda."""
    global PREMIUM_KEYS, USER_ACTIVE_KEYS, SUBSCRIPTIONS
    now = datetime.datetime.now()

    with PREMIUM_KEYS_LOCK:
        keys_to_remove = []
        for key, details in list(PREMIUM_KEYS.items()):
            if details["expires_at"] <= now:
                keys_to_remove.append(key)
                if details.get("chat_id"):
                    chat_id = details["chat_id"]

                    with USER_ACTIVE_KEYS_LOCK:
                        if USER_ACTIVE_KEYS.get(chat_id) == key:
                            del USER_ACTIVE_KEYS[chat_id]

                    with SUBSCRIPTIONS_LOCK:
                        if chat_id in SUBSCRIPTIONS:
                            SUBSCRIPTIONS.remove(chat_id)

                    try:
                        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
                        data = urllib.parse.urlencode({
                            "chat_id": chat_id,
                            "text": f"🚨 Tu clave premium ha expirado: <code>{key}</code>. Contacta a {ADMIN_USERNAME}",
                            "parse_mode": "HTML",
                        }).encode("utf-8")
                        req = urllib.request.Request(url, data=data)
                        with urllib.request.urlopen(req, timeout=5) as response: _ = response.read().decode("utf-8")
                    except Exception:
                        pass

        for key in keys_to_remove:
            if key in PREMIUM_KEYS:
                del PREMIUM_KEYS[key]

    save_keys()

def key_cleaner_thread():
    """Hilo para ejecutar la limpieza de claves periódicamente."""
    while True:
        clean_expired_keys()
        time.sleep(3600)

# ===========================
# HANDLERS de Telegram
# ===========================
def get_admin_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    keyboard = []
    keyboard.append([InlineKeyboardButton("🔑 Generar Clave (con Nivel)", callback_data="admin_prompt_generate_level")])
    keyboard.append([InlineKeyboardButton("📊 Ver Claves", callback_data="admin_view_keys")])
    keyboard.append([InlineKeyboardButton("🗑️ Eliminar Clave (usar /ban)", callback_data="admin_prompt_delete_key")])
    keyboard.append([InlineKeyboardButton("👥 Ver Usuarios", callback_data="admin_view_users")])
    keyboard.append([InlineKeyboardButton("🚫 Bloquear (Ban) Usuario (Usar /banuser)", callback_data="admin_prompt_ban_user")])
    # Botón de broadcast
    keyboard.append([InlineKeyboardButton("📢 Broadcast Multimedia", callback_data="admin_prompt_broadcast")])
    keyboard.append([InlineKeyboardButton("🔙 Volver a /start", callback_data="back_to_start")])
    return InlineKeyboardMarkup(keyboard)

def get_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    keyboard = []

    # Si el usuario está baneado, solo mostramos el contacto del admin.
    with BANNED_USERS_LOCK:
        is_banned = chat_id in BANNED_USERS

    if is_banned:
        keyboard.append([InlineKeyboardButton("🚫 Estás Bloqueado. Contacta al Admin.", url=f"https://t.me/{ADMIN_USERNAME.lstrip('@')}")])
        return InlineKeyboardMarkup(keyboard)

    with USER_ACTIVE_KEYS_LOCK:
        user_has_key = chat_id in USER_ACTIVE_KEYS

    if user_has_key:
        if chat_id in SUBSCRIPTIONS:
            keyboard.append([InlineKeyboardButton("🔕 Desuscribirme", callback_data="unsubscribe")])
        else:
            keyboard.append([InlineKeyboardButton("🔔 Suscribirme", callback_data="subscribe")])
    else:
        keyboard.append([InlineKeyboardButton(f"Comprar Claves con {ADMIN_USERNAME}", url=f"https://t.me/{ADMIN_USERNAME.lstrip('@')}")])

    if str(chat_id) == MY_CHAT_ID:
        keyboard.append([InlineKeyboardButton("🛠️ Panel de Admin", callback_data="admin_panel_start")])
        if chat_id not in SUBSCRIPTIONS:
            keyboard.append([InlineKeyboardButton("🔔 Suscribirme (Admin)", callback_data="subscribe_admin")]) 
        else:
            keyboard.append([InlineKeyboardButton("🔕 Desuscribirme (Admin)", callback_data="unsubscribe_admin")])

    return InlineKeyboardMarkup(keyboard)

def get_caption_text(user: Optional[Any], chat_id: int) -> str:
    user_id = chat_id
    # Obtenemos el nombre y username de forma segura
    user_name = user.first_name if user and user.first_name else "Usuario Desconocido"
    user_username = f"@{user.username}" if user and user.username else "sin_username"

    with BANNED_USERS_LOCK:
        is_banned = chat_id in BANNED_USERS

    if is_banned:
        return (
            f"🚫 <b>¡TU ACCESO ESTÁ BLOQUEADO!</b> 🚫\n\n"
            f"Tu ID (<code>{user_id}</code>) ha sido bloqueado por el administrador.\n"
            f"No recibirás códigos OTP y no podrás gestionar tu suscripción.\n\n"
            f"Contacta a {ADMIN_USERNAME} para resolver el problema."
        )

    personal_status = ""
    with SUBSCRIPTIONS_LOCK:
        personal_status = "🟢 SUSCRITO" if chat_id in SUBSCRIPTIONS else "🔴 NO SUSCRITO"

    key_status_text = ""
    with USER_ACTIVE_KEYS_LOCK:
        active_key = USER_ACTIVE_KEYS.get(chat_id)

    if active_key:
        with PREMIUM_KEYS_LOCK:
            key_details = PREMIUM_KEYS.get(active_key)
        if key_details:
            time_left_str = get_time_remaining(key_details["expires_at"])
            level = key_details.get("level", "N/A")

            key_status_text = f"\n🔑 <b>Clave Premium:</b> <code>{html.escape(active_key)}</code>\n" \
                              f"🌟 <b>Nivel:</b> {html.escape(level)}\n" \
                              f"⏳ <b>Expira en:</b> {time_left_str}"

            if time_left_str == "EXPIRADA":
                 key_status_text += "\n<i>(Esta clave ha expirado.)</i>"
                 personal_status = "🔴 NO SUSCRITO (Clave Expirada)"
        else: 
            key_status_text = "\n🔑 <b>Clave Premium:</b> INVÁLIDA"
            personal_status = "🔴 NO SUSCRITO (Clave Inválida)"
    else:
        key_status_text = "\n🔑 <b>Clave Premium:</b> NO ASIGNADA"
        key_status_text += f"\n👉 Usa <code>/key [CODIGO]</code> para canjear tu clave."


    caption = (
        "🎉 <b>Bienvenido a tu Bot de Códigos OTP</b>\n\n"
        f"🆔 <b>Tu ID:</b> <code>{user_id}</code>\n"
        f"👤 <b>Tu Nombre:</b> {html.escape(user_name)}\n"
        f"📛 <b>Tu Username:</b> {html.escape(user_username)}\n\n"
        f"<b>Status (tú):</b> {personal_status}"
        f"{key_status_text}\n\n"
        "—\n\n"
        "🔑 <b>Controles:</b>\n"
        "Pulsa los botones para cambiar tu estado de suscripción y recibir OTPs.\n"
        "<i>Este bot NO reenvía OTPs de restablecimiento de contraseña.</i>"
    )
    return caption

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    user = update.effective_user
    gif_url = random.choice(GIFS) if GIFS else None

    # Guarda la info de contacto al iniciar
    update_user_contacts(user, chat_id)
    # Guarda la persistencia después de actualizar el contacto
    save_keys() 

    if chat_id in ADMIN_STATE: del ADMIN_STATE[chat_id]

    reply_markup = get_keyboard(chat_id)
    caption = get_caption_text(user, chat_id)

    if update.callback_query:
        try:
            await update.callback_query.edit_message_caption(
                caption=caption,
                parse_mode="HTML",
                reply_markup=reply_markup
            )
            await update.callback_query.answer()
            return
        except Exception:
            # fallback if message has no caption (e.g., text message)
            pass

    if update.message:
        if gif_url:
            try:
                await update.message.reply_animation(gif_url, caption=caption, parse_mode="HTML", reply_markup=reply_markup)
                return
            except Exception:
                pass
        if gif_url:
            try:
                await update.message.reply_photo(gif_url, caption=caption, parse_mode="HTML", reply_markup=reply_markup)
                return
            except Exception:
                pass
        await update.message.reply_text(caption, parse_mode="HTML", reply_markup=reply_markup)

# 🌟 FUNCIÓN DE BROADCAST MODIFICADA PARA ELEGIR TARGET
async def perform_broadcast(context: ContextTypes.DEFAULT_TYPE, content_message: Message, admin_chat_id: int, target_group: str):

    with SUBSCRIPTIONS_LOCK: 
        all_subscribers = set(SUBSCRIPTIONS)
    with USER_ACTIVE_KEYS_LOCK:
        users_with_keys = set(USER_ACTIVE_KEYS.keys())
    with BANNED_USERS_LOCK:
        banned_users = set(BANNED_USERS)

    # Filtrar usuarios por Premium o No Premium (solo aquellos que han usado /start y son conocidos)
    # Usuario Premium: está suscrito Y tiene una clave activa
    # Usuario No Premium: está suscrito PERO NO tiene clave activa

    premium_users = [cid for cid in all_subscribers if cid in users_with_keys and cid not in banned_users]
    non_premium_users = [cid for cid in all_subscribers if cid not in users_with_keys and cid not in banned_users]

    if target_group == "PREMIUM":
        targets = [cid for cid in premium_users if cid != admin_chat_id]
        target_name = "Usuarios Premium"
    elif target_group == "NON_PREMIUM":
        targets = [cid for cid in non_premium_users if cid != admin_chat_id]
        target_name = "Usuarios No Premium"
    else:
        targets = []
        target_name = "Nadie (Error)"

    sent_count = 0
    failed_count = 0

    await context.bot.send_message(chat_id=admin_chat_id, text=f"📢 Iniciando Broadcast a <b>{len(targets)} {target_name}</b>...", parse_mode="HTML")

    for chat_id in targets:
        try:
            # Selecciona el método de envío basado en el tipo de contenido
            if content_message.photo:
                await context.bot.send_photo(chat_id=chat_id, photo=content_message.photo[-1].file_id, caption=content_message.caption, parse_mode="HTML")
            elif content_message.video:
                await context.bot.send_video(chat_id=chat_id, video=content_message.video.file_id, caption=content_message.caption, parse_mode="HTML")
            elif content_message.animation:
                await context.bot.send_animation(chat_id=chat_id, animation=content_message.animation.file_id, caption=content_message.caption, parse_mode="HTML")
            elif content_message.text:
                 # Si es texto puro, usa send_message con HTML parse mode
                await context.bot.send_message(chat_id=chat_id, text=content_message.text, parse_mode="HTML")
            else:
                failed_count += 1
                continue

            sent_count += 1
            # Pausa para no saturar la API
            await asyncio.sleep(0.05) 

        except Exception as e:
            failed_count += 1
            print(f"❌ Error enviando broadcast a {chat_id}: {e}")

    await context.bot.send_message(
        chat_id=admin_chat_id, 
        text=f"✅ Broadcast Terminado ({target_name}).\n\nEnviados con éxito: <b>{sent_count}</b>\nFallidos (ej. usuario bloqueó el bot, chat eliminado): <b>{failed_count}</b>",
        parse_mode="HTML"
    )

    # Limpiar estado y volver al panel de admin
    if admin_chat_id in ADMIN_BROADCAST_TARGET: del ADMIN_BROADCAST_TARGET[admin_chat_id]
    await context.bot.send_message(
        chat_id=admin_chat_id, 
        text="Volviendo al Panel de Admin.", 
        reply_markup=get_admin_keyboard(admin_chat_id), 
        parse_mode="HTML"
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query is None: return
    chat_id = query.message.chat.id
    user = query.from_user
    data = query.data

    # Comprobación de baneo al usar botones
    with BANNED_USERS_LOCK:
        if chat_id in BANNED_USERS and str(chat_id) != MY_CHAT_ID:
            await query.answer(text="🚫 Estás bloqueado y no puedes usar esta función.", show_alert=True)
            await start_command(update, context)
            return

    action_feedback = ""
    status_changed = False 

    # Actualiza info de contacto
    update_user_contacts(user, chat_id)

    # Limpiar estado si no es un paso de broadcast
    if chat_id in ADMIN_STATE and data not in ["admin_prompt_generate_level", "admin_panel_start", "admin_view_keys", "admin_prompt_delete_key", "admin_view_users", "admin_prompt_ban_user", "admin_prompt_broadcast", "admin_select_premium", "admin_select_non_premium", "admin_cancel_broadcast"]:
         if ADMIN_STATE.get(chat_id) != "AWAITING_BROADCAST_CONTENT":
            del ADMIN_STATE[chat_id]
            if chat_id in ADMIN_BROADCAST_TARGET: del ADMIN_BROADCAST_TARGET[chat_id]


    if data == "admin_panel_start" and str(chat_id) == MY_CHAT_ID:
        # Limpiar estado al entrar al panel
        if chat_id in ADMIN_STATE: del ADMIN_STATE[chat_id]
        if chat_id in ADMIN_BROADCAST_TARGET: del ADMIN_BROADCAST_TARGET[chat_id]
        await query.edit_message_caption(
            caption="🛠️ <b>Panel de Administración de Claves</b>\n\nUtiliza los botones para gestionar las claves premium.",
            parse_mode="HTML",
            reply_markup=get_admin_keyboard(chat_id)
        )
        await query.answer()
        return

    elif data == "back_to_start":
        await start_command(update, context) 
        return

    # 1. Iniciar el flujo de selección de target para broadcast
    elif data == "admin_prompt_broadcast" and str(chat_id) == MY_CHAT_ID:

        with SUBSCRIPTIONS_LOCK: 
            targets = list(SUBSCRIPTIONS)
        with BANNED_USERS_LOCK:
            active_users = [cid for cid in targets if cid != chat_id and cid not in BANNED_USERS]

        if not active_users:
            action_feedback = "⚠️ No hay usuarios activos a quienes enviar el broadcast."
        else:
            ADMIN_STATE[chat_id] = "AWAITING_BROADCAST_TARGET"
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"📢 **MODO BROADCAST**\n\nSelecciona a quién deseas enviar el mensaje ({len(active_users)} usuarios activos):",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("💎 Usuarios Premium", callback_data="admin_select_premium")],
                    [InlineKeyboardButton("💸 Usuarios No Premium", callback_data="admin_select_non_premium")],
                    [InlineKeyboardButton("❌ Cancelar Broadcast", callback_data="admin_cancel_broadcast")]
                ])
            )
            return

    # 2. Seleccionar el target y pasar a AWAITING_BROADCAST_CONTENT
    elif data in ["admin_select_premium", "admin_select_non_premium"] and str(chat_id) == MY_CHAT_ID:
        if ADMIN_STATE.get(chat_id) != "AWAITING_BROADCAST_TARGET":
            action_feedback = "⚠️ Error de secuencia. Presiona 'Broadcast Multimedia' de nuevo."
        else:
            target_group = "PREMIUM" if data == "admin_select_premium" else "NON_PREMIUM"
            ADMIN_BROADCAST_TARGET[chat_id] = target_group
            ADMIN_STATE[chat_id] = "AWAITING_BROADCAST_CONTENT"

            group_name = "Premium" if target_group == "PREMIUM" else "No Premium"

            await context.bot.send_message(
                chat_id=chat_id,
                text=f"📢 **MODO BROADCAST: {group_name}**\n\nEnvía el mensaje (texto, foto, video o GIF) que deseas enviar.\n\nEl texto/caption debe estar en formato HTML. Puedes usar el hashtag <code>#CANCEL</code> para salir.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancelar Broadcast", callback_data="admin_cancel_broadcast")]])
            )
            return

    # Cancelación del Broadcast
    elif data == "admin_cancel_broadcast" and str(chat_id) == MY_CHAT_ID:
        if chat_id in ADMIN_STATE: del ADMIN_STATE[chat_id]
        if chat_id in ADMIN_BROADCAST_TARGET: del ADMIN_BROADCAST_TARGET[chat_id]
        action_feedback = "❌ Broadcast cancelado."

    # --- Lógica de usuario (Suscripción, etc.) ---
    elif data in ["subscribe", "unsubscribe", "subscribe_admin", "unsubscribe_admin"]:
        # ... (Mantener la lógica de suscripción/desuscripción)
        if data == "subscribe": 
            with USER_ACTIVE_KEYS_LOCK: user_has_key = chat_id in USER_ACTIVE_KEYS
            if not user_has_key:
                action_feedback = f"🚨 No tienes una clave premium activa. Usa <code>/key [CODIGO]</code> o contacta a {ADMIN_USERNAME}."
            else:
                with PREMIUM_KEYS_LOCK:
                    key = USER_ACTIVE_KEYS[chat_id]
                    is_expired = PREMIUM_KEYS.get(key, {}).get("expires_at", datetime.datetime.min) < datetime.datetime.now()
                if is_expired:
                    action_feedback = "🚨 Tu clave premium ha expirado. Contacta al administrador."
                    clean_expired_keys() 
                    status_changed = True
                else:
                    with SUBSCRIPTIONS_LOCK:
                        if chat_id not in SUBSCRIPTIONS:
                            SUBSCRIPTIONS.add(chat_id)
                            save_keys()
                            status_changed = True
                            action_feedback = "✅ Suscripción ACTIVADA. Recibirás OTPs."
                        else: action_feedback = "🔔 Ya estás suscrito."
        elif data == "unsubscribe":
            with SUBSCRIPTIONS_LOCK:
                if chat_id in SUBSCRIPTIONS:
                    SUBSCRIPTIONS.remove(chat_id)
                    save_keys()
                    status_changed = True
                    action_feedback = "🛑 Suscripción DESACTIVADA."
                else: action_feedback = "🔕 Ya estás desuscrito."
        elif data in ["subscribe_admin", "unsubscribe_admin"] and str(chat_id) == MY_CHAT_ID:
            with SUBSCRIPTIONS_LOCK:
                if data == "subscribe_admin" and chat_id not in SUBSCRIPTIONS:
                    SUBSCRIPTIONS.add(chat_id)
                    save_keys()
                    status_changed = True
                    action_feedback = "✅ Suscripción ACTIVADA (Admin)."
                elif data == "unsubscribe_admin" and chat_id in SUBSCRIPTIONS:
                    SUBSCRIPTIONS.remove(chat_id)
                    save_keys()
                    status_changed = True
                    action_feedback = "🛑 Suscripción DESACTIVADA (Admin)."
                else: action_feedback = "Estado de suscripción ya es el deseado."
        else:
            action_feedback = "⛔ Solo el administrador puede usar este botón."

    # --- Lógica de Admin (Claves, Usuarios) ---
    elif data == "admin_prompt_generate_level" and str(chat_id) == MY_CHAT_ID:
        try:
            level_input = "Sin Nivel"
            # Generar clave alfanumérica de 16 caracteres
            key_code = ''.join(random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") for i in range(16))
            expires_at = datetime.datetime.now() + datetime.timedelta(days=30)

            with PREMIUM_KEYS_LOCK:
                max_gen = 0
                for key in PREMIUM_KEYS.keys():
                    match = re.search(r'paublte-gen(\d+)-', key)
                    if match: max_gen = max(max_gen, int(match.group(1)))
                generation_count = max_gen + 1

                generated_key = f"paublte-gen{generation_count}-{key_code}"
                PREMIUM_KEYS[generated_key] = {"expires_at": expires_at, "chat_id": None, "level": level_input}

            save_keys()

            popup_text = f"🔑 GENERADA:\n{generated_key}"
            try: await query.answer(text=popup_text, show_alert=True)
            except Exception: pass

            response_text = (
                f"🔑 ✅ <b>CLAVE GENERADA</b>\n\n"
                f"Nivel: <b>{html.escape(level_input)}</b>\n"
                f"Código: <code>{generated_key}</code>\n"
                f"Expira en 30 días: {expires_at.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                f"Para canjear, el usuario debe enviar: <code>/key {generated_key}</code>"
            )
            await context.bot.send_message(chat_id=chat_id, text=response_text, parse_mode="HTML", reply_markup=get_admin_keyboard(chat_id))
            return
        except Exception as e_gen:
            action_feedback = f"❌ Error al generar la clave: {e_gen}"

    elif data == "admin_view_keys" and str(chat_id) == MY_CHAT_ID:
        clean_expired_keys()
        if not PREMIUM_KEYS:
            action_feedback = "📊 No hay claves premium generadas aún."
        else:
            key_list_text = "✨ <b>Claves Premium Generadas:</b>\n\n"
            with PREMIUM_KEYS_LOCK:
                sorted_keys = sorted(PREMIUM_KEYS.items(), key=lambda item: item[1].get("expires_at", datetime.datetime.max))
                for key, details in sorted_keys:
                    time_left_str = get_time_remaining(details["expires_at"])
                    assigned_to = f"Asignada a: <code>{details['chat_id']}</code>" if details.get("chat_id") else "Sin asignar"
                    level_text = f"→ {details.get('level', 'Sin Nivel')}"

                    status_emoji = "🟢"
                    if time_left_str == "EXPIRADA": status_emoji = "🔴"
                    elif details.get("chat_id") is None: status_emoji = "⚪"

                    key_list_text += f"{status_emoji} <code>{html.escape(key)}</code> ({time_left_str}) {level_text} → {assigned_to}\n"
            action_feedback = key_list_text

    elif data == "admin_prompt_delete_key" and str(chat_id) == MY_CHAT_ID:
        action_feedback = "🗑️ Para eliminar una clave usa el comando:\n\n/ban <code>CODIGO_CLAVE</code> [ID_USUARIO]\n\nEjemplo: <code>/ban paublte-gen1-ABCDEF 123456789</code> (si conoces el ID)."

    elif data == "admin_prompt_ban_user" and str(chat_id) == MY_CHAT_ID:
        action_feedback = "🚫 Para bloquear un usuario (que no reciba OTPs), usa el comando:\n\n/banuser <code>ID_USUARIO</code>\n\nPara desbloquear:\n\n/unbanuser <code>ID_USUARIO</code>"

    elif data == "admin_view_users" and str(chat_id) == MY_CHAT_ID:
        # Construir la lista de usuarios: suscritos, con clave asignada, sin clave
        clean_expired_keys()

        with SUBSCRIPTIONS_LOCK: subs = set(SUBSCRIPTIONS)
        with USER_ACTIVE_KEYS_LOCK: active_map = dict(USER_ACTIVE_KEYS)
        with PREMIUM_KEYS_LOCK:
            assigned_details = {} 
            all_known_users = set(subs)
            for key, details in PREMIUM_KEYS.items():
                cid = details.get("chat_id")
                if cid:
                    all_known_users.add(cid)
                    expires_str = details["expires_at"].strftime('%Y-%m-%d %H:%M:%S')
                    level = details.get("level", "N/A")
                    time_left = get_time_remaining(details["expires_at"])
                    assigned_details[cid] = (key, level, expires_str, time_left)

        with USER_CONTACTS_LOCK: contacts_map = dict(USER_CONTACTS)
        with BANNED_USERS_LOCK: banned_users = set(BANNED_USERS)

        # Listas de usuarios
        subscribed_with_key = []
        not_subscribed_with_key = []
        subscribed_without_key = []
        banned_list = []
        unknown_contacts = [] 

        all_tracked_ids = all_known_users | banned_users | set(contacts_map.keys())

        for cid in sorted(list(set(active_map.keys()) | all_tracked_ids)):
            details = assigned_details.get(cid)
            is_sub = cid in subs
            is_banned = cid in banned_users
            contacts = contacts_map.get(cid, {"name": "N/A", "username": "sin_username"}) # Usar valor por defecto

            # Clasificación
            if is_banned:
                banned_list.append((cid, details, contacts))
            elif details:
                if is_sub:
                    subscribed_with_key.append((cid, details, contacts))
                else:
                    not_subscribed_with_key.append((cid, details, contacts))
            elif is_sub:
                subscribed_without_key.append((cid, contacts))
            elif cid in active_map and contacts is None: 
                 unknown_contacts.append((cid, assigned_details.get(cid), contacts))


        # --- Función para formatear un usuario (MUESTRA @USER EN TODO) ---
        def format_user_info(cid, details_tuple: Optional[tuple], contact_info: Optional[dict], banned: bool = False):
            name = html.escape(contact_info.get("name") or "N/A")
            username = html.escape(contact_info.get("username") or "sin_username")

            user_display = f"👤 <b>{name}</b>"
            if username and username != "sin_username":
                user_display += f" (<code>{username}</code>)"

            output = f"{( '🚫' if banned else '•' )} {user_display}\n"
            output += f"🆔 <b>ID:</b> <code>{cid}</code>\n"

            if details_tuple:
                key, level, expires, time_left = details_tuple
                output += f"🔑 Clave: <code>{html.escape(key)}</code>\n"
                output += f"🌟 Nivel: {html.escape(level)}\n"
                output += f"⏳ Expira en: {time_left} ({expires})\n"

            return output

        # --- Construir texto ---
        parts = []
        parts.append("👥 <b>Usuarios - Resumen Detallado</b>\n")
        parts.append(f"Total Suscritos Activos: <b>{len(subscribed_with_key) + len(subscribed_without_key)}</b>\n")
        parts.append(f"Total Bloqueados (Ban): <b>{len(banned_users)}</b>\n")
        parts.append("—\n")

        # PARTE 1: Suscritos con clave asignada
        parts.append(f"🟢 <b>Premium (Suscritos & Con Clave - Total {len(subscribed_with_key)}):</b>\n")
        if subscribed_with_key:
            for cid, details, contacts in subscribed_with_key[:100]:
                parts.append(format_user_info(cid, details, contacts) + "—\n")
            if len(subscribed_with_key) > 100: parts.append("... (lista truncada)\n")
        else:
            parts.append("— Ninguno —\n")
        parts.append("—\n")

        # PARTE 2: Con clave asignada PERO no suscritos
        parts.append(f"⚪ <b>Con Clave Asignada (NO Suscritos) (Total {len(not_subscribed_with_key)}):</b>\n")
        if not_subscribed_with_key:
            for cid, details, contacts in not_subscribed_with_key[:100]:
                parts.append(format_user_info(cid, details, contacts) + "—\n")
            if len(not_subscribed_with_key) > 100: parts.append("... (lista truncada)\n")
        else:
            parts.append("— Ninguno —\n")
        parts.append("—\n")

        # PARTE 3: Suscritos SIN clave asignada (Usuarios No Premium Activos)
        parts.append(f"⚠️ <b>No Premium (Suscritos SIN Clave - Total {len(subscribed_without_key)}):</b>\n")
        if subscribed_without_key:
             for cid, contacts in subscribed_without_key[:100]:
                parts.append(format_user_info(cid, None, contacts) + "—\n")
             if len(subscribed_without_key) > 100: parts.append("... (lista truncada)\n")
        else:
             parts.append("— Ninguno —\n")
        parts.append("—\n")

        # PARTE 4: Usuarios Bloqueados
        parts.append(f"🚫 <b>USUARIOS BLOQUEADOS (BANNED) (Total {len(banned_list)}):</b>\n")
        if banned_list:
             for cid, details, contacts in banned_list[:100]:
                parts.append(format_user_info(cid, details, contacts, banned=True) + "—\n")
             if len(banned_list) > 100: parts.append("... (lista truncada)\n")
        else:
             parts.append("— Ninguno —\n")
        parts.append("—\n")

        # Última Sección: Claves Asignadas a IDs desconocidos
        if unknown_contacts:
             parts.append(f"❓ <b>CLAVES ASIGNADAS A IDs SIN INFO DE CONTACTO (Total {len(unknown_contacts)}):</b>\n")
             for cid, details, contacts in unknown_contacts[:100]:
                parts.append(format_user_info(cid, details, contacts) + "—\n")
             if len(unknown_contacts) > 100: parts.append("... (lista truncada)\n")
             parts.append("—\n")

        parts.append("<i>Nota:</i> La información de Nombre/Username se guarda cuando el usuario utiliza /start o /key.")
        full_text = "\n".join(parts)

        # Si el mensaje es muy largo, enviar en trozos
        try:
            if len(full_text) <= 4000:
                await context.bot.send_message(chat_id=chat_id, text=full_text, parse_mode="HTML")
            else:
                lines = full_text.splitlines(True)
                chunk = ""

                for i, line in enumerate(lines):
                    if len(chunk) + len(line) > 3500 and i > 0:
                        await context.bot.send_message(chat_id=chat_id, text=chunk, parse_mode="HTML")
                        chunk = line
                    else:
                        chunk += line
                if chunk:
                    await context.bot.send_message(chat_id=chat_id, text=chunk, parse_mode="HTML")
        except Exception as e_users:
            try:
                await context.bot.send_message(chat_id=chat_id, text=f"❌ Error mostrando usuarios: {e_users}")
            except Exception:
                pass
        return

    elif str(chat_id) != MY_CHAT_ID and (data.startswith("admin_") or data == "back_to_start"):
        action_feedback = "⛔ Esta acción es solo para el administrador."

    try:
        popup_text = action_feedback.split('\n')[0] if action_feedback else "Estado actualizado."
        show_alert = any(icon in action_feedback for icon in ["🚨", "⛔", "🔑", "📊", "⚠️", "❌", "🗑️", "🚫"])
        if action_feedback:
            await query.answer(text=popup_text, show_alert=show_alert)
    except Exception:
        pass

    if status_changed and str(chat_id) != MY_CHAT_ID:
        new_caption = get_caption_text(user, chat_id)
        new_keyboard = get_keyboard(chat_id)
        try:
            await query.edit_message_caption(caption=new_caption, reply_markup=new_keyboard, parse_mode="HTML")
        except Exception:
            await context.bot.send_message(chat_id=chat_id, text=action_feedback + " (No se pudo actualizar el mensaje de estado).", parse_mode="HTML")

    if str(chat_id) == MY_CHAT_ID and action_feedback and not data.startswith("admin_prompt_"):
          await context.bot.send_message(chat_id=chat_id, text=action_feedback, parse_mode="HTML")

async def handle_admin_messages(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Maneja el texto entrante del administrador."""
    chat_id = update.effective_chat.id
    if str(chat_id) != MY_CHAT_ID: return

    message = update.message

    # Manejar Cancelación del Broadcast (por texto)
    if message.text and message.text.upper().strip() == "#CANCEL":
        if ADMIN_STATE.get(chat_id) in ["AWAITING_BROADCAST_TARGET", "AWAITING_BROADCAST_CONTENT"]:
            del ADMIN_STATE[chat_id]
            if chat_id in ADMIN_BROADCAST_TARGET: del ADMIN_BROADCAST_TARGET[chat_id]
            await message.reply_text("❌ Broadcast cancelado.", reply_markup=get_admin_keyboard(chat_id), parse_mode="HTML")
            return

    # 🌟 NUEVO: Manejar Contenido del Broadcast
    if ADMIN_STATE.get(chat_id) == "AWAITING_BROADCAST_CONTENT":
        target_group = ADMIN_BROADCAST_TARGET.get(chat_id)

        if message.text or message.photo or message.video or message.animation:
            await message.reply_text("✅ Contenido recibido. Iniciando el envío masivo...", parse_mode="HTML")
            del ADMIN_STATE[chat_id]

            # Ejecutar el broadcast en segundo plano
            await perform_broadcast(context, message, chat_id, target_group)
            return

    if ADMIN_STATE.get(chat_id) == "AWAITING_LEVEL":
        level_input = update.message.text.strip()

        # Generar clave alfanumérica de 16 caracteres
        key_code = ''.join(random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") for i in range(16))
        expires_at = datetime.datetime.now() + datetime.timedelta(days=30)

        generation_count = 1
        with PREMIUM_KEYS_LOCK:
            max_gen = 0
            for key in PREMIUM_KEYS.keys():
                match = re.search(r'paublte-gen(\d+)-', key)
                if match: max_gen = max(max_gen, int(match.group(1)))
            generation_count = max_gen + 1

            generated_key = f"paublte-gen{generation_count}-{key_code}"
            PREMIUM_KEYS[generated_key] = {"expires_at": expires_at, "chat_id": None, "level": level_input}

        save_keys()

        response_text = f"🔑 ✅ **CLAVE GENERADA**\n" \
                        f"Nivel: **{html.escape(level_input)}**\n" \
                        f"Código: <code>{generated_key}</code>\n" \
                        f"Expira en 30 días: {expires_at.strftime('%Y-%m-%d %H:%M:%S')}\n\n" \
                        f"Para canjear, el usuario debe enviar: <code>/key {generated_key}</code>"

        del ADMIN_STATE[chat_id]
        await update.message.reply_text(response_text, parse_mode="HTML")
        await context.bot.send_message(chat_id=chat_id, text="Volviendo al Panel de Admin.", reply_markup=get_admin_keyboard(chat_id), parse_mode="HTML")
        return

    pass

# --- Comandos Administrativos/Usuario (Sin cambios significativos) ---

async def key_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    user = update.effective_user
    args = context.args

    # Comprobación de baneo
    with BANNED_USERS_LOCK:
        if chat_id in BANNED_USERS:
            await update.message.reply_text(f"🚫 Tu ID (<code>{chat_id}</code>) está bloqueado. Contacta a {ADMIN_USERNAME}.", parse_mode="HTML")
            return

    if len(args) != 1:
        await update.message.reply_text("Uso: <code>/key [CODIGO_CLAVE]</code>", parse_mode="HTML")
        return

    key_to_assign = args[0]

    # Guarda la info de contacto al usar /key
    update_user_contacts(user, chat_id)

    # NUEVO: si el usuario ya tiene una clave activa, NO permitir acumular otra.
    with USER_ACTIVE_KEYS_LOCK:
        if chat_id in USER_ACTIVE_KEYS:
            current = USER_ACTIVE_KEYS[chat_id]
            await update.message.reply_text(
                f"🚨 Ya tienes una suscripción activa con la clave <code>{current}</code>. "
                "No puedes canjear otra clave a la vez.", parse_mode="HTML"
            )
            return

    with PREMIUM_KEYS_LOCK:
        # Si la clave no existe en el sistema -> mensaje uniforme "no existe o ya fue canjeada"
        if key_to_assign not in PREMIUM_KEYS:
            await update.message.reply_text(f"❌ La clave <code>{key_to_assign}</code> no existe o ya fue canjeada.", parse_mode="HTML")
            return

        details = PREMIUM_KEYS[key_to_assign]
        expires_at = details["expires_at"]
        level = details.get("level", "N/A")

        # Si la clave expiró
        if expires_at < datetime.datetime.now():
            await update.message.reply_text(f"🚨 La clave <code>{key_to_assign}</code> ha expirado. Contacta a {ADMIN_USERNAME} para obtener una nueva.", parse_mode="HTML")
            clean_expired_keys()
            return

        assigned_to = details.get("chat_id")
        # Si la clave ya está asignada a otro usuario -> mensaje: no existe o ya fue canjeada
        if assigned_to is not None and assigned_to != chat_id:
            await update.message.reply_text(f"❌ La clave <code>{key_to_assign}</code> no existe o ya fue canjeada.", parse_mode="HTML")
            return

        # OK: asignar la clave al usuario (single-use)
        PREMIUM_KEYS[key_to_assign]["chat_id"] = chat_id

    with USER_ACTIVE_KEYS_LOCK:
        USER_ACTIVE_KEYS[chat_id] = key_to_assign

    with SUBSCRIPTIONS_LOCK:
        if chat_id not in SUBSCRIPTIONS:
            SUBSCRIPTIONS.add(chat_id)

    save_keys()

    response_text = f"🎉 ¡Clave <code>{key_to_assign}</code> canjeada y activada con éxito!\n" \
                    f"🌟 Nivel: **{html.escape(level)}**\n" \
                    f"Expira el: {expires_at.strftime('%Y-%m-%d %H:%M:%S')} ({get_time_remaining(expires_at)})\n" \
                    f"Tu suscripción está <b>ACTIVADA</b> y recibirás códigos OTP de inmediato."

    await update.message.reply_text(response_text, parse_mode="HTML")
    await start_command(update, context) 

async def delete_key_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Usa el comando /ban para eliminar (banear) una clave. Ejemplo:\n/ban paublte-gen1-ABCDEF 123456789")

async def ban_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Comando /ban KEY [USER_ID] - Elimina la clave del sistema (solo admin)."""
    requester = str(update.effective_chat.id)
    if requester != MY_CHAT_ID:
        await update.message.reply_text("⛔ Solo el administrador puede usar este comando.")
        return

    args = context.args
    if len(args) == 0:
        await update.message.reply_text("Uso: <code>/ban CODIGO_CLAVE [ID_USUARIO]</code>", parse_mode="HTML")
        return

    key_to_ban = args[0]
    user_id_arg = None
    if len(args) >= 2:
        try:
            user_id_arg = int(args[1])
        except Exception:
            await update.message.reply_text("El segundo argumento debe ser el ID de usuario (número).", parse_mode="HTML")
            return

    with PREMIUM_KEYS_LOCK:
        if key_to_ban not in PREMIUM_KEYS:
            await update.message.reply_text(f"❌ La clave <code>{key_to_ban}</code> no existe.", parse_mode="HTML")
            return
        assigned_to = PREMIUM_KEYS[key_to_ban].get("chat_id")
        if user_id_arg is not None and assigned_to is not None and assigned_to != user_id_arg:
            await update.message.reply_text(f"❌ La clave <code>{key_to_ban}</code> no está asignada al usuario {user_id_arg}.", parse_mode="HTML")
            return
        del PREMIUM_KEYS[key_to_ban]

    if assigned_to:
        with USER_ACTIVE_KEYS_LOCK:
            if USER_ACTIVE_KEYS.get(assigned_to) == key_to_ban:
                del USER_ACTIVE_KEYS[assigned_to]
        with SUBSCRIPTIONS_LOCK:
            if assigned_to in SUBSCRIPTIONS:
                SUBSCRIPTIONS.remove(assigned_to)
        save_keys() # Guardar cambios
        try:
            await context.bot.send_message(chat_id=assigned_to, text=f"🔥 Tu clave premium <code>{key_to_ban}</code> ha sido ELIMINADA por el administrador.", parse_mode="HTML")
        except Exception:
            pass

    save_keys()
    await update.message.reply_text(f"✅ Clave <code>{key_to_ban}</code> ha sido eliminada del sistema.", parse_mode="HTML")


async def ban_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """COMANDO: /banuser [ID_USUARIO] - Bloquea un usuario."""
    requester = str(update.effective_chat.id)
    if requester != MY_CHAT_ID:
        await update.message.reply_text("⛔ Solo el administrador puede usar este comando.")
        return

    args = context.args
    if len(args) != 1:
        await update.message.reply_text("Uso: <code>/banuser ID_USUARIO</code> (El ID del usuario a bloquear/banear).", parse_mode="HTML")
        return

    try:
        user_id_to_ban = int(args[0])
    except ValueError:
        await update.message.reply_text("El ID de usuario debe ser un número entero.", parse_mode="HTML")
        return

    with BANNED_USERS_LOCK:
        if user_id_to_ban in BANNED_USERS:
            await update.message.reply_text(f"⚠️ El usuario <code>{user_id_to_ban}</code> ya está en la lista de bloqueados.", parse_mode="HTML")
            return
        BANNED_USERS.add(user_id_to_ban)

    # Si el usuario estaba suscrito, se desuscribe para que el filtro funcione de inmediato.
    with SUBSCRIPTIONS_LOCK:
        if user_id_to_ban in SUBSCRIPTIONS:
            SUBSCRIPTIONS.remove(user_id_to_ban)

    save_keys()

    try:
        await context.bot.send_message(chat_id=user_id_to_ban, text=f"🚫 Has sido BLOQUEADO. Tu ID (<code>{user_id_to_ban}</code>) ya no recibirá OTPs. Contacta a {ADMIN_USERNAME}.", parse_mode="HTML")
        await update.message.reply_text(f"✅ Usuario <code>{user_id_to_ban}</code> ha sido **BLOQUEADO** (Ban) y notificado.", parse_mode="HTML")
    except Exception:
        await update.message.reply_text(f"✅ Usuario <code>{user_id_to_ban}</code> ha sido **BLOQUEADO** (Ban). No se pudo notificar.", parse_mode="HTML")

async def unban_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """COMANDO: /unbanuser [ID_USUARIO] - Desbloquea un usuario."""
    requester = str(update.effective_chat.id)
    if requester != MY_CHAT_ID:
        await update.message.reply_text("⛔ Solo el administrador puede usar este comando.")
        return

    args = context.args
    if len(args) != 1:
        await update.message.reply_text("Uso: <code>/unbanuser ID_USUARIO</code> (El ID del usuario a desbloquear/desbanear).", parse_mode="HTML")
        return

    try:
        user_id_to_unban = int(args[0])
    except ValueError:
        await update.message.reply_text("El ID de usuario debe ser un número entero.", parse_mode="HTML")
        return

    with BANNED_USERS_LOCK:
        if user_id_to_unban not in BANNED_USERS:
            await update.message.reply_text(f"⚠️ El usuario <code>{user_id_to_unban}</code> NO está en la lista de bloqueados.", parse_mode="HTML")
            return
        BANNED_USERS.remove(user_id_to_unban)

    save_keys()

    try:
        await context.bot.send_message(chat_id=user_id_to_unban, text=f"🎉 Has sido DESBLOQUEADO. Tu ID (<code>{user_id_to_unban}</code>) puede volver a recibir OTPs. Usa /start para gestionar tu suscripción.", parse_mode="HTML")
        await update.message.reply_text(f"✅ Usuario <code>{user_id_to_unban}</code> ha sido **DESBLOQUEADO** (Unban) y notificado.", parse_mode="HTML")
    except Exception:
        await update.message.reply_text(f"✅ Usuario <code>{user_id_to_unban}</code> ha sido **DESBLOQUEADO** (Unban). No se pudo notificar.", parse_mode="HTML")

async def status_command_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    clean_expired_keys() 

    with BANNED_USERS_LOCK:
        is_banned = chat_id in BANNED_USERS

    if is_banned:
        await update.message.reply_text(f"🚫 Tu ID (<code>{chat_id}</code>) está bloqueado. Contacta a {ADMIN_USERNAME}.", parse_mode="HTML")
        return

    with SUBSCRIPTIONS_LOCK:
        estado_vigilancia = "🟢 Activa (tú estás suscrito)" if chat_id in SUBSCRIPTIONS else "🔴 Pausada (tú no estás suscrito)"

    key_status = "N/A"
    key = USER_ACTIVE_KEYS.get(chat_id)
    if key:
        details = PREMIUM_KEYS.get(key)
        if details:
            key_status = f"Activa (Expira en: {get_time_remaining(details['expires_at'])})"
        else:
            key_status = "Inválida (Contactar admin)"

    response = (
        "✅ Estado General: OK\n"
        f"Vigilancia personal: {estado_vigilancia}\n"
        f"Estado de tu Clave Premium: {key_status}\n"
        f"Total de suscriptores activos (sin baneados): {len(SUBSCRIPTIONS) - len(BANNED_USERS & SUBSCRIPTIONS)}\n"
        f"Última revisión: {time.strftime('%H:%M:%S', time.localtime())} (Intervalo: {IMAP_CHECK_INTERVAL_SECONDS}s)"
    )
    await update.message.reply_text(response)

async def subscribe_command_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    # Comprobación de baneo
    with BANNED_USERS_LOCK:
        if chat_id in BANNED_USERS:
            await update.message.reply_text(f"🚫 Tu ID (<code>{chat_id}</code>) está bloqueado. Contacta a {ADMIN_USERNAME}.", parse_mode="HTML")
            return

    if str(chat_id) != MY_CHAT_ID:
        await update.message.reply_text(f"🚨 La suscripción requiere una clave premium. Usa /start para ver las opciones o contacta {ADMIN_USERNAME}.")
        return
    with SUBSCRIPTIONS_LOCK:
        if chat_id in SUBSCRIPTIONS:
            await update.message.reply_text("🔔 Ya estás suscrito (Admin).")
        else:
            SUBSCRIPTIONS.add(chat_id)
            save_keys()
            await update.message.reply_text("✅ Suscripción ACTIVADA (Admin).")

async def unsubscribe_command_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    # Comprobación de baneo
    with BANNED_USERS_LOCK:
        if chat_id in BANNED_USERS:
            await update.message.reply_text(f"🚫 Tu ID (<code>{chat_id}</code>) está bloqueado. Contacta a {ADMIN_USERNAME}.", parse_mode="HTML")
            return

    if str(chat_id) != MY_CHAT_ID:
        await update.message.reply_text("🚨 La desuscripción se maneja en el menú /start.")
        return
    with SUBSCRIPTIONS_LOCK:
        if chat_id not in SUBSCRIPTIONS:
            await update.message.reply_text("🔕 Ya estás desuscrito (Admin).")
        else:
            SUBSCRIPTIONS.remove(chat_id)
            save_keys()
            await update.message.reply_text("🛑 Suscripción DESACTIVADA (Admin).")

async def assign_key_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    requester_chat_id = str(update.effective_chat.id)
    if requester_chat_id != MY_CHAT_ID:
        await update.message.reply_text("⛔ Solo el administrador puede usar este comando.")
        return
    args = context.args
    if len(args) != 2:
        await update.message.reply_text("Uso: <code>/asignarkey CODIGO_CLAVE ID_USUARIO</code>", parse_mode="HTML")
        return
    key_to_assign = args[0]
    try: target_chat_id = int(args[1])
    except ValueError:
        await update.message.reply_text("El ID de usuario debe ser un número entero.")
        return

    # Comprobación de baneo para el target
    with BANNED_USERS_LOCK:
        if target_chat_id in BANNED_USERS:
            await update.message.reply_text(f"🚫 No se puede asignar una clave al ID <code>{target_chat_id}</code> porque está BLOQUEADO. Desbloquéalo primero con /unbanuser.", parse_mode="HTML")
            return

    with PREMIUM_KEYS_LOCK:
        if key_to_assign not in PREMIUM_KEYS:
            await update.message.reply_text(f"❌ La clave <code>{key_to_assign}</code> no existe.", parse_mode="HTML")
            return
        expires_at = PREMIUM_KEYS[key_to_assign]["expires_at"]
        level = PREMIUM_KEYS[key_to_assign].get("level", "N/A")
        if PREMIUM_KEYS[key_to_assign].get("chat_id") is not None and PREMIUM_KEYS[key_to_assign]["chat_id"] != target_chat_id:
            await update.message.reply_text(f"⚠️ La clave <code>{key_to_assign}</code> ya está asignada al usuario <code>{PREMIUM_KEYS[key_to_assign]['chat_id']}</code>.", parse_mode="HTML")
            return
        PREMIUM_KEYS[key_to_assign]["chat_id"] = target_chat_id

    with USER_ACTIVE_KEYS_LOCK:
        old_key = USER_ACTIVE_KEYS.get(target_chat_id)
        if old_key and old_key != key_to_assign:
            with PREMIUM_KEYS_LOCK:
                if PREMIUM_KEYS.get(old_key): PREMIUM_KEYS[old_key]["chat_id"] = None
            await update.message.reply_text(f"La clave anterior <code>{old_key}</code> ha sido desasignada de <code>{target_chat_id}</code>.", parse_mode="HTML")
        USER_ACTIVE_KEYS[target_chat_id] = key_to_assign

    with SUBSCRIPTIONS_LOCK:
        if target_chat_id not in SUBSCRIPTIONS: SUBSCRIPTIONS.add(target_chat_id)

    save_keys()

    response_text = f"✅ Clave <code>{key_to_assign}</code> asignada a <code>{target_chat_id}</code>.\n" \
                    f"🌟 Nivel: **{html.escape(level)}**\n" \
                    f"Expira en: {expires_at.strftime('%Y-%m-%d %H:%M:%S')} ({get_time_remaining(expires_at)})"
    await update.message.reply_text(response_text, parse_mode="HTML")

    try:
        await context.bot.send_message(chat_id=target_chat_id, text=f"🎉 ¡Tu clave premium ha sido activada!\nCódigo: <code>{key_to_assign}</code>\nExpira el: {expires_at.strftime('%Y-%m-%d %H:%M:%S')}\nAhora puedes recibir códigos OTP. Usa /start para ver tu estado.", parse_mode="HTML")
    except Exception:
        await update.message.reply_text(f"⚠️ No se pudo notificar al usuario {target_chat_id}.", parse_mode="HTML")

async def revoke_key_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    requester_chat_id = str(update.effective_chat.id)
    if requester_chat_id != MY_CHAT_ID:
        await update.message.reply_text("⛔ Solo el administrador puede usar este comando.")
        return
    args = context.args
    if len(args) != 1:
        await update.message.reply_text("Uso: <code>/revocarkey CODIGO_CLAVE</code>", parse_mode="HTML")
        return
    key_to_revoke = args[0]
    with PREMIUM_KEYS_LOCK:
        if key_to_revoke not in PREMIUM_KEYS:
            await update.message.reply_text(f"❌ La clave <code>{key_to_revoke}</code> no existe.", parse_mode="HTML")
            return
        target_chat_id = PREMIUM_KEYS[key_to_revoke].get("chat_id")
        PREMIUM_KEYS[key_to_revoke]["chat_id"] = None

    if target_chat_id:
        with USER_ACTIVE_KEYS_LOCK:
            if USER_ACTIVE_KEYS.get(target_chat_id) == key_to_revoke:
                del USER_ACTIVE_KEYS[target_chat_id]

        with SUBSCRIPTIONS_LOCK:
            if target_chat_id in SUBSCRIPTIONS:
                SUBSCRIPTIONS.remove(target_chat_id)

        try:
            await context.bot.send_message(chat_id=target_chat_id, text=f"🚨 Tu clave premium <code>{key_to_revoke}</code> ha sido revocada.", parse_mode="HTML")
            await update.message.reply_text(f"✅ Clave <code>{key_to_revoke}</code> revocada de <code>{target_chat_id}</code> y notificado al usuario.", parse_mode="HTML")
        except Exception:
            await update.message.reply_text(f"✅ Clave <code>{key_to_revoke}</code> revocada de <code>{target_chat_id}</code>. No se pudo notificar al usuario.", parse_mode="HTML")
    else:
        await update.message.reply_text(f"✅ Clave <code>{key_to_revoke}</code> desasignada. No estaba activa en un usuario.", parse_mode="HTML")

    save_keys()

# ===========================
# ARRANQUE: lanzar hilos IMAP y bot
# ===========================
def start_email_threads():
    for acc in GMAIL_ACCOUNTS:
        pw = acc.get("app_password", "")
        if not pw or len(pw.replace(" ", "")) != 16:
            print(f"⚠️ Advertencia: La cuenta '{acc.get('email')}' NO tiene una app_password de 16 caracteres.")
        else:
            print(f"✅ Cuenta lista: {acc.get('email')} (app_password longitud OK)")

    for acc in GMAIL_ACCOUNTS:
        t = threading.Thread(target=check_for_otp_emails_for_account, args=(acc,), daemon=True)
        t.start()
        print(f"🔁 Hilo lanzado para cuenta: {acc.get('email')}")

if __name__ == "__main__":
    load_keys()
    start_email_threads()

    key_cleaner = threading.Thread(target=key_cleaner_thread, daemon=True)
    key_cleaner.start()
    print("🔁 Hilo de limpieza de claves iniciado.")

    time.sleep(1)
    try:
        application = Application.builder().token(BOT_TOKEN).build()
        # Handlers
        application.add_handler(CommandHandler("start", start_command))
        application.add_handler(CommandHandler("estado", status_command_text))
        application.add_handler(CommandHandler("subscribe", subscribe_command_text))
        application.add_handler(CommandHandler("unsubscribe", unsubscribe_command_text))
        application.add_handler(CommandHandler("key", key_command))
        application.add_handler(CommandHandler("asignarkey", assign_key_command))
        application.add_handler(CommandHandler("revocarkey", revoke_key_command))
        application.add_handler(CommandHandler("eliminar_key", delete_key_command))
        application.add_handler(CommandHandler("ban", ban_command))
        application.add_handler(CommandHandler("banuser", ban_user_command))
        application.add_handler(CommandHandler("unbanuser", unban_user_command))

        application.add_handler(CallbackQueryHandler(button_handler))
        # Manejar cualquier tipo de mensaje (texto/multimedia) para el broadcast
        application.add_handler(MessageHandler((filters.TEXT | filters.PHOTO | filters.VIDEO | filters.ANIMATION) & filters.Chat(int(MY_CHAT_ID)) & ~filters.COMMAND, handle_admin_messages))
        # Este handler final atrapa todo el texto que no es comando ni respuesta admin
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, lambda u, c: None))

        print("✅ Bot de Telegram iniciando polling. ¡Listo!")
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        print(f"❌ ERROR CRÍTICO al iniciar el Bot de Telegram. Error: {e}")