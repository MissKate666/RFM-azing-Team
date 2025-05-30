# === Bot.py: Основной модуль Telegram-бота для RFM-анализа и рекомендаций ===
# Этот файл реализует функциональность бота: работа с БД, анализом данных, языками и интерфейсом.

import logging
import os
import psycopg2
import pandas as pd
import aiohttp
from datetime import datetime, timedelta
import pytz
from geopy.geocoders import Nominatim
from timezonefinder import TimezoneFinder
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters
from telegram.error import BadRequest, RetryAfter
import smtplib
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
import RFM  # Импорт модуля RFM.py для анализа RFM
from Model import Gemini
import re
from VectorSearch import init_recommendations, find_recommendations
from telegram.error import RetryAfter, BadRequest


# === Конфигурация токена и базы данных ===
# Устанавливаются ключевые параметры: Telegram-токен, данные для подключения к PostgreSQL
# --- Настройки бота ---
# Токен Telegram бота и параметры подключения к базе данных
TOKEN = '8062418222:AAEdIVP15v0jMN11BeoWo1LG0i6WWceAGb8'
DB_PARAMS = {
    "dbname": "RFM_Databases",
    "user": "postgres",
    "password": "Ip1011228",
    "host": "localhost",
    "port": 5432
}
DEVELOPER_EMAIL = "katrin.planshet@gmail.com"  # Email для обратной связи
SMTP_EMAIL = "katrin.planshet@gmail.com"  # Email для отправки писем
SMTP_PASSWORD = "cgqg yiem lnib klqv"  # Пароль для SMTP (нужен App Password для Gmail)


# === Интерфейс и локализация ===
# Определены переводы сообщений для поддержки разных языков (RU/EN)
# --- Словарь локализаций ---
# Поддерживаемые языки (ru, en) с текстами для интерфейса
LANGUAGES = {
    'ru': {
        'welcome': "👋 Привет, {name}! Я бот для анализа данных. Выберите действие:",
        'about':  "👥 Команда \"RFM-azing Team\" создает интеллектуального помощника, "
            "умеющего глубоко анализировать взаимодействие с клиентами и давать полезные советы для роста вашей прибыли.",
        'help': "📌 Команды:\n"
            "• 📊 — анализ CSV: загрузите файлы для глубокого анализа клиентских данных.\n"
            "• 🕓 — история: просмотрите предыдущие запросы и результаты.\n"
            "• 🗑 — очистка: очистите историю ваших действий.\n"
            "• 🌐 — смена языка: поменяйте ваш язык с русского на английский.\n"
            "• ♻️ — обратная связь: поделитесь своими предложениями и замечаниями.\n\n"
            "📞 Контакты:\n"
            "- Тимлид: dinizavrik.tata@gmail.com\n"
            "- Ассистент: Kseniavasilchenko@mail.ru",
        'history_empty': "История пуста.",
        'history': "📜 История переписки:",
        'confirm_clear': "⚠️ Удалить переписку? Это необратимо.",
        'cleared': "✅ Переписка удалена.",
        'cancel_clear': "❎ Очистка отменена.",
        'spam': "⏳ Подождите перед следующим сообщением.",
        'mute_message': "🚫 Вы замьючены за спам (30 сек).",
        'csv_request': "📁 Загрузите CSV-файл для RFM-анализа.",
        'csv_success': "✅ Результаты RFM-анализа:",
        'csv_loaded': "✅ Данные загружены:",
        'csv_format_choice': "Файл загружен. Выберите, как посмотреть результаты:",
        'table_button': "Таблица",
        'diagram_button': "Диаграмма",
        'text_button': "Текст",
        'csv_error': "❗ Ошибка обработки файла:",
        'use_buttons': "❗ Используйте кнопки.",
        'yes_button': "✅ Да",
        'no_button': "❌ Нет",
        'cancel_button': "❌ Отмена",
        'cancel_timezone': "❎ Ввод города отменён.",
        'set_city': "История хранится в московском часовом поясе, чтобы это изменить, укажите город:",
        'keep_moscow': "Оставить московский часовой пояс",
        'invalid_city': "❗ Не удалось определить часовой пояс для города: {city}. Попробуйте снова.",
        'change_timezone': "Поменять часовой пояс",
        'filter_day': "По дням",
        'filter_week': "По неделям",
        'filter_month': "По месяцам",
        'timezone_set': "✅ Часовой пояс: {timezone}.",
        'error_message': "❗ Ошибка, попробуйте снова позже.",
        'db_error': "❗ Ошибка базы данных. Попробуйте позже.",
        'geocoder_error': "❗ Сервис часовых поясов недоступен. Попробуйте снова или выберите московский пояс.",
        'retry_message': "❗ Слишком много запросов. Подождите {seconds} сек.",
        'corrections_message': "✏️ Исправления в данных:",
        'next_page': "▶ Следующая",
        'prev_page': "◀ Предыдущая",
        'feedback_request': "📝 Напишите ваше мнение или предложение о работе бота.\n⚠️ Внимание: ваше сообщение будет отправлено разработчикам на почту.",
        'feedback_sent': "✅ Спасибо! Ваше сообщение отправлено разработчикам.",
        'feedback_error': "❗ Ошибка при отправке сообщения. Попробуйте позже.",
        'language_changed': "✅ Язык изменён на {lang}.",
        'menu': [["📊 Оценка клиентов бизнеса"], ["ℹ️ О нас", "🆘 Помощь"], ["🕓 История", "🗑 Очистка"],
                 ["🌐 Предложения и пожелания", "🌐 Сменить язык / Change Language"]]
    },
    'en': {
        'welcome': "👋 Hi, {name}! I'm a data analysis bot. Choose an option:",
        'about': "👥 The \"RFM-azing Team\" develops an intelligent assistant capable of analyzing customer interactions "
            "and providing actionable insights to increase your business profits.",
        'help': "📌 Commands:\n"
            "• 📊 — CSV Analysis: upload files for deep customer data analysis.\n"
            "• 🕓 — History: review previous queries and results.\n"
            "• 🗑 — Clear: clean up your activity history.\n"
            "• 🌐 — Language change: Changes the language from English to Russian.\n"
            "• ♻️ — Feedback: share your suggestions and comments.\n\n"
            "📞 Contacts:\n"
            "- Team Lead: dinizavrik.tata@gmail.com\n"
            "- Assistant: Kseniavasilchenko@mail.ru",
        'history_empty': "No history found.",
        'history': "📜 Chat history:",
        'confirm_clear': "⚠️ Delete chat history? This cannot be undone.",
        'cleared': "✅ Chat history deleted.",
        'cancel_clear': "❎ Deletion canceled.",
        'spam': "⏳ Wait before sending another message.",
        'mute_message': "🚫 Muted for spamming (30 sec).",
        'csv_request': "📁 Upload a CSV file for RFM analysis.",
        'csv_success': "✅ RFM Analysis Results:",
        'csv_loaded': "✅ Data loaded:",
        'csv_format_choice': "File uploaded. Choose how to view the results:",
        'table_button': "Table",
        'diagram_button': "Diagram",
        'text_button': "Text",
        'csv_error': "❗ File processing error:",
        'use_buttons': "❗ Use the buttons.",
        'yes_button': "✅ Yes",
        'no_button': "❌ No",
        'cancel_button': "❌ Cancel",
        'cancel_timezone': "❎ City input canceled.",
        'set_city': "History is stored in Moscow time. To store it in your timezone, enter your city:",
        'keep_moscow': "Keep Moscow timezone",
        'invalid_city': "❗ Could not determine timezone for city: {city}. Try again.",
        'change_timezone': "Change timezone",
        'filter_day': "By days",
        'filter_week': "By weeks",
        'filter_month': "By months",
        'timezone_set': "✅ Timezone set: {timezone}.",
        'error_message': "❗ An error occurred, please try again later.",
        'db_error': "❗ Database connection error. Please try again later.",
        'geocoder_error': "❗ Timezone service unavailable. Try again or choose Moscow timezone.",
        'retry_message': "❗ Too many requests. Wait {seconds} seconds and try again.",
        'corrections_message': "✏️ Data corrections:",
        'next_page': "▶ Next",
        'prev_page': "◀ Previous",
        'feedback_request': "📝 Write your feedback or suggestion about the bot.\n⚠️ Warning: Your message will be sent to developers.",
        'feedback_sent': "✅ Thank you! Your message has been sent to the developers.",
        'feedback_error': "❗ Error sending your message. Please try again later.",
        'language_changed': "✅ Language changed to {lang}.",
        'menu': [["📊 Customer Business Evaluation"], ["ℹ️ About Us", "🆘 Help"], ["🕓 History", "🗑 Clear"],
                 ["🌐 Suggestions and Feedback", "🌐 Сменить язык / Change Language"]]
    }
}


# === Логирование ===
# Включает запись логов для отслеживания работы бота и ошибок
# --- Настройка логирования ---
# Инициализация логирования для отслеживания работы бота
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# === Работа с PostgreSQL ===
# Содержит функции подключения, инициализации, чтения и записи в базу данных
# --- Работа с базой данных ---
# Подключение к PostgreSQL
def get_db_connection():
    try:
        return psycopg2.connect(**DB_PARAMS)
    except psycopg2.Error as e:
        logger.error(f"Ошибка подключения к базе данных: {e}")
        raise

# Инициализация таблиц базы данных
def init_db():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Создание таблиц для пользователей, истории сообщений, заблокированных пользователей и времени последнего сообщения
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    timezone TEXT DEFAULT 'Europe/Moscow',
                    timezone_set BOOLEAN DEFAULT FALSE
                );
                CREATE TABLE IF NOT EXISTS messages_history (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    message TEXT,
                    timestamp TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS muted_users (
                    user_id BIGINT PRIMARY KEY,
                    mute_time TIMESTAMP,
                    unmute_time TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS last_message_time (
                    user_id BIGINT PRIMARY KEY,
                    last_time TIMESTAMP
                );
            """)
            conn.commit()
        logger.info("База данных инициализирована")

# Сохранение данных пользователя в базу
def save_user(user_id, username, first_name, timezone='Europe/Moscow', timezone_set=False):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users (id, username, first_name, timezone, timezone_set)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET username = EXCLUDED.username, first_name = EXCLUDED.first_name,
                    timezone = EXCLUDED.timezone, timezone_set = EXCLUDED.timezone_set
            """, (user_id, username, first_name, timezone, timezone_set))
            conn.commit()

# Сохранение сообщения пользователя в историю
def save_message(user_id, message):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO messages_history (user_id, message, timestamp) VALUES (%s, %s, %s)",
                    (user_id, message, datetime.now(pytz.UTC))
                )
                conn.commit()
        logger.info(f"Сообщение сохранено для user_id {user_id}: {message}")
    except psycopg2.Error as e:
        logger.error(f"Ошибка при сохранении сообщения: {e}")

# Получение истории сообщений пользователя
def get_user_history(user_id):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT timestamp, message FROM messages_history WHERE user_id = %s ORDER BY timestamp DESC",
                        (user_id,))
            return cur.fetchall()

# Очистка истории сообщений пользователя
def clear_history(user_id):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM messages_history WHERE user_id = %s", (user_id,))
            conn.commit()

# Проверка, заблокирован ли пользователь за спам
def is_muted(user_id):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM muted_users WHERE user_id = %s AND unmute_time < %s",
                        (user_id, datetime.now(pytz.UTC)))
            cur.execute("SELECT unmute_time FROM muted_users WHERE user_id = %s", (user_id,))
            result = cur.fetchone()
            conn.commit()
            return result and result[0].replace(tzinfo=pytz.UTC) > datetime.now(pytz.UTC)

# Блокировка пользователя за спам
def mute_user(user_id, duration=30):
    mute_time = datetime.now(pytz.UTC)
    unmute_time = mute_time + timedelta(seconds=duration)
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO muted_users (user_id, mute_time, unmute_time) VALUES (%s, %s, %s)",
                        (user_id, mute_time, unmute_time))
            conn.commit()
    logger.info(f"Пользователь {user_id} заблокирован до {unmute_time}")

# Подсчет количества сообщений за последние 30 секунд
def get_recent_message_count(user_id):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM messages_history WHERE user_id = %s AND timestamp >= NOW() - INTERVAL '30 seconds'",
                (user_id,))
            return cur.fetchone()[0]

# Обновление времени последнего сообщения пользователя
def update_last_message_time(user_id):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO last_message_time (user_id, last_time) VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET last_time = %s",
                (user_id, datetime.now(pytz.UTC), datetime.now(pytz.UTC)))
            conn.commit()

# Получение часового пояса пользователя
def get_user_timezone(user_id):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT timezone, timezone_set FROM users WHERE id = %s", (user_id,))
            result = cur.fetchone()
            return result[0] if result else 'Europe/Moscow', result[1] if result else False


# === Язык и часовой пояс пользователя ===
# Позволяет управлять языковыми настройками и использовать локальное время
# --- Управление языком и часовым поясом ---
# Получение текущего языка пользователя
def get_language(context, user_id):
    lang = context.user_data.get("lang", 'ru')
    if lang not in LANGUAGES:
        logger.warning(f"Недопустимый язык {lang} для пользователя {user_id}, устанавливается 'ru'")
        context.user_data["lang"] = 'ru'
        return 'ru'
    return lang

# Установка языка интерфейса
def set_language(context, lang, user_name):
    if lang in LANGUAGES:
        context.user_data["lang"] = lang
        logger.info(f"Язык установлен на {lang} для пользователя {context._user_id}")
        lang_name = "Русский" if lang == 'ru' else "English"
        return LANGUAGES[lang]['language_changed'].format(lang=lang_name), LANGUAGES[lang]['welcome'].format(
            name=user_name)
    return None, None

# Определение часового пояса по названию города
def get_timezone_by_city(city):
    try:
        geolocator = Nominatim(user_agent="telegram_bot")
        location = geolocator.geocode(city)
        if not location:
            return None
        tf = TimezoneFinder()
        timezone_name = tf.timezone_at(lat=location.latitude, lng=location.longitude)
        if timezone_name:
            pytz.timezone(timezone_name)
            return timezone_name
        return None
    except Exception as e:
        logger.error(f"Ошибка определения часового пояса для города {city}: {e}")
        raise


# === Вспомогательные утилиты ===
# Дополнительные функции, такие как разбиение сообщений и проверка ключей
# --- Вспомогательные функции ---
# Проверка наличия всех ключей локализации
def ensure_language_keys(reply, lang):
    for key in LANGUAGES['ru']:
        if key not in reply:
            reply[key] = LANGUAGES['ru'][key]
    return reply

# Разделение длинного сообщения на части для отправки в Telegram
def split_message(message, max_length=4096):
    parts, current_part, open_tags, i = [], "", [], 0
    while i < len(message):
        char = message[i]
        current_part += char
        if char == '<':
            tag = ""
            j = i
            while j < len(message) and message[j] != '>':
                tag += message[j]
                j += 1
            if j < len(message):
                tag += '>'
                i = j
                if tag.startswith('</'):
                    if open_tags and open_tags[-1] == tag.replace('</', '<'):
                        open_tags.pop()
                elif not tag.endswith('/>'):
                    open_tags.append(tag)
        if len(current_part) >= max_length:
            last_newline = current_part.rfind('\n', 0, max_length)
            split_pos = last_newline if last_newline != -1 else max_length
            parts.append(current_part[:split_pos])
            current_part = current_part[split_pos:]
        i += 1
    if current_part:
        parts.append(current_part)
    final_parts = []
    for part in parts:
        temp_open_tags = open_tags.copy()
        part_with_tags = part
        for tag in reversed(temp_open_tags):
            part_with_tags += tag.replace('<', '</').replace('>', '>')
        final_parts.append(part_with_tags)
    return final_parts

# Отправка обратной связи на email разработчиков
async def send_feedback_email(user_id, username, feedback):
    msg = MIMEText(f"Feedback from User ID: {user_id} (Username: {username})\n\n{feedback}")
    msg['Subject'] = f"Feedback from {username or 'Anonymous'}"
    msg['From'] = SMTP_EMAIL
    msg['To'] = DEVELOPER_EMAIL
    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(SMTP_EMAIL, SMTP_PASSWORD)
            server.send_message(msg)
    except smtplib.SMTPAuthenticationError as e:
        logger.error(f"Ошибка аутентификации SMTP: {e}")
        raise Exception("Authentication failed. Check your email and password.")
    except Exception as e:
        logger.error(f"Ошибка отправки email: {e}")
        raise


# === Обработка команд Telegram ===
# Реализация логики команд /start, /recommend и других текстовых/документных запросов
# --- Обработчики команд и сообщений ---
# Обработчик команды /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    lang = get_language(context, user.id)
    try:
        save_user(user.id, user.username, user.first_name)
        await update.message.reply_text(
            LANGUAGES[lang]['welcome'].format(name=user.first_name),
            reply_markup=ReplyKeyboardMarkup(LANGUAGES[lang]['menu'], resize_keyboard=True)
        )
    except psycopg2.Error:
        await update.message.reply_text(LANGUAGES[lang]['db_error'])
    except RetryAfter as e:
        await update.message.reply_text(LANGUAGES[lang]['retry_message'].format(seconds=e.retry_after))
    except BadRequest as e:
        logger.error(f"BadRequest в start: {e}")
        await update.message.reply_text(LANGUAGES[lang]['error_message'])

# Отмена ввода города для часового пояса
async def cancel_timezone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    lang = get_language(context, user.id)
    context.user_data["awaiting_city"] = False
    try:
        await update.callback_query.message.reply_text(
            LANGUAGES[lang]['cancel_timezone'],
            reply_markup=ReplyKeyboardMarkup(LANGUAGES[lang]['menu'], resize_keyboard=True)
        )
        await update.callback_query.message.delete()
    except RetryAfter as e:
        await update.callback_query.message.reply_text(LANGUAGES[lang]['retry_message'].format(seconds=e.retry_after))
    except BadRequest as e:
        logger.error(f"BadRequest в cancel_timezone: {e}")
        await update.callback_query.message.reply_text(LANGUAGES[lang]['error_message'])

# Запрос смены часового пояса
async def change_timezone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    lang = get_language(context, user.id)
    context.user_data["awaiting_city"] = True
    try:
        await update.callback_query.message.reply_text(
            LANGUAGES[lang]['set_city'],
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(LANGUAGES[lang]['keep_moscow'], callback_data='keep_moscow')],
                [InlineKeyboardButton(LANGUAGES[lang]['cancel_button'], callback_data='cancel_timezone')]
            ])
        )
        await update.callback_query.message.delete()
    except RetryAfter as e:
        await update.callback_query.message.reply_text(LANGUAGES[lang]['retry_message'].format(seconds=e.retry_after))
    except BadRequest as e:
        logger.error(f"BadRequest в change_timezone: {e}")
        await update.callback_query.message.reply_text(LANGUAGES[lang]['error_message'])

# Установка московского часового пояса
async def keep_moscow_timezone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    lang = get_language(context, user.id)
    context.user_data["awaiting_city"] = False
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET timezone = %s, timezone_set = %s WHERE id = %s",
                            ('Europe/Moscow', True, user.id))
                conn.commit()
        await update.callback_query.message.reply_text(LANGUAGES[lang]['timezone_set'].format(timezone='Europe/Moscow'))
        await show_history_with_filter(update, context, filter_type=None)
        try:
            await update.callback_query.message.delete()
        except BadRequest as e:
            logger.warning(f"Failed to delete message: {e}")
    except psycopg2.Error:
        await update.callback_query.message.reply_text(LANGUAGES[lang]['db_error'])
    except RetryAfter as e:
        await update.callback_query.message.reply_text(LANGUAGES[lang]['retry_message'].format(seconds=e.retry_after))
    except BadRequest as e:
        logger.error(f"BadRequest в keep_moscow_timezone: {e}")
        await update.callback_query.message.reply_text(LANGUAGES[lang]['error_message'])

# Отображение истории сообщений с фильтрацией
async def show_history_with_filter(update: Update, context: ContextTypes.DEFAULT_TYPE, filter_type=None):
    user = update.effective_user
    lang = get_language(context, user.id)
    reply = ensure_language_keys(LANGUAGES[lang], lang)
    message = update.callback_query.message if update.callback_query else update.message

    # Настройка пагинации
    page_size = 10
    current_page = context.user_data.get('history_page', 0)

    try:
        history = get_user_history(user.id)
        if not history:
            await message.reply_text(reply['history_empty'])
            if update.callback_query:
                try:
                    await update.callback_query.message.delete()
                except BadRequest as e:
                    logger.warning(f"Failed to delete message: {e}")
            return

        tz = pytz.timezone(get_user_timezone(user.id)[0])
        lines, current_group = [], None
        total_messages = len(history)
        start_idx = current_page * page_size
        end_idx = min((current_page + 1) * page_size, total_messages)

        for ts, msg in history[start_idx:end_idx]:
            if not ts.tzinfo:
                ts = ts.replace(tzinfo=pytz.UTC)
            local_time = ts.astimezone(tz)
            group_key = None
            if filter_type == 'day':
                group_key = local_time.strftime('%Y-%m-%d')
            elif filter_type == 'week':
                group_key = local_time.strftime('Week %U, %Y' if lang == 'en' else 'Неделя %U, %Y')
            elif filter_type == 'month':
                group_key = local_time.strftime('%B %Y')
            if group_key and group_key != current_group:
                lines.append(f"\n<b>{group_key}</b>")
                current_group = group_key
            formatted_time = local_time.strftime('%Y-%m-%d %H:%M:%S')
            msg = "\n".join([msg[i:i + 50] for i in range(0, len(msg), 50)]) if len(msg) > 50 else msg
            lines.append(f"{formatted_time}\n{msg}")

        if not lines:
            await message.reply_text(reply['history_empty'])
            if update.callback_query:
                try:
                    await update.callback_query.message.delete()
                except BadRequest as e:
                    logger.warning(f"Failed to delete message: {e}")
            return

        full_message = f"{reply['history']} (Страница {current_page + 1} из {((total_messages - 1) // page_size) + 1})\n\n" + "\n\n".join(
            lines)
        message_parts = split_message(full_message)

        # Кнопки пагинации и фильтров
        keyboard = [
            [InlineKeyboardButton(reply['filter_day'], callback_data='filter_day'),
             InlineKeyboardButton(reply['filter_week'], callback_data='filter_week'),
             InlineKeyboardButton(reply['filter_month'], callback_data='filter_month')],
            [InlineKeyboardButton(reply['change_timezone'], callback_data='change_timezone')]
        ]
        if current_page > 0:
            keyboard.append([InlineKeyboardButton(reply['prev_page'], callback_data='prev_page')])
        if end_idx < total_messages:
            keyboard.append([InlineKeyboardButton(reply['next_page'], callback_data='next_page')])
        keyboard = InlineKeyboardMarkup(keyboard)

        for part in message_parts:
            await message.reply_text(part, parse_mode='HTML', reply_markup=keyboard)
        if update.callback_query:
            try:
                await update.callback_query.message.delete()
            except BadRequest as e:
                logger.warning(f"Failed to delete message: {e}")

    except psycopg2.Error:
        await message.reply_text(reply['db_error'])
        if update.callback_query:
            try:
                await update.callback_query.message.delete()
            except BadRequest as e:
                logger.warning(f"Failed to delete message: {e}")
    except RetryAfter as e:
        await message.reply_text(reply['retry_message'].format(seconds=e.retry_after))
    except BadRequest as e:
        logger.error(f"BadRequest в show_history_with_filter: {e}")
        await message.reply_text(reply['error_message'])

# Обработка разбиение истории
async def handle_pagination(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    lang = get_language(context, user.id)
    query = update.callback_query
    reply = ensure_language_keys(LANGUAGES[lang], lang)

    if query.data == 'next_page':
        context.user_data['history_page'] = context.user_data.get('history_page', 0) + 1
    elif query.data == 'prev_page':
        context.user_data['history_page'] = max(0, context.user_data.get('history_page', 0) - 1)

    await query.answer()
    await show_history_with_filter(update, context, filter_type=context.user_data.get('history_filter'))

# Векторный поиск. Рекомендации
async def recommend(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    lang = get_language(context, user.id)
    reply = ensure_language_keys(LANGUAGES[lang], lang)
    query = update.message.text.replace('/recommend', '').strip()

    # Сохраняем запрос пользователя
    save_message(user.id, f"/recommend {query}")

    if not query:
        response = reply['ru']['feedback_request'] if lang == 'ru' else reply['en']['feedback_request']
        await update.message.reply_text(response,
                                        reply_markup=ReplyKeyboardMarkup(LANGUAGES[lang]['menu'], resize_keyboard=True))
        save_message(user.id, response)
        return

    try:
        # Используем векторный поиск с учетом языка
        vector_results = find_recommendations(query, lang)

        if vector_results:
            recommendations_text = "\n".join([f"{r[0]}: {r[1]}" for r in vector_results])
            gemini_prompt = (
                f"{'Пользователь запросил рекомендации по бизнесу' if lang == 'ru' else 'User requested business recommendations'}: '{query}'.\n"
                f"{'Вот предварительные рекомендации из базы' if lang == 'ru' else 'Here are preliminary recommendations from the database'}:\n{recommendations_text}\n"
                f"{'Уточни и дополни эти рекомендации, чтобы они были максимально полезны и конкретны для бизнеса.' if lang == 'ru' else 'Refine and enhance these recommendations to be as helpful and specific as possible for the business.'}"
            )
            enhanced_recommendations = Gemini(gemini_prompt)
            await update.message.reply_text(
                enhanced_recommendations,
                parse_mode='HTML',
                reply_markup=ReplyKeyboardMarkup(LANGUAGES[lang]['menu'], resize_keyboard=True)
            )
            # Сохраняем ответ бота
            save_message(user.id, enhanced_recommendations)
        else:
            gemini_prompt = (
                f"{'Пользователь запросил рекомендации по бизнесу' if lang == 'ru' else 'User requested business recommendations'}: '{query}'.\n"
                f"{'Предложи конкретные и практические рекомендации для улучшения бизнеса.' if lang == 'ru' else 'Provide specific and practical recommendations for improving the business.'}"
            )
            enhanced_recommendations = Gemini(gemini_prompt)
            await update.message.reply_text(
                enhanced_recommendations,
                parse_mode='HTML',
                reply_markup=ReplyKeyboardMarkup(LANGUAGES[lang]['menu'], resize_keyboard=True)
            )
            # Сохраняем ответ бота
            save_message(user.id, enhanced_recommendations)
    except Exception as e:
        logger.error(f"Ошибка в recommend: {e}")
        error_message = f"{reply['error_message']} {str(e)}"
        await update.message.reply_text(error_message,
                                        reply_markup=ReplyKeyboardMarkup(LANGUAGES[lang]['menu'], resize_keyboard=True))
        save_message(user.id, error_message)

def boldify(text):
    # Заменяет каждую пару **...** на <b>...</b>
    return re.sub(r"\*\*(.*?)\*\*", r'<b>\1</b>', text, flags=re.DOTALL)

# Обработка выбора формата вывода результатов RFM
async def handle_result_format(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    lang = get_language(context, user.id)
    reply = ensure_language_keys(LANGUAGES[lang], lang)
    query = update.callback_query
    keyboard = ReplyKeyboardMarkup(LANGUAGES[lang]['menu'], resize_keyboard=True)

    # Получение результата RFM-анализа из контекста
    result = context.user_data.get('rfm_result', {})
    format_choice = query.data  # 'table', 'diagram', или 'text'



    try:
        # Формируем основной контент в зависимости от формата
        if format_choice == 'table' and result.get('result_table'):
            response = f"{reply['csv_success']}```{result['result_table']}```"
            await query.message.reply_text(response, parse_mode='Markdown', reply_markup=keyboard)
            save_message(user.id, response)
        elif format_choice == 'diagram' and result.get('plot_path') and os.path.exists(result['plot_path']):
            with open(result['plot_path'], 'rb') as plot_file:
                await query.message.reply_photo(photo=plot_file, caption="График сегментов RFM" if lang == 'ru' else "RFM Segments Chart", reply_markup=keyboard)
                save_message(user.id, "График сегментов RFM" if lang == 'ru' else "RFM Segments Chart")
        elif format_choice == 'text' and result.get('result_text'):
            await query.message.reply_text(result['result_text'], parse_mode='HTML', reply_markup=keyboard)
            save_message(user.id, result['result_text'])

        # Добавляем исправления, если есть
        if result.get('corrections'):
            response = f"{reply['corrections_message']}```{result['corrections']}```"
            await query.message.reply_text(response, parse_mode='Markdown', reply_markup=keyboard)
            save_message(user.id, response)

        # Показываем ошибки, если они есть
        if result.get('errors'):
            response = f"{reply['csv_error']}```{result['errors']}```"
            await query.message.reply_text(response, parse_mode='Markdown', reply_markup=keyboard)
            save_message(user.id, response)

        # Находим доминирующий сегмент и выдаем рекомендации
        if result.get('result_text'):
            segment_counts = {}
            segments_ru = ['VIP-клиенты', 'Лояльные клиенты', 'Новые покупатели', 'Рискующие клиенты', 'Спящие клиенты']
            segments_en = ['VIP Customers', 'Loyal Customers', 'New Customers', 'At-Risk Customers', 'Lost Customers']
            segments = segments_en if lang == 'en' else segments_ru

            for segment in segments:
                match = re.search(rf"{segment}:\n\s+{'Клиентов' if lang == 'ru' else 'Customers'}: (\d+)", result['result_text'])
                if match:
                    segment_counts[segment] = int(match.group(1))

            if segment_counts:
                dominant_segment = max(segment_counts, key=segment_counts.get)
                recommendation_query = f"{'Рекомендации для' if lang == 'ru' else 'Recommendations for'} {dominant_segment.lower()} in a mass-market retail"
                vector_results = find_recommendations(recommendation_query, lang, dominant_segment)

                if vector_results:
                    recommendations_text = "\n".join([r[1] for r in vector_results])
                    gemini_prompt = (
                        f"{'RFM-анализ' if lang == 'ru' else 'RFM analysis'}:\n"
                        f"{'Доминирующий сегмент' if lang == 'ru' else 'Dominant segment'}: {dominant_segment}\n"
                        f"{'Предварительные рекомендации' if lang == 'ru' else 'Preliminary recommendations'}:\n{recommendations_text}\n"
                        f"{'Уточни и дополни рекомендации для бизнеса масс-маркета. Пиши не очень много' if lang == 'ru' else 'Refine and provide actionable recommendations for a mass-market retail business.'}"
                    )
                    enhanced_recommendations = Gemini(gemini_prompt)
                    response = f"{'Рекомендации для' if lang == 'ru' else 'Recommendations for'} {dominant_segment}:\n{boldify(enhanced_recommendations)}"
                    await query.message.reply_text(response, parse_mode='HTML', reply_markup=keyboard)
                    save_message(user.id, response)
                else:
                    gemini_prompt = (
                        f"{'RFM-анализ' if lang == 'ru' else 'RFM analysis'}:\n"
                        f"{'Доминирующий сегмент' if lang == 'ru' else 'Dominant segment'}: {dominant_segment}\n"
                        f"{'Предложи рекомендации для бизнеса масс-маркета (пиши не очень много)' if lang == 'ru' else 'Provide actionable recommendations for a mass-market retail business.'}"
                    )

                    enhanced_recommendations = Gemini(gemini_prompt)
                    response = f"{'Рекомендации для' if lang == 'ru' else 'Recommendations for'} {dominant_segment}:\n{boldify(enhanced_recommendations)}"

                    await query.message.reply_text(response, parse_mode='HTML', reply_markup=keyboard)
                    save_message(user.id, response)

        # Очистка временных файлов
        if result.get('plot_path') and os.path.exists(result['plot_path']):
            os.remove(result['plot_path'])
        context.user_data['rfm_result'] = None  # Исправлено присваивание

        # Удаление сообщения с кнопками
        try:
            await query.message.delete()
        except BadRequest as e:
            logger.warning(f"Ошибка при удалении сообщения: {e}")

    except RetryAfter as e:
        response = reply['retry_message'].format(seconds=e.retry_after)
        await query.message.reply_text(response, reply_markup=keyboard)
        save_message(user.id, response)
    except BadRequest as e:
        logger.error(f"BadRequest в handle_result_format: {e}")
        response = reply['error_message']
        await query.message.reply_text(response, reply_markup=keyboard)
        save_message(user.id, response)
    except Exception as e:
        logger.error(f"Ошибка в handle_result_format: {e}")
        response = f"{reply['csv_error']} {str(e)}"
        await query.message.reply_text(response, reply_markup=keyboard)
        save_message(user.id, response)

# Обработка текстовых сообщений
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    lang = get_language(context, user.id)
    text, reply = update.message.text, ensure_language_keys(LANGUAGES[lang], lang)
    keyboard = ReplyKeyboardMarkup(LANGUAGES[lang]['menu'], resize_keyboard=True)
    try:
        if is_muted(user.id):
            await update.message.reply_text(reply['mute_message'], reply_markup=keyboard)
            return
        save_message(user.id, text)
        update_last_message_time(user.id)
        if get_recent_message_count(user.id) > 15:
            mute_user(user.id)
            await update.message.reply_text(reply['mute_message'], reply_markup=keyboard)
            return
    except psycopg2.Error:
        await update.message.reply_text(reply['db_error'], reply_markup=keyboard)
        return
    if context.user_data.get("awaiting_city", False):
        try:
            timezone_name = get_timezone_by_city(text)
            if timezone_name:
                context.user_data["custom_timezone"] = timezone_name
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("UPDATE users SET timezone = %s, timezone_set = %s WHERE id = %s",
                                    (timezone_name, True, user.id))
                        conn.commit()
                await update.message.reply_text(reply['timezone_set'].format(timezone=timezone_name))
                context.user_data["awaiting_city"] = False
                await show_history_with_filter(update, context)
            else:
                await update.message.reply_text(
                    reply['invalid_city'].format(city=text),
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton(reply['keep_moscow'], callback_data='keep_moscow')],
                        [InlineKeyboardButton(reply['cancel_button'], callback_data='cancel_timezone')]
                    ])
                )
        except Exception:
            await update.message.reply_text(
                reply['geocoder_error'],
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(reply['keep_moscow'], callback_data='keep_moscow')],
                    [InlineKeyboardButton(reply['cancel_button'], callback_data='cancel_timezone')]
                ])
            )
        return
    if context.user_data.get("awaiting_feedback", False):
        try:
            await send_feedback_email(user.id, user.username, text)
            await update.message.reply_text(reply['feedback_sent'], reply_markup=keyboard)
            context.user_data["awaiting_feedback"] = False
        except Exception:
            await update.message.reply_text(reply['feedback_error'], reply_markup=keyboard)
        return
    if context.user_data.get("awaiting_csv", False):
        await update.message.reply_text(reply['csv_request'], reply_markup=keyboard)
        return
    actions = {
        "📊 Оценка клиентов бизнеса": lambda: (reply['csv_request'], None),
        "📊 Customer Business Evaluation": lambda: (reply['csv_request'], None),
        "ℹ️ О нас": lambda: reply['about'],
        "ℹ️ About Us": lambda: reply['about'],
        "🆘 Помощь": lambda: reply['help'],
        "🆘 Help": lambda: reply['help'],
        "🗑 Очистка": lambda: (reply['confirm_clear'],
                              ReplyKeyboardMarkup([[reply['yes_button'], reply['no_button']]], resize_keyboard=True,
                                                  one_time_keyboard=True)),
        "🗑 Clear": lambda: (reply['confirm_clear'],
                            ReplyKeyboardMarkup([[reply['yes_button'], reply['no_button']]], resize_keyboard=True,
                                                one_time_keyboard=True)),
        "🌐 Предложения и пожелания": lambda: (reply['feedback_request'].format(email=DEVELOPER_EMAIL), None),
        "🌐 Suggestions and Feedback": lambda: (reply['feedback_request'].format(email=DEVELOPER_EMAIL), None),
        "🌐 Сменить язык / Change Language": lambda: set_language(context, 'en' if lang == 'ru' else 'ru',
                                                                 user.first_name),
        reply['yes_button']: lambda: (clear_history(user.id) or reply['cleared'], keyboard),
        reply['no_button']: lambda: (reply['cancel_clear'], keyboard)
    }
    if text in ["🕓 История", "🕓 History"]:
        try:
            timezone, timezone_set = get_user_timezone(user.id)
            if not timezone_set:
                context.user_data["awaiting_city"] = True
                await update.message.reply_text(
                    reply['set_city'],
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton(reply['keep_moscow'], callback_data='keep_moscow')],
                        [InlineKeyboardButton(reply['cancel_button'], callback_data='cancel_timezone')]
                    ])
                )
            else:
                await show_history_with_filter(update, context)
        except psycopg2.Error:
            await update.message.reply_text(reply['db_error'], reply_markup=keyboard)
        return
    action = actions.get(text)
    if action:
        result = action()
        if isinstance(result, tuple):
            if result[1] is None and text in ["📊 Оценка клиентов бизнеса", "📊 Customer Business Evaluation"]:
                context.user_data["awaiting_csv"] = True
                await update.message.reply_text(result[0], reply_markup=keyboard)
            elif result[1] is None:
                context.user_data["awaiting_feedback"] = True
                await update.message.reply_text(result[0])
            elif result[1] and isinstance(result[1], str):  # Смена языка
                new_lang = get_language(context, user.id)
                new_keyboard = ReplyKeyboardMarkup(LANGUAGES[new_lang]['menu'], resize_keyboard=True)
                await update.message.reply_text(result[0], reply_markup=new_keyboard)
                await update.message.reply_text(result[1], reply_markup=new_keyboard)
            else:
                await update.message.reply_text(result[0], reply_markup=result[1])
        else:
            await update.message.reply_text(result, reply_markup=keyboard)
    else:
        await update.message.reply_text(boldify(Gemini(text+"(пиши не очень много")), parse_mode='HTML', reply_markup=keyboard)

# Обработка загруженных документов (CSV-файлов)
async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        await update.message.reply_text("Error: User not found.")
        return
    lang = get_language(context, user.id)
    reply = ensure_language_keys(LANGUAGES[lang], lang)
    keyboard = ReplyKeyboardMarkup(LANGUAGES[lang]['menu'], resize_keyboard=True)
    try:
        if is_muted(user.id):
            await update.message.reply_text(reply['mute_message'], reply_markup=keyboard)
            return
        doc = update.message.document
        if not doc.file_name.lower().endswith('.csv'):
            await update.message.reply_text(f"{reply['csv_error']} Файл должен быть в формате CSV.",
                                            reply_markup=keyboard)
            return
        file_name = doc.file_name or 'file.csv'
        save_message(user.id, f"File uploaded: {file_name}")
        update_last_message_time(user.id)
        file = await doc.get_file()
        file_path = os.path.join(os.getcwd(), file_name)
        async with aiohttp.ClientSession() as session:
            async with session.get(file.file_path) as response:
                if response.status == 200:
                    with open(file_path, 'wb') as f:
                        f.write(await response.read())
        if context.user_data.get("awaiting_csv", False):
            # Показ предпросмотра CSV
            df = pd.read_csv(file_path)
            preview = f"\n{df.head().to_string(index=False)}"
            await update.message.reply_text(f"{reply['csv_loaded']}```\n{preview}\n```", parse_mode='MarkdownV2')

            # Выполнение RFM-анализа через модуль RFM
            result = RFM.main(file_path)
            context.user_data["awaiting_csv"] = False
            context.user_data['rfm_result'] = result  # Сохранение результата для выбора формата

            # Запрос формата вывода результатов
            await update.message.reply_text(
                reply['csv_format_choice'],
                reply_markup=InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton(reply['table_button'], callback_data='table'),
                        InlineKeyboardButton(reply['diagram_button'], callback_data='diagram'),
                        InlineKeyboardButton(reply['text_button'], callback_data='text')
                    ]
                ])
            )
        else:
            # Простой предпросмотр CSV без анализа
            df = pd.read_csv(file_path)
            await update.message.reply_text(f"Первые 5 строк в вашем файле:```\n{f"\n{df.head().to_string(index=False)}"}\n```",
                                            parse_mode='MarkdownV2', reply_markup=keyboard)
        # Очистка временного файла
        if os.path.exists(file_path):
            os.remove(file_path)
    except psycopg2.Error:
        await update.message.reply_text(reply['db_error'], reply_markup=keyboard)
    except Exception as e:
        await update.message.reply_text(f"{reply['csv_error']} {str(e)}", reply_markup=keyboard)

# Обработчик ошибок
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Ошибка при обработке {update}: {context.error}", exc_info=context.error)
    user_id = update.effective_user.id if update.effective_user else 0
    lang = get_language(context, user_id)
    reply = ensure_language_keys(LANGUAGES[lang], lang)
    keyboard = ReplyKeyboardMarkup(LANGUAGES[lang]['menu'], resize_keyboard=True)
    try:
        target = update.message or update.callback_query.message
        await target.reply_text(reply['error_message'], reply_markup=keyboard)
    except RetryAfter as e:
        await target.reply_text(reply['retry_message'].format(seconds=e.retry_after))
    except BadRequest as e:
        logger.error(f"BadRequest в error_handler: {e}")
        await target.reply_text(reply['error_message'])


# === Точка входа ===
# Главная функция для инициализации бота и запуска в режиме polling
# --- Главная функция ---
# Инициализация и запуск бота
def main():
    init_db()
    init_recommendations()
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("recommend", recommend))  # Новая команда
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(CallbackQueryHandler(cancel_timezone, pattern='cancel_timezone'))
    app.add_handler(CallbackQueryHandler(keep_moscow_timezone, pattern='keep_moscow'))
    app.add_handler(CallbackQueryHandler(change_timezone, pattern='change_timezone'))
    app.add_handler(CallbackQueryHandler(show_history_with_filter, pattern='filter_(day|week|month)'))
    app.add_handler(CallbackQueryHandler(handle_pagination, pattern='prev_page|next_page'))
    app.add_handler(CallbackQueryHandler(handle_result_format, pattern='table|diagram|text'))
    app.add_error_handler(error_handler)
    logger.info("Бот запущен")
    app.run_polling()  # Запуск бота в режиме polling

if __name__ == '__main__':
    main()