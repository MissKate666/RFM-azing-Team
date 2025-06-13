# === VectorSearch.py: Модуль для векторного поиска рекомендаций ===
# Этот файл отвечает за подключение к базе данных PostgreSQL, загрузку модели для создания
# векторных представлений текста и поиск рекомендаций на основе запросов пользователя.

import logging
from sentence_transformers import SentenceTransformer
import psycopg2
from datetime import datetime
import pytz
from huggingface_hub import login

# --- Настройка логирования ---
# Инициализирует систему логирования для отслеживания работы модуля
logger = logging.getLogger(__name__)

# --- Авторизация в Hugging Face ---
# Устанавливает токен для доступа к моделям Hugging Face
HF_TOKEN = "ЗДЕСЬ_ВАШ_ТОКЕН"  # Токен для доступа к моделям
login(HF_TOKEN)
logger.info("Авторизация в Hugging Face выполнена.")

# --- Параметры подключения к базе данных ---
# Определяет настройки для подключения к PostgreSQL
DB_PARAMS = {
    "dbname": "RFM_Databases",
    "user": "postgres",
    "password": "ЗДЕСЬ_ПАРОЛЬ_ОТ_БД",
    "host": "localhost",
    "port": 5432
}

# --- Загрузка модели SentenceTransformer ---
# Попытка загрузки модели для создания векторных представлений текста
try:
    model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
    logger.info("Модель SentenceTransformer успешно загружена.")
except Exception as e:
    logger.error(f"Ошибка загрузки модели SentenceTransformer: {e}")
    raise RuntimeError("Не удалось загрузить модель SentenceTransformer. Проверьте интернет-соединение или авторизацию на Hugging Face.") from e

# === Функция get_db_connection ===
# Устанавливает соединение с базой данных PostgreSQL
def get_db_connection():
    """Подключение к базе данных PostgreSQL."""
    # --- Подключение к базе данных ---
    # Использует параметры DB_PARAMS для соединения
    try:
        return psycopg2.connect(**DB_PARAMS)
    except psycopg2.Error as e:
        logger.error(f"Ошибка подключения к базе данных: {e}")
        raise

# === Функция init_recommendations ===
# Инициализирует таблицу рекомендаций в базе данных
def init_recommendations():
    """Инициализация таблицы рекомендаций с векторными представлениями."""
    # --- Создание таблицы рекомендаций ---
    # Создаёт таблицу с предопределёнными рекомендациями для разных сегментов клиентов
    recommendations = [
        # --- Рекомендации для VIP-клиентов (русский) ---
        {'segment': 'VIP-клиенты', 'language': 'ru', 'recommendation': 'Запустите программу лояльности с кэшбэком для VIP-клиентов, предлагая 5–10% возврата на каждую покупку.'},
        {'segment': 'VIP-клиенты', 'language': 'ru', 'recommendation': 'Предлагайте эксклюзивные купоны на популярные товары только для VIP-клиентов.'},
        {'segment': 'VIP-клиенты', 'language': 'ru', 'recommendation': 'Отправьте push-уведомления с персонализированными предложениями для VIP-клиентов.'},
        {'segment': 'VIP-клиенты', 'language': 'ru', 'recommendation': 'Организуйте розыгрыш подарочных карт среди VIP-клиентов.'},
        {'segment': 'VIP-клиенты', 'language': 'ru', 'recommendation': 'Предоставьте приоритетную поддержку через чат для VIP-клиентов.'},
        {'segment': 'VIP-клиенты', 'language': 'ru', 'recommendation': 'Создайте флеш-распродажи для VIP-клиентов с доступом к товарам по сниженным ценам.'},
        # --- Рекомендации для VIP-клиентов (английский) ---
        {'segment': 'VIP Customers', 'language': 'en', 'recommendation': 'Launch a loyalty program with 5–10% cashback for VIP customers to encourage frequent purchases.'},
        {'segment': 'VIP Customers', 'language': 'en', 'recommendation': 'Offer exclusive coupons on popular products only for VIP customers.'},
        {'segment': 'VIP Customers', 'language': 'en', 'recommendation': 'Send personalized push notifications with offers tailored to VIP customers.'},
        {'segment': 'VIP Customers', 'language': 'en', 'recommendation': 'Organize a gift card raffle for VIP customers to boost engagement.'},
        {'segment': 'VIP Customers', 'language': 'en', 'recommendation': 'Provide priority support via chat for VIP customers.'},
        {'segment': 'VIP Customers', 'language': 'en', 'recommendation': 'Create flash sales for VIP customers with access to discounted products.'},

        # --- Рекомендации для лояльных клиентов (русский) ---
        {'segment': 'Лояльные клиенты', 'language': 'ru', 'recommendation': 'Внедрите накопительную систему баллов, где клиенты зарабатывают бонусы за каждую покупку.'},
        {'segment': 'Лояльные клиенты', 'language': 'ru', 'recommendation': 'Отправьте email с предложением бесплатной доставки на следующий заказ.'},
        {'segment': 'Лояльные клиенты', 'language': 'ru', 'recommendation': 'Создайте реферальную программу с бонусами за приглашение друзей.'},
        {'segment': 'Лояльные клиенты', 'language': 'ru', 'recommendation': 'Проводите акции “2+1” для лояльных клиентов.'},
        {'segment': 'Лояльные клиенты', 'language': 'ru', 'recommendation': 'Добавьте персонализированные рекомендации товаров на сайте.'},
        {'segment': 'Лояльные клиенты', 'language': 'ru', 'recommendation': 'Организуйте опрос о предпочтениях с бонусом за участие.'},
        # --- Рекомендации для лояльных клиентов (английский) ---
        {'segment': 'Loyal Customers', 'language': 'en', 'recommendation': 'Introduce a points-based loyalty system where customers earn bonuses for each purchase.'},
        {'segment': 'Loyal Customers', 'language': 'en', 'recommendation': 'Send an email offering free shipping on the next order.'},
        {'segment': 'Loyal Customers', 'language': 'en', 'recommendation': 'Create a referral program with bonuses for inviting friends.'},
        {'segment': 'Loyal Customers', 'language': 'en', 'recommendation': 'Run “2+1” promotions for loyal customers.'},
        {'segment': 'Loyal Customers', 'language': 'en', 'recommendation': 'Add personalized product recommendations on the website.'},
        {'segment': 'Loyal Customers', 'language': 'en', 'recommendation': 'Conduct a survey on preferences with a bonus for participation.'},

        # --- Рекомендации для новых покупателей (русский) ---
        {'segment': 'Новые покупатели', 'language': 'ru', 'recommendation': 'Отправьте приветственный email с 10% скидкой на первый заказ.'},
        {'segment': 'Новые покупатели', 'language': 'ru', 'recommendation': 'Предложите бесплатный пробник популярного продукта при первой покупке.'},
        {'segment': 'Новые покупатели', 'language': 'ru', 'recommendation': 'Запустите рекламу в соцсетях с акцентом на бестселлеры.'},
        {'segment': 'Новые покупатели', 'language': 'ru', 'recommendation': 'Добавьте всплывающее окно с подпиской на рассылку в обмен на купон.'},
        {'segment': 'Новые покупатели', 'language': 'ru', 'recommendation': 'Упростите процесс регистрации и оформления заказа.'},
        {'segment': 'Новые покупатели', 'language': 'ru', 'recommendation': 'Создайте видеогид по магазину для новых покупателей.'},
        # --- Рекомендации для новых покупателей (английский) ---
        {'segment': 'New Customers', 'language': 'en', 'recommendation': 'Send a welcome email with a 10% discount on the first order.'},
        {'segment': 'New Customers', 'language': 'en', 'recommendation': 'Offer a free sample of a popular product with the first purchase.'},
        {'segment': 'New Customers', 'language': 'en', 'recommendation': 'Launch social media ads focusing on bestsellers.'},
        {'segment': 'New Customers', 'language': 'en', 'recommendation': 'Add a pop-up for newsletter subscription in exchange for a coupon.'},
        {'segment': 'New Customers', 'language': 'en', 'recommendation': 'Simplify the registration and checkout process.'},
        {'segment': 'New Customers', 'language': 'en', 'recommendation': 'Create a video guide for the store for new customers.'},

        # --- Рекомендации для рискующих клиентов (русский) ---
        {'segment': 'Рискующие клиенты', 'language': 'ru', 'recommendation': 'Отправьте email с напоминанием о брошенной корзине и 5% скидкой.'},
        {'segment': 'Рискующие клиенты', 'language': 'ru', 'recommendation': 'Запустите SMS-кампанию с ограниченным по времени предложением.'},
        {'segment': 'Рискующие клиенты', 'language': 'ru', 'recommendation': 'Предложите бонусные баллы за покупку в течение недели.'},
        {'segment': 'Рискующие клиенты', 'language': 'ru', 'recommendation': 'Проведите ретаргетинг в соцсетях для рискующих клиентов.'},
        {'segment': 'Рискующие клиенты', 'language': 'ru', 'recommendation': 'Отправьте письмо с рекомендациями дополняющих товаров.'},
        {'segment': 'Рискующие клиенты', 'language': 'ru', 'recommendation': 'Улучшите клиентскую поддержку для оперативного решения вопросов.'},
        # --- Рекомендации для рискующих клиентов (английский) ---
        {'segment': 'At-Risk Customers', 'language': 'en', 'recommendation': 'Send an email reminding about an abandoned cart with a 5% discount.'},
        {'segment': 'At-Risk Customers', 'language': 'en', 'recommendation': 'Launch an SMS campaign with a time-limited offer.'},
        {'segment': 'At-Risk Customers', 'language': 'en', 'recommendation': 'Offer bonus points for a purchase within a week.'},
        {'segment': 'At-Risk Customers', 'language': 'en', 'recommendation': 'Run retargeting ads on social media for at-risk customers.'},
        {'segment': 'At-Risk Customers', 'language': 'en', 'recommendation': 'Send an email with recommendations for complementary products.'},
        {'segment': 'At-Risk Customers', 'language': 'en', 'recommendation': 'Improve customer support to quickly resolve issues.'},

        # --- Рекомендации для спящих клиентов (русский) ---
        {'segment': 'Спящие клиенты', 'language': 'ru', 'recommendation': 'Запустите email-кампанию “Мы скучаем!” с 15% скидкой.'},
        {'segment': 'Спящие клиенты', 'language': 'ru', 'recommendation': 'Отправьте опрос о причинах ухода с купоном за ответ.'},
        {'segment': 'Спящие клиенты', 'language': 'ru', 'recommendation': 'Проведите кампанию в соцсетях с новыми товарами.'},
        {'segment': 'Спящие клиенты', 'language': 'ru', 'recommendation': 'Предложите бесплатную доставку при заказе в течение месяца.'},
        {'segment': 'Спящие клиенты', 'language': 'ru', 'recommendation': 'Создайте серию писем с историями о бренде.'},
        {'segment': 'Спящие клиенты', 'language': 'ru', 'recommendation': 'Добавьте спящих клиентов в программу лояльности с бонусом.'},
        # --- Рекомендации для спящих клиентов (английский) ---
        {'segment': 'Lost Customers', 'language': 'en', 'recommendation': 'Launch a “We miss you!” email campaign with a 15% discount.'},
        {'segment': 'Lost Customers', 'language': 'en', 'recommendation': 'Send a survey about why they stopped buying with a coupon for responding.'},
        {'segment': 'Lost Customers', 'language': 'en', 'recommendation': 'Run a social media campaign highlighting new products.'},
        {'segment': 'Lost Customers', 'language': 'en', 'recommendation': 'Offer free shipping for orders placed within a month.'},
        {'segment': 'Lost Customers', 'language': 'en', 'recommendation': 'Create a series of emails with stories about your brand.'},
        {'segment': 'Lost Customers', 'language': 'en', 'recommendation': 'Add lost customers to a loyalty program with a return bonus.'},
    ]
    # --- Работа с базой данных ---
    # Создаёт таблицу и заполняет её рекомендациями и их векторными представлениями
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Создаём таблицу рекомендаций
            cur.execute("""
                CREATE TABLE IF NOT EXISTS recommendations (
                    id SERIAL PRIMARY KEY,
                    segment TEXT,
                    language TEXT,
                    recommendation TEXT,
                    embedding VECTOR(384)
                );
            """)
            # Очищаем таблицу перед добавлением новых данных
            cur.execute("TRUNCATE TABLE recommendations")
            # Добавляем рекомендации и их векторы
            for rec in recommendations:
                embedding = model.encode(rec['recommendation']).tolist()
                cur.execute("INSERT INTO recommendations (segment, language, recommendation, embedding) VALUES (%s, %s, %s, %s)",
                            (rec['segment'], rec['language'], rec['recommendation'], embedding))
            conn.commit()
        logger.info("Таблица рекомендаций инициализирована")

# === Функция find_recommendations ===
# Выполняет векторный поиск рекомендаций по запросу пользователя
def find_recommendations(query, lang, segment=None, limit=3):
    """Поиск рекомендаций по векторному сходству с учетом языка."""
    # --- Векторный поиск ---
    # Преобразует запрос в вектор и ищет наиболее подходящие рекомендации
    query_embedding = model.encode(query).tolist()

    # Логируем первые элементы вектора для отладки
    logger.info(f"Вектор запроса (первые 5 элементов): {query_embedding[:5]}...")
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Если указан сегмент, ищем рекомендации только для него
                if segment:
                    cur.execute("""
                        SELECT segment, recommendation, embedding 
                        FROM recommendations 
                        WHERE segment = %s AND language = %s
                        ORDER BY embedding <-> %s::vector LIMIT %s
                    """, (segment, lang, query_embedding, limit))
                else:
                    # Ищем рекомендации для всех сегментов
                    cur.execute("""
                        SELECT segment, recommendation, embedding 
                        FROM recommendations 
                        WHERE language = %s
                        ORDER BY embedding <-> %s::vector LIMIT %s
                    """, (lang, query_embedding, limit))
                results = cur.fetchall()

        return results
    except psycopg2.Error as e:
        logger.error(f"Ошибка при поиске рекомендаций: {e}")
        return []
