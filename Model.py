# === Model.py: Модуль для работы с API Gemini через прокси ===
# Этот файл отвечает за выполнение запросов к модели Gemini с использованием прокси
# для генерации текстовых ответов на основе входных данных.

import google.generativeai as genai
import requests
import os
import warnings
import logging

# --- Настройка логирования ---
# Инициализирует систему логирования для отслеживания работы модуля
# Логи будут содержать время, уровень (DEBUG, INFO, ERROR) и сообщение
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Отключение SSL-предупреждений ---
# Отключает предупреждения об SSL (для тестирования, в продакшене лучше включить проверку)
# Это позволяет игнорировать ошибки сертификатов при использовании прокси
warnings.filterwarnings("ignore", category=requests.packages.urllib3.exceptions.InsecureRequestWarning)

# === Функция Gemini ===
# Основная функция для отправки запросов к модели Gemini через прокси
def Gemini(model_input: str) -> str:
    """
    Выполняет запрос к модели Gemini через прокси и возвращает ответ.

    Args:
        model_input (str): Входной текст для модели (запрос, который нужно обработать).

    Returns:
        str: Ответ модели или сообщение об ошибке, если что-то пошло не так.
    """
    # --- Определение API-ключа и прокси ---
    # API-ключ для доступа к Gemini и URL прокси для обхода ограничений
    API_KEY = "ЗДЕСЬ_ВАШ_ТОКЕН"  # Ключ для API Gemini
    PROXY_URL = "http://45.12.150.82:8080"  # Адрес прокси-сервера

    # --- Настройка прокси ---
    # Устанавливает прокси через переменные окружения для всех HTTP/HTTPS-запросов
    os.environ['HTTP_PROXY'] = PROXY_URL
    os.environ['HTTPS_PROXY'] = PROXY_URL

    # --- Проверка прокси ---
    # Проверяет, работает ли прокси, отправляя запрос на внешний сервис
    try:
        response = requests.get(
            "https://api.ipify.org",  # Сервис для получения текущего IP
            verify=False,  # Отключаем проверку SSL (для тестирования)
            allow_redirects=True,  # Разрешаем перенаправления
            timeout=20  # Ожидаем ответа не более 20 секунд
        )
        logging.info(f"Проверка прокси... IP: {response.text}")  # Логируем IP для проверки
    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка проверки прокси: {e}")  # Логируем ошибку
        return f"Ошибка прокси: {str(e)}"  # Возвращаем сообщение об ошибке

    # --- Инициализация API Gemini ---
    # Настраивает подключение к API Gemini с использованием API-ключа
    try:
        genai.configure(api_key=API_KEY)  # Устанавливаем ключ для API
    except Exception as e:
        logging.error(f"Ошибка инициализации API: {e}")  # Логируем ошибку
        return f"Ошибка инициализации API: {str(e)}"  # Возвращаем сообщение об ошибке

    # --- Выполнение запроса к модели Gemini ---
    # Отправляет запрос к модели и возвращает сгенерированный ответ
    try:
        model = genai.GenerativeModel("gemini-1.5-flash")  # Инициализируем модель Gemini
        response = model.generate_content(
            contents=[{"parts": [{"text": model_input}]}],  # Формируем запрос из входного текста
            generation_config={"temperature": 0.7}  # Устанавливаем креативность ответа
        )
        return response.text  # Возвращаем текст ответа модели
    except Exception as e:
        logging.error(f"Ошибка Gemini: {e}")  # Логируем ошибку
        return f"Ошибка: {str(e)}"  # Возвращаем сообщение об ошибке
