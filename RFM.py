import pandas as pd
import chardet
import re
import csv
import io
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from dateutil.parser import parse as parse_date
from word2number import w2n
import os

# === Класс для обработки файлов: от чтения до очистки данных ===
class FileProcessor:
    """Класс для обработки файлов: чтение, определение кодировки и очистка данных для RFM-анализа.
       Превратит ваш CSV в чистый и готовый к анализу набор данных!"""

    def __init__(self):
        # Храним сообщения об ошибках, чтобы потом рассказать, где что пошло не так
        self.error_message = ""
        # Словарь для распознавания столбцов — поддерживаем разные названия (рус/англ, синонимы)
        self.column_mappings = {
            'client_id': ['id', 'number', 'номер', 'client', 'клиент', 'buyer', 'покупатель', 'customerid', 'customer',
                          'userid', 'clientid'],
            'recency': ['recency', 'давность', 'dayssincelastpurchase', 'days_since_last_purchase'],
            'amount': ['amount', 'сумма', 'cost', 'стоимость', 'price', 'цена', 'monetary', 'деньги', 'totalspent',
                       'total_spent'],
            'frequency': ['frequency', 'частота', 'purchasecount', 'purchase_count'],
            'date': ['date', 'дата', 'transaction_date', 'last_purchase']
        }
        # Сегодняшняя дата — нужна для расчёта Recency (как давно была покупка)
        self.current_date = datetime.now().date()

    def detect_encoding(self, file_path):
        """Определяет кодировку файла, чтобы читать его без сюрпризов."""
        try:
            with open(file_path, 'rb') as file:
                result = chardet.detect(file.read())  # Смотрим, что за кодировка
            # Возвращаем найденную кодировку или utf-8, если ничего не вышло
            return result['encoding'] or 'utf-8'
        except Exception as e:
            # Если что-то пошло не так, записываем ошибку и возвращаем utf-8
            self.error_message += f"• Ошибка определения кодировки: {e}\n"
            return 'utf-8'

    def read_csv_robust(self, file_path, encoding):
        """Читает CSV-файл, обрабатывая ошибки, чтобы ничего не сломалось."""
        valid_rows = []  # Собираем сюда только хорошие строки
        parsing_errors = []  # А сюда — все проблемы с парсингом

        try:
            with open(file_path, 'r', encoding=encoding, errors='replace') as file:
                lines = file.readlines()  # Читаем файл построчно
                if not lines:
                    self.error_message += "• Файл пуст\n"  # Пустой файл? Это не дело!
                    return None

                # Берём первую строку как заголовок
                header = lines[0].strip().split(',')
                if len(header) < 2:
                    self.error_message += "• Некорректный заголовок файла\n"
                    return None
                expected_cols = len(header)  # Запоминаем, сколько столбцов должно быть
                valid_rows.append(header)  # Сохраняем заголовок

                # Обрабатываем строки данных
                for i, line in enumerate(lines[1:], start=2):
                    try:
                        reader = csv.reader([line], skipinitialspace=True)
                        row = next(reader, [])  # Читаем строку как CSV
                        # Проверяем, что строка подходит по количеству столбцов и не пустая
                        if len(row) == expected_cols and any(row):
                            valid_rows.append(row)
                        else:
                            parsing_errors.append(f"[{i}] некорректная структура")
                    except Exception:
                        parsing_errors.append(f"[{i}] ошибка парсинга")

                if len(valid_rows) <= 2:
                    self.error_message += "• Недостаточно данных для анализа\n"
                    return None

                if parsing_errors:
                    self.error_message += f"• Пропущено строк: {len(parsing_errors)} (пример: {parsing_errors[0]})\n"

                # Преобразуем валидные строки в DataFrame
                csv_content = '\n'.join([','.join(row) for row in valid_rows])
                return pd.read_csv(io.StringIO(csv_content), encoding=encoding, skipinitialspace=True)
        except Exception as e:
            self.error_message += f"• Ошибка чтения файла: {e}\n"
            return None

    def normalize_phone(self, phone):
        """Приводит телефонные номера к единому формату (+7XXXXXXXXXX)."""
        if pd.isna(phone):
            return None
        phone = str(phone).strip()
        # Убираем пробелы, дефисы, скобки — всё лишнее!
        phone = re.sub(r'[\s\-\(\)]', '', phone)
        # Если номер начинается с 8, меняем на +7
        if phone.startswith('8'):
            phone = '+7' + phone[1:]
        # Проверяем, что номер в формате +7 и 10 цифр
        if re.match(r'^\+7\d{10}$', phone):
            return phone
        return str(phone)  # Если формат не тот, возвращаем как есть

    def text_to_number(self, text):
        """Преобразует текст (например, 'два') в число."""
        if pd.isna(text):
            return None
        text = str(text).strip().lower()
        try:
            return float(text)  # Пробуем сразу как число
        except ValueError:
            try:
                # Если это слово (например, "два"), пробуем перевести
                return float(w2n.word_to_num(text))
            except (ValueError, NameError):
                return None  # Не получилось? Ну и ладно, None!

    def parse_date_safe(self, date_str):
        """Безопасно парсит даты, не падая при ошибках."""
        if pd.isna(date_str):
            return None
        try:
            return parse_date(str(date_str))  # Пробуем распознать дату
        except (ValueError, TypeError):
            self.error_message += f"• Некорректный формат даты: {date_str}\n"
            return None

    def validate_columns(self, df):
        """Проверяет столбцы и переименовывает их для RFM-анализа."""
        found_columns = {'client_id': None, 'recency': None, 'amount': None, 'frequency': None, 'date': None}
        df_columns_clean = [col.strip().lower() for col in df.columns]  # Приводим названия к нижнему регистру

        self.error_message += f"Обнаруженные столбцы: {', '.join(df.columns)}\n"

        # Ищем подходящие столбцы по их возможным именам
        for key, aliases in self.column_mappings.items():
            for col, col_clean in zip(df.columns, df_columns_clean):
                if col_clean in [alias.lower() for alias in aliases]:
                    found_columns[key] = col
                    break

        # Проверяем, есть ли обязательный client_id
        if found_columns['client_id'] is None:
            self.error_message += "• Отсутствует столбец с ID клиента\n"
            return None
        # Проверяем, есть ли данные для Recency и Monetary
        if found_columns['date'] is None and (found_columns['recency'] is None or found_columns['amount'] is None):
            self.error_message += "• Отсутствуют данные для Recency и Monetary\n"
            return None

        # Переименовываем столбцы в стандартные имена
        rename_dict = {v: k for k, v in found_columns.items() if v is not None}
        self.error_message += f"Переименованные столбцы: {rename_dict}\n"
        return df.rename(columns=rename_dict)[list(rename_dict.values())]

    def clean_data(self, df):
        """Чистит данные: убирает пропуски, исправляет ошибки, приводит к нужному формату."""
        issues = []  # Храним список проблем, чтобы потом отчитаться

        # Обрабатываем ID клиентов
        if 'client_id' in df.columns:
            original_count = len(df)
            df['client_id'] = df['client_id'].astype(str).apply(self.normalize_phone)
            invalid_ids = df['client_id'].isna().sum()
            if invalid_ids > 0:
                issues.append(f"Удалено {invalid_ids} строк с некорректными ID")
            df = df.dropna(subset=['client_id'])

        # Обрабатываем даты и считаем Recency
        if 'date' in df.columns and 'recency' not in df.columns:
            original_count = len(df)
            df['date'] = df['date'].apply(self.parse_date_safe)
            invalid_dates = df['date'].isna().sum()
            if invalid_dates > 0:
                issues.append(f"Удалено {invalid_dates} строк с некорректными датами")
            df = df.dropna(subset=['date'])
            if len(df) > 0:
                df['recency'] = df['date'].apply(lambda x: (self.current_date - x.date()).days)
            if len(df) < original_count:
                issues.append(f"Удалено {original_count - len(df)} строк из-за проблем с датами")

        # Обрабатываем Recency (давность покупки)
        if 'recency' in df.columns:
            original_count = len(df)
            df['recency'] = df['recency'].apply(self.text_to_number)
            invalid_recency = df['recency'].isna().sum()
            if invalid_recency > 0:
                median_recency = df['recency'].median()
                if not pd.isna(median_recency):
                    df['recency'] = df['recency'].fillna(median_recency)
                    issues.append(f"Заполнено {invalid_recency} пропусков в Recency медианой ({median_recency})")
            negative_recency = (df['recency'] < 0).sum()
            if negative_recency > 0:
                issues.append(f"Исправлено {negative_recency} строк с отрицательной давностью")
                df.loc[df['recency'] < 0, 'recency'] = 0
            df = df.dropna(subset=['recency'])
            if len(df) < original_count:
                issues.append(f"Удалено {original_count - len(df)} строк с некорректной давностью")

        # Обрабатываем Monetary (сумма покупок)
        if 'amount' in df.columns:
            original_count = len(df)
            df['amount'] = df['amount'].apply(self.text_to_number)
            invalid_amounts = df['amount'].isna().sum()
            if invalid_amounts > 0:
                median_amount = df['amount'].median()
                if not pd.isna(median_amount):
                    df['amount'] = df['amount'].fillna(median_amount)
                    issues.append(f"Заполнено {invalid_amounts} пропусков в Monetary медианой ({median_amount})")
            negative_amounts = (df['amount'] < 0).sum()
            if negative_amounts > 0:
                issues.append(f"Исправлено {negative_amounts} строк с отрицательными суммами")
                df.loc[df['amount'] < 0, 'amount'] = 0
            df = df.dropna(subset=['amount'])
            if len(df) < original_count:
                issues.append(f"Удалено {original_count - len(df)} строк с некорректными суммами")

        # Обрабатываем Frequency (частота покупок)
        if 'frequency' in df.columns:
            original_count = len(df)
            df['frequency'] = df['frequency'].apply(self.text_to_number)
            invalid_freq = df['frequency'].isna().sum()
            if invalid_freq > 0:
                median_freq = df['frequency'].median()
                if not pd.isna(median_freq):
                    df['frequency'] = df['frequency'].fillna(median_freq)
                    issues.append(f"Заполнено {invalid_freq} пропусков в Frequency медианой ({median_freq})")
            negative_freq = (df['frequency'] < 0).sum()
            if negative_freq > 0:
                issues.append(f"Исправлено {negative_freq} строк с отрицательной частотой")
                df.loc[df['frequency'] < 0, 'frequency'] = 0
            df = df.dropna(subset=['frequency'])
            if len(df) < original_count:
                issues.append(f"Удалено {original_count - len(df)} строк с некорректной частотой")

        # Если были проблемы, записываем их в лог
        if issues:
            self.error_message += "Проблемы в данных\n" + "\n".join([f"• {issue}" for issue in issues]) + "\n"

        if df.empty:
            self.error_message += "• Данные пусты после очистки. Анализ невозможен\n"

        return df

# === Класс для RFM-анализа: превращаем данные в сегменты клиентов ===
class RFMAnalyzer:
    """Класс для выполнения RFM-анализа и разделения клиентов на группы (VIP, лояльные и т.д.)."""

    def __init__(self):
        # Храним ошибки анализа, чтобы знать, где споткнулись
        self.error_message = ""

    def assign_segment(self, row):
        """Определяет сегмент клиента по его RFM-оценкам (Recency, Frequency, Monetary)."""
        r, f, m = int(row['R_Score']), int(row['F_Score']), int(row['M_Score'])
        rfm_sum = r + f + m  # Суммируем оценки для упрощения логики

        # Логика сегментации: кто VIP, а кто "спящий"?
        if rfm_sum == 15:
            return 'VIP-клиенты'  # Лучшие из лучших!
        elif rfm_sum >= 12:
            return 'Лояльные клиенты'  # Постоянные и надёжные
        elif r == 5 and (f + m) <= 7:
            return 'Новые покупатели'  # Только пришли, надо удержать!
        elif rfm_sum == 9:
            return 'Рискующие клиенты'  # Могут уйти, надо спасать
        elif rfm_sum == 3:
            return 'Спящие клиенты'  # Давно не покупали, пора разбудить
        elif r >= 4:
            return 'Новые покупатели' if (f + m) <= 7 else 'Лояльные клиенты'
        elif r <= 2:
            return 'Спящие клиенты' if (f + m) <= 4 else 'Рискующие клиенты'
        return 'Рискующие клиенты'  # По умолчанию — те, кто на грани

    def analyze(self, df):
        """Проводит RFM-анализ: присваивает оценки и сегменты клиентам."""
        # Переименовываем столбцы для удобства
        rfm = df.rename(
            columns={'client_id': 'Buyer', 'recency': 'Recency', 'frequency': 'Frequency', 'amount': 'Monetary'})
        if 'Frequency' not in rfm.columns:
            rfm['Frequency'] = 1  # Если частоты нет, считаем, что одна покупка

        # Проверяем, есть ли всё нужное для анализа
        required_columns = ['Buyer', 'Recency', 'Monetary']
        missing_columns = [col for col in required_columns if col not in rfm.columns]
        if missing_columns:
            self.error_message += f"• Анализ невозможен: отсутствуют столбцы {', '.join(missing_columns)}\n"
            return None

        # Оставляем только нужные столбцы
        rfm = rfm[required_columns + ['Frequency'] if 'Frequency' in rfm.columns else required_columns]

        # Присваиваем RFM-оценки (1–5) по квинтилям
        try:
            rfm['R_Score'] = pd.qcut(rfm['Recency'], 5, labels=[5, 4, 3, 2, 1], duplicates='drop')  # Чем меньше Recency, тем лучше
            rfm['F_Score'] = pd.qcut(rfm['Frequency'].rank(method='first'), 5, labels=[1, 2, 3, 4, 5],
                                     duplicates='drop') if 'Frequency' in rfm.columns else 1
            rfm['M_Score'] = pd.qcut(rfm['Monetary'], 5, labels=[1, 2, 3, 4, 5], duplicates='drop')
        except ValueError as e:
            self.error_message += f"• Недостаточно уникальных значений для разделения на группы\n"
            # Если квинтили не получаются, пробуем меньше групп
            for column, score in [('Recency', 'R_Score'), ('Frequency', 'F_Score'), ('Monetary', 'M_Score')]:
                if column == 'Frequency' and column not in rfm.columns:
                    rfm['F_Score'] = 1
                    continue
                unique_values = len(rfm[column].unique())
                n_bins = min(5, unique_values)
                if n_bins < 2:
                    self.error_message += f"• Невозможно разделить {column}: только {unique_values} значение\n"
                    rfm[score] = 1
                else:
                    try:
                        if column == 'Recency':
                            labels = list(range(n_bins, 0, -1))  # Для Recency — обратный порядок
                        else:
                            labels = list(range(1, n_bins + 1))
                        if column == 'Frequency':
                            rfm[score] = pd.qcut(rfm[column].rank(method='first'), n_bins, labels=labels,
                                                 duplicates='drop')
                        else:
                            rfm[score] = pd.qcut(rfm[column], n_bins, labels=labels, duplicates='drop')
                    except ValueError:
                        self.error_message += f"• Ошибка разделения {column} на {n_bins} групп\n"
                        rfm[score] = 1

        # Присваиваем сегменты каждому клиенту
        rfm['Segment'] = rfm.apply(self.assign_segment, axis=1)
        return rfm

# === Класс для визуализации и вывода результатов ===
class ResultPresenter:
    """Класс для создания красивых таблиц, графиков и текстового описания RFM-анализа."""

    def __init__(self):
        # Храним таблицу и текст результатов
        self.result_table = ""
        self.result_text = ""
        self.plot_path = ""  # Путь к сохранённому графику

    def plot_rfm_segments(self, segment_dicts, output_file='rfm_segments.png'):
        """Рисует три красивых графика: количество клиентов, средний и общий чек по сегментам."""
        if not segment_dicts:
            self.result_text += "• Визуализация невозможна: нет данных\n"
            return

        # Сохраняем график в папку Charts
        output_file = os.path.join('Charts', os.path.basename(output_file))
        self.plot_path = output_file

        # Настраиваем стиль графиков — чтобы всё выглядело стильно
        sns.set_theme(style="whitegrid", palette="muted")
        plt.rcParams['font.family'] = 'Arial'
        plt.rcParams['font.size'] = 12

        # Создаём три подграфика рядом
        fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(18, 6), sharey=True)

        segments = list(segment_dicts.keys())
        counts = [segment_dicts[s]['Количество клиентов'] for s in segments]
        avg_checks = [segment_dicts[s]['Средний чек'] for s in segments]
        total_checks = [segment_dicts[s]['Общий чек'] for s in segments]

        # График 1: Количество клиентов по сегментам
        bar1 = sns.barplot(x=counts, y=segments, hue=segments, ax=ax1, palette='viridis', legend=False)
        ax1.set_title('Количество клиентов', fontsize=14, pad=10)
        ax1.set_xlabel('Клиентов', fontsize=12)
        ax1.set_ylabel('Сегмент', fontsize=12)
        for i, v in enumerate(counts):
            bar1.text(v + 0.1, i, str(v), va='center', fontsize=10)  # Добавляем подписи

        # График 2: Средний чек по сегментам
        bar2 = sns.barplot(x=avg_checks, y=segments, hue=segments, ax=ax2, palette='magma', legend=False)
        ax2.set_title('Средний чек', fontsize=14, pad=10)
        ax2.set_xlabel('Руб.', fontsize=12)
        ax2.set_ylabel('')
        for i, v in enumerate(avg_checks):
            bar2.text(v + 0.1, i, f"{v:.2f}", va='center', fontsize=10)

        # График 3: Общий чек по сегментам
        bar3 = sns.barplot(x=total_checks, y=segments, hue=segments, ax=ax3, palette='crest', legend=False)
        ax3.set_title('Общий чек', fontsize=14, pad=10)
        ax3.set_xlabel('Руб.', fontsize=12)
        ax3.set_ylabel('')
        for i, v in enumerate(total_checks):
            bar3.text(v + 0.1, i, f"{v:.2f}", va='center', fontsize=10)

        # Делаем графики компактными и сохраняем
        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()

    def generate_results(self, rfm, file_name):
        """Создаёт таблицу и текст с результатами RFM-анализа."""
        segments = ['VIP-клиенты', 'Лояльные клиенты', 'Новые покупатели', 'Рискующие клиенты', 'Спящие клиенты']
        segment_dicts = {}

        # Собираем статистику по каждому сегменту
        for segment in segments:
            segment_data = rfm[rfm['Segment'] == segment]
            count = len(segment_data)
            avg_amount = segment_data['Monetary'].mean() if count > 0 else 0
            total_amount = segment_data['Monetary'].sum() if count > 0 else 0

            segment_dicts[segment] = {
                'Количество клиентов': count,
                'Средний чек': float(round(avg_amount, 2)),
                'Общий чек': float(round(total_amount, 2))
            }

        # Формируем таблицу результатов
        result_df = pd.DataFrame.from_dict(segment_dicts, orient='index').reset_index()
        result_df.columns = ['Сегмент', 'Количество клиентов', 'Средний чек', 'Общий чек']
        self.result_table = result_df.to_string(index=False)

        # Пишем текстовое описание — всё по полочкам
        self.result_text = f"\nРезультаты RFM-анализа\n\n"
        for segment, data in segment_dicts.items():
            self.result_text += f"{segment}:\n"
            self.result_text += f"  Клиентов: {data['Количество клиентов']}\n"
            self.result_text += f"  Средний чек: {data['Средний чек']:.2f} руб.\n"
            self.result_text += f"  Общий чек: {data['Общий чек']:.2f} руб.\n\n"

        # Создаём графики для наглядности
        self.plot_rfm_segments(segment_dicts, output_file=f'rfm_segments_{file_name.replace(".csv", "")}.png')

# === Главная функция: собираем всё воедино ===
def main(file_path):
    """Основная функция: читает файл, чистит данные, проводит RFM-анализ и выдаёт результаты."""
    processor = FileProcessor()  # Создаём обработчик файлов
    analyzer = RFMAnalyzer()    # Создаём анализатор RFM
    presenter = ResultPresenter()  # Создаём визуализатор результатов

    # Начинаем анализ файла
    processor.error_message = f"\n Анализ файла:\n"
    encoding = processor.detect_encoding(file_path)  # Определяем кодировку
    df = processor.read_csv_robust(file_path, encoding)  # Читаем CSV
    if df is None:
        return {
            'errors': processor.error_message,
            'corrections': "",
            'plot_path': "",
            'result_text': "",
            'result_table': ""
        }

    # Проверяем и чистим данные
    df = processor.validate_columns(df)
    if df is None:
        return {
            'errors': processor.error_message,
            'corrections': "",
            'plot_path': "",
            'result_text': "",
            'result_table': ""
        }

    df = processor.clean_data(df)
    if df.empty:
        return {
            'errors': processor.error_message,
            'corrections': processor.error_message,
            'plot_path': "",
            'result_text': "",
            'result_table': ""
        }

    # Проводим RFM-анализ
    rfm = analyzer.analyze(df)
    if rfm is None:
        return {
            'errors': processor.error_message + analyzer.error_message,
            'corrections': processor.error_message,
            'plot_path': "",
            'result_text': "",
            'result_table': ""
        }

    # Генерируем красивые результаты
    presenter.generate_results(rfm, file_path)
    return {
        'errors': processor.error_message + analyzer.error_message,
        'corrections': processor.error_message,
        'plot_path': presenter.plot_path,
        'result_text': presenter.result_text,
        'result_table': presenter.result_table
    }

if __name__ == "__main__":
    # Точка входа: запускаем анализ для примера с файлом 'rfm_data.csv'
    result = main('rfm_data.csv')