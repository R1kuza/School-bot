import logging
import sqlite3
import requests
import time
import re
import os
import pandas as pd
from datetime import datetime
from html import escape
from collections import defaultdict
import io

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BOT_TOKEN = os.environ.get('BOT_TOKEN')
if not BOT_TOKEN:
    logging.error("BOT_TOKEN environment variable is not set!")
    exit(1)

ADMINS = [admin.strip() for admin in os.environ.get('ADMINS', 'r1kuza,nadya_yakovleva01,Priikalist').split(',') if admin.strip()]

MAX_MESSAGE_LENGTH = 4000
MAX_USERS_PER_CLASS = 30
MAX_REQUESTS_PER_MINUTE = 20

BASE_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class RateLimiter:
    def __init__(self, max_requests=MAX_REQUESTS_PER_MINUTE, window=60):
        self.requests = defaultdict(list)
        self.max_requests = max_requests
        self.window = window
    
    def is_limited(self, user_id):
        now = time.time()
        user_requests = self.requests[user_id]
        user_requests = [req for req in user_requests if now - req < self.window]
        
        if len(user_requests) >= self.max_requests:
            return True
        
        user_requests.append(now)
        self.requests[user_id] = user_requests[-self.max_requests:]
        return False

class SimpleSchoolBot:
    def __init__(self):
        self.last_update_id = 0
        self.admin_states = {}
        self.user_states = {}
        self.processed_updates = set()
        self.rate_limiter = RateLimiter()
        self.init_db()
    
    def init_db(self):
        db_path = os.environ.get('DATABASE_PATH', 
                                os.path.join(os.path.dirname(os.path.abspath(__file__)), "school_bot.db"))
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.create_tables()
    
    def create_tables(self):
        cursor = self.conn.cursor()
        cursor.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                full_name TEXT NOT NULL,
                class TEXT NOT NULL,
                role TEXT DEFAULT 'user',
                registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS schedule (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                class TEXT NOT NULL,
                day TEXT NOT NULL,
                lesson_number INTEGER,
                subject TEXT,
                teacher TEXT,
                room TEXT,
                UNIQUE(class, day, lesson_number)
            );

            CREATE TABLE IF NOT EXISTS bell_schedule (
                lesson_number INTEGER PRIMARY KEY,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL
            );
        """)
        
        cursor.execute("SELECT COUNT(*) FROM bell_schedule")
        if cursor.fetchone()[0] == 0:
            bell_schedule = [
                (1, '8:00', '8:40'),
                (2, '8:50', '9:30'),
                (3, '9:40', '10:20'),
                (4, '10:30', '11:10'),
                (5, '11:25', '12:05'),
                (6, '12:10', '12:50'),
                (7, '13:00', '13:40')
            ]
            cursor.executemany(
                "INSERT OR REPLACE INTO bell_schedule (lesson_number, start_time, end_time) VALUES (?, ?, ?)",
                bell_schedule
            )
        
        self.conn.commit()
    
    def safe_message(self, text):
        if not text:
            return ""
        text = str(text)
        text = re.sub(r'<[^>]+>', '', text)
        text = escape(text)
        return text
    
    def truncate_message(self, text, max_length=MAX_MESSAGE_LENGTH):
        if len(text) <= max_length:
            return text
        return text[:max_length-3] + "..."
    
    def send_message(self, chat_id, text, reply_markup=None):
        safe_text = self.truncate_message(self.safe_message(text))
        
        url = f"{BASE_URL}/sendMessage"
        data = {
            "chat_id": chat_id,
            "text": safe_text,
            "parse_mode": "HTML"
        }
        if reply_markup:
            data["reply_markup"] = reply_markup
        
        try:
            response = requests.post(url, json=data, timeout=10)
            return response.json()
        except Exception as e:
            logger.error(f"Ошибка отправки сообщения: {e}")
            return None

    def send_document(self, chat_id, document, filename=None):
        url = f"{BASE_URL}/sendDocument"
        data = {"chat_id": chat_id}
        files = {"document": (filename, document)}
        
        try:
            response = requests.post(url, data=data, files=files, timeout=30)
            return response.json()
        except Exception as e:
            logger.error(f"Ошибка отправки документа: {e}")
            return None
    
    def get_file(self, file_id):
        url = f"{BASE_URL}/getFile"
        data = {"file_id": file_id}
        
        try:
            response = requests.post(url, json=data, timeout=10)
            result = response.json()
            if result.get("ok"):
                return result["result"]
            return None
        except Exception as e:
            logger.error(f"Ошибка получения файла: {e}")
            return None
    
    def download_file(self, file_path):
        url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
        
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                return response.content
            return None
        except Exception as e:
            logger.error(f"Ошибка загрузки файла: {e}")
            return None
    
    def log_security_event(self, event_type, user_id, details):
        logger.warning(f"SECURITY: {event_type} - User: {user_id} - {details}")
    
    def get_updates(self):
        url = f"{BASE_URL}/getUpdates"
        params = {
            "offset": self.last_update_id + 1,
            "timeout": 10,
            "limit": 100
        }
        
        try:
            response = requests.get(url, params=params, timeout=15)
            result = response.json()
            
            if not result.get("ok") and "Conflict" in str(result.get("description", "")):
                logger.warning("Обнаружен конфликт getUpdates")
                return {"ok": False, "conflict": True}
                
            return result
        except Exception as e:
            logger.error(f"Ошибка получения обновлений: {e}")
            return {"ok": False}
    
    def get_user(self, user_id):
        if not self.is_valid_user_id(user_id):
            return None
            
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        return cursor.fetchone()
    
    def is_valid_user_id(self, user_id):
        return isinstance(user_id, int) and user_id > 0
    
    def create_user(self, user_id, full_name, class_name):
        if not self.is_valid_user_id(user_id):
            return False
            
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users WHERE class = ?", (class_name,))
        count = cursor.fetchone()[0]
        
        if count >= MAX_USERS_PER_CLASS:
            self.log_security_event("class_limit_exceeded", user_id, f"Class: {class_name}")
            return False
        
        cursor.execute(
            "INSERT OR REPLACE INTO users (user_id, full_name, class) VALUES (?, ?, ?)",
            (user_id, full_name, class_name)
        )
        self.conn.commit()
        return True
    
    def delete_user(self, user_id):
        if not self.is_valid_user_id(user_id):
            return False
            
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
        self.conn.commit()
        return cursor.rowcount > 0
    
    def get_all_users(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT user_id, full_name, class, registered_at FROM users ORDER BY registered_at DESC")
        return cursor.fetchall()
    
    def get_schedule(self, class_name, day):
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT lesson_number, subject, teacher, room FROM schedule WHERE class = ? AND day = ? ORDER BY lesson_number",
            (class_name, day)
        )
        return cursor.fetchall()
    
    def save_schedule(self, class_name, day, lessons):
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM schedule WHERE class = ? AND day = ?", (class_name, day))
        
        for lesson_num, subject, teacher, room in lessons:
            subject = subject[:100] if subject else ""
            teacher = teacher[:50] if teacher else ""
            room = room[:20] if room else ""
            
            cursor.execute(
                "INSERT OR REPLACE INTO schedule (class, day, lesson_number, subject, teacher, room) VALUES (?, ?, ?, ?, ?, ?)",
                (class_name, day, lesson_num, subject, teacher, room)
            )
        
        self.conn.commit()
    
    def get_bell_schedule(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT lesson_number, start_time, end_time FROM bell_schedule ORDER BY lesson_number")
        return cursor.fetchall()
    
    def is_admin(self, username):
        return username and username.lower() in [admin.lower() for admin in ADMINS]
    
    def main_menu_keyboard(self):
        return {
            "keyboard": [
                [{"text": "📚 Моё расписание"}, {"text": "🏫 Общее расписание"}],
                [{"text": "🔔 Звонки"}, {"text": "ℹ️ Помощь"}]
            ],
            "resize_keyboard": True
        }
    
    def admin_menu_inline_keyboard(self):
        return {
            "inline_keyboard": [
                [{"text": "👥 Список пользователей", "callback_data": "admin_users"}],
                [{"text": "❌ Удалить пользователя", "callback_data": "admin_delete_user"}],
                [{"text": "📝 Редактировать расписание", "callback_data": "admin_edit_schedule"}],
                [{"text": "🏫 Управление классами", "callback_data": "admin_manage_classes"}],
                [{"text": "🕧 Управление звонками", "callback_data": "admin_bells"}],
                [{"text": "📤 Загрузить Excel", "callback_data": "admin_upload_excel"}],
                [{"text": "📊 Статистика", "callback_data": "admin_stats"}],
                [{"text": "⬅️ Назад", "callback_data": "admin_back"}]
            ]
        }
    
    def classes_management_inline_keyboard(self):
        return {
            "inline_keyboard": [
                [{"text": "➕ Добавить класс", "callback_data": "admin_add_class"}],
                [{"text": "➖ Удалить класс", "callback_data": "admin_delete_class"}],
                [{"text": "⬅️ Назад в админку", "callback_data": "admin_back"}]
            ]
        }
    
    def bells_management_inline_keyboard(self):
        return {
            "inline_keyboard": [
                [{"text": "✏️ Изменить звонок", "callback_data": "admin_edit_bell"}],
                [{"text": "👀 Посмотреть все звонки", "callback_data": "admin_view_bells"}],
                [{"text": "⬅️ Назад в админку", "callback_data": "admin_back"}]
            ]
        }
    
    def day_selection_inline_keyboard(self):
        return {
            "inline_keyboard": [
                [{"text": "Понедельник", "callback_data": "day_monday"}],
                [{"text": "Вторник", "callback_data": "day_tuesday"}],
                [{"text": "Среда", "callback_data": "day_wednesday"}],
                [{"text": "Четверг", "callback_data": "day_thursday"}],
                [{"text": "Пятница", "callback_data": "day_friday"}],
                [{"text": "Суббота", "callback_data": "day_saturday"}]
            ]
        }
    
    def class_selection_keyboard(self):
        classes = []
        
        for grade in range(5, 10):
            for letter in ['А', 'Б', 'В']:
                classes.append(f"{grade}{letter}")
        
        classes.extend(["10П", "10Р", "11Р"])
        
        keyboard = []
        row = []
        for i, cls in enumerate(classes):
            row.append({"text": cls})
            if (i + 1) % 3 == 0:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        
        keyboard.append([{"text": "⬅️ Назад"}])
        
        return {"keyboard": keyboard, "resize_keyboard": True}
    
    def shift_selection_keyboard(self):
        return {
            "keyboard": [
                [{"text": "1 смена"}, {"text": "2 смена"}],
                [{"text": "❌ Отменить"}]
            ],
            "resize_keyboard": True
        }
    
    def cancel_keyboard(self):
        return {
            "keyboard": [[{"text": "❌ Отменить"}]],
            "resize_keyboard": True
        }
    
    def is_valid_class(self, class_str):
        class_str = class_str.strip().upper()
        
        if re.match(r'^[5-9][А-В]$', class_str):
            return True
        
        if class_str in ['10П', '10Р', '11Р']:
            return True
        
        return False
    
    def is_valid_fullname(self, name):
        name = name.strip()
        if len(name) > 100:
            return False
            
        parts = name.split()
        if len(parts) < 2:
            return False
        
        for part in parts:
            if not part.isalpha() or len(part) < 2 or len(part) > 20:
                return False
        
        return True
    
    def is_valid_time(self, time_str):
        return bool(re.match(r'^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$', time_str))
    
    def get_existing_classes(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT DISTINCT class FROM users ORDER BY class")
        return [row[0] for row in cursor.fetchall()]
    
    def add_class(self, class_name):
        return self.is_valid_class(class_name)
    
    def delete_class(self, class_name):
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM users WHERE class = ?", (class_name,))
        deleted_count = cursor.rowcount
        self.conn.commit()
        return deleted_count > 0
    
    def update_bell_schedule(self, lesson_number, start_time, end_time):
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE bell_schedule SET start_time = ?, end_time = ? WHERE lesson_number = ?",
            (start_time, end_time, lesson_number)
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def parse_excel_schedule(self, file_content, shift):
        try:
            import pandas as pd
            
            lessons_data = []
            
            logger.info(f"=== НАЧАЛО ПАРСИНГА ДЛЯ СМЕНЫ {shift} ===")
            logger.info("Используется метод парсинга: method3 (структурный)")
            
            try:
                excel_file = pd.ExcelFile(io.BytesIO(file_content))
                sheet_names = excel_file.sheet_names
                logger.info(f"Доступные листы в файле: {sheet_names}")
                
                selected_sheet = self._select_sheet(sheet_names, shift)
                if not selected_sheet:
                    logger.error("Не удалось найти подходящий лист!")
                    return None
                
                logger.info(f"Выбран лист: '{selected_sheet}'")
                
                df = pd.read_excel(io.BytesIO(file_content), sheet_name=selected_sheet, header=None)
                logger.info(f"Размер таблицы: {df.shape} (строк: {df.shape[0]}, колонок: {df.shape[1]})")
                
                self._log_file_structure(df, selected_sheet)
                
                success = self._parse_method3(df, shift, lessons_data, selected_sheet)
                
                if not success:
                    logger.error("Метод парсинга не дал результатов")
                    return None
                
            except Exception as e:
                logger.error(f"Ошибка чтения Excel файла для смены {shift}: {e}")
                import traceback
                logger.error(f"Трассировка: {traceback.format_exc()}")
                return None
            
            logger.info(f"=== ЗАВЕРШЕНИЕ ПАРСИНГА ДЛЯ СМЕНЫ {shift} ===")
            logger.info(f"Найдено уроков: {len(lessons_data)}")
            
            if lessons_data:
                class_stats = {}
                for lesson in lessons_data:
                    class_name = lesson['class']
                    class_stats[class_name] = class_stats.get(class_name, 0) + 1
                
                logger.info(f"Статистика по классам: {class_stats}")
            
            return lessons_data if lessons_data else None
            
        except Exception as e:
            logger.error(f"Общая ошибка парсинга Excel для смены {shift}: {e}")
            import traceback
            logger.error(f"Трассировка: {traceback.format_exc()}")
            return None

    def _select_sheet(self, sheet_names, shift):
        possible_sheet_names = [
            f"{shift} СМЕНА",
            f"{shift} смена", 
            f"Смена {shift}",
            f"СМЕНА {shift}",
            f"1 СМЕНА",
            "1 СМЕНА"
        ]
        
        for sheet_name in possible_sheet_names:
            if sheet_name in sheet_names:
                return sheet_name
        
        for sheet_name in sheet_names:
            if any(name.lower() in sheet_name.lower() for name in possible_sheet_names):
                return sheet_name
        
        if sheet_names:
            logger.warning(f"Лист для смены {shift} не найден, используем первый лист: {sheet_names[0]}")
            return sheet_names[0]
        
        return None

    def _log_file_structure(self, df, sheet_name):
        logger.info(f"=== СТРУКТУРА ФАЙЛА '{sheet_name}' ===")
        
        logger.info("Первые 15 строк файла:")
        for i in range(min(15, len(df))):
            row_preview = []
            for j in range(min(20, len(df.columns))):
                cell_value = df.iloc[i, j]
                if pd.isna(cell_value):
                    row_preview.append("")
                else:
                    row_preview.append(str(cell_value).strip())
            logger.info(f"Строка {i:2d}: {row_preview}")
        
        non_empty_cells = 0
        for i in range(min(20, len(df))):
            for j in range(min(20, len(df.columns))):
                if pd.notna(df.iloc[i, j]) and str(df.iloc[i, j]).strip():
                    non_empty_cells += 1
        
        logger.info(f"Непустых ячеек в первых 20x20: {non_empty_cells}")

    def _parse_method3(self, df, shift, lessons_data, sheet_name):
        try:
            logger.info("=== МЕТОД 3: СТРУКТУРНЫЙ ПАРСИНГ ===")
            
            class_row_idx = self._find_class_header_row(df)
            if class_row_idx is None:
                logger.error("Не удалось найти строку с заголовками классов")
                return False
            
            logger.info(f"Найдена строка с классами: строка {class_row_idx}")
            
            class_columns = self._extract_class_columns(df, class_row_idx)
            if not class_columns:
                logger.error("Не удалось определить классы и их колонки")
                return False
            
            logger.info(f"Найдены классы и колонки: {class_columns}")
            
            day_rows = self._find_day_rows(df)
            if not day_rows:
                logger.error("Не удалось найти дни недели")
                return False
            
            logger.info(f"Найдены дни недели: {day_rows}")
            
            for day_name, day_row_idx in day_rows:
                logger.info(f"Обрабатываем день: {day_name} (строка {day_row_idx})")
                
                next_day_idx = None
                for next_day, next_idx in day_rows:
                    if next_idx > day_row_idx:
                        next_day_idx = next_idx
                        break
                
                end_row = next_day_idx if next_day_idx else len(df)
                
                day_lessons = self._parse_day_schedule(df, day_row_idx, end_row, class_columns, shift, day_name)
                lessons_data.extend(day_lessons)
                logger.info(f"Для дня {day_name} найдено {len(day_lessons)} уроков")
            
            logger.info(f"Метод 3: успешно распаршено {len(lessons_data)} уроков")
            return len(lessons_data) > 0
            
        except Exception as e:
            logger.error(f"Ошибка в методе 3: {e}")
            import traceback
            logger.error(f"Трассировка: {traceback.format_exc()}")
            return False

    def _find_class_header_row(self, df):
        for i in range(min(15, len(df))):
            row = df.iloc[i]
            class_count = 0
            for cell in row:
                if pd.notna(cell) and self._is_class_header(str(cell)):
                    class_count += 1
            if class_count >= 2:
                return i
        return None

    def _extract_class_columns(self, df, class_row_idx):
        class_columns = {}
        class_row = df.iloc[class_row_idx]
        
        for j, cell in enumerate(class_row):
            if pd.notna(cell):
                cell_str = str(cell).strip()
                class_name = self._extract_class_name(cell_str)
                if class_name:
                    class_columns[class_name] = j
                    logger.debug(f"Найден класс {class_name} в колонке {j}")
        
        return class_columns

    def _find_day_rows(self, df):
        day_rows = []
        day_patterns = {
            'понедельник': 'monday',
            'вторник': 'tuesday',
            'среда': 'wednesday',
            'четверг': 'thursday',
            'пятница': 'friday',
            'суббота': 'saturday'
        }
        
        for i in range(len(df)):
            for j in range(min(3, len(df.columns))):
                if pd.notna(df.iloc[i, j]) and isinstance(df.iloc[i, j], str):
                    cell_value = str(df.iloc[i, j]).lower().strip()
                    for ru_day, en_day in day_patterns.items():
                        if ru_day in cell_value:
                            day_rows.append((en_day, i))
                            logger.debug(f"Найден день '{en_day}' в строке {i}, колонке {j}")
                            break
                    else:
                        continue
                    break
        
        day_rows.sort(key=lambda x: x[1])
        return day_rows

    def _parse_day_schedule(self, df, start_row, end_row, class_columns, shift, day_name):
        lessons = []
        
        # Собираем все номера уроков из колонки с номерами (индекс 1)
        lesson_numbers = {}
        for row_idx in range(start_row, min(end_row, len(df))):
            row = df.iloc[row_idx]
            
            # Ищем номер урока во второй колонке (индекс 1)
            if len(row) > 1 and pd.notna(row[1]):
                lesson_str = str(row[1]).strip()
                # Извлекаем число из ячейки
                numbers = re.findall(r'\d+', lesson_str)
                if numbers:
                    lesson_num = int(numbers[0])
                    if 1 <= lesson_num <= 10:
                        lesson_numbers[row_idx] = lesson_num
                        logger.debug(f"Найден номер урока {lesson_num} в строке {row_idx}")
        
        # Обрабатываем строки с уроками
        current_lesson_num = 1
        
        for row_idx in range(start_row, min(end_row, len(df))):
            row = df.iloc[row_idx]
            
            if all(pd.isna(cell) for cell in row):
                continue
            
            # Определяем номер урока
            lesson_num = lesson_numbers.get(row_idx)
            if lesson_num is not None:
                current_lesson_num = lesson_num
            else:
                # Если номер не найден, используем текущий
                lesson_num = current_lesson_num
            
            lesson_found_in_row = False
            
            for class_name, col_idx in class_columns.items():
                # Проверяем колонку с предметом
                subject_col = col_idx
                if subject_col < len(row) and pd.notna(row[subject_col]):
                    subject = str(row[subject_col]).strip()
                    
                    # Пропускаем пустые значения и названия дней
                    if not subject or subject in ['-', '—', ''] or self._is_day_of_week(subject):
                        continue
                    
                    # Получаем кабинет из следующей колонки
                    room = ""
                    room_col = col_idx + 1
                    if room_col < len(row) and pd.notna(row[room_col]):
                        room_cell = str(row[room_col]).strip()
                        if room_cell and not self._is_day_of_week(room_cell):
                            room = room_cell
                    
                    lessons.append({
                        'class': class_name,
                        'day': day_name,
                        'lesson_number': lesson_num,
                        'subject': subject,
                        'room': room,
                        'teacher': '',
                        'shift': shift
                    })
                    
                    lesson_found_in_row = True
                    logger.debug(f"Добавлен урок: {class_name}, {day_name}, {lesson_num}, {subject}, {room}")
            
            # Увеличиваем номер урока только если в строке был найден хотя бы один урок
            # и если мы не используем явный номер из ячейки
            if lesson_found_in_row and row_idx not in lesson_numbers:
                current_lesson_num += 1
        
        return lessons

    def _is_class_header(self, text):
        text = text.lower().strip()
        patterns = [
            r'^\d[абв]$',
            r'^10[пр]$',
            r'^11[р]$',
            r'^\d[абв]\s*$',
            r'^\d[абв].*класс',
            r'^класс.*\d[абв]'
        ]
        return any(re.match(pattern, text) for pattern in patterns)

    def _extract_class_name(self, text):
        text = text.lower().strip()
        
        text = re.sub(r'(класс|смена|урок|расписание|№)', '', text).strip()
        
        patterns = [
            (r'(\d[абв])', 1),
            (r'(10[пр])', 1),
            (r'(11[р])', 1)
        ]
        
        for pattern, group in patterns:
            match = re.search(pattern, text)
            if match:
                class_name = match.group(group).upper()
                return class_name
        
        return None

    def _is_day_of_week(self, text):
        text = text.lower().strip()
        days = ['понедельник', 'вторник', 'среда', 'четверг', 'пятница', 'суббота']
        return any(day in text for day in days)

    def import_schedule_from_excel(self, file_content, shift):
        try:
            lessons_data = self.parse_excel_schedule(file_content, shift)
            if not lessons_data:
                return False, f"Не удалось распарсить Excel файл для {shift} смены"
            
            cursor = self.conn.cursor()
            imported_count = 0
            error_count = 0
            
            imported_classes = set(lesson['class'] for lesson in lessons_data)
            
            for class_name in imported_classes:
                cursor.execute("DELETE FROM schedule WHERE class = ?", (class_name,))
                logger.info(f"Удалены старые уроки для класса {class_name}")
            
            for lesson in lessons_data:
                try:
                    lesson_number = int(lesson['lesson_number'])
                    class_name = lesson['class']
                    day = lesson['day']
                    
                    cursor.execute(
                        "INSERT OR REPLACE INTO schedule (class, day, lesson_number, subject, teacher, room) VALUES (?, ?, ?, ?, ?, ?)",
                        (class_name, day, lesson_number, lesson['subject'], lesson['teacher'], lesson['room'])
                    )
                    imported_count += 1
                except Exception as e:
                    logger.error(f"Ошибка импорта урока {lesson}: {e}")
                    error_count += 1
            
            self.conn.commit()
            
            message = f"✅ Успешно импортировано {imported_count} уроков для {shift} смены\nМетод: method3 (структурный)"
            if error_count > 0:
                message += f", ошибок: {error_count}"
                
            return True, message
        except Exception as e:
            logger.error(f"Ошибка импорта из Excel для смены {shift}: {e}")
            return False, f"Ошибка импорта для {shift} смены: {str(e)}"

    def handle_start(self, chat_id, user):
        user_data = self.get_user(user["id"])
        
        if user_data:
            text = (
                f"Привет, {self.safe_message(user.get('first_name', 'друг'))}!\n"
                f"Ты уже зарегистрирован в системе.\n"
                f"Твой класс: {self.safe_message(user_data[2])}"
            )
        else:
            text = (
                f"Привет, {self.safe_message(user.get('first_name', 'друг'))}!\n"
                "Я бот для просмотра расписания школы.\n\n"
                "Для начала работы необходимо зарегистрироваться.\n"
                "Пожалуйста, введите своё ФИО и класс в формате:\n"
                "<b>Фамилия Имя Отчество, Класс</b>\n\n"
                "Например: <i>Иванов Иван Иванович, 10П</i>\n\n"
                "<b>Доступные классы:</b>\n"
                "5-9 классы: А, Б, В\n"
                "10 класс: П, Р\n"
                "11 класс: Р"
            )
        
        self.send_message(chat_id, text, self.main_menu_keyboard() if user_data else None)
    
    def handle_help(self, chat_id, username):
        text = (
            "📚 <b>Школьный бот - помощь</b>\n\n"
            "Я помогу тебе узнать расписание уроков.\n\n"
            "<b>Основные команды:</b>\n"
            "• /start - начать работу\n"
            "• /help - показать эту справку\n\n"
            "<b>Возможности:</b>\n"
            "• <b>Моё расписание</b> - расписание для твоего класса\n"
            "• <b>Общее расписание</b> - расписание для любого класса\n"
            "• <b>Звонки</b> - расписание звонков\n\n"
            "Для регистрации введи своё ФИО и класс в формате:\n"
            "<i>Фамилия Имя Отчество, Класс</i>\n\n"
            "<b>Доступные классы:</b>\n"
            "5-9 классы: А, Б, В\n"
            "10 класс: П, Р\n"
            "11 класс: Р\n\n"
            "🛠 <b>Техническая помощь</b>\n"
            "Если вы обнаружили ошибку или у вас есть предложения, "
            "напишите разработчику: @r1kuza"
        )
        
        if self.is_admin(username):
            text += "\n\n🔐 <b>Секретная команда для админа:</b>\n/admin_panel"
        
        self.send_message(chat_id, text)
    
    def handle_admin_panel(self, chat_id, username):
        if not self.is_admin(username):
            self.log_security_event("unauthorized_admin_access", chat_id, f"Username: {username}")
            self.send_message(chat_id, "❌ У вас нет доступа к админ-панели")
            return
        
        text = "👨‍💼 <b>Панель администратора</b>\n\nВыберите действие:"
        self.send_message(chat_id, text, self.admin_menu_inline_keyboard())
    
    def show_classes_management(self, chat_id, username):
        text = "🏫 <b>Управление классами</b>\n\nВыберите действие:"
        self.send_message(chat_id, text, self.classes_management_inline_keyboard())
    
    def show_bells_management(self, chat_id, username):
        text = "🕧 <b>Управление расписанием звонков</b>\n\nВыберите действие:"
        self.send_message(chat_id, text, self.bells_management_inline_keyboard())
    
    def start_add_class(self, chat_id, username):
        self.admin_states[username] = {"action": "add_class_input"}
        self.send_message(
            chat_id,
            "Введите название класса для добавления:\n\n"
            "Формат: 5А, 10П, 11Р и т.д.\n"
            "Доступные классы: 5-9 классы (А, Б, В), 10-11 классы (П, Р)",
            self.cancel_keyboard()
        )
    
    def start_delete_class(self, chat_id, username):
        self.admin_states[username] = {"action": "delete_class_input"}
        
        classes = self.get_existing_classes()
        classes_text = "Существующие классы:\n" + "\n".join(classes) if classes else "❌ Нет зарегистрированных классов"
        
        self.send_message(
            chat_id,
            f"{classes_text}\n\nВведите название класса для удаления:",
            self.cancel_keyboard()
        )
    
    def start_edit_bell(self, chat_id, username):
        self.admin_states[username] = {"action": "edit_bell_number"}
        self.send_message(
            chat_id,
            "Введите номер урока для изменения (1-7):",
            self.cancel_keyboard()
        )
    
    def show_all_bells(self, chat_id):
        bells = self.get_bell_schedule()
        bells_text = "🔔 <b>Текущее расписание звонков</b>\n\n"
        for bell in bells:
            bells_text += f"{bell[0]}. {bell[1]} - {bell[2]}\n"
        self.send_message(chat_id, bells_text)
    
    def handle_class_input(self, chat_id, username, text):
        if username not in self.admin_states:
            return
        
        action = self.admin_states[username].get("action")
        class_name = text.strip().upper()
        
        if not self.is_valid_class(class_name):
            self.send_message(chat_id, "❌ Неверный формат класса", self.admin_menu_inline_keyboard())
            del self.admin_states[username]
            return
        
        if action == "add_class_input":
            if self.add_class(class_name):
                self.send_message(chat_id, f"✅ Класс {class_name} доступен для регистрации", self.admin_menu_inline_keyboard())
            else:
                self.send_message(chat_id, f"❌ Неверный формат класса", self.admin_menu_inline_keyboard())
        elif action == "delete_class_input":
            if self.delete_class(class_name):
                self.send_message(chat_id, f"✅ Класс {class_name} и все связанные пользователи удалены", self.admin_menu_inline_keyboard())
            else:
                self.send_message(chat_id, f"❌ Класс {class_name} не найден или в нем нет пользователей", self.admin_menu_inline_keyboard())
        
        del self.admin_states[username]
    
    def handle_bell_input(self, chat_id, username, text):
        if username not in self.admin_states:
            return
        
        state = self.admin_states[username]
        
        if state.get("action") == "edit_bell_number":
            try:
                lesson_number = int(text)
                if 1 <= lesson_number <= 7:
                    state["action"] = "edit_bell_start"
                    state["lesson_number"] = lesson_number
                    self.send_message(chat_id, f"Урок {lesson_number}. Введите время начала (формат ЧЧ:ММ):", self.cancel_keyboard())
                else:
                    self.send_message(chat_id, "❌ Номер урока должен быть от 1 до 7", self.bells_management_inline_keyboard())
                    del self.admin_states[username]
            except ValueError:
                self.send_message(chat_id, "❌ Введите число от 1 до 7", self.bells_management_inline_keyboard())
                del self.admin_states[username]
        
        elif state.get("action") == "edit_bell_start":
            if self.is_valid_time(text):
                state["action"] = "edit_bell_end"
                state["start_time"] = text
                self.send_message(chat_id, f"Введите время окончания (формат ЧЧ:ММ):", self.cancel_keyboard())
            else:
                self.send_message(chat_id, "❌ Неверный формат времени. Используйте ЧЧ:ММ", self.bells_management_inline_keyboard())
                del self.admin_states[username]
        
        elif state.get("action") == "edit_bell_end":
            if self.is_valid_time(text):
                lesson_number = state["lesson_number"]
                start_time = state["start_time"]
                end_time = text
                
                if self.update_bell_schedule(lesson_number, start_time, end_time):
                    self.send_message(chat_id, f"✅ Звонок для урока {lesson_number} обновлен: {start_time} - {end_time}", self.bells_management_inline_keyboard())
                else:
                    self.send_message(chat_id, f"❌ Ошибка обновления звонка", self.bells_management_inline_keyboard())
                
                del self.admin_states[username]
            else:
                self.send_message(chat_id, "❌ Неверный формат времени. Используйте ЧЧ:ММ", self.bells_management_inline_keyboard())
                del self.admin_states[username]
    
    def handle_main_menu(self, chat_id, user_id, text, username):
        user_data = self.get_user(user_id)
        
        if text == "📚 Моё расписание":
            if not user_data:
                self.send_message(
                    chat_id,
                    "❌ Вы не зарегистрированы. Пожалуйста, введите своё ФИО и класс для регистрации."
                )
                return
            
            class_name = user_data[2]
            self.user_states[user_id] = {"action": "my_schedule", "class": class_name}
            self.send_message(
                chat_id,
                f"Выберите день недели для расписания {self.safe_message(class_name)} класса:",
                self.day_selection_inline_keyboard()
            )
        
        elif text == "🏫 Общее расписание":
            self.user_states[user_id] = {"action": "general_schedule"}
            self.send_message(
                chat_id,
                "Выберите класс:",
                self.class_selection_keyboard()
            )
        
        elif text == "🔔 Звонки":
            bells = self.get_bell_schedule()
            bells_text = "🔔 <b>Расписание звонков</b>\n\n"
            for bell in bells:
                bells_text += f"{bell[0]}. {bell[1]} - {bell[2]}\n"
                if bell[0] == 4:
                    bells_text += "    ⏰ Перемена 15 минут\n"
                elif bell[0] == 5:
                    bells_text += "    ⏰ Перемена 5 минут\n"
                elif bell[0] < 7:
                    bells_text += "    ⏰ Перемена 10 минут\n"
            
            bells_text += "\n📝 Уроки по 40 минут"
            self.send_message(chat_id, bells_text)
        
        elif text == "ℹ️ Помощь":
            self.handle_help(chat_id, username)
        
        elif text == "⬅️ Назад":
            if user_id in self.user_states:
                del self.user_states[user_id]
            self.send_message(chat_id, "Главное меню", self.main_menu_keyboard())
        
        elif self.is_valid_class(text):
            self.handle_class_selection(chat_id, user_id, text)
    
    def handle_callback_query(self, update):
        callback_query = update.get("callback_query")
        if not callback_query:
            return
            
        chat_id = callback_query["message"]["chat"]["id"]
        user = callback_query["from"]
        user_id = user["id"]
        username = user.get("username", "")
        data = callback_query["data"]
        
        logger.info(f"Callback received: {data} from user {username}")
        
        if data.startswith("day_"):
            day_code = data[4:]
            day_map = {
                'monday': 'понедельник',
                'tuesday': 'вторник', 
                'wednesday': 'среда',
                'thursday': 'четверг',
                'friday': 'пятница',
                'saturday': 'суббота'
            }
            day_text = day_map.get(day_code, day_code)
            
            # Проверяем, является ли это действием администратора по редактированию расписания
            if username in self.admin_states and self.admin_states[username].get("action") == "edit_schedule_day":
                logger.info(f"Admin schedule day selection: {day_text}")
                self.handle_schedule_day_selection(chat_id, username, day_text)
            else:
                logger.info(f"User day selection: {day_text}")
                self.handle_day_selection(chat_id, user_id, day_text)
            
        elif data.startswith("admin_"):
            logger.info(f"Admin callback: {data}")
            self.handle_admin_callback(chat_id, username, data)
            
        self.answer_callback_query(callback_query["id"])
    
    def handle_admin_callback(self, chat_id, username, data):
        if not self.is_admin(username):
            self.log_security_event("unauthorized_admin_access", chat_id, f"Username: {username}")
            self.send_message(chat_id, "❌ У вас нет доступа к админ-панели")
            return
        
        if data == "admin_users":
            self.show_users_list(chat_id)
        elif data == "admin_delete_user":
            self.start_delete_user(chat_id, username)
        elif data == "admin_edit_schedule":
            self.start_edit_schedule(chat_id, username)
        elif data == "admin_manage_classes":
            self.show_classes_management(chat_id, username)
        elif data == "admin_bells":
            self.show_bells_management(chat_id, username)
        elif data == "admin_upload_excel":
            self.send_message(
                chat_id,
                "📤 <b>Загрузка расписания из Excel</b>\n\n"
                "Выберите смену для загрузки:",
                self.shift_selection_keyboard()
            )
            self.admin_states[username] = {"action": "select_shift"}
        elif data == "admin_stats":
            self.show_statistics(chat_id)
        elif data == "admin_back":
            if username in self.admin_states:
                del self.admin_states[username]
            self.send_message(chat_id, "Главное меню", self.main_menu_keyboard())
        elif data == "admin_add_class":
            self.start_add_class(chat_id, username)
        elif data == "admin_delete_class":
            self.start_delete_class(chat_id, username)
        elif data == "admin_edit_bell":
            self.start_edit_bell(chat_id, username)
        elif data == "admin_view_bells":
            self.show_all_bells(chat_id)
    
    def answer_callback_query(self, callback_query_id, text=None):
        url = f"{BASE_URL}/answerCallbackQuery"
        data = {"callback_query_id": callback_query_id}
        if text:
            data["text"] = text
        
        try:
            response = requests.post(url, json=data, timeout=10)
            return response.json()
        except Exception as e:
            logger.error(f"Ошибка ответа на callback: {e}")
            return None
    
    def handle_day_selection(self, chat_id, user_id, day_text):
        if user_id not in self.user_states:
            logger.error(f"User state not found for user {user_id}")
            self.send_message(chat_id, "❌ Ошибка: действие не найдено", self.main_menu_keyboard())
            return
        
        state = self.user_states[user_id]
        day_map = {
            'понедельник': 'monday',
            'вторник': 'tuesday',
            'среда': 'wednesday',
            'четверг': 'thursday',
            'пятница': 'friday',
            'суббота': 'saturday'
        }
        
        day_code = day_map.get(day_text.lower())
        if not day_code:
            self.send_message(chat_id, "❌ Неверный день недели", self.main_menu_keyboard())
            return
        
        if state.get("action") == "my_schedule":
            class_name = state.get("class")
            if not class_name:
                self.send_message(chat_id, "❌ Ошибка: класс не найден", self.main_menu_keyboard())
                return
            
            self.show_schedule(chat_id, class_name, day_code, day_text)
        
        elif state.get("action") == "general_schedule":
            class_name = state.get("selected_class")
            if not class_name:
                self.send_message(chat_id, "❌ Ошибка: класс не выбран", self.main_menu_keyboard())
                return
            
            self.show_schedule(chat_id, class_name, day_code, day_text)
    
    def handle_class_selection(self, chat_id, user_id, class_name):
        if user_id not in self.user_states:
            self.send_message(chat_id, "❌ Ошибка: действие не найдено", self.main_menu_keyboard())
            return
        
        state = self.user_states[user_id]
        
        if state.get("action") == "general_schedule":
            self.user_states[user_id] = {
                "action": "general_schedule",
                "selected_class": class_name
            }
            self.send_message(
                chat_id,
                f"Выбран класс: {class_name}\nТеперь выберите день недели:",
                self.day_selection_inline_keyboard()
            )
    
    def show_schedule(self, chat_id, class_name, day_code, day_name):
        schedule = self.get_schedule(class_name, day_code)
        
        if schedule:
            schedule_text = f"📅 <b>Расписание {self.safe_message(class_name)} класса</b>\n{day_name}\n\n"
            for lesson in schedule:
                schedule_text += f"{lesson[0]}. <b>{self.safe_message(lesson[1])}</b>"
                if lesson[2]:
                    schedule_text += f" ({self.safe_message(lesson[2])})"
                if lesson[3]:
                    schedule_text += f" - {self.safe_message(lesson[3])}"
                schedule_text += "\n"
        else:
            schedule_text = f"❌ Расписание для {self.safe_message(class_name)} класса на {day_name.lower()} не найдено"
        
        self.send_message(chat_id, schedule_text, self.main_menu_keyboard())
    
    def handle_admin_menu(self, chat_id, username, text):
        if not self.is_admin(username):
            self.log_security_event("unauthorized_admin_action", chat_id, f"Action: {text}")
            self.send_message(chat_id, "❌ У вас нет доступа к этой функции")
            return
        
        if text == "👥 Список пользователей":
            self.show_users_list(chat_id)
        elif text == "❌ Удалить пользователя":
            self.start_delete_user(chat_id, username)
        elif text == "📝 Редактировать расписание":
            self.start_edit_schedule(chat_id, username)
        elif text == "🏫 Управление классами":
            self.show_classes_management(chat_id, username)
        elif text == "🕧 Управление звонками":
            self.show_bells_management(chat_id, username)
        elif text == "📤 Загрузить Excel":
            self.send_message(
                chat_id,
                "📤 <b>Загрузка расписания из Excel</b>\n\n"
                "Выберите смену для загрузки:",
                self.shift_selection_keyboard()
            )
            self.admin_states[username] = {"action": "select_shift"}
        elif text == "📊 Статистика":
            self.show_statistics(chat_id)
        elif text == "⬅️ Назад":
            self.send_message(chat_id, "Главное меню", self.main_menu_keyboard())
        elif text in ["1 смена", "2 смена"]:
            self.handle_shift_selection(chat_id, username, text)
    
    def handle_shift_selection(self, chat_id, username, shift_text):
        if username not in self.admin_states:
            return
        
        shift = "1" if shift_text == "1 смена" else "2"
        self.admin_states[username] = {"action": "waiting_excel", "shift": shift}
        
        self.send_message(
            chat_id,
            f"📤 <b>Загрузка расписания для {shift_text}</b>\n\n"
            f"Отправьте Excel файл с расписанием для {shift_text}.\n"
            f"После загрузки файла расписание для {shift_text} будет автоматически обновлено.",
            self.cancel_keyboard()
        )
    
    def show_users_list(self, chat_id):
        users = self.get_all_users()
        
        if not users:
            self.send_message(chat_id, "❌ Нет зарегистрированных пользователей")
            return
        
        users_text = "👥 <b>Список пользователей</b>\n\n"
        for user in users:
            reg_date = user[3].split()[0] if user[3] else "неизвестно"
            users_text += f"👤 {self.safe_message(user[1])} - {self.safe_message(user[2])} (ID: {user[0]})\n"
            users_text += f"   📅 Зарегистрирован: {reg_date}\n\n"
        
        self.send_message(chat_id, users_text, self.admin_menu_inline_keyboard())
    
    def start_delete_user(self, chat_id, username):
        self.admin_states[username] = {"action": "delete_user"}
        self.send_message(
            chat_id,
            "Введите ID пользователя для удаления:\n\n"
            "ID можно узнать через команду '👥 Список пользователей'",
            self.cancel_keyboard()
        )
    
    def delete_user_by_id(self, chat_id, admin_username, user_id_str):
        try:
            user_id = int(user_id_str)
            if not self.is_valid_user_id(user_id):
                self.send_message(chat_id, "❌ Неверный формат ID пользователя", self.admin_menu_inline_keyboard())
                return
                
            if self.delete_user(user_id):
                self.log_security_event("user_deleted", admin_username, f"Deleted user: {user_id}")
                self.send_message(chat_id, f"✅ Пользователь с ID {user_id} удален", self.admin_menu_inline_keyboard())
            else:
                self.send_message(chat_id, f"❌ Пользователь с ID {user_id} не найден", self.admin_menu_inline_keyboard())
        except ValueError:
            self.send_message(chat_id, "❌ Неверный формат ID. ID должен быть числом", self.admin_menu_inline_keyboard())
        
        if admin_username in self.admin_states:
            del self.admin_states[admin_username]
    
    def start_edit_schedule(self, chat_id, username):
        self.admin_states[username] = {"action": "edit_schedule_class"}
        self.send_message(
            chat_id,
            "Выберите класс для редактирования расписания:",
            self.class_selection_keyboard()
        )
    
    def handle_schedule_class_selection(self, chat_id, username, class_name):
        if username not in self.admin_states:
            return
        
        self.admin_states[username] = {
            "action": "edit_schedule_day",
            "class": class_name
        }
        
        self.send_message(
            chat_id,
            f"Выбран класс: {self.safe_message(class_name)}\nТеперь выберите день недели:",
            self.day_selection_inline_keyboard()
        )
    
    def handle_schedule_day_selection(self, chat_id, username, day_name):
        logger.info(f"Handling schedule day selection for {username}, day: {day_name}")
        
        if username not in self.admin_states:
            logger.error(f"Admin state not found for {username}")
            self.send_message(chat_id, "❌ Ошибка: действие не найдено", self.admin_menu_inline_keyboard())
            return
        
        class_name = self.admin_states[username].get("class")
        if not class_name:
            logger.error(f"Class not found in admin state for {username}")
            self.send_message(chat_id, "❌ Ошибка: класс не выбран", self.admin_menu_inline_keyboard())
            return
        
        day_map = {
            "понедельник": "monday",
            "вторник": "tuesday",
            "среда": "wednesday",
            "четверг": "thursday",
            "пятница": "friday",
            "суббота": "saturday"
        }
        
        day_code = day_map.get(day_name.lower(), day_name.lower())
        
        current_schedule = self.get_schedule(class_name, day_code)
        
        schedule_text = ""
        if current_schedule:
            schedule_text = "<b>Текущее расписание:</b>\n"
            for lesson in current_schedule:
                schedule_text += f"{lesson[0]}. {self.safe_message(lesson[1])}"
                if lesson[2]:
                    schedule_text += f" ({self.safe_message(lesson[2])})"
                if lesson[3]:
                    schedule_text += f" - {self.safe_message(lesson[3])}"
                schedule_text += "\n"
            schedule_text += "\n"
        
        self.admin_states[username] = {
            "action": "edit_schedule_input",
            "class": class_name,
            "day": day_code
        }
        
        self.send_message(
            chat_id,
            f"Редактирование расписания:\n"
            f"Класс: {self.safe_message(class_name)}\n"
            f"День: {day_name}\n\n"
            f"{schedule_text}"
            f"Введите новое расписание в формате:\n\n"
            f"<code>1. Математика\n2. Физика (Иванов) - 201\n3. Химия - 301</code>\n\n"
            f"Или отправьте '-' для очистки расписания.",
            self.cancel_keyboard()
        )
    
    def handle_schedule_input(self, chat_id, username, text):
        if username not in self.admin_states:
            return
        
        class_name = self.admin_states[username].get("class")
        day_code = self.admin_states[username].get("day")
        
        if not class_name or not day_code:
            self.send_message(chat_id, "❌ Ошибка: данные не найдены", self.admin_menu_inline_keyboard())
            return
        
        if text == '-':
            self.save_schedule(class_name, day_code, [])
            self.send_message(chat_id, "✅ Расписание очищено!", self.admin_menu_inline_keyboard())
        else:
            lessons = []
            lines = text.split('\n')
            
            for line in lines:
                line = line.strip()
                if not line or not line[0].isdigit():
                    continue
                    
                parts = line.split('.', 1)
                if len(parts) < 2:
                    continue
                    
                try:
                    lesson_num = int(parts[0].strip())
                    lesson_info = parts[1].strip()
                    
                    subject = lesson_info
                    teacher = ""
                    room = ""
                    
                    if '(' in lesson_info and ')' in lesson_info:
                        start = lesson_info.find('(')
                        end = lesson_info.find(')')
                        teacher = lesson_info[start+1:end]
                        subject = lesson_info[:start].strip()
                        lesson_info = lesson_info[end+1:].strip()
                    
                    if ' - ' in lesson_info:
                        room_parts = lesson_info.split(' - ', 1)
                        subject = subject if subject else room_parts[0].strip()
                        room = room_parts[1].strip()
                    elif lesson_info and not subject:
                        subject = lesson_info
                    
                    if subject:
                        lessons.append((lesson_num, subject, teacher, room))
                except ValueError:
                    continue
            
            self.save_schedule(class_name, day_code, lessons)
            self.send_message(chat_id, f"✅ Расписание для {self.safe_message(class_name)} класса обновлено!", self.admin_menu_inline_keyboard())
        
        if username in self.admin_states:
            del self.admin_states[username]
    
    def show_statistics(self, chat_id):
        users = self.get_all_users()
        total_users = len(users)
        
        classes = {}
        for user in users:
            class_name = user[2]
            if class_name in classes:
                classes[class_name] += 1
            else:
                classes[class_name] = 1
        
        stats_text = "📊 <b>Статистика бота</b>\n\n"
        stats_text += f"👥 Всего пользователей: {total_users}\n\n"
        
        if classes:
            stats_text += "<b>Распределение по классам:</b>\n"
            for class_name, count in sorted(classes.items()):
                stats_text += f"• {self.safe_message(class_name)}: {count} чел.\n"
        
        self.send_message(chat_id, stats_text, self.admin_menu_inline_keyboard())
    
    def handle_registration(self, chat_id, user_id, text):
        if self.get_user(user_id):
            self.send_message(chat_id, "Вы уже зарегистрированы!", self.main_menu_keyboard())
            return
        
        parts = text.split(',')
        if len(parts) != 2:
            self.send_message(
                chat_id,
                "❌ Неверный формат. Пожалуйста, введите данные в формате:\n"
                "<b>Фамилия Имя Отчество, Класс</b>\n\n"
                "Например: <i>Иванов Иван Иванович, 10П</i>\n\n"
                "<b>Доступные классы:</b>\n"
                "5-9 классы: А, Б, В\n"
                "10 класс: П, Р\n"
                "11 класс: Р"
            )
            return
        
        full_name = parts[0].strip()
        class_name = parts[1].strip()
        
        if not self.is_valid_fullname(full_name):
            self.send_message(
                chat_id,
                "❌ Неверный формат ФИО. ФИО должно содержать как минимум 2 слова, "
                "состоять только из букв и каждое слово должно быть от 2 до 20 символов."
            )
            return
        
        if not self.is_valid_class(class_name):
            self.send_message(
                chat_id,
                "❌ Неверный формат класса.\n\n"
                "<b>Доступные классы:</b>\n"
                "5-9 классы: А, Б, В\n"
                "10 класс: П, Р\n"
                "11 класс: Р\n\n"
                "Пример: 5А, 10П, 11Р"
            )
            return
        
        class_name = class_name.upper()
        if self.create_user(user_id, full_name, class_name):
            self.send_message(
                chat_id,
                f"✅ Регистрация прошла успешно!\nФИО: {self.safe_message(full_name)}\nКласс: {class_name}",
                self.main_menu_keyboard()
            )
        else:
            self.send_message(
                chat_id,
                f"❌ Не удалось зарегистрироваться. Возможно, достигнут лимит пользователей в классе {class_name}.",
                self.main_menu_keyboard()
            )
    
    def process_update(self, update):
        update_id = update.get("update_id")
        
        if update_id in self.processed_updates:
            logger.info(f"Пропускаем уже обработанное обновление: {update_id}")
            return
        
        self.processed_updates.add(update_id)
        
        if len(self.processed_updates) > 1000:
            self.processed_updates = set(list(self.processed_updates)[-500:])
        
        try:
            if "callback_query" in update:
                self.handle_callback_query(update)
                return
            
            if "message" in update:
                message = update["message"]
                chat_id = message["chat"]["id"]
                user = message.get("from", {})
                user_id = user.get("id")
                username = user.get("username", "")
                
                if user_id and self.rate_limiter.is_limited(user_id):
                    self.log_security_event("rate_limit_exceeded", user_id, f"Username: {username}")
                    self.send_message(chat_id, "⚠️ Слишком много запросов. Пожалуйста, подождите.")
                    return
                
                if "document" in message and username in self.admin_states and self.admin_states[username].get("action") == "waiting_excel":
                    document = message["document"]
                    file_id = document["file_id"]
                    file_name = document.get("file_name", "")
                    shift = self.admin_states[username].get("shift", "1")
                    
                    if not file_name.lower().endswith(('.xlsx', '.xls')):
                        self.send_message(chat_id, "❌ Пожалуйста, отправьте файл в формате Excel (.xlsx или .xls)")
                        return
                    
                    self.send_message(chat_id, f"📥 Начинаю загрузку файла для {shift} смены...")
                    
                    file_info = self.get_file(file_id)
                    if not file_info:
                        self.send_message(chat_id, "❌ Ошибка получения информации о файле")
                        return
                    
                    file_content = self.download_file(file_info["file_path"])
                    if not file_content:
                        self.send_message(chat_id, "❌ Ошибка загрузки файла")
                        return
                    
                    self.send_message(chat_id, f"🔍 Обрабатываю расписание для {shift} смены...")
                    
                    success, message = self.import_schedule_from_excel(file_content, shift)
                    
                    if success:
                        self.send_message(chat_id, f"✅ {message}", self.admin_menu_inline_keyboard())
                    else:
                        self.send_message(chat_id, f"❌ {message}", self.admin_menu_inline_keyboard())
                    
                    if username in self.admin_states:
                        del self.admin_states[username]
                    return
                
                if "text" in message:
                    text = message["text"]
                    
                    if text == "❌ Отменить":
                        if username in self.admin_states:
                            del self.admin_states[username]
                        if user_id in self.user_states:
                            del self.user_states[user_id]
                        self.send_message(chat_id, "Действие отменено", self.main_menu_keyboard())
                        return
                    
                    if username in self.admin_states:
                        state = self.admin_states[username]
                        
                        if state.get("action") in ["add_class_input", "delete_class_input"]:
                            self.handle_class_input(chat_id, username, text)
                            return
                        
                        if state.get("action") in ["edit_bell_number", "edit_bell_start", "edit_bell_end"]:
                            self.handle_bell_input(chat_id, username, text)
                            return
                        
                        if state.get("action") == "delete_user":
                            self.delete_user_by_id(chat_id, username, text)
                            return
                        elif state.get("action") == "edit_schedule_input":
                            self.handle_schedule_input(chat_id, username, text)
                            return
                        elif state.get("action") == "edit_schedule_class":
                            self.handle_schedule_class_selection(chat_id, username, text)
                            return
                        elif state.get("action") == "edit_schedule_day":
                            self.handle_schedule_day_selection(chat_id, username, text)
                            return
                        elif state.get("action") == "select_shift":
                            self.handle_shift_selection(chat_id, username, text)
                            return
                    
                    if text.startswith("/start"):
                        self.handle_start(chat_id, user)
                    elif text.startswith("/help"):
                        self.handle_help(chat_id, username)
                    elif text.startswith("/admin_panel"):
                        self.handle_admin_panel(chat_id, username)
                    elif text in ["📚 Моё расписание", "🏫 Общее расписание", "🔔 Звонки", "ℹ️ Помощь"]:
                        self.handle_main_menu(chat_id, user_id, text, username)
                    elif text in ["👥 Список пользователей", "❌ Удалить пользователя", "📝 Редактировать расписание", 
                                  "🏫 Управление классами", "🕧 Управление звонками", "📤 Загрузить Excel", "📊 Статистика", "⬅️ Назад"]:
                        self.handle_admin_menu(chat_id, username, text)
                    elif text in ["1 смена", "2 смена"]:
                        self.handle_shift_selection(chat_id, username, text)
                    elif text == "⬅️ Назад" or self.is_valid_class(text):
                        self.handle_main_menu(chat_id, user_id, text, username)
                    else:
                        self.handle_registration(chat_id, user_id, text)
        
        except Exception as e:
            logger.error(f"Ошибка в process_update: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def run(self):
        logger.info("Бот запущен")
        
        try:
            delete_url = f"{BASE_URL}/deleteWebhook"
            response = requests.get(delete_url, timeout=10)
            if response.json().get("ok"):
                logger.info("Вебхук очищен, используется long polling")
            else:
                logger.warning("Не удалось очистить вебхук")
        except Exception as e:
            logger.error(f"Ошибка при очистке вебхука: {e}")
        
        conflict_count = 0
        max_conflicts = 5
        
        while True:
            try:
                updates = self.get_updates()
                
                if updates.get("conflict"):
                    conflict_count += 1
                    logger.warning(f"Обнаружен конфликт getUpdates ({conflict_count}/{max_conflicts})")
                    
                    if conflict_count >= max_conflicts:
                        logger.error("Достигнуто максимальное количество конфликтов. Завершаем работу.")
                        break
                    
                    time.sleep(10)
                    continue
                else:
                    conflict_count = 0
                
                if updates.get("ok") and "result" in updates:
                    for update in updates["result"]:
                        self.last_update_id = update["update_id"]
                        self.process_update(update)
                else:
                    if "description" in updates:
                        error_desc = updates.get('description', '')
                        if "Conflict" not in error_desc:
                            logger.error(f"Ошибка Telegram API: {error_desc}")
                
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Ошибка в основном цикле: {e}")
                time.sleep(5)

if __name__ == "__main__":
    bot = SimpleSchoolBot()
    bot.run()