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
                "INSERT INTO bell_schedule (lesson_number, start_time, end_time) VALUES (?, ?, ?)",
                bell_schedule
            )
        
        self.conn.commit()
    
    def safe_message(self, text):
        if not text:
            return ""
        text = str(text)
        text = text.replace('<b>', '___BOLD_OPEN___')
        text = text.replace('</b>', '___BOLD_CLOSE___')
        text = escape(text)
        text = text.replace('___BOLD_OPEN___', '<b>')
        text = text.replace('___BOLD_CLOSE___', '</b>')
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
                "INSERT INTO schedule (class, day, lesson_number, subject, teacher, room) VALUES (?, ?, ?, ?, ?, ?)",
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
    
    def admin_menu_keyboard(self):
        return {
            "keyboard": [
                [{"text": "👥 Список пользователей"}, {"text": "❌ Удалить пользователя"}],
                [{"text": "📝 Редактировать расписание"}, {"text": "🏫 Управление классами"}],
                [{"text": "🕧 Управление звонками"}, {"text": "📤 Загрузить Excel"}],
                [{"text": "📊 Статистика"}, {"text": "⬅️ Назад"}]
            ],
            "resize_keyboard": True
        }
    
    def classes_management_keyboard(self):
        return {
            "keyboard": [
                [{"text": "➕ Добавить класс"}, {"text": "➖ Удалить класс"}],
                [{"text": "⬅️ Назад в админку"}]
            ],
            "resize_keyboard": True
        }
    
    def bells_management_keyboard(self):
        return {
            "keyboard": [
                [{"text": "✏️ Изменить звонок"}, {"text": "👀 Посмотреть все звонки"}],
                [{"text": "⬅️ Назад в админку"}]
            ],
            "resize_keyboard": True
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
            row.append({"text": cls, "callback_data": f"class_{cls}"})
            if (i + 1) % 3 == 0:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        
        return {"inline_keyboard": keyboard}
    
    def day_selection_keyboard(self, class_name=None):
        days = [
            ("Понедельник", "monday"),
            ("Вторник", "tuesday"),
            ("Среда", "wednesday"),
            ("Четверг", "thursday"),
            ("Пятница", "friday"),
            ("Суббота", "saturday")
        ]
        
        keyboard = []
        for day_name, day_code in days:
            if class_name:
                callback_data = f"schedule_{class_name}_{day_code}"
            else:
                callback_data = f"day_{day_code}"
            keyboard.append([{"text": day_name, "callback_data": callback_data}])
        
        return {"inline_keyboard": keyboard}
    
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

    def parse_excel_schedule(self, file_content):
        """Парсинг Excel файла с расписанием"""
        try:
            # Читаем оба листа
            df_first_shift = pd.read_excel(io.BytesIO(file_content), sheet_name='1 СМЕНА', header=None)
            df_second_shift = pd.read_excel(io.BytesIO(file_content), sheet_name='2 СМЕНА', header=None)
            
            lessons_data = []
            
            # Парсим первую смену
            self._parse_shift_schedule(df_first_shift, '1', lessons_data)
            
            # Парсим вторую смену
            self._parse_shift_schedule(df_second_shift, '2', lessons_data)
            
            return lessons_data
        except Exception as e:
            logger.error(f"Ошибка парсинга Excel: {e}")
            return None

    def _parse_shift_schedule(self, df, shift, lessons_data):
        """Парсинг расписания для одной смены"""
        # Находим строку с заголовками классов
        header_row = None
        for i in range(len(df)):
            row = df.iloc[i]
            if row.isna().all():
                continue
            for cell in row:
                if isinstance(cell, str) and '5а' in cell.lower():
                    header_row = i
                    break
            if header_row is not None:
                break
        
        if header_row is None:
            return
        
        # Собираем информацию о колонках для каждого класса
        class_columns = {}
        header_cells = df.iloc[header_row]
        
        current_class = None
        for col_idx, cell in enumerate(header_cells):
            if pd.isna(cell):
                continue
                
            cell_str = str(cell).strip()
            
            # Определяем класс
            if any(class_pattern in cell_str for class_pattern in ['5а', '5б', '5в', '6а', '6б', '6в', '6г', 
                                                                  '7а', '7б', '7в', '8а', '8б', '8в', 
                                                                  '9а', '9б', '9р', '10п', '10р', '11р']):
                current_class = cell_str
                class_columns[current_class] = {'subject_col': col_idx, 'room_col': col_idx + 1}
            elif cell_str.lower() == 'каб' and current_class:
                class_columns[current_class]['room_col'] = col_idx
        
        # Парсим расписание по дням
        current_day = None
        for i in range(header_row + 1, len(df)):
            row = df.iloc[i]
            
            # Проверяем, является ли строка днем недели
            day_cell = row[0] if len(row) > 0 else None
            if not pd.isna(day_cell) and isinstance(day_cell, str):
                day_name = day_cell.strip().lower()
                if any(day in day_name for day in ['понедельник', 'вторник', 'среда', 'четверг', 'пятница', 'суббота']):
                    current_day = day_name
                    continue
            
            if current_day and not pd.isna(row[1]) and str(row[1]).strip().isdigit():
                lesson_num = int(str(row[1]).strip())
                
                for class_name, cols in class_columns.items():
                    subject_col = cols.get('subject_col')
                    room_col = cols.get('room_col')
                    
                    if subject_col and len(row) > subject_col and not pd.isna(row[subject_col]):
                        subject = str(row[subject_col]).strip()
                        room = str(row[room_col]).strip() if room_col and len(row) > room_col and not pd.isna(row[room_col]) else ""
                        
                        if subject and subject not in ['', 'nan', 'None']:
                            # Нормализуем названия дней
                            day_map = {
                                'понедельник': 'monday',
                                'вторник': 'tuesday',
                                'среда': 'wednesday',
                                'четверг': 'thursday',
                                'пятница': 'friday',
                                'суббота': 'saturday'
                            }
                            
                            day_code = day_map.get(current_day, current_day)
                            lessons_data.append({
                                'class': class_name,
                                'day': day_code,
                                'lesson_number': lesson_num,
                                'subject': subject,
                                'room': room
                            })

    def import_schedule_from_excel(self, file_content):
        """Импорт расписания из Excel в базу данных"""
        try:
            lessons_data = self.parse_excel_schedule(file_content)
            if not lessons_data:
                return False, "Не удалось распарсить Excel файл"
            
            # Очищаем существующее расписание
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM schedule")
            
            # Добавляем новые данные
            for lesson in lessons_data:
                cursor.execute(
                    "INSERT INTO schedule (class, day, lesson_number, subject, teacher, room) VALUES (?, ?, ?, ?, ?, ?)",
                    (lesson['class'], lesson['day'], lesson['lesson_number'], lesson['subject'], '', lesson['room'])
                )
            
            self.conn.commit()
            return True, f"Успешно импортировано {len(lessons_data)} уроков"
        except Exception as e:
            logger.error(f"Ошибка импорта из Excel: {e}")
            return False, f"Ошибка импорта: {str(e)}"
    
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
        self.send_message(chat_id, text, self.admin_menu_keyboard())
    
    def show_classes_management(self, chat_id, username):
        self.admin_states[username] = {"menu": "classes_management"}
        self.send_message(chat_id, "🏫 Управление классами", self.classes_management_keyboard())
    
    def show_bells_management(self, chat_id, username):
        self.admin_states[username] = {"menu": "bells_management"}
        self.send_message(chat_id, "🕧 Управление расписанием звонков", self.bells_management_keyboard())
    
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
    
    def handle_management_menus(self, chat_id, username, text):
        if text == "➕ Добавить класс":
            self.start_add_class(chat_id, username)
        elif text == "➖ Удалить класс":
            self.start_delete_class(chat_id, username)
        elif text == "✏️ Изменить звонок":
            self.start_edit_bell(chat_id, username)
        elif text == "👀 Посмотреть все звонки":
            self.show_all_bells(chat_id)
        elif text == "⬅️ Назад в админку":
            self.handle_admin_panel(chat_id, username)
        elif text == "📤 Загрузить Excel":
            self.send_message(
                chat_id,
                "📤 <b>Загрузка расписания из Excel</b>\n\n"
                "Отправьте Excel файл с расписанием.\n"
                "Файл должен иметь два листа: '1 СМЕНА' и '2 СМЕНА'.\n\n"
                "После загрузки файла расписание будет автоматически обновлено.",
                self.cancel_keyboard()
            )
            self.admin_states[username] = {"action": "waiting_excel"}
    
    def handle_class_input(self, chat_id, username, text):
        if username not in self.admin_states:
            return
        
        action = self.admin_states[username].get("action")
        class_name = text.strip().upper()
        
        if not self.is_valid_class(class_name):
            self.send_message(chat_id, "❌ Неверный формат класса", self.classes_management_keyboard())
            del self.admin_states[username]
            return
        
        if action == "add_class_input":
            if self.add_class(class_name):
                self.send_message(chat_id, f"✅ Класс {class_name} доступен для регистрации", self.classes_management_keyboard())
            else:
                self.send_message(chat_id, f"❌ Неверный формат класса", self.classes_management_keyboard())
        elif action == "delete_class_input":
            if self.delete_class(class_name):
                self.send_message(chat_id, f"✅ Класс {class_name} и все связанные пользователи удалены", self.classes_management_keyboard())
            else:
                self.send_message(chat_id, f"❌ Класс {class_name} не найден или в нем нет пользователей", self.classes_management_keyboard())
        
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
                    self.send_message(chat_id, "❌ Номер урока должен быть от 1 до 7", self.bells_management_keyboard())
                    del self.admin_states[username]
            except ValueError:
                self.send_message(chat_id, "❌ Введите число от 1 до 7", self.bells_management_keyboard())
                del self.admin_states[username]
        
        elif state.get("action") == "edit_bell_start":
            if self.is_valid_time(text):
                state["action"] = "edit_bell_end"
                state["start_time"] = text
                self.send_message(chat_id, f"Введите время окончания (формат ЧЧ:ММ):", self.cancel_keyboard())
            else:
                self.send_message(chat_id, "❌ Неверный формат времени. Используйте ЧЧ:ММ", self.bells_management_keyboard())
                del self.admin_states[username]
        
        elif state.get("action") == "edit_bell_end":
            if self.is_valid_time(text):
                lesson_number = state["lesson_number"]
                start_time = state["start_time"]
                end_time = text
                
                if self.update_bell_schedule(lesson_number, start_time, end_time):
                    self.send_message(chat_id, f"✅ Звонок для урока {lesson_number} обновлен: {start_time} - {end_time}", self.bells_management_keyboard())
                else:
                    self.send_message(chat_id, f"❌ Ошибка обновления звонка", self.bells_management_keyboard())
                
                del self.admin_states[username]
            else:
                self.send_message(chat_id, "❌ Неверный формат времени. Используйте ЧЧ:ММ", self.bells_management_keyboard())
                del self.admin_states[username]
    
    def handle_main_menu(self, chat_id, user_id, text, username):
        user_data = self.get_user(user_id)
        if not user_data:
            self.send_message(
                chat_id,
                "❌ Вы не зарегистрированы. Пожалуйста, введите своё ФИО и класс для регистрации."
            )
            return
        
        if text == "📚 Моё расписание":
            class_name = user_data[2]
            self.send_message(
                chat_id,
                f"Выберите день недели для расписания {self.safe_message(class_name)} класса:",
                self.day_selection_keyboard()
            )
        elif text == "🏫 Общее расписание":
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
            
            bells_text += "\n📝 <i>Уроки по 40 минут</i>"
            self.send_message(chat_id, bells_text)
        elif text == "ℹ️ Помощь":
            self.handle_help(chat_id, username)
    
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
            self.handle_management_menus(chat_id, username, text)
        elif text == "📊 Статистика":
            self.show_statistics(chat_id)
        elif text == "⬅️ Назад":
            self.send_message(chat_id, "Главное меню", self.main_menu_keyboard())
        elif text in ["➕ Добавить класс", "➖ Удалить класс", "⬅️ Назад в админку", 
                      "✏️ Изменить звонок", "👀 Посмотреть все звонки"]:
            self.handle_management_menus(chat_id, username, text)
    
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
        
        self.send_message(chat_id, users_text)
    
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
                self.send_message(chat_id, "❌ Неверный формат ID пользователя", self.admin_menu_keyboard())
                return
                
            if self.delete_user(user_id):
                self.log_security_event("user_deleted", admin_username, f"Deleted user: {user_id}")
                self.send_message(chat_id, f"✅ Пользователь с ID {user_id} удален", self.admin_menu_keyboard())
            else:
                self.send_message(chat_id, f"❌ Пользователь с ID {user_id} не найден", self.admin_menu_keyboard())
        except ValueError:
            self.send_message(chat_id, "❌ Неверный формат ID. ID должен быть числом", self.admin_menu_keyboard())
        
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
            self.day_selection_keyboard()
        )
    
    def handle_schedule_day_selection(self, chat_id, username, day_code):
        if username not in self.admin_states:
            return
        
        class_name = self.admin_states[username].get("class")
        if not class_name:
            self.send_message(chat_id, "❌ Ошибка: класс не выбран", self.admin_menu_keyboard())
            return
        
        day_names = {
            "monday": "понедельник",
            "tuesday": "вторник", 
            "wednesday": "среду",
            "thursday": "четверг",
            "friday": "пятницу",
            "saturday": "субботу"
        }
        
        day_name = day_names.get(day_code, day_code)
        
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
            self.send_message(chat_id, "❌ Ошибка: данные не найдены", self.admin_menu_keyboard())
            return
        
        if text == '-':
            self.save_schedule(class_name, day_code, [])
            self.send_message(chat_id, "✅ Расписание очищено!", self.admin_menu_keyboard())
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
            self.send_message(chat_id, f"✅ Расписание для {self.safe_message(class_name)} класса обновлено!", self.admin_menu_keyboard())
        
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
        
        self.send_message(chat_id, stats_text)
    
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
                
                # Обработка документов (Excel файлов)
                if "document" in message and username in self.admin_states and self.admin_states[username].get("action") == "waiting_excel":
                    document = message["document"]
                    file_id = document["file_id"]
                    file_name = document.get("file_name", "")
                    
                    if not file_name.lower().endswith(('.xlsx', '.xls')):
                        self.send_message(chat_id, "❌ Пожалуйста, отправьте файл в формате Excel (.xlsx или .xls)")
                        return
                    
                    self.send_message(chat_id, "📥 Начинаю загрузку файла...")
                    
                    # Получаем информацию о файле
                    file_info = self.get_file(file_id)
                    if not file_info:
                        self.send_message(chat_id, "❌ Ошибка получения информации о файле")
                        return
                    
                    # Скачиваем файл
                    file_content = self.download_file(file_info["file_path"])
                    if not file_content:
                        self.send_message(chat_id, "❌ Ошибка загрузки файла")
                        return
                    
                    self.send_message(chat_id, "🔍 Обрабатываю расписание...")
                    
                    # Импортируем расписание
                    success, message = self.import_schedule_from_excel(file_content)
                    
                    if success:
                        self.send_message(chat_id, f"✅ {message}", self.admin_menu_keyboard())
                    else:
                        self.send_message(chat_id, f"❌ {message}", self.admin_menu_keyboard())
                    
                    if username in self.admin_states:
                        del self.admin_states[username]
                    return
                
                if "text" in message:
                    text = message["text"]
                    
                    if username in self.admin_states:
                        state = self.admin_states[username]
                        
                        if text == "❌ Отменить":
                            if username in self.admin_states:
                                del self.admin_states[username]
                            if state.get("menu") == "classes_management":
                                self.send_message(chat_id, "Действие отменено", self.classes_management_keyboard())
                            elif state.get("menu") == "bells_management":
                                self.send_message(chat_id, "Действие отменено", self.bells_management_keyboard())
                            else:
                                self.send_message(chat_id, "Действие отменено", self.admin_menu_keyboard())
                            return
                        
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
                    
                    if text.startswith("/start"):
                        self.handle_start(chat_id, user)
                    elif text.startswith("/help"):
                        self.handle_help(chat_id, username)
                    elif text.startswith("/admin_panel"):
                        self.handle_admin_panel(chat_id, username)
                    elif text in ["📚 Моё расписание", "🏫 Общее расписание", "🔔 Звонки", "ℹ️ Помощь"]:
                        self.handle_main_menu(chat_id, user_id, text, username)
                    elif text in ["👥 Список пользователей", "❌ Удалить пользователя", "📝 Редактировать расписание", 
                                  "🏫 Управление классами", "🕧 Управление звонками", "📤 Загрузить Excel", "📊 Статистика", "⬅️ Назад",
                                  "➕ Добавить класс", "➖ Удалить класс", "⬅️ Назад в админку", 
                                  "✏️ Изменить звонок", "👀 Посмотреть все звонки"]:
                        self.handle_admin_menu(chat_id, username, text)
                    else:
                        self.handle_registration(chat_id, user_id, text)
            
            elif "callback_query" in update:
                callback_query = update["callback_query"]
                data = callback_query["data"]
                chat_id = callback_query["message"]["chat"]["id"]
                user = callback_query["from"]
                username = user.get("username", "")
                
                if user.get("id") and self.rate_limiter.is_limited(user["id"]):
                    self.log_security_event("rate_limit_exceeded", user["id"], f"Callback from: {username}")
                    return
                
                self.answer_callback_query(callback_query["id"])
                
                if data.startswith("class_"):
                    class_name = data.replace("class_", "")
                    
                    if username in self.admin_states and self.admin_states[username].get("action") == "edit_schedule_class":
                        self.handle_schedule_class_selection(chat_id, username, class_name)
                    else:
                        # Для общего расписания - предлагаем выбрать день
                        self.send_message(
                            chat_id,
                            f"Выберите день недели для расписания {self.safe_message(class_name)} класса:",
                            self.day_selection_keyboard(class_name)
                        )
                
                elif data.startswith("schedule_"):
                    # Обработка выбора дня для общего расписания
                    parts = data.split("_")
                    if len(parts) >= 3:
                        class_name = parts[1]
                        day_code = "_".join(parts[2:])
                        
                        schedule = self.get_schedule(class_name, day_code)
                        
                        day_names = {
                            "monday": "Понедельник",
                            "tuesday": "Вторник",
                            "wednesday": "Среда",
                            "thursday": "Четверг", 
                            "friday": "Пятница",
                            "saturday": "Суббота"
                        }
                        
                        day_name = day_names.get(day_code, day_code)
                        
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
                        
                        self.send_message(chat_id, schedule_text)
                
                elif data.startswith("day_"):
                    day_code = data.replace("day_", "")
                    
                    if username in self.admin_states and self.admin_states[username].get("action") == "edit_schedule_day":
                        self.handle_schedule_day_selection(chat_id, username, day_code)
                    else:
                        user_data = self.get_user(user["id"])
                        if user_data:
                            class_name = user_data[2]
                            schedule = self.get_schedule(class_name, day_code)
                            
                            day_names = {
                                "monday": "Понедельник",
                                "tuesday": "Вторник",
                                "wednesday": "Среда", 
                                "thursday": "Четверг",
                                "friday": "Пятница",
                                "saturday": "Суббота"
                            }
                            
                            day_name = day_names.get(day_code, day_code)
                            
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
                            
                            self.send_message(chat_id, schedule_text)
        
        except Exception as e:
            logger.error(f"Ошибка в process_update: {e}")
    
    def answer_callback_query(self, callback_query_id):
        url = f"{BASE_URL}/answerCallbackQuery"
        data = {"callback_query_id": callback_query_id}
        try:
            requests.post(url, json=data, timeout=5)
        except Exception as e:
            logger.error(f"Ошибка ответа на callback: {e}")
    
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