import logging
import sqlite3
import requests
import time
import re
import os
import pandas as pd
from datetime import datetime, timedelta
from html import escape
from collections import defaultdict
import io
import psycopg2
from urllib.parse import urlparse
import sys
import json
import pytz
from threading import Thread
import schedule

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
WEATHER_API_KEY = os.environ.get('WEATHER_API_KEY')
SAMARA_TIMEZONE = pytz.timezone('Europe/Samara')

MAX_MESSAGE_LENGTH = 4000
MAX_USERS_PER_CLASS = 30
MAX_REQUESTS_PER_MINUTE = 20

BASE_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.conn = None
        self.db_type = None
        self.connect()
    
    def connect(self):
        database_url = os.environ.get('DATABASE_URL')
        
        if database_url:
            try:
                url = urlparse(database_url)
                self.conn = psycopg2.connect(
                    database=url.path[1:],
                    user=url.username,
                    password=url.password,
                    host=url.hostname,
                    port=url.port,
                    sslmode='require'
                )
                self.db_type = 'postgresql'
                logger.info("✅ Подключение к PostgreSQL установлено")
            except Exception as e:
                logger.error(f"❌ Ошибка подключения к PostgreSQL: {e}")
                self.fallback_to_sqlite()
        else:
            self.fallback_to_sqlite()
    
    def fallback_to_sqlite(self):
        try:
            db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "school_bot.db")
            self.conn = sqlite3.connect(db_path, check_same_thread=False)
            self.db_type = 'sqlite'
            logger.info("✅ Используется SQLite база данных")
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к SQLite: {e}")
            raise
    
    def execute(self, query, params=None):
        if self.db_type == 'postgresql':
            query = query.replace('?', '%s')
        
        cursor = self.conn.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            self.conn.commit()
            return cursor
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Ошибка выполнения запроса: {e}")
            raise e
    
    def fetchone(self, query, params=None):
        cursor = self.execute(query, params)
        return cursor.fetchone()
    
    def fetchall(self, query, params=None):
        cursor = self.execute(query, params)
        return cursor.fetchall()
    
    def close(self):
        if self.conn:
            self.conn.close()

    def create_tables(self):
        try:
            # Существующие таблицы
            self.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    full_name TEXT NOT NULL,
                    class TEXT NOT NULL,
                    role TEXT DEFAULT 'user',
                    username TEXT,
                    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            self.execute("""
                CREATE TABLE IF NOT EXISTS schedule (
                    id SERIAL PRIMARY KEY,
                    class TEXT NOT NULL,
                    day TEXT NOT NULL,
                    lesson_number INTEGER,
                    subject TEXT,
                    teacher TEXT,
                    room TEXT,
                    UNIQUE(class, day, lesson_number)
                )
            """)
            
            self.execute("""
                CREATE TABLE IF NOT EXISTS bell_schedule (
                    lesson_number INTEGER PRIMARY KEY,
                    start_time TEXT NOT NULL,
                    end_time TEXT NOT NULL
                )
            """)
            
            # НОВЫЕ ТАБЛИЦЫ
            # Настройки уведомлений
            self.execute("""
                CREATE TABLE IF NOT EXISTS notification_settings (
                    user_id BIGINT PRIMARY KEY,
                    smart_notifications BOOLEAN DEFAULT FALSE,
                    weather_notifications BOOLEAN DEFAULT FALSE,
                    news_notifications BOOLEAN DEFAULT TRUE,
                    achievement_notifications BOOLEAN DEFAULT TRUE,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            """)
            
            # Система ролей
            self.execute("""
                CREATE TABLE IF NOT EXISTS user_roles (
                    user_id BIGINT PRIMARY KEY,
                    role_type TEXT NOT NULL CHECK(role_type IN ('guest', 'student', 'teacher', 'user')),
                    additional_info TEXT,
                    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            """)
            
            # Школьные новости
            self.execute("""
                CREATE TABLE IF NOT EXISTS school_news (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    content TEXT NOT NULL,
                    author TEXT NOT NULL,
                    target_audience TEXT DEFAULT 'all',
                    publish_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_published BOOLEAN DEFAULT TRUE
                )
            """)
            
            # Система достижений
            self.execute("""
                CREATE TABLE IF NOT EXISTS achievements (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT NOT NULL,
                    icon TEXT NOT NULL,
                    condition_type TEXT NOT NULL,
                    condition_value INTEGER
                )
            """)
            
            self.execute("""
                CREATE TABLE IF NOT EXISTS user_achievements (
                    user_id BIGINT,
                    achievement_id INTEGER,
                    achieved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, achievement_id),
                    FOREIGN KEY (user_id) REFERENCES users(user_id),
                    FOREIGN KEY (achievement_id) REFERENCES achievements(id)
                )
            """)
            
            # Статистика посещений
            self.execute("""
                CREATE TABLE IF NOT EXISTS user_activity (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    action_type TEXT NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    details TEXT,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            """)
            
            # Электронный дневник
            self.execute("""
                CREATE TABLE IF NOT EXISTS student_grades (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    subject TEXT NOT NULL,
                    grade INTEGER NOT NULL,
                    grade_type TEXT NOT NULL,
                    lesson_date DATE NOT NULL,
                    teacher_comment TEXT,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            """)
            
            # Добавляем начальные данные для звонков
            result = self.fetchone("SELECT COUNT(*) FROM bell_schedule")
            if result and result[0] == 0:
                bell_schedule = [
                    (1, '8:00', '8:40'),
                    (2, '8:50', '9:30'),
                    (3, '9:40', '10:20'),
                    (4, '10:30', '11:10'),
                    (5, '11:25', '12:05'),
                    (6, '12:10', '12:50'),
                    (7, '13:00', '13:40')
                ]
                for bell in bell_schedule:
                    self.execute(
                        "INSERT INTO bell_schedule (lesson_number, start_time, end_time) VALUES (?, ?, ?) ON CONFLICT (lesson_number) DO NOTHING",
                        bell
                    )
                logger.info("✅ Начальные данные для звонков созданы")
            
            # Добавляем стандартные достижения
            self._create_default_achievements()
            
        except Exception as e:
            logger.error(f"Ошибка создания таблиц: {e}")
            raise

    def _create_default_achievements(self):
        """Создаем стандартные достижения"""
        default_achievements = [
            ("🎓 Первые шаги", "Зарегистрировался в системе", "🎓", "registration", 1),
            ("📚 Любознательный", "Посмотрел расписание 10 раз", "📚", "schedule_views", 10),
            ("⭐ Активный ученик", "Использовал бота 50 раз", "⭐", "total_actions", 50),
            ("🏆 Отличник", "Получил 5 хороших оценок", "🏆", "good_grades", 5),
            ("📰 Информированный", "Прочитал все новости", "📰", "news_read", 10),
            ("🌦️ Метеоролог", "Включил уведомления о погоде", "🌦️", "weather_enabled", 1)
        ]
        
        for name, description, icon, condition_type, condition_value in default_achievements:
            self.execute(
                "INSERT INTO achievements (name, description, icon, condition_type, condition_value) VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                (name, description, icon, condition_type, condition_value)
            )

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
        self.db = DatabaseManager()
        self.init_db()
        self.setup_scheduler()
    
    def init_db(self):
        self.create_tables()
    
    def create_tables(self):
        self.db.create_tables()
    
    def setup_scheduler(self):
        """Настраиваем планировщик для уведомлений"""
        def run_scheduler():
            while True:
                schedule.run_pending()
                time.sleep(60)
        
        schedule.every().day.at("07:00").do(self.send_weather_notifications)
        schedule.every().day.at("12:00").do(self.send_weather_notifications)
        
        scheduler_thread = Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()
    
    # НОВЫЕ ФУНКЦИИ - УМНЫЕ УВЕДОМЛЕНИЯ
    def get_notification_settings(self, user_id):
        result = self.db.fetchone(
            "SELECT smart_notifications, weather_notifications, news_notifications, achievement_notifications FROM notification_settings WHERE user_id = ?",
            (user_id,)
        )
        if result:
            return {
                'smart_notifications': result[0],
                'weather_notifications': result[1],
                'news_notifications': result[2],
                'achievement_notifications': result[3]
            }
        else:
            self.db.execute(
                "INSERT INTO notification_settings (user_id) VALUES (?)",
                (user_id,)
            )
            return {
                'smart_notifications': False,
                'weather_notifications': False,
                'news_notifications': True,
                'achievement_notifications': True
            }
    
    def update_notification_settings(self, user_id, settings):
        self.db.execute(
            """INSERT INTO notification_settings 
            (user_id, smart_notifications, weather_notifications, news_notifications, achievement_notifications) 
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (user_id) DO UPDATE SET
            smart_notifications = EXCLUDED.smart_notifications,
            weather_notifications = EXCLUDED.weather_notifications,
            news_notifications = EXCLUDED.news_notifications,
            achievement_notifications = EXCLUDED.achievement_notifications""",
            (user_id, settings.get('smart_notifications', False), settings.get('weather_notifications', False),
             settings.get('news_notifications', True), settings.get('achievement_notifications', True))
        )
    
    # НОВЫЕ ФУНКЦИИ - РЕГИСТРАЦИЯ ПО РОЛЯМ
    def register_user_with_role(self, user_id, full_name, class_name, role_type, additional_info=None, username=None):
        if not self.create_user(user_id, full_name, class_name, username):
            return False
        
        self.db.execute(
            "INSERT INTO user_roles (user_id, role_type, additional_info) VALUES (?, ?, ?)",
            (user_id, role_type, additional_info)
        )
        
        self.log_user_activity(user_id, "registration", f"Role: {role_type}")
        self.check_achievements(user_id, "registration")
        return True
    
    def get_user_role(self, user_id):
        result = self.db.fetchone(
            "SELECT role_type, additional_info FROM user_roles WHERE user_id = ?",
            (user_id,)
        )
        return result if result else ('user', None)
    
    # НОВЫЕ ФУНКЦИИ - ШКОЛЬНЫЕ НОВОСТИ
    def add_news(self, title, content, author, target_audience="all"):
        self.db.execute(
            "INSERT INTO school_news (title, content, author, target_audience) VALUES (?, ?, ?, ?)",
            (title, content, author, target_audience)
        )
        self.notify_about_news(title, content)
        return True
    
    def get_news(self, limit=10, for_class=None):
        if for_class:
            return self.db.fetchall(
                """SELECT title, content, author, publish_date 
                FROM school_news 
                WHERE (target_audience = ? OR target_audience = 'all') AND is_published = TRUE
                ORDER BY publish_date DESC LIMIT ?""",
                (for_class, limit)
            )
        else:
            return self.db.fetchall(
                """SELECT title, content, author, publish_date 
                FROM school_news 
                WHERE is_published = TRUE
                ORDER BY publish_date DESC LIMIT ?""",
                (limit,)
            )
    
    def notify_about_news(self, title, content):
        users = self.db.fetchall(
            "SELECT user_id FROM notification_settings WHERE news_notifications = TRUE"
        )
        for user in users:
            message = f"📰 <b>Новая школьная новость</b>\n\n<b>{self.safe_message(title)}</b>\n\n{self.safe_message(content)}"
            self.send_message(user[0], message)
    
    # НОВЫЕ ФУНКЦИИ - СИСТЕМА ДОСТИЖЕНИЙ
    def check_achievements(self, user_id, action_type, value=1):
        achievements = self.db.fetchall(
            "SELECT id, name, description, icon, condition_type, condition_value FROM achievements WHERE condition_type = ?",
            (action_type,)
        )
        
        for achievement in achievements:
            achievement_id, name, description, icon, condition_type, condition_value = achievement
            
            user_progress = self.get_user_achievement_progress(user_id, condition_type)
            if user_progress >= condition_value:
                self.grant_achievement(user_id, achievement_id, name, description, icon)
    
    def get_user_achievement_progress(self, user_id, condition_type):
        if condition_type == "registration":
            return 1
        elif condition_type == "schedule_views":
            result = self.db.fetchone(
                "SELECT COUNT(*) FROM user_activity WHERE user_id = ? AND action_type = 'schedule_view'",
                (user_id,)
            )
            return result[0] if result else 0
        elif condition_type == "total_actions":
            result = self.db.fetchone(
                "SELECT COUNT(*) FROM user_activity WHERE user_id = ?",
                (user_id,)
            )
            return result[0] if result else 0
        elif condition_type == "good_grades":
            result = self.db.fetchone(
                "SELECT COUNT(*) FROM student_grades WHERE user_id = ? AND grade >= 4",
                (user_id,)
            )
            return result[0] if result else 0
        elif condition_type == "news_read":
            result = self.db.fetchone(
                "SELECT COUNT(*) FROM user_activity WHERE user_id = ? AND action_type = 'news_read'",
                (user_id,)
            )
            return result[0] if result else 0
        elif condition_type == "weather_enabled":
            settings = self.get_notification_settings(user_id)
            return 1 if settings.get('weather_notifications') else 0
        
        return 0
    
    def grant_achievement(self, user_id, achievement_id, name, description, icon):
        existing = self.db.fetchone(
            "SELECT 1 FROM user_achievements WHERE user_id = ? AND achievement_id = ?",
            (user_id, achievement_id)
        )
        if existing:
            return
        
        self.db.execute(
            "INSERT INTO user_achievements (user_id, achievement_id) VALUES (?, ?)",
            (user_id, achievement_id)
        )
        
        settings = self.get_notification_settings(user_id)
        if settings.get('achievement_notifications'):
            message = f"{icon} <b>Новое достижение!</b>\n\n<b>{name}</b>\n{description}"
            self.send_message(user_id, message)
    
    def get_user_achievements(self, user_id):
        return self.db.fetchall("""
            SELECT a.name, a.description, a.icon, ua.achieved_at 
            FROM user_achievements ua 
            JOIN achievements a ON ua.achievement_id = a.id 
            WHERE ua.user_id = ? 
            ORDER BY ua.achieved_at DESC
        """, (user_id,))
    
    # НОВЫЕ ФУНКЦИИ - ПОГОДА
    def get_weather(self):
        if not WEATHER_API_KEY:
            return "🌤️ Погода в Самаре: сервис погоды не настроен"
        
        try:
            url = f"http://api.weatherapi.com/v1/current.json?key={WEATHER_API_KEY}&q=Samara&lang=ru"
            response = requests.get(url, timeout=10)
            data = response.json()
            
            current = data['current']
            temp = current['temp_c']
            condition = current['condition']['text']
            humidity = current['humidity']
            wind = current['wind_kph']
            
            return (f"🌤️ <b>Погода в Самаре</b>\n\n"
                   f"🌡️ Температура: {temp}°C\n"
                   f"☁️ Состояние: {condition}\n"
                   f"💧 Влажность: {humidity}%\n"
                   f"💨 Ветер: {wind} км/ч")
        
        except Exception as e:
            logger.error(f"Ошибка получения погоды: {e}")
            return "🌤️ Погода в Самаре: временно недоступна"
    
    def send_weather_notifications(self):
        users = self.db.fetchall(
            "SELECT user_id FROM notification_settings WHERE weather_notifications = TRUE"
        )
        weather_message = self.get_weather()
        
        for user in users:
            self.send_message(user[0], weather_message)
    
    # НОВЫЕ ФУНКЦИИ - СТАТИСТИКА ПОСЕЩЕНИЙ
    def log_user_activity(self, user_id, action_type, details=None):
        self.db.execute(
            "INSERT INTO user_activity (user_id, action_type, details) VALUES (?, ?, ?)",
            (user_id, action_type, details)
        )
    
    def get_user_statistics(self, user_id):
        total_actions = self.db.fetchone(
            "SELECT COUNT(*) FROM user_activity WHERE user_id = ?",
            (user_id,)
        )
        total_actions = total_actions[0] if total_actions else 0
        
        schedule_views = self.db.fetchone(
            "SELECT COUNT(*) FROM user_activity WHERE user_id = ? AND action_type = 'schedule_view'",
            (user_id,)
        )
        schedule_views = schedule_views[0] if schedule_views else 0
        
        news_read = self.db.fetchone(
            "SELECT COUNT(*) FROM user_activity WHERE user_id = ? AND action_type = 'news_read'",
            (user_id,)
        )
        news_read = news_read[0] if news_read else 0
        
        last_active = self.db.fetchone(
            "SELECT timestamp FROM user_activity WHERE user_id = ? ORDER BY timestamp DESC LIMIT 1",
            (user_id,)
        )
        
        return {
            'total_actions': total_actions,
            'schedule_views': schedule_views,
            'news_read': news_read,
            'last_active': last_active[0] if last_active else None
        }
    
    # НОВЫЕ ФУНКЦИИ - ЭЛЕКТРОННЫЙ ДНЕВНИК
    def add_grade(self, user_id, subject, grade, grade_type, lesson_date, teacher_comment=None):
        self.db.execute(
            """INSERT INTO student_grades (user_id, subject, grade, grade_type, lesson_date, teacher_comment) 
            VALUES (?, ?, ?, ?, ?, ?)""",
            (user_id, subject, grade, grade_type, lesson_date, teacher_comment)
        )
        
        if grade >= 4:
            self.check_achievements(user_id, "good_grades")
    
    def get_student_grades(self, user_id, subject=None, limit=20):
        if subject:
            return self.db.fetchall(
                """SELECT subject, grade, grade_type, lesson_date, teacher_comment 
                FROM student_grades 
                WHERE user_id = ? AND subject = ? 
                ORDER BY lesson_date DESC LIMIT ?""",
                (user_id, subject, limit)
            )
        else:
            return self.db.fetchall(
                """SELECT subject, grade, grade_type, lesson_date, teacher_comment 
                FROM student_grades 
                WHERE user_id = ? 
                ORDER BY lesson_date DESC LIMIT ?""",
                (user_id, limit)
            )
    
    def get_student_average_grade(self, user_id, subject=None):
        if subject:
            result = self.db.fetchone(
                "SELECT AVG(grade) FROM student_grades WHERE user_id = ? AND subject = ?",
                (user_id, subject)
            )
        else:
            result = self.db.fetchone(
                "SELECT AVG(grade) FROM student_grades WHERE user_id = ?",
                (user_id,)
            )
        
        return round(result[0], 2) if result and result[0] else 0.0

    # СУЩЕСТВУЮЩИЕ МЕТОДЫ (оригинальные 800+ строк)
    def format_date(self, date_obj):
        if not date_obj:
            return "неизвестно"
        
        if hasattr(date_obj, 'strftime'):
            return date_obj.strftime("%Y-%m-%d")
        elif isinstance(date_obj, str):
            return date_obj.split()[0]
        else:
            return str(date_obj)
    
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
            response = requests.post(url, json=data, timeout=30)
            return response.json()
        except Exception as e:
            logger.error(f"Ошибка отправки сообщения: {e}")
            return None

    def send_document(self, chat_id, document, filename=None):
        url = f"{BASE_URL}/sendDocument"
        data = {"chat_id": chat_id}
        files = {"document": (filename, document)}
        
        try:
            response = requests.post(url, data=data, files=files, timeout=60)
            return response.json()
        except Exception as e:
            logger.error(f"Ошибка отправки документа: {e}")
            return None
    
    def get_file(self, file_id):
        url = f"{BASE_URL}/getFile"
        data = {"file_id": file_id}
        
        try:
            response = requests.post(url, json=data, timeout=30)
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
            response = requests.get(url, timeout=60)
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
            "timeout": 30,
            "limit": 100
        }
        
        try:
            response = requests.get(url, params=params, timeout=35)
            result = response.json()
            
            if not result.get("ok") and "Conflict" in str(result.get("description", "")):
                logger.warning("Обнаружен конфликт getUpdates")
                return {"ok": False, "conflict": True}
                
            return result
        except requests.exceptions.ReadTimeout:
            logger.warning("⚠️ Таймаут получения обновлений, продолжаем работу...")
            return {"ok": False}
        except Exception as e:
            logger.error(f"Ошибка получения обновлений: {e}")
            return {"ok": False}
    
    def get_user(self, user_id):
        if not self.is_valid_user_id(user_id):
            return None
            
        try:
            return self.db.fetchone("SELECT * FROM users WHERE user_id = ?", (user_id,))
        except Exception as e:
            logger.error(f"Ошибка получения пользователя: {e}")
            return None

    def find_user_by_username(self, username):
        """Поиск пользователя по username"""
        try:
            return self.db.fetchone("SELECT * FROM users WHERE username = ?", (username,))
        except Exception as e:
            logger.error(f"Ошибка поиска пользователя по username: {e}")
            return None
    
    def is_valid_user_id(self, user_id):
        return isinstance(user_id, int) and user_id > 0
    
    def create_user(self, user_id, full_name, class_name, username=None):
        if not self.is_valid_user_id(user_id):
            return False
            
        try:
            result = self.db.fetchone("SELECT COUNT(*) FROM users WHERE class = ?", (class_name,))
            count = result[0] if result else 0
            
            if count >= MAX_USERS_PER_CLASS:
                self.log_security_event("class_limit_exceeded", user_id, f"Class: {class_name}")
                return False
            
            self.db.execute(
                "INSERT INTO users (user_id, full_name, class, username) VALUES (?, ?, ?, ?) ON CONFLICT (user_id) DO UPDATE SET full_name = EXCLUDED.full_name, class = EXCLUDED.class, username = EXCLUDED.username",
                (user_id, full_name, class_name, username)
            )
            return True
        except Exception as e:
            logger.error(f"Ошибка создания пользователя: {e}")
            return False
    
    def delete_user(self, user_id):
        if not self.is_valid_user_id(user_id):
            return False
            
        try:
            self.db.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
            return True
        except Exception as e:
            logger.error(f"Ошибка удаления пользователя: {e}")
            return False

    def delete_user_by_username(self, username):
        """Удаление пользователя по username"""
        try:
            self.db.execute("DELETE FROM users WHERE username = ?", (username,))
            return True
        except Exception as e:
            logger.error(f"Ошибка удаления пользователя по username: {e}")
            return False
    
    def get_all_users(self):
        try:
            return self.db.fetchall("SELECT user_id, full_name, class, username, registered_at FROM users ORDER BY registered_at DESC")
        except Exception as e:
            logger.error(f"Ошибка получения пользователей: {e}")
            return []
    
    def get_schedule(self, class_name, day):
        try:
            return self.db.fetchall(
                "SELECT lesson_number, subject, teacher, room FROM schedule WHERE class = ? AND day = ? ORDER BY lesson_number",
                (class_name, day)
            )
        except Exception as e:
            logger.error(f"Ошибка получения расписания: {e}")
            return []
    
    def save_schedule(self, class_name, day, lessons):
        try:
            self.db.execute("DELETE FROM schedule WHERE class = ? AND day = ?", (class_name, day))
            
            for lesson_num, subject, teacher, room in lessons:
                subject = subject[:100] if subject else ""
                teacher = teacher[:50] if teacher else ""
                room = room[:20] if room else ""
                
                self.db.execute(
                    "INSERT INTO schedule (class, day, lesson_number, subject, teacher, room) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (class, day, lesson_number) DO UPDATE SET subject = EXCLUDED.subject, teacher = EXCLUDED.teacher, room = EXCLUDED.room",
                    (class_name, day, lesson_num, subject, teacher, room)
                )
            
            return True
        except Exception as e:
            logger.error(f"Ошибка сохранения расписания: {e}")
            return False
    
    def get_bell_schedule(self):
        try:
            return self.db.fetchall("SELECT lesson_number, start_time, end_time FROM bell_schedule ORDER BY lesson_number")
        except Exception as e:
            logger.error(f"Ошибка получения расписания звонков: {e}")
            return []
    
    def is_admin(self, username):
        return username and username.lower() in [admin.lower() for admin in ADMINS]
    
    # ОБНОВЛЕННОЕ ГЛАВНОЕ МЕНЮ
    def main_menu_keyboard(self):
        return {
            "keyboard": [
                [{"text": "📚 Моё расписание"}, {"text": "🏫 Общее расписание"}],
                [{"text": "🔔 Звонки"}, {"text": "📰 Новости"}],
                [{"text": "⚙️ Настройки"}, {"text": "🏆 Достижения"}],
                [{"text": "📊 Дневник"}, {"text": "📈 Статистика"}],
                [{"text": "ℹ️ Помощь"}]
            ],
            "resize_keyboard": True
        }
    
    # НОВЫЕ КЛАВИАТУРЫ
    def notifications_settings_keyboard(self):
        return {
            "inline_keyboard": [
                [{"text": "🔔 Умные уведомления", "callback_data": "toggle_smart"}],
                [{"text": "🌤️ Уведомления о погоде", "callback_data": "toggle_weather"}],
                [{"text": "📰 Новости школы", "callback_data": "toggle_news"}],
                [{"text": "🏆 Достижения", "callback_data": "toggle_achievements"}],
                [{"text": "⬅️ Назад", "callback_data": "settings_back"}]
            ]
        }
    
    def role_selection_keyboard(self):
        return {
            "keyboard": [
                [{"text": "👨‍🎓 Ученик"}, {"text": "👨‍🏫 Учитель"}],
                [{"text": "👤 Гость"}]
            ],
            "resize_keyboard": True
        }
    
    def achievements_keyboard(self):
        return {
            "inline_keyboard": [
                [{"text": "🏆 Мои достижения", "callback_data": "my_achievements"}],
                [{"text": "📊 Прогресс", "callback_data": "achievement_progress"}],
                [{"text": "⬅️ Назад", "callback_data": "achievements_back"}]
            ]
        }
    
    def news_keyboard(self):
        return {
            "inline_keyboard": [
                [{"text": "📰 Последние новости", "callback_data": "recent_news"}],
                [{"text": "📊 Статистика новостей", "callback_data": "news_stats"}],
                [{"text": "⬅️ Назад", "callback_data": "news_back"}]
            ]
        }
    
    def diary_keyboard(self):
        return {
            "inline_keyboard": [
                [{"text": "📊 Мои оценки", "callback_data": "my_grades"}],
                [{"text": "📈 Средний балл", "callback_data": "average_grade"}],
                [{"text": "📚 По предметам", "callback_data": "grades_by_subject"}],
                [{"text": "⬅️ Назад", "callback_data": "diary_back"}]
            ]
        }
    
    def statistics_keyboard(self):
        return {
            "inline_keyboard": [
                [{"text": "📈 Моя статистика", "callback_data": "my_statistics"}],
                [{"text": "🏆 Достижения", "callback_data": "my_achievements"}],
                [{"text": "⬅️ Назад", "callback_data": "stats_back"}]
            ]
        }

    # СУЩЕСТВУЮЩИЕ КЛАВИАТУРЫ
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
        try:
            result = self.db.fetchall("SELECT DISTINCT class FROM users ORDER BY class")
            return [row[0] for row in result]
        except Exception as e:
            logger.error(f"Ошибка получения классов: {e}")
            return []
    
    def add_class(self, class_name):
        return self.is_valid_class(class_name)
    
    def delete_class(self, class_name):
        try:
            self.db.execute("DELETE FROM users WHERE class = ?", (class_name,))
            return True
        except Exception as e:
            logger.error(f"Ошибка удаления класса: {e}")
            return False
    
    def update_bell_schedule(self, lesson_number, start_time, end_time):
        try:
            self.db.execute(
                "UPDATE bell_schedule SET start_time = ?, end_time = ? WHERE lesson_number = ?",
                (start_time, end_time, lesson_number)
            )
            return True
        except Exception as e:
            logger.error(f"Ошибка обновления расписания звонков: {e}")
            return False

    # ОРИГИНАЛЬНЫЙ ПАРСЕР EXCEL (полностью сохранен)
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
        
        lesson_numbers = {}
        for row_idx in range(start_row, min(end_row, len(df))):
            row = df.iloc[row_idx]
            
            if len(row) > 1 and pd.notna(row[1]):
                lesson_str = str(row[1]).strip()
                numbers = re.findall(r'\d+', lesson_str)
                if numbers:
                    lesson_num = int(numbers[0])
                    if 1 <= lesson_num <= 10:
                        lesson_numbers[row_idx] = lesson_num
                        logger.debug(f"Найден номер урока {lesson_num} в строке {row_idx}")
        
        current_lesson_num = 1
        
        for row_idx in range(start_row, min(end_row, len(df))):
            row = df.iloc[row_idx]
            
            if all(pd.isna(cell) for cell in row):
                continue
            
            lesson_num = lesson_numbers.get(row_idx)
            if lesson_num is not None:
                current_lesson_num = lesson_num
            else:
                lesson_num = current_lesson_num
            
            lesson_found_in_row = False
            
            for class_name, col_idx in class_columns.items():
                subject_col = col_idx
                if subject_col < len(row) and pd.notna(row[subject_col]):
                    subject = str(row[subject_col]).strip()
                    
                    if not subject or subject in ['-', '—', ''] or self._is_day_of_week(subject):
                        continue
                    
                    room = ""
                    room_col = col_idx + 1
                    if room_col < len(row) and pd.notna(row[room_col]):
                        room_cell = str(row[room_col]).strip()
                        if room_cell and not self._is_day_of_week(room_cell):
                            room = room_cell
                    
                    teacher = ""
                    if '(' in subject and ')' in subject:
                        teacher_match = re.search(r'\((.*?)\)', subject)
                        if teacher_match:
                            teacher = teacher_match.group(1)
                            subject = re.sub(r'\(.*?\)', '', subject).strip()
                    
                    lessons.append({
                        'class': class_name,
                        'day': day_name,
                        'lesson_number': lesson_num,
                        'subject': subject,
                        'teacher': teacher,
                        'room': room,
                        'shift': shift
                    })
                    
                    lesson_found_in_row = True
                    logger.debug(f"Добавлен урок: {class_name}, {day_name}, {lesson_num}, {subject}, {teacher}, {room}")
        
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

    def import_schedule_from_excel(self, file_content, shift):
        try:
            lessons_data = self.parse_excel_schedule(file_content, shift)
            if not lessons_data:
                return False, f"Не удалось распарсить Excel файл для {shift} смены"
            
            imported_count = 0
            error_count = 0
            
            imported_classes = set(lesson['class'] for lesson in lessons_data)
            
            for class_name in imported_classes:
                self.db.execute("DELETE FROM schedule WHERE class = ?", (class_name,))
                logger.info(f"Удалены старые уроки для класса {class_name}")
            
            for lesson in lessons_data:
                try:
                    lesson_number = int(lesson['lesson_number'])
                    class_name = lesson['class']
                    day = lesson['day']
                    
                    self.db.execute(
                        "INSERT INTO schedule (class, day, lesson_number, subject, teacher, room) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (class, day, lesson_number) DO UPDATE SET subject = EXCLUDED.subject, teacher = EXCLUDED.teacher, room = EXCLUDED.room",
                        (class_name, day, lesson_number, lesson['subject'], lesson['teacher'], lesson['room'])
                    )
                    imported_count += 1
                except Exception as e:
                    logger.error(f"Ошибка импорта урока {lesson}: {e}")
                    error_count += 1
            
            message = f"✅ Успешно импортировано {imported_count} уроков для {shift} смены"
            if error_count > 0:
                message += f", ошибок: {error_count}"
                
            return True, message
        except Exception as e:
            logger.error(f"Ошибка импорта из Excel для смены {shift}: {e}")
            return False, f"Ошибка импорта для {shift} смены: {str(e)}"

    # ОБНОВЛЕННЫЕ ОБРАБОТЧИКИ С НОВЫМИ ФУНКЦИЯМИ
    def handle_start(self, chat_id, user):
        user_data = self.get_user(user["id"])
        
        if user_data:
            text = (
                f"Привет, {self.safe_message(user.get('first_name', 'друг'))}!\n"
                f"Ты уже зарегистрирован в системе.\n"
                f"Твой класс: {self.safe_message(user_data[2])}"
            )
            self.send_message(chat_id, text, self.main_menu_keyboard())
        else:
            self.handle_role_selection(chat_id, user["id"])
    
    def handle_role_selection(self, chat_id, user_id):
        """Обработка выбора роли при регистрации"""
        self.user_states[user_id] = {"action": "role_selection"}
        self.send_message(
            chat_id,
            "👋 <b>Добро пожаловать!</b>\n\n"
            "Пожалуйста, выберите вашу роль:",
            self.role_selection_keyboard()
        )
    
    def handle_help(self, chat_id, username):
        text = (
            "📚 <b>Школьный бот - помощь</b>\n\n"
            "Я помогу тебе узнать расписание уроков и многое другое.\n\n"
            "<b>Основные команды:</b>\n"
            "• /start - начать работу\n"
            "• /help - показать эту справку\n\n"
            "<b>Новые возможности:</b>\n"
            "• <b>📰 Новости</b> - школьные новости и объявления\n"
            "• <b>⚙️ Настройки</b> - умные уведомления и предпочтения\n"
            "• <b>🏆 Достижения</b> - система наград за активность\n"
            "• <b>📊 Дневник</b> - электронный дневник с оценками\n"
            "• <b>📈 Статистика</b> - ваша активность и прогресс\n\n"
            "<b>Классические функции:</b>\n"
            "• <b>Моё расписание</b> - расписание для твоего класса\n"
            "• <b>Общее расписание</b> - расписание для любого класса\n"
            "• <b>Звонки</b> - расписание звонков\n\n"
            "Для регистрации выберите вашу роль из меню.\n\n"
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
    
    # ОБНОВЛЕННЫЙ ГЛАВНЫЙ ОБРАБОТЧИК МЕНЮ
    def handle_main_menu(self, chat_id, user_id, text, username):
        user_data = self.get_user(user_id)
        
        if text == "📚 Моё расписание":
            if not user_data:
                self.send_message(
                    chat_id,
                    "❌ Вы не зарегистрированы. Пожалуйста, выберите роль для регистрации."
                )
                return
            
            class_name = user_data[2]
            self.user_states[user_id] = {"action": "my_schedule", "class": class_name}
            self.send_message(
                chat_id,
                f"Выберите день недели для расписания {self.safe_message(class_name)} класса:",
                self.day_selection_inline_keyboard()
            )
            self.log_user_activity(user_id, "schedule_view", f"Class: {class_name}")
        
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
        
        elif text == "📰 Новости":
            self.handle_news_menu(chat_id, user_id)
        
        elif text == "⚙️ Настройки":
            self.handle_notifications_settings(chat_id, user_id)
        
        elif text == "🏆 Достижения":
            self.handle_achievements_menu(chat_id, user_id)
        
        elif text == "📊 Дневник":
            self.handle_diary_menu(chat_id, user_id)
        
        elif text == "📈 Статистика":
            self.handle_statistics_menu(chat_id, user_id)
        
        elif text == "ℹ️ Помощь":
            self.handle_help(chat_id, username)
        
        elif text in ["👨‍🎓 Ученик", "👨‍🏫 Учитель", "👤 Гость"]:
            self.handle_role_registration(chat_id, user_id, text)
        
        elif text == "⬅️ Назад":
            if user_id in self.user_states:
                del self.user_states[user_id]
            self.send_message(chat_id, "Главное меню", self.main_menu_keyboard())
        
        elif self.is_valid_class(text):
            self.handle_class_selection(chat_id, user_id, text)
    
    # НОВЫЕ ОБРАБОТЧИКИ МЕНЮ
    def handle_notifications_settings(self, chat_id, user_id):
        settings = self.get_notification_settings(user_id)
        
        smart_status = "✅ ВКЛ" if settings['smart_notifications'] else "❌ ВЫКЛ"
        weather_status = "✅ ВКЛ" if settings['weather_notifications'] else "❌ ВЫКЛ"
        news_status = "✅ ВКЛ" if settings['news_notifications'] else "❌ ВЫКЛ"
        achievements_status = "✅ ВКЛ" if settings['achievement_notifications'] else "❌ ВЫКЛ"
        
        text = (f"⚙️ <b>Настройки уведомлений</b>\n\n"
               f"🔔 Умные уведомления: {smart_status}\n"
               f"🌤️ Погода: {weather_status}\n"
               f"📰 Новости: {news_status}\n"
               f"🏆 Достижения: {achievements_status}\n\n"
               f"Нажмите на кнопку для переключения:")
        
        self.send_message(chat_id, text, self.notifications_settings_keyboard())
    
    def handle_achievements_menu(self, chat_id, user_id):
        achievements = self.get_user_achievements(user_id)
        text = "🏆 <b>Система достижений</b>\n\n"
        
        if achievements:
            text += f"🎯 Получено достижений: {len(achievements)}\n\n"
            for i, (name, desc, icon, date) in enumerate(achievements[:3], 1):
                text += f"{icon} <b>{name}</b>\n{desc}\n\n"
        else:
            text += "У вас пока нет достижений. Продолжайте использовать бота для их получения!"
        
        self.send_message(chat_id, text, self.achievements_keyboard())
    
    def handle_news_menu(self, chat_id, user_id):
        news_count = self.db.fetchone("SELECT COUNT(*) FROM school_news WHERE is_published = TRUE")
        news_count = news_count[0] if news_count else 0
        user_news_read = self.get_user_statistics(user_id)['news_read']
        
        text = (f"📰 <b>Школьные новости</b>\n\n"
               f"📊 Всего новостей: {news_count}\n"
               f"📖 Прочитано вами: {user_news_read}\n\n"
               f"Будьте в курсе всех школьных событий!")
        
        self.send_message(chat_id, text, self.news_keyboard())
    
    def handle_diary_menu(self, chat_id, user_id):
        avg_grade = self.get_student_average_grade(user_id)
        total_grades = self.db.fetchone(
            "SELECT COUNT(*) FROM student_grades WHERE user_id = ?",
            (user_id,)
        )
        total_grades = total_grades[0] if total_grades else 0
        
        text = (f"📊 <b>Электронный дневник</b>\n\n"
               f"📈 Средний балл: {avg_grade}\n"
               f"📚 Всего оценок: {total_grades}\n\n"
               f"Здесь вы можете посмотреть свои оценки и успеваемость.")
        
        self.send_message(chat_id, text, self.diary_keyboard())
    
    def handle_statistics_menu(self, chat_id, user_id):
        stats = self.get_user_statistics(user_id)
        achievements = len(self.get_user_achievements(user_id))
        
        last_active = self.format_date(stats['last_active']) if stats['last_active'] else "неизвестно"
        
        text = (f"📈 <b>Ваша статистика</b>\n\n"
               f"📊 Всего действий: {stats['total_actions']}\n"
               f"📚 Просмотров расписания: {stats['schedule_views']}\n"
               f"📰 Прочитано новостей: {stats['news_read']}\n"
               f"🏆 Получено достижений: {achievements}\n"
               f"🕐 Последняя активность: {last_active}")
        
        self.send_message(chat_id, text, self.statistics_keyboard())
    
    def handle_role_registration(self, chat_id, user_id, role_text):
        role_map = {
            "👨‍🎓 Ученик": "student",
            "👨‍🏫 Учитель": "teacher", 
            "👤 Гость": "guest"
        }
        
        role_type = role_map[role_text]
        self.user_states[user_id] = {"action": "role_registration", "role": role_type}
        
        if role_type == "guest":
            self.send_message(chat_id, "Введите ваше ФИО:", self.cancel_keyboard())
        else:
            self.send_message(
                chat_id, 
                "Введите ваше ФИО и класс в формате:\n<b>Фамилия Имя Отчество, Класс</b>\n\n"
                "Например: <i>Иванов Иван Иванович, 10П</i>",
                self.cancel_keyboard()
            )
    
    # ОБНОВЛЕННЫЙ ОБРАБОТЧИК CALLBACK
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
        
        # Обработка новых callback
        if data.startswith("toggle_"):
            self.handle_toggle_setting(chat_id, user_id, data)
        elif data == "my_achievements":
            self.show_user_achievements(chat_id, user_id)
        elif data == "achievement_progress":
            self.show_achievement_progress(chat_id, user_id)
        elif data == "recent_news":
            self.show_recent_news(chat_id, user_id)
        elif data == "news_stats":
            self.show_news_statistics(chat_id, user_id)
        elif data == "my_grades":
            self.show_user_grades(chat_id, user_id)
        elif data == "average_grade":
            self.show_average_grades(chat_id, user_id)
        elif data == "grades_by_subject":
            self.show_grades_by_subject(chat_id, user_id)
        elif data == "my_statistics":
            self.show_detailed_statistics(chat_id, user_id)
        elif data in ["settings_back", "achievements_back", "news_back", "diary_back", "stats_back"]:
            self.send_message(chat_id, "Главное меню", self.main_menu_keyboard())
        
        # Существующие обработчики
        elif data.startswith("day_"):
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
            
            if username in self.admin_states and self.admin_states[username].get("action") == "edit_schedule_day":
                self.handle_schedule_day_selection(chat_id, username, day_text)
            else:
                self.handle_day_selection(chat_id, user_id, day_text)
            
        elif data.startswith("admin_"):
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
    
    # НОВЫЕ МЕТОДЫ ДЛЯ ОБРАБОТКИ CALLBACK
    def handle_toggle_setting(self, chat_id, user_id, data):
        settings = self.get_notification_settings(user_id)
        setting_map = {
            "toggle_smart": "smart_notifications",
            "toggle_weather": "weather_notifications", 
            "toggle_news": "news_notifications",
            "toggle_achievements": "achievement_notifications"
        }
        
        setting_key = setting_map[data]
        settings[setting_key] = not settings[setting_key]
        self.update_notification_settings(user_id, settings)
        
        if setting_key == "weather_notifications" and settings[setting_key]:
            self.check_achievements(user_id, "weather_enabled")
        
        self.handle_notifications_settings(chat_id, user_id)

    def show_user_achievements(self, chat_id, user_id):
        achievements = self.get_user_achievements(user_id)
        
        if not achievements:
            self.send_message(chat_id, "🎯 У вас пока нет достижений. Продолжайте использовать бота!", self.achievements_keyboard())
            return
        
        text = "🏆 <b>Ваши достижения</b>\n\n"
        for name, description, icon, achieved_at in achievements:
            date_str = self.format_date(achieved_at)
            text += f"{icon} <b>{name}</b>\n{description}\n📅 {date_str}\n\n"
        
        self.send_message(chat_id, text, self.achievements_keyboard())

    def show_achievement_progress(self, chat_id, user_id):
        achievement_types = ["registration", "schedule_views", "total_actions", "good_grades", "news_read", "weather_enabled"]
        text = "📊 <b>Ваш прогресс по достижениям</b>\n\n"
        
        for achievement_type in achievement_types:
            progress = self.get_user_achievement_progress(user_id, achievement_type)
            achievements = self.db.fetchall(
                "SELECT name, condition_value FROM achievements WHERE condition_type = ?",
                (achievement_type,)
            )
            
            for name, condition_value in achievements:
                percentage = min(100, int((progress / condition_value) * 100)) if condition_value > 0 else 100
                progress_bar = "🟩" * (percentage // 20) + "⬜" * (5 - percentage // 20)
                text += f"{name}: {progress}/{condition_value}\n{progress_bar} {percentage}%\n\n"
        
        self.send_message(chat_id, text, self.achievements_keyboard())

    def show_recent_news(self, chat_id, user_id):
        news = self.get_news(limit=5)
        
        if not news:
            self.send_message(chat_id, "📰 Пока нет новостей.", self.news_keyboard())
            return
        
        text = "📰 <b>Последние новости</b>\n\n"
        for title, content, author, publish_date in news:
            date_str = self.format_date(publish_date)
            text += f"<b>{self.safe_message(title)}</b>\n"
            text += f"{self.safe_message(content[:100])}...\n"
            text += f"👤 {self.safe_message(author)} | 📅 {date_str}\n\n"
            
            self.log_user_activity(user_id, "news_read", f"News: {title}")
        
        self.send_message(chat_id, text, self.news_keyboard())

    def show_news_statistics(self, chat_id, user_id):
        total_news = self.db.fetchone("SELECT COUNT(*) FROM school_news WHERE is_published = TRUE")
        total_news = total_news[0] if total_news else 0
        
        user_stats = self.get_user_statistics(user_id)
        user_news_read = user_stats['news_read']
        
        percentage = (user_news_read / total_news * 100) if total_news > 0 else 0
        
        text = (f"📊 <b>Статистика новостей</b>\n\n"
               f"📰 Всего новостей: {total_news}\n"
               f"📖 Прочитано вами: {user_news_read}\n"
               f"📈 Процент прочитанного: {percentage:.1f}%\n\n")
        
        if percentage >= 80:
            text += "🎉 Вы отлично информированы!"
        elif percentage >= 50:
            text += "👍 Вы в курсе основных событий!"
        else:
            text += "💡 Читайте больше новостей, чтобы быть в курсе!"
        
        self.send_message(chat_id, text, self.news_keyboard())

    def show_user_grades(self, chat_id, user_id):
        grades = self.get_student_grades(user_id, limit=10)
        
        if not grades:
            self.send_message(chat_id, "📊 У вас пока нет оценок.", self.diary_keyboard())
            return
        
        text = "📊 <b>Ваши последние оценки</b>\n\n"
        for subject, grade, grade_type, lesson_date, comment in grades:
            date_str = self.format_date(lesson_date)
            grade_emoji = "🟢" if grade >= 4 else "🟡" if grade == 3 else "🔴"
            text += f"{grade_emoji} <b>{subject}</b>: {grade} ({grade_type})\n"
            if comment:
                text += f"💬 {comment}\n"
            text += f"📅 {date_str}\n\n"
        
        self.send_message(chat_id, text, self.diary_keyboard())

    def show_average_grades(self, chat_id, user_id):
        overall_avg = self.get_student_average_grade(user_id)
        
        subjects = self.db.fetchall(
            "SELECT DISTINCT subject FROM student_grades WHERE user_id = ?",
            (user_id,)
        )
        
        text = f"📈 <b>Средние баллы</b>\n\n"
        text += f"📊 Общий средний балл: {overall_avg}\n\n"
        
        if subjects:
            text += "<b>По предметам:</b>\n"
            for subject_row in subjects:
                subject = subject_row[0]
                subject_avg = self.get_student_average_grade(user_id, subject)
                text += f"• {subject}: {subject_avg}\n"
        
        self.send_message(chat_id, text, self.diary_keyboard())

    def show_grades_by_subject(self, chat_id, user_id):
        subjects = self.db.fetchall(
            "SELECT DISTINCT subject FROM student_grades WHERE user_id = ? ORDER BY subject",
            (user_id,)
        )
        
        if not subjects:
            self.send_message(chat_id, "📚 У вас пока нет оценок по предметам.", self.diary_keyboard())
            return
        
        text = "📚 <b>Оценки по предметам</b>\n\n"
        
        for subject_row in subjects:
            subject = subject_row[0]
            grades = self.get_student_grades(user_id, subject, limit=5)
            avg_grade = self.get_student_average_grade(user_id, subject)
            
            text += f"<b>{subject}</b> (средний: {avg_grade}):\n"
            
            grade_list = []
            for _, grade, grade_type, lesson_date, _ in grades:
                date_str = self.format_date(lesson_date)
                grade_emoji = "🟢" if grade >= 4 else "🟡" if grade == 3 else "🔴"
                grade_list.append(f"{grade_emoji} {grade} ({grade_type}) - {date_str}")
            
            text += ", ".join(grade_list) + "\n\n"
        
        self.send_message(chat_id, text, self.diary_keyboard())

    def get_user_role_display(self, role_type):
        """Получение отображаемого названия роли"""
        role_translations = {
            'student': 'Ученик',
            'teacher': 'Учитель', 
            'guest': 'Гость',
            'user': 'Пользователь'
        }
        return role_translations.get(role_type, role_type)

    def show_detailed_statistics(self, chat_id, user_id):
        stats = self.get_user_statistics(user_id)
        achievements = self.get_user_achievements(user_id)
        user_data = self.get_user(user_id)
        
        role_data = self.get_user_role(user_id)
        role_type, additional_info = role_data
        role_display = self.get_user_role_display(role_type)
        
        text = (f"📈 <b>Подробная статистика</b>\n\n"
               f"👤 <b>Профиль</b>\n"
               f"• Имя: {self.safe_message(user_data[1]) if user_data else 'Неизвестно'}\n"
               f"• Класс: {self.safe_message(user_data[2]) if user_data else 'Неизвестно'}\n"
               f"• Роль: {role_display}\n\n"
               
               f"📊 <b>Активность</b>\n"
               f"• Всего действий: {stats['total_actions']}\n"
               f"• Просмотров расписания: {stats['schedule_views']}\n"
               f"• Прочитано новостей: {stats['news_read']}\n"
               f"• Получено достижений: {len(achievements)}\n"
               f"• Последняя активность: {self.format_date(stats['last_active']) if stats['last_active'] else 'неизвестно'}\n\n")
        
        if achievements:
            text += "🏆 <b>Последние достижения</b>\n"
            for name, _, icon, date in achievements[:3]:
                text += f"{icon} {name} - {self.format_date(date)}\n"
        
        self.send_message(chat_id, text, self.statistics_keyboard())
    
    # ОБНОВЛЕННЫЕ МЕТОДЫ ДЛЯ УДАЛЕНИЯ ПОЛЬЗОВАТЕЛЕЙ
    def start_delete_user(self, chat_id, username):
        """Начало процесса удаления пользователя"""
        self.admin_states[username] = {"action": "delete_user"}
        self.send_message(
            chat_id,
            "Введите ID пользователя или username для удаления:\n\n"
            "ID можно узнать через команду '👥 Список пользователей'\n"
            "Username должен начинаться с @",
            self.cancel_keyboard()
        )

    def delete_user_by_identifier(self, chat_id, admin_username, identifier):
        """Удаление пользователя по ID или username"""
        try:
            # Пробуем удалить по ID
            if identifier.isdigit():
                user_id = int(identifier)
                if self.delete_user(user_id):
                    self.log_security_event("user_deleted", admin_username, f"Deleted user: {user_id}")
                    self.send_message(chat_id, f"✅ Пользователь с ID {user_id} удален", self.admin_menu_inline_keyboard())
                else:
                    self.send_message(chat_id, f"❌ Пользователь с ID {identifier} не найден", self.admin_menu_inline_keyboard())
            # Удаляем по username
            elif identifier.startswith('@'):
                username = identifier[1:]  # Убираем @
                if self.delete_user_by_username(username):
                    self.log_security_event("user_deleted", admin_username, f"Deleted user by username: {username}")
                    self.send_message(chat_id, f"✅ Пользователь с username @{username} удален", self.admin_menu_inline_keyboard())
                else:
                    self.send_message(chat_id, f"❌ Пользователь с username @{username} не найден", self.admin_menu_inline_keyboard())
            else:
                self.send_message(chat_id, "❌ Неверный формат. Введите ID (число) или username (начинается с @)", self.admin_menu_inline_keyboard())
        
        except ValueError:
            self.send_message(chat_id, "❌ Неверный формат ID", self.admin_menu_inline_keyboard())
        
        if admin_username in self.admin_states:
            del self.admin_states[admin_username]

    def handle_role_registration_input(self, chat_id, user_id, username, text):
        """Обработка ввода данных для регистрации по роли"""
        if user_id not in self.user_states or self.user_states[user_id].get("action") != "role_registration":
            return
        
        role_type = self.user_states[user_id].get("role")
        telegram_username = username  # username из Telegram
        
        if role_type == "guest":
            if not self.is_valid_fullname(text):
                self.send_message(chat_id, "❌ Неверный формат ФИО. Введите корректное ФИО:")
                return
            
            if self.register_user_with_role(user_id, text, "Гость", "guest", None, telegram_username):
                self.send_message(chat_id, f"✅ Регистрация гостя прошла успешно!\nФИО: {self.safe_message(text)}", self.main_menu_keyboard())
            else:
                self.send_message(chat_id, "❌ Ошибка регистрации", self.main_menu_keyboard())
        
        else:
            parts = text.split(',')
            if len(parts) != 2:
                self.send_message(chat_id, "❌ Неверный формат. Введите: Фамилия Имя Отчество, Класс")
                return
            
            full_name = parts[0].strip()
            class_name = parts[1].strip()
            
            if not self.is_valid_fullname(full_name):
                self.send_message(chat_id, "❌ Неверный формат ФИО")
                return
            
            if not self.is_valid_class(class_name):
                self.send_message(chat_id, "❌ Неверный формат класса")
                return
            
            class_name = class_name.upper()
            additional_info = f"Учитель предмета" if role_type == "teacher" else None
            
            if self.register_user_with_role(user_id, full_name, class_name, role_type, additional_info, telegram_username):
                role_text = "учителя" if role_type == "teacher" else "ученика"
                self.send_message(
                    chat_id, 
                    f"✅ Регистрация {role_text} прошла успешно!\n"
                    f"ФИО: {self.safe_message(full_name)}\n"
                    f"Класс: {class_name}", 
                    self.main_menu_keyboard()
                )
            else:
                self.send_message(chat_id, f"❌ Не удалось зарегистрироваться", self.main_menu_keyboard())
        
        if user_id in self.user_states:
            del self.user_states[user_id]

    # СУЩЕСТВУЮЩИЕ МЕТОДЫ ОБРАБОТКИ
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
            reg_date_str = self.format_date(user[4])
            username_display = f" (@{user[3]})" if user[3] else ""
                
            users_text += f"👤 {self.safe_message(user[1])}{username_display}\n"
            users_text += f"   Класс: {self.safe_message(user[2])} | ID: {user[0]}\n"
            users_text += f"   📅 Зарегистрирован: {reg_date_str}\n\n"
        
        self.send_message(chat_id, users_text, self.admin_menu_inline_keyboard())
    
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
                    
                    # Обработка админских состояний
                    if username in self.admin_states:
                        state = self.admin_states[username]
                        
                        if state.get("action") in ["add_class_input", "delete_class_input"]:
                            self.handle_class_input(chat_id, username, text)
                            return
                        
                        if state.get("action") in ["edit_bell_number", "edit_bell_start", "edit_bell_end"]:
                            self.handle_bell_input(chat_id, username, text)
                            return
                        
                        if state.get("action") == "delete_user":
                            self.delete_user_by_identifier(chat_id, username, text)
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
                        elif state.get("action") == "role_registration":
                            self.handle_role_registration_input(chat_id, user_id, username, text)
                            return
                    
                    # Обработка пользовательских состояний
                    if user_id in self.user_states:
                        state = self.user_states[user_id]
                        if state.get("action") == "role_registration":
                            self.handle_role_registration_input(chat_id, user_id, username, text)
                            return
                    
                    # Обработка команд
                    if text.startswith("/start"):
                        self.handle_start(chat_id, user)
                    elif text.startswith("/help"):
                        self.handle_help(chat_id, username)
                    elif text.startswith("/admin_panel"):
                        self.handle_admin_panel(chat_id, username)
                    elif text in ["📚 Моё расписание", "🏫 Общее расписание", "🔔 Звонки", "📰 Новости", 
                                "⚙️ Настройки", "🏆 Достижения", "📊 Дневник", "📈 Статистика", "ℹ️ Помощь"]:
                        self.handle_main_menu(chat_id, user_id, text, username)
                    elif text in ["👥 Список пользователей", "❌ Удалить пользователя", "📝 Редактировать расписание", 
                                  "🏫 Управление классами", "🕧 Управление звонками", "📤 Загрузить Excel", "📊 Статистика", "⬅️ Назад"]:
                        self.handle_admin_menu(chat_id, username, text)
                    elif text in ["1 смена", "2 смена"]:
                        self.handle_shift_selection(chat_id, username, text)
                    elif text in ["👨‍🎓 Ученик", "👨‍🏫 Учитель", "👤 Гость"]:
                        self.handle_role_registration(chat_id, user_id, text)
                    elif text == "⬅️ Назад" or self.is_valid_class(text):
                        self.handle_main_menu(chat_id, user_id, text, username)
                    else:
                        # Если пользователь не зарегистрирован, предлагаем выбрать роль
                        if not self.get_user(user_id):
                            self.handle_role_selection(chat_id, user_id)
                        else:
                            # Старая регистрация для обратной совместимости
                            self.handle_legacy_registration(chat_id, user_id, text)
        
        except Exception as e:
            logger.error(f"Ошибка в process_update: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def handle_legacy_registration(self, chat_id, user_id, text):
        """Обработка старого формата регистрации для обратной совместимости"""
        parts = text.split(',')
        if len(parts) != 2:
            self.send_message(
                chat_id,
                "❌ Неверный формат. Пожалуйста, выберите роль из меню или введите данные в формате:\n"
                "<b>Фамилия Имя Отчество, Класс</b>\n\n"
                "Например: <i>Иванов Иван Иванович, 10П</i>"
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
        if self.register_user_with_role(user_id, full_name, class_name, "student"):
            self.send_message(
                chat_id,
                f"✅ Регистрация прошла успешно!\nФИО: {self.safe_message(full_name)}\nКласс: {class_name}\nРоль: Ученик",
                self.main_menu_keyboard()
            )
        else:
            self.send_message(
                chat_id,
                f"❌ Не удалось зарегистрироваться. Возможно, достигнут лимит пользователей в классе {class_name}.",
                self.main_menu_keyboard()
            )

    def run(self):
        logger.info("Бот запущен со всеми функциями!")
        
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