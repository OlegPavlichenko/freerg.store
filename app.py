import os
import re
import sqlite3
import hashlib
import asyncio
import requests

from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from apscheduler.triggers.cron import CronTrigger

import random
import uuid
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from jinja2 import Template

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Bot, InlineKeyboardMarkup, InlineKeyboardButton


# --------------------
# CONFIG (env)
# -----------------------------------------------------
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "@freeredeemgames")

ITAD_API_KEY = os.getenv("ITAD_API_KEY", "")

DB_PATH = os.getenv("DB_PATH", "/opt/freerg/data/data.sqlite3")
SITE_BASE = os.getenv("SITE_BASE", "https://freerg.store")

# расписания (аккуратно)
STEAM_MIN = int(os.getenv("STEAM_MIN", "60"))     # Steam/ITAD раз в 60 минут
EPIC_MIN = int(os.getenv("EPIC_MIN", "720"))      # Epic раз в 12 часов
GOG_MIN = int(os.getenv("GOG_MIN", "1440"))  # 24 часа
PRIME_MIN = int(os.getenv("PRIME_MIN", "1440"))

# сколько максимум постов за 1 прогон (чтобы не залить канал)
POST_LIMIT = int(os.getenv("POST_LIMIT", "10"))

# tz для красивого дедлайна (Бишкек UTC+6)
BISHKEK_TZ = ZoneInfo("Asia/Bishkek")

EPIC_COUNTRY = os.getenv("EPIC_COUNTRY", "KG")   # попробуй KG
EPIC_LOCALE  = os.getenv("EPIC_LOCALE", "ru-RU")

app = FastAPI()
bot = Bot(token=TG_BOT_TOKEN) if TG_BOT_TOKEN else None

scheduler = AsyncIOScheduler()
_scheduler_started = False
JOB_LOCK = asyncio.Lock()



# ==========================================
# 🛡️ АНТИСПАМ И АДМИНКА
# ==========================================

import hashlib
import re

# Хэш пароля админа (по умолчанию: "admin123")
# Генерируй свой: echo -n "твой_пароль" | sha256sum
ADMIN_PASSWORD_HASH = os.getenv(
    "ADMIN_PASSWORD_HASH",
    "9f3fd4cc3c3d4d80c229578b00dec9c253494ed2370b20caa23b3cf4bc63b3ba"  # admin123
)

def validate_lfg_text(text: str) -> tuple[bool, str | None]:
    """
    Проверяет текст на спам/ссылки/контакты.
    Возвращает (valid, error_message)
    """
    if not text:
        return True, None
    
    text_lower = text.lower()
    text_clean = text.replace(" ", "").replace("-", "")
    
    # 🔥 ЗАПРЕТ ССЫЛОК
    link_patterns = [
        r'https?://',           # http://, https://
        r'www\.',               # www.
        r'\.(com|ru|org|net|io|gg|me|cc|tv|link)',  # домены
        r't\.me',               # Telegram
        r'discord\.gg',         # Discord
        r'vk\.com',             # VK
        r'youtube\.com',        # YouTube
        r'twitch\.tv',          # Twitch
    ]
    
    for pattern in link_patterns:
        if re.search(pattern, text_lower, re.IGNORECASE):
            return False, "❌ Ссылки запрещены"
    
    # 🔥 ЗАПРЕТ EMAIL
    if '@' in text:
        email_domains = ['gmail', 'mail', 'yandex', 'outlook', 'icloud', 'yahoo', 'proton']
        for domain in email_domains:
            if domain in text_lower:
                return False, "❌ Email запрещены"
    
    # 🔥 ЗАПРЕТ ТЕЛЕФОНОВ
    phone_patterns = [
        r'\+\d{10,}',           # +79991234567
        r'8-?800',              # 8-800
        r'\d{3}[-\s]?\d{3}[-\s]?\d{2}[-\s]?\d{2}',  # 999-123-45-67
    ]
    
    for pattern in phone_patterns:
        if re.search(pattern, text_clean):
            return False, "❌ Телефоны запрещены"
    
    # 🔥 ЗАПРЕТ ПОДОЗРИТЕЛЬНЫХ СЛОВ (опционально)
    spam_words = ['casino', 'viagra', 'buy now', 'click here', 'free money']
    for word in spam_words:
        if word in text_lower:
            return False, "❌ Подозрительный текст"
    
    return True, None


def check_rate_limit(ip: str, hours: int = 1, limit: int = 3) -> bool:
    """
    Проверяет лимит заявок с одного IP.
    Возвращает True если можно создавать, False если превышен лимит.
    """
    if not ip or ip == "unknown":
        return True  # не блокируем если IP неизвестен
    
    conn = db()
    cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
    
    count = conn.execute("""
        SELECT COUNT(*) FROM lfg
        WHERE ip=? AND created_at > ?
    """, (ip, cutoff)).fetchone()[0]
    
    conn.close()
    return count < limit


def check_admin_password(password: str) -> bool:
    """Проверяет пароль админа"""
    if not password:
        return False
    hash_input = hashlib.sha256(password.encode()).hexdigest()
    return hash_input == ADMIN_PASSWORD_HASH


def require_admin(request: Request):
    """Проверяет что пользователь залогинен как админ"""
    token = request.cookies.get("admin_token")
    if token != ADMIN_PASSWORD_HASH:
        return RedirectResponse("/admin/login", status_code=302)
    return None


# --------------------
# DB helpers
# --------------------
def db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row

    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA busy_timeout=5000;")

    # порядок важен
    ensure_tables(conn)        # таблицы
    ensure_lfg_columns(conn)   # колонки
    ensure_lfg_indexes(conn)   # индексы

    # Создаем папку, если её нет (на всякий случай)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    # ВОТ ЭТА СТРОКА ОЖИВИТ КЛИКИ И LFG:
    conn.row_factory = sqlite3.Row

    return conn

def ensure_tables(conn: sqlite3.Connection) -> None:
    # 🔥 DEALS - ОСНОВНАЯ ТАБЛИЦА!
    conn.execute("""
      CREATE TABLE IF NOT EXISTS deals (
        id TEXT PRIMARY KEY,
        store TEXT,
        external_id TEXT,
        kind TEXT,
        title TEXT,
        url TEXT,
        image_url TEXT,
        source TEXT,
        starts_at TEXT,
        ends_at TEXT,
        discount_pct INTEGER,
        price_old REAL,
        price_new REAL,
        currency TEXT,
        posted INTEGER DEFAULT 0,
        created_at TEXT
      );
    """)
    
    # Индексы для deals
    conn.execute("CREATE INDEX IF NOT EXISTS idx_deals_posted ON deals(posted);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_deals_store ON deals(store);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_deals_kind ON deals(kind);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_deals_created ON deals(created_at);")

    # clicks
    conn.execute("""
      CREATE TABLE IF NOT EXISTS clicks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT,
        deal_id TEXT,
        src TEXT,
        utm_campaign TEXT,
        utm_content TEXT,
        ip TEXT,
        user_agent TEXT,
        referer TEXT
      );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_clicks_deal_id ON clicks(deal_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_clicks_created ON clicks(created_at);")

    # free_games
    conn.execute("""
      CREATE TABLE IF NOT EXISTS free_games (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        store TEXT NOT NULL,
        title TEXT NOT NULL,
        url TEXT NOT NULL UNIQUE,
        image_url TEXT,
        note TEXT,
        sort INTEGER DEFAULT 100,
        created_at TEXT DEFAULT (datetime('now'))
      );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS votes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT,
        deal_id TEXT,
        vote INTEGER,              -- +1 / -1
        ip TEXT,
        user_agent TEXT
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_votes_deal_id ON votes(deal_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_votes_created ON votes(created_at);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_votes_ip ON votes(ip);")

    # (опционально) защита от дублей "1 голос на 24ч" на уровне базы:
    conn.execute("""
    CREATE TABLE IF NOT EXISTS vote_locks (
        deal_id TEXT,
        ip TEXT,
        day TEXT,                  -- YYYY-MM-DD
        PRIMARY KEY (deal_id, ip, day)
    );
    """)

    # Ручное добавление новостей (эксклюзив) manual_news
    conn.execute("""
    CREATE TABLE IF NOT EXISTS manual_news (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT,
        title TEXT,
        url TEXT,
        image_url TEXT,
        store TEXT,
        kind TEXT,
        price_old REAL,
        price_new REAL,
        currency TEXT,
        ends_at TEXT,
        is_published INTEGER DEFAULT 1
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_manual_news_created ON manual_news(created_at);")

    conn.commit()

import sqlite3

def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;",
        (name,),
    ).fetchone()
    return row is not None

def has_column(conn: sqlite3.Connection, table: str, col: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
    # rows: (cid, name, type, notnull, dflt_value, pk)
    return any(r[1] == col for r in rows)

def add_column_if_missing(conn: sqlite3.Connection, table: str, col: str, ddl_type: str) -> None:
    if not has_column(conn, table, col):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl_type};")

def ensure_columns() -> None:
    """
    Миграция для deals - добавляем недостающие колонки.
    """
    conn = db()
    
    # Проверяем наличие колонок
    if not table_exists(conn, "deals"):
        conn.close()
        return
    
    # Добавляем недостающие колонки если их нет
    add_column_if_missing(conn, "deals", "store", "TEXT")
    add_column_if_missing(conn, "deals", "external_id", "TEXT")
    add_column_if_missing(conn, "deals", "kind", "TEXT")
    add_column_if_missing(conn, "deals", "image_url", "TEXT")
    add_column_if_missing(conn, "deals", "discount_pct", "INTEGER")
    add_column_if_missing(conn, "deals", "price_old", "REAL")
    add_column_if_missing(conn, "deals", "price_new", "REAL")
    add_column_if_missing(conn, "deals", "currency", "TEXT")
    
    conn.commit()
    conn.close()


def ensure_lfg_columns(conn: sqlite3.Connection) -> None:
    # таблица точно есть после ensure_tables, но оставим защиту
    if not table_exists(conn, "lfg"):
        return

    add_column_if_missing(conn, "lfg", "active", "INTEGER DEFAULT 1")
    add_column_if_missing(conn, "lfg", "expires_at", "TEXT")
    add_column_if_missing(conn, "lfg", "tg_chat_url", "TEXT")

    conn.commit()


def ensure_lfg_indexes(conn: sqlite3.Connection) -> None:
    if not table_exists(conn, "lfg"):
        return

    if has_column(conn, "lfg", "expires_at"):
        conn.execute("CREATE INDEX IF NOT EXISTS idx_lfg_expires ON lfg(expires_at);")

    if has_column(conn, "lfg", "active") and has_column(conn, "lfg", "expires_at"):
        conn.execute("CREATE INDEX IF NOT EXISTS idx_lfg_active_expires ON lfg(active, expires_at);")

    conn.commit()


def backfill_defaults():
    """
    Чтобы старые записи (до миграции) не пропали при фильтрации.
    """
    conn = db()
    conn.execute("UPDATE deals SET store='steam' WHERE store IS NULL OR store=''")
    conn.execute("UPDATE deals SET kind='free_to_keep' WHERE kind IS NULL OR kind=''")
    conn.commit()
    conn.close()


def deal_id(store: str, external_id: str, url: str) -> str:
    base = f"{store}|{url}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:24]


def format_expiry(expiry_iso: str | None) -> str:
    if not expiry_iso:
        return "ограниченно (проверь в магазине)"
    s = expiry_iso.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
        dt_b = dt.astimezone(BISHKEK_TZ)
        return dt_b.strftime("%d.%m.%Y %H:%M") + " (UTC+6)"
    except Exception:
        return expiry_iso


def parse_iso_utc(s: str | None) -> datetime | None:
    if not s:
        return None
    t = s.strip()
    if t.endswith("Z"):
        t = t[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(t)
    except Exception:
        return None


def is_new(created_at: str | None, hours: int = 24) -> bool:
    dt = parse_iso_utc(created_at)
    if not dt:
        return False
    return dt >= (datetime.now(timezone.utc) - timedelta(hours=hours))


def time_left_label(ends_at: str | None) -> str | None:
    dt = parse_iso_utc(ends_at)
    if not dt:
        return None
    now = datetime.now(timezone.utc)
    delta = dt - now
    if delta.total_seconds() <= 0:
        return "истекло"

    hours = int(delta.total_seconds() // 3600)
    mins = int((delta.total_seconds() % 3600) // 60)

    if hours >= 48:
        days = hours // 24
        return f"осталось {days} дн"
    if hours >= 1:
        return f"осталось {hours} ч"
    return f"осталось {mins} мин"


def sort_key_by_ends(ends_at: str | None):
    dt = parse_iso_utc(ends_at)
    # None/битые — в конец
    return dt if dt else datetime.max.replace(tzinfo=timezone.utc)


def is_active_end(ends_at: str | None) -> bool:
    dt = parse_iso_utc(ends_at)
    if not dt:
        return True  # если дедлайна нет — считаем актуальным
    return dt > datetime.now(timezone.utc)


def is_expired_recent(ends_at: str | None, days: int = 7) -> bool:
    dt = parse_iso_utc(ends_at)
    if not dt:
        return False
    now = datetime.now(timezone.utc)
    return (dt <= now) and (dt >= now - timedelta(days=days))


def cleanup_expired(keep_days: int = 7) -> int:
    """
    Удаляем записи, у которых ends_at прошло больше, чем keep_days назад.
    keep_days=7 => неделю храним, потом чистим.
    Возвращает количество удалённых.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=keep_days)

    conn = db()
    rows = conn.execute(
        "SELECT id, ends_at FROM deals WHERE ends_at IS NOT NULL AND ends_at != ''"
    ).fetchall()

    to_delete = []
    for did, ends_at in rows:
        s = (ends_at or "").strip()
        if not s:
            continue
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
            if dt < cutoff:
                to_delete.append((did,))
        except Exception:
            # если формат даты кривой — не трогаем
            pass

    if to_delete:
        conn.executemany("DELETE FROM deals WHERE id=?", to_delete)
        conn.commit()

    conn.close()
    return len(to_delete)

import secrets
from fastapi import Response

def get_or_set_vid(request: Request, response: Response | None = None) -> str:
    vid = request.cookies.get("vid")
    if not vid:
        vid = secrets.token_urlsafe(16)
        if response is not None:
            # 180 дней
            response.set_cookie("vid", vid, max_age=180*24*3600, httponly=True, samesite="Lax")
    return vid

def log_click(
    conn: sqlite3.Connection,
    deal_id: str,
    request: Request,
    src: str | None = None,
    utm_campaign: str | None = None,
    utm_content: str | None = None,
    visitor_id: str | None = None
):
    try:
        ip = request.client.host if request.client else None
        ua = request.headers.get("user-agent")
        ref = request.headers.get("referer")

        conn.execute("""
            INSERT INTO clicks (created_at, deal_id, src, utm_campaign, utm_content, ip, user_agent, referer, visitor_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.utcnow().isoformat(),
            deal_id,
            src,
            utm_campaign,
            utm_content,
            ip,
            ua,
            ref,
            visitor_id
        ))
        conn.commit()
    except Exception:
        pass

import secrets

VOTE_COOKIE_NAME = "frg_vid"  # visitor id

def get_client_ip(request: Request) -> str | None:
    # если у тебя прокси/CF — можно будет расширить X-Forwarded-For
    return request.client.host if request.client else None

def fmt_price(x):
    if x is None:
        return None
    try:
        v = float(x)
        if v.is_integer():
            return str(int(v))
        return f"{v:.2f}".rstrip("0").rstrip(".")
    except Exception:
        return str(x)

def normalize_currency(cur: str | None) -> str:
    c = (cur or "").strip().upper()
    if c in ("USD", "$"):
        return "USD"
    if c in ("RUB", "RUR", "₽"):
        return "RUB"
    return ""  # всё остальное не показываем

def currency_symbol(cur: str | None) -> str:
    c = normalize_currency(cur)
    return "$" if c == "USD" else ("₽" if c == "RUB" else "")

def price_line(old, new, cur):
    sym = currency_symbol(cur)
    if not sym:
        return ""  # скрываем непонятные валюты полностью
    o = fmt_price(old)
    n = fmt_price(new)
    if o and n:
        return f"{sym}{o} → {sym}{n}"
    if n:
        return f"{sym}{n}"
    if o:
        return f"{sym}{o}"
    return ""

def calc_savings(conn: sqlite3.Connection) -> dict:
    """
    Считаем по кликам и deals:
    savings_today, savings_all, clicks_today, clicks_all
    """
    today = datetime.utcnow().strftime("%Y-%m-%d")
    # created_at у тебя ISO, значит LIKE 'YYYY-MM-DD%'
    row = conn.execute("""
        SELECT
          COALESCE(SUM(CASE
            WHEN d.price_old IS NOT NULL AND d.price_new IS NOT NULL
            THEN (d.price_old - d.price_new)
            ELSE 0 END), 0) AS saved,
          COUNT(*) AS clicks
        FROM clicks c
        JOIN deals d ON d.id = c.deal_id
        WHERE c.created_at LIKE ? || '%'
    """, (today,)).fetchone()

    row_all = conn.execute("""
        SELECT
          COALESCE(SUM(CASE
            WHEN d.price_old IS NOT NULL AND d.price_new IS NOT NULL
            THEN (d.price_old - d.price_new)
            ELSE 0 END), 0) AS saved,
          COUNT(*) AS clicks
        FROM clicks c
        JOIN deals d ON d.id = c.deal_id
    """).fetchone()

    return {
        "saved_today": float(row[0] or 0),
        "clicks_today": int(row[1] or 0),
        "saved_all": float(row_all[0] or 0),
        "clicks_all": int(row_all[1] or 0),
    }

#--------------------------------------
# TG (forum conf)
#--------------------------------------

import secrets

# 1) Сюда вставишь свои ссылки на темы (из Telegram)
TG_TOPICS = {
    "general": "https://t.me/freergstore/1",
    "hot": "https://t.me/freergstore/6",
    "cs2": "https://t.me/freergstore/8",
    "bf6": "https://t.me/freergstore/16",
    "dota2": "https://t.me/freergstore/10",
    "fortnite": "https://t.me/freergstore/12",
    "gta": "https://t.me/freergstore/14",
    "other": "https://t.me/freergstore/21",
}

ALLOWED_GAMES = {"general", "hot", "cs2", "bf6", "dota2", "fortnite", "gta", "other"}
ALLOWED_PLATFORMS = {"pc", "ps", "xbox", "mobile", "other"}
ALLOWED_REGIONS = {"eu", "us", "asia", "other"}

def now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def make_id() -> str:
    # короткий id, но достаточно случайный
    return secrets.token_hex(8)

def normalize_choice(v: str | None, allowed: set[str], default: str) -> str:
    v = (v or "").strip().lower()
    return v if v in allowed else default

def clamp_text(s: str | None, max_len: int) -> str:
    s = (s or "").strip()
    if len(s) > max_len:
        s = s[:max_len].rstrip() + "…"
    return s


# --------------------
# Steam image helpers
# --------------------
def extract_steam_app_id_fast(url: str) -> str | None:
    """Извлекает app_id ЛЮБЫМ способом"""
    if not url:
        return None
    
    import re
    
    # 1. Прямой Steam URL: /app/123456
    match = re.search(r'/app/(\d+)', url)
    if match:
        return match.group(1)
    
    # 2. Из image_url если он есть в кэше или параметрах
    # Пример: если URL содержит ?appid=123456
    match = re.search(r'[?&]appid=(\d+)', url)
    if match:
        return match.group(1)
    
    # 3. 🔥 ВАЖНО: Из image_url который УЖЕ в БД!
    # Вам нужно передать image_url в extract_steam_app_id_fast
    # ИЛИ изменить логику
    
    return None

def get_real_steam_app_id(url: str) -> str | None:
    """
    Получает реальный Steam AppID, следуя по редиректам itad.link
    """
    if not url:
        return None
    
    # Если это прямой Steam URL - извлекаем быстро
    if "store.steampowered.com" in url:
        return extract_steam_app_id_fast(url)
    
    # Если это itad.link или другой редирект - делаем запрос
    try:
        resp = requests.head(url, timeout=5, allow_redirects=True)
        final_url = str(resp.url)
        
        # Извлекаем AppID из конечного URL
        return extract_steam_app_id_fast(final_url)
    except Exception as e:
        print(f"Error getting final URL for {url}: {e}")
        return None

def steam_header_image_from_url_fast(url: str) -> str | None:
    app_id = extract_steam_app_id_fast(url)
    if not app_id:
        return None
    return f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/header.jpg"

def steam_header_candidates(app_id: str) -> list[str]:
    """
    Возвращает список URL-ов обложек Steam в порядке приоритета.
    Включает как новый формат (с хешами), так и старый.
    """
    if not app_id:
        return []
    
    candidates = []
    
    # Новый формат (с хешами) - для новых игр
    candidates.extend([
        f"https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/{app_id}/header.jpg",
        f"https://shared.fastly.steamstatic.com/store_item_assets/steam/apps/{app_id}/header.jpg",
        f"https://shared.cloudflare.steamstatic.com/store_item_assets/steam/apps/{app_id}/header.jpg",
    ])
    
    # Старые CDN URL - для старых игр
    candidates.extend([
        f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/header.jpg",
        f"https://cdn.akamai.steamstatic.com/steam/apps/{app_id}/header.jpg",
        f"https://steamcdn-a.akamaihd.net/steam/apps/{app_id}/header.jpg",
        f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/capsule_616x353.jpg",
        f"https://cdn.akamai.steamstatic.com/steam/apps/{app_id}/capsule_616x353.jpg",
        f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/library_600x900.jpg",
    ])
    
    return candidates


def resolve_steam_app_id(url: str) -> str | None:
    """
    Добывает appid:
    1) быстро из URL
    2) если не получилось — ОДИН раз делает requests с редиректами
       (использовать только в update job, НЕ в рендере)
    """
    app_id = extract_steam_app_id_fast(url)
    if app_id:
        return app_id

    try:
        resp = requests.get(url, timeout=10, allow_redirects=True, headers={"User-Agent":"Mozilla/5.0"})
        final_url = str(resp.url)
        return extract_steam_app_id_fast(final_url)
    except Exception:
        return None
    
def resolve_steam_app_id_limited(url: str, allow_slow: bool = True) -> str | None:
    app_id = extract_steam_app_id_fast(url)
    if app_id:
        return app_id
    if not allow_slow:
        return None
    try:
        resp = requests.get(url, timeout=10, allow_redirects=True, headers={"User-Agent":"Mozilla/5.0"})
        return extract_steam_app_id_fast(str(resp.url))
    except Exception:
        return None

def resolve_steam_app_id_slow(url: str) -> str | None:
    """
    Делает 1 HTTP запрос с редиректами и пытается вытащить appid из финального URL.
    Использовать ТОЛЬКО в update job (fetch_*), НЕ в рендере.
    """
    try:
        resp = requests.get(url, timeout=10, allow_redirects=True, headers={"User-Agent": "Mozilla/5.0"})
        return extract_steam_app_id_fast(str(resp.url))
    except Exception:
        return None


def get_steam_images_from_page(app_id: str, url: str = None) -> dict:
    """
    УНИВЕРСАЛЬНАЯ функция для получения изображений Steam.
    Поддерживает как новый формат (с хешами), так и старый.
    """
    if not app_id:
        return {}
    
    try:
        page_url = url or f"https://store.steampowered.com/app/{app_id}/"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Cookie': 'birthtime=0; mature_content=1; wants_mature_content=1; lastagecheckage=1-0-1990',
        }
        
        resp = requests.get(page_url, headers=headers, timeout=15, allow_redirects=True)
        
        if resp.status_code != 200:
            return {}
        
        html = resp.text
        
        # Если попали на agecheck — редирект с параметром
        if '/agecheck/' in resp.url or 'agecheck' in html.lower():
            age_url = f"https://store.steampowered.com/app/{app_id}/?ageDay=1&ageMonth=1&ageYear=1990"
            resp2 = requests.get(age_url, headers=headers, timeout=15)
            if resp2.status_code == 200:
                html = resp2.text
        
        result = {
            'header': None,
            'capsule': None,
            'hero': None,
            'library': None,
            'all': []
        }
        
        # 🔥 1. НОВЫЙ ФОРМАТ (с хешами) - для новых игр
        # Пример: https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/3660800/f4994d6feded29512ec4467e2fda2decdc79b322/header.jpg
        
        # 1a. Header в новом формате
        pattern_new_header = rf'(https://shared\.[^"\'\s<>]+?steamstatic\.com/store_item_assets/steam/apps/{app_id}/[a-f0-9]{{30,50}}/header\.jpg[^"\'\s<>]*)'
        matches = re.findall(pattern_new_header, html)
        if matches:
            result['header'] = matches[0]
            result['all'].append(matches[0])
        
        # 1b. Capsule в новом формате
        pattern_new_capsule = rf'(https://shared\.[^"\'\s<>]+?steamstatic\.com/store_item_assets/steam/apps/{app_id}/[a-f0-9]{{30,50}}/capsule_616x353\.jpg[^"\'\s<>]*)'
        matches = re.findall(pattern_new_capsule, html)
        if matches:
            result['capsule'] = matches[0]
            if matches[0] not in result['all']:
                result['all'].append(matches[0])
        
        # 1c. Любые изображения в новом формате
        pattern_new_any = rf'(https://shared\.[^"\'\s<>]+?steamstatic\.com/store_item_assets/steam/apps/{app_id}/[a-f0-9]{{30,50}}/[^"\'\s<>]+?\.jpg[^"\'\s<>]*)'
        matches = re.findall(pattern_new_any, html)
        for img_url in matches[:10]:
            if img_url not in result['all']:
                result['all'].append(img_url)
        
        # 🔥 2. СТАРЫЙ ФОРМАТ (без хешей) - для старых игр
        # Пример: https://cdn.cloudflare.steamstatic.com/steam/apps/730/header.jpg
        
        # 2a. Header в старом формате (если еще не нашли)
        if not result['header']:
            pattern_old_header = rf'(https://[^"\'\s<>]+?steamstatic\.com/steam/apps/{app_id}/header\.jpg)'
            matches = re.findall(pattern_old_header, html)
            if matches:
                result['header'] = matches[0]
                if matches[0] not in result['all']:
                    result['all'].append(matches[0])
        
        # 2b. Capsule в старом формате (если еще не нашли)
        if not result['capsule']:
            pattern_old_capsule = rf'(https://[^"\'\s<>]+?steamstatic\.com/steam/apps/{app_id}/capsule_616x353\.jpg)'
            matches = re.findall(pattern_old_capsule, html)
            if matches:
                result['capsule'] = matches[0]
                if matches[0] not in result['all']:
                    result['all'].append(matches[0])
        
        # 2c. Hero в старом формате
        pattern_old_hero = rf'(https://[^"\'\s<>]+?steamstatic\.com/steam/apps/{app_id}/hero_capsule\.jpg)'
        matches = re.findall(pattern_old_hero, html)
        if matches:
            result['hero'] = matches[0]
            if matches[0] not in result['all']:
                result['all'].append(matches[0])
        
        # 2d. Library в старом формате
        pattern_old_lib = rf'(https://[^"\'\s<>]+?steamstatic\.com/steam/apps/{app_id}/library_600x900\.jpg)'
        matches = re.findall(pattern_old_lib, html)
        if matches:
            result['library'] = matches[0]
            if matches[0] not in result['all']:
                result['all'].append(matches[0])
        
        # 🔥 3. JSON данные в HTML (часто там есть изображения)
        pattern_json = r'"header_image":"([^"]+)"'
        matches = re.findall(pattern_json, html)
        for img_url in matches:
            if img_url and img_url not in result['all']:
                result['all'].append(img_url)
                if not result['header'] and 'header' in img_url:
                    result['header'] = img_url
        
        # 🔥 4. Если ничего не нашли, пробуем стандартные URL
        if not result['all']:
            standard_urls = [
                f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/header.jpg",
                f"https://cdn.akamai.steamstatic.com/steam/apps/{app_id}/header.jpg",
                f"https://steamcdn-a.akamaihd.net/steam/apps/{app_id}/header.jpg",
                f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/capsule_616x353.jpg",
            ]
            
            for standard_url in standard_urls:
                try:
                    resp_test = requests.head(standard_url, timeout=2)
                    if resp_test.status_code == 200:
                        result['all'].append(standard_url)
                        if not result['header'] and 'header.jpg' in standard_url:
                            result['header'] = standard_url
                        elif not result['capsule'] and 'capsule_616x353' in standard_url:
                            result['capsule'] = standard_url
                        break
                except:
                    continue
        
        # Выбираем лучшее изображение
        best = result['header'] or result['capsule'] or result['hero'] or result['library']
        if best and best not in result.get('all', []):
            result['all'].append(best)
        
        return result
        
    except Exception as e:
        print(f"Error scraping Steam page for {app_id}: {e}")
        return {}


def steam_header_image_from_url(url: str) -> str | None:
    app_id = extract_steam_app_id_fast(url)
    if not app_id:
        return None
    return f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/header.jpg"

def steam_best_header_from_url(url: str) -> str | None:
    app_id = extract_steam_app_id_fast(url)
    if not app_id:
        return None
    return steam_header_candidates(app_id)[0]  # первый как основной

def steam_header_cdn_from_url(url: str) -> str | None:
    """
    Быстро строит ссылку на обложку Steam по appid из URL:
    https://cdn.akamai.steamstatic.com/steam/apps/<appid>/header.jpg
    """
    if not url:
        return None
    m = re.search(r"/app/(\d+)", url)
    if not m:
        return None
    appid = m.group(1)
    return f"https://cdn.akamai.steamstatic.com/steam/apps/{appid}/header.jpg"

def validate_steam_app_id(app_id: str) -> bool:
    """
    Проверяет, является ли AppID валидным для Steam.
    Возвращает True если изображение существует.
    """
    if not app_id or not app_id.isdigit():
        return False
    
    # Пробуем несколько типов изображений
    test_urls = [
        f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/header.jpg",
        f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/capsule_616x353.jpg",
        f"https://cdn.akamai.steamstatic.com/steam/apps/{app_id}/header.jpg",
    ]
    
    for test_url in test_urls:
        try:
            resp = requests.head(test_url, timeout=3, allow_redirects=True)
            if resp.status_code == 200:
                content_type = resp.headers.get('Content-Type', '')
                if 'image' in content_type or 'jpeg' in content_type:
                    return True
        except:
            continue
    
    return False


# --------------------
# SOURCES: ITAD (Prime)
# --------------------
def fetch_prime_blog():
    """
    Берём последние статьи Prime Gaming Blog по тегу "free-games-with-prime"
    и добавляем как записи (дайджест).
    """
    url = "https://primegaming.blog/tagged/free-games-with-prime"
    r = requests.get(url, timeout=25, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    html = r.text

    # очень простой парсинг ссылок на статьи (Medium-подобная разметка часто меняется)
    # но работает как старт. Если захочешь — улучшим до BeautifulSoup.
    links = []
    for part in html.split('href="'):
        if part.startswith("https://primegaming.blog/") and "-" in part:
            link = part.split('"', 1)[0]
            if link not in links:
                links.append(link)
        if len(links) >= 5:
            break

    out = []
    for link in links:
        out.append({
            "store": "prime",
            "external_id": link,
            "kind": "free_to_keep",
            "title": "Prime Gaming: Free Games with Prime (monthly update)",
            "url": link,
            "image_url": None,
            "source": "primegaming.blog",
            "starts_at": None,
            "ends_at": None,  # обычно в посте нет строгого дедлайна на уровне статьи
        })

    return out


# --------------------
# SOURCES: ITAD (GOG)
# --------------------
def fetch_itad_gog():
    """
    GOG freebies через ITAD deals/v2.
    shop id GOG у ITAD = 35.
    """
    if not ITAD_API_KEY:
        return []

    endpoint = "https://api.isthereanydeal.com/deals/v2"
    params = {
        "key": ITAD_API_KEY,
        "shops": "35",     # GOG
        "limit": "200",
        "sort": "-cut",
    }

    r = requests.get(endpoint, params=params, timeout=25)
    r.raise_for_status()
    data = r.json()

    if isinstance(data, list):
        items = data
    else:
        items = data.get("list") or data.get("data") or data.get("items") or data.get("result") or []

    out = []
    for it in items:
        if not isinstance(it, dict):
            continue

        deal = it.get("deal") if isinstance(it.get("deal"), dict) else it
        cut = deal.get("cut")
        price_obj = deal.get("price") or {}
        price_amount = price_obj.get("amount") if isinstance(price_obj, dict) else None

        # free-to-keep: 100% или цена 0
        if not (cut == 100 or price_amount == 0):
            continue

        title = it.get("title") or it.get("name") or deal.get("title") or deal.get("name") or "GOG giveaway"
        url = deal.get("url") or it.get("url")
        if not url:
            continue

        expiry = deal.get("expiry") or it.get("expiry")
        start = deal.get("start") or it.get("start")

        out.append({
            "store": "gog",
            "external_id": deal.get("id") or url,  # fallback
            "kind": "free_to_keep",
            "title": title,
            "url": url,
            "image_url": None,
            "source": "itad",
            "starts_at": start,
            "ends_at": expiry,
        })

    return out


# --------------------
# SOURCES: ITAD (Steam)
# --------------------
def fetch_itad_steam(limit: int = 200, slow_limit: int = 20):
    """
    Steam freebies через ITAD deals/v2.
    Сразу получаем конечные Steam URL вместо itad.link!
    """
    if not ITAD_API_KEY:
        return []

    endpoint = "https://api.isthereanydeal.com/deals/v2"
    params = {
        "key": ITAD_API_KEY,
        "shops": "61",          # Steam
        "limit": str(limit),
        "sort": "-cut",
    }

    r = requests.get(endpoint, params=params, timeout=25)
    print("ITAD URL:", r.url)
    print("ITAD STATUS:", r.status_code)
    if r.status_code >= 400:
      print("ITAD ERROR BODY:", (r.text or "")[:800])
    r.raise_for_status()
    data = r.json()

    items = data if isinstance(data, list) else (
        data.get("list") or data.get("data") or data.get("items") or data.get("result") or []
    )

    out: list[dict] = []
    scrape_left = 10  # парсинг страниц для изображений

    for it in items:
        if not isinstance(it, dict):
            continue

        deal = it.get("deal") if isinstance(it.get("deal"), dict) else it

        cut = deal.get("cut")
        price_obj = deal.get("price") or {}
        price_amount = price_obj.get("amount") if isinstance(price_obj, dict) else None

        # free-to-keep: 100% или цена 0
        if not (cut == 100 or price_amount == 0):
            continue

        title = (
            it.get("title") or it.get("name")
            or deal.get("title") or deal.get("name")
            or "Steam giveaway"
        )

        itad_url = deal.get("url") or it.get("url")
        if not itad_url:
            continue

        # 🔥 ВАЖНО: Получаем конечный Steam URL вместо itad.link
        steam_url = itad_url
        app_id = None

        try:
            if "itad.link" in itad_url:
                # Делаем GET запрос с редиректами
                resp = requests.get(itad_url, timeout=8, allow_redirects=True, headers={"User-Agent": "Mozilla/5.0"})
                steam_url = str(resp.url)
                print(f"  🔄 Редирект: {itad_url[:50]}... -> {steam_url[:60]}...")
        
                # Извлекаем AppID
                app_id = extract_steam_app_id_fast(steam_url)
        except Exception as e:
            print(f"  ⚠️  Редирект ошибка: {e}")
            # Пробуем извлечь из исходного URL
            app_id = extract_steam_app_id_fast(itad_url)

        if not app_id:
            app_id = extract_steam_app_id_fast(steam_url) or ""
        
        expiry = deal.get("expiry") or it.get("expiry")
        start = deal.get("start") or it.get("start")

        # appid: теперь извлекаем из конечного Steam URL
        app_id = extract_steam_app_id_fast(steam_url) or ""

        # 🔥 Парсим изображения со страницы Steam
        image_url = None
        if app_id and scrape_left > 0:
            scrape_left -= 1
            try:
                images = get_steam_images_from_page(app_id, steam_url)
                image_url = (
                    images.get('header') or 
                    images.get('hero') or 
                    images.get('capsule') or 
                    images.get('library')
                )
            except Exception:
                pass
        
        # Фоллбэк на стандартные URL
        if not image_url and app_id:
            # Для новых игр (> 10 млн) используем новый формат
            app_num = int(app_id) if app_id.isdigit() else 0
            if app_num >= 10000000:  # Новые игры
                image_url = f"https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/{app_id}/header.jpg"
            else:  # Старые игры
                image_url = f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/header.jpg"

        out.append({
            "store": "steam",
            "external_id": app_id,
            "kind": "free_to_keep",
            "title": title,
            "url": steam_url,  # 🔥 Сохраняем конечный Steam URL, а не itad.link!
            "image_url": image_url,
            "source": "itad",
            "starts_at": start,
            "ends_at": expiry,
        })

    return out

def fetch_itad_steam_hot_deals(
    min_cut: int = 70,
    limit: int = 200,
    keep: int = 20,
    mix_70_89: int = 14,
    mix_90_plus: int = 6,
):
    """
    Steam hot deals через ITAD deals/v2.
    Гарантируем микс:
      - mix_70_89 штук со скидкой 70–89
      - mix_90_plus штук со скидкой 90+
    Остальное добиваем чем есть.
    """
    limit = min(int(limit), 200)     # ✅ ITAD max
    keep = min(int(keep), limit)     # ✅ нельзя keep больше чем limit
    if not ITAD_API_KEY:
        return []

    endpoint = "https://api.isthereanydeal.com/deals/v2"
    params = {
        "key": ITAD_API_KEY,
        "shops": "61",          # Steam
        "limit": str(limit),
        "sort": "-cut",
    }

    r = requests.get(endpoint, params=params, timeout=25)

    print("ITAD URL:", r.url)
    print("ITAD STATUS:", r.status_code)

    if r.status_code >= 400:
        print("ITAD ERROR BODY:", (r.text or "")[:800])
    r.raise_for_status()
    data = r.json()

    items = data if isinstance(data, list) else (
        data.get("list") or data.get("data") or data.get("items") or data.get("result") or []
    )

    cand_70_89: list[dict] = []
    cand_90_plus: list[dict] = []
    seen_urls = set()

    def push_candidate(it: dict, deal: dict, cut: int, url: str) -> None:
        title = it.get("title") or it.get("name") or deal.get("title") or deal.get("name") or "Steam deal"
        expiry = deal.get("expiry") or it.get("expiry")
        start = deal.get("start") or it.get("start")

        price_obj = deal.get("price") or {}
        price_amount = price_obj.get("amount") if isinstance(price_obj, dict) else None
        currency = price_obj.get("currency") if isinstance(price_obj, dict) else None
        currency = normalize_currency(currency)  # USD/RUB/"" (как мы обсуждали)

        regular_obj = deal.get("regular") or deal.get("regularPrice") or deal.get("regular_price") or {}
        old_amount = regular_obj.get("amount") if isinstance(regular_obj, dict) else None

        cand = {
            "store": "steam",
            "external_id": "",           # не обязательно для hot_deal
            "kind": "hot_deal",
            "title": title,
            "url": url,
            "image_url": None,           # пусть сайт сам строит header по appid
            "source": "itad",
            "starts_at": start,
            "ends_at": expiry,
            "discount_pct": int(cut),
            "price_old": old_amount,
            "price_new": price_amount,
            "currency": currency,
        }

        if 70 <= cut <= 89:
            cand_70_89.append(cand)
        elif cut >= 90:
            cand_90_plus.append(cand)

    for it in items:
        if not isinstance(it, dict):
            continue

        deal = it.get("deal") if isinstance(it.get("deal"), dict) else it
        cut = deal.get("cut")
        if cut is None:
            continue
        try:
            cut = int(cut)
        except Exception:
            continue

        if cut < min_cut:
            continue

        # не берём бесплатные, чтобы не дублировать free_to_keep
        price_obj = deal.get("price") or {}
        price_amount = price_obj.get("amount") if isinstance(price_obj, dict) else None
        if cut == 100 or price_amount == 0:
            continue

        url = deal.get("url") or it.get("url")
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)

        push_candidate(it, deal, cut, url)

    import random
    random.shuffle(cand_70_89)
    random.shuffle(cand_90_plus)

    picked = cand_70_89[:mix_70_89] + cand_90_plus[:mix_90_plus]

    # если не хватило одной корзины — добиваем из другой
    if len(picked) < keep:
        rest = cand_90_plus[mix_90_plus:] + cand_70_89[mix_70_89:]
        picked += rest[: (keep - len(picked))]

    return picked[:keep]


# --------------------
# SOURCES: Epic
# --------------------
def epic_product_url(e: dict, locale: str) -> str:
    loc = (locale or "en-US").split("-")[0]

    # 1) Самый надёжный путь — offerMappings
    for m in (e.get("offerMappings") or []):
        if m.get("pageType") == "productHome" and m.get("pageSlug"):
            slug = m["pageSlug"].strip("/")
            return f"https://store.epicgames.com/{loc}/p/{slug}"

    # 2) fallback — старые поля (на всякий случай)
    slug = (
        e.get("productPageSlug")
        or e.get("urlSlug")
        or e.get("productSlug")
        or ""
    ).strip().replace("/home", "").strip("/")

    if slug:
        return f"https://store.epicgames.com/{loc}/p/{slug}"

    # 3) последний fallback
    return f"https://store.epicgames.com/{loc}/free-games"


def epic_canonicalize(url: str) -> str:
    try:
        resp = requests.get(
            url,
            timeout=15,
            allow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        # если страница реально существует, resp.url станет канонической
        if resp.status_code in (200, 301, 302, 303, 307, 308):
            return str(resp.url)
    except Exception:
        pass
    return url


def fetch_epic(locale=None, country=None):
    locale = locale or EPIC_LOCALE
    country = country or EPIC_COUNTRY
    print("FETCH_EPIC RUN", locale, country)

    url = "https://store-site-backend-static-ipv4.ak.epicgames.com/freeGamesPromotions"
    params = {"locale": locale, "country": country, "allowCountries": country}

    r = requests.get(url, params=params, timeout=25, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    data = r.json()

    root = data or {}
    catalog = root.get("data", {}).get("Catalog", {})
    elements = catalog.get("searchStore", {}).get("elements", []) or []

    now = datetime.now(timezone.utc)

    def parse_iso(s):
        if not s:
            return None
        s = s.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(s)
        except Exception:
            return None

    out = []
    for e in elements:
        promos = (e.get("promotions") or {})
        blocks = (promos.get("promotionalOffers") or [])

        # собираем ВСЕ offers из всех блоков
        offers = []
        for b in blocks:
            offers.extend((b or {}).get("promotionalOffers") or [])

        # ищем активный оффер
        active = None
        for off in offers:
            sdt = parse_iso(off.get("startDate"))
            edt = parse_iso(off.get("endDate"))
            if sdt and edt and sdt <= now <= edt:
                active = off
                break
        if not active:
            continue

        title = e.get("title") or "Epic freebie"
        cands = epic_url_candidates(e, locale)
        page_url = epic_pick_working_url(cands)

        if epic_is_dlc(e):
          print("EPIC DLC:", title, "->", page_url)
        else:
          print("EPIC GAME:", title, "->", page_url)

        img = None
        for ki in (e.get("keyImages") or []):
            if isinstance(ki, dict) and ki.get("url"):
                img = ki["url"]
                break

        start = active.get("startDate")
        end = active.get("endDate")

        # 🔥 ИСПРАВЛЕНИЕ: правильная логика для Epic
        price = (((e.get("price") or {}).get("totalPrice")) or {})
        discount_price = price.get("discountPrice", 0)
        original_price = price.get("originalPrice", 0)
        discount_pct = price.get("discountPercentage", 0)

        # Определяем тип:
        # - Если цена 0 И была > 0 → бесплатная раздача
        # - Если цена > 0 И скидка 70%+ → hot_deal
        # - Остальное пропускаем

        if discount_price == 0 and original_price > 0:
            kind = "free_to_keep"
        elif discount_price > 0 and original_price > 0 and discount_pct >= 70:
            kind = "hot_deal"
        else:
            continue  # пропускаем

        out.append({
            "store": "epic",
            "external_id": str(e.get("id") or e.get("namespace") or page_url),
            "kind": kind,
            "title": title,
            "url": page_url,
            "image_url": img,
            "source": "epic",
            "starts_at": start,
            "ends_at": end,
            
            # 🔥 ДОБАВИТЬ ДЛЯ HOT_DEAL:
            "discount_pct": int(discount_pct) if kind == "hot_deal" else None,
            "price_old": original_price if kind == "hot_deal" else None,
            "price_new": discount_price if kind == "hot_deal" else None,
            "currency": price.get("currencyCode") if kind == "hot_deal" else None,
        })

    return out

def epic_is_dlc(e: dict) -> bool:
    # часто type = "DLC" или "ADD_ON", но поля гуляют
    t = str(e.get("offerType") or e.get("type") or "").upper()
    if "DLC" in t or "ADD" in t or "ADDON" in t:
        return True

    # иногда есть categories
    for c in (e.get("categories") or []):
        path = str((c or {}).get("path") or "").lower()
        if "dlc" in path or "addons" in path or "add-ons" in path or "add-ons" in path:
            return True

    return False

def epic_offer_url(e: dict, locale: str) -> str | None:
    # locale -> язык в url
    loc = (locale or "en-US")
    # часть epic иногда хочет ru-RU именно, но в URL обычно "ru" или "en-US"
    # оставим en-US/ru как у тебя:
    loc_short = loc.split("-")[0]  # ru / en

    ns = e.get("namespace")
    offer_id = e.get("id") or e.get("offerId")  # чаще id
    if ns and offer_id:
        # это очень часто работает и для DLC
        return f"https://store.epicgames.com/{loc_short}/purchase?namespace={ns}&offers={offer_id}"
    return None

def epic_url_candidates(e: dict, locale: str) -> list[str]:
    cands = []

    # 0) DLC-first: offer url
    offer = epic_offer_url(e, locale)
    if offer:
        cands.append(offer)

    # 1) productHome pageSlug
    loc_short = (locale or "en-US").split("-")[0]
    for m in (e.get("offerMappings") or []):
        if m.get("pageType") == "productHome" and m.get("pageSlug"):
            slug = m["pageSlug"].strip("/")
            cands.append(f"https://store.epicgames.com/{loc_short}/p/{slug}")
            cands.append(f"https://store.epicgames.com/en-US/p/{slug}")  # fallback locale

    # 2) fallback поля
    for k in ("productPageSlug", "urlSlug", "productSlug"):
        slug = (e.get(k) or "").strip().replace("/home", "").strip("/")
        if slug:
            cands.append(f"https://store.epicgames.com/{loc_short}/p/{slug}")
            cands.append(f"https://store.epicgames.com/en-US/p/{slug}")

    # 3) общий fallback
    cands.append(f"https://store.epicgames.com/{loc_short}/free-games")
    cands.append("https://store.epicgames.com/free-games")

    # уникальные
    uniq = []
    for u in cands:
        if u not in uniq:
            uniq.append(u)
    return uniq

def epic_pick_working_url(cands: list[str]) -> str:
    """
    Проверяем первые 3 кандидата, но принимаем любой успешный ответ (200-399).
    Если ничего не работает - возвращаем первый кандидат.
    """
    headers = {"User-Agent": "Mozilla/5.0"}
    
    for u in cands[:3]:
        try:
            r = requests.get(u, timeout=8, allow_redirects=True, headers=headers)
            # 🔥 Принимаем любой успешный код (200-399)
            if 200 <= r.status_code < 400:
                return str(r.url)
        except Exception as e:
            print(f"  ⚠️ URL failed {u[:50]}: {e}")
            continue
    
    # 🔥 Если ничего не сработало - возвращаем первый (offer URL для DLC)
    print(f"  ⚠️ All URLs failed, using first: {cands[0] if cands else 'none'}")
    return cands[0] if cands else "https://store.epicgames.com/free-games"

# --------------------
# manual_news
# --------------------

import re
import requests
from datetime import datetime
from html import unescape

ADMIN_KEY = os.getenv("ADMIN_KEY", "")

def fetch_og(url: str) -> dict:
    """
    Достаём og:title / og:image с любой страницы.
    Работает для большинства магазинов/страниц.
    """
    try:
        r = requests.get(url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0 (compatible; FreeRGbot/1.0; +https://freerg.store)"
        })
        html = r.text

        def pick(pattern):
            m = re.search(pattern, html, re.IGNORECASE)
            return unescape(m.group(1)).strip() if m else None

        og_title = pick(r'property=["\']og:title["\']\s+content=["\']([^"\']+)')
        og_image = pick(r'property=["\']og:image["\']\s+content=["\']([^"\']+)')
        title_tag = pick(r"<title[^>]*>(.*?)</title>")

        return {
            "title": og_title or title_tag,
            "image": og_image
        }
    except Exception:
        return {"title": None, "image": None}

from fastapi import Form
from fastapi.responses import HTMLResponse, RedirectResponse

ADD_NEWS_PAGE = Template("""
<!doctype html><html><head>
<meta charset="utf-8"/>
<meta name="robots" content="noindex,nofollow">
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Add news</title>
<style>
 body{font-family:system-ui;background:#0a0e1a;color:#e2e8f0;padding:24px}
 .card{max-width:720px;margin:0 auto;background:#11162a;border:1px solid rgba(255,255,255,.1);border-radius:16px;padding:16px}
 input,select{width:100%;padding:10px;border-radius:12px;border:1px solid rgba(255,255,255,.15);background:#0b1022;color:#e2e8f0;margin:6px 0}
 button{padding:10px 14px;border-radius:12px;border:0;background:#4f46e5;color:white;font-weight:700;cursor:pointer}
 .row{display:grid;grid-template-columns:1fr 1fr;gap:10px}
 .muted{opacity:.75;font-size:13px}
</style>
</head><body>
<div class="card">
  <h2 style="margin:0 0 8px">Добавить эксклюзив</h2>
  <div class="muted">Вставь ссылку — подтянем title + картинку. Цена: можно вручную или Steam auto.</div>
  <form method="post" action="/admin/news/add?key={{ key }}">
    <label>URL</label>
    <input name="url" required placeholder="https://...">
    <div class="row">
      <div>
        <label>Store</label>
        <select name="store">
          <option value="steam">steam</option>
          <option value="epic">epic</option>
          <option value="gog">gog</option>
          <option value="prime">prime</option>
          <option value="other" selected>other</option>
        </select>
      </div>
      <div>
        <label>Kind</label>
        <select name="kind">
          <option value="free_to_keep">free_to_keep</option>
          <option value="hot_deal">hot_deal</option>
          <option value="free_weekend">free_weekend</option>
          <option value="news" selected>news</option>
        </select>
      </div>
    </div>

    <label>Title (если оставить пустым — возьмём с сайта)</label>
    <input name="title" placeholder="необязательно">

    <div class="row">
      <div>
        <label>Old price</label>
        <input name="price_old" placeholder="например 59.99">
      </div>
      <div>
        <label>New price</label>
        <input name="price_new" placeholder="например 0">
      </div>
    </div>

    <label>Currency</label>
    <input name="currency" placeholder="USD" value="USD">

    <label>Ends at (опционально, ISO или просто текст)</label>
    <input name="ends_at" placeholder="2026-03-01 23:59">

    <button type="submit">Добавить</button>
  </form>
</div>
</body></html>
""")

@app.get("/admin/news", response_class=HTMLResponse)
def admin_news(key: str):
    if key != ADMIN_KEY:
        return HTMLResponse("Forbidden", status_code=403)
    return ADD_NEWS_PAGE.render(key=key)

@app.post("/admin/news/add")
def admin_news_add(
    key: str,
    url: str = Form(...),
    store: str = Form("other"),
    kind: str = Form("news"),
    title: str = Form(""),
    price_old: str = Form(""),
    price_new: str = Form(""),
    currency: str = Form("USD"),
    ends_at: str = Form(""),
):
    if key != ADMIN_KEY:
        return HTMLResponse("Forbidden", status_code=403)

    meta = fetch_og(url)
    final_title = title.strip() or meta.get("title") or "(no title)"
    image = meta.get("image") or ""

    old_val = float(price_old) if price_old.strip() else None
    new_val = float(price_new) if price_new.strip() else None

    # Steam auto-price если user не указал цену
    if store == "steam" and (old_val is None and new_val is None):
        sp = steam_price_by_url(url)
        if sp:
            old_val, new_val, currency = sp

    conn = db()
    conn.execute("""
        INSERT INTO manual_news (created_at, title, url, image_url, store, kind, price_old, price_new, currency, ends_at, is_published)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
    """, (
        datetime.utcnow().isoformat(),
        final_title, url, image, store, kind,
        old_val, new_val, currency, (ends_at.strip() or None)
    ))
    conn.commit()
    conn.close()

    return RedirectResponse(url=f"/admin/news?key={key}", status_code=302)

# --------------------
# SAVE + POST
# --------------------
def save_deals(deals: list[dict]):
    conn = db()
    now = datetime.now(timezone.utc).isoformat()

    new_items = 0
    for d in deals:
        store = d.get("store") or ""
        external_id = d.get("external_id") or ""
        url = d.get("url") or ""
        if not url:
            continue

        did = deal_id(store, external_id, url)

        cur = conn.execute(
            "INSERT OR IGNORE INTO deals (id,store,external_id,kind,title,url,image_url,source,starts_at,ends_at,discount_pct,price_old,price_new,currency,posted,created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?)",
            (
                did,
                store,
                external_id,
                d.get("kind", ""),
                d.get("title", ""),
                url,
                d.get("image_url", ""),
                d.get("source", ""),
                d.get("starts_at"),
                d.get("ends_at"),
                d.get("discount_pct"),
                d.get("price_old"),
                d.get("price_new"),
                d.get("currency"),
                now,
            ),
        )
        if cur.rowcount == 1:
            new_items += 1

    conn.commit()
    conn.close()
    return new_items

def tg_go_url(deal_id: str, utm_content: str) -> str:
    include_button = (random.random() < 0.4)  # 40% с кнопкой, 60% без
    return f"{SITE_BASE}/go/{deal_id}?src=tg&utm_campaign=freeredeemgames&utm_content={utm_content}"

INCLUDE_BUTTON = True  # можно потом привязать к .env

def include_button() -> bool:
    return bool(INCLUDE_BUTTON)


async def post_unposted_to_telegram(limit: int = POST_LIMIT, store: str | None = None):
    """
    Постим kind in ('free_to_keep', 'free_weekend').
    Если store задан (steam/epic/...), постим только для этого магазина.
    Картинки:
      - Epic: image_url из БД
      - Steam: header.jpg по app_id из URL/редиректа
    """
    if not bot or not TG_CHAT_ID:
        return {"posted": 0, "queued": 0, "reason": "bot/chat_id missing"}

    conn = db()

    sql = """
        SELECT id,store,kind,title,url,image_url,ends_at
        FROM deals
        WHERE posted=0 AND kind IN ('free_to_keep','free_weekend')
    """
    params: list = []
    if store:
        sql += " AND store=?"
        params.append(store)

    # Сначала "навсегда", потом "временно" (чтобы лента приятнее смотрелась)
    sql += """
        ORDER BY
            CASE kind WHEN 'free_to_keep' THEN 0 ELSE 1 END,
            created_at ASC
        LIMIT ?
    """
    params.append(limit)

    rows = conn.execute(sql, tuple(params)).fetchall()
    queued = len(rows)
    posted_count = 0

    for did, st, kind, title, url, image_url, ends_at in rows:
        st = (st or "").strip().lower()

        badge = {
            "steam": "🎮 Steam",
            "epic": "🟦 Epic",
            "gog": "🟪 GOG",
            "prime": "🟨 Prime",
        }.get(st, st or "Store")

        extra = ""
        if st == "prime":
            extra = "⚠️ Требуется Prime Gaming/подписка.\n"

        # заголовок + кнопка по типу раздачи
        if kind == "free_to_keep":
          header = "🎁 *Бесплатно навсегда*"
          button_pool = ["🧭 Открыть", "🔎 Подробности", "🎮 Забрать"]
          utm_content = "free_forever"
        elif kind == "free_weekend":
          header = "⏱ *Free Weekend (временно)*"
          button_pool = ["▶️ Играть", "🧭 Открыть", "🔎 Подробности"]
          utm_content = "free_weekend"
        else:
          header = "🎮 *Акция*"
          button_pool = ["🧭 Открыть", "🔎 Подробности"]
          utm_content = "other"

        button_text = random.choice(button_pool)

        site_url = tg_go_url(did, utm_content)



        tags = f"\n#freegame #{st} #giveaway" if st else "\n#freegame #giveaway"

        # если ends_at пустой — строку "До" лучше не показывать
        expires_line = f"⏳ До: {format_expiry(ends_at)}\n" if ends_at else ""

        text = (
            f"{badge} · {header}\n\n"
            f"*{title}*\n"
            f"{extra}"
            f"{expires_line}"
            f"{tags}"
        )

        kb = InlineKeyboardMarkup([[InlineKeyboardButton(button_text, url=site_url)]]) if include_button() else None


        # выбор картинки
        photo = None
        if st == "epic" and image_url:
            photo = image_url
        elif st == "steam":
            photo = steam_header_image_from_url(url)

        try:
            # 🔥 ФИКС: Проверяем что photo валидный URL
            if photo and photo.startswith("http") and ("steamstatic.com" in photo or "epicgames.com" in photo):
                try:
                    await bot.send_photo(
                        chat_id=TG_CHAT_ID,
                        photo=photo,
                        caption=text,
                        parse_mode="Markdown",
                        reply_markup=kb,
                    )
                except Exception as e:
                    # Если фото не загрузилось - постим текстом
                    print(f"Photo failed, posting as text: {e}")
                    await bot.send_message(
                        chat_id=TG_CHAT_ID,
                        text=text,
                        parse_mode="Markdown",
                        reply_markup=kb,
                        disable_web_page_preview=False,
                    )
            else:
                # Без фото
                await bot.send_message(
                    chat_id=TG_CHAT_ID,
                    text=text,
                    parse_mode="Markdown",
                    reply_markup=kb,
                    disable_web_page_preview=False,
                )

            conn.execute("UPDATE deals SET posted=1 WHERE id=?", (did,))
            conn.commit()
            posted_count += 1

        except Exception as e:
            print("TG SEND ERROR:", e)
            break
    
    manual = conn.execute("""
        SELECT
            'm_' || id as id,
            store, kind, title, url, image_url, ends_at,
            price_old, price_new, currency
        FROM manual_news
        WHERE is_published=1
        ORDER BY datetime(created_at) DESC
        LIMIT 20
    """).fetchall()

    conn.close()
    return {"posted": posted_count, "queued": queued, "store": store or "all"}

async def job_async(store: str = "steam"):
    """
    1) забираем данные из нужного источника
    2) сохраняем в БД
    3) постим только free_to_keep (и только 'новое' — posted=0)

    Ограничения по постингу:
      - steam: до POST_LIMIT
      - epic: до 2 за прогон (чтобы не шумел)
      - другие: до 3 за прогон (можно менять)
    """
    async with JOB_LOCK:
        try:
            st = (store or "").strip().lower()

            if st == "steam":
                deals = fetch_itad_steam() + fetch_itad_steam_hot_deals(min_cut=70, limit=200, keep=60)
                new_items = save_deals(deals)
                tg = await post_unposted_to_telegram(limit=POST_LIMIT, store="steam")

            elif st == "epic":
                print("🟦 EPIC JOB RUN @", datetime.now(BISHKEK_TZ))
                deals = fetch_epic()
                new_items = save_deals(deals)
                tg = await post_unposted_to_telegram(limit=2, store="epic")

            elif st == "gog":
                deals = fetch_itad_gog()
                new_items = save_deals(deals)
                tg = await post_unposted_to_telegram(limit=3, store="gog")

            elif st == "prime":
                deals = fetch_prime_blog()
                new_items = save_deals(deals)
                tg = await post_unposted_to_telegram(limit=1, store="prime")

            else:
                deals = []
                new_items = 0
                tg = {"posted": 0, "queued": 0, "reason": f"unknown store: {store}"}

            return {"store": st, "fetched": len(deals), "new": new_items, "tg": tg}

        except Exception as e:
            print("JOB ERROR:", e)
            return {"store": store, "error": str(e)}

def fetch_gog(): return []
def fetch_prime(): return []

# --------------------
# WEBSITE
# --------------------
PAGE = Template("""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link rel="manifest" href="/static/manifest.webmanifest">
    <meta name="theme-color" content="#0a0e1a">
    <link rel="apple-touch-icon" href="/static/icons/icon-192.png">
    <link rel="manifest" href="/manifest.json">
    <meta name="theme-color" content="#0b0f19">
    <meta name="robots" content="noindex,nofollow">
    <title>Free Redeem Games Store - Бесплатные игры</title>
    <link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='75' font-size='75'>🎮</text></svg>">
    <style>
        :root {
            --bg-primary: #0a0e1a;
            --bg-card: #1a1f36;
            --bg-hover: #252a44;
            --text-primary: #e2e8f0;
            --text-secondary: #94a3b8;
            --text-muted: #64748b;
            --accent: #667eea;
            --accent-hover: #764ba2;
            --border: rgba(255, 255, 255, 0.1);
            --shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
            --radius: 12px;
        }
        
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            line-height: 1.6;
            padding-top: 0px; /* 🔥 Увеличил отступ чтобы заголовки не налезали */
            background-image: 
                radial-gradient(circle at 20% 10%, rgba(102, 126, 234, 0.08) 0%, transparent 50%),
                radial-gradient(circle at 80% 90%, rgba(118, 75, 162, 0.08) 0%, transparent 50%);
        }
                
        .collapse-btn{
            margin-top:10px;
            padding:10px 12px;
            border-radius:12px;
            border:1px solid rgba(255,255,255,.12);
            background:rgba(255,255,255,.06);
            color:#e2e8f0;
            font-weight:700;
            cursor:pointer;
        }
        .collapse-btn:hover{ background:rgba(255,255,255,.10); }

        
        /* ШАПКА */
        .header {
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            background: rgba(10, 14, 26, 0.95);
            backdrop-filter: blur(20px);
            border-bottom: 1px solid var(--border);
            z-index: 100;
            box-shadow: var(--shadow);
            transition: transform .22s ease; will-change: transform;
        }
                
        .t-mobile{ display:none; }
        .t-desktop{ display:inline; }
                
        .header.hidden {
            transform: translateY(-100%);
        }
        
        .header-content {
            max-width: 1200px;
            margin: 0 auto;
            padding: 16px 20px;
            text-align: center;
            position: relative;
        }
                
        /* мини-статистика уводим вправо */
        .mini-stats{
            position:absolute;
            top:10px;
            right:20px;

            display:flex;
            flex-direction:column;
            gap:6px;

            text-align:right;
            font-size:14px;
            opacity:.9;
        }
        
        .brand {
            margin-bottom: 12px;
        }
        
        .brand h1 {
            font-size: 1.75rem;
            font-weight: 800;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            letter-spacing: -0.5px;
            margin-bottom: 4px;
        }
        
        .brand p {
            font-size: 0.875rem;
            color: var(--text-secondary);
        }
        
        .filters {
            display: flex;
            gap: 8px;
            justify-content: center;
            flex-wrap: wrap;
            padding: 0 10px;
        }
        
        .filter-group {
            display: flex;
            gap: 6px;
            background: rgba(255, 255, 255, 0.03);
            padding: 4px;
            border-radius: 12px;
            border: 1px solid var(--border);
        }
        
        .filter-btn {
            padding: 8px 16px;
            border-radius: 8px;
            background: transparent;
            color: var(--text-secondary);
            border: 1px solid transparent;
            font-size: 0.875rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
            text-decoration: none;
            display: inline-flex;
            align-items: center;
            gap: 6px;
            white-space: nowrap;
        }
        
        .filter-btn:hover {
            background: var(--bg-hover);
            color: var(--text-primary);
            transform: translateY(-1px);
        }
        
        .filter-btn.active {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border-color: rgba(255, 255, 255, 0.2);
            box-shadow: 0 4px 12px rgba(102, 126, 234, 0.3);
        }
        
        /* 🚀 КНОПКА "НАВЕРХ" */
        .scroll-to-top {
            position: fixed;
            bottom: 30px;
            right: 30px;
            width: 50px;
            height: 50px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            border-radius: 50%;
            font-size: 1.5rem;
            cursor: pointer;
            box-shadow: 0 4px 16px rgba(102, 126, 234, 0.4);
            opacity: 0;
            visibility: hidden;
            transition: all 0.3s ease;
            z-index: 999;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        
        .scroll-to-top.show {
            opacity: 1;
            visibility: visible;
        }
        
        .scroll-to-top:hover {
            transform: translateY(-4px);
            box-shadow: 0 6px 20px rgba(102, 126, 234, 0.6);
        }
        
        .scroll-to-top:active {
            transform: translateY(-2px);
        }
        
        /* Контейнер */
        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }
        
        /* Секции */
        .section {
            margin-bottom: 40px;
        }
        
        .section-header {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 12px;
            margin-bottom: 20px;
            padding-bottom: 12px;
            border-bottom: 1px solid var(--border);
        }
        
        .section-icon {
            font-size: 1.5rem;
        }
        
        .section-title {
            font-size: 1.5rem;
            font-weight: 700;
            background: linear-gradient(135deg, #667eea, #764ba2);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        
        .section-count {
            background: var(--accent);
            color: white;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 0.875rem;
            font-weight: 700;
        }
        
        /* Сетка карточек */
        .games-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
            gap: 20px;
        }
        
        /* Карточка игры */
        .game-card {
            background: var(--bg-card);
            border-radius: var(--radius);
            overflow: hidden;
            border: 1px solid var(--border);
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            position: relative;
        }
        
        .game-card:hover {
            transform: translateY(-6px);
            border-color: rgba(102, 126, 234, 0.4);
            box-shadow: 0 12px 32px rgba(0, 0, 0, 0.4);
        }
        
        
        
        /* Бейдж магазина */
        .store-badge {
            position: absolute;
            top: 10px;
            left: 10px;
            padding: 6px 12px;
            border-radius: 8px;
            font-size: 0.75rem;
            font-weight: 700;
            text-transform: uppercase;
            backdrop-filter: blur(10px);
            z-index: 2;
            letter-spacing: 0.5px;
        }
        
        .store-steam { 
            background: rgba(27, 40, 56, 0.95);
            color: #fff;
            border: 1px solid rgba(255, 255, 255, 0.2);
        }
        .store-epic { 
            background: rgba(0, 0, 0, 0.9);
            color: #fff;
            border: 1px solid rgba(255, 255, 255, 0.2);
        }
        .store-gog { 
            background: rgba(134, 58, 138, 0.95);
            color: #fff;
            border: 1px solid rgba(255, 255, 255, 0.2);
        }
        .store-prime { 
            background: rgba(255, 153, 0, 0.95);
            color: #000;
            border: 1px solid rgba(0, 0, 0, 0.2);
        }
        
        /* Изображение */
        .game-image-container {
            position: relative;
            height: 150px;
            overflow: hidden;
            background: linear-gradient(135deg, #2d3748 0%, #4a5568 100%);
        }
        
        .game-image {
            width: 100%;
            height: 100%;
            object-fit: cover;
            transition: transform 0.4s ease;
        }
        
        .game-card:hover .game-image {
            transform: scale(1.1);
        }
        
        .image-placeholder {
            width: 100%;
            height: 100%;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            color: var(--text-muted);
            gap: 8px;
        }
        
        .image-placeholder-icon {
            font-size: 3rem;
            opacity: 0.6;
        }
        
        /* Контент карточки */
        .game-content {
            padding: 16px;
        }
        
        .game-title {
            font-size: 1.05rem;
            font-weight: 700;
            margin-bottom: 12px;
            line-height: 1.3;
            color: var(--text-primary);
            display: -webkit-box;
            -webkit-line-clamp: 2;
            -webkit-box-orient: vertical;
            overflow: hidden;
            min-height: 2.6em;
        }
                
        .section { margin-top: 18px; }
        .section-head { display:flex; align-items:flex-end; justify-content:space-between; gap:10px; margin: 10px 0 10px; }
        .section-title { margin:0; font-size:18px; font-weight:900; }
        .section-sub { opacity:.75; font-size:12px; }

        .card.exclusive { border:1px solid rgba(255,215,0,.2); }
        .exclusive-pill{
            position:absolute; top:10px; right:10px;
            padding:6px 12px; border-radius:999px;
            background:rgba(255,215,0,.2);
            border:1px solid rgba(255,215,0,.4);
            font-weight:800; font-size:11px;
            color:#ffd700;
            text-shadow:0 1px 2px rgba(0,0,0,.3);
        }        
        
        /* Теги */
        .game-meta {
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
            margin-bottom: 12px;
        }
        
        .meta-tag {
            padding: 4px 10px;
            border-radius: 6px;
            font-size: 0.75rem;
            font-weight: 700;
            background: rgba(255, 255, 255, 0.08);
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.3px;
        }
        
        .tag-new { 
            background: rgba(16, 185, 129, 0.2);
            color: #10b981;
            border: 1px solid rgba(16, 185, 129, 0.3);
        }
        
        .tag-free { 
            background: rgba(59, 130, 246, 0.2);
            color: #3b82f6;
            border: 1px solid rgba(59, 130, 246, 0.3);
        }
        
        .tag-discount {
            background: rgba(239, 68, 68, 0.2);
            color: #ef4444;
            border: 1px solid rgba(239, 68, 68, 0.3);
        }
        
        /* Таймер */
        .game-timer {
            background: rgba(255, 255, 255, 0.05);
            padding: 10px;
            border-radius: 8px;
            margin-bottom: 12px;
            font-size: 0.85rem;
            color: var(--text-secondary);
            border: 1px solid var(--border);
        }
        
        .timer-time {
            font-weight: 700;
            color: var(--text-primary);
        }
        
        /* Кнопка */
        .btn {
            display: block;
            width: 100%;
            padding: 12px;
            border-radius: 10px;
            border: none;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            font-weight: 700;
            font-size: 0.95rem;
            cursor: pointer;
            transition: all 0.3s ease;
            text-align: center;
            text-decoration: none;
            box-shadow: 0 4px 12px rgba(102, 126, 234, 0.2);
        }
        
        .btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(102, 126, 234, 0.4);
        }
        
        .btn:active {
            transform: translateY(0);
        }
        
        /* Пустой стейт */
        .empty-state {
            text-align: center;
            padding: 60px 24px;
            background: var(--bg-card);
            border-radius: var(--radius);
            border: 2px dashed var(--border);
        }
        
        .empty-icon {
            font-size: 4rem;
            margin-bottom: 20px;
            opacity: 0.5;
        }
        
        .empty-title {
            font-size: 1.5rem;
            margin-bottom: 8px;
            color: var(--text-primary);
        }
        
        .empty-description {
            color: var(--text-secondary);
        }
        
        /* 📱 АДАПТАЦИЯ ДЛЯ МОБИЛЬНЫХ */
        @media (max-width: 768px) {
            .header-content { padding: 12px 16px; }
            .brand h1 { font-size: 1.5rem; }
            .brand p { font-size: 0.8rem; }
            .filters { gap: 6px; }
            .filter-group { flex-wrap: wrap; justify-content: center; }
            .filter-btn { padding: 6px 12px; font-size: 0.8rem; }
            .games-grid { grid-template-columns: repeat(2, 1fr); gap: 12px; }
            .game-image-container { height: 110px; }
            .game-content { padding: 12px; }
            .game-title { font-size: 0.95rem; }
            .section-title { font-size: 1.25rem; }
            .container { padding: 16px 12px; }
            .scroll-to-top { width: 45px; height: 45px; bottom: 20px; right: 20px; font-size: 1.3rem; }
            .t-mobile{ display:inline; }
            .t-desktop{ display:none; }
        }
                
       .header-content{
  position:relative;
  display:flex;
  flex-direction:column;
  align-items:center;
}

/* DESKTOP: справа сверху */
.mini-stats{
  position:absolute;
  top:10px;
  right:20px;

  display:flex;
  flex-direction:column;
  gap:6px;

  text-align:right;
  font-size:14px;
  opacity:.9;

  background:rgba(255,255,255,.05);
  padding:10px 14px;
  border-radius:14px;
  border:1px solid rgba(255,255,255,.08);
  backdrop-filter:blur(6px);
}

/* MOBILE: превращаем в обычный блок, чтобы не перекрывал */
@media (max-width:900px){
  .mini-stats{
    position:static;        /* ключевое */
    width:100%;
    max-width:520px;
    margin:0 auto 12px auto;
    text-align:center;
  }
}
                 
        /* 💻 БОЛЬШИЕ ЭКРАНЫ */
        @media (min-width: 1400px) {
            .games-grid {
                grid-template-columns: repeat(4, 1fr);
            }
        }
        
        /* Анимации */
        @keyframes fadeIn {
            from {
                opacity: 0;
                transform: translateY(20px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }
        
        .game-card {
            animation: fadeIn 0.4s ease-out;
        }
        
        /* Плавная прокрутка */
        html {
            scroll-behavior: smooth;
        }
                
        /* Центр и красивый лайк-блок */
.vote-wrap{
  display:flex;
  justify-content:center;   /* по центру ячейки */
  align-items:center;
  gap:10px;
  margin-top:10px;
}

.vote-btn{
  display:inline-flex;
  align-items:center;
  justify-content:center;
  gap:8px;
  padding:8px 12px;
  border:1px solid rgba(255,255,255,.12);
  border-radius:14px;
  background:rgba(255,255,255,.06);
  color:inherit;
  cursor:pointer;
  transition:transform .08s ease, background .15s ease, border-color .15s ease;
  user-select:none;
}

.vote-btn:hover{
  background:rgba(255,255,255,.10);
  border-color:rgba(255,255,255,.22);
}

.vote-btn:active{
  transform:scale(.98);
}

.vote-ico{ font-size:18px; line-height:1; }
.vote-count{ font-weight:700; opacity:.9; }

/* подсветка выбранного */
.vote-btn.is-active.vote-up{ background:rgba(0,255,153,.12); border-color:rgba(0,255,153,.25); }
.vote-btn.is-active.vote-down{ background:rgba(255,80,80,.12); border-color:rgba(255,80,80,.25); }

/* ===== TOUR (мини-экскурс) ===== */
.tour-overlay{
  position:fixed; inset:0;
  background:rgba(0,0,0,.65);
  z-index:9999;
  display:none;
}
.tour-overlay.active{ display:block; }

.tour-highlight{
  position:relative;
  z-index:10000;
  border-radius:16px;
  box-shadow:0 0 0 4px rgba(255,255,255,.15), 0 0 0 9999px rgba(0,0,0,.65);
  transition: box-shadow .2s ease;
}

.tour-pop{
  position:fixed;
  z-index:10001;
  max-width:320px;
  background:#111827;
  color:#e5e7eb;
  border:1px solid rgba(255,255,255,.12);
  border-radius:16px;
  padding:12px 12px 10px;
  box-shadow:0 12px 40px rgba(0,0,0,.45);
  display:none;
}
.tour-pop.active{ display:block; }

.tour-title{ font-weight:800; margin:0 0 6px; font-size:14px; }
.tour-text{ margin:0 0 10px; font-size:13px; color:#cbd5e1; line-height:1.35; }

.tour-actions{
  display:flex; gap:8px; justify-content:space-between; align-items:center;
}
.tour-actions .left{ display:flex; gap:8px; }
.tour-btn{
  cursor:pointer;
  border:1px solid rgba(255,255,255,.14);
  background:rgba(255,255,255,.06);
  color:#e5e7eb;
  padding:7px 10px;
  border-radius:12px;
  font-size:13px;
}
.tour-btn.primary{
  background:rgba(99,102,241,.25);
  border-color:rgba(99,102,241,.45);
}
.tour-step{
  font-size:12px; color:#94a3b8;
}    
    </style>
</head>
<body>
    <!-- ШАПКА -->
    <div class="header">
        <div class="header-content">
            <div class="brand">
                <div class="mini-stats" data-tour="stats">
  <div class="mini-stat">💸 Сэкономили сегодня: <b>${{ "%.2f"|format(savings.saved_today) }}</b></div>
  <div class="mini-stat">📦 Клики сегодня: <b>{{ savings.clicks_today }}</b></div>
  <div class="mini-stat" style="opacity:.8">Всего сэкономили: <b>${{ "%.2f"|format(savings.saved_all) }}</b></div>
</div></div>
              <h1>🎮 FreeRG.store</h1>
              <p>Актуальные бесплатные игры и скидки</p>
                
                
              <div class="header-divider">

  <!-- КНОПКА раскрытия -->
  <button class="collapse-btn" id="collapseBtn" type="button" aria-expanded="false">
    Фильтры ▾
  </button>
                
    <button class="tour-btn primary" id="tourStartBtn" type="button">✨ Tutorial (Подсказка)</button>

  <!-- ПАНЕЛЬ фильтров (по умолчанию свернута) -->
  <div class="filters-wrap" id="filtersWrap" style="max-height:0; overflow:hidden; transition:max-height .25s ease;">
    <div class="filters" data-tour="filters">
      <!-- Группа: Тип -->
      <div class="filter-group">
        <a href="/?kind=all&store={{ store }}" class="filter-btn {% if kind == 'all' %}active{% endif %}">
            Все
        </a>
        <a href="/?kind=keep&store={{ store }}" class="filter-btn {% if kind == 'keep' %}active{% endif %}">
            🎁 Навсегда
        </a>
        <a href="/?kind=weekend&store={{ store }}" class="filter-btn {% if kind == 'weekend' %}active{% endif %}">
            ⏱ Временно
        </a>
        <a href="/?kind=deals&store={{ store }}" class="filter-btn {% if kind == 'deals' %}active{% endif %}">
            💸 Скидки
        </a>
        <a href="/?kind=free&store={{ store }}" class="filter-btn {% if kind == 'free' %}active{% endif %}">
            🔥 F2P
        </a>
      </div>

      <!-- Группа: Магазин -->
      <div class="filter-group">
        <a href="/?store=steam&kind={{ kind }}" class="filter-btn {% if store == 'steam' %}active{% endif %}">
            🎮 Steam
        </a>
        <a href="/?store=epic&kind={{ kind }}" class="filter-btn {% if store == 'epic' %}active{% endif %}">
            🟦 Epic
        </a>
        <a href="/?store=gog&kind={{ kind }}" class="filter-btn {% if store == 'gog' %}active{% endif %}">
            🟪 GOG
        </a>
        <a href="/?store=prime&kind={{ kind }}" class="filter-btn {% if store == 'prime' %}active{% endif %}">
            🟨 Prime
        </a>
        <a href="/?store=all&kind={{ kind }}" class="filter-btn {% if store == 'all' %}active{% endif %}">
            📦 Все
        </a>
      </div>
    </div>
  </div>

</div>

<script>
(() => {
  const btn = document.getElementById('collapseBtn');
  const wrap = document.getElementById('filtersWrap');
  const KEY = 'freerg_filters_open';

  function setOpen(isOpen) {
    btn.setAttribute('aria-expanded', String(isOpen));
    btn.textContent = isOpen ? 'Фильтры ▴' : 'Фильтры ▾';

    if (isOpen) {
      // раскрываем по реальной высоте контента
      wrap.style.maxHeight = wrap.scrollHeight + 'px';
    } else {
      wrap.style.maxHeight = '0px';
    }
    try { localStorage.setItem(KEY, isOpen ? '1' : '0'); } catch(e) {}
  }

  // стартовое состояние: свернуто, но если в localStorage было открыто — откроем
  let initialOpen = false;
  try { initialOpen = localStorage.getItem(KEY) === '1'; } catch(e) {}

  setOpen(initialOpen);

  // если окно ресайзится и панель открыта — пересчитать maxHeight
  window.addEventListener('resize', () => {
    if (btn.getAttribute('aria-expanded') === 'true') {
      wrap.style.maxHeight = wrap.scrollHeight + 'px';
    }
  });

  btn.addEventListener('click', () => {
    const isOpen = btn.getAttribute('aria-expanded') === 'true';
    setOpen(!isOpen);
  });
})();
</script>
        </div>
  </div>
    
    <!-- 🚀 КНОПКА НАВЕРХ -->
    <button class="scroll-to-top" id="scrollToTop" onclick="scrollToTop()">
        ↑
    </button>
    
    <div class="container">
        {% if kind in ["all", "keep"] and keep|length > 0 %}
        <div class="section">
            <section data-tour="free">
                <div class="section-header">
                <span class="section-icon">🎁</span>
                <h2 class="section-title">
                <span class="t-desktop">Бесплатно навсегда • Free to keep</span>
                <span class="t-mobile">Забрать навсегда • Free</span>
                </h2>
                <span class="section-count">{{ keep|length }}</span>
            </div>
                </section>
            
            <div class="games-grid">
                {% for game in keep %}
                <div class="game-card">
                    <div class="game-image-container">
                        <div class="store-badge store-{{ game.store }}">
                            {% if game.store == 'steam' %}STEAM
                            {% elif game.store == 'epic' %}EPIC
                            {% elif game.store == 'gog' %}GOG
                            {% elif game.store == 'prime' %}PRIME
                            {% else %}{{ game.store|upper }}
                            {% endif %}
                        </div>
                        
                        {% if game.image %}
                        <img src="{{ game.image }}" 
                             alt="{{ game.title }}"
                             class="game-image"
                             loading="lazy"
                             onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';">
                        {% endif %}
                        
                        <div class="image-placeholder" style="{% if game.image %}display:none{% endif %}">
                            <div class="image-placeholder-icon">🎮</div>
                            <div style="font-size: 0.85rem;">{{ game.title[:30] }}...</div>
                        </div>
                    </div>
                    
                    <div class="game-content">
                        <h3 class="game-title">{{ game.title }}</h3>
                        
                        <div class="game-meta">
                            <span class="meta-tag tag-free">FREE GIFT 🎁</span>
                            {% if game.is_new %}
                            <span class="meta-tag tag-new">NEW</span>
                            {% endif %}
                        </div>
                        
                        {% if game.ends_at_fmt and not game.expired %}
                        <div class="game-timer">
                            ⏳ До: <span class="timer-time">{{ game.ends_at_fmt }}</span>
                        </div>
                        {% endif %}
                        
                        <a href="{{ game.go_url }}" target="_blank" class="btn">
                            Забрать →
                        </a>
                    </div>
                </div>
                {% endfor %}
            </div>
        </div>
        {% endif %}
        
        {% if kind in ["all", "weekend"] and weekend|length > 0 %}
        <div class="section">
            <div class="section-header">
                <span class="section-icon">⏱</span>
                <h2 class="section-title">Free Weekend</h2>
                <span class="section-count">{{ weekend|length }}</span>
            </div>
            
            <div class="games-grid">
                {% for game in weekend %}
                <div class="game-card">
                    <div class="game-image-container">
                        <div class="store-badge store-{{ game.store }}">
                            {% if game.store == 'steam' %}STEAM
                            {% elif game.store == 'epic' %}EPIC
                            {% elif game.store == 'gog' %}GOG
                            {% elif game.store == 'prime' %}PRIME 
                            {% else %}{{ game.store|upper }}
                            {% endif %}
                        </div>
                        
                        {% if game.image %}
                        <img src="{{ game.image }}" alt="{{ game.title }}" class="game-image" loading="lazy">
                        {% else %}
                        <div class="image-placeholder">
                            <div class="image-placeholder-icon">🎮</div>
                        </div>
                        {% endif %}
                    </div>
                    
                    <div class="game-content">
                        <h3 class="game-title">{{ game.title }}</h3>
                        
                        <div class="game-meta">
                            <span class="meta-tag">WEEKEND</span>
                            {% if game.is_new %}<span class="meta-tag tag-new">NEW</span>{% endif %}
                        </div>
                        
                        {% if game.ends_at_fmt and not game.expired %}
                        <div class="game-timer">
                            ⏳ До: <span class="timer-time">{{ game.ends_at_fmt }}</span>
                        </div>
                        {% endif %}
                        
                        <a href="{{ game.go_url }}" target="_blank" class="btn">
                            Играть →
                        </a>
                <div class="vote-wrap" data-deal-id="{{ game.id }}">
  <button class="vote-btn vote-up" type="button" aria-label="Нравится" data-vote="1">
    <span class="vote-ico">👍</span>
    <span class="vote-count" data-count="up">{{ game.up or 0 }}</span>
  </button>

  <button class="vote-btn vote-down" type="button" aria-label="Не нравится" data-vote="-1">
    <span class="vote-ico">👎</span>
    <span class="vote-count" data-count="down">{{ game.down or 0 }}</span>
  </button>
</div>
                    </div>
                </div>
                {% endfor %}
            </div>
        </div>
        {% endif %}
                
    {% if kind in ["all"] and lfg|length > 0 %}
        <div class="section">
  <div class="section-header">
    <span class="section-icon">🎮</span>
                <h2 class="section-title">
                <span class="t-desktop">Поиск напарников • Looking for teammate</span>
                <span class="t-mobile">Поиск напарника • LFP</span>
                </h2>
    <span class="section-count">{{ lfg|length }}</span>
  </div>
    <div data-tour="lfg">
  <div style="display:flex; gap:10px; flex-wrap:wrap; margin-bottom:12px;">
    <button class="btn" onclick="openLfgModal()">Создать заявку →</button>
                
    {% if tg_group_url %}
      <a data-tour="tg" class="btn" href="{{ tg_group_url }}" target="_blank">Перейти в Telegram группу</a>
    {% endif %}
  </div>
                </div>
            
    

  {% if lfg|length == 0 %}
    <div class="muted" style="opacity:.85;">Пока нет заявок. Создай первую 🙂</div>
  {% else %}
    <div class="games-grid">
      {% for p in lfg %}
      <div class="game-card">
        <div class="game-content">
          <h3 class="game-title">{{ p.game }}</h3>
          <div class="game-meta" style="flex-wrap:wrap;">
            {% if p.region %}<span class="meta-tag">🌍 {{ p.region }}</span>{% endif %}
            {% if p.platform %}<span class="meta-tag">🕹 {{ p.platform }}</span>{% endif %}
          </div>
          {% if p.note %}
            <div style="opacity:.9; margin:8px 0;">{{ p.note }}</div>
          {% endif %}
          <a class="btn" href="{{ p.tg_url }}" target="_blank">💬 Написать в TG</a>
        </div>
      </div>
      {% endfor %}
    </div>
  {% endif %}
</div>
    {% endif %}
                

        {% if kind in ["all", "deals"] and hot|length > 0 %}
<div class="section">
                <section data-tour="hot">
  <div class="section-header">
    <span class="section-icon">💸</span>
                <h2 class="section-title">
                <span class="t-desktop">Горячие скидки • Hot Sale</span>
                <span class="t-mobile">Горячие скидки • Hot Sale</span>
                </h2>                
    <span class="section-count">{{ hot|length }}</span>
  </div>
                </section>

  <div class="games-grid">
    {% for game in hot %}
    <div class="game-card">
      <div class="game-image-container">
        <div class="store-badge store-{{ game.store }}">
          {% if game.store == 'steam' %}STEAM
          {% elif game.store == 'epic' %}EPIC
          {% elif game.store == 'gog' %}GOG
          {% elif game.store == 'prime' %}PRIME
          {% else %}{{ game.store|upper }}
          {% endif %}
        </div>

        {% if game.image %}
          <img src="{{ game.image }}" alt="{{ game.title }}" class="game-image" loading="lazy">
        {% else %}
          <div class="image-placeholder">
            <div class="image-placeholder-icon">🎮</div>
          </div>
        {% endif %}
      </div>

      <div class="game-content">
        <h3 class="game-title">{{ game.title }}</h3>

        <div class="game-meta" style="align-items:center;justify-content:space-between;">
          <div style="display:flex;gap:6px;flex-wrap:wrap;align-items:center;">
            {% if game.discount_pct %}
              <span class="meta-tag tag-discount">-{{ game.discount_pct }}%</span>
            {% endif %}
            {% if game.is_new %}
              <span class="meta-tag tag-new">NEW</span>
            {% endif %}
          </div>

          {% if game.currency_sym and game.price_new_fmt %}
          <div style="font-size:0.85rem;color:var(--text-secondary);font-weight:700;white-space:nowrap;">
            {% if game.price_old_fmt %}
              <span style="opacity:.75;text-decoration:line-through;">
                {{ game.currency_sym }}{{ game.price_old_fmt }}
              </span>
              <span style="margin:0 6px;opacity:.6;">→</span>
            {% endif %}
            <span style="color:var(--text-primary);">
              {{ game.currency_sym }}{{ game.price_new_fmt }}
            </span>
          </div>
          {% endif %}
        </div>

        {% if game.ends_at_fmt and not game.expired %}
        <div class="game-timer">
          ⏳ До: <span class="timer-time">{{ game.ends_at_fmt }}</span>
        </div>
        {% endif %}

        <a href="{{ game.go_url }}" target="_blank" class="btn">Купить →</a>
      </div>
                <div class="vote-wrap" data-deal-id="{{ game.id }}">
  <button class="vote-btn vote-up" type="button" aria-label="Нравится" data-vote="1">
    <svg class="vote-svg" viewBox="0 0 24 24" aria-hidden="true">
      <path d="M2 21h4V9H2v12zm20-11.5c0-.83-.67-1.5-1.5-1.5H14.3l.95-4.57.03-.32c0-.41-.17-.79-.44-1.06L13.9 1 7.6 7.3C7.22 7.68 7 8.2 7 8.75V19c0 1.1.9 2 2 2h7.5c.83 0 1.54-.5 1.84-1.22l2.58-6.02c.05-.13.08-.27.08-.41V9.5z"/>
    </svg>
    <span class="vote-count" data-count="up">{{ game.up or 0 }}</span>
  </button>

  <button class="vote-btn vote-down" type="button" aria-label="Не нравится" data-vote="-1">
    <svg class="vote-svg" viewBox="0 0 24 24" aria-hidden="true">
      <path d="M22 3h-4v12h4V3zM2 14.5c0 .83.67 1.5 1.5 1.5H9.7l-.95 4.57-.03.32c0 .41.17.79.44 1.06L10.1 23l6.3-6.3c.38-.38.6-.9.6-1.45V5c0-1.1-.9-2-2-2H7.5c-.83 0-1.54.5-1.84 1.22L3.08 10.24c-.05.13-.08.27-.08.41v3.85z"/>
    </svg>
    <span class="vote-count" data-count="down">{{ game.down or 0 }}</span>
  </button>
</div>
    </div>
    {% endfor %}
  </div>
</div>
{% endif %}

{% if kind in ["all", "deals"] and manual and manual|length > 0 %}
<div class="section">
  <section data-tour="exclusive">
    <div class="section-header">
      <span class="section-icon">✨</span>
      <h2 class="section-title">
        <span class="t-desktop">Эксклюзивы • Manual Picks</span>
        <span class="t-mobile">Эксклюзивы</span>
      </h2>
      <span class="section-count">{{ manual|length }}</span>
    </div>
  </section>

  <div class="games-grid">
    {% for it in manual %}
    <div class="game-card exclusive">
      <div class="game-image-container">
        <div class="store-badge store-{{ it.store }}">
          {{ it.badge|safe }}
        </div>
        
        {% if it.image %}
        <img src="{{ it.image }}" alt="{{ it.title }}" class="game-image" loading="lazy">
        <div class="exclusive-pill">EXCLUSIVE</div>
        {% else %}
        <div class="image-placeholder">
          <div class="image-placeholder-icon">✨</div>
        </div>
        {% endif %}
      </div>

      <div class="game-content">
        <h3 class="game-title">{{ it.title }}</h3>

        <div class="game-meta" style="align-items:center;justify-content:space-between;">
          <div style="display:flex;gap:6px;flex-wrap:wrap;">
            <span class="meta-tag" style="background:rgba(255,215,0,0.2);color:#ffd700;border:1px solid rgba(255,215,0,0.3)">EXCLUSIVE</span>
          </div>

          {% if it.price_old is not none or it.price_new is not none %}
          <div style="font-size:0.85rem;color:var(--text-secondary);font-weight:700;white-space:nowrap;">
            {% if it.price_old is not none and it.price_new is not none and it.price_new < it.price_old %}
              <span style="opacity:.75;text-decoration:line-through;">
                {{ "%.0f"|format(it.price_old) }} {{ it.currency }}
              </span>
              <span style="margin:0 6px;opacity:.6;">→</span>
              <span style="color:var(--text-primary);">
                {{ "%.0f"|format(it.price_new) }} {{ it.currency }}
              </span>
            {% elif it.price_new is not none %}
              <span style="color:var(--text-primary);">
                {{ "%.0f"|format(it.price_new) }} {{ it.currency }}
              </span>
            {% endif %}
          </div>
          {% endif %}
        </div>

        {% if it.ends_at_fmt %}
        <div class="game-timer">
          ⏳ До: <span class="timer-time">{{ it.ends_at_fmt }}</span>
        </div>
        {% endif %}

        <a href="{{ it.go }}" target="_blank" class="btn">Забрать →</a>
      </div>
    </div>
    {% endfor %}
  </div>
</div>
{% endif %}
        
        {% if kind in ["all", "free"] and free_games is defined and free_games|length > 0 %}
        <div class="section">
                <section data-tour="f2p">
            <div class="section-header">
                <span class="section-icon">🔥</span>
                <h2 class="section-title">
                <span class="t-desktop">Бесплатные игры • Free to Play</span>
                <span class="t-mobile">Бесплатные игры • F2P</span>
                </h2>                
                <span class="section-count">{{ free_games|length }}</span>
            </div>
                </section>
                
<div id="lfgModal" style="display:none; position:fixed; inset:0; background:rgba(0,0,0,.55); padding:16px; z-index:9999;">
  <div style="max-width:520px; margin:40px auto; background:#111; border:1px solid rgba(255,255,255,.12); border-radius:16px; padding:16px;">
    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
      <div style="font-size:18px; font-weight:700;">Создать заявку</div>
      <button class="btn" onclick="closeLfgModal()">✕</button>
    </div>

    <div style="display:grid; gap:10px;">
      <input id="lfg_game" placeholder="Игра (например: CS2)" style="padding:10px; border-radius:12px; border:1px solid rgba(255,255,255,.12); background:#0b0b0b; color:#fff;">
      <input id="lfg_region" placeholder="Регион (EU/RU/US/ASIA)" style="padding:10px; border-radius:12px; border:1px solid rgba(255,255,255,.12); background:#0b0b0b; color:#fff;">
      <input id="lfg_platform" placeholder="Платформа (PC/PS/Xbox/Mobile)" style="padding:10px; border-radius:12px; border:1px solid rgba(255,255,255,.12); background:#0b0b0b; color:#fff;">
      <input id="lfg_tg" placeholder="Твой Telegram @username (необязательно)" style="padding:10px; border-radius:12px; border:1px solid rgba(255,255,255,.12); background:#0b0b0b; color:#fff;">
      <textarea id="lfg_note" placeholder="Комментарий (необязательно)" rows="3" style="padding:10px; border-radius:12px; border:1px solid rgba(255,255,255,.12); background:#0b0b0b; color:#fff;"></textarea>
      <button class="btn" onclick="submitLfg()">Опубликовать →</button>
      <div id="lfg_msg" style="opacity:.9;"></div>
    </div>
  </div>
</div>

<script>
function openLfgModal(){ document.getElementById('lfgModal').style.display='block'; }
function closeLfgModal(){ document.getElementById('lfgModal').style.display='none'; }

async function submitLfg(){
  const payload = {
    game: document.getElementById('lfg_game').value,
    region: document.getElementById('lfg_region').value,
    platform: document.getElementById('lfg_platform').value,
    tg_user: document.getElementById('lfg_tg').value,
    note: document.getElementById('lfg_note').value
  };
  const msg = document.getElementById('lfg_msg');
  msg.textContent = 'Публикую...';

  try{
    const r = await fetch('/api/lfg/create', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify(payload)
    });
    const j = await r.json();
    if(j.ok){
      msg.textContent = '✅ Заявка создана! Обновляю страницу...';
      setTimeout(()=>location.reload(), 700);
    } else {
      msg.textContent = '⚠️ Не получилось: ' + (j.error || 'unknown');
    }
  }catch(e){
    msg.textContent = '⚠️ Ошибка сети/сервера';
  }
}
</script>
                


            <div class="games-grid">
                {% for game in free_games %}
                <div class="game-card">
                    <div class="game-image-container">
                        <div class="store-badge store-{{ game.store }}">
                            {{ game.store|upper }}
                        </div>
                        
                        {% if game.image_url %}
                        <img src="{{ game.image_url }}" alt="{{ game.title }}" class="game-image" loading="lazy">
                        {% else %}
                        <div class="image-placeholder">
                            <div class="image-placeholder-icon">🎮</div>
                        </div>
                        {% endif %}
                    </div>
                    
                    <div class="game-content">
                        <h3 class="game-title">{{ game.title }}</h3>
                        
                        <div class="game-meta">
                            <span class="meta-tag tag-free">F2P</span>
                        </div>
                        
                        {% if game.note %}
                        <div class="game-timer">{{ game.note }}</div>
                        {% endif %}
                        
                        <a href="{{ game.go_url }}" target="_blank" class="btn">
                            Играть →
                        </a>
                    </div>
                </div>
                {% endfor %}
            </div>
        </div>
        {% endif %}
        
        {% if (keep|length == 0 and weekend|length == 0 and hot|length == 0 and (not free_games or free_games|length == 0)) %}
        <div class="empty-state">
            <div class="empty-icon">🎮</div>
            <h2 class="empty-title">Игры не найдены</h2>
            <p class="empty-description">Попробуйте изменить фильтры или проверьте позже</p>
        </div>
        {% endif %}
    </div>

    <script>
        // 🚀 Кнопка "Наверх"
        const scrollBtn = document.getElementById('scrollToTop');
        
        // Показываем кнопку при прокрутке вниз
        window.addEventListener('scroll', function() {
            if (window.pageYOffset > 300) {
                scrollBtn.classList.add('show');
            } else {
                scrollBtn.classList.remove('show');
            }
        });
        
        // Плавная прокрутка наверх
        function scrollToTop() {
            window.scrollTo({
                top: 0,
                behavior: 'smooth'
            });
        }
    </script>
    <script>
(function(){
  const header = document.querySelector(".header");
  const btn = document.getElementById("collapseBtn");
  if(!header) return;

  let lastY = window.scrollY;
  let ticking = false;

  function applyPadding(){
    // ✅ padding всегда равен высоте header (даже когда hidden)
    // иначе появляются "прыжки" и "пустота"
    document.body.style.paddingTop = header.offsetHeight + "px";
  }

  // старт / resize
  applyPadding();
  window.addEventListener("resize", () => requestAnimationFrame(applyPadding));

  // collapse toggle
  if(btn){
    btn.addEventListener("click", () => {
      header.classList.toggle("collapsed");
      btn.textContent = header.classList.contains("collapsed") ? "Фильтры ▼" : "Свернуть ▲";
      requestAnimationFrame(applyPadding);
    });
  }

  function onScroll(){
    const y = window.scrollY;

    // верх страницы — всегда показываем
    if (y < 30){
      header.classList.remove("hidden");
      lastY = y;
      return;
    }

    // вниз — прячем, вверх — показываем
    if (y > lastY + 12){
      header.classList.add("hidden");
    } else if (y < lastY - 12){
      header.classList.remove("hidden");
    }

    lastY = y;
  }

  window.addEventListener("scroll", () => {
    if(!ticking){
      requestAnimationFrame(() => {
        onScroll();
        ticking = false;
      });
      ticking = true;
    }
  }, { passive:true });

  // если шрифт/контент в header догрузился и высота изменилась
  setTimeout(applyPadding, 200);
})();
</script>
<script>
async function vote(dealId, v, root){
  try{
    const r = await fetch("/api/vote", {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify({deal_id: dealId, vote: v})
    });
    const j = await r.json();

    if (j.up !== undefined) root.querySelector(".v-up").textContent = j.up;
    if (j.down !== undefined) root.querySelector(".v-down").textContent = j.down;

    if (!j.ok && j.error === "already_voted"){
      root.classList.add("voted");
    }
  } catch(e){}
}

document.addEventListener("click", (e)=>{
  const btn = e.target.closest(".vote-btn");
  if(!btn) return;
  const root = btn.closest(".vote-row");
  const dealId = root.getAttribute("data-deal");
  const v = parseInt(btn.getAttribute("data-vote"), 10);
  vote(dealId, v, root);
});
</script>

<script>
document.addEventListener("click", async (e) => {
  const btn = e.target.closest(".vote-btn");
  if (!btn) return;

  const wrap = btn.closest(".vote-wrap");
  const dealId = wrap?.dataset?.dealId;
  const vote = parseInt(btn.dataset.vote, 10);

  if (!dealId || !vote) return;

  try {
    const res = await fetch("/api/vote", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ deal_id: dealId, vote })
    });

    const data = await res.json();
    if (!data || data.ok !== true) return;

    // обновляем числа
    wrap.querySelector('[data-count="up"]').textContent = data.up ?? 0;
    wrap.querySelector('[data-count="down"]').textContent = data.down ?? 0;

    // визуально отмечаем выбор
    wrap.querySelectorAll(".vote-btn").forEach(b => b.classList.remove("is-active"));
    btn.classList.add("is-active");
  } catch (err) {
    // молча, чтобы не бесить юзера
  }
});
</script>

<script>
if ('serviceWorker' in navigator) {
  navigator.serviceWorker.register('/service-worker.js');
}
</script>

<script>
if ("serviceWorker" in navigator) {
  window.addEventListener("load", () => {
    navigator.serviceWorker.register("/static/sw.js").catch(()=>{});
  });
}
</script>
                
                <script>
(function(){
  const steps = [
    { sel: '[data-tour="stats"]',   title:"Мини-статистика", text:"Здесь видно, сколько кликов и сколько сэкономили — живые цифры.", placement:"bottom" },
    { sel: '[data-tour="filters"]', title:"Фильтры", text:"Выбирай тип (навсегда/временно/скидки) и магазин (Steam/Epic…).", placement:"bottom" },
    { sel: '[data-tour="exclusive"]',     title:"💎 EXCLUSEVE", text:"Самое сладкое — то, что легко пропустить", placement:"bottom" },
    { sel: '[data-tour="free"]',    title:"🎁 Бесплатно навсегда", text:"Самое вкусное — забираешь и остаётся навсегда.", placement:"top" },
    { sel: '[data-tour="f2p"]',     title:"⏱ Free-to-Play", text:"Free To Play / Бесплатные игры: доступ открыт всегда.", placement:"top" },
    { sel: '[data-tour="hot"]',     title:"💸 Скидки", text:"Горячие скидки — иногда цена падает до копеек.", placement:"top" },
    { sel: '[data-tour="lfg"]',     title:"🔍️ Поиск тиммейтов", text:"Создай заявку и собирай пати — а дальше общение в TG.", placement:"bottom" },
    { sel: '[data-tour="tg"]',      title:"Telegram-общение", text:"Тут обсуждения, поиск друзей и быстрый чат по играм.", placement:"bottom" },
  ];

  let i = -1;
  let currentEl = null;

  const overlay = document.createElement("div");
  overlay.className = "tour-overlay";
  document.body.appendChild(overlay);

  const pop = document.createElement("div");
  pop.className = "tour-pop";
  pop.innerHTML = `
    <div class="tour-step" id="tourStep"></div>
    <h3 class="tour-title" id="tourTitle"></h3>
    <p class="tour-text" id="tourText"></p>
    <div class="tour-actions">
      <div class="left">
        <button class="tour-btn" id="tourPrev" type="button">Назад</button>
        <button class="tour-btn primary" id="tourNext" type="button">Далее</button>
      </div>
      <button class="tour-btn" id="tourClose" type="button">Закрыть</button>
    </div>
  `;
  document.body.appendChild(pop);

  const $ = (id) => document.getElementById(id);

  function cleanupHighlight(){
    if(currentEl){
      currentEl.classList.remove("tour-highlight");
      currentEl = null;
    }
  }

  function placePop(el, placement){
    const r = el.getBoundingClientRect();
    const pad = 10;

    let left = Math.min(
      Math.max(pad, r.left + r.width/2 - pop.offsetWidth/2),
      window.innerWidth - pop.offsetWidth - pad
    );

    let top;
    if(placement === "top"){
      top = r.top - pop.offsetHeight - 12;
      if(top < pad) top = r.bottom + 12;
    } else {
      top = r.bottom + 12;
      if(top + pop.offsetHeight > window.innerHeight - pad) top = r.top - pop.offsetHeight - 12;
    }

    if(top < pad) top = pad;
    if(top + pop.offsetHeight > window.innerHeight - pad) top = window.innerHeight - pop.offsetHeight - pad;

    pop.style.left = left + "px";
    pop.style.top = top + "px";
  }

  function showStep(nextIndex){
    cleanupHighlight();
    i = nextIndex;

    const step = steps[i];
    const el = document.querySelector(step.sel);

    if(!el){
      if(i < steps.length - 1) return showStep(i+1);
      return endTour();
    }

    currentEl = el;
    overlay.classList.add("active");
    pop.classList.add("active");

    el.scrollIntoView({ behavior:"smooth", block:"center" });

    setTimeout(() => {
      el.classList.add("tour-highlight");

      $("tourStep").textContent = `Шаг ${i+1} из ${steps.length}`;
      $("tourTitle").textContent = step.title;
      $("tourText").textContent = step.text;

      $("tourPrev").disabled = (i === 0);
      $("tourNext").textContent = (i === steps.length - 1) ? "Готово" : "Далее";

      placePop(el, step.placement || "bottom");
    }, 250);
  }

  function endTour(){
    overlay.classList.remove("active");
    pop.classList.remove("active");
    cleanupHighlight();
    i = -1;
  }

  document.addEventListener("click", (e) => {
    if(e.target && e.target.id === "tourStartBtn") showStep(0);
    if(e.target && e.target.id === "tourClose") endTour();
    if(e.target && e.target.id === "tourNext"){
      if(i >= steps.length - 1) endTour();
      else showStep(i+1);
    }
    if(e.target && e.target.id === "tourPrev"){
      if(i > 0) showStep(i-1);
    }
  });

  overlay.addEventListener("click", endTour);

  window.addEventListener("resize", () => {
    if(i >= 0 && currentEl) placePop(currentEl, steps[i].placement || "bottom");
  });

  window.addEventListener("keydown", (e) => {
    if(e.key === "Escape" && i >= 0) endTour();
  });
})();
</script>

</body>
</html>
""")

DEAL_PAGE = Template("""
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>{{ title }} — FreeRG</title>
  <style>
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;background:#0a0e1a;color:#e2e8f0;margin:0;padding:24px;}
    .wrap{max-width:720px;margin:0 auto;}
    .card{background:#1a1f36;border:1px solid rgba(255,255,255,.1);border-radius:16px;overflow:hidden;}
    .img{height:220px;background:#111827;display:flex;align-items:center;justify-content:center;}
    img{width:100%;height:100%;object-fit:cover;display:block;}
    .pad{padding:16px;}
    .badge{display:inline-block;padding:6px 10px;border-radius:10px;font-weight:700;font-size:12px;background:rgba(255,255,255,.08);margin-bottom:10px;}
    .btn{display:block;text-align:center;margin-top:14px;padding:12px 14px;border-radius:12px;font-weight:800;text-decoration:none;color:#fff;
         background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);}
    .muted{color:#94a3b8;font-size:13px;margin-top:8px}
    a.small{color:#94a3b8}
  </style>
</head>
<body>
<div class="wrap">
  <div class="card">
    <div class="img">
      {% if image %}
        <img src="{{ image }}" alt="{{ title }}">
      {% else %}
        <div style="opacity:.7;font-size:48px">🎮</div>
      {% endif %}
    </div>
    <div class="pad">
      <div class="badge">{{ badge }}</div>
      <h1 style="margin:0 0 6px 0;font-size:22px;line-height:1.2">{{ title }}</h1>
      {% if ends_at_fmt %}
        <div class="muted">⏳ До: {{ ends_at_fmt }}</div>
      {% endif %}
      <a class="btn" href="{{ out_route }}">🔎 Открыть в магазине</a>
      <div class="muted">Можно вернуться в список: <a class="small" href="/">freerg.store</a></div>
    </div>
  </div>
</div>
</body>
</html>
""")

from fastapi.staticfiles import StaticFiles
app.mount("/static", StaticFiles(directory="/opt/freerg/static"), name="static")

@app.get("/d/{deal_id}", response_class=HTMLResponse)
def deal_page(deal_id: str, request: Request):
    conn = db()
    row = conn.execute("""
        SELECT store, kind, title, url, image_url, ends_at
        FROM deals
        WHERE id=? LIMIT 1
    """, (deal_id,)).fetchone()

    if not row:
        conn.close()
        return HTMLResponse("<h3 style='font-family:system-ui'>Deal not found</h3><p><a href='/'>Back</a></p>", status_code=404)

    st, kind, title, url, image_url, ends_at = row
    st = (st or "").strip().lower()
    badge = store_badge(st)
    img_main, _ = images_for_row(st, url, image_url)

    # если пришли напрямую на карточку (не через /go), можно залогировать
    # vid выставляем через Set-Cookie -> нужен Response объект, поэтому тут проще НЕ ставить cookie.
    # (cookie уже появится через /go или /out)
    # Если хочешь ставить cookie и тут — скажи, сделаем через HTMLResponse + set_cookie.
    if request.query_params.get("src") is None:
        try:
            log_click(conn, deal_id, request, src="card")
        except Exception:
            pass

    conn.close()

    return DEAL_PAGE.render(
        title=title,
        badge=badge,
        image=img_main,
        out_url=url,
        ends_at_fmt=(format_expiry(ends_at) if ends_at else ""),
        out_route=f"{SITE_BASE}/out/{deal_id}",
    )


def store_badge(store: str | None) -> str:
    return {"steam": "🎮 Steam", "epic": "🟦 Epic", "gog": "🟪 GOG", "prime": "🟨 Prime"}.get(store or "", store or "Store")


def images_for_row(row_store: str | None, url: str, image_url: str | None):
    """Правильное извлечение изображений"""
    st = (str(row_store) or "").strip().lower()
    
    # 1. Если есть image_url в БД - используем его!
    if image_url and str(image_url).strip():
        return str(image_url), ""
    
    # 2. Только для Steam
    if st != "steam":
        return "", ""
    
    # 3. Извлекаем AppID из URL
    appid = extract_steam_app_id_fast(url)
    
    # 4. Если нашли - генерируем URL
    if appid:
        main = f"https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/{appid}/header.jpg"
        fallback = f"https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/header.jpg"
        return main, fallback
    
    return "", ""

@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
# ИСПРАВЛЕННАЯ ФУНКЦИЯ INDEX()
# Замени с строки 3583 до строки 3879

def index(show_expired: int = 0, store: str = "all", kind: str = "all"):
    conn = db()
    
    # Нормализация параметров
    store = (store or "all").strip().lower()
    if store not in {"all", "steam", "epic", "gog", "prime"}:
        store = "all"
    
    kind = (kind or "all").strip().lower()
    if kind not in {"all", "keep", "weekend", "free", "deals"}:
        kind = "all"
    
    # ===== ФУНКЦИИ ФИЛЬТРАЦИИ =====
    def allow_time(ends_at: str | None) -> bool:
        if is_active_end(ends_at):
            return True
        return bool(show_expired) and is_expired_recent(ends_at, days=7)
    
    def allow_store(row_store: str | None) -> bool:
        if store == "all":
            return True
        return (row_store or "").strip().lower() == store
    
    # ===== ПОЛУЧАЕМ ДАННЫЕ ИЗ БД =====
    
    # Free to Keep
    keep_rows = conn.execute("""
        SELECT id, store, title, url, image_url, ends_at, created_at
        FROM deals
        WHERE kind='free_to_keep'
        ORDER BY created_at DESC
        LIMIT 150
    """).fetchall()
    
    # Free Weekend
    weekend_rows = conn.execute("""
        SELECT id, store, title, url, image_url, ends_at, created_at
        FROM deals
        WHERE kind='free_weekend'
        ORDER BY created_at DESC
        LIMIT 150
    """).fetchall()
    
    # Hot Deals (витрина 20 игр: 6x90%+ и 14x70-89%)
    HOT_TOTAL = 20
    HOT_90 = 6
    HOT_70_89 = 14
    
    hot_rows = []
    
    # 1) Скидки 90%+
    hot_rows += conn.execute("""
        SELECT id, store, title, url, image_url, ends_at, created_at,
               discount_pct, price_old, price_new, currency
        FROM deals
        WHERE kind='hot_deal' AND discount_pct >= 90
        ORDER BY RANDOM()
        LIMIT ?
    """, (HOT_90,)).fetchall()
    
    # 2) Скидки 70-89%
    hot_rows += conn.execute("""
        SELECT id, store, title, url, image_url, ends_at, created_at,
               discount_pct, price_old, price_new, currency
        FROM deals
        WHERE kind='hot_deal' AND discount_pct BETWEEN 70 AND 89
        ORDER BY RANDOM()
        LIMIT ?
    """, (HOT_70_89,)).fetchall()
    
    # 3) Фоллбек если мало
    if len(hot_rows) < HOT_TOTAL:
        need = HOT_TOTAL - len(hot_rows)
        hot_rows += conn.execute("""
            SELECT id, store, title, url, image_url, ends_at, created_at,
                   discount_pct, price_old, price_new, currency
            FROM deals
            WHERE kind='hot_deal' AND discount_pct >= 70
            ORDER BY RANDOM()
            LIMIT ?
        """, (need,)).fetchall()
    
    # Убираем дубли
    uniq = {}
    for r in hot_rows:
        uniq[r[0]] = r
    hot_rows = list(uniq.values())[:HOT_TOTAL]
    
    # Free Games (F2P)
    free_games_rows = conn.execute("""
        SELECT store, title, url, image_url, note
        FROM free_games
        ORDER BY sort ASC, created_at DESC
        LIMIT 24
    """).fetchall()
    
    # LFG заявки
    lfg_rows = conn.execute("""
        SELECT id, created_at, game, region, platform, note, tg, expires_at
        FROM lfg
        WHERE active=1
          AND (expires_at IS NULL OR expires_at > ?)
        ORDER BY created_at DESC
        LIMIT 12
    """, (datetime.utcnow().isoformat(),)).fetchall()
    
    # Manual News (Эксклюзивы)
    manual_rows = conn.execute("""
        SELECT id, created_at, title, url, image_url, store, kind,
                price_old, price_new, currency, ends_at
        FROM manual_news
        WHERE is_published=1
        ORDER BY datetime(created_at) DESC
        LIMIT 12
    """).fetchall()
    
    conn.close()
    
    # ===== ОБРАБАТЫВАЕМ ДАННЫЕ =====
    
    # Keep
    keep = []
    for r in keep_rows:
        did, st, title, url, image_url, ends_at, created_at = r
        
        if not (allow_time(ends_at) and allow_store(st)):
            continue
        
        img_main, img_fb = images_for_row(st, url, image_url)
        
        keep.append({
            "id": did,
            "store": (st or "").strip().lower(),
            "store_badge": store_badge(st),
            "title": title,
            "url": url,
            "image": img_main,
            "image_fallback": img_fb,
            "ends_at": ends_at,
            "is_new": is_new(created_at),
            "ends_at_fmt": format_expiry(ends_at) if ends_at else "",
            "created_at": created_at,
            "expired": not is_active_end(ends_at),
            "time_left": time_left_label(ends_at),
            "go_url": f"{SITE_BASE}/go/{did}?src=site&utm_campaign=freeredeemgames&utm_content=keep",
        })
    
    # Weekend
    weekend = []
    for r in weekend_rows:
        did, st, title, url, image_url, ends_at, created_at = r
        
        if not (allow_time(ends_at) and allow_store(st)):
            continue
        
        img_main, img_fb = images_for_row(st, url, image_url)
        
        weekend.append({
            "id": did,
            "store": (st or "").strip().lower(),
            "store_badge": store_badge(st),
            "title": title,
            "url": url,
            "image": img_main,
            "image_fallback": img_fb,
            "ends_at": ends_at,
            "is_new": is_new(created_at),
            "ends_at_fmt": format_expiry(ends_at) if ends_at else "",
            "created_at": created_at,
            "expired": not is_active_end(ends_at),
            "time_left": time_left_label(ends_at),
            "go_url": f"{SITE_BASE}/go/{did}?src=site&utm_campaign=freeredeemgames&utm_content=weekend",
        })
    
    # Hot Deals
    hot = []
    for r in hot_rows:
        did, st, title, url, image_url, ends_at, created_at, discount_pct, price_old, price_new, currency = r
        
        if not allow_store(st):
            continue
        
        img_main, img_fb = images_for_row(st, url, image_url)
        
        hot.append({
            "id": did,
            "store": (st or "").strip().lower(),
            "store_badge": store_badge(st),
            "title": title,
            "url": url,
            "image": img_main,
            "image_fallback": img_fb,
            "ends_at": ends_at,
            "is_new": is_new(created_at),
            "ends_at_fmt": format_expiry(ends_at) if ends_at else "",
            "created_at": created_at,
            "expired": not is_active_end(ends_at),
            "time_left": time_left_label(ends_at),
            "discount_pct": discount_pct,
            "price_old": price_old,
            "price_new": price_new,
            "currency": currency,
            "price_old_fmt": fmt_price(price_old),
            "price_new_fmt": fmt_price(price_new),
            "currency_sym": currency_symbol(currency),
            "go_url": f"{SITE_BASE}/go/{did}?src=site&utm_campaign=freeredeemgames&utm_content=deals",
        })
    
    # LFG
    lfg = []
    for r in lfg_rows:
        lfg.append({
            "id": r[0],
            "created_at": r[1],
            "game": r[2],
            "region": r[3],
            "platform": r[4],
            "note": r[5],
            "tg": r[6],
            "tg_url": f"{SITE_BASE}/tg/lfg/{r[0]}",
        })
    
    # Manual (Эксклюзивы)
    manual_items = []
    for (mid, created_at, title, url, image_url, store_val, kind_val, po, pn, cur, ends_at) in manual_rows:
        store_norm = (store_val or "").strip().lower()
        badge = store_badge(store_norm)
        img_main, _ = images_for_row(store_norm, url, image_url)
        
        manual_items.append({
            "id": f"m_{mid}",
            "title": title,
            "url": url,
            "image": img_main,
            "badge": badge,
            "store": store_norm,
            "kind": kind_val or "news",
            "price_old": po,
            "price_new": pn,
            "currency": cur or "USD",
            "ends_at_fmt": (format_expiry(ends_at) if ends_at else ""),
            "go": f"/go_manual/{mid}?src=manual",
        })
    
    # Free Games
    free_games = []
    for st, title, url, image_url, note in free_games_rows:
        st_norm = (st or "").strip().lower()
        img = image_url or ""
        if not img and st_norm == "steam":
            img = steam_header_cdn_from_url(url) or ""
        
        free_games.append({
            "store": st_norm,
            "store_badge": store_badge(st_norm),
            "title": title,
            "url": url,
            "image_url": img,
            "note": note,
            "go_url": url,  # F2P идёт напрямую в магазин
        })
    
    # Сортируем по дате окончания
    keep.sort(key=lambda d: sort_key_by_ends(d["ends_at"]))
    weekend.sort(key=lambda d: sort_key_by_ends(d["ends_at"]))
    hot.sort(key=lambda d: sort_key_by_ends(d["ends_at"]))
    
    # Статистика
    total_games = len(keep) + len(weekend) + len(hot)
    new_today = sum(1 for g in (keep + weekend + hot) if g.get("is_new"))
    expiring_soon = sum(
        1 for g in (keep + weekend)
        if g.get("time_left") and ("час" in g.get("time_left", "") or "мин" in g.get("time_left", ""))
    )
    last_update = datetime.now().strftime("%d.%m.%Y %H:%M")
    
    stats = calc_savings(db())
    
    # Рендерим страницу
    return PAGE.render(
        keep=keep,
        weekend=weekend,
        hot=hot,
        free_games=free_games,
        steam_min=STEAM_MIN,
        epic_min=EPIC_MIN,
        show_expired=int(show_expired),
        store=store,
        kind=kind,
        total_games=total_games,
        new_today=new_today,
        expiring_soon=expiring_soon,
        last_update=last_update,
        generate_placeholder=lambda t, s: "",
        lfg=lfg,
        tg_group_url=TG_GROUP_URL,
        savings=stats,
        manual=manual_items,
    )

# Вспомогательная функция для сборки словаря (чтобы не дублировать код)
def build_item_dict(r, img, fb, content_type):
    return {
        "id": r['id'],
        "store": (r['store'] or "").lower(),
        "title": r['title'],
        "url": r['url'],
        "image": img,
        "image_fallback": fb,
        "ends_at": r['ends_at'],
        "is_new": is_new(r['created_at']),
        "ends_at_fmt": format_expiry(r['ends_at']) if r['ends_at'] else "",
        "expired": not is_active_end(r['ends_at']),
        "time_left": time_left_label(r['ends_at']),
        "go_url": f"{SITE_BASE}/go/{r['id']}?src=site&content={content_type}"
    }

from fastapi import Form, Request

@app.get("/lfg", response_class=HTMLResponse)
def lfg(request: Request, game: str = "general"):
    game = normalize_choice(game, ALLOWED_GAMES, "general")

    conn = db()
    rows = conn.execute("""
        SELECT id, created_at, game, title, note, region, platform, when_text, tg_topic_url
        FROM lfg_posts
        WHERE active=1 AND game=?
        ORDER BY created_at DESC
        LIMIT 80
    """, (game,)).fetchall()
    conn.close()

    posts = []
    for r in rows:
        pid, created_at, g, title, note, region, platform, when_text, tg_topic_url = r
        posts.append({
            "id": pid,
            "created_at": created_at,
            "game": g,
            "title": title,
            "note": note,
            "region": region or "",
            "platform": platform or "",
            "when_text": when_text or "",
            "tg_topic_url": tg_topic_url,
            # ведём в телегу через трекинг
            "go_url": f"{SITE_BASE}/go/lfg/{pid}?to=tg&src=site&utm_campaign=lfg&utm_content=card",
        })

    # МИНИ-HTML прямо тут (чтобы не трогать шаблон пока)
    # Потом красиво интегрируем в твой PAGE.render
    html = f"""
    <html><head><meta charset="utf-8"><title>LFG</title></head>
    <body style="font-family:system-ui;max-width:900px;margin:20px auto;padding:0 12px;">
      <h1>🎮 LFG — {game.upper()}</h1>

      <p>
        <a href="/lfg/new?game={game}">➕ Создать объявление</a> |
        <a href="/">← На главную</a>
      </p>

      <p>Фильтр:
        <a href="/lfg?game=general">general</a> ·
        <a href="/lfg?game=hot">hot</a> ·
        <a href="/lfg?game=bf6">battlefield6</a> ·
        <a href="/lfg?game=cs2">cs2</a> ·
        <a href="/lfg?game=dota2">dota2</a> ·
        <a href="/lfg?game=fortnite">fortnite</a> ·
        <a href="/lfg?game=gta">gta</a> ·
        <a href="/lfg?game=other">other</a>
      </p>

      <hr>
    """

    if not posts:
        html += "<p>Пока пусто. Создай первое объявление 👇</p>"
    else:
        for p in posts:
            html += f"""
            <div style="border:1px solid #ddd;border-radius:12px;padding:12px;margin:12px 0;">
              <div style="display:flex;gap:8px;justify-content:space-between;align-items:center;">
                <b>{p['title']}</b>
                <span style="opacity:.7;font-size:12px">{p['created_at']}</span>
              </div>
              <div style="margin-top:6px;white-space:pre-wrap">{p['note']}</div>
              <div style="margin-top:8px;opacity:.85;font-size:13px">
                {("🕒 " + p['when_text']) if p['when_text'] else ""}
                {(" · 🌍 " + p['region']) if p['region'] else ""}
                {(" · 🎮 " + p['platform']) if p['platform'] else ""}
              </div>
              <div style="margin-top:10px;">
                <a href="{p['go_url']}" style="display:inline-block;padding:8px 12px;border-radius:10px;border:1px solid #333;text-decoration:none;">
                  💬 В чат →
                </a>
              </div>
            </div>
            """

    html += "</body></html>"
    return HTMLResponse(html)

@app.get("/go_manual/{mid}")
def go_manual(mid: int, request: Request):
    conn = db()

    src = request.query_params.get("src") or "manual"
    utm_campaign = request.query_params.get("utm_campaign")
    utm_content = request.query_params.get("utm_content")

    # deal_id сделаем строкой "m_<id>" — это нормально для clicks.deal_id TEXT
    log_click(conn, f"m_{mid}", request, src=src, utm_campaign=utm_campaign, utm_content=utm_content)

    row = conn.execute("SELECT url FROM manual_news WHERE id=? LIMIT 1", (mid,)).fetchone()
    conn.close()

    if not row:
        return RedirectResponse(url="/", status_code=302)

    return RedirectResponse(url=row[0], status_code=302)

@app.get("/lfg/new", response_class=HTMLResponse)
def lfg_new(game: str = "general"):
    game = normalize_choice(game, ALLOWED_GAMES, "general")
    html = f"""
    <html><head><meta charset="utf-8"><title>New LFG</title></head>
    <body style="font-family:system-ui;max-width:700px;margin:20px auto;padding:0 12px;">
      <h1>➕ Новое LFG — {game.upper()}</h1>
      <form method="post" action="/lfg/create">
        <input type="hidden" name="game" value="{game}">
        <p>Заголовок (коротко):<br>
          <input name="title" maxlength="80" style="width:100%;padding:10px;border-radius:10px;border:1px solid #ccc">
        </p>
        <p>Описание (что ищешь):<br>
          <textarea name="note" maxlength="800" rows="6" style="width:100%;padding:10px;border-radius:10px;border:1px solid #ccc"></textarea>
        </p>
        <p>Когда (например: “сейчас”, “в 22:00”, “завтра”):<br>
          <input name="when_text" maxlength="60" style="width:100%;padding:10px;border-radius:10px;border:1px solid #ccc">
        </p>
        <p>Платформа:
          <select name="platform">
            <option value="pc">PC</option>
            <option value="ps">PlayStation</option>
            <option value="xbox">Xbox</option>
            <option value="mobile">Mobile</option>
            <option value="other">Other</option>
          </select>
        </p>
        <p>Регион:
          <select name="region">
            <option value="eu">EU</option>
            <option value="us">US</option>
            <option value="asia">Asia</option>
            <option value="other">Other</option>
          </select>
        </p>
        <button type="submit" style="padding:10px 14px;border-radius:10px;border:1px solid #333;background:#fff;cursor:pointer">
          Создать ✅
        </button>
        <a href="/lfg?game={game}" style="margin-left:10px;">Отмена</a>
      </form>
    </body></html>
    """
    return HTMLResponse(html)

from pydantic import BaseModel
import secrets

class LfgCreate(BaseModel):
    game: str
    region: str | None = ""
    platform: str | None = ""
    note: str | None = ""
    tg_user: str | None = ""     # @username или username

@app.post("/api/lfg/create")
def lfg_create_api(payload: LfgCreate, request: Request):
    conn = db()

    game = (payload.game or "").strip()
    if not game:
        conn.close()
        return {"ok": False, "error": "Укажите игру"}

    region = (payload.region or "").strip()
    platform = (payload.platform or "").strip()
    note = (payload.note or "").strip()
    tg_user = (payload.tg_user or "").strip()

    # 🔥 ВАЛИДАЦИЯ ТЕКСТА
    valid, error = validate_lfg_text(note)
    if not valid:
        conn.close()
        return {"ok": False, "error": error}
    
    valid, error = validate_lfg_text(tg_user)
    if not valid:
        conn.close()
        return {"ok": False, "error": error}
    
    valid, error = validate_lfg_text(game)
    if not valid:
        conn.close()
        return {"ok": False, "error": error}

    # 🔥 RATE LIMIT
    ip = request.client.host if request.client else "unknown"
    if not check_rate_limit(ip, hours=1, limit=3):
        conn.close()
        return {"ok": False, "error": "⏱ Слишком много заявок! Подождите час."}

    # Нормализуем @username
    if tg_user.startswith("@"):
        tg_user = tg_user[1:]

    now = datetime.utcnow()
    expires = now + timedelta(hours=24)
    lfg_id = secrets.token_hex(8)

    conn.execute("""
        INSERT INTO lfg (id, created_at, game, region, platform, note, tg_user, expires_at, active, ip)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
    """, (
        lfg_id,
        now.isoformat(),
        game[:80],
        region[:40],
        platform[:40],
        note[:280],
        tg_user[:64],
        expires.isoformat(),
        ip,
    ))
    conn.commit()

    # Логируем создание
    try:
        log_click(conn, f"lfg:{lfg_id}", request, src="site", utm_campaign="lfg", utm_content="created")
    except Exception:
        pass

    conn.close()
    return {"ok": True, "id": lfg_id}


@app.post("/lfg/create")
def lfg_create(payload: LfgCreate, request: Request):
    conn = db()
    ensure_tables(conn)

    lfg_id = uuid.uuid4().hex[:12]
    ip = request.client.host if request.client else None
    ua = request.headers.get("user-agent")

    conn.execute("""
        INSERT INTO lfg (id, created_at, game, platform, region, note, tg, ip, user_agent)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        lfg_id,
        datetime.utcnow().isoformat(),
        (payload.game or "").strip(),
        (payload.platform or "PC").strip(),
        (payload.region or "").strip(),
        (payload.note or "").strip(),
        (payload.tg or "").strip(),
        ip,
        ua
    ))
    conn.commit()
    conn.close()

    return {"ok": True, "id": lfg_id}

TG_GROUP_URL = os.getenv("TG_GROUP_URL", "").strip()  # добавим в .env

@app.get("/tg/lfg/{lfg_id}")
def tg_lfg_redirect(lfg_id: str, request: Request):
    conn = db()
    row = conn.execute("""
        SELECT id, tg_user, active, expires_at
        FROM lfg WHERE id=?
    """, (lfg_id,)).fetchone()

    # fallback: если заявки нет
    target = TG_GROUP_URL or "https://t.me/"

    if row:
        # если есть username — ведём в ЛС, иначе в группу
        tg_user = (row["tg_user"] or "").strip()
        if tg_user:
            target = f"https://t.me/{tg_user}"
        else:
            target = TG_GROUP_URL or target

        # лог клика
        try:
            log_click(conn, f"lfg:{lfg_id}", request, src="site", utm_campaign="freeredeemgames", utm_content="lfg_tg_click")
        except Exception:
            pass

    conn.close()
    return RedirectResponse(target, status_code=302)


@app.get("/lfg/list")
def lfg_list(limit: int = 50):
    conn = db()
    rows = conn.execute("""
        SELECT id, created_at, game, platform, region, note, tg
        FROM lfg
        ORDER BY created_at DESC
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()

    return {
        "items": [
            {
                "id": r[0],
                "created_at": r[1],
                "game": r[2],
                "platform": r[3],
                "region": r[4],
                "note": r[5],
                "tg": r[6],
            } for r in rows
        ]
    }


@app.get("/go/lfg/{pid}")
def go_lfg(
    pid: str,
    request: Request,
    src: str = "site",
    utm_campaign: str = "lfg",
    utm_content: str = "card",
):
    conn = db()
    row = conn.execute("""
        SELECT tg_topic_url
        FROM lfg_posts
        WHERE id=? AND active=1
        LIMIT 1
    """, (pid,)).fetchone()

    if not row:
        conn.close()
        return RedirectResponse(url=f"{SITE_BASE}/lfg", status_code=302)

    tg_url = row[0]

    # один общий логгер на всё:
    log_click(conn, deal_id=f"lfg:{pid}", request=request, src=src, utm_campaign=utm_campaign, utm_content=utm_content)

    conn.close()
    return RedirectResponse(url=tg_url, status_code=302)

# --------------------
# API endpoints
# --------------------

@app.get("/go/{deal_id}")
def go_deal(deal_id: str, request: Request):
    conn = db()

    src = request.query_params.get("src") or "tg"
    utm_campaign = request.query_params.get("utm_campaign")
    utm_content = request.query_params.get("utm_content")

    resp = RedirectResponse(url=f"/d/{deal_id}", status_code=302)

    # ставим cookie тут
    vid = get_or_set_vid(request, resp)

    log_click(conn, deal_id, request,
              src=src,
              utm_campaign=utm_campaign,
              utm_content=utm_content,
              visitor_id=vid)

    conn.close()
    return resp

@app.api_route("/health", methods=["GET", "HEAD"])
def health():
    return {"ok": True}


@app.get("/debug_tg")
def debug_tg():
    return {"bot_token_present": bool(TG_BOT_TOKEN), "chat_id": TG_CHAT_ID}

STATS_PAGE = Template("""
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Stats — FreeRG</title>
  <style>
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;background:#0a0e1a;color:#e2e8f0;margin:0;padding:24px}
    .wrap{max-width:1100px;margin:0 auto}
    .row{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:12px}
    .card{background:#1a1f36;border:1px solid rgba(255,255,255,.1);border-radius:16px;padding:14px}
    .h{font-weight:900;font-size:22px;margin:0}
    .muted{color:#94a3b8;font-size:13px}
    table{width:100%;border-collapse:collapse;margin-top:12px}
    th,td{padding:10px;border-bottom:1px solid rgba(255,255,255,.08);text-align:left;font-size:14px}
    a{color:#a5b4fc;text-decoration:none}
    .bar{height:10px;border-radius:999px;background:rgba(255,255,255,.08);overflow:hidden}
    .bar > div{height:100%;background:linear-gradient(135deg,#667eea,#764ba2)}
    .grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    @media(max-width:900px){.row{grid-template-columns:1fr}.grid2{grid-template-columns:1fr}}
  </style>
</head>
<body>
<div class="wrap">
  <div class="row">
      <div class="row">
    <div class="card">
      <div class="muted">Users ({{ days }}d, no bots)</div>
      <div class="h">{{ users_total }}</div>
    </div>
    <div class="card">
      <div class="muted">New / Returning</div>
      <div class="h">{{ users_new }} / {{ users_returning }}</div>
    </div>
    <div class="card">
      <div class="muted">Returning rate</div>
      <div class="h">{{ returning_rate }}</div>
    </div>
  </div>
    <div class="card">
      <div class="muted">Клики за 24 часа</div>
      <div class="h">{{ clicks_24h }}</div>
    </div>
    <div class="card">
      <div class="muted">Клики за {{ days }} дней</div>
      <div class="h">{{ clicks_total }}</div>
    </div>
    <div class="card">
      <div class="muted">Диапазон</div>
      <div class="h">{{ days }}d</div>
    </div>
  </div>

  <div class="grid2">
      <div class="card" style="margin-top:12px">
    <div class="h" style="font-size:18px">Пики по часам (Asia/Bishkek)</div>
    <div class="muted">клики (без bot) за последние {{ days }} дней</div>
    <table>
      <thead><tr><th>Час</th><th>Клики</th><th style="width:55%"></th></tr></thead>
      <tbody>
      {% for x in hours %}
        <tr>
          <td>{{ x.h }}:00</td>
          <td><b>{{ x.c }}</b></td>
          <td><div class="bar"><div style="width: {{ (x.c / hour_max * 100) | int }}%"></div></div></td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
  </div>
      <div class="card" style="margin-top:12px">
    <div class="h" style="font-size:18px">TG → Store по форматам (utm_content)</div>
    <div class="muted">что лучше конвертит (tg клики → out клики)</div>
    <table>
      <thead><tr><th>Формат</th><th>TG</th><th>OUT</th><th>Conv</th></tr></thead>
      <tbody>
      {% for f in formats %}
        <tr>
          <td><b>{{ f.fmt }}</b></td>
          <td>{{ f.tg }}</td>
          <td>{{ f.out }}</td>
          <td><b>{{ f.conv }}</b></td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
  </div>
    <div class="card">
      <div class="h" style="font-size:18px">Динамика</div>
      <div class="muted">последние {{ days }} дней</div>
      <table>
        <thead><tr><th>День</th><th>Клики</th><th style="width:45%"> </th></tr></thead>
        <tbody>
        {% for d in daily %}
          <tr>
            <td>{{ d.day }}</td>
            <td><b>{{ d.clicks }}</b></td>
            <td>
              <div class="bar"><div style="width: {{ d.pct }}%"></div></div>
            </td>
          </tr>
        {% endfor %}
        </tbody>
      </table>
    </div>

    <div class="card">
      <div class="h" style="font-size:18px">Топ игр</div>
      <div class="muted">клики на карточку (по deal)</div>
      <table>
        <thead><tr><th>#</th><th>Игра</th><th>Store</th><th>Клики</th></tr></thead>
        <tbody>
        {% for it in top %}
          <tr>
            <td>{{ loop.index }}</td>
            <td><a href="{{ it.link }}" target="_blank">{{ it.title }}</a></td>
            <td>{{ it.store }}</td>
            <td><b>{{ it.clicks }}</b></td>
          </tr>
        {% endfor %}
        </tbody>
      </table>
    </div>
  </div>

  <div class="muted" style="margin-top:14px">
    Параметры: <a href="/stats_html?days=1">1 день</a> · <a href="/stats_html?days=7">7 дней</a> · <a href="/stats_html?days=30">30 дней</a>
  </div>
</div>
</body>
</html>
""")

import os, secrets
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

security = HTTPBasic()

ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "Mysupersecret!")

def require_basic(credentials: HTTPBasicCredentials = Depends(security)):
    ok_user = secrets.compare_digest(credentials.username, ADMIN_USER)
    ok_pass = secrets.compare_digest(credentials.password, ADMIN_PASS)
    if not (ok_user and ok_pass):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            headers={"WWW-Authenticate": "Basic"},
        )

@app.get("/stats_html", response_class=HTMLResponse, dependencies=[Depends(require_basic)])
def stats_html(days: int = 7, top: int = 15):
    if days < 1: days = 1
    if days > 90: days = 90
    if top < 1: top = 1
    if top > 50: top = 50

    conn = db()

    clicks_total = conn.execute("""
        SELECT COUNT(*) FROM clicks
        WHERE datetime(created_at) >= datetime('now', ?)
    """, (f"-{days} days",)).fetchone()[0]
        # уникальные пользователи (visitor_id) за диапазон
    users_total = conn.execute("""
        SELECT COUNT(DISTINCT visitor_id)
        FROM clicks
        WHERE visitor_id IS NOT NULL
          AND datetime(created_at) >= datetime('now', ?)
          AND COALESCE(user_agent,'') NOT LIKE '%bot%'
    """, (f"-{days} days",)).fetchone()[0]

    # returning: был ДО начала окна
    users_returning = conn.execute("""
        WITH in_range AS (
          SELECT DISTINCT visitor_id
          FROM clicks
          WHERE visitor_id IS NOT NULL
            AND datetime(created_at) >= datetime('now', ?)
            AND COALESCE(user_agent,'') NOT LIKE '%bot%'
        )
        SELECT COUNT(*)
        FROM in_range r
        WHERE EXISTS (
          SELECT 1 FROM clicks c
          WHERE c.visitor_id = r.visitor_id
            AND datetime(c.created_at) < datetime('now', ?)
        )
    """, (f"-{days} days", f"-{days} days")).fetchone()[0]

    users_new = max(users_total - users_returning, 0)
    returning_rate = (users_returning / users_total) if users_total else 0.0

    # пики по часам (по Бишкеку +6)
    hours = conn.execute("""
        SELECT strftime('%H', datetime(created_at, '+6 hours')) as h, COUNT(*) cnt
        FROM clicks
        WHERE datetime(created_at) >= datetime('now', ?)
          AND COALESCE(user_agent,'') NOT LIKE '%bot%'
        GROUP BY h
        ORDER BY h
    """, (f"-{days} days",)).fetchall()
    hour_map = {h: c for h, c in hours}
    hour_series = [{"h": f"{i:02d}", "c": int(hour_map.get(f"{i:02d}", 0))} for i in range(24)]
    hour_max = max([x["c"] for x in hour_series], default=1)

    # TG форматы: tg->out по utm_content
    fmt_rows = conn.execute("""
        WITH tg AS (
          SELECT COALESCE(utm_content,'') fmt, COUNT(*) cnt
          FROM clicks
          WHERE src='tg' AND datetime(created_at) >= datetime('now', ?)
          GROUP BY fmt
        ),
        out AS (
          SELECT COALESCE(utm_content,'') fmt, COUNT(*) cnt
          FROM clicks
          WHERE src='out' AND datetime(created_at) >= datetime('now', ?)
          GROUP BY fmt
        )
        SELECT tg.fmt, tg.cnt tg_clicks, COALESCE(out.cnt,0) out_clicks
        FROM tg
        LEFT JOIN out ON out.fmt = tg.fmt
        ORDER BY (1.0*COALESCE(out.cnt,0)/tg.cnt) DESC, tg_clicks DESC
        LIMIT 12
    """, (f"-{days} days", f"-{days} days")).fetchall()

    formats = []
    for fmt, tg_clicks, out_clicks in fmt_rows:
        conv = (out_clicks / tg_clicks) if tg_clicks else 0.0
        formats.append({
            "fmt": fmt or "(empty)",
            "tg": tg_clicks,
            "out": out_clicks,
            "conv": round(conv, 3)
        })

    clicks_24h = conn.execute("""
        SELECT COUNT(*) FROM clicks
        WHERE datetime(created_at) >= datetime('now', '-1 day')
    """).fetchone()[0]
        # уникальные пользователи (visitor_id) за диапазон
    users_total = conn.execute("""
        SELECT COUNT(DISTINCT visitor_id)
        FROM clicks
        WHERE visitor_id IS NOT NULL
          AND datetime(created_at) >= datetime('now', ?)
          AND COALESCE(user_agent,'') NOT LIKE '%bot%'
    """, (f"-{days} days",)).fetchone()[0]

    # returning: был ДО начала окна
    users_returning = conn.execute("""
        WITH in_range AS (
          SELECT DISTINCT visitor_id
          FROM clicks
          WHERE visitor_id IS NOT NULL
            AND datetime(created_at) >= datetime('now', ?)
            AND COALESCE(user_agent,'') NOT LIKE '%bot%'
        )
        SELECT COUNT(*)
        FROM in_range r
        WHERE EXISTS (
          SELECT 1 FROM clicks c
          WHERE c.visitor_id = r.visitor_id
            AND datetime(c.created_at) < datetime('now', ?)
        )
    """, (f"-{days} days", f"-{days} days")).fetchone()[0]

    users_new = max(users_total - users_returning, 0)
    returning_rate = (users_returning / users_total) if users_total else 0.0

    # пики по часам (по Бишкеку +6)
    hours = conn.execute("""
        SELECT strftime('%H', datetime(created_at, '+6 hours')) as h, COUNT(*) cnt
        FROM clicks
        WHERE datetime(created_at) >= datetime('now', ?)
          AND COALESCE(user_agent,'') NOT LIKE '%bot%'
        GROUP BY h
        ORDER BY h
    """, (f"-{days} days",)).fetchall()
    hour_map = {h: c for h, c in hours}
    hour_series = [{"h": f"{i:02d}", "c": int(hour_map.get(f"{i:02d}", 0))} for i in range(24)]
    hour_max = max([x["c"] for x in hour_series], default=1)

    # TG форматы: tg->out по utm_content
    fmt_rows = conn.execute("""
        WITH tg AS (
          SELECT COALESCE(utm_content,'') fmt, COUNT(*) cnt
          FROM clicks
          WHERE src='tg' AND datetime(created_at) >= datetime('now', ?)
          GROUP BY fmt
        ),
        out AS (
          SELECT COALESCE(utm_content,'') fmt, COUNT(*) cnt
          FROM clicks
          WHERE src='out' AND datetime(created_at) >= datetime('now', ?)
          GROUP BY fmt
        )
        SELECT tg.fmt, tg.cnt tg_clicks, COALESCE(out.cnt,0) out_clicks
        FROM tg
        LEFT JOIN out ON out.fmt = tg.fmt
        ORDER BY (1.0*COALESCE(out.cnt,0)/tg.cnt) DESC, tg_clicks DESC
        LIMIT 12
    """, (f"-{days} days", f"-{days} days")).fetchall()

    formats = []
    for fmt, tg_clicks, out_clicks in fmt_rows:
        conv = (out_clicks / tg_clicks) if tg_clicks else 0.0
        formats.append({
            "fmt": fmt or "(empty)",
            "tg": tg_clicks,
            "out": out_clicks,
            "conv": round(conv, 3)
        })

    series = conn.execute("""
        SELECT substr(created_at, 1, 10) as day, COUNT(*) as cnt
        FROM clicks
        WHERE datetime(created_at) >= datetime('now', ?)
        GROUP BY day
        ORDER BY day ASC
    """, (f"-{days} days",)).fetchall()

    rows = conn.execute("""
        SELECT c.deal_id, COUNT(*) as cnt, d.title, d.store
        FROM clicks c
        LEFT JOIN deals d ON d.id = c.deal_id
        WHERE datetime(c.created_at) >= datetime('now', ?)
            AND c.src = 'out'
        GROUP BY c.deal_id
        ORDER BY cnt DESC
        LIMIT ?
    """, (f"-{days} days", top)).fetchall()

    conn.close()

    max_cnt = max([c for _, c in series], default=1)
    daily = [{"day": d, "clicks": c, "pct": int((c / max_cnt) * 100)} for d, c in series]

    top_items = [{
        "title": (title or "(не найдено)"),
        "store": (store or ""),
        "clicks": cnt,
        "link": f"{SITE_BASE}/d/{deal_id}",
    } for deal_id, cnt, title, store in rows]

    return STATS_PAGE.render(
        days=days,
        clicks_total=clicks_total,
        clicks_24h=clicks_24h,
        daily=daily,
        top=top_items,
        users_total=users_total,
        users_new=users_new,
        users_returning=users_returning,
        returning_rate=round(returning_rate, 3),
        hours=hour_series,
        hour_max=hour_max,
        formats=formats,
    )


@app.get("/stats")
def stats(days: int = 7, top: int = 15):
    # days=7 по умолчанию; можно /stats?days=1 для суток
    if days < 1:
        days = 1
    if days > 90:
        days = 90
    if top < 1:
        top = 1
    if top > 50:
        top = 50

    conn = db()

    # всего кликов за N дней
    total = conn.execute("""
        SELECT COUNT(*)
        FROM clicks
        WHERE datetime(created_at) >= datetime('now', ?)
    """, (f"-{days} days",)).fetchone()[0]

    # клики за последние 24 часа (отдельно)
    day_total = conn.execute("""
        SELECT COUNT(*)
        FROM clicks
        WHERE datetime(created_at) >= datetime('now', '-1 day')
    """).fetchone()[0]

    # топ по deal_id за N дней + подтягиваем title/store
    rows = conn.execute("""
        SELECT c.deal_id,
               COUNT(*) as cnt,
               d.title,
               d.store,
               d.kind
        FROM clicks c
        LEFT JOIN deals d ON d.id = c.deal_id
        WHERE datetime(c.created_at) >= datetime('now', ?)
        GROUP BY c.deal_id
        ORDER BY cnt DESC
        LIMIT ?
    """, (f"-{days} days", top)).fetchall()

    # дневная динамика по дням (последние N дней)
    series = conn.execute("""
        SELECT substr(created_at, 1, 10) as day,
               COUNT(*) as cnt
        FROM clicks
        WHERE datetime(created_at) >= datetime('now', ?)
        GROUP BY day
        ORDER BY day ASC
    """, (f"-{days} days",)).fetchall()

    conn.close()

    top_items = []
    for deal_id, cnt, title, store, kind in rows:
        top_items.append({
            "deal_id": deal_id,
            "clicks": cnt,
            "title": title or "(не найдено в deals)",
            "store": store or "",
            "kind": kind or "",
            "link": f"{SITE_BASE}/d/{deal_id}",
        })

    return {
        "range_days": days,
        "clicks_last_24h": day_total,
        "clicks_range_total": total,
        "daily": [{"day": d, "clicks": c} for d, c in series],
        "top": top_items,
    }


@app.get("/stats_hours")
def stats_hours(days: int = 7):
    if days < 1: days = 1
    if days > 90: days = 90

    conn = db()

    # created_at у тебя UTC isoformat → используем datetime(created_at)
    # Если хочешь по Бишкеку (+06), добавь '+6 hours'
    rows = conn.execute("""
        SELECT strftime('%H', datetime(created_at, '+6 hours')) as hour, COUNT(*) cnt
        FROM clicks
        WHERE datetime(created_at) >= datetime('now', ?)
          AND COALESCE(user_agent,'') NOT LIKE '%bot%'
        GROUP BY hour
        ORDER BY hour
    """, (f"-{days} days",)).fetchall()

    conn.close()

    # заполним отсутствующие часы нулями
    m = {h: c for h, c in rows}
    series = [{"hour": f"{i:02d}", "clicks": int(m.get(f"{i:02d}", 0))} for i in range(24)]
    peak = max(series, key=lambda x: x["clicks"]) if series else {"hour":"00","clicks":0}

    return {"range_days": days, "series": series, "peak": peak}

@app.get("/stats_retention")
def stats_retention(days: int = 7):
    if days < 1: days = 1
    if days > 90: days = 90

    conn = db()

    # users that appeared in range
    total_users = conn.execute("""
        SELECT COUNT(DISTINCT visitor_id)
        FROM clicks
        WHERE visitor_id IS NOT NULL
          AND datetime(created_at) >= datetime('now', ?)
          AND COALESCE(user_agent,'') NOT LIKE '%bot%'
    """, (f"-{days} days",)).fetchone()[0]

    # returning: had activity BEFORE range start
    returning = conn.execute("""
        WITH in_range AS (
          SELECT DISTINCT visitor_id
          FROM clicks
          WHERE visitor_id IS NOT NULL
            AND datetime(created_at) >= datetime('now', ?)
            AND COALESCE(user_agent,'') NOT LIKE '%bot%'
        )
        SELECT COUNT(*)
        FROM in_range r
        WHERE EXISTS (
          SELECT 1 FROM clicks c
          WHERE c.visitor_id = r.visitor_id
            AND datetime(c.created_at) < datetime('now', ?)
        )
    """, (f"-{days} days", f"-{days} days")).fetchone()[0]

    new_users = max(total_users - returning, 0)

    conn.close()
    return {
        "range_days": days,
        "users_total": total_users,
        "users_new": new_users,
        "users_returning": returning,
        "returning_rate": round((returning/total_users), 3) if total_users else 0.0
    }

@app.get("/count")
def count_rows():
    conn = db()
    total = conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0]
    conn.close()
    return {"total": total}


@app.get("/backfill")
def backfill():
    backfill_defaults()
    return {"ok": True}


def job_sync(store: str = "steam"):
    return asyncio.run(job_async(store=store))

import subprocess

@app.get("/update")
async def update_now(store: str = "steam"):
    subprocess.Popen(["systemctl", "start", f"freerg-update@{store}.service"])
    return {"ok": True, "queued": True, "store": store}


@app.get("/testpost")
async def testpost():
    if not bot:
        return {"ok": False, "error": "bot is None (no TG_BOT_TOKEN?)"}
    await bot.send_message(chat_id=TG_CHAT_ID, text="✅ Тест: бот может постить в канал")
    return {"ok": True}


@app.get("/post_last")
async def post_last(n: int = 1):
    """
    Форс-пост последних N (для тестов): помечаем posted=0 и отправляем.
    """
    conn = db()
    ids = conn.execute("SELECT id FROM deals ORDER BY created_at DESC LIMIT ?", (n,)).fetchall()
    for (did,) in ids:
        conn.execute("UPDATE deals SET posted=0 WHERE id=?", (did,))
    conn.commit()
    conn.close()

    tg = await post_unposted_to_telegram(limit=n)
    return {"ok": True, "result": tg}


@app.get("/debug_epic")
def debug_epic():
    try:
        deals = fetch_epic()
        return {"ok": True, "count": len(deals), "sample": deals[0] if deals else None}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/debug_itad")
def debug_itad():
    if not ITAD_API_KEY:
        return {"ok": False, "error": "ITAD_API_KEY is empty"}
    try:
        deals = fetch_itad_steam()
        return {"ok": True, "count": len(deals), "sample": deals[0] if deals else None}
    except Exception as e:
        return {"ok": False, "error": str(e)}



@app.get("/cleanup")
def cleanup(keep_days: int = 7):
    deleted = cleanup_expired(keep_days=keep_days)
    return {"ok": True, "deleted": deleted, "keep_days": keep_days}


# --------------------
# Startup / Shutdown
# --------------------
def run_job(store: str):
    # APScheduler вызывает обычную функцию (sync),
    # поэтому запускаем async-джоб через asyncio.run()
    asyncio.run(job_async(store=store))


# ==========================================
# 🛡️ АДМИН-ПАНЕЛЬ
# ==========================================

@app.get("/admin/login", response_class=HTMLResponse)
def admin_login_page(error: str = ""):
    error_msg = f"<div style='color:red;margin:10px 0'>{error}</div>" if error else ""
    
    return f"""
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Admin Login</title>
        <style>
            body {{
                font-family: system-ui, -apple-system, sans-serif;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                display: flex;
                align-items: center;
                justify-content: center;
                margin: 0;
            }}
            .login-box {{
                background: white;
                padding: 40px;
                border-radius: 16px;
                box-shadow: 0 8px 32px rgba(0,0,0,0.2);
                width: 100%;
                max-width: 400px;
            }}
            h2 {{
                margin: 0 0 24px 0;
                color: #333;
                text-align: center;
            }}
            input {{
                width: 100%;
                padding: 12px;
                border: 1px solid #ddd;
                border-radius: 8px;
                font-size: 16px;
                margin-bottom: 16px;
                box-sizing: border-box;
            }}
            button {{
                width: 100%;
                padding: 12px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                border: none;
                border-radius: 8px;
                font-size: 16px;
                font-weight: 600;
                cursor: pointer;
            }}
            button:hover {{
                opacity: 0.9;
            }}
        </style>
    </head>
    <body>
        <div class="login-box">
            <h2>🔐 Admin Login</h2>
            {error_msg}
            <form method="post" action="/admin/auth">
                <input type="password" name="password" placeholder="Пароль" required autofocus>
                <button type="submit">Войти</button>
            </form>
            <div style="text-align:center;margin-top:20px;opacity:0.6">
                <a href="/" style="color:#667eea">← На главную</a>
            </div>
        </div>
    </body>
    </html>
    """


from fastapi import Form

@app.post("/admin/auth")
def admin_auth(password: str = Form(...)):
    if check_admin_password(password):
        response = RedirectResponse("/admin/lfg", status_code=302)
        response.set_cookie("admin_token", ADMIN_PASSWORD_HASH, max_age=3600*24*7)  # 7 дней
        return response
    return RedirectResponse("/admin/login?error=Неверный пароль", status_code=302)


@app.get("/admin/logout")
def admin_logout():
    response = RedirectResponse("/admin/login", status_code=302)
    response.delete_cookie("admin_token")
    return response


@app.get("/admin/lfg", response_class=HTMLResponse)
def admin_lfg_panel(request: Request, filter: str = "all"):
    # Проверка авторизации
    redirect_check = require_admin(request)
    if redirect_check:
        return redirect_check
    
    conn = db()
    
    # Фильтры
    if filter == "active":
        rows = conn.execute("""
            SELECT id, created_at, game, region, platform, note, tg_user, ip, expires_at
            FROM lfg
            WHERE active=1
            ORDER BY created_at DESC
            LIMIT 100
        """).fetchall()
    elif filter == "expired":
        rows = conn.execute("""
            SELECT id, created_at, game, region, platform, note, tg_user, ip, expires_at
            FROM lfg
            WHERE expires_at < ?
            ORDER BY created_at DESC
            LIMIT 100
        """, (datetime.utcnow().isoformat(),)).fetchall()
    else:  # all
        rows = conn.execute("""
            SELECT id, created_at, game, region, platform, note, tg_user, ip, expires_at
            FROM lfg
            ORDER BY created_at DESC
            LIMIT 100
        """).fetchall()
    
    # Статистика
    total = conn.execute("SELECT COUNT(*) FROM lfg").fetchone()[0]
    active = conn.execute("SELECT COUNT(*) FROM lfg WHERE active=1").fetchone()[0]
    expired = conn.execute("SELECT COUNT(*) FROM lfg WHERE expires_at < ?", 
                          (datetime.utcnow().isoformat(),)).fetchone()[0]
    
    conn.close()
    
    # HTML
    html = f"""
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Admin - LFG</title>
        <style>
            body {{
                font-family: system-ui, -apple-system, sans-serif;
                background: #0a0e1a;
                color: #e2e8f0;
                margin: 0;
                padding: 20px;
            }}
            .container {{
                max-width: 1200px;
                margin: 0 auto;
            }}
            .header {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 30px;
                padding-bottom: 20px;
                border-bottom: 1px solid rgba(255,255,255,0.1);
            }}
            .stats {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 15px;
                margin-bottom: 20px;
            }}
            .stat-card {{
                background: #1a1f36;
                padding: 20px;
                border-radius: 12px;
                border: 1px solid rgba(255,255,255,0.1);
            }}
            .stat-value {{
                font-size: 2rem;
                font-weight: 700;
                color: #667eea;
            }}
            .stat-label {{
                color: #94a3b8;
                font-size: 0.9rem;
                margin-top: 5px;
            }}
            .filters {{
                display: flex;
                gap: 10px;
                margin-bottom: 20px;
                flex-wrap: wrap;
            }}
            .filter-btn {{
                padding: 10px 20px;
                background: #1a1f36;
                color: #e2e8f0;
                border: 1px solid rgba(255,255,255,0.1);
                border-radius: 8px;
                text-decoration: none;
                cursor: pointer;
                transition: all 0.2s;
            }}
            .filter-btn:hover {{
                background: #252a44;
                border-color: #667eea;
            }}
            .filter-btn.active {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                border-color: transparent;
            }}
            .card {{
                background: #1a1f36;
                padding: 20px;
                margin-bottom: 15px;
                border-radius: 12px;
                border: 1px solid rgba(255,255,255,0.1);
            }}
            .card-header {{
                display: flex;
                justify-content: space-between;
                align-items: flex-start;
                margin-bottom: 12px;
            }}
            .game-title {{
                font-size: 1.2rem;
                font-weight: 700;
                color: #e2e8f0;
            }}
            .meta {{
                display: flex;
                gap: 12px;
                flex-wrap: wrap;
                margin: 12px 0;
                font-size: 0.9rem;
            }}
            .meta-item {{
                background: rgba(255,255,255,0.05);
                padding: 4px 10px;
                border-radius: 6px;
                color: #94a3b8;
            }}
            .note {{
                margin: 12px 0;
                padding: 12px;
                background: rgba(0,0,0,0.2);
                border-radius: 8px;
                white-space: pre-wrap;
                word-break: break-word;
            }}
            .actions {{
                display: flex;
                gap: 10px;
                margin-top: 15px;
            }}
            .btn {{
                padding: 8px 16px;
                border-radius: 8px;
                border: none;
                cursor: pointer;
                font-weight: 600;
                transition: all 0.2s;
            }}
            .btn-delete {{
                background: #ef4444;
                color: white;
            }}
            .btn-delete:hover {{
                background: #dc2626;
            }}
            .btn-activate {{
                background: #10b981;
                color: white;
            }}
            .btn-activate:hover {{
                background: #059669;
            }}
            .btn-logout {{
                background: transparent;
                color: #94a3b8;
                border: 1px solid rgba(255,255,255,0.1);
                padding: 8px 16px;
            }}
            .expired {{
                opacity: 0.5;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🛡️ Admin Panel - LFG</h1>
                <a href="/admin/logout" class="btn btn-logout">Выйти</a>
            </div>
            
            <div class="stats">
                <div class="stat-card">
                    <div class="stat-value">{total}</div>
                    <div class="stat-label">Всего заявок</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">{active}</div>
                    <div class="stat-label">Активные</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">{expired}</div>
                    <div class="stat-label">Истекшие</div>
                </div>
            </div>
            
            <div class="filters">
                <a href="/admin/lfg?filter=all" class="filter-btn {'active' if filter == 'all' else ''}">
                    📋 Все ({total})
                </a>
                <a href="/admin/lfg?filter=active" class="filter-btn {'active' if filter == 'active' else ''}">
                    ✅ Активные ({active})
                </a>
                <a href="/admin/lfg?filter=expired" class="filter-btn {'active' if filter == 'expired' else ''}">
                    ⏰ Истекшие ({expired})
                </a>
            </div>
            
            <div>
    """
    
    for r in rows:
        lfg_id, created, game, region, platform, note, tg_user, ip, expires = r
        
        # Проверяем истёк ли срок
        is_expired = False
        if expires:
            try:
                exp_dt = datetime.fromisoformat(expires.replace('Z', '+00:00'))
                is_expired = exp_dt < datetime.utcnow().replace(tzinfo=None)
            except:
                pass
        
        card_class = "card expired" if is_expired else "card"
        
        html += f"""
        <div class="{card_class}">
            <div class="card-header">
                <div class="game-title">{game}</div>
                <div style="font-size:0.85rem;color:#64748b">
                    {created[:16] if created else ''}
                </div>
            </div>
            
            <div class="meta">
                {f'<span class="meta-item">🌍 {region}</span>' if region else ''}
                {f'<span class="meta-item">🎮 {platform}</span>' if platform else ''}
                {f'<span class="meta-item">💬 @{tg_user}</span>' if tg_user else ''}
                {f'<span class="meta-item">🔒 {ip[:15] if ip else "?"}</span>'}
                {f'<span class="meta-item">⏰ до {expires[:16]}</span>' if expires else ''}
            </div>
            
            {f'<div class="note">{note}</div>' if note else ''}
            
            <div class="actions">
                <button class="btn btn-delete" onclick="deletePost('{lfg_id}')">
                    🗑 Удалить
                </button>
            </div>
        </div>
        """
    
    if not rows:
        html += "<div class='card'>Заявок нет</div>"
    
    html += """
            </div>
        </div>
        
        <script>
        async function deletePost(id) {
            if(!confirm('Удалить эту заявку?')) return;
            
            const r = await fetch(`/admin/lfg/delete/${id}`, {method: 'POST'});
            if(r.ok) {
                alert('Удалено!');
                location.reload();
            } else {
                alert('Ошибка удаления');
            }
        }
        </script>
    </body>
    </html>
    """
    
    return HTMLResponse(html)


@app.post("/admin/lfg/delete/{lfg_id}")
def admin_delete_lfg(lfg_id: str, request: Request):
    # Проверка авторизации
    redirect_check = require_admin(request)
    if redirect_check:
        return redirect_check
    
    conn = db()
    conn.execute("DELETE FROM lfg WHERE id=?", (lfg_id,))
    conn.commit()
    conn.close()
    
    return {"ok": True}


# Очистка истекших заявок (можно вызывать вручную или по крону)
@app.get("/admin/cleanup")
def admin_cleanup(request: Request):
    redirect_check = require_admin(request)
    if redirect_check:
        return redirect_check
    
    conn = db()
    deleted = conn.execute("""
        DELETE FROM lfg
        WHERE expires_at < ?
    """, (datetime.utcnow().isoformat(),)).rowcount
    conn.commit()
    conn.close()
    
    return {"ok": True, "deleted": deleted}

# ✨ АДМИНКА ДЛЯ ЭКСКЛЮЗИВОВ
# Добавь этот код после функции admin_lfg_panel (примерно после строки 5150)

@app.get("/admin/exclusive", response_class=HTMLResponse)
def admin_exclusive_panel(request: Request, filter: str = "all"):
    """Админ-панель для управления эксклюзивами"""
    # Проверка авторизации
    redirect_check = require_admin(request)
    if redirect_check:
        return redirect_check
    
    conn = db()
    
    # Фильтры
    if filter == "active":
        rows = conn.execute("""
            SELECT id, created_at, title, url, store, kind, 
                   price_old, price_new, currency, ends_at, is_published
            FROM manual_news
            WHERE is_published=1
            ORDER BY created_at DESC
        """).fetchall()
    elif filter == "hidden":
        rows = conn.execute("""
            SELECT id, created_at, title, url, store, kind,
                   price_old, price_new, currency, ends_at, is_published
            FROM manual_news
            WHERE is_published=0
            ORDER BY created_at DESC
        """).fetchall()
    else:  # all
        rows = conn.execute("""
            SELECT id, created_at, title, url, store, kind,
                   price_old, price_new, currency, ends_at, is_published
            FROM manual_news
            ORDER BY created_at DESC
        """).fetchall()
    
    # Статистика
    total = conn.execute("SELECT COUNT(*) FROM manual_news").fetchone()[0]
    active = conn.execute("SELECT COUNT(*) FROM manual_news WHERE is_published=1").fetchone()[0]
    hidden = conn.execute("SELECT COUNT(*) FROM manual_news WHERE is_published=0").fetchone()[0]
    
    conn.close()
    
    # HTML
    html = f"""
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Admin - Эксклюзивы</title>
        <style>
            body {{
                font-family: system-ui, -apple-system, sans-serif;
                background: #0a0e1a;
                color: #e2e8f0;
                margin: 0;
                padding: 20px;
            }}
            .container {{
                max-width: 1400px;
                margin: 0 auto;
            }}
            .header {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 30px;
                padding-bottom: 20px;
                border-bottom: 1px solid rgba(255,255,255,0.1);
            }}
            .nav {{
                display: flex;
                gap: 15px;
                margin-bottom: 20px;
            }}
            .nav a {{
                padding: 10px 20px;
                background: #1a1f36;
                color: #e2e8f0;
                border: 1px solid rgba(255,255,255,0.1);
                border-radius: 8px;
                text-decoration: none;
                transition: all 0.2s;
            }}
            .nav a:hover {{
                background: #252a44;
                border-color: #667eea;
            }}
            .nav a.active {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                border-color: transparent;
            }}
            .stats {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 15px;
                margin-bottom: 20px;
            }}
            .stat-card {{
                background: #1a1f36;
                padding: 20px;
                border-radius: 12px;
                border: 1px solid rgba(255,255,255,0.1);
            }}
            .stat-value {{
                font-size: 2rem;
                font-weight: 700;
                color: #667eea;
            }}
            .stat-label {{
                color: #94a3b8;
                font-size: 0.9rem;
                margin-top: 5px;
            }}
            .filters {{
                display: flex;
                gap: 10px;
                margin-bottom: 20px;
                flex-wrap: wrap;
            }}
            .filter-btn {{
                padding: 10px 20px;
                background: #1a1f36;
                color: #e2e8f0;
                border: 1px solid rgba(255,255,255,0.1);
                border-radius: 8px;
                text-decoration: none;
                cursor: pointer;
                transition: all 0.2s;
            }}
            .filter-btn:hover {{
                background: #252a44;
                border-color: #667eea;
            }}
            .filter-btn.active {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                border-color: transparent;
            }}
            .card {{
                background: #1a1f36;
                padding: 20px;
                margin-bottom: 15px;
                border-radius: 12px;
                border: 1px solid rgba(255,255,255,0.1);
            }}
            .card.hidden {{
                opacity: 0.5;
                border-color: rgba(255,100,100,0.3);
            }}
            .card-header {{
                display: flex;
                justify-content: space-between;
                align-items: flex-start;
                margin-bottom: 12px;
            }}
            .game-title {{
                font-size: 1.2rem;
                font-weight: 700;
                color: #e2e8f0;
            }}
            .meta {{
                display: flex;
                gap: 12px;
                flex-wrap: wrap;
                margin: 12px 0;
                font-size: 0.9rem;
            }}
            .meta-item {{
                background: rgba(255,255,255,0.05);
                padding: 4px 10px;
                border-radius: 6px;
                color: #94a3b8;
            }}
            .price {{
                font-size: 1.1rem;
                font-weight: 700;
                color: #10b981;
            }}
            .price .old {{
                text-decoration: line-through;
                opacity: 0.6;
                margin-right: 8px;
            }}
            .actions {{
                display: flex;
                gap: 10px;
                margin-top: 15px;
                flex-wrap: wrap;
            }}
            .btn {{
                padding: 8px 16px;
                border-radius: 8px;
                border: none;
                cursor: pointer;
                font-weight: 600;
                transition: all 0.2s;
                text-decoration: none;
                display: inline-block;
            }}
            .btn-delete {{
                background: #ef4444;
                color: white;
            }}
            .btn-delete:hover {{
                background: #dc2626;
            }}
            .btn-hide {{
                background: #f59e0b;
                color: white;
            }}
            .btn-hide:hover {{
                background: #d97706;
            }}
            .btn-show {{
                background: #10b981;
                color: white;
            }}
            .btn-show:hover {{
                background: #059669;
            }}
            .btn-edit {{
                background: #3b82f6;
                color: white;
            }}
            .btn-edit:hover {{
                background: #2563eb;
            }}
            .btn-logout {{
                background: transparent;
                color: #94a3b8;
                border: 1px solid rgba(255,255,255,0.1);
                padding: 8px 16px;
            }}
            .btn-add {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 12px 24px;
                font-size: 1rem;
            }}
            .badge {{
                display: inline-block;
                padding: 4px 8px;
                border-radius: 4px;
                font-size: 0.75rem;
                font-weight: 700;
                text-transform: uppercase;
            }}
            .badge-active {{
                background: rgba(16, 185, 129, 0.2);
                color: #10b981;
            }}
            .badge-hidden {{
                background: rgba(239, 68, 68, 0.2);
                color: #ef4444;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🛡️ Admin Panel - Эксклюзивы</h1>
                <div style="display: flex; gap: 10px;">
                    <a href="/admin/lfg" class="btn btn-logout">LFG →</a>
                    <a href="/admin/logout" class="btn btn-logout">Выйти</a>
                </div>
            </div>
            
            <div class="nav">
                <a href="/admin/news?key={os.getenv('ADMIN_KEY', '')}" class="btn-add">
                    ➕ Добавить эксклюзив
                </a>
            </div>
            
            <div class="stats">
                <div class="stat-card">
                    <div class="stat-value">{total}</div>
                    <div class="stat-label">Всего эксклюзивов</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">{active}</div>
                    <div class="stat-label">Активные</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">{hidden}</div>
                    <div class="stat-label">Скрытые</div>
                </div>
            </div>
            
            <div class="filters">
                <a href="/admin/exclusive?filter=all" class="filter-btn {'active' if filter == 'all' else ''}">
                    📋 Все ({total})
                </a>
                <a href="/admin/exclusive?filter=active" class="filter-btn {'active' if filter == 'active' else ''}">
                    ✅ Активные ({active})
                </a>
                <a href="/admin/exclusive?filter=hidden" class="filter-btn {'active' if filter == 'hidden' else ''}">
                    👁️ Скрытые ({hidden})
                </a>
            </div>
            
            <div>
    """
    
    for r in rows:
        exc_id, created, title, url, store, kind, price_old, price_new, currency, ends_at, is_published = r
        
        # Форматирование
        store_norm = (store or "").strip().lower()
        store_emoji = {
            "steam": "🎮",
            "epic": "🟦",
            "gog": "🟪",
            "prime": "🟨"
        }.get(store_norm, "📦")
        
        # Цены
        price_html = ""
        if price_old is not None or price_new is not None:
            old = f"{price_old:.0f}" if price_old else ""
            new = f"{price_new:.0f}" if price_new else ""
            cur = currency or "USD"
            
            if old and new:
                price_html = f'<div class="price"><span class="old">{old} {cur}</span> → {new} {cur}</div>'
            elif new:
                price_html = f'<div class="price">{new} {cur}</div>'
        
        # Статус
        status_badge = '<span class="badge badge-active">АКТИВЕН</span>' if is_published else '<span class="badge badge-hidden">СКРЫТ</span>'
        
        card_class = "card" if is_published else "card hidden"
        
        html += f"""
        <div class="{card_class}">
            <div class="card-header">
                <div>
                    <div class="game-title">{store_emoji} {title}</div>
                    {status_badge}
                </div>
                <div style="font-size:0.85rem;color:#64748b">
                    ID: {exc_id} | {created[:16] if created else ''}
                </div>
            </div>
            
            <div class="meta">
                <span class="meta-item">🏪 {store_norm or 'other'}</span>
                <span class="meta-item">📁 {kind or 'news'}</span>
                {f'<span class="meta-item">⏰ до {ends_at[:16]}</span>' if ends_at else ''}
            </div>
            
            {price_html}
            
            <div style="margin:10px 0; opacity:0.8; font-size:0.9rem;">
                🔗 <a href="{url}" target="_blank" style="color:#a5b4fc">{url[:60]}...</a>
            </div>
            
            <div class="actions">
                {'<button class="btn btn-hide" onclick="togglePublish(' + str(exc_id) + ', 0)">👁️ Скрыть</button>' if is_published else '<button class="btn btn-show" onclick="togglePublish(' + str(exc_id) + ', 1)">✅ Показать</button>'}
                <button class="btn btn-delete" onclick="deleteExclusive({exc_id})">
                    🗑 Удалить
                </button>
                <a href="{url}" target="_blank" class="btn btn-edit">
                    🔗 Открыть
                </a>
            </div>
        </div>
        """
    
    if not rows:
        html += "<div class='card'>Эксклюзивов нет</div>"
    
    html += """
            </div>
        </div>
        
        <script>
        async function deleteExclusive(id) {
            if(!confirm('Удалить этот эксклюзив навсегда?')) return;
            
            try {
                const r = await fetch(`/admin/exclusive/delete/${id}`, {method: 'POST'});
                if(r.ok) {
                    alert('✅ Удалено!');
                    location.reload();
                } else {
                    alert('❌ Ошибка удаления');
                }
            } catch(e) {
                alert('❌ Ошибка сети: ' + e);
            }
        }
        
        async function togglePublish(id, value) {
            try {
                const r = await fetch(`/admin/exclusive/toggle/${id}`, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({is_published: value})
                });
                
                if(r.ok) {
                    location.reload();
                } else {
                    alert('❌ Ошибка');
                }
            } catch(e) {
                alert('❌ Ошибка: ' + e);
            }
        }
        </script>
    </body>
    </html>
    """
    
    return HTMLResponse(html)


@app.post("/admin/exclusive/delete/{exc_id}")
def admin_delete_exclusive(exc_id: int, request: Request):
    """Удалить эксклюзив"""
    redirect_check = require_admin(request)
    if redirect_check:
        return redirect_check
    
    conn = db()
    conn.execute("DELETE FROM manual_news WHERE id=?", (exc_id,))
    conn.commit()
    conn.close()
    
    return {"ok": True}


@app.post("/admin/exclusive/toggle/{exc_id}")
async def admin_toggle_exclusive(exc_id: int, request: Request):
    """Скрыть/показать эксклюзив"""
    redirect_check = require_admin(request)
    if redirect_check:
        return redirect_check
    
    # Читаем JSON из body
    body = await request.json()
    is_pub = body.get('is_published', 1)
    
    conn = db()
    conn.execute("UPDATE manual_news SET is_published=? WHERE id=?", (is_pub, exc_id))
    conn.commit()
    conn.close()
    
    return {"ok": True}



@app.on_event("startup")
async def on_startup():
    global _scheduler_started

    # 1) Миграции схемы БД (обязательно!)
    try:
        ensure_columns()
    except Exception as e:
        # лучше увидеть ошибку в journalctl, чем молча упасть/сломаться
        print("STARTUP MIGRATION ERROR:", repr(e))
        raise

    # 2) Нормализация старых записей
    try:
        backfill_defaults()
    except Exception as e:
        print("STARTUP BACKFILL ERROR:", repr(e))
        # можно не падать, но я бы пока поднимал ошибку
        raise

    # 3) Защита от двойного старта (reload/несколько воркеров)
    if _scheduler_started:
        return

    def add_once(job_id: str, *args, **kwargs):
        if not scheduler.get_job(job_id):
            scheduler.add_job(*args, id=job_id, replace_existing=True, **kwargs)

    # Steam — каждые STEAM_MIN минут
    add_once(
        "steam_job",
        run_job,
        "interval",
        minutes=STEAM_MIN,
        kwargs={"store": "steam"},
        coalesce=True,
        max_instances=1,
        misfire_grace_time=60 * 10,
    )

    # Epic/GOG/Prime — ежедневно 00:05 по Бишкеку
    daily = CronTrigger(hour=0, minute=5, timezone=BISHKEK_TZ)

    add_once(
        "epic_job",
        run_job,
        trigger=daily,
        kwargs={"store": "epic"},
        coalesce=True,
        max_instances=1,
        misfire_grace_time=60 * 30,
    )

    add_once(
        "gog_job",
        run_job,
        trigger=daily,
        kwargs={"store": "gog"},
        coalesce=True,
        max_instances=1,
        misfire_grace_time=60 * 30,
    )

    add_once(
        "prime_job",
        run_job,
        trigger=daily,
        kwargs={"store": "prime"},
        coalesce=True,
        max_instances=1,
        misfire_grace_time=60 * 30,
    )

    # Чистка раз в сутки
    add_once(
        "cleanup_job",
        cleanup_expired,
        "interval",
        hours=24,
        kwargs={"keep_days": 7},
        coalesce=True,
        max_instances=1,
        misfire_grace_time=60 * 60,
    )

    if not scheduler.running:
        scheduler.start()

    _scheduler_started = True

from pydantic import BaseModel, Field

class VoteIn(BaseModel):
    deal_id: str = Field(min_length=6, max_length=64)
    vote: int  # +1 or -1

from fastapi import Cookie
from fastapi.responses import JSONResponse

@app.post("/api/vote")
def api_vote(payload: VoteIn, request: Request):
    if payload.vote not in (1, -1):
        return JSONResponse({"ok": False, "error": "bad_vote"}, status_code=400)

    conn = db()
    ip = get_client_ip(request) or ""
    ua = request.headers.get("user-agent") or ""

    vid, need_set_cookie = get_or_set_vid(request)
    day = datetime.utcnow().strftime("%Y-%m-%d")

    # 1) лок на сутки по IP (и одновременно можно считать это антиспамом)
    try:
        conn.execute(
            "INSERT INTO vote_locks(deal_id, ip, day) VALUES (?, ?, ?)",
            (payload.deal_id, ip, day),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        # уже голосовал сегодня
        row = conn.execute("""
            SELECT
              SUM(CASE WHEN vote=1 THEN 1 ELSE 0 END) AS up,
              SUM(CASE WHEN vote=-1 THEN 1 ELSE 0 END) AS down
            FROM votes
            WHERE deal_id=?
        """, (payload.deal_id,)).fetchone()
        conn.close()
        resp = JSONResponse({"ok": False, "error": "already_voted", "up": row[0] or 0, "down": row[1] or 0})
        return resp

    # 2) пишем голос
    conn.execute("""
        INSERT INTO votes (created_at, deal_id, vote, ip, user_agent)
        VALUES (?, ?, ?, ?, ?)
    """, (datetime.utcnow().isoformat(), payload.deal_id, int(payload.vote), ip, ua))
    conn.commit()

    row = conn.execute("""
        SELECT
          SUM(CASE WHEN vote=1 THEN 1 ELSE 0 END) AS up,
          SUM(CASE WHEN vote=-1 THEN 1 ELSE 0 END) AS down
        FROM votes
        WHERE deal_id=?
    """, (payload.deal_id,)).fetchone()
    conn.close()

    resp = JSONResponse({"ok": True, "up": row[0] or 0, "down": row[1] or 0})

    if need_set_cookie:
        # на год
        resp.set_cookie(VOTE_COOKIE_NAME, vid, max_age=31536000, httponly=True, samesite="Lax")
    return resp

@app.get("/debug_images")
def debug_images(limit: int = 5):
    """Отладочная информация по изображениям"""
    conn = db()
    
    # Получаем Steam игры
    rows = conn.execute("""
        SELECT id, store, title, url, image_url 
        FROM deals 
        WHERE store='steam'
        ORDER BY created_at DESC 
        LIMIT ?
    """, (limit,)).fetchall()
    
    result = []
    for did, store, title, url, image_url in rows:
        appid = extract_steam_app_id_fast(url)
        
        # Проверяем доступность image_url
        image_ok = False
        if image_url:
            try:
                resp = requests.head(image_url, timeout=3)
                image_ok = resp.status_code == 200
            except:
                pass
        
        # Генерируем кандидаты
        candidates = []
        if appid:
            candidates = [
                f"https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/{appid}/header.jpg",
                f"https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/header.jpg",
                f"https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/capsule_616x353.jpg",
            ]
        
        # Проверяем кандидатов
        working_candidates = []
        for cand in candidates:
            try:
                resp = requests.head(cand, timeout=2)
                if resp.status_code == 200:
                    working_candidates.append(cand)
            except:
                pass
        
        result.append({
            "id": did,
            "store": store,
            "title": title[:50],
            "url": url,
            "appid": appid,
            "image_in_db": image_url,
            "image_ok": image_ok,
            "candidates": candidates,
            "working_candidates": working_candidates,
        })
    
    conn.close()
    
    return {
        "total": len(result),
        "games": result,
        "summary": {
            "with_images": sum(1 for r in result if r["image_in_db"]),
            "images_working": sum(1 for r in result if r["image_ok"]),
            "has_working_candidates": sum(1 for r in result if r["working_candidates"]),
        }
    }

@app.get("/stats_funnel")
def stats_funnel(days: int = 7, top: int = 15):
    if days < 1: days = 1
    if days > 90: days = 90
    if top < 1: top = 1
    if top > 50: top = 50

    conn = db()

    # клики по источникам
    by_src = conn.execute("""
        SELECT COALESCE(src,'') as src, COUNT(*) as cnt
        FROM clicks
        WHERE datetime(created_at) >= datetime('now', ?)
        GROUP BY src
        ORDER BY cnt DESC
    """, (f"-{days} days",)).fetchall()

    # топ по "конверсии": out / tg
    rows = conn.execute("""
        WITH tg AS (
          SELECT deal_id, COUNT(*) cnt
          FROM clicks
          WHERE src='tg' AND datetime(created_at) >= datetime('now', ?)
          GROUP BY deal_id
        ),
        out AS (
          SELECT deal_id, COUNT(*) cnt
          FROM clicks
          WHERE src='out' AND datetime(created_at) >= datetime('now', ?)
          GROUP BY deal_id
        )
        SELECT d.id, d.title, d.store,
               COALESCE(tg.cnt,0) as tg_clicks,
               COALESCE(out.cnt,0) as out_clicks
        FROM deals d
        LEFT JOIN tg ON tg.deal_id = d.id
        LEFT JOIN out ON out.deal_id = d.id
        WHERE COALESCE(tg.cnt,0) > 0
        ORDER BY (1.0*COALESCE(out.cnt,0)/tg.cnt) DESC, tg_clicks DESC
        LIMIT ?
    """, (f"-{days} days", f"-{days} days", top)).fetchall()

    conn.close()

    funnel = [{"src": s, "clicks": c} for s, c in by_src]
    top_conv = []
    for did, title, store, tg_clicks, out_clicks in rows:
        conv = (out_clicks / tg_clicks) if tg_clicks else 0.0
        top_conv.append({
            "deal_id": did,
            "title": title or "(no title)",
            "store": store or "",
            "tg_clicks": tg_clicks,
            "out_clicks": out_clicks,
            "conv_tg_to_store": round(conv, 3),
            "link": f"{SITE_BASE}/d/{did}",
        })

    return {"range_days": days, "by_src": funnel, "top_conversion": top_conv}

@app.get("/stats_tg_formats")
def stats_tg_formats(days: int = 7):
    if days < 1: days = 1
    if days > 90: days = 90

    conn = db()

    rows = conn.execute("""
        WITH tg AS (
          SELECT COALESCE(utm_content,'') as fmt, COUNT(*) cnt
          FROM clicks
          WHERE src='tg' AND datetime(created_at) >= datetime('now', ?)
          GROUP BY fmt
        ),
        out AS (
          SELECT COALESCE(utm_content,'') as fmt, COUNT(*) cnt
          FROM clicks
          WHERE src='out' AND datetime(created_at) >= datetime('now', ?)
          GROUP BY fmt
        )
        SELECT tg.fmt,
               tg.cnt as tg_clicks,
               COALESCE(out.cnt,0) as out_clicks
        FROM tg
        LEFT JOIN out ON out.fmt = tg.fmt
        ORDER BY (1.0*COALESCE(out.cnt,0)/tg.cnt) DESC, tg_clicks DESC
    """, (f"-{days} days", f"-{days} days")).fetchall()

    conn.close()

    items = []
    for fmt, tg_clicks, out_clicks in rows:
        conv = (out_clicks / tg_clicks) if tg_clicks else 0.0
        items.append({
            "utm_content": fmt,
            "tg_clicks": tg_clicks,
            "out_clicks": out_clicks,
            "conv": round(conv, 3)
        })
    return {"range_days": days, "formats": items}

from fastapi.responses import JSONResponse
from datetime import datetime

@app.get("/stats_live")
def stats_live(minutes: int = 60):
    if minutes < 5: minutes = 5
    if minutes > 24*60: minutes = 24*60

    conn = db()

    # кликов за последние N минут
    clicks = conn.execute("""
        SELECT COUNT(*)
        FROM clicks
        WHERE datetime(created_at) >= datetime('now', ?)
    """, (f"-{minutes} minutes",)).fetchone()[0]

    # уникальные IP (не идеал, но достаточно для лайва)
    uniq_ip = conn.execute("""
        SELECT COUNT(DISTINCT ip)
        FROM clicks
        WHERE datetime(created_at) >= datetime('now', ?)
          AND COALESCE(user_agent,'') NOT LIKE '%bot%'
    """, (f"-{minutes} minutes",)).fetchone()[0]

    # последние 10 кликов
    last = conn.execute("""
        SELECT created_at, deal_id, src, utm_content
        FROM clicks
        ORDER BY id DESC
        LIMIT 10
    """).fetchall()

    conn.close()

    return {
        "minutes": minutes,
        "clicks": clicks,
        "uniq_ip": uniq_ip,
        "last": [
            {"at": a, "deal_id": d, "src": s or "", "utm_content": u or ""}
            for a, d, s, u in last
        ],
        "server_utc": datetime.utcnow().isoformat()
    }

from fastapi.responses import HTMLResponse

DASHBOARD_HTML = """
<!doctype html><html><head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>freerg • dashboard</title>
<style>
  body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;margin:24px;background:#0b0f19;color:#e8eefc}
  .grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}
  .card{background:#121a2a;border:1px solid #1f2a44;border-radius:14px;padding:14px}
  .kpi{font-size:28px;font-weight:800}
  .muted{opacity:.75}
  table{width:100%;border-collapse:collapse;margin-top:10px}
  td{padding:8px;border-top:1px solid #1f2a44;font-size:13px}
  @media(max-width:900px){.grid{grid-template-columns:1fr}}
</style>
</head><body>
<h1 style="margin:0 0 10px">freerg • real-time</h1>
<div class="muted" id="meta"></div>

<div class="grid" style="margin-top:12px">
  <div class="card">
    <div class="muted">Clicks (last <span id="mins">60</span> min)</div>
    <div class="kpi" id="clicks">—</div>
  </div>
  <div class="card">
    <div class="muted">Unique IP (no bots)</div>
    <div class="kpi" id="uniq">—</div>
  </div>
  <div class="card">
    <div class="muted">Refresh</div>
    <div class="kpi"><span id="sec">—</span>s</div>
  </div>
</div>

<div class="card" style="margin-top:12px">
  <div class="muted">Last 10 clicks</div>
  <table id="tbl"></table>
</div>

<script>
let t = 10;
async function tick(){
  try{
    const r = await fetch('/stats_live?minutes=60', {cache:'no-store'});
    const j = await r.json();
    document.getElementById('mins').textContent = j.minutes;
    document.getElementById('clicks').textContent = j.clicks;
    document.getElementById('uniq').textContent = j.uniq_ip;
    document.getElementById('meta').textContent = 'server utc: ' + j.server_utc;

    const rows = j.last.map(x => 
      `<tr><td>${(x.at||'').replace('T',' ').slice(0,19)}</td><td>${x.deal_id}</td><td>${x.src}</td><td>${x.utm_content}</td></tr>`
    ).join('');
    document.getElementById('tbl').innerHTML = `<tr><td class="muted">time</td><td class="muted">deal</td><td class="muted">src</td><td class="muted">utm</td></tr>` + rows;
  }catch(e){}
}
function countdown(){
  document.getElementById('sec').textContent = t;
  t--;
  if(t < 0){ t = 10; tick(); }
}
tick(); setInterval(countdown, 1000);
</script>
</body></html>
"""

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    return DASHBOARD_HTML

from fastapi.responses import RedirectResponse

@app.get("/out/{deal_id}")
def out(deal_id: str, request: Request):
    conn = db()

    row = conn.execute("SELECT url FROM deals WHERE id=? LIMIT 1", (deal_id,)).fetchone()
    out_url = row[0] if row and row[0] else "/"

    resp = RedirectResponse(url=out_url, status_code=302)
    vid = get_or_set_vid(request, resp)
    log_click(conn, deal_id, request, src="out", visitor_id=vid)

    conn.close()
    return resp


from fastapi.responses import JSONResponse

@app.get("/manifest.json")
def manifest():
    return JSONResponse({
        "name": "FreeRedeemGames",
        "short_name": "freerg",
        "start_url": "/?src=pwa",
        "display": "standalone",
        "background_color": "#0b0f19",
        "theme_color": "#0b0f19",
        "icons": [
            {"src": "/static/icons/icon-192.png", "sizes": "192x192", "type": "image/png"},
            {"src": "/static/icons/icon-512.png", "sizes": "512x512", "type": "image/png"}
        ]
    })

from fastapi.responses import Response

SW_JS = r"""
const CACHE = 'freerg-v1';
const CORE = ['/', '/manifest.json'];

self.addEventListener('install', (e) => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(CORE)));
  self.skipWaiting();
});

self.addEventListener('activate', (e) => {
  e.waitUntil(self.clients.claim());
});

self.addEventListener('fetch', (e) => {
  const req = e.request;
  const url = new URL(req.url);

  // только свой домен
  if (url.origin !== location.origin) return;

  // для html: network-first (чтобы обновлялось)
  if (req.headers.get('accept')?.includes('text/html')) {
    e.respondWith(
      fetch(req).then(res => {
        const copy = res.clone();
        caches.open(CACHE).then(c => c.put(req, copy));
        return res;
      }).catch(() => caches.match(req).then(r => r || caches.match('/')))
    );
    return;
  }

  // для статики: cache-first
  e.respondWith(
    caches.match(req).then(cached => cached || fetch(req).then(res => {
      const copy = res.clone();
      caches.open(CACHE).then(c => c.put(req, copy));
      return res;
    }))
  );
});
"""

@app.get("/service-worker.js")
def service_worker():
    return Response(SW_JS, media_type="application/javascript")



@app.get("/debug_hot")
def debug_hot():
    conn = db()
    total = conn.execute("SELECT COUNT(*) FROM deals WHERE kind='hot_deal'").fetchone()[0]
    sample = conn.execute("SELECT id, store, title, discount_pct FROM deals WHERE kind='hot_deal' LIMIT 5").fetchall()
    conn.close()
    return {"total_hot_deal": total, "sample": sample}

@app.on_event("shutdown")
async def on_shutdown():
    try:
        if scheduler.running:
            scheduler.shutdown(wait=False)
    except Exception:
        pass