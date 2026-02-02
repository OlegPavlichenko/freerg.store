import os
import re
import sqlite3
import hashlib
import asyncio
import requests

from datetime import datetime, timezone as dt_timezone, timedelta
from zoneinfo import ZoneInfo
from apscheduler.triggers.cron import CronTrigger

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
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

# расписания (аккуратно)
STEAM_MIN = int(os.getenv("STEAM_MIN", "180"))     # Steam/ITAD раз в 60 минут
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

# --------------------
# DB helpers
# --------------------
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA busy_timeout=5000;")

    conn.execute("""
      CREATE TABLE IF NOT EXISTS deals (
        id TEXT PRIMARY KEY,
        title TEXT,
        url TEXT,
        source TEXT,
        starts_at TEXT,
        ends_at TEXT,
        posted INTEGER DEFAULT 0,
        created_at TEXT
      )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_deals_posted ON deals(posted)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_deals_created ON deals(created_at)")
    
    # ✅ добавь это:
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
      )
    """)

    return conn



def ensure_columns():
    """
    Миграция: добавляем колонки для мульти-магазинов и категорий.
    Потом создаём индексы по этим колонкам (когда они точно есть).
    """
    conn = db()
    cols = {row[1] for row in conn.execute("PRAGMA table_info(deals)").fetchall()}

    def add(col_def: str):
        conn.execute(f"ALTER TABLE deals ADD COLUMN {col_def}")

    if "store" not in cols:
        add("store TEXT")
    if "external_id" not in cols:
        add("external_id TEXT")
    if "kind" not in cols:
        add("kind TEXT")  # free_to_keep / free_weekend / ...
    if "image_url" not in cols:
        add("image_url TEXT")

    if "discount_pct" not in cols:
        add("discount_pct INTEGER")
    if "price_old" not in cols:
        add("price_old REAL")
    if "price_new" not in cols:
        add("price_new REAL")
    if "currency" not in cols:
        add("currency TEXT")

    # индексы на новые колонки — только после миграции
    conn.execute("CREATE INDEX IF NOT EXISTS idx_deals_store ON deals(store)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_deals_kind ON deals(kind)")

    conn.commit()
    conn.close()


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
    return dt >= (datetime.now(dt_timezone.utc) - timedelta(hours=hours))


def time_left_label(ends_at: str | None) -> str | None:
    dt = parse_iso_utc(ends_at)
    if not dt:
        return None
    now = datetime.now(dt_timezone.utc)
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
    return dt >= (datetime.now(dt_timezone.utc) - timedelta(hours=hours))


def is_expired_recent(ends_at: str | None, days: int = 7) -> bool:
    dt = parse_iso_utc(ends_at)
    if not dt:
        return False
    now = datetime.now(dt_timezone.utc)
    return (dt <= now) and (dt >= now - timedelta(days=days))


def cleanup_expired(keep_days: int = 7) -> int:
    """
    Удаляем записи, у которых ends_at прошло больше, чем keep_days назад.
    keep_days=7 => неделю храним, потом чистим.
    Возвращает количество удалённых.
    """
    cutoff = datetime.now(dt_timezone.utc) - timedelta(days=keep_days)

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
        steam_url = itad_url  # по умолчанию
        try:
            if "itad.link" in itad_url:
                resp = requests.head(itad_url, timeout=5, allow_redirects=True)
                steam_url = str(resp.url)
                print(f"  🔄 Редирект: {itad_url[:50]}... -> {steam_url[:60]}...")
        except Exception as e:
            print(f"  ⚠️  Не удалось получить конечный URL для {itad_url}: {e}")

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

def fetch_itad_steam_hot_deals(min_cut: int = 70, limit: int = 200, keep: int = 30):
    """
    Steam hot deals через ITAD deals/v2.
    - Пытаемся набрать keep штук с порогом скидки min_cut (по умолчанию 70%).
    - Если набралось мало — автоматически пробуем 60%, затем 50%.
    - Парсим изображения прямо со страниц Steam (до 10 игр).
    """
    if not ITAD_API_KEY:
        return []

    endpoint = "https://api.isthereanydeal.com/deals/v2"
    params = {
        "key": ITAD_API_KEY,
        "shops": "61",          # Steam
        "limit": str(limit),    # сколько тянуть из API
        "sort": "-cut",
    }

    r = requests.get(endpoint, params=params, timeout=25)
    r.raise_for_status()
    data = r.json()

    items = data if isinstance(data, list) else (
        data.get("list") or data.get("data") or data.get("items") or data.get("result") or []
    )

    # Пороги: сначала 70, если мало — 60, потом 50
    thresholds = [min_cut]
    if min_cut > 60:
        thresholds.append(60)
    if min_cut > 50:
        thresholds.append(50)

    out: list[dict] = []
    seen_urls = set()

    slow_left = 40  # редиректы для получения app_id
    scrape_left = 10  # парсинг страниц для получения изображений

    def add_item(it: dict, deal: dict, cut: int, url: str) -> None:
        nonlocal slow_left, scrape_left, out, seen_urls

        title = it.get("title") or it.get("name") or deal.get("title") or deal.get("name") or "Steam deal"

        expiry = deal.get("expiry") or it.get("expiry")
        start = deal.get("start") or it.get("start")

        price_obj = deal.get("price") or {}
        price_amount = price_obj.get("amount") if isinstance(price_obj, dict) else None
        currency = price_obj.get("currency") if isinstance(price_obj, dict) else None

        regular_obj = deal.get("regular") or deal.get("regularPrice") or deal.get("regular_price") or {}
        old_amount = regular_obj.get("amount") if isinstance(regular_obj, dict) else None

        # appid: сначала быстрый парсинг
        app_id = extract_steam_app_id_fast(url)

        # если не нашли — пробуем редиректами
        if not app_id and slow_left > 0:
            slow_left -= 1
            try:
                app_id = resolve_steam_app_id_slow(url)
            except Exception:
                pass

        # дополнительная попытка: извлечь из deal.id
        if not app_id:
            deal_id_field = deal.get("id") or it.get("id") or ""
            if isinstance(deal_id_field, str) and deal_id_field.isdigit():
                app_id = deal_id_field

        app_id = app_id or ""
        
        # 🔥 ГЛАВНОЕ: парсим изображения со страницы Steam
        image_url = None
        if app_id and scrape_left > 0:
            scrape_left -= 1
            try:
                images = get_steam_images_from_page(app_id, url)
                # Приоритет: header > hero > capsule > library
                image_url = (
                    images.get('header') or 
                    images.get('hero') or 
                    images.get('capsule') or 
                    images.get('library')
                )
            except Exception as e:
                print(f"Scrape error for {app_id}: {e}")
        
        # Фоллбэк на стандартные URL если парсинг не сработал
        if not image_url and app_id:
            cands = steam_header_candidates(app_id)
            # Пробуем найти работающий URL
            for cand in cands:
                try:
                    resp = requests.head(cand, timeout=2)
                    if resp.status_code == 200:
                        image_url = cand
                        break
                except:
                    continue

        out.append({
            "store": "steam",
            "external_id": app_id,
            "kind": "hot_deal",
            "title": title,
            "url": url,
            "image_url": image_url,
            "source": "itad",
            "starts_at": start,
            "ends_at": expiry,
            "discount_pct": int(cut),
            "price_old": old_amount,
            "price_new": price_amount,
            "currency": currency,
        })
        seen_urls.add(url)

    # Проходим по порогам, пока не наберём keep
    for thr in thresholds:
        for it in items:
            if len(out) >= keep:
                break
            if not isinstance(it, dict):
                continue

            deal = it.get("deal") if isinstance(it.get("deal"), dict) else it
            cut = deal.get("cut")
            if cut is None or cut < thr:
                continue

            # не берём бесплатные, чтобы не дублировать free_to_keep
            price_obj = deal.get("price") or {}
            price_amount = price_obj.get("amount") if isinstance(price_obj, dict) else None
            if cut == 100 or price_amount == 0:
                continue

            url = deal.get("url") or it.get("url")
            if not url or url in seen_urls:
                continue

            add_item(it, deal, int(cut), url)

        if len(out) >= keep:
            break

    return out

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

    now = datetime.now(dt_timezone.utc)

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
        page_url = epic_product_url(e, locale)
        if re.search(r"/p/[^/]+$", page_url):   # очень часто короткий slug заканчивается сразу
          page_url = epic_canonicalize(page_url)
          print("EPIC URL:", page_url)

        img = None
        for ki in (e.get("keyImages") or []):
            if isinstance(ki, dict) and ki.get("url"):
                img = ki["url"]
                break

        start = active.get("startDate")
        end = active.get("endDate")

        # определяем free_to_keep: чаще всего discountPrice==0
        price = (((e.get("price") or {}).get("totalPrice")) or {})
        discount_price = price.get("discountPrice")

        kind = "free_to_keep" if discount_price == 0 else "free_weekend"

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
        })

    return out


# --------------------
# SAVE + POST
# --------------------
def save_deals(deals: list[dict]):
    conn = db()
    now = datetime.now(dt_timezone.utc).isoformat()

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
            button_text = "✅ Забрать навсегда"
        elif kind == "free_weekend":
            header = "⏱ *Free Weekend (временно)*"
            button_text = "🎮 Играть бесплатно"
        else:
            header = "🎮 *Акция*"
            button_text = "🎮 Открыть"

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

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(button_text, url=url)]
        ])

        # выбор картинки
        photo = None
        if st == "epic" and image_url:
            photo = image_url
        elif st == "steam":
            photo = steam_header_image_from_url(url)

        try:
            if photo:
                await bot.send_photo(
                    chat_id=TG_CHAT_ID,
                    photo=photo,
                    caption=text,
                    parse_mode="Markdown",
                    reply_markup=kb,
                )
            else:
                await bot.send_message(
                    chat_id=TG_CHAT_ID,
                    text=text + f"\n\n{url}",
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
                deals = fetch_itad_steam() + fetch_itad_steam_hot_deals(70)
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
            padding-top: 190px; /* 🔥 Увеличил отступ чтобы заголовки не налезали */
            background-image: 
                radial-gradient(circle at 20% 10%, rgba(102, 126, 234, 0.08) 0%, transparent 50%),
                radial-gradient(circle at 80% 90%, rgba(118, 75, 162, 0.08) 0%, transparent 50%);
        }
                
        .collapse-btn{
        margin-top:10px;
        padding:8px 12px;
        border-radius:10px;
        border:1px solid var(--border);
        background: rgba(255,255,255,.06);
        color: var(--text-primary);
        font-weight:700;
        }

        .header.collapsed .filters{ display:none; }
        .header.collapsed .brand p{ display:none; } /* опционально */

        
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
        }
        
        .header-content {
            max-width: 1200px;
            margin: 0 auto;
            padding: 16px 20px;
            text-align: center;
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
            body {
                padding-top: 310px; /* Больше отступ для мобилки */
            }
            
            .header-content {
                padding: 12px 16px;
            }
                
            body {
                transition: padding-top .22s ease
                }
                
            .header {
                transition: transform .22s ease;
                will-change: transform;
            }
                
            .header.hidden {
                transform: translateY(-100%);
            }
            
            .brand h1 {
                font-size: 1.5rem;
            }
            
            .brand p {
                font-size: 0.8rem;
            }
            
            .filters {
                gap: 6px;
            }
            
            .filter-group {
                flex-wrap: wrap;
                justify-content: center;
            }
            
            .filter-btn {
                padding: 6px 12px;
                font-size: 0.8rem;
            }
            
            .games-grid {
                grid-template-columns: repeat(2, 1fr);
                gap: 12px;
            }
            
            .game-image-container {
                height: 110px;
            }
            
            .game-content {
                padding: 12px;
            }
            
            .game-title {
                font-size: 0.95rem;
            }
            
            .section-title {
                font-size: 1.25rem;
            }
            
            .container {
                padding: 16px 12px;
            }
            
            /* Кнопка наверх на мобилке */
            .scroll-to-top {
                width: 45px;
                height: 45px;
                bottom: 20px;
                right: 20px;
                font-size: 1.3rem;
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
    </style>
</head>
<body>
    <!-- ШАПКА -->
    <div class="header">
        <div class="header-content">
            <div class="brand">
                <h1>🎮 Free Redeem Games Store</h1>
                <p>Актуальные бесплатные игры и скидки</p>
            </div>
                <div class="header-divider">
  <button class="collapse-btn" id="collapseBtn" type="button">Свернуть ▲</button>
            
            <div class="filters">
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
    </div>
    
    <!-- 🚀 КНОПКА НАВЕРХ -->
    <button class="scroll-to-top" id="scrollToTop" onclick="scrollToTop()">
        ↑
    </button>
    
    <div class="container">
        {% if kind in ["all", "keep"] and keep|length > 0 %}
        <div class="section">
            <div class="section-header">
                <span class="section-icon">🎁</span>
                <h2 class="section-title">Бесплатно навсегда</h2>
                <span class="section-count">{{ keep|length }}</span>
            </div>
            
            <div class="games-grid">
                {% for game in keep %}
                <div class="game-card">
                    <div class="game-image-container">
                        <div class="store-badge store-{{ game.store }}">
                            {% if game.store == 'steam' %}STEAM
                            {% elif game.store == 'epic' %}EPIC
                            {% elif game.store == 'gog' %}GOG
                            {% elif game.store == 'prime' %}PRIME
                            {% else %}{{ game.store|upper }}{% endif %}
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
                        
                        <a href="{{ game.url }}" target="_blank" class="btn">
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
                            {{ game.store|upper }}{% endif %}
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
                        
                        <a href="{{ game.url }}" target="_blank" class="btn">
                            Играть →
                        </a>
                    </div>
                </div>
                {% endfor %}
            </div>
        </div>
        {% endif %}
        
        {% if kind in ["all", "deals"] and hot|length > 0 %}
        <div class="section">
            <div class="section-header">
                <span class="section-icon">💸</span>
                <h2 class="section-title">Hot Deals 70%+</h2>
                <span class="section-count">{{ hot|length }}</span>
            </div>
            
            <div class="games-grid">
                {% for game in hot %}
                <div class="game-card">
                    <div class="game-image-container">
                        <div class="store-badge store-{{ game.store }}">
                            {% if game.store == 'steam' %}STEAM
                            {% elif game.store == 'epic' %}EPIC
                            {% elif game.store == 'gog' %}GOG
                            {% elif game.store == 'prime' %}PRIME            
                            {{ game.store|upper }}{% endif %}
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
                            {% if game.discount_pct %}
                            <span class="meta-tag tag-discount">-{{ game.discount_pct }}%</span>
                            {% endif %}
                            {% if game.is_new %}<span class="meta-tag tag-new">NEW</span>{% endif %}
                        </div>
                        
                        {% if game.ends_at_fmt and not game.expired %}
                        <div class="game-timer">
                            ⏳ До: <span class="timer-time">{{ game.ends_at_fmt }}</span>
                        </div>
                        {% endif %}
                        
                        <a href="{{ game.url }}" target="_blank" class="btn">
                            Купить →
                        </a>
                    </div>
                </div>
                {% endfor %}
            </div>
        </div>
        {% endif %}
        
        {% if kind in ["all", "free"] and free_games is defined and free_games|length > 0 %}
        <div class="section">
            <div class="section-header">
                <span class="section-icon">🔥</span>
                <h2 class="section-title">Бесплатные игры</h2>
                <span class="section-count">{{ free_games|length }}</span>
            </div>
            
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
                        
                        <a href="{{ game.url }}" target="_blank" class="btn">
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
  if(!header) return;

  let lastY = window.scrollY;
  let ticking = false;
  let headerHeight = header.offsetHeight;

  // 🔥 ставим корректный padding-top
  function syncPadding(){
    headerHeight = header.offsetHeight;
    document.body.style.paddingTop = headerHeight + "px";
  }

  syncPadding();
  window.addEventListener("resize", syncPadding);

  function onScroll(){
    const y = window.scrollY;

    // вверху всегда показываем
    if (y < 30){
      header.classList.remove("hidden");
      document.body.style.paddingTop = headerHeight + "px";
      lastY = y;
      return;
    }

    // вниз — прячем
    if (y > lastY + 8){
      header.classList.add("hidden");
      document.body.style.paddingTop = "0px";
    }
    // вверх — показываем
    else if (y < lastY - 8){
      header.classList.remove("hidden");
      document.body.style.paddingTop = headerHeight + "px";
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
})();
</script>

    <script>
(function(){
  const btn = document.getElementById("collapseBtn");
  const header = document.querySelector(".header");
  if(!btn || !header) return;

  btn.addEventListener("click", () => {
    header.classList.toggle("collapsed");
    btn.textContent = header.classList.contains("collapsed") ? "Фильтры ▼" : "Свернуть ▲";
  });
})();
    </script>
</body>
</html>
""")

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
def index(show_expired: int = 0, store: str = "all", kind: str = "all"):
    conn = db()

    # нормализуем параметры
    store = (store or "all").strip().lower()
    if store not in {"all", "steam", "epic", "gog", "prime"}:
        store = "all"

    kind = (kind or "all").strip().lower()
    if kind not in {"all", "keep", "weekend", "free", "deals"}:
        kind = "all"

    keep_rows = conn.execute("""
        SELECT store,title,url,image_url,ends_at,created_at
        FROM deals
        WHERE kind='free_to_keep'
        ORDER BY created_at DESC
        LIMIT 150
    """).fetchall()

    weekend_rows = conn.execute("""
        SELECT store,title,url,image_url,ends_at,created_at
        FROM deals
        WHERE kind='free_weekend'
        ORDER BY created_at DESC
        LIMIT 150
    """).fetchall()

    hot_rows = conn.execute("""
        SELECT store,title,url,image_url,ends_at,created_at,discount_pct,price_old,price_new,currency
        FROM deals
        WHERE kind='hot_deal'
        ORDER BY RANDOM()
        LIMIT 16
    """).fetchall()

    free_games_rows = conn.execute("""
        SELECT store,title,url,image_url,note
        FROM free_games
        ORDER BY sort ASC, created_at DESC
        LIMIT 24
    """).fetchall()

    conn.close()

    def allow_time(ends_at: str | None) -> bool:
        if is_active_end(ends_at):
            return True
        return bool(show_expired) and is_expired_recent(ends_at, days=7)

    def allow_store(row_store: str | None) -> bool:
        if store == "all":
            return True
        return (row_store or "").strip().lower() == store

    # keep
    keep = []
    for r in keep_rows:
        if not (allow_time(r[4]) and allow_store(r[0])):
            continue
        img_main, img_fb = images_for_row(r[0], r[2], r[3])

        keep.append({
            "store": (r[0] or "").strip().lower(),
            "store_badge": store_badge(r[0]),
            "title": r[1],
            "url": r[2],
            "image": img_main,
            "image_fallback": img_fb,
            "ends_at": r[4],
            "is_new": is_new(r[5]),
            "ends_at_fmt": format_expiry(r[4]),
            "created_at": r[5],
            "expired": not is_active_end(r[4]),
            "time_left": time_left_label(r[4]),
        })

    # weekend
    weekend = []
    for r in weekend_rows:
        if not (allow_time(r[4]) and allow_store(r[0])):
            continue
        img_main, img_fb = images_for_row(r[0], r[2], r[3])

        weekend.append({
            "store": (r[0] or "").strip().lower(),
            "store_badge": store_badge(r[0]),
            "title": r[1],
            "url": r[2],
            "image": img_main,
            "image_fallback": img_fb,
            "ends_at": r[4],
            "is_new": is_new(r[5]),
            "ends_at_fmt": format_expiry(r[4]),
            "created_at": r[5],
            "expired": not is_active_end(r[4]),
            "time_left": time_left_label(r[4]),
        })

    # hot (по магазину фильтруем, по времени можно НЕ фильтровать)
    hot = []
    for r in hot_rows:
        if not allow_store(r[0]):
            continue
        img_main, img_fb = images_for_row(r[0], r[2], r[3])

        hot.append({
            "store": (r[0] or "").strip().lower(),
            "store_badge": store_badge(r[0]),
            "title": r[1],
            "url": r[2],
            "image": img_main,
            "image_fallback": img_fb,
            "ends_at": r[4],
            "is_new": is_new(r[5]),
            "ends_at_fmt": format_expiry(r[4]),
            "created_at": r[5],
            "expired": not is_active_end(r[4]),
            "time_left": time_left_label(r[4]),
            "discount_pct": r[6],
            "price_old": r[7],
            "price_new": r[8],
            "currency": r[9],
        })

    keep.sort(key=lambda d: sort_key_by_ends(d["ends_at"]))
    weekend.sort(key=lambda d: sort_key_by_ends(d["ends_at"]))
    hot.sort(key=lambda d: sort_key_by_ends(d["ends_at"]))

    # free_games
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
        })

# Подсчитываем статистику
    # Подсчитываем статистику
    total_games = len(keep) + len(weekend) + len(hot)
    new_today = sum(1 for g in (keep + weekend + hot) if g.get("is_new"))
    expiring_soon = sum(1 for g in (keep + weekend) if g.get("time_left") and "час" in g.get("time_left", ""))
    last_update = datetime.now().strftime("%d.%m.%Y %H:%M")

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
)

# --------------------
# API endpoints
# --------------------
@app.api_route("/health", methods=["GET", "HEAD"])
def health():
    return {"ok": True}


@app.get("/debug_tg")
def debug_tg():
    return {"bot_token_present": bool(TG_BOT_TOKEN), "chat_id": TG_CHAT_ID}


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


@app.on_event("startup")
async def on_startup():
    global _scheduler_started

    # 1) миграция БД
    ensure_columns()
    backfill_defaults()

    # 2) startup в проде может вызываться повторно (и при reload тоже)
    if _scheduler_started:
        return

    if not scheduler.get_job("steam_job"):
        scheduler.add_job(
            run_job,
            "interval",
            minutes=STEAM_MIN,
            id="steam_job",
            replace_existing=True,
            kwargs={"store": "steam"},
        )

    if not scheduler.get_job("epic_job"):
        scheduler.add_job(
            run_job,
            trigger=CronTrigger(hour=0, minute=5, timezone=BISHKEK_TZ),
            id="epic_job",
            replace_existing=True,
            kwargs={"store": "epic"},
        )

    if not scheduler.get_job("gog_job"):
        scheduler.add_job(
            run_job,
            trigger=CronTrigger(
            hour=0,
            minute=5,
            timezone=BISHKEK_TZ_APS
            ),
            id="gog_job",
            replace_existing=True,
            kwargs={"store": "gog"},
        )

    if not scheduler.get_job("prime_job"):
        scheduler.add_job(
            run_job,
            trigger=CronTrigger(
            hour=0,
            minute=5,
            timezone=BISHKEK_TZ_APS
            ),
            id="prime_job",
            replace_existing=True,
            kwargs={"store": "prime"},
        )

    if not scheduler.get_job("cleanup_job"):
        scheduler.add_job(
            cleanup_expired,
            "interval",
            hours=24,
            id="cleanup_job",
            replace_existing=True,
            kwargs={"keep_days": 7},
        )

    if not scheduler.running:
        scheduler.start()

    _scheduler_started = True

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

@app.on_event("shutdown")
async def on_shutdown():
    try:
        if scheduler.running:
            scheduler.shutdown(wait=False)
    except Exception:
        pass