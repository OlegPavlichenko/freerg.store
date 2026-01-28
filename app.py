import os
import re
import sqlite3
import hashlib
import asyncio
import requests
from datetime import datetime, timezone, timedelta

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
BISHKEK_TZ = timezone(timedelta(hours=6))
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
    base = f"{store}|{external_id}|{url}"
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


# --------------------
# Steam image helpers
# --------------------
def extract_steam_app_id_fast(url: str) -> str | None:
    """Извлекает app_id из URL Steam без HTTP-запросов"""
    if not url:
        return None
    m = re.search(r"store\.steampowered\.com/app/(\d+)", url)
    if m:
        return m.group(1)
    m = re.search(r"/app/(\d+)", url)
    if m:
        return m.group(1)
    return None

def steam_header_image_from_url_fast(url: str) -> str | None:
    app_id = extract_steam_app_id_fast(url)
    if not app_id:
        return None
    return f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/header.jpg"

def steam_image_candidates(appid: str) -> list[str]:
    if not appid:
        return []
    return [
        f"https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/header.jpg",
        f"https://cdn.cloudflare.steamstatic.com/steam/apps/{appid}/library_600x900.jpg",
        f"https://shared.akamai.steamstatic.com/steam/apps/{appid}/header.jpg",
        f"https://shared.fastly.steamstatic.com/steam/apps/{appid}/header.jpg",
    ]

def steam_header_candidates(appid: str) -> list[str]:
    # совместимость со старым кодом
    return steam_image_candidates(appid)


def images_for_row(row_store: str | None, url: str, image_url: str | None):
    st = (row_store or "").strip().lower()

    # 1) если картинка уже есть в БД (Epic / или Steam если сохранили) — используем её
    if image_url:
        return image_url, "" , ""  # main, fb1, fb2

    # 2) Steam — строим кандидаты
    if st == "steam":
        appid = extract_steam_app_id_fast(url)
        c = steam_image_candidates(appid) if appid else []
        main = c[0] if len(c) > 0 else ""
        fb1  = c[1] if len(c) > 1 else ""
        fb2  = c[2] if len(c) > 2 else ""
        return main, fb1, fb2

    return "", "", ""


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
    Использовать ТОЛЬКО в update job (fetch_*), НЕ в рендере страниц.
    """
    try:
        resp = requests.get(url, timeout=10, allow_redirects=True, headers={"User-Agent": "Mozilla/5.0"})
        return extract_steam_app_id_fast(str(resp.url))
    except Exception:
        return None


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
    Фильтр: cut==100 или price.amount==0.
    Картинка:
      - если appid нашли -> берём более "новый" header (akamai store_item_assets)
      - если appid не нашли -> пробуем добыть через редирект, но не больше slow_limit раз за запуск
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

    resolved_slow = 0
    out: list[dict] = []

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

        url = deal.get("url") or it.get("url")
        if not url:
            continue

        expiry = deal.get("expiry") or it.get("expiry")
        start = deal.get("start") or it.get("start")

        # appid: быстрый парсинг, иначе ограниченно через редиректы
        app_id = extract_steam_app_id_fast(url) or ""
        if not app_id and resolved_slow < slow_limit:
            resolved_slow += 1
            app_id = resolve_steam_app_id_limited(url, allow_slow=True) or ""

        # картинка: предпочтительно akamai store_item_assets
        image_url = None
        if app_id:
            cands = steam_header_candidates(app_id)
            image_url = cands[1] if len(cands) > 1 else (cands[0] if cands else None)

        out.append({
            "store": "steam",
            "external_id": app_id,
            "kind": "free_to_keep",
            "title": title,
            "url": url,
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
    - Увеличен лимит редиректов до 40 для лучшего покрытия обложек.
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

    slow_left = 40  # ⭐ увеличен лимит для лучшего покрытия

    def add_item(it: dict, deal: dict, cut: int, url: str) -> None:
        nonlocal slow_left, out, seen_urls

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

        # ⭐ дополнительная попытка: извлечь из deal.id, если это число
        if not app_id:
            deal_id_field = deal.get("id") or it.get("id") or ""
            if isinstance(deal_id_field, str) and deal_id_field.isdigit():
                app_id = deal_id_field

        app_id = app_id or ""
        
        # формируем картинку
        cands = steam_header_candidates(app_id) if app_id else []
        image_url = cands[1] if len(cands) > 1 else (cands[0] if cands else None)

        out.append({
            "store": "steam",
            "external_id": app_id,
            "kind": "hot_deal",
            "title": title,
            "url": url,
            "image_url": image_url,  # ⭐ сохраняем в БД
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
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>FreeRedeemGames</title>
  <link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'%3E%3Crect width='64' height='64' rx='14' fill='%23101a33'/%3E%3Ctext x='50%25' y='56%25' font-size='34' text-anchor='middle'%3E%F0%9F%8E%AE%3C/text%3E%3C/svg%3E">
  <style>
    :root{
      --bg:#0b1020;
      --panel:#101a33;
      --panel2:#0f1730;
      --text:#e7ecff;
      --muted:#a9b4dd;
      --line:rgba(255,255,255,.10);
      --chip:rgba(255,255,255,.08);
      --ok:#2dd4bf;
      --warn:#fbbf24;
      --bad:#fb7185;
      --shadow: 0 12px 30px rgba(0,0,0,.35);
      --radius:16px;
    }
    *{box-sizing:border-box}
    body{
      margin:0;
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
      background: radial-gradient(1200px 700px at 20% 0%, #182a5a 0%, var(--bg) 55%) fixed;
      color:var(--text);
    }
    a{color:inherit; text-decoration:none}
    .wrap{max-width:1100px; margin:28px auto; padding:0 16px;}
    .top{
      display:flex; gap:14px; align-items:flex-start; justify-content:space-between; flex-wrap:wrap;
      margin-bottom:14px;
    }
    .brand{
      background: linear-gradient(180deg, rgba(255,255,255,.07), rgba(255,255,255,.03));
      border:1px solid var(--line);
      border-radius: var(--radius);
      padding:14px 16px;
      box-shadow: var(--shadow);
      flex: 1 1 520px;
    }
    .brand h1{margin:0 0 6px 0; font-size:22px; letter-spacing:.2px}
    .brand p{margin:0; color:var(--muted); font-size:13px; line-height:1.4}
    .controls{
      flex: 0 0 auto;
      display:flex; gap:10px; flex-wrap:wrap;
      align-items:center; justify-content:flex-end;
    }

    .seg{
      background: rgba(255,255,255,.06);
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 6px;
      display:flex;
      gap:6px;
      box-shadow: var(--shadow);
    }
    .seg a, .seg span{
      padding:8px 12px;
      border-radius:999px;
      font-size:13px;
      color:var(--muted);
      display:inline-flex; align-items:center; gap:8px;
      border:1px solid transparent;
      white-space:nowrap;
    }
    .seg .on{
      background: rgba(45,212,191,.18);
      color:var(--text);
      border-color: rgba(45,212,191,.35);
    }
    .seg a:hover{background: rgba(255,255,255,.08); color:var(--text)}
    .chips{
      display:flex; flex-wrap:wrap; gap:8px; margin:14px 0 16px;
    }
    .chip{
      display:inline-flex; align-items:center; gap:8px;
      background: var(--chip);
      border:1px solid var(--line);
      padding:8px 10px;
      border-radius:999px;
      font-size:13px;
      color:var(--muted);
    }
    .chip strong{color:var(--text); font-weight:600}
    .grid{
      display:grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 14px;
    }
    @media (max-width: 980px){
      .grid{grid-template-columns: repeat(2, 1fr);}
    }
    @media (max-width: 640px){
      .grid{grid-template-columns: 1fr;}
    }

    .card{
      background: linear-gradient(180deg, rgba(255,255,255,.06), rgba(255,255,255,.03));
      border:1px solid var(--line);
      border-radius: var(--radius);
      overflow:hidden;
      box-shadow: var(--shadow);
      transition: transform .15s ease, border-color .15s ease;
    }
    .card:hover{transform: translateY(-2px); border-color: rgba(255,255,255,.18)}
    .thumb{
      height: 140px;
      background: rgba(255,255,255,.05);
      border-bottom:1px solid var(--line);
      display:flex; align-items:center; justify-content:center;
      overflow:hidden;
    }
    .thumb img{width:100%; height:100%; object-fit:cover; display:block}
    .thumb .ph{
      color: rgba(255,255,255,.35);
      font-size:12px;
      padding:12px;
      text-align:center;
    }
    .body{padding:12px 12px 14px}
    .row1{display:flex; align-items:center; justify-content:space-between; gap:10px; margin-bottom:8px;}
    .badge{
      display:inline-flex; align-items:center; gap:8px;
      padding:6px 10px;
      border-radius:999px;
      font-size:12px;
      background: rgba(255,255,255,.08);
      border:1px solid var(--line);
      color: var(--text);
      white-space:nowrap;
    }
    .meta{
      font-size:12px;
      color:var(--muted);
      white-space:nowrap;
    }
    .title{
      font-size:14px;
      font-weight:650;
      line-height:1.25;
      margin:0 0 10px 0;
      min-height: 36px;
    }
    .actions{
      display:flex; gap:8px; flex-wrap:wrap;
    }
    .btn{
      display:inline-flex; align-items:center; justify-content:center;
      padding:10px 12px;
      border-radius: 12px;
      border:1px solid var(--line);
      background: rgba(255,255,255,.06);
      color: var(--text);
      font-size:13px;
      cursor:pointer;
    }
    .btn:hover{background: rgba(255,255,255,.10)}
    .btn.primary{
      background: rgba(45,212,191,.18);
      border-color: rgba(45,212,191,.35);
    }
    .btn.primary:hover{background: rgba(45,212,191,.24)}
    .pill{
      display:inline-flex; align-items:center;
      padding:6px 10px;
      border-radius:999px;
      font-size:12px;
      border:1px solid var(--line);
      background: rgba(255,255,255,.06);
      color:var(--muted);
    }
    .pill.ok{color:var(--ok); border-color: rgba(45,212,191,.35); background: rgba(45,212,191,.12);}
    .pill.exp{color:var(--bad); border-color: rgba(251,113,133,.35); background: rgba(251,113,133,.10);}
    .section{
      margin-top:18px;
    }
    .section h2{
      margin: 18px 0 10px;
      font-size: 16px;
      color: var(--text);
      letter-spacing:.2px;
    }
    .empty{
      color: var(--muted);
      background: rgba(255,255,255,.05);
      border: 1px dashed rgba(255,255,255,.18);
      border-radius: var(--radius);
      padding: 14px;
    }
     .toast{
  position: fixed;
  left: 50%;
  bottom: 18px;
  transform: translateX(-50%);
  background: rgba(16,26,51,.95);
  border: 1px solid var(--line);
  padding: 10px 14px;
  border-radius: 12px;
  box-shadow: var(--shadow);
  color: var(--text);
  font-size: 13px;
  opacity: 0;
  pointer-events: none;
  transition: opacity .18s ease, transform .18s ease;
}
.toast.on{
  opacity: 1;
  transform: translateX(-50%) translateY(-2px);
}
button.btn{font-family: inherit}

  </style>
</head>

<body>
  <div class="wrap">
    <div class="top">
      <div class="brand">
        <h1>FreeRedeemGames</h1>
        <p>
          Автоматическая лента раздач: 🎮 Steam · 🟦 Epic · 🟪 GOG · 🟨 Prime.
          Steam ~ каждые {{ steam_min }} мин, Epic ~ каждые {{ epic_min }} мин.
          Free weekend показываем отдельно.
        </p>

        <div class="chips">
          <span class="chip"><strong>Режим:</strong>
            {% if show_expired %}
              Актуальные + истёкшие за 7 дней
            {% else %}
              Только актуальные
            {% endif %}
          </span>
          <span class="chip"><strong>Фильтр:</strong>
            {% if store == "all" %}Все{% else %}{{ store|upper }}{% endif %}
          </span>
        </div>
      </div>

      <div class="controls">
        {% set base = "/?show_expired=" ~ show_expired ~ "&store=" ~ store %}
                {% set base_kind = base %}
<div class="seg" title="Фильтр по типу раздачи">
  {% if kind == "all" %}<span class="on">Все</span>{% else %}<a href="{{ base_kind }}&kind=all">Все</a>{% endif %}
  {% if kind == "keep" %}<span class="on">🎁 Навсегда</span>{% else %}<a href="{{ base_kind }}&kind=keep">🎁 Навсегда</a>{% endif %}
  {% if kind == "weekend" %}<span class="on">⏱ Временно</span>{% else %}<a href="{{ base_kind }}&kind=weekend">⏱ Временно</a>{% endif %}
  {% if kind == "deals" %}<span class="on">💸 Deals 70%+</span>{% else %}<a href="{{ base_kind }}&kind=deals">💸 Deals 70%+</a>{% endif %}
  {% if kind == "free" %}<span class="on">🔥 Бесплатные</span>{% else %}<a href="{{ base_kind }}&kind=free">🔥 Бесплатные</a>{% endif %}
</div>
        <div class="seg" title="Фильтр по магазину">
          {% if store == "all" %}<span class="on">Все</span>{% else %}<a href="{{ base }}&store=all">Все</a>{% endif %}
          {% if store == "steam" %}<span class="on">🎮 Steam</span>{% else %}<a href="{{ base }}&store=steam">🎮 Steam</a>{% endif %}
          {% if store == "epic" %}<span class="on">🟦 Epic</span>{% else %}<a href="{{ base }}&store=epic">🟦 Epic</a>{% endif %}
          {% if store == "gog" %}<span class="on">🟪 GOG</span>{% else %}<a href="{{ base }}&store=gog">🟪 GOG</a>{% endif %}
          {% if store == "prime" %}<span class="on">🟨 Prime</span>{% else %}<a href="{{ base }}&store=prime">🟨 Prime</a>{% endif %}
        </div>

        <div class="seg" title="Показывать истёкшие за 7 дней">
          {% if show_expired %}
            <a href="/?show_expired=0&store={{ store }}">✅ Только актуальные</a>
            <span class="on">Истёкшие за 7 дней</span>
          {% else %}
            <span class="on">✅ Только актуальные</span>
            <a href="/?show_expired=1&store={{ store }}">Истёкшие за 7 дней</a>
          {% endif %}
        </div>
      </div>
    </div>

                {% if kind in ["all", "keep"] %}
    <div class="section">
      <h2>🆓 Free to keep</h2>
      {% if keep|length == 0 %}
        <div class="empty">Пока нет подходящих раздач под текущий фильтр.</div>
      {% else %}
        <div class="grid">
          {% for d in keep %}
          <div class="card">
            <div class="thumb">
              {% if d["image"] %}
                <img src="{{ d["image"] }}" alt="cover"
     onerror="this.onerror=null; this.src=this.dataset.fallback || '';"
     data-fallback="{{ d.get('image_fallback','') }}"/>
              {% else %}
                <div class="ph">Нет обложки</div>
              {% endif %}
            </div>
            <div class="body">
              <div class="row1">
                <span class="badge">{{ d["store_badge"] }}</span>
                <span class="meta">
                <span class="pill ok">FREE</span>
  {% if d["is_new"] %}
    <span class="pill ok">🆕 NEW</span>
  {% endif %}
  {% if d["expired"] %}
    <span class="pill exp">❌ истекло</span>
  {% else %}
    <span class="pill ok">✅ актуально</span>
  {% endif %}
</span>
              </div>
              <div class="title">{{ d["title"] }}</div>
              <div class="row1">
                <span class="pill">⏳ До: {{ d["ends_at_fmt"] }}</span>
                {% if d["time_left"] and not d["expired"] %}
                  <span class="pill ok">⏱ {{ d["time_left"] }}</span>
                {% endif %}
                </div>
              <div class="actions" style="margin-top:10px;">
                <a class="btn primary" href="{{ d["url"] }}" target="_blank">Открыть</a>
                <button class="btn copy" data-url="{{ d["url"] }}">Копировать ссылку</button>
              </div>
            </div>
          </div>
          {% endfor %}
        </div>
      {% endif %}
    </div>
                {% endif %}

{% if kind in ["all", "weekend"] %}
    <div class="section">
      <h2>⏱ Free weekend / временно</h2>
      {% if weekend|length == 0 %}
        <div class="empty">Пока нет активных временных акций под текущий фильтр.</div>
      {% else %}
        <div class="grid">
          {% for d in weekend %}
          <div class="card">
            <div class="thumb">
  {% if d["image"] %}
    <img src="{{ d["image"] }}" alt="cover"
         onerror="this.onerror=null; this.src=this.dataset.fallback || '';"
         data-fallback="{{ d.get('image_fallback','') }}"/>
  {% else %}
    <div class="ph">Нет обложки</div>
  {% endif %}
</div>
            </div>
            <div class="body">
              <div class="row1">
                <span class="badge">{{ d["store_badge"] }}</span>
                <span class="meta">
                <span class="pill ok">FREE WEEKEND</span>
  {% if d["is_new"] %}
    <span class="pill ok">🆕 NEW</span>
  {% endif %}
  {% if d["expired"] %}
    <span class="pill exp">❌ истекло</span>
  {% else %}
    <span class="pill ok">✅ актуально</span>
  {% endif %}
</span>
              </div>
              <div class="title">{{ d["title"] }}</div>
              <div class="row1">
                <span class="pill">⏳ До: {{ d["ends_at_fmt"] }}</span>
                {% if d["time_left"] and not d["expired"] %}
                  <span class="pill ok">⏱ {{ d["time_left"] }}</span>
                {% endif %}
              </div>
              <div class="actions" style="margin-top:10px;">
                <a class="btn primary" href="{{ d["url"] }}" target="_blank">Открыть</a>
              <button class="btn copy" data-url="{{ d["url"] }}">Копировать ссылку</button>
                </div>
            </div>
          </div>
          {% endfor %}
        </div>
      {% endif %}
     </div>
          {% endif %}

{% if kind in ["all", "deals"] %}
<div class="section">
                

  <h2>💸 Hot deals 70%+</h2>
  {% if hot|length == 0 %}
    <div class="empty">Пока нет подходящих скидок под текущий фильтр.</div>
  {% else %}
    <div class="grid">
      {% for d in hot %}
      <div class="card">
        <div class="thumb">
  {% if d["image"] %}
    <img
   src="{{ d['image'] }}"
  alt=""
  loading="lazy"
  referrerpolicy="no-referrer"
  data-fallback="{{ d.get('image_fallback','') }}"
  data-fallback2="{{ d.get('image_fallback2','') }}"
  onerror="
    if(!this.dataset.try1){ this.dataset.try1=1; if(this.dataset.fallback){ this.src=this.dataset.fallback; return; } }
    if(!this.dataset.try2){ this.dataset.try2=1; if(this.dataset.fallback2){ this.src=this.dataset.fallback2; return; } }
    this.remove();">
    {% else %}
    <div class="ph">Нет обложки</div>
  {% endif %}
</div>
        <div class="body">
          <div class="row1">
            <span class="badge">{{ d["store_badge"] }}</span>
            <span class="meta">
              {% if d["discount_pct"] %}
                <span class="pill ok">💸 -{{ d["discount_pct"] }}%</span>
              {% endif %}
              {% if d["is_new"] %}
                <span class="pill ok">🆕 NEW</span>
              {% endif %}
            </span>
          </div>

          <div class="title">{{ d["title"] }}</div>

          <div class="row1">
            {% if d["ends_at_fmt"] %}
              <span class="pill">⏳ До: {{ d["ends_at_fmt"] }}</span>
            {% endif %}
            {% if d["time_left"] and not d["expired"] %}
              <span class="pill ok">⏱ {{ d["time_left"] }}</span>
            {% endif %}
          </div>

          <div class="actions" style="margin-top:10px;">
            <a class="btn primary" href="{{ d["url"] }}" target="_blank">Открыть</a>
            <button class="btn copy" data-url="{{ d["url"] }}">Копировать ссылку</button>
          </div>
        </div>
      </div>
      {% endfor %}
    </div>
  {% endif %}
</div>
{% endif %}

                {% if kind in ["all", "free"] %}
<div class="section">
                


  <h2>🔥 Популярные бесплатные игры</h2>

  {% if free_games is not defined or free_games|length == 0 %}
    <div class="empty">Пока список бесплатных игр не заполнен.</div>
  {% else %}
    <div class="grid">
      {% for g in free_games %}
      <div class="card">
        <div class="thumb">
  {% if g["image_url"] %}
    <img src="{{ g["image_url"] }}" alt="cover"/>
  {% else %}
    <div class="ph">Нет обложки</div>
  {% endif %}
</div>
        <div class="body">
          <div class="row1">
            <span class="badge">{{ g["store_badge"] }}</span>
            <span class="meta">
              <span class="pill ok">FREE TO PLAY</span>
            </span>
          </div>

          <div class="title">{{ g["title"] }}</div>

          {% if g["note"] %}
            <div class="row1">
              <span class="pill">{{ g["note"] }}</span>
            </div>
          {% endif %}

          <div class="actions" style="margin-top:10px;">
            <a class="btn primary" href="{{ g["url"] }}" target="_blank">Играть</a>
          </div>
        </div>
      </div>
      {% endfor %}
    </div>
  {% endif %}
</div>
{% endif %}

  </div>
                <div id="toast" class="toast">Ссылка скопирована ✅</div>
<script>
(function(){
  const toast = document.getElementById("toast");
  let t = null;

  function showToast(msg){
    if (!toast) return;
    toast.textContent = msg || "Готово ✅";
    toast.classList.add("on");
    clearTimeout(t);
    t = setTimeout(() => toast.classList.remove("on"), 1200);
  }

  async function copyText(text){
    try{
      await navigator.clipboard.writeText(text);
      showToast("Ссылка скопирована ✅");
    }catch(e){
      // fallback
      const ta = document.createElement("textarea");
      ta.value = text;
      document.body.appendChild(ta);
      ta.select();
      document.execCommand("copy");
      document.body.removeChild(ta);
      showToast("Ссылка скопирована ✅");
    }
  }

  document.addEventListener("click", (e) => {
    const btn = e.target.closest(".copy");
    if (!btn) return;
    e.preventDefault();
    const url = btn.getAttribute("data-url") || "";
    if (url) copyText(url);
  });
})();
</script>
                <script>
document.addEventListener("error", function(e){
  const img = e.target;
  if(img && img.tagName === "IMG" && img.parentElement && img.parentElement.classList.contains("thumb")){
    // если img удалился — покажем "Нет обложки"
    if(!img.isConnected){
      const ph = document.createElement("div");
      ph.className = "ph";
      ph.textContent = "Нет обложки";
      img.parentElement.appendChild(ph);
    }
  }
}, true);
</script>
</body>
</html>
""")


def store_badge(store: str | None) -> str:
    return {"steam": "🎮 Steam", "epic": "🟦 Epic", "gog": "🟪 GOG", "prime": "🟨 Prime"}.get(store or "", store or "Store")


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
        LIMIT 15
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

    def images_for_row(row_store: str | None, url: str, image_url: str | None):
     """Всегда отдаёт 3 значения: main, fb1, fb2"""
    st = (row_store or "").strip().lower()

    # если в БД уже есть картинка — используем её
    if image_url:
        return image_url, "", ""

    if st == "steam":
        appid = extract_steam_app_id_fast(url)
        if not appid:
            return "", "", ""
        c = steam_image_candidates(appid)  # <— ниже дам правильную функцию
        main = c[0] if len(c) > 0 else ""
        fb1  = c[1] if len(c) > 1 else ""
        fb2  = c[2] if len(c) > 2 else ""
        return main, fb1, fb2

    return "", "", ""

    # keep
    keep = []
    for r in keep_rows:
        if not (allow_time(r[4]) and allow_store(r[0])):
            continue
        img_main, img_fb1, img_fb2 = images_for_row(r[0], r[2], r[3])

        keep.append({
            "store_badge": store_badge(r[0]),
            "title": r[1],
            "url": r[2],
            "image": img_main,
            "image_fallback": img_fb1,
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
        img_main, img_fb1, img_fb2 = images_for_row(r[0], r[2], r[3])

        weekend.append({
            "store_badge": store_badge(r[0]),
            "title": r[1],
            "url": r[2],
            "image": img_main,
            "image_fallback": img_fb1,
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
        img_main, img_fb1, img_fb2 = images_for_row(r[0], r[2], r[3])

        hot.append({
            "store_badge": store_badge(r[0]),
            "title": r[1],
            "url": r[2],
            "image": img_main,
            "image_fallback": img_fb1,
            "image_fallback2": img_fb2,
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

    return PAGE.render(
        keep=keep,
        weekend=weekend,
        free_games=free_games,
        steam_min=STEAM_MIN,
        epic_min=EPIC_MIN,
        show_expired=int(show_expired),
        store=store,
        kind=kind,
        hot=hot,
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
            "interval",
            minutes=EPIC_MIN,
            id="epic_job",
            replace_existing=True,
            kwargs={"store": "epic"},
        )

    if not scheduler.get_job("gog_job"):
        scheduler.add_job(
            run_job,
            "interval",
            minutes=GOG_MIN,
            id="gog_job",
            replace_existing=True,
            kwargs={"store": "gog"},
        )

    if not scheduler.get_job("prime_job"):
        scheduler.add_job(
            run_job,
            "interval",
            minutes=PRIME_MIN,
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


@app.on_event("shutdown")
async def on_shutdown():
    try:
        if scheduler.running:
            scheduler.shutdown(wait=False)
    except Exception:
        pass