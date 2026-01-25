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
# --------------------
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "@freeredeemgames")

ITAD_API_KEY = os.getenv("ITAD_API_KEY", "")

DB_PATH = os.getenv("DB_PATH", "data.sqlite3")

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
    """
    Важно: здесь НЕ создаём индексы по новым колонкам (store/kind),
    чтобы не падать до миграции. Только базовая таблица и базовые индексы.
    """
    conn = sqlite3.connect(DB_PATH)
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
def extract_steam_app_id(url: str) -> str | None:
    # 1) прямой Steam Store URL
    m = re.search(r"store\.steampowered\.com/app/(\d+)", url)
    if m:
        return m.group(1)

    # 2) любой /app/12345
    m = re.search(r"/app/(\d+)", url)
    if m:
        return m.group(1)

    # 3) пробуем раскрыть редирект (ITAD / трекеры) — аккуратно
    try:
        resp = requests.get(url, timeout=15, allow_redirects=True, headers={"User-Agent": "Mozilla/5.0"})
        final_url = str(resp.url)
        m = re.search(r"store\.steampowered\.com/app/(\d+)", final_url)
        if m:
            return m.group(1)
    except Exception:
        pass

    return None


def steam_header_image_from_url(url: str) -> str | None:
    app_id = extract_steam_app_id(url)
    if not app_id:
        return None
    return f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/header.jpg"

import re

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
    Источник: primegaming.blog/tagged/free-games-with-prime :contentReference[oaicite:4]{index=4}
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
    shop id GOG у ITAD = 35. :contentReference[oaicite:2]{index=2}
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
def fetch_itad_steam():
    """
    Steam freebies через ITAD deals/v2.
    Фильтр: cut==100 или price.amount==0.
    """
    if not ITAD_API_KEY:
        return []

    endpoint = "https://api.isthereanydeal.com/deals/v2"
    params = {
        "key": ITAD_API_KEY,
        "shops": "61",     # Steam shop id у ITAD
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

        if not (cut == 100 or price_amount == 0):
            continue

        title = it.get("title") or it.get("name") or deal.get("title") or deal.get("name") or "Steam giveaway"
        url = deal.get("url") or it.get("url")
        if not url:
            continue

        expiry = deal.get("expiry") or it.get("expiry")
        start = deal.get("start") or it.get("start")

        # external_id попробуем как app_id (если можно без редиректа)
        app_id = None
        m = re.search(r"/app/(\d+)", url)
        if m:
            app_id = m.group(1)

        out.append({
            "store": "steam",
            "external_id": app_id or "",
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
        store = d.get("store", "") or ""
        external_id = d.get("external_id", "") or ""
        url = d.get("url", "") or ""
        if not url:
            continue

        did = deal_id(store, external_id, url)

        cur = conn.execute("SELECT id FROM deals WHERE id=?", (did,))
        if cur.fetchone() is None:
            conn.execute(
                "INSERT INTO deals (id,store,external_id,kind,title,url,image_url,source,starts_at,ends_at,posted,created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,0,?)",
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
                    now,
                ),
            )
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
                deals = fetch_itad_steam()
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
        {% set base = "/?show_expired=" ~ show_expired ~ "&kind=" ~ kind %}
                {% set base_kind = base ~ "&store=" ~ store %}
<div class="seg" title="Фильтр по типу раздачи">
  {% if kind == "all" %}<span class="on">Все</span>{% else %}<a href="{{ base_kind }}&kind=all">Все</a>{% endif %}
  {% if kind == "keep" %}<span class="on">🎁 Навсегда</span>{% else %}<a href="{{ base_kind }}&kind=keep">🎁 Навсегда</a>{% endif %}
  {% if kind == "weekend" %}<span class="on">⏱ Временно</span>{% else %}<a href="{{ base_kind }}&kind=weekend">⏱ Временно</a>{% endif %}
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
                <img src="{{ d["image"] }}" alt="cover"/>
              {% else %}
                <div class="ph">Нет обложки</div>
              {% endif %}
            </div>
            <div class="body">
              <div class="row1">
                <span class="badge">{{ d["store_badge"] }}</span>
                <span class="meta">
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
                <a class="btn" href="{{ d["url"] }}" target="_blank">Копировать ссылку</a>
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
                <img src="{{ d["image"] }}" alt="cover"/>
              {% else %}
                <div class="ph">Нет обложки</div>
              {% endif %}
            </div>
            <div class="body">
              <div class="row1">
                <span class="badge">{{ d["store_badge"] }}</span>
                <span class="meta">
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
</body>
</html>
""")


def store_badge(store: str | None) -> str:
    return {"steam": "🎮 Steam", "epic": "🟦 Epic", "gog": "🟪 GOG", "prime": "🟨 Prime"}.get(store or "", store or "Store")


@app.get("/", response_class=HTMLResponse)
def index(show_expired: int = 0, store: str = "all"):
    conn = db()

    keep_rows = conn.execute("""
        SELECT store,title,url,image_url,ends_at,created_at
        FROM deals
        WHERE kind='free_to_keep'
        ORDER BY created_at DESC
        LIMIT 400
    """).fetchall()

    weekend_rows = conn.execute("""
        SELECT store,title,url,image_url,ends_at,created_at
        FROM deals
        WHERE kind='free_weekend'
        ORDER BY created_at DESC
        LIMIT 400
    """).fetchall()

    free_games = conn.execute("""
    SELECT store,title,url,image_url,note
    FROM free_games
    ORDER BY sort ASC, created_at DESC
    LIMIT 24
""").fetchall()

    conn.close()

    # фильтр по времени (актуальные/истёкшие за неделю)
    def allow_time(ends_at: str | None) -> bool:
        if is_active_end(ends_at):
            return True
        return bool(show_expired) and is_expired_recent(ends_at, days=7)

    # фильтр по магазину (вкладки)
    store = (store or "all").strip().lower()
    allowed_stores = {"all", "steam", "epic", "gog", "prime"}
    if store not in allowed_stores:
        store = "all"

    def allow_store(row_store: str | None) -> bool:
        if store == "all":
            return True
        return (row_store or "").strip().lower() == store

    keep = [{
        "store_badge": store_badge(r[0]),
        "title": r[1],
        "url": r[2],
        "image": (r[3] or "") or (steam_header_image_from_url(r[2]) if (r[0] or "").lower() == "steam" else ""),
        "ends_at": r[4],
        "is_new": is_new(r[5]),
        "ends_at_fmt": format_expiry(r[4]),
        "created_at": r[5],
        "expired": not is_active_end(r[4]),
        "time_left": time_left_label(r[4]),
    } for r in keep_rows if allow_time(r[4]) and allow_store(r[0])]

    weekend = [{
        "store_badge": store_badge(r[0]),
        "title": r[1],
        "url": r[2],
        "image": (r[3] or "") or (steam_header_image_from_url(r[2]) if (r[0] or "").lower() == "steam" else ""),
        "ends_at": r[4],
        "is_new": is_new(r[5]),
        "ends_at_fmt": format_expiry(r[4]),
        "created_at": r[5],
        "expired": not is_active_end(r[4]),
        "time_left": time_left_label(r[4]),
    } for r in weekend_rows if allow_time(r[4]) and allow_store(r[0])]

    keep.sort(key=lambda d: sort_key_by_ends(d["ends_at"]))
    weekend.sort(key=lambda d: sort_key_by_ends(d["ends_at"]))

    kind = request.args.get("kind", "all")  # all | keep | weekend | free

    free_games_rows = conn.execute("""
    SELECT store, title, url, image_url, note
    FROM free_games
    ORDER BY sort ASC, created_at DESC
    LIMIT 24
    """).fetchall()

    free_games = []
    for st, title, url, image_url, note in free_games_rows:
      st = (st or "").strip().lower()

    store_badge = {
        "steam": "🎮 Steam",
        "epic": "🟦 Epic",
        "gog": "🟪 GOG",
        "prime": "🟨 Prime",
    }.get(st, st or "Store")

    # картинка: если в таблице пусто — строим для Steam по appid
    img = image_url
    if not img and st == "steam":
        img = steam_header_cdn_from_url(url)

    free_games.append({
        "store": st,
        "store_badge": store_badge,
        "title": title,
        "url": url,
        "image_url": img,
        "note": note,
    })

    return PAGE.render(
        keep=keep,
        weekend=weekend,
        steam_min=STEAM_MIN,
        epic_min=EPIC_MIN,
        show_expired=int(show_expired),
        store=store,
        kind=kind,
        free_games=free_games,
    )


# --------------------
# API endpoints
# --------------------
@app.get("/health")
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


@app.get("/update")
async def update_now(store: str = "steam"):
    result = await job_async(store=store)
    return {"ok": True, "result": result}


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

