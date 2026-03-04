import os
import re
import sqlite3
import hashlib
import asyncio
import requests
import random
import uuid

from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from jinja2 import Template
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Bot

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "@freeredeemgames")
ITAD_API_KEY = os.getenv("ITAD_API_KEY", "")
DB_PATH = os.getenv("DB_PATH", "/opt/freerg/data/data.sqlite3")
SITE_BASE = os.getenv("SITE_BASE", "https://freerg.store")

app = FastAPI()

def db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # КРИТИЧЕСКИ ВАЖНО
    return conn

# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------
def is_active_end(dt_str):
    if not dt_str: return True
    try:
        if "T" in dt_str:
            end_dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        else:
            end_dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return end_dt > datetime.now(timezone.utc)
    except:
        return True

def allow_time(dt_str, show_expired):
    if show_expired: return True
    return is_active_end(dt_str)

def allow_store(st, target):
    if target == "all": return True
    return str(st).lower() == target.lower()

def is_new(created_at_str):
    try:
        dt = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt).total_seconds() < 86400
    except: return False

def format_expiry(dt_str):
    if not dt_str: return ""
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.strftime("%d.%m %H:%M")
    except: return dt_str

def fmt_price(p):
    if p is None: return None
    try: return f"{float(p):.2f}"
    except: return str(p)

def currency_symbol(curr):
    m = {"USD": "$", "EUR": "€", "RUB": "₽", "KGS": "с"}
    return m.get(curr, curr or "$")

def images_for_row(store, url, img_db):
    if img_db and img_db.startswith("http"):
        return img_db, img_db
    return None, None

# ---------------------------------------------------------
# CORE LOGIC
# ---------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def index(request: Request, show_expired: int = 0, store: str = "all", kind: str = "all"):
    conn = db()
    store = (store or "all").lower()
    kind = (kind or "all").lower()

    # Маппинг фильтров URL -> БД
    mapping = {
        "keep": "free_to_keep",
        "weekend": "free_weekend",
        "deals": "hot_deal",
        "free": "free_to_play"
    }

    keep, weekend, hot, free_games = [], [], [], []

    # 1. Free to Keep
    if kind in ["all", "keep"]:
        rows = conn.execute("SELECT * FROM deals WHERE kind='free_to_keep' ORDER BY created_at DESC").fetchall()
        for r in rows:
            if allow_time(r['ends_at'], show_expired) and allow_store(r['store'], store):
                img, _ = images_for_row(r['store'], r['url'], r['image_url'])
                item = dict(r)
                item.update({"image": img, "is_new": is_new(r['created_at']), "ends_at_fmt": format_expiry(r['ends_at']), "go_url": f"/go/{r['id']}"})
                keep.append(item)

    # 2. Weekend
    if kind in ["all", "weekend"]:
        rows = conn.execute("SELECT * FROM deals WHERE kind='free_weekend' ORDER BY created_at DESC").fetchall()
        for r in rows:
            if allow_time(r['ends_at'], show_expired) and allow_store(r['store'], store):
                img, _ = images_for_row(r['store'], r['url'], r['image_url'])
                item = dict(r)
                item.update({"image": img, "is_new": is_new(r['created_at']), "ends_at_fmt": format_expiry(r['ends_at']), "go_url": f"/go/{r['id']}"})
                weekend.append(item)

    # 3. Hot Deals
    if kind in ["all", "deals"]:
        rows = conn.execute("SELECT * FROM deals WHERE kind='hot_deal' AND discount_pct >= 70 ORDER BY discount_pct DESC LIMIT 50").fetchall()
        for r in rows:
            if allow_store(r['store'], store):
                img, _ = images_for_row(r['store'], r['url'], r['image_url'])
                item = dict(r)
                item.update({
                    "image": img, "is_new": is_new(r['created_at']), "ends_at_fmt": format_expiry(r['ends_at']), "go_url": f"/go/{r['id']}",
                    "price_old_fmt": fmt_price(r['price_old']), "price_new_fmt": fmt_price(r['price_new']), "currency_sym": currency_symbol(r['currency'])
                })
                hot.append(item)

    # LFG
    lfg_rows = conn.execute("SELECT * FROM lfg WHERE active=1 ORDER BY created_at DESC LIMIT 15").fetchall()
    lfg = [dict(r) for r in lfg_rows]

    # Stats
    today = datetime.now().strftime("%Y-%m-%d")
    saved_row = conn.execute("SELECT SUM(price_old - price_new) as s FROM deals WHERE price_old > price_new").fetchone()
    clicks_row = conn.execute("SELECT COUNT(*) as c FROM clicks WHERE date(created_at) = ?", (today,)).fetchone()
    
    stats = {
        "saved_all": saved_row['s'] or 0,
        "saved_today": (saved_row['s'] or 0) / 365, # Заглушка, если нет дневной статистики
        "clicks_today": clicks_row['c'] or 0
    }

    conn.close()
    
    # Рендерим шаблон (PAGE должен быть определен в твоем шаблоне)
    # Если переменная PAGE у тебя ниже, перемести её определение ВЫШЕ этой функции
    try:
        return PAGE.render(
            keep=keep, weekend=weekend, hot=hot, lfg=lfg,
            store=store, kind=kind, savings=stats,
            tg_group_url="https://t.me/freeredeemgames"
        )
    except NameError:
        return "Ошибка: Переменная PAGE (шаблон) не найдена в коде."

@app.get("/go/{did}")
def go_deal(did: str, request: Request):
    conn = db()
    res = conn.execute("SELECT url FROM deals WHERE id=?", (did,)).fetchone()
    if res:
        url = res['url']
        conn.execute("INSERT INTO clicks (deal_id, ip, ua) VALUES (?, ?, ?)", 
                     (did, request.client.host, request.headers.get("user-agent")))
        conn.commit()
        conn.close()
        return RedirectResponse(url)
    conn.close()
    return RedirectResponse("/")

# ---------------------------------------------------------
# ВСТАВЬ СВОЙ ШАБЛОН (PAGE = Template(""...)) НИЖЕ ИЛИ ВЫШЕ
# ---------------------------------------------------------
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)