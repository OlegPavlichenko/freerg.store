#!/usr/bin/env python3
"""
Скрипт для добавления популярных бесплатных игр (F2P) в базу данных
Запустить: python3 add_free_games.py
"""

import sqlite3

DB_PATH = "/opt/freerg/data/data.sqlite3"

# Популярные бесплатные игры
FREE_GAMES = [
    # Steam
    {
        "store": "steam",
        "title": "Counter-Strike 2",
        "url": "https://store.steampowered.com/app/730/",
        "image_url": "https://cdn.cloudflare.steamstatic.com/steam/apps/730/header.jpg",
        "note": "Легендарный шутер",
        "sort": 1
    },
    {
        "store": "steam",
        "title": "Dota 2",
        "url": "https://store.steampowered.com/app/570/",
        "image_url": "https://cdn.cloudflare.steamstatic.com/steam/apps/570/header.jpg",
        "note": "MOBA",
        "sort": 2
    },
    {
        "store": "steam",
        "title": "Team Fortress 2",
        "url": "https://store.steampowered.com/app/440/",
        "image_url": "https://cdn.cloudflare.steamstatic.com/steam/apps/440/header.jpg",
        "note": "Классический шутер",
        "sort": 3
    },
    {
        "store": "steam",
        "title": "Warframe",
        "url": "https://store.steampowered.com/app/230410/",
        "image_url": "https://cdn.cloudflare.steamstatic.com/steam/apps/230410/header.jpg",
        "note": "Sci-Fi shooter",
        "sort": 4
    },
    {
        "store": "steam",
        "title": "Path of Exile",
        "url": "https://store.steampowered.com/app/238960/",
        "image_url": "https://cdn.cloudflare.steamstatic.com/steam/apps/238960/header.jpg",
        "note": "Action RPG",
        "sort": 5
    },
    {
        "store": "steam",
        "title": "Apex Legends",
        "url": "https://store.steampowered.com/app/1172470/",
        "image_url": "https://cdn.cloudflare.steamstatic.com/steam/apps/1172470/header.jpg",
        "note": "Battle Royale",
        "sort": 6
    },
    {
        "store": "steam",
        "title": "Lost Ark",
        "url": "https://store.steampowered.com/app/1599340/",
        "image_url": "https://cdn.cloudflare.steamstatic.com/steam/apps/1599340/header.jpg",
        "note": "MMORPG",
        "sort": 7
    },
    {
        "store": "steam",
        "title": "Destiny 2",
        "url": "https://store.steampowered.com/app/1085660/",
        "image_url": "https://cdn.cloudflare.steamstatic.com/steam/apps/1085660/header.jpg",
        "note": "Sci-Fi MMO",
        "sort": 8
    },
    {
        "store": "steam",
        "title": "War Thunder",
        "url": "https://store.steampowered.com/app/236390/",
        "image_url": "https://cdn.cloudflare.steamstatic.com/steam/apps/236390/header.jpg",
        "note": "Action Simulator MMO",
        "sort": 9
    },
    {
        "store": "steam",
        "title": "Battlefield REDSEC",
        "url": "https://store.steampowered.com/app/3028330/",
        "image_url": "https://cdn.cloudflare.steamstatic.com/steam/apps/3028330/header.jpg",
        "note": "Action",
        "sort": 10
    },
    {
        "store": "steam",
        "title": "VRChat",
        "url": "https://store.steampowered.com/app/438100/",
        "image_url": "https://cdn.cloudflare.steamstatic.com/steam/apps/438100/header.jpg",
        "note": "VR MMO",
        "sort": 11
    },
    {
        "store": "steam",
        "title": "Marvel_Rivals",
        "url": "https://store.steampowered.com/app/2767030/",
        "image_url": "https://cdn.cloudflare.steamstatic.com/steam/apps/2767030/header.jpg",
        "note": "Action",
        "sort": 12
    },
]


def add_free_games():
    """Добавляет бесплатные игры в базу данных"""
    
    conn = sqlite3.connect(DB_PATH)
    
    # Проверяем существование таблицы
    cursor = conn.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='free_games'
    """)
    
    if not cursor.fetchone():
        print("❌ Таблица free_games не существует!")
        print("   Сначала запусти основное приложение для создания таблиц.")
        conn.close()
        return
    
    added = 0
    updated = 0
    
    for game in FREE_GAMES:
        try:
            # Пробуем вставить
            cursor = conn.execute("""
                INSERT INTO free_games (store, title, url, image_url, note, sort)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                game["store"],
                game["title"],
                game["url"],
                game["image_url"],
                game["note"],
                game["sort"]
            ))
            
            if cursor.rowcount > 0:
                added += 1
                print(f"✅ Добавлено: {game['title']} ({game['store']})")
            
        except sqlite3.IntegrityError:
            # Если игра уже есть (UNIQUE constraint на url) - обновляем
            conn.execute("""
                UPDATE free_games 
                SET title=?, image_url=?, note=?, sort=?
                WHERE url=?
            """, (
                game["title"],
                game["image_url"],
                game["note"],
                game["sort"],
                game["url"]
            ))
            updated += 1
            print(f"🔄 Обновлено: {game['title']} ({game['store']})")
    
    conn.commit()
    conn.close()
    
    print(f"\n{'='*50}")
    print(f"✅ Готово!")
    print(f"   Добавлено: {added}")
    print(f"   Обновлено: {updated}")
    print(f"   Всего игр: {len(FREE_GAMES)}")
    print(f"\nТеперь открой сайт и перейди в раздел '🔥 Бесплатные'")


if __name__ == "__main__":
    print("🎮 Добавление бесплатных игр в базу данных")
    print("="*50)
    add_free_games()