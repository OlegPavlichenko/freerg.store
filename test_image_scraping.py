#!/usr/bin/env python3
"""
Тестируем конкретные игры
"""

import requests

def test_steam_image(app_id):
    """Тестируем разные CDN для одного app_id"""
    urls = [
        f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/header.jpg",
        f"https://cdn.akamai.steamstatic.com/steam/apps/{app_id}/header.jpg",
        f"https://steamcdn-a.akamaihd.net/steam/apps/{app_id}/header.jpg",
        f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/capsule_616x353.jpg",
        f"https://cdn.akamai.steamstatic.com/steam/apps/{app_id}/capsule_231x87.jpg",
        f"https://cdn.cloudflare.steamstatic.com/steam/apps/{app_id}/library_600x900.jpg",
    ]
    
    print(f"\nТестируем игру AppID: {app_id}")
    print("-" * 50)
    
    for url in urls:
        try:
            resp = requests.head(url, timeout=5, allow_redirects=True)
            status = resp.status_code
            content_type = resp.headers.get('Content-Type', '')
            
            if status == 200 and ('image' in content_type or 'jpeg' in content_type):
                print(f"✅ {url}")
                print(f"   Status: {status}, Type: {content_type}")
                return url  # Возвращаем первый рабочий
            else:
                print(f"❌ {url}")
                print(f"   Status: {status}, Type: {content_type}")
        except Exception as e:
            print(f"❌ {url}")
            print(f"   Error: {e}")
    
    return None

# Тестируем популярные игры
test_games = [
    ("730", "Counter-Strike 2"),
    ("570", "Dota 2"),
    ("578080", "PUBG"),
    ("1172470", "Apex Legends"),
    ("1091500", "Cyberpunk 2077"),
]

for app_id, name in test_games:
    working_url = test_steam_image(app_id)
    if working_url:
        print(f"\n🎮 {name}: Используем {working_url}")
    else:
        print(f"\n🎮 {name}: Ни один URL не работает")
    print("=" * 60)