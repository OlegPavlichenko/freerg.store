#!/usr/bin/env python3
"""
Тестовый скрипт для проверки парсинга изображений Steam.
Запусти на своём сервере где есть интернет!
"""

import requests
import re

def get_steam_images_from_page(app_id: str, url: str = None):
    """Парсит страницу игры Steam и извлекает изображения"""
    
    if not app_id:
        return {}
    
    try:
        page_url = url or f"https://store.steampowered.com/app/{app_id}/"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            # 🔥 Обход age gate
            'Cookie': 'birthtime=0; mature_content=1; wants_mature_content=1; lastagecheckage=1-0-1990',
        }
        
        print(f"\n{'='*60}")
        print(f"Fetching: {page_url}")
        
        resp = requests.get(page_url, headers=headers, timeout=15, allow_redirects=True)
        
        print(f"Status: {resp.status_code}")
        print(f"Final URL: {resp.url}")
        
        if resp.status_code != 200:
            return {}
        
        html = resp.text
        
        # Если попали на agecheck — пробуем с параметрами
        if '/agecheck/' in resp.url or 'agecheck' in html.lower():
            print("  ⚠️  Age gate detected, retrying with parameters...")
            age_url = f"https://store.steampowered.com/app/{app_id}/?ageDay=1&ageMonth=1&ageYear=1990"
            resp2 = requests.get(age_url, headers=headers, timeout=15)
            if resp2.status_code == 200:
                html = resp2.text
                print(f"  ✅ Bypassed age gate")
        
        print(f"HTML size: {len(html):,} chars")
        
        result = {
            'header': None,
            'capsule': None,
            'hero': None,
            'library': None,
            'all': []
        }
        
        # Улучшенные паттерны
        
        # 1. Новый header с хешем
        pattern_new = rf'(https://shared\.[^"\'\s<>]+?steamstatic\.com/store_item_assets/steam/apps/{app_id}/[a-f0-9]{{30,50}}/header\.jpg)'
        matches = re.findall(pattern_new, html)
        if matches:
            img = matches[0]
            print(f"  ✅ Found header_new: {img[:80]}...")
            result['header'] = img
            result['all'].append(img)
        
        # 2. Старый header
        if not result['header']:
            pattern_old = rf'(https://[^"\'\s<>]+?steamstatic\.com/steam/apps/{app_id}/header\.jpg)'
            matches = re.findall(pattern_old, html)
            if matches:
                img = matches[0]
                print(f"  ✅ Found header_old: {img[:80]}...")
                result['header'] = img
                result['all'].append(img)
        
        # 3. Hero capsule
        pattern_hero = rf'(https://[^"\'\s<>]+?steamstatic\.com/steam/apps/{app_id}/hero_capsule\.jpg)'
        matches = re.findall(pattern_hero, html)
        if matches:
            img = matches[0]
            print(f"  ✅ Found hero: {img[:80]}...")
            result['hero'] = img
            result['all'].append(img)
        
        # 4. Capsule
        pattern_capsule = rf'(https://[^"\'\s<>]+?steamstatic\.com/steam/apps/{app_id}/capsule_616x353\.jpg)'
        matches = re.findall(pattern_capsule, html)
        if matches:
            img = matches[0]
            print(f"  ✅ Found capsule: {img[:80]}...")
            result['capsule'] = img
            result['all'].append(img)
        
        # 5. Library
        pattern_lib = rf'(https://[^"\'\s<>]+?steamstatic\.com/steam/apps/{app_id}/library_600x900\.jpg)'
        matches = re.findall(pattern_lib, html)
        if matches:
            img = matches[0]
            print(f"  ✅ Found library: {img[:80]}...")
            result['library'] = img
            result['all'].append(img)
        
        # Выбираем лучшую
        best = result['header'] or result['hero'] or result['capsule'] or result['library']
        print(f"\n  🎯 Best choice: {best}")
        
        return result
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return {}


def test_games():
    """Тестирует разные игры"""
    
    test_cases = [
        ("730", "Counter-Strike 2 (popular, old)"),
        ("1938090", "Call of Duty (new game)"),
        ("2358720", "Black Myth Wukong (very new)"),
        ("2050650", "Elden Ring (new-ish)"),
        ("570", "Dota 2 (very old)"),
    ]
    
    print("🔍 Testing Steam image scraping")
    print("="*60)
    
    for app_id, description in test_cases:
        print(f"\n📦 {description}")
        result = get_steam_images_from_page(app_id)
        
        if result and result['all']:
            print(f"  ✅ SUCCESS: Found {len(result['all'])} images")
        else:
            print(f"  ❌ FAILED: No images found")


if __name__ == "__main__":
    test_games()
    
    print("\n" + "="*60)
    print("✅ Test complete!")
    print("\nЕсли видишь найденные URL — парсинг работает!")
    print("Скопируй этот файл на свой сервер и запусти:")
    print("  python3 test_image_scraping.py")