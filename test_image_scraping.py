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
        }
        
        print(f"\n{'='*60}")
        print(f"Fetching: {page_url}")
        
        resp = requests.get(page_url, headers=headers, timeout=15, allow_redirects=True)
        
        print(f"Status: {resp.status_code}")
        print(f"Final URL: {resp.url}")
        
        if resp.status_code != 200:
            return {}
        
        html = resp.text
        print(f"HTML size: {len(html):,} chars")
        
        result = {
            'header': None,
            'capsule': None,
            'hero': None,
            'library': None,
            'all': []
        }
        
        # Паттерны
        patterns = {
            'header_new': rf'(https://shared\.[^"\'<>\s]+?steamstatic\.com/store_item_assets/steam/apps/{app_id}/[a-f0-9]+/header\.jpg)',
            'header_old': rf'(https://cdn\.[^"\'<>\s]+?steamstatic\.com/steam/apps/{app_id}/header\.jpg)',
            'hero': rf'(https://[^"\'<>\s]+?steamstatic\.com/steam/apps/{app_id}/hero_capsule\.jpg)',
            'capsule': rf'(https://[^"\'<>\s]+?steamstatic\.com/steam/apps/{app_id}/capsule_616x353\.jpg)',
            'library': rf'(https://[^"\'<>\s]+?steamstatic\.com/steam/apps/{app_id}/library_600x900\.jpg)',
        }
        
        # Ищем каждый тип
        for key, pattern in patterns.items():
            matches = re.findall(pattern, html)
            if matches:
                img = matches[0]
                print(f"  ✅ Found {key}: {img[:80]}...")
                
                if 'header' in key and not result['header']:
                    result['header'] = img
                elif key == 'hero':
                    result['hero'] = img
                elif key == 'capsule':
                    result['capsule'] = img
                elif key == 'library':
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