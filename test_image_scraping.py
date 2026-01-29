#!/usr/bin/env python3
"""
Тестируем получение изображений Steam в новом формате
"""
import requests
import re


def get_steam_images_from_page_new(app_id: str):
    """Упрощенная версия для теста"""
    try:
        url = f"https://store.steampowered.com/app/{app_id}/"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Cookie': 'birthtime=0; mature_content=1; wants_mature_content=1',
        }
        
        print(f"  📡 Запрос: {url}")
        resp = requests.get(url, headers=headers, timeout=10)
        print(f"  ✓ Статус: {resp.status_code}")
        
        html = resp.text
        print(f"  ✓ HTML: {len(html):,} символов")
        
        # Ищем новый формат с хешами
        pattern = rf'(https://shared\.[^"\'\s<>]+?steamstatic\.com/store_item_assets/steam/apps/{app_id}/[a-f0-9]{{40}}/[^"\'\s<>]+?\.jpg[^"\'\s<>]*)'
        matches = re.findall(pattern, html)
        
        # Также ищем старый формат
        pattern_old = rf'(https://cdn\.[^"\'\s<>]+?steamstatic\.com/steam/apps/{app_id}/header\.jpg)'
        matches_old = re.findall(pattern_old, html)
        
        all_matches = list(set(matches + matches_old))  # Убираем дубли
        
        result = {'all': all_matches}
        
        # Определяем типы изображений
        for img in result['all']:
            if 'header.jpg' in img and not result.get('header'):
                result['header'] = img
            elif 'capsule_616x353' in img and not result.get('capsule'):
                result['capsule'] = img
        
        return result
        
    except Exception as e:
        print(f"  ❌ Ошибка: {e}")
        return {'all': []}


def test_new_steam_images():
    """Тестируем новые игры Steam"""
    
    test_cases = [
        ("3660800", "3D PUZZLE - Race Track"),
        ("3660810", "ROOM FOOTBALL - Abandoned Factory"),
        ("730", "Counter-Strike 2 (старая игра для сравнения)"),
        ("1938090", "Call of Duty"),
        ("2358720", "Black Myth Wukong"),
    ]
    
    for app_id, name in test_cases:
        print(f"\n{'='*60}")
        print(f"🎮 Тестируем: {name}")
        print(f"   AppID: {app_id}")
        print("-" * 60)
        
        # Пробуем получить изображения через парсинг
        images = get_steam_images_from_page_new(app_id)
        
        if images.get('all'):
            print(f"\n✅ Найдено {len(images['all'])} изображений:")
            for i, img_url in enumerate(images['all'][:5]):  # Показываем первые 5
                print(f"  {i+1}. {img_url[:100]}...")
            
            if images.get('header'):
                print(f"\n📸 Основное изображение (header):")
                print(f"   {images['header']}")
                
                # Проверяем доступность
                try:
                    resp = requests.head(images['header'], timeout=5)
                    status = "✅ OK" if resp.status_code == 200 else f"❌ {resp.status_code}"
                    print(f"   Статус: {status}")
                except Exception as e:
                    print(f"   ❌ Ошибка проверки: {e}")
            
            if images.get('capsule'):
                print(f"\n📸 Capsule изображение:")
                print(f"   {images['capsule']}")
        else:
            print(f"\n❌ Изображения не найдены")
            print(f"   Попробуй проверить страницу вручную:")
            print(f"   https://store.steampowered.com/app/{app_id}/")


if __name__ == "__main__":
    print("🔍 Тестирование извлечения изображений Steam")
    print("=" * 60)
    test_new_steam_images()
    print("\n" + "=" * 60)
    print("✅ Тест завершён!")