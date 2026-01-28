#!/usr/bin/env python3
"""
Тестируем получение изображений Steam в новом формате
"""

import requests
import re

def test_new_steam_images():
    """Тестируем новые игры Steam"""
    
    test_cases = [
        ("3660800", "3D PUZZLE - Race Track"),
        ("3660810", "ROOM FOOTBALL - Abandoned Factory"),
        ("730", "Counter-Strike 2 (старая игра для сравнения)"),
    ]
    
    for app_id, name in test_cases:
        print(f"\n🎮 Тестируем: {name} (AppID: {app_id})")
        print("-" * 50)
        
        # Пробуем получить изображения через парсинг
        images = get_steam_images_from_page_new(app_id)
        
        if images.get('all'):
            print(f"✅ Найдено {len(images['all'])} изображений:")
            for i, img_url in enumerate(images['all'][:3]):  # Показываем первые 3
                print(f"  {i+1}. {img_url[:80]}...")
            
            if images.get('header'):
                print(f"\n📸 Основное изображение: {images['header'][:80]}...")
                
                # Проверяем доступность
                try:
                    resp = requests.head(images['header'], timeout=5)
                    print(f"   Статус: {resp.status_code}")
                except Exception as e:
                    print(f"   Ошибка: {e}")
        else:
            print(f"❌ Изображения не найдены")

def get_steam_images_from_page_new(app_id: str):
    """Упрощенная версия для теста"""
    try:
        url = f"https://store.steampowered.com/app/{app_id}/"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Cookie': 'birthtime=0; mature_content=1; wants_mature_content=1',
        }
        
        resp = requests.get(url, headers=headers, timeout=10)
        html = resp.text
        
        # Ищем новый формат с хешами
        pattern = rf'(https://shared\.[^"\'\s<>]+?steamstatic\.com/store_item_assets/steam/apps/{app_id}/[a-f0-9]{{40}}/[^"\'\s<>]+?\.jpg[^"\'\s<>]*)'
        matches = re.findall(pattern, html)
        
        result = {'all': list(set(matches))}  # Убираем дубли
        
        # Определяем типы изображений
        for img in result['all']:
            if 'header.jpg' in img and not result.get('header'):
                result['header'] = img
        
        return result
        
    except Exception as e:
        print(f"Ошибка: {e}")
        return {'all': []}

if __name__ == "__main__":
    test_new_steam_images()