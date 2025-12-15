#!/usr/bin/env python3
"""
GMod Server Status v8.1 - Ultra Optimized for Spark Plan
- Writes ONLY on changes
- Avatar checked on JOIN, written only if different
- Synced to exact minute (:00)
"""

import os
import sys
import json
import re
import time
import unicodedata
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Set

import a2s
import firebase_admin
from firebase_admin import credentials, firestore

GMOD_HOST = os.environ.get('GMOD_HOST', '51.91.215.65')
GMOD_PORT = int(os.environ.get('GMOD_PORT', '27015'))

def get_france_time():
    return datetime.now(timezone.utc) + timedelta(hours=1)

def wait_for_next_minute():
    """Attendre jusqu'à la prochaine minute :00"""
    now = datetime.now()
    seconds_to_wait = 60 - now.second - (now.microsecond / 1_000_000)
    if 0 < seconds_to_wait < 60:
        print(f"    ⏳ Sync: attente {seconds_to_wait:.1f}s jusqu'à XX:{(now.minute + 1) % 60:02d}:00")
        time.sleep(seconds_to_wait)

STEAMID64_BASE = 76561197960265728
STEAM2_RE = re.compile(r"^STEAM_[0-5]:([0-1]):(\d+)$", re.IGNORECASE)

def normalize_name(name):
    if not name:
        return ""
    name = name.lower().strip()
    name = unicodedata.normalize('NFD', name)
    name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
    return re.sub(r'[^a-z0-9]', '', name)

def steam2_to_steam64(steamid):
    match = STEAM2_RE.match(steamid)
    if not match:
        return None
    return str(STEAMID64_BASE + (int(match.group(2)) * 2) + int(match.group(1)))

def fetch_steam_avatar(steamid):
    """Récupère l'avatar Steam actuel"""
    try:
        steam64 = steam2_to_steam64(steamid)
        if not steam64:
            return None
        
        response = requests.get(
            f"https://steamcommunity.com/profiles/{steam64}",
            timeout=5,
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        if response.status_code != 200:
            return None
        
        for pattern in [r'<link rel="image_src" href="([^"]+)"', r'<meta property="og:image" content="([^"]+)"']:
            match = re.search(pattern, response.text)
            if match and 'steamcommunity.com' in match.group(1):
                return match.group(1).replace('_medium', '_full')
        return None
    except:
        return None

def init_firebase():
    if firebase_admin._apps:
        return firestore.client()
    
    service_account_json = os.environ.get('FIREBASE_SERVICE_ACCOUNT')
    if not service_account_json:
        raise ValueError("FIREBASE_SERVICE_ACCOUNT not set")
    
    cred = credentials.Certificate(json.loads(service_account_json))
    firebase_admin.initialize_app(cred)
    return firestore.client()

def query_gmod_server(host: str, port: int) -> Optional[dict]:
    try:
        address = (host, port)
        info = a2s.info(address, timeout=10)
        players = a2s.players(address, timeout=10)
        
        player_list = []
        for p in sorted(players, key=lambda x: x.duration, reverse=True):
            if p.name and p.name.strip():
                player_list.append({
                    'name': p.name.strip(),
                    'time': int(p.duration)
                })
        
        return {
            'server_name': info.server_name,
            'map': info.map_name,
            'players': player_list,
            'max_players': info.max_players,
            'count': len(player_list)
        }
    except Exception as e:
        print(f"    ❌ Server error: {e}")
        return None

def sync_to_firebase(db, server_data: dict, now: datetime, france_now: datetime):
    """
    Ultra-optimized sync - writes only on actual changes
    """
    try:
        current_players = {p['name']: p['time'] for p in server_data['players']}
        current_names = set(current_players.keys())
        
        # 1. Get previous state
        live_doc = db.collection('live').document('status').get()
        previous_names: Set[str] = set()
        previous_data = {}
        
        if live_doc.exists:
            prev = live_doc.to_dict()
            for p in prev.get('players', []):
                previous_names.add(p['name'])
                previous_data[p['name']] = p
        
        # 2. Detect changes
        joined = current_names - previous_names
        left = previous_names - current_names
        stayed = current_names & previous_names
        
        print(f"       📊 +{len(joined)} | -{len(left)} | ={len(stayed)}")
        
        # 3. Load player docs ONLY if needed
        players_cache: Dict[str, tuple] = {}
        
        if joined or left:
            all_players = list(db.collection('players').get())
            for doc in all_players:
                data = doc.to_dict()
                name = data.get('name', '')
                players_cache[name.lower().strip()] = (doc.id, data)
                players_cache[normalize_name(name)] = (doc.id, data)
                for ingame in data.get('ingame_names', []):
                    players_cache[ingame.lower().strip()] = (doc.id, data)
                    players_cache[normalize_name(ingame)] = (doc.id, data)
        
        writes = 0
        
        # 4. Handle JOINED players
        for name in joined:
            session_time = current_players[name]
            key = name.lower().strip()
            key_norm = normalize_name(name)
            
            existing = players_cache.get(key) or players_cache.get(key_norm)
            
            if existing:
                doc_id, data = existing
                steam_id = data.get('steam_id', '')
                current_avatar = data.get('avatar_url')
                
                # Toujours vérifier l'avatar à l'arrivée
                update_needed = False
                update = {
                    'last_seen': firestore.SERVER_TIMESTAMP,
                    'connected_at': now.isoformat(),
                    'current_session_start': session_time,
                    'session_count': data.get('session_count', 0) + 1
                }
                update_needed = True
                
                # Vérifier avatar si Steam ID valide
                if steam_id.startswith('STEAM_'):
                    new_avatar = fetch_steam_avatar(steam_id)
                    if new_avatar and new_avatar != current_avatar:
                        update['avatar_url'] = new_avatar
                        print(f"          🖼️ Avatar mis à jour: {name}")
                
                # Ajouter aux ingame_names si nécessaire
                ingame = data.get('ingame_names', [])
                if name not in ingame and name != data.get('name'):
                    update['ingame_names'] = ingame + [name]
                
                db.collection('players').document(doc_id).update(update)
                writes += 1
                print(f"          ⬆️ {name}")
            else:
                # Nouveau joueur
                doc_id = f"auto_{key.replace(' ', '_').replace('.', '_')[:50]}"
                db.collection('players').document(doc_id).set({
                    'name': name,
                    'steam_id': doc_id,
                    'roles': ['Joueur'],
                    'ingame_names': [],
                    'created_at': firestore.SERVER_TIMESTAMP,
                    'last_seen': firestore.SERVER_TIMESTAMP,
                    'connected_at': now.isoformat(),
                    'current_session_start': session_time,
                    'total_time_seconds': 0,
                    'session_count': 1,
                    'is_auto_detected': True
                })
                writes += 1
                print(f"          ➕ {name}")
        
        # 5. Handle LEFT players
        for name in left:
            key = name.lower().strip()
            existing = players_cache.get(key) or players_cache.get(normalize_name(name))
            
            if existing:
                doc_id, data = existing
                prev_player = previous_data.get(name, {})
                session_time = prev_player.get('time', 0)
                
                current_total = data.get('total_time_seconds', 0)
                new_total = current_total + session_time
                
                db.collection('players').document(doc_id).update({
                    'total_time_seconds': new_total,
                    'last_seen': firestore.SERVER_TIMESTAMP,
                    'connected_at': firestore.DELETE_FIELD
                })
                writes += 1
                print(f"          ⬇️ {name} (+{session_time//60}m)")
        
        # 6. Update live status
        live_players = []
        for name, t in current_players.items():
            player_data = {'name': name, 'time': t}
            if name in previous_data and 'connected_at' in previous_data[name]:
                player_data['connected_at'] = previous_data[name]['connected_at']
            elif name in joined:
                player_data['connected_at'] = now.isoformat()
            live_players.append(player_data)
        
        db.collection('live').document('status').set({
            "ok": True,
            "count": server_data['count'],
            "players": live_players,
            "serverName": server_data['server_name'],
            "map": server_data['map'],
            "maxPlayers": server_data['max_players'],
            "timestamp": now.isoformat(),
            "updatedAt": now.isoformat()
        })
        writes += 1
        
        # 7. Daily stats - only if new peak
        today = france_now.strftime('%Y-%m-%d')
        hour = france_now.hour
        stats_ref = db.collection('stats').document('daily').collection('days').document(today)
        
        try:
            stats_doc = stats_ref.get()
            if stats_doc.exists:
                data = stats_doc.to_dict()
                hourly = data.get('hourly', {})
                hour_peak = hourly.get(str(hour), 0)
                daily_peak = data.get('peak', 0)
                
                if server_data['count'] > hour_peak or server_data['count'] > daily_peak:
                    hourly[str(hour)] = max(hour_peak, server_data['count'])
                    stats_ref.update({
                        'peak': max(daily_peak, server_data['count']),
                        'hourly': hourly,
                        'last_update': firestore.SERVER_TIMESTAMP
                    })
                    writes += 1
            else:
                stats_ref.set({
                    'date': today,
                    'peak': server_data['count'],
                    'hourly': {str(hour): server_data['count']},
                    'created_at': firestore.SERVER_TIMESTAMP
                })
                writes += 1
        except:
            pass
        
        # 8. Records - only if new record
        try:
            records_ref = db.collection('stats').document('records')
            records_doc = records_ref.get()
            
            if records_doc.exists:
                if server_data['count'] > records_doc.to_dict().get('peak_count', 0):
                    records_ref.update({
                        'peak_count': server_data['count'],
                        'peak_date': now.isoformat()
                    })
                    writes += 1
                    print(f"    🏆 Record: {server_data['count']}!")
            else:
                records_ref.set({'peak_count': server_data['count'], 'peak_date': now.isoformat()})
                writes += 1
        except:
            pass
        
        print(f"       ✅ {writes} writes")
        return True
        
    except Exception as e:
        print(f"    ❌ Sync error: {e}")
        try:
            db.collection('live').document('status').set({
                "ok": False, "error": str(e), "count": 0, "players": [],
                "timestamp": now.isoformat(), "updatedAt": now.isoformat()
            })
        except:
            pass
        return False

def main():
    print("=" * 50)
    print("🎮 GMod Status v8.1")
    print("=" * 50)
    
    try:
        db = init_firebase()
        print("✅ Firebase OK")
    except Exception as e:
        print(f"❌ Firebase: {e}")
        sys.exit(1)
    
    # Attendre la prochaine minute :00
    wait_for_next_minute()
    
    now = datetime.now(timezone.utc)
    france_now = get_france_time()
    
    print(f"🎮 {france_now.strftime('%H:%M:%S')} - Query {GMOD_HOST}:{GMOD_PORT}")
    
    server_data = query_gmod_server(GMOD_HOST, GMOD_PORT)
    
    if server_data:
        print(f"    ✅ {server_data['count']}/{server_data['max_players']} on {server_data['map']}")
        
        for p in server_data['players'][:3]:
            m = p['time'] // 60
            print(f"       • {p['name']} ({m//60}h{m%60:02d}m)" if m >= 60 else f"       • {p['name']} ({m}m)")
        if len(server_data['players']) > 3:
            print(f"       ... +{len(server_data['players']) - 3}")
        
        sync_to_firebase(db, server_data, now, france_now)
    else:
        print("    ❌ Offline")
        try:
            db.collection('live').document('status').set({
                "ok": False, "error": "Offline", "count": 0, "players": [],
                "timestamp": now.isoformat(), "updatedAt": now.isoformat()
            })
        except:
            pass

if __name__ == '__main__':
    main()
