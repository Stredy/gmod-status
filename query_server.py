#!/usr/bin/env python3
"""
GMod Server Status v11 - Optimized for GitHub Actions
- Runs 10 queries per workflow execution (~10 minutes total)
- Cache loaded ONCE at start, reused for all 10 queries
- Workflow runs every 10 minutes via cron

With 200 players:
- Reads per workflow: ~202 (init) + 10 (live/status) = ~212
- Reads per hour: 6 workflows × 212 = ~1,272
- Reads per day: ~30,528 (well under 50k free quota)
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
QUERIES_PER_RUN = 10  # Number of queries per workflow execution (1 per minute)

# ============================================
# Memory Cache - Loaded once per workflow run
# ============================================
cache = {
    'hourly_stats': {},
    'daily_peak': 0,
    'record_peak': 0,
    'today_date': None,
    'players': {},
    'players_by_name': {},
}

def get_france_time():
    return datetime.now(timezone.utc) + timedelta(hours=1)

def wait_for_next_minute():
    now = datetime.now()
    seconds_to_wait = 60 - now.second - (now.microsecond / 1_000_000)
    if 0 < seconds_to_wait < 60:
        print(f"    ⏳ Attente {seconds_to_wait:.1f}s → XX:{(now.minute + 1) % 60:02d}:00")
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

def init_cache(db, france_now):
    """Load all caches once at workflow start"""
    global cache
    reads = 0
    
    today = france_now.strftime('%Y-%m-%d')
    hour = france_now.hour
    
    # 1. Load today's stats
    try:
        stats_doc = db.collection('stats').document('daily').collection('days').document(today).get()
        reads += 1
        if stats_doc.exists:
            data = stats_doc.to_dict()
            cache['hourly_stats'] = {int(k): v for k, v in data.get('hourly', {}).items()}
            cache['daily_peak'] = data.get('peak', 0)
    except:
        pass
    
    # 2. Load record
    try:
        records_doc = db.collection('stats').document('records').get()
        reads += 1
        if records_doc.exists:
            cache['record_peak'] = records_doc.to_dict().get('peak_count', 0)
    except:
        pass
    
    # 3. Load ALL players
    try:
        all_players = db.collection('players').get()
        player_count = 0
        for doc in all_players:
            player_count += 1
            data = doc.to_dict()
            doc_id = doc.id
            
            cache['players'][doc_id] = data
            
            name = data.get('name', '')
            cache['players_by_name'][name.lower().strip()] = doc_id
            cache['players_by_name'][normalize_name(name)] = doc_id
            
            for ingame in data.get('ingame_names', []):
                cache['players_by_name'][ingame.lower().strip()] = doc_id
                cache['players_by_name'][normalize_name(ingame)] = doc_id
        
        reads += player_count
        print(f"    👥 {player_count} joueurs en cache")
    except Exception as e:
        print(f"    ⚠️ Erreur joueurs: {e}")
    
    cache['today_date'] = today
    
    print(f"    📦 H{hour}={cache['hourly_stats'].get(hour, 'new')}, peak={cache['daily_peak']}, record={cache['record_peak']}")
    print(f"    📊 Init: {reads} reads")
    return reads

def find_player_in_cache(name):
    key = name.lower().strip()
    key_norm = normalize_name(name)
    doc_id = cache['players_by_name'].get(key) or cache['players_by_name'].get(key_norm)
    if doc_id and doc_id in cache['players']:
        return (doc_id, cache['players'][doc_id])
    return None

def update_player_cache(doc_id, data):
    global cache
    if doc_id in cache['players']:
        cache['players'][doc_id].update(data)
    else:
        cache['players'][doc_id] = data
    
    name = data.get('name', '')
    if name:
        cache['players_by_name'][name.lower().strip()] = doc_id
        cache['players_by_name'][normalize_name(name)] = doc_id
    
    for ingame in data.get('ingame_names', []):
        cache['players_by_name'][ingame.lower().strip()] = doc_id
        cache['players_by_name'][normalize_name(ingame)] = doc_id

def sync_to_firebase(db, server_data: dict, now: datetime, france_now: datetime):
    """Sync with Firebase - uses cache, minimal reads"""
    global cache
    
    try:
        current_players = {p['name']: p['time'] for p in server_data['players']}
        current_names = set(current_players.keys())
        current_count = server_data['count']
        today = france_now.strftime('%Y-%m-%d')
        hour = france_now.hour
        
        writes = 0
        reads = 0
        
        # Check day change
        if cache['today_date'] != today:
            print(f"       🌅 Nouveau jour")
            cache['hourly_stats'] = {}
            cache['daily_peak'] = 0
            cache['today_date'] = today
        
        # 1. Read live/status (only read per sync!)
        live_doc = db.collection('live').document('status').get()
        reads += 1
        
        previous_names: Set[str] = set()
        previous_data = {}
        previous_count = 0
        
        if live_doc.exists:
            prev = live_doc.to_dict()
            previous_count = prev.get('count', 0)
            for p in prev.get('players', []):
                previous_names.add(p['name'])
                previous_data[p['name']] = p
        
        # 2. Detect changes
        joined = current_names - previous_names
        left = previous_names - current_names
        stayed = current_names & previous_names
        players_identical = (current_names == previous_names) and (current_count == previous_count)
        
        print(f"       📊 {current_count} | +{len(joined)} -{len(left)} ={len(stayed)}")
        
        # 3. Handle JOINED (use cache!)
        for name in joined:
            session_time = current_players[name]
            existing = find_player_in_cache(name)
            
            if existing:
                doc_id, data = existing
                steam_id = data.get('steam_id', '')
                current_avatar = data.get('avatar_url', '')
                
                update = {
                    'last_seen': firestore.SERVER_TIMESTAMP,
                    'connected_at': now.isoformat(),
                    'current_session_start': session_time,
                    'session_count': data.get('session_count', 0) + 1
                }
                
                if steam_id.startswith('STEAM_'):
                    if not current_avatar:
                        new_avatar = fetch_steam_avatar(steam_id)
                        if new_avatar:
                            update['avatar_url'] = new_avatar
                            print(f"          🖼️ +avatar: {name}")
                    else:
                        new_avatar = fetch_steam_avatar(steam_id)
                        if new_avatar and new_avatar != current_avatar:
                            update['avatar_url'] = new_avatar
                            print(f"          🖼️ maj: {name}")
                
                ingame = data.get('ingame_names', [])
                if name not in ingame and name != data.get('name'):
                    update['ingame_names'] = ingame + [name]
                
                db.collection('players').document(doc_id).update(update)
                writes += 1
                update_player_cache(doc_id, {**data, **update})
                print(f"          ⬆️ {name}")
            else:
                key = name.lower().strip()
                doc_id = f"auto_{key.replace(' ', '_').replace('.', '_')[:50]}"
                new_player = {
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
                }
                db.collection('players').document(doc_id).set(new_player)
                writes += 1
                update_player_cache(doc_id, new_player)
                print(f"          🆕 {name}")
        
        # 4. Handle LEFT (use cache!)
        for name in left:
            existing = find_player_in_cache(name)
            if existing:
                doc_id, data = existing
                prev_session = previous_data.get(name, {}).get('time', 0)
                new_total = data.get('total_time_seconds', 0) + prev_session
                
                update = {
                    'total_time_seconds': new_total,
                    'last_seen': firestore.SERVER_TIMESTAMP,
                    'current_session_start': None
                }
                db.collection('players').document(doc_id).update(update)
                writes += 1
                update_player_cache(doc_id, {**data, **update})
                print(f"          👋 {name} (+{prev_session//60}min)")
        
        # 5. Live status
        if not players_identical:
            db.collection('live').document('status').set({
                'ok': True,
                'count': current_count,
                'max': server_data['max_players'],
                'map': server_data['map'],
                'server': server_data['server_name'],
                'players': server_data['players'],
                'timestamp': now.isoformat(),
                'updatedAt': now.isoformat()
            })
            writes += 1
        else:
            print(f"       ⏭️ Live identique")
        
        # 6. Stats (use cache!)
        cached_hour_value = cache['hourly_stats'].get(hour, -1)
        if cached_hour_value == -1 or current_count > cached_hour_value:
            cache['hourly_stats'][hour] = max(cached_hour_value if cached_hour_value >= 0 else 0, current_count)
            cache['daily_peak'] = max(cache['daily_peak'], current_count)
            
            hourly_for_fb = {str(k): v for k, v in cache['hourly_stats'].items()}
            db.collection('stats').document('daily').collection('days').document(today).set({
                'date': today,
                'peak': cache['daily_peak'],
                'hourly': hourly_for_fb,
                'last_update': firestore.SERVER_TIMESTAMP
            }, merge=True)
            writes += 1
            print(f"       📈 H{hour}: {current_count}")
        else:
            print(f"       ⏭️ H{hour} cache: {cached_hour_value}")
        
        # 7. Records (use cache!)
        if current_count > cache['record_peak']:
            cache['record_peak'] = current_count
            db.collection('stats').document('records').set({
                'peak_count': current_count,
                'peak_date': now.isoformat()
            })
            writes += 1
            print(f"       🏆 Record: {current_count}!")
        
        print(f"       ✅ {writes}W/{reads}R")
        return True
        
    except Exception as e:
        print(f"    ❌ Sync error: {e}")
        import traceback
        traceback.print_exc()
        return False

def mark_offline(db):
    try:
        now = datetime.now(timezone.utc)
        live_doc = db.collection('live').document('status').get()
        if live_doc.exists:
            current = live_doc.to_dict()
            if current.get('ok') == False:
                print(f"       ⏭️ Déjà offline")
                return
        
        db.collection('live').document('status').set({
            'ok': False,
            'count': 0,
            'players': [],
            'timestamp': now.isoformat(),
            'updatedAt': now.isoformat()
        })
        print(f"       ⚠️ Offline")
    except Exception as e:
        print(f"    ❌ Offline error: {e}")

def main():
    print("=" * 50)
    print(f"🎮 GMod Status v11 ({QUERIES_PER_RUN} queries)")
    print("=" * 50)
    
    try:
        db = init_firebase()
        print("✅ Firebase OK")
    except Exception as e:
        print(f"❌ Firebase: {e}")
        sys.exit(1)
    
    # Wait for next minute
    wait_for_next_minute()
    
    # Init cache ONCE
    france_now = get_france_time()
    total_reads = init_cache(db, france_now)
    total_writes = 0
    
    # Run queries
    for i in range(QUERIES_PER_RUN):
        now = datetime.now(timezone.utc)
        france_now = get_france_time()
        
        print(f"\n[{i+1}/{QUERIES_PER_RUN}] 🕐 {france_now.strftime('%H:%M:%S')}")
        
        server_data = query_gmod_server(GMOD_HOST, GMOD_PORT)
        
        if server_data:
            print(f"    ✅ {server_data['count']}/{server_data['max_players']} on {server_data['map']}")
            sync_to_firebase(db, server_data, now, france_now)
        else:
            print(f"    ❌ Serveur inaccessible")
            mark_offline(db)
        
        # Wait 1 minute between queries (except last)
        if i < QUERIES_PER_RUN - 1:
            time.sleep(60)
    
    print(f"\n🏁 Terminé")

if __name__ == "__main__":
    main()
