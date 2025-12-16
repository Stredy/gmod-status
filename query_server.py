#!/usr/bin/env python3
"""
GMod Server Status v9 - ULTRA SMART Firebase Optimization
- Writes ONLY when data actually changes
- Hourly stats: write once per hour, skip if same value
- Avatars: fetch only if missing, check only on new session
- Live status: skip write if identical to previous
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
    ULTRA SMART sync - minimizes Firebase reads/writes
    """
    try:
        current_players = {p['name']: p['time'] for p in server_data['players']}
        current_names = set(current_players.keys())
        current_count = server_data['count']
        
        # 1. Get previous state (1 read)
        live_doc = db.collection('live').document('status').get()
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
        
        # Check if player list is identical (same names AND same count)
        players_identical = (current_names == previous_names) and (current_count == previous_count)
        
        print(f"       📊 {current_count} joueurs | +{len(joined)} | -{len(left)} | ={len(stayed)}")
        
        writes = 0
        reads = 1  # live/status déjà lu
        
        # 3. Load player docs ONLY if someone joined or left
        players_cache: Dict[str, tuple] = {}
        
        if joined or left:
            all_players = list(db.collection('players').get())
            reads += 1
            for doc in all_players:
                data = doc.to_dict()
                name = data.get('name', '')
                players_cache[name.lower().strip()] = (doc.id, data)
                players_cache[normalize_name(name)] = (doc.id, data)
                for ingame in data.get('ingame_names', []):
                    players_cache[ingame.lower().strip()] = (doc.id, data)
                    players_cache[normalize_name(ingame)] = (doc.id, data)
        
        # 4. Handle JOINED players
        for name in joined:
            session_time = current_players[name]
            key = name.lower().strip()
            key_norm = normalize_name(name)
            
            existing = players_cache.get(key) or players_cache.get(key_norm)
            
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
                
                # SMART AVATAR LOGIC:
                # - If has valid Steam ID but NO avatar → fetch avatar
                # - If has valid Steam ID AND avatar → check if changed (new session = good time to check)
                if steam_id.startswith('STEAM_'):
                    if not current_avatar:
                        # No avatar yet → fetch and add
                        new_avatar = fetch_steam_avatar(steam_id)
                        if new_avatar:
                            update['avatar_url'] = new_avatar
                            print(f"          🖼️ Avatar ajouté: {name}")
                    else:
                        # Has avatar → check if changed (only on new session)
                        new_avatar = fetch_steam_avatar(steam_id)
                        if new_avatar and new_avatar != current_avatar:
                            update['avatar_url'] = new_avatar
                            print(f"          🖼️ Avatar mis à jour: {name}")
                
                # Add to ingame_names if needed
                ingame = data.get('ingame_names', [])
                if name not in ingame and name != data.get('name'):
                    update['ingame_names'] = ingame + [name]
                
                db.collection('players').document(doc_id).update(update)
                writes += 1
                print(f"          ⬆️ {name} (session #{data.get('session_count', 0) + 1})")
            else:
                # New auto-detected player (no Steam ID, no avatar possible)
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
                print(f"          🆕 {name} (auto)")
        
        # 5. Handle LEFT players - add session time
        for name in left:
            key = name.lower().strip()
            key_norm = normalize_name(name)
            existing = players_cache.get(key) or players_cache.get(key_norm)
            
            if existing:
                doc_id, data = existing
                prev_session = previous_data.get(name, {}).get('time', 0)
                current_total = data.get('total_time_seconds', 0)
                new_total = current_total + prev_session
                
                db.collection('players').document(doc_id).update({
                    'total_time_seconds': new_total,
                    'last_seen': firestore.SERVER_TIMESTAMP,
                    'current_session_start': None
                })
                writes += 1
                
                mins = prev_session // 60
                print(f"          👋 {name} (+{mins}min, total: {new_total // 3600}h)")
        
        # 6. SMART Live status update - ONLY if something changed
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
            print(f"       ✏️ Live status mis à jour")
        else:
            print(f"       ⏭️ Live status identique, pas d'écriture")
        
        # 7. SMART Daily stats - only write if value changed for this hour
        today = france_now.strftime('%Y-%m-%d')
        hour = france_now.hour
        stats_ref = db.collection('stats').document('daily').collection('days').document(today)
        
        try:
            stats_doc = stats_ref.get()
            reads += 1
            
            if stats_doc.exists:
                data = stats_doc.to_dict()
                hourly = data.get('hourly', {})
                hour_value = hourly.get(str(hour), -1)  # -1 means not set
                daily_peak = data.get('peak', 0)
                
                # Only write if:
                # - Hour not yet recorded (hour_value == -1)
                # - OR new value is higher (new peak)
                if hour_value == -1 or current_count > hour_value:
                    hourly[str(hour)] = max(hour_value if hour_value >= 0 else 0, current_count)
                    new_peak = max(daily_peak, current_count)
                    
                    stats_ref.update({
                        'peak': new_peak,
                        'hourly': hourly,
                        'last_update': firestore.SERVER_TIMESTAMP
                    })
                    writes += 1
                    print(f"       📈 Stats {today} H{hour}: {current_count} joueurs")
                else:
                    print(f"       ⏭️ Stats H{hour} déjà à {hour_value}, pas d'écriture")
            else:
                # First entry for this day
                stats_ref.set({
                    'date': today,
                    'peak': current_count,
                    'hourly': {str(hour): current_count},
                    'created_at': firestore.SERVER_TIMESTAMP
                })
                writes += 1
                print(f"       📈 Stats {today} créées (H{hour}: {current_count})")
        except Exception as e:
            print(f"       ⚠️ Stats error: {e}")
        
        # 8. Records - only if new record
        try:
            records_ref = db.collection('stats').document('records')
            records_doc = records_ref.get()
            reads += 1
            
            if records_doc.exists:
                if current_count > records_doc.to_dict().get('peak_count', 0):
                    records_ref.update({
                        'peak_count': current_count,
                        'peak_date': now.isoformat()
                    })
                    writes += 1
                    print(f"       🏆 Nouveau record: {current_count}!")
            else:
                records_ref.set({'peak_count': current_count, 'peak_date': now.isoformat()})
                writes += 1
        except:
            pass
        
        print(f"       ✅ {writes} writes / {reads} reads")
        return True
        
    except Exception as e:
        print(f"    ❌ Sync error: {e}")
        import traceback
        traceback.print_exc()
        return False

def mark_offline(db):
    """Marquer le serveur hors ligne - SMART: skip if already offline"""
    try:
        now = datetime.now(timezone.utc)
        
        # Check current state first
        live_doc = db.collection('live').document('status').get()
        if live_doc.exists:
            current = live_doc.to_dict()
            if current.get('ok') == False and current.get('count', 0) == 0:
                # Already marked as offline, skip write
                print(f"       ⏭️ Déjà hors ligne, pas d'écriture")
                return
        
        db.collection('live').document('status').set({
            'ok': False,
            'count': 0,
            'players': [],
            'timestamp': now.isoformat(),
            'updatedAt': now.isoformat()
        })
        print(f"       ⚠️ Serveur marqué hors ligne (1 write)")
    except Exception as e:
        print(f"    ❌ Offline error: {e}")

def main():
    print("=" * 50)
    print("🎮 GMod Status v9 - ULTRA SMART")
    print("=" * 50)
    
    # Init
    try:
        db = init_firebase()
        print("✅ Firebase OK")
    except Exception as e:
        print(f"❌ Firebase error: {e}")
        sys.exit(1)
    
    # Wait for minute sync
    wait_for_next_minute()
    
    # Main loop
    iteration = 0
    while True:
        iteration += 1
        now = datetime.now(timezone.utc)
        france_now = get_france_time()
        
        print(f"\n[{iteration}] 🕐 {france_now.strftime('%H:%M:%S')}")
        
        # Query server
        server_data = query_gmod_server(GMOD_HOST, GMOD_PORT)
        
        if server_data:
            print(f"    ✅ {server_data['count']}/{server_data['max_players']} on {server_data['map']}")
            sync_to_firebase(db, server_data, now, france_now)
        else:
            print(f"    ❌ Serveur inaccessible")
            mark_offline(db)
        
        # Next iteration
        if iteration >= 55:  # ~55 minutes max
            print("\n🏁 Fin du workflow")
            break
        
        # Wait exactly 60 seconds
        time.sleep(60)

if __name__ == "__main__":
    main()
