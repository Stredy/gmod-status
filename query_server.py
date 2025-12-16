#!/usr/bin/env python3
"""
GMod Server Status v12 - PRODUCTION OPTIMIZED
- 30 queries per workflow (30 minutes)
- Cache: players + stats + offline state
- Workflow every 30 minutes via cron

With 250 players:
- Init: 252 reads (once per workflow)
- Queries: 30 reads (1 per minute)
- Total: 282 reads per workflow
- 2 workflows/hour = 564 reads/hour
- 24h = 13,536 reads/day (27% of quota)
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
QUERIES_PER_RUN = 30

# ============================================
# Memory Cache
# ============================================
cache = {
    # Stats cache
    'hourly_stats': {},
    'daily_peak': 0,
    'record_peak': 0,
    'today_date': None,
    # Players cache
    'players': {},
    'players_by_name': {},
    # Offline state cache (NEW!)
    'is_offline': False,
    'offline_since': None,
    # Previous live state (for comparison)
    'prev_names': set(),
    'prev_count': 0,
    # Mapping direct: nom live -> doc_id (pour retrouver les joueurs au départ)
    'live_to_doc': {},
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
                player_list.append({'name': p.name.strip(), 'time': int(p.duration)})
        return {
            'server_name': info.server_name,
            'map': info.map_name,
            'players': player_list,
            'max_players': info.max_players,
            'count': len(player_list)
        }
    except Exception as e:
        print(f"    ❌ Serveur: {e}")
        return None

def init_cache(db, france_now):
    """Load all caches once at workflow start"""
    global cache
    reads = 0
    today = france_now.strftime('%Y-%m-%d')
    
    # 1. Stats du jour
    try:
        stats_doc = db.collection('stats').document('daily').collection('days').document(today).get()
        reads += 1
        if stats_doc.exists:
            data = stats_doc.to_dict()
            cache['hourly_stats'] = {int(k): v for k, v in data.get('hourly', {}).items()}
            cache['daily_peak'] = data.get('peak', 0)
    except:
        pass
    
    # 2. Record
    try:
        records_doc = db.collection('stats').document('records').get()
        reads += 1
        if records_doc.exists:
            cache['record_peak'] = records_doc.to_dict().get('peak_count', 0)
    except:
        pass
    
    # 3. Tous les joueurs
    try:
        all_players = db.collection('players').get()
        player_count = 0
        for doc in all_players:
            player_count += 1
            data = doc.to_dict()
            doc_id = doc.id
            cache['players'][doc_id] = data
            name = data.get('name', '')
            # Index ONLY by Steam name, NOT by ingame_names
            cache['players_by_name'][name.lower().strip()] = doc_id
            cache['players_by_name'][normalize_name(name)] = doc_id
        reads += player_count
        print(f"    👥 {player_count} joueurs")
    except Exception as e:
        print(f"    ⚠️ Erreur joueurs: {e}")
    
    # 4. État live actuel (pour initialiser prev_names et live_to_doc)
    try:
        live_doc = db.collection('live').document('status').get()
        reads += 1
        if live_doc.exists:
            data = live_doc.to_dict()
            cache['is_offline'] = not data.get('ok', True)
            cache['prev_count'] = data.get('count', 0)
            for p in data.get('players', []):
                live_name = p['name']
                cache['prev_names'].add(live_name)
                # Chercher le doc_id pour ce joueur et créer le mapping
                found = find_player(live_name)
                if found:
                    cache['live_to_doc'][live_name] = found[0]
    except:
        pass
    
    # 5. Vérifier les avatars manquants pour les joueurs en ligne
    avatars_checked = 0
    avatars_added = 0
    for live_name in cache['prev_names']:
        found = find_player(live_name)
        if found:
            doc_id, player_data = found
            steam_id = player_data.get('steam_id', '')
            current_avatar = player_data.get('avatar_url', '')
            
            # Si pas d'avatar et Steam ID valide, récupérer
            if not current_avatar and steam_id.startswith('STEAM_'):
                avatars_checked += 1
                new_avatar = fetch_steam_avatar(steam_id)
                if new_avatar:
                    db.collection('players').document(doc_id).update({'avatar_url': new_avatar})
                    update_player_cache(doc_id, {**player_data, 'avatar_url': new_avatar})
                    avatars_added += 1
    
    if avatars_checked > 0:
        print(f"    🖼️ Avatars: {avatars_added}/{avatars_checked} ajoutés")
    
    cache['today_date'] = today
    print(f"    📦 Stats H{france_now.hour}, peak={cache['daily_peak']}, record={cache['record_peak']}")
    print(f"    🔗 {len(cache['live_to_doc'])} joueurs mappés")
    print(f"    📊 Init: {reads} reads")
    return reads

def find_player(name):
    key = name.lower().strip()
    doc_id = cache['players_by_name'].get(key) or cache['players_by_name'].get(normalize_name(name))
    if doc_id and doc_id in cache['players']:
        return (doc_id, cache['players'][doc_id])
    return None

def update_player_cache(doc_id, data):
    global cache
    if doc_id in cache['players']:
        cache['players'][doc_id].update(data)
    else:
        cache['players'][doc_id] = data
    # Index ONLY by Steam name, NOT by ingame_names
    name = data.get('name', '')
    if name:
        cache['players_by_name'][name.lower().strip()] = doc_id
        cache['players_by_name'][normalize_name(name)] = doc_id

def sync_to_firebase(db, server_data: dict, now: datetime, france_now: datetime):
    """Sync with Firebase using cache - ZERO unnecessary reads"""
    global cache
    
    try:
        current_players = {p['name']: p['time'] for p in server_data['players']}
        current_names = set(current_players.keys())
        current_count = server_data['count']
        today = france_now.strftime('%Y-%m-%d')
        hour = france_now.hour
        
        writes = 0
        reads = 0
        
        # Reset offline state (server is responding)
        cache['is_offline'] = False
        
        # Day change?
        if cache['today_date'] != today:
            print(f"       🌅 Nouveau jour")
            cache['hourly_stats'] = {}
            cache['daily_peak'] = 0
            cache['today_date'] = today
        
        # Use cached previous state instead of reading!
        previous_names = cache['prev_names']
        previous_count = cache['prev_count']
        
        # Detect changes
        joined = current_names - previous_names
        left = previous_names - current_names
        stayed = current_names & previous_names
        players_identical = (current_names == previous_names) and (current_count == previous_count)
        
        print(f"       📊 {current_count} | +{len(joined)} -{len(left)} ={len(stayed)}")
        
        # Handle JOINED
        for name in joined:
            session_time = current_players[name]
            existing = find_player(name)
            
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
                
                # Avatar: fetch only if missing or on new session
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
                
                db.collection('players').document(doc_id).update(update)
                writes += 1
                update_player_cache(doc_id, {**data, **update})
                # Mapper le nom live au doc_id
                cache['live_to_doc'][name] = doc_id
                print(f"          ⬆️ {name}")
            else:
                key = name.lower().strip()
                doc_id = f"auto_{key.replace(' ', '_').replace('.', '_')[:50]}"
                new_player = {
                    'name': name, 'steam_id': doc_id, 'roles': ['Joueur'],
                    'ingame_names': [], 'created_at': firestore.SERVER_TIMESTAMP,
                    'last_seen': firestore.SERVER_TIMESTAMP, 'connected_at': now.isoformat(),
                    'current_session_start': session_time, 'total_time_seconds': 0,
                    'session_count': 1, 'is_auto_detected': True
                }
                db.collection('players').document(doc_id).set(new_player)
                writes += 1
                update_player_cache(doc_id, new_player)
                # Mapper le nom live au doc_id
                cache['live_to_doc'][name] = doc_id
                print(f"          🆕 {name}")
        
        # Handle LEFT - need to read live/status to get session times
        if left:
            # Only read if someone left (to get their session time)
            live_doc = db.collection('live').document('status').get()
            reads += 1
            prev_data = {}
            if live_doc.exists:
                for p in live_doc.to_dict().get('players', []):
                    prev_data[p['name']] = p
            
            for name in left:
                # Utiliser le mapping direct d'abord, puis fallback sur find_player
                doc_id = cache['live_to_doc'].get(name)
                data = None
                
                if doc_id and doc_id in cache['players']:
                    data = cache['players'][doc_id]
                else:
                    # Fallback: chercher par nom
                    existing = find_player(name)
                    if existing:
                        doc_id, data = existing
                
                if doc_id and data:
                    prev_session = prev_data.get(name, {}).get('time', 0)
                    new_total = data.get('total_time_seconds', 0) + prev_session
                    update = {
                        'total_time_seconds': new_total,
                        'last_seen': firestore.SERVER_TIMESTAMP,
                        'current_session_start': None
                    }
                    db.collection('players').document(doc_id).update(update)
                    writes += 1
                    update_player_cache(doc_id, {**data, **update})
                    # Retirer du mapping
                    cache['live_to_doc'].pop(name, None)
                    print(f"          👋 {name} (+{prev_session//60}min)")
                else:
                    # Joueur non trouvé - log warning
                    print(f"          ⚠️ {name} non trouvé dans le cache!")
                    cache['live_to_doc'].pop(name, None)
        
        # Live status - only if changed
        if not players_identical:
            db.collection('live').document('status').set({
                'ok': True, 'count': current_count,
                'max': server_data['max_players'], 'map': server_data['map'],
                'server': server_data['server_name'], 'players': server_data['players'],
                'timestamp': now.isoformat(), 'updatedAt': now.isoformat()
            })
            writes += 1
        else:
            print(f"       ⏭️ Live identique")
        
        # Update cache for next iteration
        cache['prev_names'] = current_names.copy()
        cache['prev_count'] = current_count
        
        # Stats (use cache)
        cached_hour = cache['hourly_stats'].get(hour, -1)
        if cached_hour == -1 or current_count > cached_hour:
            cache['hourly_stats'][hour] = max(cached_hour if cached_hour >= 0 else 0, current_count)
            cache['daily_peak'] = max(cache['daily_peak'], current_count)
            hourly_fb = {str(k): v for k, v in cache['hourly_stats'].items()}
            db.collection('stats').document('daily').collection('days').document(today).set({
                'date': today, 'peak': cache['daily_peak'],
                'hourly': hourly_fb, 'last_update': firestore.SERVER_TIMESTAMP
            }, merge=True)
            writes += 1
            print(f"       📈 H{hour}: {current_count}")
        else:
            print(f"       ⏭️ H{hour} cache: {cached_hour}")
        
        # Records (use cache)
        if current_count > cache['record_peak']:
            cache['record_peak'] = current_count
            db.collection('stats').document('records').set({
                'peak_count': current_count, 'peak_date': now.isoformat()
            })
            writes += 1
            print(f"       🏆 Record: {current_count}!")
        
        print(f"       ✅ {writes}W/{reads}R")
        return True
        
    except Exception as e:
        print(f"    ❌ Sync: {e}")
        import traceback
        traceback.print_exc()
        return False

def mark_offline(db):
    """Mark server offline - SAVE player stats before clearing!"""
    global cache
    
    # Already offline in cache? Skip everything!
    if cache['is_offline']:
        print(f"       ⏭️ Déjà offline (cache)")
        return
    
    try:
        now = datetime.now(timezone.utc)
        writes = 0
        reads = 0
        
        # IMPORTANT: Sauvegarder les stats des joueurs qui étaient connectés!
        if cache['prev_names']:
            # Lire le dernier état live pour avoir les temps de session
            live_doc = db.collection('live').document('status').get()
            reads += 1
            prev_data = {}
            if live_doc.exists:
                for p in live_doc.to_dict().get('players', []):
                    prev_data[p['name']] = p
            
            # Traiter chaque joueur comme "parti"
            for name in cache['prev_names']:
                doc_id = cache['live_to_doc'].get(name)
                data = None
                
                if doc_id and doc_id in cache['players']:
                    data = cache['players'][doc_id]
                else:
                    existing = find_player(name)
                    if existing:
                        doc_id, data = existing
                
                if doc_id and data:
                    prev_session = prev_data.get(name, {}).get('time', 0)
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
                else:
                    print(f"          ⚠️ {name} non trouvé!")
        
        # Mark in Firebase
        db.collection('live').document('status').set({
            'ok': False, 'count': 0, 'players': [],
            'timestamp': now.isoformat(), 'updatedAt': now.isoformat()
        })
        writes += 1
        
        # Update cache
        cache['is_offline'] = True
        cache['prev_names'] = set()
        cache['prev_count'] = 0
        cache['live_to_doc'] = {}
        
        print(f"       ⚠️ Offline ({writes}W/{reads}R)")
    except Exception as e:
        print(f"    ❌ Offline: {e}")

def main():
    print("=" * 50)
    print(f"🎮 GMod Status v12 ({QUERIES_PER_RUN} queries/30min)")
    print("=" * 50)
    
    try:
        db = init_firebase()
        print("✅ Firebase OK")
    except Exception as e:
        print(f"❌ Firebase: {e}")
        sys.exit(1)
    
    wait_for_next_minute()
    
    france_now = get_france_time()
    init_cache(db, france_now)
    
    for i in range(QUERIES_PER_RUN):
        now = datetime.now(timezone.utc)
        france_now = get_france_time()
        
        print(f"\n[{i+1}/{QUERIES_PER_RUN}] 🕐 {france_now.strftime('%H:%M:%S')}")
        
        server_data = query_gmod_server(GMOD_HOST, GMOD_PORT)
        
        if server_data:
            print(f"    ✅ {server_data['count']}/{server_data['max_players']} on {server_data['map']}")
            sync_to_firebase(db, server_data, now, france_now)
        else:
            mark_offline(db)
        
        if i < QUERIES_PER_RUN - 1:
            time.sleep(60)
    
    print(f"\n🏁 Terminé")

if __name__ == "__main__":
    main()
