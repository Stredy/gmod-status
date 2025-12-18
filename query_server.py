#!/usr/bin/env python3
"""
GMod Server Status v16 - ROBUST TIME TRACKING
=============================================

Architecture du comptage de temps :
- session_baseline : temps de session (secondes) au moment où on commence à tracker
- session_started_at : timestamp ISO pour l'affichage frontend
- Quand un joueur part : time_to_add = final_time - session_baseline

Cas gérés :
1. Joueur rejoint → baseline = temps actuel, started_at = now - temps actuel
2. Joueur part → ajoute (temps final - baseline) au total
3. Changement de nom Steam → détecté, pas de double comptage
4. Redémarrage backend → baseline recalculé pour joueurs déjà connectés
5. Serveur offline → sauvegarde tous les temps correctement
6. Crash/Signal → sauvegarde gracieuse
"""

import os
import sys
import json
import re
import time
import signal
import unicodedata
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Set, Tuple, List
from html.parser import HTMLParser

import a2s
import firebase_admin
from firebase_admin import credentials, firestore

GMOD_HOST = os.environ.get('GMOD_HOST', '51.91.215.65')
GMOD_PORT = int(os.environ.get('GMOD_PORT', '27015'))
QUERIES_PER_RUN = 30

# Global db reference for signal handler
_db = None

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
    'players': {},           # doc_id -> player data
    'players_by_name': {},   # normalized_name -> doc_id
    # Offline state cache
    'is_offline': False,
    'offline_since': None,
    # Previous live state
    'prev_names': set(),
    'prev_count': 0,
    # Mapping: nom live -> doc_id
    'live_to_doc': {},
    # Tracking sessions: nom live -> {'baseline': int, 'doc_id': str}
    'active_sessions': {},
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

# ============================================
# Steam Avatar Parser
# ============================================
class SteamAvatarParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_inner = 0
        self.in_frame = 0
        self.animated = None
        self.static_candidates = []

    def _first_url_from_srcset(self, srcset):
        if not srcset:
            return None
        return srcset.split(",")[0].strip().split()[0]

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        classes = attrs_dict.get("class", "").split()
        if tag == "div":
            if "playerAvatarAutoSizeInner" in classes:
                self.in_inner += 1
            if "profile_avatar_frame" in classes:
                self.in_frame += 1
        if tag == "img" and self.in_inner and not self.in_frame:
            srcset = attrs_dict.get("srcset", "")
            src = attrs_dict.get("src", "")
            if "animated_avatar" in src or "animated_avatar" in srcset:
                url = self._first_url_from_srcset(srcset) or src
                if url:
                    self.animated = url
            else:
                url = self._first_url_from_srcset(srcset) or src
                if url:
                    self.static_candidates.append(url)

    def handle_endtag(self, tag):
        if tag == "div":
            if self.in_inner:
                self.in_inner -= 1
            if self.in_frame:
                self.in_frame -= 1

def fetch_steam_avatar(steam_id: str) -> Optional[str]:
    """Fetch avatar from Steam profile page"""
    steam64 = steam2_to_steam64(steam_id)
    if not steam64:
        return None
    
    url = f"https://steamcommunity.com/profiles/{steam64}"
    try:
        resp = requests.get(url, timeout=10, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        if resp.status_code != 200:
            return None
        parser = SteamAvatarParser()
        parser.feed(resp.text)
        return parser.animated or (parser.static_candidates[0] if parser.static_candidates else None)
    except Exception:
        return None

# ============================================
# Steam Profile Search
# ============================================
class SteamSearchParser(HTMLParser):
    """Parse Steam search results"""
    def __init__(self):
        super().__init__()
        self.results = []
        self.in_result = False
        self.current_url = None
    
    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag == 'a' and 'searchPersonaName' in attrs_dict.get('class', ''):
            self.current_url = attrs_dict.get('href')
    
    def handle_data(self, data):
        if self.current_url:
            self.results.append((data.strip(), self.current_url))
            self.current_url = None

def resolve_to_steamid64(session, profile_url, timeout=15):
    """Resolve a Steam profile URL to SteamID64"""
    try:
        if '/profiles/' in profile_url:
            match = re.search(r'/profiles/(\d+)', profile_url)
            if match:
                return match.group(1)
        
        xml_url = profile_url.rstrip('/') + '/?xml=1'
        r = session.get(xml_url, timeout=timeout)
        if r.status_code == 200:
            match = re.search(r'<steamID64>(\d+)</steamID64>', r.text)
            if match:
                return match.group(1)
    except Exception:
        pass
    return None

def steamid64_to_steam2(steamid64: str) -> Optional[str]:
    """Convert SteamID64 to STEAM_X:Y:Z format"""
    try:
        steam64_int = int(steamid64)
        y = steam64_int & 1
        z = (steam64_int - STEAMID64_BASE - y) // 2
        return f"STEAM_0:{y}:{z}"
    except:
        return None

def find_steam_profile(player_name: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Search for a Steam profile by player name.
    Returns: (steamid64, steam2, profile_url, avatar_url) or (None, None, None, None)
    """
    try:
        s = requests.Session()
        s.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept-Language': 'en-US,en;q=0.9'
        })
        
        # Get session ID
        s.get("https://steamcommunity.com", timeout=10)
        sessionid = s.cookies.get("sessionid")
        if not sessionid:
            return (None, None, None, None)
        
        # Search
        search_url = "https://steamcommunity.com/search/SearchCommunityAjax"
        r = s.get(search_url, params={
            "text": player_name,
            "filter": "users",
            "sessionid": sessionid,
            "page": 1
        }, timeout=15)
        
        if r.status_code != 200:
            return (None, None, None, None)
        
        data = r.json()
        html = data.get("html", "")
        
        parser = SteamSearchParser()
        parser.feed(html)
        
        if not parser.results:
            return (None, None, None, None)
        
        # Check first 5 results for exact match
        normalized_search = normalize_name(player_name)
        for result_name, profile_url in parser.results[:5]:
            if normalize_name(result_name) == normalized_search:
                steamid64 = resolve_to_steamid64(s, profile_url)
                if steamid64:
                    steam2 = steamid64_to_steam2(steamid64)
                    avatar_url = None
                    try:
                        resp = s.get(profile_url, timeout=10)
                        if resp.status_code == 200:
                            avatar_parser = SteamAvatarParser()
                            avatar_parser.feed(resp.text)
                            avatar_url = avatar_parser.animated or (avatar_parser.static_candidates[0] if avatar_parser.static_candidates else None)
                    except:
                        pass
                    return (steamid64, steam2, profile_url, avatar_url)
        
        return (None, None, None, None)
        
    except Exception as e:
        print(f"          ⚠️ Steam search error: {e}")
        return (None, None, None, None)

# ============================================
# Firebase Cache
# ============================================
def init_firebase():
    """Initialize Firebase with service account"""
    if firebase_admin._apps:
        return firestore.client()
    
    service_account_json = os.environ.get('FIREBASE_SERVICE_ACCOUNT')
    if not service_account_json:
        raise ValueError("FIREBASE_SERVICE_ACCOUNT not set")
    
    cred = credentials.Certificate(json.loads(service_account_json))
    firebase_admin.initialize_app(cred)
    return firestore.client()

def load_cache(db):
    """Load all necessary data into cache at startup"""
    global cache
    reads = 0
    
    print("    📦 Loading cache...")
    
    # Load all players
    players_docs = db.collection('players').get()
    reads += len(players_docs) + 1
    
    for doc in players_docs:
        data = doc.to_dict()
        doc_id = doc.id
        cache['players'][doc_id] = data
        
        # Index by normalized name
        name = data.get('name', '')
        if name:
            cache['players_by_name'][normalize_name(name)] = doc_id
        
        # Index by normalized ingame names
        for ig in data.get('ingame_names', []):
            cache['players_by_name'][normalize_name(ig)] = doc_id
    
    print(f"       ✓ {len(cache['players'])} joueurs")
    
    # Load today's stats
    france_now = get_france_time()
    today = france_now.strftime('%Y-%m-%d')
    cache['today_date'] = today
    
    stats_doc = db.collection('stats').document('daily').collection('days').document(today).get()
    reads += 1
    if stats_doc.exists:
        data = stats_doc.to_dict()
        cache['daily_peak'] = data.get('peak', 0)
        cache['hourly_stats'] = {int(k): v for k, v in data.get('hourly', {}).items()}
    
    # Load record
    record_doc = db.collection('stats').document('records').get()
    reads += 1
    if record_doc.exists:
        cache['record_peak'] = record_doc.to_dict().get('peak_count', 0)
    
    # Load live status for recovering active sessions
    live_doc = db.collection('live').document('status').get()
    reads += 1
    if live_doc.exists:
        live_data = live_doc.to_dict()
        if live_data.get('ok', False):
            # Server was online - recover session tracking
            for p in live_data.get('players', []):
                name = p['name']
                session_time = p.get('time', 0)
                cache['prev_names'].add(name)
                
                # Try to find the player's doc_id
                existing = find_player(name)
                if existing:
                    doc_id, _ = existing
                    cache['live_to_doc'][name] = doc_id
                    # Restore session tracking with current baseline
                    cache['active_sessions'][name] = {
                        'baseline': session_time,
                        'doc_id': doc_id
                    }
            
            cache['prev_count'] = live_data.get('count', 0)
            print(f"       ✓ Récupéré {len(cache['prev_names'])} sessions actives")
        else:
            cache['is_offline'] = True
    
    print(f"    ✅ Cache: {reads} reads")
    return reads

def update_player_cache(doc_id: str, data: dict):
    """Update player in cache"""
    cache['players'][doc_id] = data
    name = data.get('name', '')
    if name:
        cache['players_by_name'][normalize_name(name)] = doc_id
    for ig in data.get('ingame_names', []):
        cache['players_by_name'][normalize_name(ig)] = doc_id

def find_player(name: str) -> Optional[Tuple[str, dict]]:
    """Find player by name (Steam name or ingame name)"""
    normalized = normalize_name(name)
    doc_id = cache['players_by_name'].get(normalized)
    if doc_id and doc_id in cache['players']:
        return (doc_id, cache['players'][doc_id])
    return None

# ============================================
# Server Query
# ============================================
def query_gmod_server() -> Optional[dict]:
    """Query GMod server for current status"""
    try:
        address = (GMOD_HOST, GMOD_PORT)
        info = a2s.info(address, timeout=10)
        players_raw = a2s.players(address, timeout=10)
        
        player_list = []
        for p in players_raw:
            if p.name and p.name.strip():
                player_list.append({
                    'name': p.name.strip(),
                    'time': int(p.duration)  # Secondes depuis connexion au serveur
                })
        
        return {
            'ok': True,
            'count': len(player_list),
            'max_players': info.max_players,
            'map': info.map_name,
            'server_name': info.server_name,
            'players': player_list
        }
    except Exception as e:
        print(f"       ❌ Query: {e}")
        return None

# ============================================
# Core Sync Logic - ROBUST TIME TRACKING
# ============================================
def sync_to_firebase(db, server_data: dict, now: datetime, france_now: datetime):
    """
    Sync with Firebase using robust time tracking.
    
    Key principle: We track session_baseline (time when we started tracking)
    and only add (final_time - baseline) when player leaves.
    """
    global cache
    
    try:
        # Build current state
        current_players = {p['name']: p['time'] for p in server_data['players']}
        current_names = set(current_players.keys())
        current_count = server_data['count']
        today = france_now.strftime('%Y-%m-%d')
        hour = france_now.hour
        
        writes = 0
        reads = 0
        
        # Reset offline state
        cache['is_offline'] = False
        
        # Day change?
        if cache['today_date'] != today:
            print(f"       🌅 Nouveau jour")
            cache['hourly_stats'] = {}
            cache['daily_peak'] = 0
            cache['today_date'] = today
        
        # Previous state
        previous_names = cache['prev_names']
        previous_count = cache['prev_count']
        
        # Detect changes
        joined = current_names - previous_names
        left = previous_names - current_names
        stayed = current_names & previous_names
        players_identical = (current_names == previous_names) and (current_count == previous_count)
        
        print(f"       📊 {current_count} | +{len(joined)} -{len(left)} ={len(stayed)}")
        
        # ============================================
        # PHASE 1: Detect Steam name changes
        # ============================================
        # A name change = same SteamID in both joined and left
        name_changes = {}  # old_name -> (new_name, steam_id, avatar_url)
        
        for new_name in list(joined):
            # Check if this player has a SteamID that was in "left"
            steamid64, steam2, profile_url, avatar_url = find_steam_profile(new_name)
            
            if steam2:
                # Check if this SteamID was connected under a different name
                for old_name in list(left):
                    old_session = cache['active_sessions'].get(old_name)
                    if old_session and old_session['doc_id'] == steam2:
                        # Found a name change!
                        name_changes[old_name] = (new_name, steam2, avatar_url)
                        print(f"          🔄 Détecté: {old_name} → {new_name}")
                        break
        
        # Remove name changes from joined/left sets
        for old_name, (new_name, steam2, avatar_url) in name_changes.items():
            joined.discard(new_name)
            left.discard(old_name)
        
        # ============================================
        # PHASE 2: Handle name changes (NO time counting)
        # ============================================
        for old_name, (new_name, steam2, avatar_url) in name_changes.items():
            old_session = cache['active_sessions'].get(old_name, {})
            data = cache['players'].get(steam2, {})
            new_time = current_players[new_name]
            
            update = {
                'name': new_name,
                'last_seen': firestore.SERVER_TIMESTAMP,
            }
            
            # Update avatar if found
            if avatar_url and avatar_url != data.get('avatar_url'):
                update['avatar_url'] = avatar_url
            
            db.collection('players').document(steam2).update(update)
            writes += 1
            update_player_cache(steam2, {**data, **update})
            
            # Update mappings - transfer session tracking to new name
            cache['live_to_doc'].pop(old_name, None)
            cache['live_to_doc'][new_name] = steam2
            
            # Transfer session with SAME baseline (session continues)
            cache['active_sessions'].pop(old_name, None)
            cache['active_sessions'][new_name] = {
                'baseline': old_session.get('baseline', new_time),
                'doc_id': steam2
            }
            
            print(f"          🔄 {old_name} → {new_name} (session continue)")
        
        # ============================================
        # PHASE 3: Handle real departures (ADD time)
        # ============================================
        for name in left:
            session = cache['active_sessions'].get(name)
            
            if not session:
                # No session tracking - try to find player
                existing = find_player(name)
                if existing:
                    doc_id, data = existing
                    # We don't have baseline, so we can't add time accurately
                    # Just mark as offline
                    update = {
                        'last_seen': firestore.SERVER_TIMESTAMP,
                        'current_session_start': None
                    }
                    db.collection('players').document(doc_id).update(update)
                    writes += 1
                    update_player_cache(doc_id, {**data, **update})
                    print(f"          👋 {name} (pas de baseline)")
                else:
                    print(f"          ⚠️ {name} non trouvé!")
                
                cache['live_to_doc'].pop(name, None)
                cache['active_sessions'].pop(name, None)
                continue
            
            doc_id = session['doc_id']
            baseline = session['baseline']
            data = cache['players'].get(doc_id, {})
            
            # Get final session time from previous live status
            # (we need to read it because player is now gone)
            if not hasattr(sync_to_firebase, '_prev_player_times'):
                # Read live/status to get final times
                live_doc = db.collection('live').document('status').get()
                reads += 1
                sync_to_firebase._prev_player_times = {}
                if live_doc.exists:
                    for p in live_doc.to_dict().get('players', []):
                        sync_to_firebase._prev_player_times[p['name']] = p.get('time', 0)
            
            final_time = sync_to_firebase._prev_player_times.get(name, baseline)
            
            # Calculate time to add: final - baseline
            time_to_add = max(0, final_time - baseline)
            
            new_total = data.get('total_time_seconds', 0) + time_to_add
            
            update = {
                'total_time_seconds': new_total,
                'last_seen': firestore.SERVER_TIMESTAMP,
                'current_session_start': None
            }
            
            db.collection('players').document(doc_id).update(update)
            writes += 1
            update_player_cache(doc_id, {**data, **update})
            
            # Clean up
            cache['live_to_doc'].pop(name, None)
            cache['active_sessions'].pop(name, None)
            
            print(f"          👋 {name} (+{time_to_add//60}min, base={baseline//60}min)")
        
        # Clear temp storage
        if hasattr(sync_to_firebase, '_prev_player_times'):
            delattr(sync_to_firebase, '_prev_player_times')
        
        # ============================================
        # PHASE 4: Handle real arrivals (START tracking)
        # ============================================
        for name in joined:
            session_time = current_players[name]
            existing = find_player(name)
            
            if existing:
                doc_id, data = existing
                steam_id = data.get('steam_id', '')
                current_avatar = data.get('avatar_url', '')
                
                # Calculate session_started_at for frontend display
                session_started_at = (now - timedelta(seconds=session_time)).isoformat()
                
                update = {
                    'last_seen': firestore.SERVER_TIMESTAMP,
                    'connected_at': now.isoformat(),
                    'current_session_start': session_started_at,  # ISO timestamp for frontend
                    'session_count': data.get('session_count', 0) + 1
                }
                
                # Fetch avatar if missing
                if steam_id.startswith('STEAM_') and not current_avatar:
                    new_avatar = fetch_steam_avatar(steam_id)
                    if new_avatar:
                        update['avatar_url'] = new_avatar
                        print(f"          🖼️ +avatar: {name}")
                
                db.collection('players').document(doc_id).update(update)
                writes += 1
                update_player_cache(doc_id, {**data, **update})
                
                # Start session tracking
                cache['live_to_doc'][name] = doc_id
                cache['active_sessions'][name] = {
                    'baseline': session_time,
                    'doc_id': doc_id
                }
                
                print(f"          ⬆️ {name} (base={session_time//60}min)")
            else:
                # New player - try to find Steam profile
                steamid64, steam2, profile_url, avatar_url = find_steam_profile(name)
                
                # Calculate session_started_at for frontend
                session_started_at = (now - timedelta(seconds=session_time)).isoformat()
                
                if steam2:
                    # Check if SteamID already exists
                    existing_by_steamid = cache['players'].get(steam2)
                    
                    if existing_by_steamid:
                        # Update existing player with new name
                        update = {
                            'name': name,
                            'last_seen': firestore.SERVER_TIMESTAMP,
                            'connected_at': now.isoformat(),
                            'current_session_start': session_started_at,
                            'session_count': existing_by_steamid.get('session_count', 0) + 1
                        }
                        if avatar_url:
                            update['avatar_url'] = avatar_url
                        
                        db.collection('players').document(steam2).update(update)
                        writes += 1
                        update_player_cache(steam2, {**existing_by_steamid, **update})
                        
                        cache['live_to_doc'][name] = steam2
                        cache['active_sessions'][name] = {
                            'baseline': session_time,
                            'doc_id': steam2
                        }
                        
                        print(f"          ⬆️ {name} (steamid existant)")
                    else:
                        # Create new player with real SteamID
                        new_player = {
                            'name': name,
                            'steam_id': steam2,
                            'roles': ['Joueur'],
                            'ingame_names': [],
                            'created_at': firestore.SERVER_TIMESTAMP,
                            'last_seen': firestore.SERVER_TIMESTAMP,
                            'connected_at': now.isoformat(),
                            'current_session_start': session_started_at,
                            'total_time_seconds': 0,
                            'session_count': 1,
                            'is_auto_detected': False,
                            'avatar_url': avatar_url
                        }
                        
                        db.collection('players').document(steam2).set(new_player)
                        writes += 1
                        update_player_cache(steam2, new_player)
                        
                        cache['live_to_doc'][name] = steam2
                        cache['active_sessions'][name] = {
                            'baseline': session_time,
                            'doc_id': steam2
                        }
                        
                        print(f"          🆕✅ {name} ({steam2})")
                else:
                    # No Steam profile found - create auto_xxx
                    key = name.lower().strip()
                    doc_id = f"auto_{key.replace(' ', '_').replace('.', '_')[:50]}"
                    
                    existing_auto = cache['players'].get(doc_id)
                    if existing_auto:
                        update = {
                            'last_seen': firestore.SERVER_TIMESTAMP,
                            'connected_at': now.isoformat(),
                            'current_session_start': session_started_at,
                            'session_count': existing_auto.get('session_count', 0) + 1
                        }
                        db.collection('players').document(doc_id).update(update)
                        writes += 1
                        update_player_cache(doc_id, {**existing_auto, **update})
                    else:
                        new_player = {
                            'name': name,
                            'steam_id': doc_id,
                            'roles': ['Joueur'],
                            'ingame_names': [],
                            'created_at': firestore.SERVER_TIMESTAMP,
                            'last_seen': firestore.SERVER_TIMESTAMP,
                            'connected_at': now.isoformat(),
                            'current_session_start': session_started_at,
                            'total_time_seconds': 0,
                            'session_count': 1,
                            'is_auto_detected': True
                        }
                        db.collection('players').document(doc_id).set(new_player)
                        writes += 1
                        update_player_cache(doc_id, new_player)
                    
                    cache['live_to_doc'][name] = doc_id
                    cache['active_sessions'][name] = {
                        'baseline': session_time,
                        'doc_id': doc_id
                    }
                    
                    print(f"          🆕 {name} (auto)")
        
        # ============================================
        # PHASE 5: Update live status
        # ============================================
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
        
        # Update cache
        cache['prev_names'] = current_names.copy()
        cache['prev_count'] = current_count
        
        # ============================================
        # PHASE 6: Stats
        # ============================================
        cached_hour = cache['hourly_stats'].get(hour, -1)
        if cached_hour == -1 or current_count > cached_hour:
            cache['hourly_stats'][hour] = max(cached_hour if cached_hour >= 0 else 0, current_count)
            cache['daily_peak'] = max(cache['daily_peak'], current_count)
            hourly_fb = {str(k): v for k, v in cache['hourly_stats'].items()}
            db.collection('stats').document('daily').collection('days').document(today).set({
                'date': today,
                'peak': cache['daily_peak'],
                'hourly': hourly_fb,
                'last_update': firestore.SERVER_TIMESTAMP
            }, merge=True)
            writes += 1
            print(f"       📈 H{hour}: {current_count}")
        else:
            print(f"       ⏭️ H{hour} cache: {cached_hour}")
        
        # Records
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
        print(f"    ❌ Sync: {e}")
        import traceback
        traceback.print_exc()
        return False

def mark_offline(db):
    """Mark server offline - save all player times correctly"""
    global cache
    
    if cache['is_offline']:
        print(f"       ⏭️ Déjà offline (cache)")
        return
    
    try:
        now = datetime.now(timezone.utc)
        writes = 0
        reads = 0
        
        # Get final times from live/status
        prev_player_times = {}
        if cache['prev_names']:
            live_doc = db.collection('live').document('status').get()
            reads += 1
            if live_doc.exists:
                for p in live_doc.to_dict().get('players', []):
                    prev_player_times[p['name']] = p.get('time', 0)
        
        # Save time for each active session
        for name, session in cache['active_sessions'].items():
            doc_id = session['doc_id']
            baseline = session['baseline']
            data = cache['players'].get(doc_id, {})
            
            final_time = prev_player_times.get(name, baseline)
            time_to_add = max(0, final_time - baseline)
            
            new_total = data.get('total_time_seconds', 0) + time_to_add
            
            update = {
                'total_time_seconds': new_total,
                'last_seen': firestore.SERVER_TIMESTAMP,
                'current_session_start': None
            }
            
            db.collection('players').document(doc_id).update(update)
            writes += 1
            update_player_cache(doc_id, {**data, **update})
            
            print(f"          👋 {name} (+{time_to_add//60}min)")
        
        # Mark as offline
        db.collection('live').document('status').set({
            'ok': False,
            'count': 0,
            'players': [],
            'timestamp': now.isoformat(),
            'updatedAt': now.isoformat()
        })
        writes += 1
        
        # Clear cache
        cache['is_offline'] = True
        cache['prev_names'] = set()
        cache['prev_count'] = 0
        cache['live_to_doc'] = {}
        cache['active_sessions'] = {}
        
        print(f"       ⚠️ Offline ({writes}W/{reads}R)")
        
    except Exception as e:
        print(f"    ❌ Offline: {e}")

# ============================================
# Signal Handling
# ============================================
def graceful_shutdown(signum, frame):
    """Handle termination - save all player times"""
    global _db, cache
    signal_name = signal.Signals(signum).name
    print(f"\n⚠️ Signal {signal_name} reçu - sauvegarde en cours...")
    
    if _db:
        if cache.get('active_sessions'):
            try:
                mark_offline(_db)
                print("✅ Stats sauvegardées avant arrêt")
            except Exception as e:
                print(f"❌ Erreur sauvegarde: {e}")
        
        try:
            _db.collection('system').document('workflow_lock').delete()
            print("🔓 Lock libéré")
        except Exception as e:
            print(f"⚠️ Erreur release lock: {e}")
    
    sys.exit(0)

signal.signal(signal.SIGTERM, graceful_shutdown)
signal.signal(signal.SIGINT, graceful_shutdown)

# ============================================
# Workflow Lock
# ============================================
def acquire_lock(db):
    """Try to acquire workflow lock"""
    lock_ref = db.collection('system').document('workflow_lock')
    
    try:
        lock_doc = lock_ref.get()
        if lock_doc.exists:
            lock_data = lock_doc.to_dict()
            locked_at = lock_data.get('locked_at')
            
            if locked_at:
                if hasattr(locked_at, 'timestamp'):
                    lock_time = datetime.fromtimestamp(locked_at.timestamp(), tz=timezone.utc)
                else:
                    lock_time = datetime.fromisoformat(locked_at.replace('Z', '+00:00'))
                
                age = (datetime.now(timezone.utc) - lock_time).total_seconds()
                
                if age < 1800:  # 30 minutes
                    print(f"🔒 Lock actif depuis {int(age)}s")
                    return False
                else:
                    print(f"🔓 Lock expiré ({int(age)}s) - récupération")
        
        lock_ref.set({
            'locked_at': firestore.SERVER_TIMESTAMP,
            'workflow_id': os.environ.get('GITHUB_RUN_ID', 'local')
        })
        print("🔐 Lock acquis")
        return True
        
    except Exception as e:
        print(f"⚠️ Lock error: {e}")
        return False

def release_lock(db):
    """Release workflow lock"""
    try:
        db.collection('system').document('workflow_lock').delete()
        print("🔓 Lock libéré")
    except Exception as e:
        print(f"⚠️ Release lock error: {e}")

# ============================================
# Main
# ============================================
def main():
    global _db
    
    print("=" * 50)
    print("🎮 GMod Status v16 - Robust Time Tracking")
    print("=" * 50)
    
    # Initialize Firebase
    print("\n📡 Connexion Firebase...")
    db = init_firebase()
    _db = db
    print("    ✅ Connecté")
    
    # Acquire lock
    if not acquire_lock(db):
        print("❌ Autre workflow en cours - abandon")
        sys.exit(0)
    
    # Load cache
    load_cache(db)
    
    # Query loop
    print(f"\n🔄 Démarrage ({QUERIES_PER_RUN} queries)...")
    
    for i in range(QUERIES_PER_RUN):
        wait_for_next_minute()
        
        now = datetime.now(timezone.utc)
        france_now = get_france_time()
        
        print(f"\n[{i+1}/{QUERIES_PER_RUN}] {france_now.strftime('%H:%M:%S')}")
        
        # Query server
        server_data = query_gmod_server()
        
        if server_data:
            sync_to_firebase(db, server_data, now, france_now)
        else:
            mark_offline(db)
    
    # Cleanup
    release_lock(db)
    print("\n✅ Terminé")

if __name__ == "__main__":
    main()
