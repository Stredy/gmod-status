#!/usr/bin/env python3
"""
GMod Server Status v18 - ROBUST & ACCURATE
==========================================

Fusion de v16 (robustesse) + v17 (correction bug temps)

Architecture du comptage de temps :
- active_sessions: track baseline pour chaque joueur connecté
- prev_player_times: temps de session du run précédent (en mémoire, pas Firebase)
- Quand un joueur part : time_to_add = prev_player_times[name] - baseline

Cas gérés :
1. Joueur rejoint → baseline = temps actuel
2. Joueur part → ajoute (temps précédent - baseline) au total
3. Changement de nom Steam → détecté, pas de double comptage
4. Redémarrage backend → baseline recalculé pour joueurs déjà connectés
5. Serveur offline → sauvegarde tous les temps correctement
6. Crash/Signal → sauvegarde gracieuse
7. "Live identique" → temps toujours trackés en mémoire
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
    'players_by_name': {},   # normalized_name -> doc_id (Steam name only, NOT ingame_names)
    # Offline state cache
    'is_offline': False,
    'offline_since': None,
    # Previous live state
    'prev_names': set(),
    'prev_count': 0,
    'prev_player_times': {},  # name -> session time (seconds) from last run
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

# ============================================
# Steam Search Parser
# ============================================
class SteamSearchParser(HTMLParser):
    """Parse Steam search results - extracts (name, profile_url) pairs"""
    def __init__(self):
        super().__init__()
        self.results = []
        self.current_url = None
    
    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag == 'a' and 'searchPersonaName' in attrs_dict.get('class', ''):
            self.current_url = attrs_dict.get('href')
    
    def handle_data(self, data):
        if self.current_url:
            self.results.append((data.strip(), self.current_url))
            self.current_url = None

# ============================================
# Steam ID64 Parser (from XML)
# ============================================
class SteamID64Parser(HTMLParser):
    """Parse Steam profile XML to extract steamID64"""
    def __init__(self):
        super().__init__()
        self.in_steamid64 = False
        self.steamid64 = None

    def handle_starttag(self, tag, attrs):
        if tag.lower() == "steamid64":
            self.in_steamid64 = True

    def handle_endtag(self, tag):
        if tag.lower() == "steamid64":
            self.in_steamid64 = False

    def handle_data(self, data):
        if self.in_steamid64 and data.strip():
            self.steamid64 = data.strip()

# ============================================
# Steam Helper Functions
# ============================================
def _extract_html_from_ajax_response(text):
    """Extract HTML from Steam AJAX response (handles both JSON and raw HTML)"""
    try:
        data = json.loads(text)
        return data.get("html", "")
    except json.JSONDecodeError:
        return text

def _to_xml_url(profile_url):
    """Convert Steam profile URL to XML format"""
    profile_url = profile_url.rstrip("/")
    if "?xml=1" not in profile_url:
        return profile_url + "/?xml=1"
    return profile_url

def _extract_steamid64_from_profiles_url(url):
    """Extract SteamID64 from /profiles/XXXXX URL"""
    match = re.search(r"/profiles/(\d+)", url)
    return match.group(1) if match else None

def steamid64_to_steam2(steamid64):
    """Convert SteamID64 to STEAM_X:Y:Z format"""
    try:
        steam64_int = int(steamid64)
        y = steam64_int & 1
        z = (steam64_int - STEAMID64_BASE - y) // 2
        return f"STEAM_0:{y}:{z}"
    except:
        return None

def fetch_steam_avatar(steamid, verbose=False):
    """Fetch avatar from Steam profile page"""
    steam64 = steam2_to_steam64(steamid)
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
        result = parser.animated or (parser.static_candidates[0] if parser.static_candidates else None)
        if verbose and result:
            print(f"          🖼️ Avatar trouvé")
        return result
    except Exception as e:
        if verbose:
            print(f"          ⚠️ Avatar error: {e}")
        return None

# ============================================
# Steam Profile Search (ROBUST - from v16)
# ============================================
def find_steam_profile(pseudo, max_pages=5, timeout=15):
    """
    Search for a Steam profile by player name.
    Returns: (steamid64, steam2, profile_url, avatar_url) or (None, None, None, None)
    
    Features:
    - Case-sensitive exact match
    - Checks for duplicates (rejects if multiple matches)
    - Checks first 5 results only
    """
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "Chrome/120.0 Safari/537.36",
        "Accept": "*/*",
        "Referer": "https://steamcommunity.com/search/users/",
    })

    # Get session ID
    try:
        r0 = s.get("https://steamcommunity.com/", timeout=timeout)
        r0.raise_for_status()
    except requests.RequestException:
        return None, None, None, None
    
    sessionid = s.cookies.get("sessionid")
    if not sessionid:
        return None, None, None, None

    def fetch_page(page):
        r = s.get(
            "https://steamcommunity.com/search/SearchCommunityAjax",
            params={
                "text": pseudo,
                "filter": "users",
                "sessionid": sessionid,
                "steamid_user": "false",
                "page": str(page),
            },
            timeout=timeout,
        )
        r.raise_for_status()
        html = _extract_html_from_ajax_response(r.text)
        parser = SteamSearchParser()
        parser.feed(html)
        return parser.results

    # Fetch first page
    try:
        p1 = fetch_page(1)
    except requests.RequestException:
        return None, None, None, None
    
    if not p1:
        return None, None, None, None

    # Check first 5 results for an EXACT match (case-sensitive)
    matched_href = None
    check_limit = min(5, len(p1))
    
    for i in range(check_limit):
        name, href = p1[i]
        if name == pseudo:  # Case-sensitive exact match
            matched_href = href
            break
    
    # No exact match in first 5 results
    if not matched_href:
        return None, None, None, None

    # Check for duplicates in first page
    exact_count = sum(1 for n, _ in p1 if n == pseudo)
    if exact_count > 1:
        return None, None, None, None

    # Check remaining pages for duplicates
    for page in range(2, max_pages + 1):
        try:
            px = fetch_page(page)
        except requests.RequestException:
            break
        if not px:
            break
        exact_count += sum(1 for n, _ in px if n == pseudo)
        if exact_count > 1:
            return None, None, None, None

    # Resolve to steamID64
    # First check if URL is already /profiles/xxxx
    steamid64 = _extract_steamid64_from_profiles_url(matched_href)
    
    if not steamid64:
        # Need to fetch XML to get steamID64
        xml_url = _to_xml_url(matched_href)
        try:
            r = s.get(xml_url, timeout=timeout)
            r.raise_for_status()
            parser = SteamID64Parser()
            parser.feed(r.text)
            steamid64 = parser.steamid64
        except requests.RequestException:
            return None, None, None, None
    
    if not steamid64:
        return None, None, None, None

    steam2 = steamid64_to_steam2(steamid64)
    profile_url = f"https://steamcommunity.com/profiles/{steamid64}"
    
    # Try to fetch avatar
    avatar_url = None
    try:
        r = s.get(profile_url + "/?l=english", timeout=timeout)
        if r.status_code == 200:
            parser = SteamAvatarParser()
            parser.feed(r.text)
            if parser.animated:
                avatar_url = parser.animated
            elif parser.static_candidates:
                avatar_url = parser.static_candidates[0]
    except:
        pass  # Avatar is optional

    return steamid64, steam2, profile_url, avatar_url

# ============================================
# Firebase
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

# ============================================
# Cache Management
# ============================================
def init_cache(db, france_now):
    """Initialize cache at startup - includes avatar check for online players"""
    global cache
    reads = 0
    today = france_now.strftime('%Y-%m-%d')
    
    print("    📦 Chargement cache...")
    
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
    
    # 4. État live actuel (pour initialiser prev_names, live_to_doc, active_sessions)
    try:
        live_doc = db.collection('live').document('status').get()
        reads += 1
        if live_doc.exists:
            data = live_doc.to_dict()
            cache['is_offline'] = not data.get('ok', True)
            cache['prev_count'] = data.get('count', 0)
            for p in data.get('players', []):
                live_name = p['name']
                session_time = p.get('time', 0)
                cache['prev_names'].add(live_name)
                cache['prev_player_times'][live_name] = session_time
                # Chercher le doc_id pour ce joueur et créer le mapping
                found = find_player(live_name)
                if found:
                    doc_id = found[0]
                    cache['live_to_doc'][live_name] = doc_id
                    # Initialize active session with current time as baseline
                    cache['active_sessions'][live_name] = {
                        'baseline': session_time,
                        'doc_id': doc_id
                    }
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
                new_avatar = fetch_steam_avatar(steam_id, verbose=True)
                if new_avatar:
                    db.collection('players').document(doc_id).update({'avatar_url': new_avatar})
                    update_player_cache(doc_id, {**player_data, 'avatar_url': new_avatar})
                    avatars_added += 1
                    print(f"        ✅ Avatar: {live_name}")
    
    if avatars_checked > 0:
        print(f"    🖼️ Avatars: {avatars_added}/{avatars_checked} ajoutés")
    
    cache['today_date'] = today
    print(f"    📦 Stats H{france_now.hour}, peak={cache['daily_peak']}, record={cache['record_peak']}")
    print(f"    🔗 {len(cache['live_to_doc'])} joueurs mappés, {len(cache['active_sessions'])} sessions actives")
    print(f"    📊 Init: {reads} reads")
    return reads

def find_player(name):
    """Find player by Steam name (NOT ingame_names)"""
    key = name.lower().strip()
    doc_id = cache['players_by_name'].get(key) or cache['players_by_name'].get(normalize_name(name))
    if doc_id and doc_id in cache['players']:
        return (doc_id, cache['players'][doc_id])
    return None

def update_player_cache(doc_id, data):
    """Update player in cache"""
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

# ============================================
# Server Query
# ============================================
def query_gmod_server(host: str, port: int) -> Optional[dict]:
    """Query GMod server for current status"""
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
            'count': len(player_list),
            'max_players': info.max_players,
            'players': player_list,
            'ok': True
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
    
    Key principle: We track baseline (time when we started tracking)
    and use prev_player_times (cached from last run) to calculate time to add.
    This works even when live/status is not updated ("Live identique").
    """
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
        
        # Use cached previous state
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
        name_changes = {}  # old_name -> (new_name, steam2, avatar_url)
        
        for new_name in list(joined):
            # Check if this player has a SteamID that was connected under old name
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
            cache['prev_player_times'].pop(old_name, None)
            cache['prev_player_times'][new_name] = new_time
            
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
                cache['prev_player_times'].pop(name, None)
                continue
            
            doc_id = session['doc_id']
            baseline = session['baseline']
            data = cache['players'].get(doc_id, {})
            
            # Get final session time from CACHED previous run times
            # This is the key fix: we use in-memory cache, not Firebase live/status
            final_time = cache['prev_player_times'].get(name, baseline)
            
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
            cache['prev_player_times'].pop(name, None)
            
            print(f"          👋 {name} (+{time_to_add//60}min, base={baseline//60}min, final={final_time//60}min)")
        
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
                
                update = {
                    'last_seen': firestore.SERVER_TIMESTAMP,
                    'connected_at': now.isoformat(),
                    'current_session_start': session_time,
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
                
                if steam2:
                    # Check if SteamID already exists (name change detected at join)
                    existing_by_steamid = cache['players'].get(steam2)
                    
                    if existing_by_steamid:
                        # Update existing player
                        update = {
                            'name': name,
                            'last_seen': firestore.SERVER_TIMESTAMP,
                            'connected_at': now.isoformat(),
                            'current_session_start': session_time,
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
                        
                        print(f"          🔄 {name} (changement nom, steamid existant)")
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
                            'current_session_start': session_time,
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
                            'current_session_start': session_time,
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
                            'current_session_start': session_time,
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
        
        # Update cache for next iteration
        cache['prev_names'] = current_names.copy()
        cache['prev_count'] = current_count
        cache['prev_player_times'] = current_players.copy()  # KEY: Store times for next run
        
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
        
        # Save time for each active session using cached times
        for name, session in cache['active_sessions'].items():
            doc_id = session['doc_id']
            baseline = session['baseline']
            data = cache['players'].get(doc_id, {})
            
            # Use cached times from last run
            final_time = cache['prev_player_times'].get(name, baseline)
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
        cache['prev_player_times'] = {}
        cache['live_to_doc'] = {}
        cache['active_sessions'] = {}
        
        print(f"       ⚠️ Offline ({writes}W/0R)")
        
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
    print("🎮 GMod Status v18 - Robust & Accurate")
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
    
    # Initialize cache
    france_now = get_france_time()
    init_cache(db, france_now)
    
    # Query loop
    print(f"\n🔄 Démarrage ({QUERIES_PER_RUN} queries)...")
    
    for i in range(QUERIES_PER_RUN):
        wait_for_next_minute()
        
        now = datetime.now(timezone.utc)
        france_now = get_france_time()
        
        print(f"\n[{i+1}/{QUERIES_PER_RUN}] {france_now.strftime('%H:%M:%S')}")
        
        # Query server
        server_data = query_gmod_server(GMOD_HOST, GMOD_PORT)
        
        if server_data:
            sync_to_firebase(db, server_data, now, france_now)
        else:
            mark_offline(db)
    
    # Cleanup
    release_lock(db)
    print("\n✅ Terminé")

if __name__ == "__main__":
    main()
