#!/usr/bin/env python3
"""
GMod Server Status v20 - BULLETPROOF TIME TRACKING
==================================================

Principe: session_started_at = now - time (timestamp absolu)
Au départ: duration = now - session_started_at

Protections:
1. Timezone France correcte (été/hiver)
2. Détection changement de nom Steam (évite double comptage)
3. Détection reset serveur GMod (temps qui diminue)
4. Sanitization doc_id
5. Try/except par joueur

Structure live/status.players[]:
{
    "name": "Alice",
    "time": 3600,
    "session_started_at": "2024-01-15T14:30:00+00:00"
}
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
from typing import Optional, Tuple, Dict, Set
from html.parser import HTMLParser

import a2s
import firebase_admin
from firebase_admin import credentials, firestore

# Try to use proper timezone, fallback to manual calculation
try:
    from zoneinfo import ZoneInfo
    PARIS_TZ = ZoneInfo('Europe/Paris')
    def get_france_time():
        return datetime.now(PARIS_TZ)
except ImportError:
    PARIS_TZ = None
    def get_france_time():
        """Manual DST calculation for France (last Sunday of March/October)"""
        now_utc = datetime.now(timezone.utc)
        year = now_utc.year
        
        # Last Sunday of March (start DST)
        march_last = datetime(year, 3, 31, 2, 0, tzinfo=timezone.utc)
        while march_last.weekday() != 6:  # Sunday
            march_last -= timedelta(days=1)
        
        # Last Sunday of October (end DST)
        october_last = datetime(year, 10, 31, 3, 0, tzinfo=timezone.utc)
        while october_last.weekday() != 6:
            october_last -= timedelta(days=1)
        
        # UTC+2 in summer, UTC+1 in winter
        if march_last <= now_utc < october_last:
            return now_utc + timedelta(hours=2)
        else:
            return now_utc + timedelta(hours=1)

GMOD_HOST = os.environ.get('GMOD_HOST', '51.91.215.65')
GMOD_PORT = int(os.environ.get('GMOD_PORT', '27015'))
QUERIES_PER_RUN = 30

_db = None

# ============================================
# Memory Cache
# ============================================
cache = {
    # Stats
    'hourly_stats': {},
    'daily_peak': 0,
    'record_peak': 0,
    'today_date': None,
    # Players database
    'players': {},           # doc_id -> player data
    'players_by_name': {},   # name.lower() -> doc_id
    # Server state
    'is_offline': False,
    'prev_names': set(),
    'prev_count': 0,
    'prev_times': {},        # name -> time (pour détecter reset serveur)
    # Session tracking
    'sessions': {},          # name -> {'started_at': datetime, 'doc_id': str}
}

def wait_for_next_minute():
    now = datetime.now()
    seconds_to_wait = 60 - now.second - (now.microsecond / 1_000_000)
    if 0 < seconds_to_wait < 60:
        print(f"    ⏳ Attente {seconds_to_wait:.1f}s → XX:{(now.minute + 1) % 60:02d}:00")
        time.sleep(seconds_to_wait)

# ============================================
# Helpers
# ============================================
STEAMID64_BASE = 76561197960265728
STEAM2_RE = re.compile(r"^STEAM_[0-5]:([0-1]):(\d+)$", re.IGNORECASE)

def normalize_name(name):
    if not name:
        return ""
    name = name.lower().strip()
    name = unicodedata.normalize('NFD', name)
    name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
    return re.sub(r'[^a-z0-9]', '', name)

def sanitize_doc_id(doc_id):
    if not doc_id:
        return None
    doc_id = doc_id.replace('/', '_').replace('\\', '_')
    if doc_id in ['.', '..']:
        doc_id = f"dot_{doc_id}"
    doc_id = doc_id[:100].strip()
    return doc_id if doc_id else None

def steam2_to_steam64(steamid):
    match = STEAM2_RE.match(steamid)
    if not match:
        return None
    return str(STEAMID64_BASE + (int(match.group(2)) * 2) + int(match.group(1)))

def steamid64_to_steam2(steamid64):
    try:
        steam64_int = int(steamid64)
        y = steam64_int & 1
        z = (steam64_int - STEAMID64_BASE - y) // 2
        return f"STEAM_0:{y}:{z}"
    except:
        return None

def format_duration(seconds):
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}min"
    else:
        h = seconds // 3600
        m = (seconds % 3600) // 60
        return f"{h}h{m:02d}"

# ============================================
# Steam Parsers
# ============================================
class SteamAvatarParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_inner = 0
        self.in_frame = 0
        self.animated = None
        self.static_candidates = []

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
            url = (srcset.split(",")[0].strip().split()[0] if srcset else None) or src
            if url:
                if "animated_avatar" in (src + srcset):
                    self.animated = url
                else:
                    self.static_candidates.append(url)

    def handle_endtag(self, tag):
        if tag == "div":
            if self.in_inner:
                self.in_inner -= 1
            if self.in_frame:
                self.in_frame -= 1

class SteamSearchParser(HTMLParser):
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

class SteamID64Parser(HTMLParser):
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
# Steam API
# ============================================
def fetch_steam_avatar(steamid):
    steam64 = steam2_to_steam64(steamid)
    if not steam64:
        return None
    try:
        resp = requests.get(
            f"https://steamcommunity.com/profiles/{steam64}/?l=english",
            timeout=10,
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        )
        if resp.status_code != 200:
            return None
        parser = SteamAvatarParser()
        parser.feed(resp.text)
        return parser.animated or (parser.static_candidates[0] if parser.static_candidates else None)
    except:
        return None

def find_steam_profile(pseudo, max_pages=5, timeout=15):
    """Returns: (steam2, avatar_url) or (None, None)"""
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0",
    })

    try:
        s.get("https://steamcommunity.com/", timeout=timeout)
    except:
        return None, None
    
    sessionid = s.cookies.get("sessionid")
    if not sessionid:
        return None, None

    def fetch_page(page):
        r = s.get(
            "https://steamcommunity.com/search/SearchCommunityAjax",
            params={"text": pseudo, "filter": "users", "sessionid": sessionid, "page": str(page)},
            timeout=timeout,
        )
        r.raise_for_status()
        try:
            html = r.json().get("html", "")
        except:
            html = r.text
        parser = SteamSearchParser()
        parser.feed(html)
        return parser.results

    try:
        p1 = fetch_page(1)
    except:
        return None, None
    
    if not p1:
        return None, None

    matched_href = None
    for name, href in p1[:5]:
        if name == pseudo:
            matched_href = href
            break
    
    if not matched_href:
        return None, None

    exact_count = sum(1 for n, _ in p1 if n == pseudo)
    if exact_count > 1:
        return None, None

    for page in range(2, max_pages + 1):
        try:
            px = fetch_page(page)
            if not px:
                break
            exact_count += sum(1 for n, _ in px if n == pseudo)
            if exact_count > 1:
                return None, None
        except:
            break

    match = re.search(r"/profiles/(\d+)", matched_href)
    steamid64 = match.group(1) if match else None
    
    if not steamid64:
        try:
            xml_url = matched_href.rstrip("/") + "/?xml=1"
            r = s.get(xml_url, timeout=timeout)
            parser = SteamID64Parser()
            parser.feed(r.text)
            steamid64 = parser.steamid64
        except:
            return None, None
    
    if not steamid64:
        return None, None

    steam2 = steamid64_to_steam2(steamid64)
    if not steam2 or not STEAM2_RE.match(steam2):
        return None, None
    
    avatar_url = None
    try:
        r = s.get(f"https://steamcommunity.com/profiles/{steamid64}/?l=english", timeout=timeout)
        if r.status_code == 200:
            parser = SteamAvatarParser()
            parser.feed(r.text)
            avatar_url = parser.animated or (parser.static_candidates[0] if parser.static_candidates else None)
    except:
        pass

    return steam2, avatar_url

# ============================================
# Firebase
# ============================================
def init_firebase():
    if firebase_admin._apps:
        return firestore.client()
    
    service_account_json = os.environ.get('FIREBASE_SERVICE_ACCOUNT')
    if not service_account_json:
        raise ValueError("FIREBASE_SERVICE_ACCOUNT not set")
    
    cred = credentials.Certificate(json.loads(service_account_json))
    firebase_admin.initialize_app(cred)
    return firestore.client()

# ============================================
# Cache
# ============================================
def init_cache(db, france_now):
    global cache
    reads = 0
    today = france_now.strftime('%Y-%m-%d')
    
    print("    📦 Chargement cache...")
    
    # Stats
    try:
        doc = db.collection('stats').document('daily').collection('days').document(today).get()
        reads += 1
        if doc.exists:
            data = doc.to_dict()
            cache['hourly_stats'] = {int(k): v for k, v in data.get('hourly', {}).items()}
            cache['daily_peak'] = data.get('peak', 0)
    except Exception as e:
        print(f"    ⚠️ Stats: {e}")
    
    # Records
    try:
        doc = db.collection('stats').document('records').get()
        reads += 1
        if doc.exists:
            cache['record_peak'] = doc.to_dict().get('peak_count', 0)
    except Exception as e:
        print(f"    ⚠️ Records: {e}")
    
    # Players
    try:
        docs = db.collection('players').get()
        for doc in docs:
            reads += 1
            data = doc.to_dict()
            doc_id = doc.id
            cache['players'][doc_id] = data
            name = data.get('name', '')
            if name:
                cache['players_by_name'][name.lower().strip()] = doc_id
                cache['players_by_name'][normalize_name(name)] = doc_id
        print(f"    👥 {len(cache['players'])} joueurs")
    except Exception as e:
        print(f"    ⚠️ Players: {e}")
    
    # Live status
    try:
        doc = db.collection('live').document('status').get()
        reads += 1
        if doc.exists:
            data = doc.to_dict()
            cache['is_offline'] = not data.get('ok', True)
            cache['prev_count'] = data.get('count', 0)
            
            for p in data.get('players', []):
                name = p['name']
                time_val = p.get('time', 0)
                cache['prev_names'].add(name)
                cache['prev_times'][name] = time_val
                
                started_at_str = p.get('session_started_at')
                started_at = None
                if started_at_str:
                    try:
                        started_at = datetime.fromisoformat(started_at_str.replace('Z', '+00:00'))
                    except:
                        pass
                
                found = find_player(name)
                if found:
                    doc_id = found[0]
                    cache['sessions'][name] = {
                        'started_at': started_at,
                        'doc_id': doc_id
                    }
            
            print(f"    🔗 {len(cache['sessions'])} sessions")
    except Exception as e:
        print(f"    ⚠️ Live: {e}")
    
    cache['today_date'] = today
    print(f"    📊 H{france_now.hour}, peak={cache['daily_peak']}, record={cache['record_peak']}")
    print(f"    📖 {reads} reads")
    return reads

def find_player(name):
    key = name.lower().strip()
    doc_id = cache['players_by_name'].get(key) or cache['players_by_name'].get(normalize_name(name))
    if doc_id and doc_id in cache['players']:
        return (doc_id, cache['players'][doc_id])
    return None

def update_player_cache(doc_id, data):
    if doc_id in cache['players']:
        cache['players'][doc_id].update(data)
    else:
        cache['players'][doc_id] = data
    name = data.get('name', '')
    if name:
        cache['players_by_name'][name.lower().strip()] = doc_id
        cache['players_by_name'][normalize_name(name)] = doc_id

# ============================================
# Server Query
# ============================================
def query_gmod_server() -> Optional[dict]:
    try:
        address = (GMOD_HOST, GMOD_PORT)
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
            'count': len(player_list),
            'max_players': info.max_players,
            'players': player_list,
        }
    except Exception as e:
        print(f"       ❌ Query: {e}")
        return None

# ============================================
# Core Sync - BULLETPROOF
# ============================================
def sync_to_firebase(db, server_data: dict, now: datetime, france_now: datetime):
    global cache
    
    try:
        current_players = {p['name']: p['time'] for p in server_data['players']}
        current_names = set(current_players.keys())
        current_count = server_data['count']
        today = france_now.strftime('%Y-%m-%d')
        hour = france_now.hour
        
        writes = 0
        
        if cache['today_date'] != today:
            print(f"       🌅 Nouveau jour")
            cache['hourly_stats'] = {}
            cache['daily_peak'] = 0
            cache['today_date'] = today
        
        previous_names = cache['prev_names']
        joined = current_names - previous_names
        left = previous_names - current_names
        stayed = current_names & previous_names
        
        print(f"       📊 {current_count} | +{len(joined)} -{len(left)} ={len(stayed)}")
        
        cache['is_offline'] = False
        
        # ============================================
        # PHASE 0: Detect server reset (time decreased)
        # ============================================
        for name in stayed:
            prev_time = cache['prev_times'].get(name, 0)
            curr_time = current_players[name]
            
            # Si le temps a diminué de plus de 2 minutes, c'est un reset serveur
            if prev_time > curr_time + 120:
                session = cache['sessions'].get(name)
                if session and session.get('started_at'):
                    # Sauvegarder le temps de l'ancienne session
                    doc_id = sanitize_doc_id(session['doc_id'])
                    if doc_id:
                        duration = int((now - session['started_at']).total_seconds())
                        # Mais limiter au temps réel du serveur (prev_time)
                        duration = min(duration, prev_time)
                        duration = max(0, duration)
                        
                        if duration > 60:  # Seulement si > 1 min
                            data = cache['players'].get(doc_id, {})
                            new_total = data.get('total_time_seconds', 0) + duration
                            db.collection('players').document(doc_id).update({
                                'total_time_seconds': new_total
                            })
                            writes += 1
                            update_player_cache(doc_id, {**data, 'total_time_seconds': new_total})
                            print(f"          🔄 Reset serveur: {name} (+{format_duration(duration)} sauvé)")
                
                # Recalculer started_at avec le nouveau temps
                new_started_at = now - timedelta(seconds=curr_time)
                if name in cache['sessions']:
                    cache['sessions'][name]['started_at'] = new_started_at
        
        # ============================================
        # PHASE 1: Detect name changes (BEFORE departures!)
        # ============================================
        name_changes = {}  # old_name -> new_name
        
        for new_name in list(joined):
            try:
                steam2, avatar_url = find_steam_profile(new_name)
                if steam2:
                    doc_id = sanitize_doc_id(steam2)
                    if doc_id:
                        for old_name in list(left):
                            old_session = cache['sessions'].get(old_name)
                            if old_session and old_session.get('doc_id') == doc_id:
                                name_changes[old_name] = (new_name, doc_id, avatar_url)
                                print(f"          🔄 Renommage: {old_name} → {new_name}")
                                break
            except:
                pass
        
        # Remove name changes from joined/left
        for old_name, (new_name, doc_id, avatar_url) in name_changes.items():
            joined.discard(new_name)
            left.discard(old_name)
        
        # ============================================
        # PHASE 2: Handle name changes (transfer session, NO time added)
        # ============================================
        for old_name, (new_name, doc_id, avatar_url) in name_changes.items():
            try:
                old_session = cache['sessions'].get(old_name, {})
                data = cache['players'].get(doc_id, {})
                
                update = {
                    'name': new_name,
                    'last_seen': firestore.SERVER_TIMESTAMP,
                }
                if avatar_url and avatar_url != data.get('avatar_url'):
                    update['avatar_url'] = avatar_url
                
                db.collection('players').document(doc_id).update(update)
                writes += 1
                update_player_cache(doc_id, {**data, **update})
                
                # Transfer session (keep same started_at!)
                cache['sessions'].pop(old_name, None)
                cache['sessions'][new_name] = {
                    'started_at': old_session.get('started_at'),
                    'doc_id': doc_id
                }
                cache['prev_times'].pop(old_name, None)
                cache['prev_times'][new_name] = current_players[new_name]
                
            except Exception as e:
                print(f"          ❌ Name change {old_name}: {e}")
        
        # ============================================
        # PHASE 3: Handle departures (save time)
        # ============================================
        for name in left:
            try:
                session = cache['sessions'].get(name)
                
                if not session or not session.get('started_at'):
                    existing = find_player(name)
                    if existing:
                        doc_id = sanitize_doc_id(existing[0])
                        if doc_id:
                            db.collection('players').document(doc_id).update({
                                'last_seen': firestore.SERVER_TIMESTAMP,
                                'current_session_start': None
                            })
                            writes += 1
                    print(f"          👋 {name} (no session)")
                    cache['sessions'].pop(name, None)
                    cache['prev_times'].pop(name, None)
                    continue
                
                doc_id = sanitize_doc_id(session['doc_id'])
                started_at = session['started_at']
                
                if not doc_id:
                    cache['sessions'].pop(name, None)
                    cache['prev_times'].pop(name, None)
                    continue
                
                # Duration = now - started_at
                duration = int((now - started_at).total_seconds())
                
                # Safety: cap at last known time + 5 minutes (avoid huge errors)
                last_known_time = cache['prev_times'].get(name, 0)
                max_duration = last_known_time + 300
                duration = min(duration, max_duration)
                duration = max(0, duration)
                
                data = cache['players'].get(doc_id, {})
                new_total = data.get('total_time_seconds', 0) + duration
                
                db.collection('players').document(doc_id).update({
                    'total_time_seconds': new_total,
                    'last_seen': firestore.SERVER_TIMESTAMP,
                    'current_session_start': None
                })
                writes += 1
                update_player_cache(doc_id, {**data, 'total_time_seconds': new_total})
                
                cache['sessions'].pop(name, None)
                cache['prev_times'].pop(name, None)
                print(f"          👋 {name} (+{format_duration(duration)})")
                
            except Exception as e:
                print(f"          ❌ Departure {name}: {e}")
                cache['sessions'].pop(name, None)
                cache['prev_times'].pop(name, None)
        
        # ============================================
        # PHASE 4: Handle arrivals
        # ============================================
        for name in joined:
            try:
                session_time = current_players[name]
                started_at = now - timedelta(seconds=session_time)
                
                existing = find_player(name)
                
                if existing:
                    doc_id, data = existing
                    doc_id = sanitize_doc_id(doc_id)
                    if not doc_id:
                        continue
                    
                    steam_id = data.get('steam_id', '')
                    
                    update = {
                        'last_seen': firestore.SERVER_TIMESTAMP,
                        'current_session_start': started_at.isoformat(),
                        'session_count': data.get('session_count', 0) + 1
                    }
                    
                    if steam_id.startswith('STEAM_') and not data.get('avatar_url'):
                        avatar = fetch_steam_avatar(steam_id)
                        if avatar:
                            update['avatar_url'] = avatar
                    
                    db.collection('players').document(doc_id).update(update)
                    writes += 1
                    update_player_cache(doc_id, {**data, **update})
                    
                    cache['sessions'][name] = {'started_at': started_at, 'doc_id': doc_id}
                    print(f"          ⬆️ {name} ({format_duration(session_time)})")
                    
                else:
                    steam2, avatar_url = find_steam_profile(name)
                    
                    if steam2:
                        doc_id = sanitize_doc_id(steam2)
                        if not doc_id:
                            continue
                        
                        existing_data = cache['players'].get(doc_id)
                        
                        if existing_data:
                            update = {
                                'name': name,
                                'last_seen': firestore.SERVER_TIMESTAMP,
                                'current_session_start': started_at.isoformat(),
                                'session_count': existing_data.get('session_count', 0) + 1
                            }
                            if avatar_url:
                                update['avatar_url'] = avatar_url
                            
                            db.collection('players').document(doc_id).update(update)
                            writes += 1
                            update_player_cache(doc_id, {**existing_data, **update})
                            print(f"          🔄 {name} (steam existant)")
                        else:
                            new_player = {
                                'name': name,
                                'steam_id': doc_id,
                                'roles': ['Joueur'],
                                'ingame_names': [],
                                'created_at': firestore.SERVER_TIMESTAMP,
                                'last_seen': firestore.SERVER_TIMESTAMP,
                                'current_session_start': started_at.isoformat(),
                                'total_time_seconds': 0,
                                'session_count': 1,
                                'is_auto_detected': False,
                                'avatar_url': avatar_url
                            }
                            db.collection('players').document(doc_id).set(new_player)
                            writes += 1
                            update_player_cache(doc_id, new_player)
                            print(f"          🆕✅ {name}")
                        
                        cache['sessions'][name] = {'started_at': started_at, 'doc_id': doc_id}
                    else:
                        key = normalize_name(name) or 'unknown'
                        doc_id = sanitize_doc_id(f"auto_{key}") or f"auto_{hash(name) & 0xFFFFFFFF}"
                        
                        existing_auto = cache['players'].get(doc_id)
                        
                        if existing_auto:
                            update = {
                                'name': name,
                                'last_seen': firestore.SERVER_TIMESTAMP,
                                'current_session_start': started_at.isoformat(),
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
                                'current_session_start': started_at.isoformat(),
                                'total_time_seconds': 0,
                                'session_count': 1,
                                'is_auto_detected': True
                            }
                            db.collection('players').document(doc_id).set(new_player)
                            writes += 1
                            update_player_cache(doc_id, new_player)
                        
                        cache['sessions'][name] = {'started_at': started_at, 'doc_id': doc_id}
                        print(f"          🆕 {name} (auto)")
                        
            except Exception as e:
                print(f"          ❌ Arrival {name}: {e}")
        
        # ============================================
        # PHASE 5: Ensure STAYED players have sessions
        # ============================================
        for name in stayed:
            if name not in cache['sessions']:
                session_time = current_players[name]
                started_at = now - timedelta(seconds=session_time)
                
                existing = find_player(name)
                if existing:
                    doc_id = sanitize_doc_id(existing[0])
                    if doc_id:
                        cache['sessions'][name] = {'started_at': started_at, 'doc_id': doc_id}
        
        # ============================================
        # PHASE 6: Update live/status
        # ============================================
        players_changed = (current_names != previous_names) or (current_count != cache['prev_count'])
        
        players_for_firebase = []
        for p in server_data['players']:
            name = p['name']
            session = cache['sessions'].get(name)
            
            entry = {'name': name, 'time': p['time']}
            
            if session and session.get('started_at'):
                entry['session_started_at'] = session['started_at'].isoformat()
            else:
                entry['session_started_at'] = (now - timedelta(seconds=p['time'])).isoformat()
            
            players_for_firebase.append(entry)
        
        if players_changed:
            db.collection('live').document('status').set({
                'ok': True,
                'count': current_count,
                'max': server_data['max_players'],
                'map': server_data['map'],
                'server': server_data['server_name'],
                'players': players_for_firebase,
                'timestamp': now.isoformat(),
                'updatedAt': now.isoformat()
            })
            writes += 1
        else:
            print(f"       ⏭️ Live identique")
        
        # Update cache
        cache['prev_names'] = current_names.copy()
        cache['prev_count'] = current_count
        cache['prev_times'] = current_players.copy()
        
        # ============================================
        # PHASE 7: Stats
        # ============================================
        cached_hour = cache['hourly_stats'].get(hour, -1)
        if cached_hour == -1 or current_count > cached_hour:
            cache['hourly_stats'][hour] = max(cached_hour if cached_hour >= 0 else 0, current_count)
            cache['daily_peak'] = max(cache['daily_peak'], current_count)
            
            db.collection('stats').document('daily').collection('days').document(today).set({
                'date': today,
                'peak': cache['daily_peak'],
                'hourly': {str(k): v for k, v in cache['hourly_stats'].items()},
                'last_update': firestore.SERVER_TIMESTAMP
            }, merge=True)
            writes += 1
            print(f"       📈 H{hour}: {current_count}")
        else:
            print(f"       ⏭️ H{hour}: {cached_hour}")
        
        if current_count > cache['record_peak']:
            cache['record_peak'] = current_count
            db.collection('stats').document('records').set({
                'peak_count': current_count,
                'peak_date': now.isoformat()
            })
            writes += 1
            print(f"       🏆 Record: {current_count}!")
        
        print(f"       ✅ {writes}W")
        return True
        
    except Exception as e:
        print(f"    ❌ Sync: {e}")
        import traceback
        traceback.print_exc()
        return False

def mark_offline(db):
    global cache
    
    if cache['is_offline']:
        print(f"       ⏭️ Déjà offline")
        return
    
    now = datetime.now(timezone.utc)
    writes = 0
    
    try:
        for name, session in list(cache['sessions'].items()):
            try:
                if not session.get('started_at'):
                    continue
                
                doc_id = sanitize_doc_id(session['doc_id'])
                if not doc_id:
                    continue
                
                duration = int((now - session['started_at']).total_seconds())
                
                # Safety cap
                last_known = cache['prev_times'].get(name, 0)
                duration = min(duration, last_known + 300)
                duration = max(0, duration)
                
                data = cache['players'].get(doc_id, {})
                new_total = data.get('total_time_seconds', 0) + duration
                
                db.collection('players').document(doc_id).update({
                    'total_time_seconds': new_total,
                    'last_seen': firestore.SERVER_TIMESTAMP,
                    'current_session_start': None
                })
                writes += 1
                update_player_cache(doc_id, {**data, 'total_time_seconds': new_total})
                print(f"          👋 {name} (+{format_duration(duration)})")
                
            except Exception as e:
                print(f"          ❌ {name}: {e}")
        
        db.collection('live').document('status').set({
            'ok': False,
            'count': 0,
            'players': [],
            'timestamp': now.isoformat(),
            'updatedAt': now.isoformat()
        })
        writes += 1
        
        cache['is_offline'] = True
        cache['prev_names'] = set()
        cache['prev_count'] = 0
        cache['prev_times'] = {}
        cache['sessions'] = {}
        
        print(f"       ⚠️ Offline ({writes}W)")
        
    except Exception as e:
        print(f"    ❌ Offline: {e}")

# ============================================
# Signals
# ============================================
def graceful_shutdown(signum, frame):
    global _db, cache
    print(f"\n⚠️ Signal {signal.Signals(signum).name}")
    
    if _db and cache.get('sessions'):
        try:
            mark_offline(_db)
        except:
            pass
    
    if _db:
        try:
            _db.collection('system').document('workflow_lock').delete()
            print("🔓 Lock libéré")
        except:
            pass
    
    sys.exit(0)

signal.signal(signal.SIGTERM, graceful_shutdown)
signal.signal(signal.SIGINT, graceful_shutdown)

# ============================================
# Lock
# ============================================
def acquire_lock(db):
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
                    lock_time = datetime.fromisoformat(str(locked_at).replace('Z', '+00:00'))
                
                age = (datetime.now(timezone.utc) - lock_time).total_seconds()
                
                if age < 1800:
                    print(f"🔒 Lock actif ({int(age)}s)")
                    return False
                print(f"🔓 Lock expiré ({int(age)}s)")
        
        lock_ref.set({
            'locked_at': firestore.SERVER_TIMESTAMP,
            'workflow_id': os.environ.get('GITHUB_RUN_ID', 'local')
        })
        print("🔐 Lock acquis")
        return True
        
    except Exception as e:
        print(f"⚠️ Lock: {e}")
        return False

def release_lock(db):
    try:
        db.collection('system').document('workflow_lock').delete()
        print("🔓 Lock libéré")
    except:
        pass

# ============================================
# Main
# ============================================
def main():
    global _db
    
    print("=" * 50)
    print("🎮 GMod Status v20 - Bulletproof")
    print("=" * 50)
    
    print("\n📡 Firebase...")
    db = init_firebase()
    _db = db
    print("    ✅ Connecté")
    
    if not acquire_lock(db):
        print("❌ Autre workflow actif")
        sys.exit(0)
    
    france_now = get_france_time()
    init_cache(db, france_now)
    
    print(f"\n🔄 Démarrage ({QUERIES_PER_RUN} queries)...")
    
    for i in range(QUERIES_PER_RUN):
        wait_for_next_minute()
        
        now = datetime.now(timezone.utc)
        france_now = get_france_time()
        
        print(f"\n[{i+1}/{QUERIES_PER_RUN}] {france_now.strftime('%H:%M:%S')}")
        
        server_data = query_gmod_server()
        
        if server_data:
            sync_to_firebase(db, server_data, now, france_now)
        else:
            mark_offline(db)
    
    release_lock(db)
    print("\n✅ Terminé")

if __name__ == "__main__":
    main()
