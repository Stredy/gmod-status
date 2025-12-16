#!/usr/bin/env python3
"""
GMod Server Status v15 - PRODUCTION OPTIMIZED
- 30 queries per workflow (30 minutes)
- Cache: players + stats + offline state
- Workflow every 30 minutes via cron
- Signal handling for graceful shutdown (saves stats on interruption)
- Steam profile lookup for new players (auto SteamID + avatar)

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
import signal
import unicodedata
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Set
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

class SteamAvatarParser(HTMLParser):
    """Parse la page profil Steam pour extraire l'avatar"""
    def __init__(self):
        super().__init__()
        self.in_inner = 0
        self.in_frame = 0
        self.animated = None
        self.static_candidates = []

    def _first_url_from_srcset(self, srcset):
        if not srcset:
            return None
        first = srcset.split(",")[0].strip()
        if not first:
            return None
        return first.split(" ")[0].strip()

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        klass = a.get("class", "")

        if tag == "div":
            if "playerAvatarAutoSizeInner" in klass:
                self.in_inner += 1
            elif self.in_inner > 0 and "profile_avatar_frame" in klass:
                self.in_frame += 1

        if self.in_inner <= 0 or self.in_frame > 0:
            return

        if tag == "img":
            url = None
            if "srcset" in a:
                url = self._first_url_from_srcset(a.get("srcset", ""))
            if not url and "src" in a:
                url = a.get("src")

            if url:
                lower_url = url.lower()
                if self.animated is None and any(lower_url.endswith(ext) for ext in (".gif", ".webm", ".mp4")):
                    self.animated = url
                elif any(lower_url.endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".webp")):
                    self.static_candidates.append(url)

        if tag == "source":
            media = (a.get("media") or "").strip()
            url = self._first_url_from_srcset(a.get("srcset", "") or "")
            if url and any(url.lower().endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".webp")):
                if "prefers-reduced-motion" in media:
                    self.static_candidates.insert(0, url)
                else:
                    self.static_candidates.append(url)

    def handle_endtag(self, tag):
        if tag != "div":
            return
        if self.in_frame > 0:
            self.in_frame -= 1
            return
        if self.in_inner > 0:
            self.in_inner -= 1

class SteamSearchParser(HTMLParser):
    """Extrait tous les <a class="searchPersonaName" href="...">TEXT</a> dans l'ordre."""
    def __init__(self):
        super().__init__()
        self.results = []  # [(display_name, href), ...]
        self._in_target_a = False
        self._current_href = None
        self._current_text_parts = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return
        attr_dict = dict(attrs)
        class_attr = attr_dict.get("class", "") or ""
        classes = set(class_attr.split())
        if "searchPersonaName" in classes and "href" in attr_dict:
            self._in_target_a = True
            self._current_href = attr_dict["href"]
            self._current_text_parts = []

    def handle_data(self, data):
        if self._in_target_a:
            self._current_text_parts.append(data)

    def handle_endtag(self, tag):
        if tag.lower() == "a" and self._in_target_a:
            name = "".join(self._current_text_parts).strip()
            href = self._current_href or ""
            if name and href:
                self.results.append((name, href))
            self._in_target_a = False
            self._current_href = None
            self._current_text_parts = []


class SteamID64Parser(HTMLParser):
    """Parse la page XML (?xml=1) et extrait le contenu de <steamID64>...</steamID64>."""
    def __init__(self):
        super().__init__()
        self.steamid64 = None
        self._in_steamid64 = False
        self._buf = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() == "steamid64":
            self._in_steamid64 = True
            self._buf = []

    def handle_data(self, data):
        if self._in_steamid64:
            self._buf.append(data)

    def handle_endtag(self, tag):
        if tag.lower() == "steamid64" and self._in_steamid64:
            val = "".join(self._buf).strip()
            if val.isdigit():
                self.steamid64 = val
            self._in_steamid64 = False
            self._buf = []


# ============================================
# Steam Profile Lookup
# ============================================
def _extract_html_from_ajax_response(text):
    """L'endpoint Steam peut renvoyer du JSON avec un champ HTML, ou du HTML direct."""
    t = text.lstrip()
    if t.startswith("{"):
        try:
            obj = json.loads(text)
            for key in ("html", "results_html", "searchresults", "data"):
                if key in obj and isinstance(obj[key], str):
                    return obj[key]
        except json.JSONDecodeError:
            pass
    return text


def _to_xml_url(profile_url):
    """Ajoute ?xml=1 proprement à une URL de profil Steam."""
    u = profile_url.strip()
    if not u:
        return u
    if "xml=1" in u:
        return u
    if "?" in u:
        return u + "&xml=1"
    if u.endswith("/"):
        return u + "?xml=1"
    return u + "?xml=1"


def _extract_steamid64_from_profiles_url(url):
    """Extrait le steamID64 d'une URL /profiles/xxxx"""
    marker = "/profiles/"
    if marker in url:
        tail = url.split(marker, 1)[1]
        digits = ""
        for ch in tail:
            if ch.isdigit():
                digits += ch
            else:
                break
        return digits if len(digits) >= 16 else None
    return None


def steamid64_to_steamid2(steamid64):
    """Convertit steamID64 -> SteamID2 (legacy): STEAM_0:X:Y"""
    sid64 = int(steamid64)
    base = 76561197960265728
    a = sid64 - base
    x = a % 2
    y = (a - x) // 2
    return f"STEAM_0:{x}:{y}"


def resolve_to_steamid64(session, profile_url, timeout=15):
    """
    Retourne (steamid64, message)
    - Si déjà /profiles/xxxx, on extrait xxxx
    - Sinon (/id/...), on fetch ?xml=1 et on lit <steamID64>
    """
    sid = _extract_steamid64_from_profiles_url(profile_url)
    if sid:
        return sid, "OK (déjà en steamID64)."

    xml_url = _to_xml_url(profile_url)
    r = session.get(xml_url, timeout=timeout)
    r.raise_for_status()

    parser = SteamID64Parser()
    parser.feed(r.text)
    if parser.steamid64:
        return parser.steamid64, "OK (résolu via XML)."

    return None, "Impossible d'extraire steamID64."


def find_steam_profile(pseudo, max_pages=3, timeout=10):
    """
    Cherche un profil Steam par pseudo (case-sensitive exact match).
    Règles d'incertitude (retourne None):
    - Aucun résultat
    - 1er résultat != pseudo (case-sensitive)
    - Plusieurs résultats exactement == pseudo (doublon)
    - Lien trouvé mais steamID64 non extractible (profil privé/bloqué)
    Retourne (steamid64, steam2, profile_url, avatar_url) ou (None, None, None, None)
    """
    pseudo = pseudo.strip()
    if not pseudo:
        return None, None, None, None

    try:
        s = requests.Session()
        s.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "*/*",
            "Referer": "https://steamcommunity.com/search/users/",
        })

        # Get session ID
        r0 = s.get("https://steamcommunity.com/", timeout=timeout)
        r0.raise_for_status()
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
        p1 = fetch_page(1)
        if not p1:
            return None, None, None, None

        first_name, first_href = p1[0]
        # Case-sensitive exact match required
        if first_name != pseudo:
            return None, None, None, None

        # Check for duplicates
        exact_count = sum(1 for n, _ in p1 if n == pseudo)
        if exact_count > 1:
            return None, None, None, None

        for page in range(2, max_pages + 1):
            try:
                px = fetch_page(page)
                if not px:
                    break
                exact_count += sum(1 for n, _ in px if n == pseudo)
                if exact_count > 1:
                    return None, None, None, None
            except:
                break

        # Resolve to steamID64
        steamid64, msg = resolve_to_steamid64(s, first_href)
        if not steamid64:
            return None, None, None, None

        steam2 = steamid64_to_steamid2(steamid64)
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
            pass

        return steamid64, steam2, profile_url, avatar_url

    except Exception as e:
        return None, None, None, None


def fetch_steam_avatar(steamid, verbose=False):
    """Récupère l'URL de l'avatar Steam à partir d'un Steam ID"""
    try:
        steam64 = steam2_to_steam64(steamid)
        if not steam64:
            if verbose:
                print(f"            ⚠️ Conversion Steam64 échouée pour {steamid}")
            return None
        
        url = f"https://steamcommunity.com/profiles/{steam64}/?l=english"
        response = requests.get(
            url,
            timeout=15,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            },
        )
        
        if response.status_code == 429:
            if verbose:
                print(f"            ⚠️ Rate-limit Steam pour {steamid}")
            return None
        
        if response.status_code != 200:
            if verbose:
                print(f"            ⚠️ Steam HTTP {response.status_code} pour {steamid}")
            return None
        
        parser = SteamAvatarParser()
        parser.feed(response.text)
        
        # Priorité à l'avatar animé, sinon statique
        if parser.animated:
            return parser.animated
        if parser.static_candidates:
            return parser.static_candidates[0]
        
        if verbose:
            print(f"            ⚠️ Pas d'image trouvée pour {steamid}")
        return None
    except Exception as e:
        if verbose:
            print(f"            ⚠️ Erreur avatar {steamid}: {e}")
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
                # Nouveau joueur - essayer de trouver son profil Steam
                steamid64, steam2, profile_url, avatar_url = find_steam_profile(name)
                
                if steam2:
                    # Profil Steam trouvé!
                    # Vérifier si ce SteamID existe déjà (changement de nom)
                    existing_by_steamid = cache['players'].get(steam2)
                    
                    if existing_by_steamid:
                        # C'est un changement de nom Steam!
                        doc_id = steam2
                        update = {
                            'name': name,  # Nouveau nom Steam
                            'last_seen': firestore.SERVER_TIMESTAMP,
                            'current_session_start': session_time
                        }
                        db.collection('players').document(doc_id).update(update)
                        writes += 1
                        update_player_cache(doc_id, {**existing_by_steamid, **update})
                        cache['live_to_doc'][name] = doc_id
                        print(f"          🔄 {name} (changement de nom)")
                    else:
                        # Nouveau joueur avec SteamID réel
                        doc_id = steam2
                        new_player = {
                            'name': name, 'steam_id': steam2, 'roles': ['Joueur'],
                            'ingame_names': [], 'created_at': firestore.SERVER_TIMESTAMP,
                            'last_seen': firestore.SERVER_TIMESTAMP, 'connected_at': now.isoformat(),
                            'current_session_start': session_time, 'total_time_seconds': 0,
                            'session_count': 1, 'is_auto_detected': False,
                            'avatar_url': avatar_url
                        }
                        db.collection('players').document(doc_id).set(new_player)
                        writes += 1
                        update_player_cache(doc_id, new_player)
                        cache['live_to_doc'][name] = doc_id
                        print(f"          🆕✅ {name} ({steam2})")
                else:
                    # Profil Steam non trouvé - fallback auto_xxx
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
                    cache['live_to_doc'][name] = doc_id
                    print(f"          🆕 {name} (auto)")
        
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

# ============================================
# Signal Handling (graceful shutdown)
# ============================================
def graceful_shutdown(signum, frame):
    """Handle termination signal - save player stats and release lock before exit"""
    global _db, cache
    signal_name = signal.Signals(signum).name
    print(f"\n⚠️ Signal {signal_name} reçu - sauvegarde en cours...")
    
    if _db:
        # Save player stats
        if cache.get('prev_names'):
            try:
                mark_offline(_db)
                print("✅ Stats sauvegardées avant arrêt")
            except Exception as e:
                print(f"❌ Erreur sauvegarde: {e}")
        
        # Release lock
        try:
            _db.collection('system').document('workflow_lock').delete()
            print("🔓 Lock libéré")
        except Exception as e:
            print(f"⚠️ Erreur release lock: {e}")
    
    sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGTERM, graceful_shutdown)
signal.signal(signal.SIGINT, graceful_shutdown)

# ============================================
# Workflow Lock (prevent parallel execution)
# ============================================
def acquire_lock(db):
    """Try to acquire workflow lock. Returns True if acquired, False if another workflow is running."""
    lock_ref = db.collection('system').document('workflow_lock')
    now = datetime.now(timezone.utc)
    
    try:
        lock_doc = lock_ref.get()
        if lock_doc.exists:
            lock_data = lock_doc.to_dict()
            lock_time = lock_data.get('started_at')
            if lock_time:
                # Convert Firestore timestamp to datetime
                if hasattr(lock_time, 'timestamp'):
                    lock_time = datetime.fromtimestamp(lock_time.timestamp(), tz=timezone.utc)
                
                # Lock is valid if less than 35 minutes old
                age_minutes = (now - lock_time).total_seconds() / 60
                if age_minutes < 35:
                    print(f"    🔒 Autre workflow en cours depuis {age_minutes:.1f}min - abandon")
                    return False
                else:
                    print(f"    ⚠️ Lock expiré ({age_minutes:.1f}min) - reprise")
        
        # Acquire lock
        lock_ref.set({
            'started_at': firestore.SERVER_TIMESTAMP,
            'pid': os.getpid(),
            'status': 'running'
        })
        print("    🔓 Lock acquis")
        return True
        
    except Exception as e:
        print(f"    ⚠️ Erreur lock: {e} - continuation")
        return True  # Continue anyway if lock check fails

def release_lock(db):
    """Release workflow lock."""
    try:
        db.collection('system').document('workflow_lock').delete()
        print("    🔓 Lock libéré")
    except Exception as e:
        print(f"    ⚠️ Erreur release lock: {e}")

# ============================================
# Main
# ============================================
def main():
    global _db
    
    print("=" * 50)
    print(f"🎮 GMod Status v15 ({QUERIES_PER_RUN} queries/30min)")
    print("=" * 50)
    
    try:
        db = init_firebase()
        _db = db  # For signal handler
        print("✅ Firebase OK")
    except Exception as e:
        print(f"❌ Firebase: {e}")
        sys.exit(1)
    
    # Check for parallel workflow
    if not acquire_lock(db):
        print("🛑 Workflow abandonné (autre instance en cours)")
        sys.exit(0)
    
    try:
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
    finally:
        release_lock(db)

if __name__ == "__main__":
    main()
