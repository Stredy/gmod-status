#!/usr/bin/env python3
"""
GMod Server Status v21 - ROBUST & BULLETPROOF
==============================================

Architecture robuste pour monitoring 24/7 sans perte de données.

Principe clé: Les timestamps absolus sont la source de vérité.
- session_started_at = moment réel de connexion
- Durée = now - session_started_at (pas de cap abusif)

Gestion des cas limites:
1. Joueur part entre 2 runs → détecté au démarrage, session reconstruite
2. Reset serveur GMod → détecté si time < prev_time - 60s
3. Workflow rate un run → les timestamps absolus permettent de recalculer
4. Changement de nom Steam → détecté via SteamID

Quotas Firebase:
- Init: ~260 reads (1x par run)
- Par query: 0-5 writes
- Par run (60 queries): ~150 writes max
- Par jour (48 runs): ~7500 writes, ~15000 reads
- Limites: 20k writes, 50k reads → ~35% utilisé
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
from typing import Optional, Tuple, Dict, Set, List
from html.parser import HTMLParser

import a2s
import firebase_admin
from firebase_admin import credentials, firestore

# ============================================
# Timezone France (avec gestion DST)
# ============================================
try:
    from zoneinfo import ZoneInfo
    PARIS_TZ = ZoneInfo('Europe/Paris')
    def get_france_time():
        return datetime.now(PARIS_TZ)
except ImportError:
    PARIS_TZ = None
    def get_france_time():
        # Fallback UTC+1 (approximatif)
        return datetime.now(timezone(timedelta(hours=1)))

# ============================================
# Configuration
# ============================================
QUERY_INTERVAL = 30          # Secondes entre chaque query
MAX_QUERIES = 60             # Nombre de queries par run (30 min)
SERVER_IP = os.environ.get('GMOD_HOST', '51.91.215.65')
SERVER_PORT = int(os.environ.get('GMOD_PORT', '27015'))

# Seuils et limites
TIMEOUTS_BEFORE_OFFLINE = 3  # 3 timeouts (1m30) avant de considérer offline
MAX_ACTIVITY_FEED = 20       # Garder les 20 derniers événements
MAX_SESSION_HISTORY = 50     # Garder les 50 dernières sessions par joueur
MIN_RECORD_THRESHOLD = 5     # Jamais de record < 5 joueurs
MAX_SESSION_DURATION = 86400 # 24h max par session (protection anti-bug)
LOCK_TIMEOUT = 35 * 60       # 35 minutes - si un lock est plus vieux, il est considéré abandonné
STEAM_DELAY = 0.5            # Délai entre les appels Steam (anti rate-limit)

# État global
running = True
_db = None

# ============================================
# Cache mémoire
# ============================================
cache = {
    # Stats
    'hourly_stats': {},
    'daily_peak': 0,
    'record_peak': 0,
    'record_valid': False,
    'today_date': None,
    
    # Players
    'players': {},           # doc_id -> player data
    'players_by_name': {},   # name.lower() -> doc_id
    
    # État serveur (du run précédent, lu depuis Firestore)
    'prev_players': {},      # name -> {time, session_started_at, doc_id}
    'prev_count': 0,
    'is_offline': False,
    'last_update_time': None,  # Timestamp de la dernière MAJ de live/status
    
    # Sessions en cours (ce run)
    'sessions': {},          # name -> {started_at, doc_id}
    'prev_times': {},        # name -> time (pour détecter reset GMod)
    
    # Tracking
    'consecutive_timeouts': 0,
    'activity_feed': [],
    
    # Reset detection
    'run_started_at': None,  # Timestamp de démarrage du run
}

# ============================================
# Signal handlers
# ============================================
def signal_handler(signum, frame):
    global running
    print(f"\n⚠️ Signal {signum} reçu, arrêt propre...")
    running = False
    
    # Libérer le lock si possible
    try:
        if _db:
            release_lock(_db)
    except:
        pass

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# ============================================
# Helpers
# ============================================
def wait_for_next_interval():
    """Attend le prochain intervalle de 30 secondes (:00 ou :30)"""
    now = datetime.now()
    current_second = now.second
    
    if current_second < 30:
        wait_until = 30
    else:
        wait_until = 60
    
    sleep_time = wait_until - current_second - now.microsecond / 1000000
    if sleep_time > 0:
        time.sleep(sleep_time)

def format_duration(seconds):
    """Formate une durée en heures/minutes"""
    if not seconds or seconds < 0:
        return "0m"
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    if hours > 0:
        return f"{hours}h{minutes:02d}"
    return f"{minutes}m"

def validate_player_name(name):
    """Valide un nom de joueur"""
    if not name:
        return False
    if len(name) > 64:
        return False
    if name.isspace():
        return False
    return True

def validate_player_time(time_val):
    """Valide un temps de connexion"""
    if time_val is None:
        return False
    if time_val < 0:
        return False
    if time_val > 86400 * 7:  # Max 7 jours
        return False
    return True

def normalize_name(name):
    """Normalise un nom pour la recherche"""
    if not name:
        return ""
    normalized = unicodedata.normalize('NFKD', name)
    ascii_name = normalized.encode('ASCII', 'ignore').decode('ASCII')
    return re.sub(r'[^a-zA-Z0-9]', '', ascii_name).lower()

def sanitize_doc_id(doc_id):
    """Nettoie un ID pour Firestore"""
    if not doc_id:
        return None
    sanitized = re.sub(r'[/\\.\[\]*`~]', '_', str(doc_id))
    sanitized = sanitized.strip('_')
    if not sanitized or len(sanitized) > 1500:
        return None
    return sanitized

# ============================================
# Steam API
# ============================================
STEAMID64_BASE = 76561197960265728
STEAM2_RE = re.compile(r"^STEAM_[0-5]:([0-1]):(\d+)$", re.IGNORECASE)

def steam2_to_steamid64(steamid):
    """Convertit STEAM_0:0:123456789 en SteamID64"""
    if not steamid:
        return None
    steamid = steamid.strip()
    if steamid.isdigit() and len(steamid) >= 16:
        return steamid
    m = STEAM2_RE.match(steamid)
    if not m:
        return None
    x = int(m.group(1))
    z = int(m.group(2))
    accountid = 2 * z + x
    return str(STEAMID64_BASE + accountid)

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

class SteamProfileParser(HTMLParser):
    """Parse le profil Steam pour extraire SteamID (pour la recherche par nom)"""
    def __init__(self):
        super().__init__()
        self.steam_id = None
        self.in_script = False
        
    def handle_starttag(self, tag, attrs):
        if tag == 'script':
            self.in_script = True
    
    def handle_endtag(self, tag):
        if tag == 'script':
            self.in_script = False
    
    def handle_data(self, data):
        if self.in_script and 'g_rgProfileData' in data:
            match = re.search(r'"steamid"\s*:\s*"(\d+)"', data)
            if match:
                self.steam_id = match.group(1)

def steam64_to_steam2(steam64):
    """Convertit SteamID64 en STEAM_0:X:Y"""
    try:
        steam64_int = int(steam64)
        y = steam64_int - STEAMID64_BASE
        x = y % 2
        y = (y - x) // 2
        return f"STEAM_0:{x}:{y}"
    except:
        return None

def fetch_steam_avatar(steam_id):
    """Récupère l'URL de l'avatar Steam à partir d'un Steam ID"""
    try:
        steamid64 = steam2_to_steamid64(steam_id)
        if not steamid64:
            return None
        
        # Anti rate-limit
        time.sleep(STEAM_DELAY)
        
        url = f"https://steamcommunity.com/profiles/{steamid64}/?l=english"
        r = requests.get(
            url,
            timeout=15,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            },
        )
        
        if r.status_code == 429:
            print(f"          ⏳ Rate-limit Steam, skip...")
            return None
        
        r.raise_for_status()
        
        parser = SteamAvatarParser()
        parser.feed(r.text)
        
        # Priorité à l'avatar animé, sinon statique
        if parser.animated:
            return parser.animated
        if parser.static_candidates:
            return parser.static_candidates[0]
        
        return None
    except Exception as e:
        return None

def fetch_steam_info(name):
    """Récupère SteamID et avatar depuis le profil Steam (recherche par nom)"""
    try:
        # Anti rate-limit
        time.sleep(STEAM_DELAY)
        
        # Chercher le profil
        resp = requests.get(
            f"https://steamcommunity.com/search/SearchCommunityAjax",
            params={'text': name, 'filter': 'users', 'sessionid': '', 'page': 1},
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            },
            timeout=10
        )
        
        if resp.status_code != 200:
            return None, None
        
        data = resp.json()
        html = data.get('html', '')
        
        # Trouver le lien du profil
        match = re.search(r'href="(https://steamcommunity\.com/(?:id|profiles)/[^"]+)"', html)
        if not match:
            return None, None
        
        profile_url = match.group(1)
        
        # Anti rate-limit
        time.sleep(STEAM_DELAY)
        
        # Récupérer le profil
        resp = requests.get(
            profile_url + "?l=english",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=15
        )
        if resp.status_code != 200:
            return None, None
        
        # Extraire le SteamID
        profile_parser = SteamProfileParser()
        profile_parser.feed(resp.text)
        steam2 = steam64_to_steam2(profile_parser.steam_id) if profile_parser.steam_id else None
        
        # Extraire l'avatar
        avatar_parser = SteamAvatarParser()
        avatar_parser.feed(resp.text)
        avatar = avatar_parser.animated or (avatar_parser.static_candidates[0] if avatar_parser.static_candidates else None)
        
        return steam2, avatar
        
    except Exception as e:
        return None, None

# ============================================
# Firebase
# ============================================
def init_firebase():
    """Initialise Firebase"""
    global _db
    if _db:
        return _db
    
    service_account_json = os.environ.get('FIREBASE_SERVICE_ACCOUNT')
    if not service_account_json:
        raise ValueError("FIREBASE_SERVICE_ACCOUNT not set")
    
    cred = credentials.Certificate(json.loads(service_account_json))
    firebase_admin.initialize_app(cred)
    _db = firestore.client()
    return _db

def acquire_lock(db):
    """
    Acquiert un lock pour éviter les runs parallèles.
    Retourne True si le lock est acquis, False sinon.
    """
    lock_ref = db.collection('system').document('lock')
    now = get_france_time()
    
    try:
        lock_doc = lock_ref.get()
        
        if lock_doc.exists:
            lock_data = lock_doc.to_dict()
            locked_at_str = lock_data.get('locked_at', '')
            
            if locked_at_str:
                try:
                    locked_at = datetime.fromisoformat(locked_at_str.replace('Z', '+00:00'))
                    age = (now - locked_at).total_seconds()
                    
                    if age < LOCK_TIMEOUT:
                        # Lock encore valide
                        print(f"    🔒 Lock actif depuis {int(age)}s par {lock_data.get('run_id', '?')}")
                        return False
                    else:
                        # Lock expiré (workflow précédent a crash)
                        print(f"    ⚠️ Lock expiré ({int(age)}s), récupération...")
                except:
                    pass
        
        # Créer/Remplacer le lock
        run_id = f"{now.strftime('%H%M%S')}_{os.getpid()}"
        lock_ref.set({
            'locked_at': now.isoformat(),
            'run_id': run_id,
            'expires_at': (now + timedelta(seconds=LOCK_TIMEOUT)).isoformat()
        })
        
        print(f"    🔓 Lock acquis: {run_id}")
        return True
        
    except Exception as e:
        print(f"    ⚠️ Erreur lock: {e}")
        return False

def release_lock(db):
    """Libère le lock"""
    try:
        db.collection('system').document('lock').delete()
        print("    🔓 Lock libéré")
    except Exception as e:
        print(f"    ⚠️ Erreur release lock: {e}")

# ============================================
# Player lookup
# ============================================
def find_player(name):
    """Trouve un joueur par nom Steam dans le cache"""
    if not name:
        return None
    key = name.lower().strip()
    normalized = normalize_name(name)
    
    # Recherche par nom Steam uniquement
    doc_id = cache['players_by_name'].get(key) or cache['players_by_name'].get(normalized)
    if doc_id and doc_id in cache['players']:
        return (doc_id, cache['players'][doc_id])
    
    return None

def update_player_cache(doc_id, data):
    """Met à jour le cache local d'un joueur"""
    if not doc_id:
        return
    if doc_id in cache['players']:
        cache['players'][doc_id].update(data)
    else:
        cache['players'][doc_id] = data
    
    name = data.get('name', '')
    if name:
        cache['players_by_name'][name.lower().strip()] = doc_id
        cache['players_by_name'][normalize_name(name)] = doc_id

# ============================================
# Activity Feed
# ============================================
def add_activity_event(event_type, name, duration=0, doc_id=None, timestamp=None):
    """Ajoute un événement au feed d'activité"""
    if timestamp is None:
        timestamp = get_france_time()
    
    timestamp_str = timestamp.isoformat() if hasattr(timestamp, 'isoformat') else str(timestamp)
    
    event = {
        'type': event_type,
        'name': name,
        'timestamp': timestamp_str,
        'duration': duration,
        'doc_id': doc_id
    }
    
    cache['activity_feed'].insert(0, event)
    cache['activity_feed'] = cache['activity_feed'][:MAX_ACTIVITY_FEED]

# ============================================
# Reset Detection
# ============================================
def check_and_handle_reset(db):
    """
    Vérifie si un reset a été effectué depuis le frontend.
    Si oui, recharge les données depuis Firestore.
    Retourne True si un reset a été détecté.
    """
    try:
        doc = db.collection('system').document('reset').get()
        if not doc.exists:
            return False
        
        data = doc.to_dict()
        reset_at_str = data.get('reset_at')
        
        if not reset_at_str:
            # Document existe mais pas de timestamp, le supprimer
            db.collection('system').document('reset').delete()
            return False
        
        # Parser le timestamp
        try:
            reset_at = datetime.fromisoformat(reset_at_str.replace('Z', '+00:00'))
        except:
            db.collection('system').document('reset').delete()
            return False
        
        # Comparer avec le démarrage du run
        if cache['run_started_at'] and reset_at > cache['run_started_at']:
            print(f"       🔄 RESET détecté! Rechargement des données...")
            reload_players_from_firestore(db)
            
            # Mettre à jour run_started_at pour ne pas re-détecter
            cache['run_started_at'] = get_france_time()
            
            # Supprimer le flag de reset
            db.collection('system').document('reset').delete()
            
            return True
        else:
            # Reset ancien (avant ce run), juste supprimer le document
            db.collection('system').document('reset').delete()
            return False
        
    except Exception as e:
        # Silencieux - pas grave si on ne peut pas vérifier
        return False

def reload_players_from_firestore(db):
    """Recharge toutes les données des joueurs depuis Firestore après un reset"""
    try:
        # Vider le cache des joueurs
        cache['players'].clear()
        cache['players_by_name'].clear()
        
        # Recharger les joueurs
        docs = db.collection('players').get()
        for doc in docs:
            data = doc.to_dict()
            doc_id = doc.id
            cache['players'][doc_id] = data
            name = data.get('name', '')
            if name:
                cache['players_by_name'][name.lower().strip()] = doc_id
                cache['players_by_name'][normalize_name(name)] = doc_id
        
        # Réinitialiser les sessions en cours pour qu'elles démarrent maintenant
        # ET incrémenter session_count pour les joueurs avec session active
        now = get_france_time()
        sessions_updated = 0
        
        for name, session in cache['sessions'].items():
            session['started_at'] = now
            doc_id = session.get('doc_id')
            if doc_id and doc_id in cache['players']:
                try:
                    # Incrémenter session_count (car c'est une nouvelle session post-reset)
                    data = cache['players'][doc_id]
                    new_count = data.get('session_count', 0) + 1
                    
                    db.collection('players').document(doc_id).update({
                        'current_session_start': now.isoformat(),
                        'session_count': new_count,
                        'last_seen': firestore.SERVER_TIMESTAMP
                    })
                    
                    # Mettre à jour le cache local
                    cache['players'][doc_id]['session_count'] = new_count
                    sessions_updated += 1
                except:
                    pass
        
        # Réinitialiser prev_times (sera recalculé au prochain query)
        cache['prev_times'].clear()
        
        # Vider l'activity feed et régénérer pour les joueurs en ligne
        cache['activity_feed'].clear()
        for name, session in cache['sessions'].items():
            doc_id = session.get('doc_id')
            add_activity_event('join', name, 0, doc_id, timestamp=now)
        
        # Mettre à jour live/status immédiatement
        online_players = []
        for name, session in cache['sessions'].items():
            doc_id = session.get('doc_id')
            online_players.append({
                'name': name,
                'time': 0,  # Temps reset à 0
                'doc_id': doc_id,
                'session_started_at': now.isoformat()
            })
        
        try:
            db.collection('live').document('status').set({
                'ok': True,
                'count': len(online_players),
                'players': online_players,
                'activity_feed': cache['activity_feed'],
                'timestamp': now.isoformat(),
                'updatedAt': now.isoformat()
            })
        except:
            pass
        
        # Mettre à jour cache/players
        write_players_cache(db)
        
        print(f"       ✅ {len(cache['players'])} joueurs rechargés, {sessions_updated} sessions réinitialisées")
        
    except Exception as e:
        print(f"       ⚠️ Erreur rechargement: {e}")

# ============================================
# Session Management
# ============================================
def finalize_session(db, name, doc_id, started_at, ended_at, writes):
    """Finalise une session : calcule durée, met à jour total, ajoute à l'historique"""
    if not doc_id or not started_at:
        return writes
    
    # Calculer la durée depuis les timestamps (source de vérité)
    duration = int((ended_at - started_at).total_seconds())
    
    # Protection: durée négative ou > 24h = bug
    if duration < 0:
        print(f"          ⚠️ Durée négative pour {name}, ignoré")
        return writes
    if duration > MAX_SESSION_DURATION:
        print(f"          ⚠️ Session > 24h pour {name}, capée à 24h")
        duration = MAX_SESSION_DURATION
    
    # Récupérer les données du joueur
    data = cache['players'].get(doc_id, {})
    new_total = data.get('total_time_seconds', 0) + duration
    
    # Récupérer l'historique existant
    existing_history = data.get('session_history', [])
    
    # Créer l'entrée de session
    new_session = {
        'start': started_at.isoformat(),
        'end': ended_at.isoformat(),
        'duration': duration
    }
    existing_history.insert(0, new_session)
    existing_history = existing_history[:MAX_SESSION_HISTORY]
    
    # Mettre à jour Firestore
    try:
        db.collection('players').document(doc_id).update({
            'total_time_seconds': new_total,
            'last_seen': firestore.SERVER_TIMESTAMP,
            'current_session_start': None,
            'session_history': existing_history
        })
        writes += 1
        
        # Mettre à jour le cache
        update_player_cache(doc_id, {
            **data,
            'total_time_seconds': new_total,
            'session_history': existing_history
        })
        
        # Ajouter au feed d'activité
        add_activity_event('leave', name, duration, doc_id, timestamp=ended_at)
        
        print(f"          👋 {name} (+{format_duration(duration)})")
    except Exception as e:
        print(f"          ❌ Erreur finalisation {name}: {e}")
    
    return writes

# ============================================
# Cache Players pour Frontend
# ============================================
def write_players_cache(db):
    """Écrit le cache des joueurs pour le frontend (1 seul document)
    
    IMPORTANT: Lit d'abord les données existantes pour préserver les champs
    modifiés par le frontend (roles, ingame_names, steam_id) qui ne sont pas gérés par le backend.
    Préserve aussi les joueurs créés par le frontend que le backend ne connaît pas.
    """
    if not cache['players']:
        return
    
    # Lire les données existantes pour préserver les modifications du frontend
    existing_cache = {}
    try:
        doc = db.collection('cache').document('players').get()
        if doc.exists:
            existing_cache = doc.to_dict().get('players', {})
    except:
        pass
    
    # Commencer avec les joueurs existants du frontend (pour ne pas les perdre)
    players_cache = {}
    
    # D'abord, garder tous les joueurs du frontend que le backend ne connaît pas
    for doc_id, existing_data in existing_cache.items():
        if doc_id not in cache['players']:
            # Joueur créé par le frontend (scanner, etc.) - le garder tel quel
            players_cache[doc_id] = existing_data
    
    # Ensuite, mettre à jour avec les données du backend
    for doc_id, data in cache['players'].items():
        session_history = data.get('session_history', [])[:10]
        
        # Calculer last_played à partir de session_history
        # C'est la date de fin de la dernière session
        last_played = None
        if session_history and len(session_history) > 0:
            last_session = session_history[0]
            if last_session.get('end'):
                last_played = last_session['end']
        
        # Préserver certains champs du cache existant si présents
        # (au cas où le frontend les a modifiés pendant ce run)
        existing = existing_cache.get(doc_id, {})
        
        players_cache[doc_id] = {
            'name': data.get('name', '') or existing.get('name', ''),
            'steam_id': existing.get('steam_id') or data.get('steam_id', ''),  # Préférer le frontend
            # Préférer les valeurs existantes du cache pour ces champs
            'roles': existing.get('roles') or data.get('roles', ['Joueur']),
            'avatar_url': data.get('avatar_url', '') or existing.get('avatar_url', ''),
            'ingame_names': existing.get('ingame_names') if existing.get('ingame_names') else data.get('ingame_names', []),
            'total_time_seconds': data.get('total_time_seconds', 0),
            'session_count': data.get('session_count', 0),
            'is_auto_detected': data.get('is_auto_detected', False),
            'session_history': session_history,
            'last_played': last_played,  # Date ISO de la dernière session
        }
    
    try:
        db.collection('cache').document('players').set({
            'players': players_cache,
            'count': len(players_cache),
            'updatedAt': firestore.SERVER_TIMESTAMP
        })
        print(f"    📦 Cache: {len(players_cache)} joueurs")
    except Exception as e:
        print(f"    ⚠️ Cache: {e}")

# ============================================
# Init Cache
# ============================================
def init_cache(db, france_now):
    """
    Charge toutes les données au démarrage.
    IMPORTANT: Détecte les départs manqués (joueurs partis entre 2 runs)
    """
    global cache
    reads = 0
    writes = 0
    today = france_now.strftime('%Y-%m-%d')
    
    print("    📦 Chargement...")
    
    # Stats du jour
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
            record_data = doc.to_dict()
            cache['record_peak'] = record_data.get('peak_count', 0)
            if cache['record_peak'] >= MIN_RECORD_THRESHOLD:
                cache['record_valid'] = True
                print(f"    ✅ Record: {cache['record_peak']}")
            else:
                print(f"    ⚠️ Record suspect: {cache['record_peak']}")
        else:
            print(f"    ⚠️ Document records inexistant")
    except Exception as e:
        print(f"    ❌ Records: {e}")
        cache['record_valid'] = False
    
    # Reconstruire le record si nécessaire
    if not cache['record_valid']:
        try:
            print(f"    🔧 Reconstruction du record...")
            days_ref = db.collection('stats').document('daily').collection('days')
            days_docs = days_ref.get()
            max_peak = 0
            max_date = None
            for day_doc in days_docs:
                data = day_doc.to_dict()
                peak = data.get('peak', 0)
                if peak > max_peak:
                    max_peak = peak
                    max_date = day_doc.id
                reads += 1
            
            if max_peak >= MIN_RECORD_THRESHOLD:
                cache['record_peak'] = max_peak
                cache['record_valid'] = True
                db.collection('stats').document('records').set({
                    'peak_count': max_peak,
                    'peak_date': max_date
                })
                writes += 1
                print(f"    ✅ Record reconstruit: {max_peak} ({max_date})")
            else:
                print(f"    ⚠️ Pas de record valide trouvé (max={max_peak})")
        except Exception as e:
            print(f"    ❌ Reconstruction record: {e}")
    
    # Charger tous les joueurs
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
    
    # Charger live/status (état du run précédent)
    last_update_time = None
    try:
        doc = db.collection('live').document('status').get()
        reads += 1
        if doc.exists:
            data = doc.to_dict()
            cache['is_offline'] = not data.get('ok', True)
            cache['prev_count'] = data.get('count', 0)
            
            # Récupérer le timestamp de la dernière mise à jour
            timestamp_str = data.get('timestamp')
            if timestamp_str:
                try:
                    last_update_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                except:
                    pass
            
            # Récupérer l'activity feed existant
            cache['activity_feed'] = data.get('activity_feed', [])
            if cache['activity_feed']:
                print(f"    📜 {len(cache['activity_feed'])} événements récupérés")
            
            # Sauvegarder les joueurs du run précédent
            for p in data.get('players', []):
                name = p['name']
                time_val = p.get('time', 0)
                started_at_str = p.get('session_started_at')
                
                started_at = None
                if started_at_str:
                    try:
                        started_at = datetime.fromisoformat(started_at_str.replace('Z', '+00:00'))
                    except:
                        started_at = france_now - timedelta(seconds=time_val)
                else:
                    started_at = france_now - timedelta(seconds=time_val)
                
                found = find_player(name)
                doc_id = found[0] if found else None
                
                cache['prev_players'][name] = {
                    'time': time_val,
                    'session_started_at': started_at,
                    'doc_id': doc_id
                }
                cache['prev_times'][name] = time_val
                
                # Créer la session active
                if doc_id:
                    cache['sessions'][name] = {
                        'started_at': started_at,
                        'doc_id': doc_id
                    }
            
            print(f"    🔗 {len(cache['prev_players'])} joueurs au run précédent")
            if last_update_time:
                age = (france_now - last_update_time).total_seconds()
                print(f"    ⏰ Dernière MAJ il y a {int(age)}s")
    except Exception as e:
        print(f"    ⚠️ Live: {e}")
    
    # Stocker pour detect_missed_departures
    cache['last_update_time'] = last_update_time
    
    cache['today_date'] = today
    print(f"    📊 H{france_now.hour}, peak={cache['daily_peak']}, record={cache['record_peak']}")
    print(f"    📖 {reads} reads, {writes} writes")
    
    # Écrire le cache pour le frontend
    write_players_cache(db)
    
    return reads, writes

def detect_missed_departures(db, current_players, france_now):
    """
    Détecte les joueurs qui étaient là au run précédent mais ne sont plus là.
    Ce sont des départs manqués qu'il faut comptabiliser.
    """
    writes = 0
    
    prev_names = set(cache['prev_players'].keys())
    current_names = set(current_players.keys())
    
    missed_departures = prev_names - current_names
    
    if missed_departures:
        print(f"    🔍 {len(missed_departures)} départ(s) manqué(s) détecté(s)")
        
        # Utiliser le timestamp réel de la dernière MAJ + quelques secondes
        # C'est la meilleure estimation possible du moment du départ
        last_update = cache.get('last_update_time')
        if last_update:
            # Estimer le départ juste après la dernière MAJ (+ 30s de marge)
            estimated_departure = last_update + timedelta(seconds=30)
        else:
            # Fallback si pas de timestamp
            estimated_departure = france_now - timedelta(minutes=25)
        
        for name in missed_departures:
            prev_data = cache['prev_players'].get(name, {})
            doc_id = prev_data.get('doc_id')
            started_at = prev_data.get('session_started_at')
            
            if doc_id and started_at:
                writes = finalize_session(db, name, doc_id, started_at, estimated_departure, writes)
            else:
                print(f"          ⚠️ {name}: données manquantes, ignoré")
            
            # Nettoyer
            cache['sessions'].pop(name, None)
            cache['prev_times'].pop(name, None)
    
    return writes

# ============================================
# Query Server
# ============================================
def query_server():
    """Query le serveur GMod et retourne les données"""
    try:
        address = (SERVER_IP, SERVER_PORT)
        info = a2s.info(address, timeout=5)
        players = a2s.players(address, timeout=5)
        
        player_data = {}
        for p in players:
            name = p.name
            time_val = int(p.duration) if p.duration else 0
            
            # Validation des données
            if not validate_player_name(name):
                continue
            if not validate_player_time(time_val):
                time_val = 0
            
            player_data[name] = max(0, time_val)
        
        return {
            'ok': True,
            'count': len(player_data),
            'max_players': info.max_players,
            'map': info.map_name,
            'server_name': info.server_name,
            'players': player_data
        }
    except Exception as e:
        return {'ok': False, 'error': str(e)}

# ============================================
# Main Sync Loop
# ============================================
def run_sync(db):
    """Boucle principale de synchronisation"""
    global running
    
    france_now = get_france_time()
    cache['run_started_at'] = france_now  # Pour détecter les resets
    
    print(f"\n🚀 GMod Monitor v21 - {france_now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"    Serveur: {SERVER_IP}:{SERVER_PORT}")
    print(f"    Intervalle: {QUERY_INTERVAL}s, Max queries: {MAX_QUERIES}")
    
    # Initialisation
    print("\n📦 INIT")
    reads, writes = init_cache(db, france_now)
    
    # Supprimer tout document reset résiduel
    just_reset = False
    try:
        reset_doc = db.collection('system').document('reset').get()
        if reset_doc.exists:
            db.collection('system').document('reset').delete()
            just_reset = True
            print("    🧹 Document reset nettoyé - mode post-reset")
    except:
        pass
    
    # Query initial pour détecter les départs manqués
    print("\n🔍 DÉTECTION DÉPARTS MANQUÉS")
    initial_data = query_server()
    if initial_data['ok']:
        current_players = initial_data.get('players', {})
        now = get_france_time()
        
        # Si post-reset, NE PAS finaliser les sessions (elles datent d'avant le reset)
        if just_reset:
            print(f"    ⏭️ Skip (post-reset) - les sessions précédentes ne comptent pas")
            
            # IMPORTANT: Recharger les données des joueurs depuis Firestore
            # car init_cache() a pu charger des données avant que le reset soit complet
            print(f"    🔄 Rechargement des données post-reset...")
            cache['players'].clear()
            cache['players_by_name'].clear()
            docs = db.collection('players').get()
            for doc in docs:
                data = doc.to_dict()
                doc_id = doc.id
                cache['players'][doc_id] = data
                name = data.get('name', '')
                if name:
                    cache['players_by_name'][name.lower().strip()] = doc_id
                    cache['players_by_name'][normalize_name(name)] = doc_id
            print(f"    ✅ {len(cache['players'])} joueurs rechargés depuis Firestore")
            
            # Vider les sessions du cache (elles sont invalides)
            cache['sessions'].clear()
            cache['prev_players'].clear()
            cache['prev_times'].clear()
            cache['activity_feed'].clear()
            
            # Incrémenter session_count pour tous les joueurs présents
            if len(current_players) > 0:
                print(f"    🔄 Post-reset: incrémentation des sessions...")
                for name, time_val in current_players.items():
                    found = find_player(name)
                    if found:
                        doc_id = found[0]
                        started_at = now - timedelta(seconds=time_val)
                        try:
                            data = cache['players'].get(doc_id, {})
                            new_count = data.get('session_count', 0) + 1
                            db.collection('players').document(doc_id).update({
                                'session_count': new_count,
                                'current_session_start': started_at.isoformat(),
                                'last_seen': firestore.SERVER_TIMESTAMP
                            })
                            writes += 1
                            if doc_id in cache['players']:
                                cache['players'][doc_id]['session_count'] = new_count
                            
                            # Créer la session
                            cache['sessions'][name] = {'started_at': started_at, 'doc_id': doc_id}
                            cache['prev_times'][name] = time_val
                            
                            # Ajouter au feed
                            add_activity_event('join', name, time_val, doc_id, timestamp=started_at)
                            
                            print(f"        ✅ {name}: session #{new_count}")
                        except Exception as e:
                            print(f"        ⚠️ {name}: {e}")
            
            # Mettre à jour live/status IMMÉDIATEMENT avec les vrais joueurs en ligne
            online_players = []
            for name, time_val in current_players.items():
                found = find_player(name)
                doc_id = found[0] if found else None
                session = cache['sessions'].get(name)
                started_at = session['started_at'] if session else now - timedelta(seconds=time_val)
                
                online_players.append({
                    'name': name,
                    'time': time_val,
                    'doc_id': doc_id,
                    'session_started_at': started_at.isoformat()
                })
            
            db.collection('live').document('status').set({
                'ok': True,
                'count': len(current_players),
                'players': online_players,
                'activity_feed': cache['activity_feed'],
                'timestamp': now.isoformat(),
                'updatedAt': now.isoformat()
            })
            writes += 1
            print(f"    📡 live/status mis à jour: {len(online_players)} joueurs en ligne")
            
            # Mettre à jour le cache pour le frontend
            write_players_cache(db)
        else:
            # Mode normal: détecter les départs manqués
            writes += detect_missed_departures(db, current_players, now)
        
        # Générer le feed initial si vide ET créer les sessions
        if len(cache['activity_feed']) == 0 and len(current_players) > 0:
            print(f"    📜 Génération du feed initial...")
            for name, time_val in current_players.items():
                started_at = now - timedelta(seconds=time_val)
                found = find_player(name)
                doc_id = found[0] if found else None
                
                add_activity_event('join', name, time_val, doc_id, timestamp=started_at)
                
                # Créer la session si elle n'existe pas
                if name not in cache['sessions'] and doc_id:
                    cache['sessions'][name] = {'started_at': started_at, 'doc_id': doc_id}
                
                cache['prev_times'][name] = time_val
            
            cache['activity_feed'].sort(key=lambda x: x['timestamp'], reverse=True)
            print(f"    ✅ {len(cache['activity_feed'])} événements générés")
            
            # Sauvegarder immédiatement
            try:
                db.collection('live').document('status').update({
                    'activity_feed': cache['activity_feed']
                })
            except:
                pass
    else:
        print(f"    ⚠️ Serveur inaccessible: {initial_data.get('error')}")
    
    # Boucle principale
    print(f"\n🔄 MONITORING")
    query_count = 0
    total_writes = writes
    steam_cache = {}  # Cache des lookups Steam (par run)
    
    while running and query_count < MAX_QUERIES:
        query_count += 1
        now = get_france_time()
        today = now.strftime('%Y-%m-%d')
        hour = now.hour
        
        # Vérifier si un reset a été effectué depuis le frontend
        check_and_handle_reset(db)
        
        # Changement de jour ?
        if today != cache['today_date']:
            print(f"\n    🌅 Nouveau jour: {today}")
            cache['today_date'] = today
            cache['hourly_stats'] = {}
            cache['daily_peak'] = 0
        
        print(f"\n    [{query_count}/{MAX_QUERIES}] {now.strftime('%H:%M:%S')}")
        
        # Query serveur
        server_data = query_server()
        
        if not server_data['ok']:
            cache['consecutive_timeouts'] += 1
            
            if cache['is_offline']:
                # Déjà offline, pas besoin de re-afficher
                print(f"       ⏱️ Toujours hors ligne...")
            else:
                print(f"       ⏱️ Timeout ({cache['consecutive_timeouts']}/{TIMEOUTS_BEFORE_OFFLINE})")
                
                if cache['consecutive_timeouts'] >= TIMEOUTS_BEFORE_OFFLINE:
                    cache['is_offline'] = True
                    print(f"       🔴 Serveur hors ligne")
                    
                    # Finaliser toutes les sessions
                    for name, session in list(cache['sessions'].items()):
                        doc_id = session.get('doc_id')
                        started_at = session.get('started_at')
                        if doc_id and started_at:
                            total_writes = finalize_session(db, name, doc_id, started_at, now, total_writes)
                    
                    cache['sessions'].clear()
                    cache['prev_times'].clear()
                    
                    # Écrire statut offline
                    db.collection('live').document('status').set({
                        'ok': False,
                        'count': 0,
                        'players': [],
                        'activity_feed': cache['activity_feed'],
                        'timestamp': now.isoformat(),
                        'updatedAt': now.isoformat()
                    })
                    total_writes += 1
            
            wait_for_next_interval()
            continue
        
        # Serveur OK
        cache['consecutive_timeouts'] = 0
        was_offline = cache['is_offline']
        cache['is_offline'] = False
        
        current_players = server_data.get('players', {})
        current_names = set(current_players.keys())
        current_count = len(current_players)
        
        previous_names = set(cache['sessions'].keys())
        
        joined = current_names - previous_names
        left = previous_names - current_names
        stayed = current_names & previous_names
        
        players_changed = (joined or left or was_offline)
        
        print(f"       👥 {current_count} joueurs | +{len(joined)} -{len(left)} ={len(stayed)}")
        
        # ============================================
        # PHASE 1: Détection reset serveur GMod
        # ============================================
        for name in stayed:
            current_time = current_players[name]
            prev_time = cache['prev_times'].get(name, 0)
            
            if current_time < prev_time - 60:
                print(f"       🔄 Reset GMod détecté pour {name} ({prev_time}s → {current_time}s)")
                
                # Finaliser l'ancienne session
                session = cache['sessions'].get(name)
                if session:
                    doc_id = session.get('doc_id')
                    started_at = session.get('started_at')
                    if doc_id and started_at:
                        total_writes = finalize_session(db, name, doc_id, started_at, now, total_writes)
                
                # Créer une nouvelle session
                new_started_at = now - timedelta(seconds=current_time)
                found = find_player(name)
                if found:
                    doc_id = found[0]
                    cache['sessions'][name] = {'started_at': new_started_at, 'doc_id': doc_id}
                    add_activity_event('join', name, current_time, doc_id, timestamp=new_started_at)
        
        # ============================================
        # PHASE 2: Recherche Steam (pour les nouveaux)
        # ============================================
        for name in joined:
            if name not in steam_cache:
                steam_id, avatar = fetch_steam_info(name)
                steam_cache[name] = (steam_id, avatar)
                if steam_id:
                    print(f"       🔍 Steam: {name} → {steam_id}")
        
        # ============================================
        # PHASE 3: Départs
        # ============================================
        for name in left:
            session = cache['sessions'].get(name)
            
            if session:
                doc_id = session.get('doc_id')
                started_at = session.get('started_at')
                
                if doc_id and started_at:
                    total_writes = finalize_session(db, name, doc_id, started_at, now, total_writes)
                else:
                    # Session sans données complètes
                    found = find_player(name)
                    if found:
                        doc_id = found[0]
                        try:
                            db.collection('players').document(doc_id).update({
                                'last_seen': firestore.SERVER_TIMESTAMP,
                                'current_session_start': None
                            })
                            total_writes += 1
                            add_activity_event('leave', name, 0, doc_id)
                        except:
                            pass
                    print(f"          👋 {name} (session incomplète)")
            
            cache['sessions'].pop(name, None)
            cache['prev_times'].pop(name, None)
        
        # ============================================
        # PHASE 4: Arrivées
        # ============================================
        for name in joined:
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
                
                # Refresh avatar
                if steam_id.startswith('STEAM_'):
                    avatar = fetch_steam_avatar(steam_id)
                    if avatar and avatar != data.get('avatar_url'):
                        update['avatar_url'] = avatar
                
                try:
                    db.collection('players').document(doc_id).update(update)
                    total_writes += 1
                    update_player_cache(doc_id, {**data, **update})
                    
                    cache['sessions'][name] = {'started_at': started_at, 'doc_id': doc_id}
                    add_activity_event('join', name, session_time, doc_id, timestamp=started_at)
                    print(f"          ⬆️ {name} ({format_duration(session_time)})")
                except Exception as e:
                    print(f"          ❌ Arrivée {name}: {e}")
            else:
                # Nouveau joueur
                steam2, avatar_url = steam_cache.get(name, (None, None))
                
                if steam2:
                    doc_id = sanitize_doc_id(steam2)
                    if not doc_id:
                        continue
                    
                    existing_data = cache['players'].get(doc_id)
                    
                    if existing_data:
                        # SteamID existe déjà
                        update = {
                            'name': name,
                            'last_seen': firestore.SERVER_TIMESTAMP,
                            'current_session_start': started_at.isoformat(),
                            'session_count': existing_data.get('session_count', 0) + 1
                        }
                        if avatar_url:
                            update['avatar_url'] = avatar_url
                        
                        try:
                            db.collection('players').document(doc_id).update(update)
                            total_writes += 1
                            update_player_cache(doc_id, {**existing_data, **update})
                            add_activity_event('join', name, session_time, doc_id, timestamp=started_at)
                            print(f"          🔄 {name} (steam existant)")
                        except Exception as e:
                            print(f"          ❌ {name}: {e}")
                    else:
                        # Vraiment nouveau
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
                            'session_history': [],
                            'is_auto_detected': False,
                            'avatar_url': avatar_url
                        }
                        try:
                            db.collection('players').document(doc_id).set(new_player)
                            total_writes += 1
                            update_player_cache(doc_id, new_player)
                            add_activity_event('join', name, session_time, doc_id, timestamp=started_at)
                            print(f"          🆕✅ {name}")
                        except Exception as e:
                            print(f"          ❌ {name}: {e}")
                    
                    cache['sessions'][name] = {'started_at': started_at, 'doc_id': doc_id}
                else:
                    # Pas de Steam → auto_xxx
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
                        try:
                            db.collection('players').document(doc_id).update(update)
                            total_writes += 1
                            update_player_cache(doc_id, {**existing_auto, **update})
                        except:
                            pass
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
                            'session_history': [],
                            'is_auto_detected': True
                        }
                        try:
                            db.collection('players').document(doc_id).set(new_player)
                            total_writes += 1
                            update_player_cache(doc_id, new_player)
                        except:
                            pass
                    
                    cache['sessions'][name] = {'started_at': started_at, 'doc_id': doc_id}
                    add_activity_event('join', name, session_time, doc_id, timestamp=started_at)
                    print(f"          🆕 {name} (auto)")
        
        # ============================================
        # PHASE 5: Stayed - vérifier cohérence
        # ============================================
        for name in stayed:
            if name not in cache['sessions']:
                session_time = current_players[name]
                started_at = now - timedelta(seconds=session_time)
                
                found = find_player(name)
                if found:
                    doc_id = found[0]
                    cache['sessions'][name] = {'started_at': started_at, 'doc_id': doc_id}
        
        # ============================================
        # PHASE 6: Écrire live/status
        # ============================================
        players_for_firebase = []
        for name, time_val in current_players.items():
            session = cache['sessions'].get(name)
            doc_id = session.get('doc_id') if session else None
            
            entry = {
                'name': name, 
                'time': time_val,
                'doc_id': doc_id
            }
            
            if session and session.get('started_at'):
                entry['session_started_at'] = session['started_at'].isoformat()
            else:
                entry['session_started_at'] = (now - timedelta(seconds=time_val)).isoformat()
            
            players_for_firebase.append(entry)
        
        if players_changed:
            try:
                db.collection('live').document('status').set({
                    'ok': True,
                    'count': current_count,
                    'max': server_data['max_players'],
                    'map': server_data['map'],
                    'server': server_data['server_name'],
                    'players': players_for_firebase,
                    'activity_feed': cache['activity_feed'],
                    'timestamp': now.isoformat(),
                    'updatedAt': now.isoformat()
                })
                total_writes += 1
            except Exception as e:
                print(f"       ⚠️ Live: {e}")
        
        # Mettre à jour prev_times
        cache['prev_times'] = current_players.copy()
        
        # ============================================
        # PHASE 7: Stats
        # ============================================
        cached_hour = cache['hourly_stats'].get(hour, -1)
        if cached_hour == -1 or current_count > cached_hour:
            cache['hourly_stats'][hour] = max(cached_hour if cached_hour >= 0 else 0, current_count)
            cache['daily_peak'] = max(cache['daily_peak'], current_count)
            
            try:
                db.collection('stats').document('daily').collection('days').document(today).set({
                    'date': today,
                    'peak': cache['daily_peak'],
                    'hourly': {str(k): v for k, v in cache['hourly_stats'].items()},
                    'last_update': firestore.SERVER_TIMESTAMP
                }, merge=True)
                total_writes += 1
                print(f"       📈 H{hour}: {current_count}")
            except:
                pass
        
        # Record
        if cache['record_valid'] and current_count > cache['record_peak'] and current_count >= MIN_RECORD_THRESHOLD:
            try:
                current_record_doc = db.collection('stats').document('records').get()
                if current_record_doc.exists:
                    current_record = current_record_doc.to_dict().get('peak_count', 0)
                    if current_count > current_record:
                        cache['record_peak'] = current_count
                        db.collection('stats').document('records').set({
                            'peak_count': current_count,
                            'peak_date': now.isoformat()
                        })
                        total_writes += 1
                        print(f"       🏆 NOUVEAU RECORD: {current_count}!")
                    else:
                        cache['record_peak'] = current_record
            except Exception as e:
                print(f"       ⚠️ Record: {e}")
        
        # ============================================
        # PHASE 8: Cache players (si changements)
        # ============================================
        if players_changed:
            write_players_cache(db)
        
        # Attendre le prochain intervalle
        wait_for_next_interval()
    
    # Fin du run
    print(f"\n✅ Fin du monitoring: {query_count} queries, {total_writes} writes")
    return total_writes

# ============================================
# Main
# ============================================
def main():
    try:
        print("🔧 Initialisation Firebase...")
        db = init_firebase()
        
        # Acquérir le lock
        print("\n🔒 LOCK")
        if not acquire_lock(db):
            print("❌ Un autre run est en cours, abandon.")
            return 0  # Pas une erreur, juste on attend le prochain
        
        try:
            writes = run_sync(db)
            print(f"\n📊 Total writes: {writes}")
        finally:
            # Toujours libérer le lock
            release_lock(db)
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Erreur fatale: {e}")
        import traceback
        traceback.print_exc()
        
        # Tenter de libérer le lock même en cas d'erreur
        try:
            if _db:
                release_lock(_db)
        except:
            pass
        
        return 1

if __name__ == '__main__':
    sys.exit(main())
