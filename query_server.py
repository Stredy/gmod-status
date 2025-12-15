#!/usr/bin/env python3
"""
GMod Server Status - GitHub Actions
Queries a GMod server using A2S protocol and syncs to Firebase
Runs in a loop to achieve ~1 minute update frequency
"""

import os
import sys
import json
import time
import re
import unicodedata
import requests
from html.parser import HTMLParser
from datetime import datetime, timezone, timedelta

# Configuration
GMOD_HOST = os.environ.get('GMOD_HOST', '51.91.215.65')
GMOD_PORT = int(os.environ.get('GMOD_PORT', '27015'))
LOOP_COUNT = int(os.environ.get('LOOP_COUNT', '5'))  # Nombre de requêtes par exécution
LOOP_DELAY = int(os.environ.get('LOOP_DELAY', '55'))  # Délai entre requêtes (secondes)

# Timezone France (UTC+1 hiver, UTC+2 été)
# On utilise une approximation simple pour l'heure française
def get_france_time():
    """Retourne l'heure actuelle en France (approximation UTC+1)"""
    utc_now = datetime.now(timezone.utc)
    # France est UTC+1 en hiver, UTC+2 en été
    # Approximation simple: UTC+1 (on peut améliorer avec pytz si besoin)
    france_offset = timedelta(hours=1)
    return utc_now + france_offset

# Steam Avatar Configuration
STEAMID64_BASE = 76561197960265728
STEAM2_RE = re.compile(r"^STEAM_[0-5]:([0-1]):(\d+)$", re.IGNORECASE)

def normalize_name(name):
    """
    Normalise un nom pour la comparaison:
    - Convertit en minuscules
    - Supprime les accents
    - Garde seulement les lettres et chiffres
    """
    if not name:
        return ""
    # Convertir en minuscules
    name = name.lower().strip()
    # Supprimer les accents
    name = unicodedata.normalize('NFD', name)
    name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
    # Garder seulement alphanumériques
    name = re.sub(r'[^a-z0-9]', '', name)
    return name

# ============================================
# Steam Avatar Scraper
# ============================================

def steam2_to_steamid64(steamid: str) -> Optional[str]:
    """Convertit STEAM_0:0:123456789 en SteamID64"""
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

    def _first_url_from_srcset(self, srcset: str) -> Optional[str]:
        if not srcset:
            return None
        first = srcset.split(",")[0].strip()
        if not first:
            return None
        return first.split(" ")[0].strip()

    def handle_starttag(self, tag: str, attrs):
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

    def handle_endtag(self, tag: str):
        if tag != "div":
            return
        if self.in_frame > 0:
            self.in_frame -= 1
            return
        if self.in_inner > 0:
            self.in_inner -= 1

def fetch_steam_avatar(steam_id: str) -> Optional[str]:
    """Récupère l'URL de l'avatar Steam à partir d'un Steam ID"""
    try:
        steamid64 = steam2_to_steamid64(steam_id)
        if not steamid64:
            return None
        
        url = f"https://steamcommunity.com/profiles/{steamid64}/?l=english"
        r = requests.get(
            url,
            timeout=10,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept-Language": "en-US,en;q=0.9",
            },
        )
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
        print(f"       ⚠️ Erreur avatar pour {steam_id}: {e}")
        return None

def format_duration(seconds):
    """Format seconds to human readable duration"""
    if not seconds or seconds < 60:
        return "< 1m"
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    if hours > 0:
        return f"{hours}h{minutes:02d}m"
    return f"{minutes}m"

def query_server(db, iteration):
    """Query server and update Firebase"""
    address = (GMOD_HOST, GMOD_PORT)
    now = datetime.now(timezone.utc)
    france_now = get_france_time()
    
    print(f"\n[{iteration}] 🎮 Query {GMOD_HOST}:{GMOD_PORT} à {france_now.strftime('%H:%M:%S')} (France)")
    
    try:
        # Get server info
        info = a2s.info(address, timeout=10)
        
        # Get players
        players_raw = a2s.players(address, timeout=10)
        
        # Format players data
        players = []
        for p in players_raw:
            if p.name and p.name.strip():
                players.append({
                    "name": p.name,
                    "score": p.score,
                    "time": int(p.duration)
                })
        
        players.sort(key=lambda x: x['time'], reverse=True)
        
        print(f"    ✅ {len(players)}/{info.max_players} joueurs sur {info.map_name}")
        
        if players:
            for p in players[:5]:
                print(f"       • {p['name']} ({format_duration(p['time'])})")
            if len(players) > 5:
                print(f"       ... et {len(players) - 5} autres")
        
        # Update live status
        live_data = {
            "ok": True,
            "serverName": info.server_name,
            "map": info.map_name,
            "count": len(players),
            "maxPlayers": info.max_players,
            "players": players,
            "timestamp": now.isoformat(),
            "updatedAt": now.isoformat()
        }
        
        db.collection('live').document('status').set(live_data)
        
        # Update players - chercher d'abord si le joueur existe déjà
        for p in players:
            player_name = p['name']
            player_name_lower = player_name.lower().strip()
            # Normaliser le nom pour la comparaison (enlever accents, caractères spéciaux)
            player_name_normalized = normalize_name(player_name)
            
            print(f"       🔍 Recherche: '{player_name}'")
            
            # Chercher le joueur dans tous les documents
            existing_player = None
            existing_player_id = None
            
            all_players = db.collection('players').get()
            for doc in all_players:
                data = doc.to_dict()
                doc_name = data.get('name', '')
                doc_name_lower = doc_name.lower().strip()
                doc_name_normalized = normalize_name(doc_name)
                
                # Match par nom exact
                if doc_name == player_name:
                    existing_player = doc
                    existing_player_id = doc.id
                    print(f"          ✓ Match exact: {doc.id}")
                    break
                
                # Match par nom normalisé (insensible à la casse)
                if doc_name_lower == player_name_lower:
                    existing_player = doc
                    existing_player_id = doc.id
                    print(f"          ✓ Match lowercase: {doc.id}")
                    break
                
                # Match par nom normalisé (sans caractères spéciaux)
                if doc_name_normalized == player_name_normalized and player_name_normalized:
                    existing_player = doc
                    existing_player_id = doc.id
                    print(f"          ✓ Match normalized: {doc.id}")
                    break
                
                # Match dans ingame_names
                ingame_names = data.get('ingame_names', [])
                for ingame in ingame_names:
                    ingame_lower = ingame.lower().strip()
                    ingame_normalized = normalize_name(ingame)
                    
                    if ingame == player_name or ingame_lower == player_name_lower:
                        existing_player = doc
                        existing_player_id = doc.id
                        print(f"          ✓ Match ingame_name: {doc.id} ({ingame})")
                        break
                    
                    if ingame_normalized == player_name_normalized and player_name_normalized:
                        existing_player = doc
                        existing_player_id = doc.id
                        print(f"          ✓ Match ingame normalized: {doc.id} ({ingame})")
                        break
                
                if existing_player:
                    break
            
            if existing_player:
                # Mettre à jour le joueur existant
                player_data = existing_player.to_dict()
                current_total = player_data.get('total_time_seconds', 0)
                steam_id = player_data.get('steam_id', existing_player_id)
                
                update_data = {
                    'last_seen': firestore.SERVER_TIMESTAMP,
                    'total_time_seconds': current_total + 60,
                    'current_session_time': p['time']
                }
                
                # Ajouter le nom aux ingame_names s'il n'y est pas
                ingame_names = player_data.get('ingame_names', [])
                if player_name not in ingame_names and player_name != player_data.get('name'):
                    ingame_names.append(player_name)
                    update_data['ingame_names'] = ingame_names
                
                # Récupérer l'avatar si pas déjà présent et si c'est un vrai Steam ID
                current_avatar = player_data.get('avatar_url')
                if not current_avatar and steam_id.startswith('STEAM_'):
                    print(f"       🖼️ Récupération avatar pour {steam_id}...")
                    avatar_url = fetch_steam_avatar(steam_id)
                    if avatar_url:
                        update_data['avatar_url'] = avatar_url
                        print(f"          ✓ Avatar trouvé!")
                
                db.collection('players').document(existing_player_id).update(update_data)
                print(f"          ✓ Mis à jour: +60s (total: {current_total + 60}s)")
            else:
                # Nouveau joueur - créer avec un ID auto
                player_id = f"auto_{player_name_lower.replace(' ', '_').replace('.', '_')}"
                print(f"          ➕ Nouveau joueur auto: {player_id}")
                db.collection('players').document(player_id).set({
                    'name': player_name,
                    'steam_id': player_id,
                    'roles': ['Joueur'],
                    'ingame_names': [],
                    'created_at': firestore.SERVER_TIMESTAMP,
                    'last_seen': firestore.SERVER_TIMESTAMP,
                    'total_time_seconds': p['time'],
                    'current_session_time': p['time'],
                    'is_auto_detected': True
                })
        
        # Update daily stats (using France time)
        today = france_now.strftime('%Y-%m-%d')
        current_hour = france_now.hour
        stats_ref = db.collection('stats').document('daily').collection('days').document(today)
        stats_doc = stats_ref.get()
        
        if stats_doc.exists:
            stats_data = stats_doc.to_dict()
            hourly = stats_data.get('hourly', {})
            hourly[str(current_hour)] = max(hourly.get(str(current_hour), 0), len(players))
            stats_ref.update({
                'peak': max(stats_data.get('peak', 0), len(players)),
                'hourly': hourly,
                'last_update': firestore.SERVER_TIMESTAMP
            })
        else:
            stats_ref.set({
                'date': today,
                'peak': len(players),
                'hourly': {str(current_hour): len(players)},
                'created_at': firestore.SERVER_TIMESTAMP,
                'last_update': firestore.SERVER_TIMESTAMP
            })
        
        # Check all-time records
        records_ref = db.collection('stats').document('records')
        records_doc = records_ref.get()
        
        if records_doc.exists:
            records = records_doc.to_dict()
            if len(players) > records.get('peak_count', 0):
                records_ref.update({
                    'peak_count': len(players),
                    'peak_date': now.isoformat()
                })
                print(f"    🏆 Nouveau record: {len(players)} joueurs!")
        else:
            records_ref.set({
                'peak_count': len(players),
                'peak_date': now.isoformat()
            })
        
        return True
        
    except Exception as e:
        print(f"    ⚠️ Erreur: {e}")
        
        db.collection('live').document('status').set({
            "ok": False,
            "error": str(e),
            "count": 0,
            "players": [],
            "timestamp": now.isoformat(),
            "updatedAt": now.isoformat()
        })
        return False

def main():
    print("=" * 50)
    print("🎮 GMod Status - GitHub Actions")
    print(f"   Configuration: {LOOP_COUNT} requêtes, {LOOP_DELAY}s d'intervalle")
    print("=" * 50)
    
    # Get Firebase credentials
    service_account_json = os.environ.get('FIREBASE_SERVICE_ACCOUNT')
    
    if not service_account_json:
        print("❌ FIREBASE_SERVICE_ACCOUNT non configuré!")
        sys.exit(1)
    
    # Initialize Firebase
    try:
        cred_dict = json.loads(service_account_json)
        cred = credentials.Certificate(cred_dict)
        firebase_admin.initialize_app(cred)
        db = firestore.client()
        print("✅ Firebase initialisé")
    except Exception as e:
        print(f"❌ Erreur Firebase: {e}")
        sys.exit(1)
    
    # Run queries in a loop
    success_count = 0
    for i in range(1, LOOP_COUNT + 1):
        if query_server(db, i):
            success_count += 1
        
        # Wait before next iteration (except for the last one)
        if i < LOOP_COUNT:
            print(f"    ⏳ Attente {LOOP_DELAY}s...")
            time.sleep(LOOP_DELAY)
    
    print("\n" + "=" * 50)
    print(f"✅ Terminé: {success_count}/{LOOP_COUNT} requêtes réussies")
    print("=" * 50)

if __name__ == "__main__":
    main()
