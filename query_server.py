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
from datetime import datetime, timezone

import a2s
import firebase_admin
from firebase_admin import credentials, firestore

# Configuration
GMOD_HOST = os.environ.get('GMOD_HOST', '51.91.215.65')
GMOD_PORT = int(os.environ.get('GMOD_PORT', '27015'))
LOOP_COUNT = int(os.environ.get('LOOP_COUNT', '5'))  # Nombre de requêtes par exécution
LOOP_DELAY = int(os.environ.get('LOOP_DELAY', '55'))  # Délai entre requêtes (secondes)

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
    
    print(f"\n[{iteration}] 🎮 Query {GMOD_HOST}:{GMOD_PORT} à {now.strftime('%H:%M:%S')}")
    
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
        
        # Update players
        for p in players:
            player_id = f"auto_{p['name'].lower().replace(' ', '_')}"
            player_ref = db.collection('players').document(player_id)
            player_doc = player_ref.get()
            
            if player_doc.exists:
                player_data = player_doc.to_dict()
                current_total = player_data.get('total_time_seconds', 0)
                player_ref.update({
                    'last_seen': firestore.SERVER_TIMESTAMP,
                    'total_time_seconds': current_total + 60,
                    'current_session_time': p['time']
                })
            else:
                player_ref.set({
                    'name': p['name'],
                    'steam_id': player_id,
                    'roles': ['??'],
                    'ingame_names': [p['name']],
                    'created_at': firestore.SERVER_TIMESTAMP,
                    'last_seen': firestore.SERVER_TIMESTAMP,
                    'total_time_seconds': p['time'],
                    'current_session_time': p['time'],
                    'is_auto_detected': True
                })
        
        # Update daily stats
        today = now.strftime('%Y-%m-%d')
        current_hour = now.hour
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
