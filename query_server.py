#!/usr/bin/env python3
"""
GMod Server Status - GitHub Actions
Queries a GMod server using A2S protocol and syncs to Firebase
"""

import os
import sys
import json
from datetime import datetime, timezone

# Install dependencies
import a2s
import firebase_admin
from firebase_admin import credentials, firestore

def format_duration(seconds):
    """Format seconds to human readable duration"""
    if not seconds or seconds < 60:
        return "< 1m"
    
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    
    if hours > 0:
        return f"{hours}h{minutes:02d}m"
    return f"{minutes}m"

def main():
    print("🎮 GMod Status - GitHub Actions")
    print("=" * 40)
    
    # Get configuration from environment
    host = os.environ.get('GMOD_HOST', '51.91.215.65')
    port = int(os.environ.get('GMOD_PORT', '27015'))
    service_account_json = os.environ.get('FIREBASE_SERVICE_ACCOUNT')
    
    if not service_account_json:
        print("❌ FIREBASE_SERVICE_ACCOUNT non configuré!")
        print("   Ajoutez le secret dans Settings → Secrets → Actions")
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
    
    # Query server
    address = (host, port)
    print(f"🎮 Query du serveur {host}:{port}...")
    
    try:
        # Get server info
        info = a2s.info(address, timeout=10)
        
        # Get players
        players_raw = a2s.players(address, timeout=10)
        
        # Format players data
        players = []
        for p in players_raw:
            if p.name and p.name.strip():  # Skip empty names
                players.append({
                    "name": p.name,
                    "score": p.score,
                    "time": int(p.duration)  # Session time in seconds
                })
        
        # Sort by time (longest first)
        players.sort(key=lambda x: x['time'], reverse=True)
        
        print(f"✅ Serveur en ligne: {len(players)}/{info.max_players} joueurs sur {info.map_name}")
        
        if players:
            print("   Joueurs:")
            for p in players[:10]:  # Show first 10
                print(f"      • {p['name']} (en jeu depuis {format_duration(p['time'])})")
            if len(players) > 10:
                print(f"      ... et {len(players) - 10} autres")
        
        # Prepare live status data
        now = datetime.now(timezone.utc)
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
        
        # Update Firebase /live/status
        db.collection('live').document('status').set(live_data)
        print("✅ Status live mis à jour dans Firebase")
        
        # Update/create players in database
        updated_count = 0
        for p in players:
            # Create a deterministic ID based on name (since we don't have Steam ID from A2S)
            player_id = f"auto_{p['name'].lower().replace(' ', '_')}"
            player_ref = db.collection('players').document(player_id)
            player_doc = player_ref.get()
            
            if player_doc.exists:
                # Update existing player
                player_data = player_doc.to_dict()
                current_total = player_data.get('total_time_seconds', 0)
                
                player_ref.update({
                    'last_seen': firestore.SERVER_TIMESTAMP,
                    'total_time_seconds': current_total + 60,  # Add 1 minute per check
                    'current_session_time': p['time']
                })
            else:
                # Create new player (auto-detected)
                player_ref.set({
                    'name': p['name'],
                    'steam_id': player_id,
                    'roles': ['Non scanné'],  # Important: mark as unscanned
                    'ingame_names': [p['name']],
                    'created_at': firestore.SERVER_TIMESTAMP,
                    'last_seen': firestore.SERVER_TIMESTAMP,
                    'total_time_seconds': p['time'],
                    'current_session_time': p['time'],
                    'is_auto_detected': True
                })
            
            updated_count += 1
        
        print(f"✅ {updated_count} joueur(s) mis à jour")
        
        # Update daily stats
        today = now.strftime('%Y-%m-%d')
        stats_ref = db.collection('stats').document('daily').collection('days').document(today)
        stats_doc = stats_ref.get()
        
        current_hour = now.hour
        
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
        
        print("✅ Stats journalières mises à jour")
        
        # Check all-time records
        records_ref = db.collection('stats').document('records')
        records_doc = records_ref.get()
        
        if records_doc.exists:
            records = records_doc.to_dict()
            if len(players) > records.get('all_time_peak', 0):
                records_ref.update({
                    'all_time_peak': len(players),
                    'all_time_peak_date': now.isoformat()
                })
                print(f"🏆 Nouveau record: {len(players)} joueurs!")
        else:
            records_ref.set({
                'all_time_peak': len(players),
                'all_time_peak_date': now.isoformat()
            })
        
    except Exception as e:
        print(f"⚠️ Serveur hors ligne ou erreur: {e}")
        
        # Update Firebase with offline status
        now = datetime.now(timezone.utc)
        db.collection('live').document('status').set({
            "ok": False,
            "error": str(e),
            "count": 0,
            "players": [],
            "timestamp": now.isoformat(),
            "updatedAt": now.isoformat()
        })
    
    print("=" * 40)
    print("✅ Terminé!")

if __name__ == "__main__":
    main()
