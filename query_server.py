"""
GMod Server Query Script (Python + a2s)

Ce script est exécuté par GitHub Actions toutes les minutes.
Il query le serveur GMod avec a2s et écrit les résultats dans Firebase.
"""

import os
import sys
import socket
import json
from datetime import datetime, timezone

import a2s
import firebase_admin
from firebase_admin import credentials, firestore

# Configuration
GMOD_HOST = os.environ.get('GMOD_HOST', '51.91.215.65')
GMOD_PORT = int(os.environ.get('GMOD_PORT', '27015'))

def init_firebase():
    """Initialise Firebase avec les credentials depuis la variable d'environnement."""
    try:
        service_account_json = os.environ.get('FIREBASE_SERVICE_ACCOUNT', '{}')
        service_account = json.loads(service_account_json)
        
        if 'project_id' not in service_account:
            print('❌ FIREBASE_SERVICE_ACCOUNT non configuré')
            sys.exit(1)
        
        cred = credentials.Certificate(service_account)
        firebase_admin.initialize_app(cred)
        
        print('✅ Firebase initialisé')
        return firestore.client()
        
    except Exception as e:
        print(f'❌ Erreur Firebase: {e}')
        sys.exit(1)

def query_gmod_server():
    """Query le serveur GMod avec a2s."""
    addr = (GMOD_HOST, GMOD_PORT)
    socket.setdefaulttimeout(5.0)
    
    print(f'🎮 Query du serveur {GMOD_HOST}:{GMOD_PORT}...')
    
    try:
        # Récupérer les infos du serveur
        info = a2s.info(addr)
        
        # Récupérer la liste des joueurs
        players_raw = a2s.players(addr)
        
        players = []
        for p in players_raw:
            name = getattr(p, 'name', '') or 'Unknown'
            score = getattr(p, 'score', 0)
            duration = int(getattr(p, 'duration', 0))
            
            # Ignorer les joueurs sans nom ou avec nom vide
            if name and name.strip() and name != 'Unknown':
                players.append({
                    'name': name.strip(),
                    'score': score,
                    'time': duration
                })
        
        data = {
            'ok': True,
            'serverName': getattr(info, 'server_name', 'Unknown'),
            'map': getattr(info, 'map_name', 'Unknown'),
            'game': getattr(info, 'game', 'DarkRP'),
            'count': len(players),
            'maxPlayers': getattr(info, 'max_players', 128),
            'players': players,
            'ping': 0,
            'updatedAt': datetime.now(timezone.utc).isoformat()
        }
        
        print(f'✅ Serveur en ligne: {data["count"]}/{data["maxPlayers"]} joueurs sur {data["map"]}')
        
        if players:
            player_names = [p['name'] for p in players]
            print(f'   Joueurs: {", ".join(player_names)}')
        
        return data
        
    except Exception as e:
        print(f'❌ Serveur hors ligne ou erreur: {e}')
        
        return {
            'ok': False,
            'error': str(e),
            'count': 0,
            'players': [],
            'updatedAt': datetime.now(timezone.utc).isoformat()
        }

def write_to_firebase(db, data):
    """Écrit les données dans Firebase."""
    now = datetime.now(timezone.utc)
    
    try:
        # 1. Écrire le status live
        db.collection('live').document('status').set({
            **data,
            'timestamp': firestore.SERVER_TIMESTAMP
        })
        
        print('✅ Status live mis à jour dans Firebase')
        
        # 2. Si des joueurs sont en ligne, les enregistrer/mettre à jour
        if data['ok'] and data['players']:
            players_ref = db.collection('players')
            
            for player in data['players']:
                name = player['name']
                if not name or name == 'Unknown':
                    continue
                
                # Chercher si le joueur existe déjà
                existing = players_ref.where('name', '==', name).limit(1).get()
                
                if existing:
                    # Joueur existant - mettre à jour
                    doc = existing[0]
                    existing_data = doc.to_dict()
                    
                    doc.reference.update({
                        'last_seen': firestore.SERVER_TIMESTAMP,
                        'total_time_seconds': existing_data.get('total_time_seconds', 0) + 60,
                        'current_session_time': player['time']
                    })
                else:
                    # Nouveau joueur - le créer
                    import random
                    import string
                    random_id = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
                    new_id = f'auto_{int(now.timestamp())}_{random_id}'
                    
                    players_ref.document(new_id).set({
                        'name': name,
                        'steam_id': new_id,
                        'roles': ['Joueur'],
                        'ingame_names': [name],
                        'created_at': firestore.SERVER_TIMESTAMP,
                        'last_seen': firestore.SERVER_TIMESTAMP,
                        'total_time_seconds': 60,
                        'current_session_time': player['time'],
                        'is_auto_detected': True
                    })
                    
                    print(f'   🆕 Nouveau joueur enregistré: {name}')
            
            print(f'✅ {len(data["players"])} joueur(s) mis à jour')
        
        # 3. Mettre à jour les stats du jour
        update_daily_stats(db, data, now)
        
    except Exception as e:
        print(f'❌ Erreur écriture Firebase: {e}')

def update_daily_stats(db, data, now):
    """Met à jour les statistiques journalières."""
    if not data['ok']:
        return
    
    date_key = now.strftime('%Y-%m-%d')
    hour = now.hour
    
    try:
        stats_ref = db.collection('stats').document('daily').collection('days').document(date_key)
        stats_doc = stats_ref.get()
        
        hourly_counts = [0] * 24
        unique_players = []
        peak_count = data['count']
        peak_time = now
        
        if stats_doc.exists:
            existing = stats_doc.to_dict()
            hourly_counts = existing.get('hourly_counts', [0] * 24)
            unique_players = existing.get('unique_players', [])
            
            if data['count'] > existing.get('peak_count', 0):
                peak_count = data['count']
                peak_time = now
            else:
                peak_count = existing.get('peak_count', 0)
                peak_time = existing.get('peak_time', now)
        
        # Mettre à jour le max pour cette heure
        if data['count'] > hourly_counts[hour]:
            hourly_counts[hour] = data['count']
        
        # Ajouter les joueurs uniques
        player_names = [p['name'] for p in data['players'] if p['name'] and p['name'] != 'Unknown']
        unique_players = list(set(unique_players + player_names))
        
        stats_ref.set({
            'date': firestore.SERVER_TIMESTAMP,
            'peak_count': peak_count,
            'peak_time': peak_time if isinstance(peak_time, datetime) else peak_time,
            'hourly_counts': hourly_counts,
            'unique_players': unique_players
        })
        
        # Vérifier le record all-time
        records_ref = db.collection('stats').document('records')
        records_doc = records_ref.get()
        current_record = records_doc.to_dict().get('peak_count', 0) if records_doc.exists else 0
        
        if data['count'] > current_record:
            records_ref.set({
                'peak_count': data['count'],
                'peak_date': firestore.SERVER_TIMESTAMP,
                'peak_players': player_names
            })
            
            print(f'🏆 NOUVEAU RECORD: {data["count"]} joueurs!')
        
        print('✅ Stats journalières mises à jour')
        
    except Exception as e:
        print(f'❌ Erreur mise à jour stats: {e}')

def main():
    print('═══════════════════════════════════════════')
    print('🎮 GMod Status - GitHub Actions (Python)')
    print(f'📅 {datetime.now(timezone.utc).isoformat()}')
    print('═══════════════════════════════════════════')
    
    db = init_firebase()
    data = query_gmod_server()
    write_to_firebase(db, data)
    
    print('')
    print('✅ Terminé!')

if __name__ == '__main__':
    main()
