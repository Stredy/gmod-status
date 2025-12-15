/**
 * GMod Server Query Script
 * 
 * Ce script est exécuté par GitHub Actions toutes les 5 minutes.
 * Il query le serveur GMod et écrit les résultats dans Firebase.
 */

const Gamedig = require('gamedig');
const admin = require('firebase-admin');

// Configuration
const GMOD_HOST = process.env.GMOD_HOST || '51.91.215.65';
const GMOD_PORT = parseInt(process.env.GMOD_PORT) || 27015;

// Initialiser Firebase
function initFirebase() {
  try {
    const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT || '{}');
    
    if (!serviceAccount.project_id) {
      console.error('❌ FIREBASE_SERVICE_ACCOUNT non configuré');
      process.exit(1);
    }
    
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount)
    });
    
    console.log('✅ Firebase initialisé');
    return admin.firestore();
    
  } catch (error) {
    console.error('❌ Erreur Firebase:', error.message);
    process.exit(1);
  }
}

// Query le serveur GMod
async function queryGmodServer() {
  console.log(`🎮 Query du serveur ${GMOD_HOST}:${GMOD_PORT}...`);
  
  try {
    const result = await Gamedig.query({
      type: 'garrysmod',
      host: GMOD_HOST,
      port: GMOD_PORT,
      socketTimeout: 5000,
      attemptTimeout: 10000,
      givenPortOnly: true
    });
    
    const data = {
      ok: true,
      serverName: result.name || 'Unknown',
      map: result.map || 'Unknown',
      count: result.players?.length || 0,
      maxPlayers: result.maxplayers || 0,
      players: (result.players || []).map(p => ({
        name: p.name || 'Unknown',
        score: p.score || 0,
        time: Math.round(p.raw?.time || 0)
      })),
      ping: result.ping || 0,
      updatedAt: new Date().toISOString()
    };
    
    console.log(`✅ Serveur en ligne: ${data.count}/${data.maxPlayers} joueurs sur ${data.map}`);
    
    if (data.players.length > 0) {
      console.log('   Joueurs:', data.players.map(p => p.name).join(', '));
    }
    
    return data;
    
  } catch (error) {
    console.error('❌ Serveur hors ligne ou erreur:', error.message);
    
    return {
      ok: false,
      error: error.message,
      count: 0,
      players: [],
      updatedAt: new Date().toISOString()
    };
  }
}

// Écrire dans Firebase
async function writeToFirebase(db, data) {
  const now = new Date();
  
  try {
    // 1. Écrire le status live
    await db.collection('live').doc('status').set({
      ...data,
      timestamp: admin.firestore.Timestamp.fromDate(now)
    });
    
    console.log('✅ Status live mis à jour dans Firebase');
    
    // 2. Si des joueurs sont en ligne, les enregistrer/mettre à jour
    if (data.ok && data.players && data.players.length > 0) {
      const batch = db.batch();
      const playersRef = db.collection('players');
      
      for (const player of data.players) {
        if (!player.name || player.name === 'Unknown' || player.name.trim() === '') continue;
        
        // Chercher si le joueur existe déjà (par nom)
        const existingQuery = await playersRef
          .where('name', '==', player.name)
          .limit(1)
          .get();
        
        if (!existingQuery.empty) {
          // Joueur existant - mettre à jour
          const doc = existingQuery.docs[0];
          const existingData = doc.data();
          
          batch.update(doc.ref, {
            last_seen: admin.firestore.Timestamp.fromDate(now),
            // Ajouter 1 minute (60 secondes) de temps de jeu
            total_time_seconds: (existingData.total_time_seconds || 0) + 60,
            current_session_time: player.time || 0
          });
        } else {
          // Nouveau joueur - le créer
          const newId = `auto_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
          const newPlayerRef = playersRef.doc(newId);
          
          batch.set(newPlayerRef, {
            name: player.name,
            steam_id: newId,
            roles: ['Joueur'],
            ingame_names: [player.name],
            created_at: admin.firestore.Timestamp.fromDate(now),
            last_seen: admin.firestore.Timestamp.fromDate(now),
            total_time_seconds: 60,
            current_session_time: player.time || 0,
            is_auto_detected: true
          });
          
          console.log(`   🆕 Nouveau joueur enregistré: ${player.name}`);
        }
      }
      
      await batch.commit();
      console.log(`✅ ${data.players.length} joueur(s) mis à jour`);
    }
    
    // 3. Mettre à jour les stats du jour
    await updateDailyStats(db, data, now);
    
  } catch (error) {
    console.error('❌ Erreur écriture Firebase:', error.message);
  }
}

// Mettre à jour les statistiques journalières
async function updateDailyStats(db, data, now) {
  if (!data.ok) return;
  
  const dateKey = now.toISOString().split('T')[0]; // YYYY-MM-DD
  const hour = now.getUTCHours();
  
  try {
    const statsRef = db.collection('stats').doc('daily').collection('days').doc(dateKey);
    const statsDoc = await statsRef.get();
    
    let hourlyCount = new Array(24).fill(0);
    let uniquePlayers = [];
    let peakCount = data.count;
    let peakTime = admin.firestore.Timestamp.fromDate(now);
    
    if (statsDoc.exists) {
      const existing = statsDoc.data();
      hourlyCount = existing.hourly_counts || new Array(24).fill(0);
      uniquePlayers = existing.unique_players || [];
      
      // Mettre à jour le peak si battu
      if (data.count > (existing.peak_count || 0)) {
        peakCount = data.count;
        peakTime = admin.firestore.Timestamp.fromDate(now);
      } else {
        peakCount = existing.peak_count || 0;
        peakTime = existing.peak_time || admin.firestore.Timestamp.fromDate(now);
      }
    }
    
    // Mettre à jour le max pour cette heure
    if (data.count > hourlyCount[hour]) {
      hourlyCount[hour] = data.count;
    }
    
    // Ajouter les joueurs uniques
    const playerNames = data.players.map(p => p.name).filter(n => n && n !== 'Unknown');
    uniquePlayers = [...new Set([...uniquePlayers, ...playerNames])];
    
    await statsRef.set({
      date: admin.firestore.Timestamp.fromDate(now),
      peak_count: peakCount,
      peak_time: peakTime,
      hourly_counts: hourlyCount,
      unique_players: uniquePlayers
    });
    
    // Vérifier le record all-time
    const recordsRef = db.collection('stats').doc('records');
    const recordsDoc = await recordsRef.get();
    const currentRecord = recordsDoc.exists ? (recordsDoc.data().peak_count || 0) : 0;
    
    if (data.count > currentRecord) {
      await recordsRef.set({
        peak_count: data.count,
        peak_date: admin.firestore.Timestamp.fromDate(now),
        peak_players: playerNames
      });
      
      console.log(`🏆 NOUVEAU RECORD: ${data.count} joueurs!`);
    }
    
    console.log('✅ Stats journalières mises à jour');
    
  } catch (error) {
    console.error('❌ Erreur mise à jour stats:', error.message);
  }
}

// Main
async function main() {
  console.log('═══════════════════════════════════════════');
  console.log('🎮 GMod Status - GitHub Actions');
  console.log(`📅 ${new Date().toISOString()}`);
  console.log('═══════════════════════════════════════════');
  
  const db = initFirebase();
  const data = await queryGmodServer();
  await writeToFirebase(db, data);
  
  console.log('');
  console.log('✅ Terminé!');
}

main().catch(err => {
  console.error('❌ Erreur fatale:', err);
  process.exit(1);
});
