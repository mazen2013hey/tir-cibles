'use strict';

const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(path.join(__dirname, 'public')));

// ---------------------------------------------------------------------------
// Constantes
// ---------------------------------------------------------------------------
const PORT = process.env.PORT || 3000;
const MATCH_DURATION_MS = 60_000; // durée de la bataille
const COUNTDOWN_MS = 3_000; // compte à rebours avant le début
const INVITE_TTL_MS = 20_000; // durée de vie d'une invitation
const POINTS_PER_TARGET = 10;
const SPAWNS = [
  { x: -2.5, z: 7 },
  { x: 2.5, z: 7 },
];

// ---------------------------------------------------------------------------
// État en mémoire
// ---------------------------------------------------------------------------
/** socket.id -> { id, nick, roomId } */
const players = new Map();
/** pseudo en minuscules -> socket.id  (garantit l'unicité insensible à la casse) */
const nickIndex = new Map();
/** inviteId -> { id, from, to, timer } */
const invites = new Map();
/** roomId -> match */
const matches = new Map();

let inviteCounter = 0;
let matchCounter = 0;

// ---------------------------------------------------------------------------
// Utilitaires
// ---------------------------------------------------------------------------
const ack = (cb, payload) => {
  if (typeof cb === 'function') cb(payload);
};

const cleanNick = (s) => s.normalize('NFC').trim().replace(/\s+/g, ' ');
const nickKey = (s) => cleanNick(s).toLocaleLowerCase('fr');
const rand = (min, max) => min + Math.random() * (max - min);
const round2 = (n) => Math.round(n * 100) / 100;
const isNum = (n) => typeof n === 'number' && Number.isFinite(n);
const clamp = (n, a, b) => Math.min(b, Math.max(a, n));

function validateNick(raw) {
  if (typeof raw !== 'string') return { error: 'Pseudo invalide.' };
  const nick = cleanNick(raw);
  if (nick.length < 2 || nick.length > 16) {
    return { error: 'Le pseudo doit contenir entre 2 et 16 caractères.' };
  }
  if (!/^[\p{L}\p{N}_\- ]+$/u.test(nick)) {
    return { error: 'Utilisez uniquement des lettres, chiffres, espaces, « _ » et « - ».' };
  }
  return { nick };
}

// ---------------------------------------------------------------------------
// Invitations
// ---------------------------------------------------------------------------
const hasOutgoingInvite = (id) => [...invites.values()].some((i) => i.from === id);
const hasIncomingInvite = (id) => [...invites.values()].some((i) => i.to === id);

function removeInvite(inv) {
  clearTimeout(inv.timer);
  invites.delete(inv.id);
}

/**
 * Annule toutes les invitations qui impliquent ce joueur.
 * statusForInviter : statut transmis à l'expéditeur si c'est le destinataire qui disparaît.
 */
function cancelInvitesOf(playerId, statusForInviter = 'offline') {
  for (const inv of [...invites.values()]) {
    if (inv.from !== playerId && inv.to !== playerId) continue;
    removeInvite(inv);
    if (inv.from === playerId) {
      io.to(inv.to).emit('invite-cancelled', { inviteId: inv.id });
    } else {
      const target = players.get(inv.to);
      io.to(inv.from).emit('invite-result', {
        status: statusForInviter,
        nick: target?.nick ?? '',
      });
    }
  }
}

// ---------------------------------------------------------------------------
// Bataille
// ---------------------------------------------------------------------------
function newTarget(match) {
  const prev = match.target;
  let t;
  do {
    t = {
      id: ++match.targetSeq,
      x: round2(rand(-8, 8)),
      y: round2(rand(1.2, 4.8)),
      z: round2(-rand(12, 26)),
    };
  } while (prev && Math.hypot(prev.x - t.x, prev.y - t.y) < 2); // évite de réapparaître au même endroit
  return t;
}

function startMatch(a, b) {
  const roomId = `match-${++matchCounter}`;
  const match = {
    id: roomId,
    players: [a.id, b.id],
    names: { [a.id]: a.nick, [b.id]: b.nick },
    scores: { [a.id]: 0, [b.id]: 0 },
    target: null,
    targetSeq: 0,
    state: 'countdown',
    ended: false,
    timers: [],
  };
  match.target = newTarget(match);
  matches.set(roomId, match);

  [a, b].forEach((p) => {
    p.roomId = roomId;
    cancelInvitesOf(p.id, 'cancelled');
    io.sockets.sockets.get(p.id)?.join(roomId);
  });

  match.players.forEach((pid, i) => {
    const oppId = match.players[1 - i];
    io.to(pid).emit('match-start', {
      you: SPAWNS[i],
      opponent: { nick: match.names[oppId], ...SPAWNS[1 - i] },
      countdownMs: COUNTDOWN_MS,
      durationMs: MATCH_DURATION_MS,
      points: POINTS_PER_TARGET,
      target: match.target,
    });
  });

  match.timers.push(setTimeout(() => (match.state = 'running'), COUNTDOWN_MS));
  match.timers.push(setTimeout(() => endMatch(match), COUNTDOWN_MS + MATCH_DURATION_MS));
}

function endMatch(match, forfeitBy = null) {
  if (match.ended) return;
  match.ended = true;
  match.state = 'ended';
  match.timers.forEach(clearTimeout);
  matches.delete(match.id);

  const [aId, bId] = match.players;
  const sa = match.scores[aId];
  const sb = match.scores[bId];
  let winner = null;
  if (forfeitBy) winner = forfeitBy === aId ? bId : aId;
  else if (sa > sb) winner = aId;
  else if (sb > sa) winner = bId;

  for (const pid of match.players) {
    const oppId = pid === aId ? bId : aId;
    const p = players.get(pid);
    if (p) p.roomId = null;
    io.sockets.sockets.get(pid)?.leave(match.id);

    io.to(pid).emit('match-end', {
      result: winner === null ? 'draw' : winner === pid ? 'win' : 'lose',
      you: match.scores[pid],
      opponent: match.scores[oppId],
      opponentNick: players.get(oppId)?.nick ?? match.names[oppId],
      reason: forfeitBy ? (forfeitBy === pid ? 'left' : 'opponent-left') : 'time',
    });
  }
}

const matchOf = (player) => (player?.roomId ? matches.get(player.roomId) : null);

// ---------------------------------------------------------------------------
// Socket.io
// ---------------------------------------------------------------------------
io.on('connection', (socket) => {
  players.set(socket.id, { id: socket.id, nick: null, roomId: null });

  // --- Pseudo (choix initial ET changement : même vérification anti-doublon) --
  socket.on('set-nick', (raw, cb) => {
    const me = players.get(socket.id);
    if (!me) return;

    const v = validateNick(raw);
    if (v.error) return ack(cb, { ok: false, error: v.error });

    const key = nickKey(v.nick);
    const owner = nickIndex.get(key);
    if (owner && owner !== socket.id) {
      return ack(cb, { ok: false, error: `Le pseudo « ${v.nick} » est déjà pris. Choisissez-en un autre.` });
    }

    const previous = me.nick;
    if (previous) {
      const prevKey = nickKey(previous);
      if (nickIndex.get(prevKey) === socket.id) nickIndex.delete(prevKey);
      // Les invitations envoyées sous l'ancien pseudo n'ont plus de sens
      cancelInvitesOf(socket.id, 'cancelled');
    }

    me.nick = v.nick;
    nickIndex.set(key, socket.id);

    const match = matchOf(me);
    if (match) {
      match.names[me.id] = me.nick;
      const oppId = match.players.find((id) => id !== me.id);
      io.to(oppId).emit('opponent-renamed', { nick: me.nick });
    }

    ack(cb, { ok: true, nick: me.nick });
  });

  // --- Invitation -----------------------------------------------------------
  socket.on('invite', (raw, cb) => {
    const me = players.get(socket.id);
    if (!me?.nick) return ack(cb, { ok: false, error: 'Choisissez d’abord un pseudo.' });
    if (me.roomId) return ack(cb, { ok: false, error: 'Vous êtes déjà en duel.' });
    if (hasOutgoingInvite(me.id)) {
      return ack(cb, { ok: false, error: 'Vous avez déjà une invitation en attente.' });
    }

    const name = typeof raw === 'string' ? cleanNick(raw) : '';
    if (!name) return ack(cb, { ok: false, error: 'Entrez le pseudo exact de votre ami.' });

    const targetId = nickIndex.get(nickKey(name));
    const target = targetId ? players.get(targetId) : null;
    if (!target) {
      return ack(cb, { ok: false, error: `Aucun joueur en ligne ne s’appelle « ${name} » (pseudo inexistant ou hors-ligne).` });
    }
    if (target.id === me.id) return ack(cb, { ok: false, error: 'Vous ne pouvez pas vous inviter vous-même.' });
    if (target.roomId) return ack(cb, { ok: false, error: `${target.nick} est déjà en duel.` });
    if (hasIncomingInvite(target.id)) {
      return ack(cb, { ok: false, error: `${target.nick} a déjà reçu une invitation. Réessayez dans quelques secondes.` });
    }

    const inv = { id: `inv-${++inviteCounter}`, from: me.id, to: target.id, timer: null };
    inv.timer = setTimeout(() => {
      if (!invites.has(inv.id)) return;
      invites.delete(inv.id);
      io.to(inv.to).emit('invite-cancelled', { inviteId: inv.id });
      io.to(inv.from).emit('invite-result', { status: 'expired', nick: target.nick });
    }, INVITE_TTL_MS);
    invites.set(inv.id, inv);

    io.to(target.id).emit('invite-received', { inviteId: inv.id, from: me.nick });
    ack(cb, { ok: true, to: target.nick });
  });

  socket.on('invite-response', (data) => {
    const inv = invites.get(data?.inviteId);
    if (!inv || inv.to !== socket.id) return;
    removeInvite(inv);

    const from = players.get(inv.from);
    const to = players.get(inv.to);
    if (!from || !to) return;

    if (!data.accept) {
      return void io.to(from.id).emit('invite-result', { status: 'declined', nick: to.nick });
    }
    if (from.roomId || to.roomId) {
      return void io.to(from.id).emit('invite-result', { status: 'cancelled', nick: to.nick });
    }
    startMatch(from, to);
  });

  // --- Pendant la bataille ---------------------------------------------------
  socket.on('hit', (data) => {
    const me = players.get(socket.id);
    const match = matchOf(me);
    if (!match || match.state !== 'running') return;
    // Le premier message valide pour la cible courante gagne les points
    if (!data || data.targetId !== match.target.id) return;

    const oldTarget = match.target;
    match.scores[me.id] += POINTS_PER_TARGET;
    match.target = newTarget(match);

    for (const pid of match.players) {
      const oppId = match.players.find((id) => id !== pid);
      io.to(pid).emit('target-hit', {
        byYou: pid === me.id,
        you: match.scores[pid],
        opponent: match.scores[oppId],
        oldTarget,
        target: match.target,
      });
    }
  });

  socket.on('move', (d) => {
    const me = players.get(socket.id);
    if (!me?.roomId || !d || !isNum(d.x) || !isNum(d.z) || !isNum(d.yaw) || !isNum(d.pitch)) return;
    socket.to(me.roomId).emit('opponent-move', {
      x: clamp(d.x, -12, 12),
      z: clamp(d.z, 3, 12),
      yaw: d.yaw,
      pitch: clamp(d.pitch, -1.5, 1.5),
    });
  });

  socket.on('fire', (d) => {
    const me = players.get(socket.id);
    if (!me?.roomId || !d || !Array.isArray(d.dir) || d.dir.length !== 3 || !d.dir.every(isNum)) return;
    socket.to(me.roomId).emit('opponent-fire', { dir: d.dir });
  });

  socket.on('leave-match', () => {
    const match = matchOf(players.get(socket.id));
    if (match) endMatch(match, socket.id);
  });

  // --- Déconnexion : le pseudo est libéré immédiatement --------------------
  socket.on('disconnect', () => {
    const me = players.get(socket.id);
    if (!me) return;

    const match = matchOf(me);
    if (match) endMatch(match, me.id); // forfait : l'adversaire gagne

    cancelInvitesOf(me.id, 'offline');

    if (me.nick && nickIndex.get(nickKey(me.nick)) === me.id) {
      nickIndex.delete(nickKey(me.nick));
    }
    players.delete(socket.id);
  });
});

server.listen(PORT, () => {
  console.log(`Serveur démarré sur http://localhost:${PORT}`);
});
