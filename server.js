// Bilard VR - serwer gry online (przekaźnik WebSocket, pokoje z 4-cyfrowym kodem).
// Czysty Node.js, bez żadnych bibliotek. Ten sam protokół co scripts/net/server.gd.
// Uruchomienie: node server.js   (port z PORT, domyślnie 9080) - na Render.com działa od razu.
'use strict';
const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 9080;
const TEST_CODE = process.env.TEST_CODE || '';
const MAX_MSG = 1 << 20;
const rooms = new Map();           // kod -> { host, guest }
let clients = new Set();

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end(`Bilard VR - serwer gry online dziala. Pokoje: ${rooms.size}, gracze: ${clients.size}\n`);
});

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if ((req.headers.upgrade || '').toLowerCase() !== 'websocket' || !key) {
    socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
    return;
  }
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n' +
               `Sec-WebSocket-Accept: ${accept}\r\n\r\n`);
  socket.setNoDelay(true);
  const peer = { socket, room: null, buf: Buffer.alloc(0), frag: [], alive: true };
  clients.add(peer);
  socket.on('data', (chunk) => onData(peer, chunk));
  socket.on('close', () => drop(peer));
  socket.on('error', () => drop(peer));
});

// --- ramki WebSocket (RFC 6455) ----------------------------------------------------

function sendFrame(peer, opcode, payload) {
  if (!peer.socket.writable) return;
  const len = payload.length;
  let head;
  if (len < 126) {
    head = Buffer.from([0x80 | opcode, len]);
  } else if (len < 65536) {
    head = Buffer.alloc(4);
    head[0] = 0x80 | opcode; head[1] = 126; head.writeUInt16BE(len, 2);
  } else {
    head = Buffer.alloc(10);
    head[0] = 0x80 | opcode; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2);
  }
  peer.socket.write(Buffer.concat([head, payload]));
}

function sendText(peer, text) { if (peer) sendFrame(peer, 0x1, Buffer.from(text, 'utf8')); }
function send(peer, obj) { sendText(peer, JSON.stringify(obj)); }

function onData(peer, chunk) {
  peer.buf = Buffer.concat([peer.buf, chunk]);
  for (;;) {
    const b = peer.buf;
    if (b.length < 2) return;
    const fin = (b[0] & 0x80) !== 0, opcode = b[0] & 0x0f, masked = (b[1] & 0x80) !== 0;
    let len = b[1] & 0x7f, off = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
    if (len > MAX_MSG) { peer.socket.destroy(); return; }
    const need = off + (masked ? 4 : 0) + len;
    if (b.length < need) return;
    let payload = b.subarray(off + (masked ? 4 : 0), need);
    if (masked) {
      const mask = b.subarray(off, off + 4);
      payload = Buffer.from(payload);
      for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
    }
    peer.buf = b.subarray(need);
    if (opcode === 0x8) { sendFrame(peer, 0x8, Buffer.alloc(0)); peer.socket.end(); return; }
    if (opcode === 0x9) { sendFrame(peer, 0xA, payload); continue; }
    if (opcode === 0xA) { peer.alive = true; continue; }
    if (opcode === 0x1 || opcode === 0x2 || opcode === 0x0) {
      peer.frag.push(payload);
      if (fin) {
        const text = Buffer.concat(peer.frag).toString('utf8');
        peer.frag = [];
        onMessage(peer, text);
      }
    }
  }
}

// --- pokoje i przekazywanie wiadomości ------------------------------------------------

function newCode() {
  if (TEST_CODE && !rooms.has(TEST_CODE)) return TEST_CODE;
  for (let i = 0; i < 200; i++) {
    const c = String(1000 + Math.floor(Math.random() * 9000));
    if (!rooms.has(c)) return c;
  }
  return String(1000 + Math.floor(Math.random() * 9000));
}

function onMessage(peer, text) {
  let msg;
  try { msg = JSON.parse(text); } catch { return; }
  if (!msg || typeof msg !== 'object') return;
  switch (msg.t) {
    case 'host': {
      leave(peer);
      const code = newCode();
      rooms.set(code, { host: peer, guest: null });
      peer.room = code;
      send(peer, { t: 'room', code, role: 0 });
      console.log(`[serwer] pokoj ${code} zalozony`);
      break;
    }
    case 'join': {
      const code = String(msg.code || '');
      const r = rooms.get(code);
      if (!r || r.guest) { send(peer, { t: 'error', text: `Nie ma wolnego pokoju o kodzie ${code}` }); return; }
      leave(peer);
      r.guest = peer;
      peer.room = code;
      send(peer, { t: 'room', code, role: 1 });
      send(r.host, { t: 'peer_joined' });
      send(peer, { t: 'peer_joined' });
      console.log(`[serwer] gracz dolaczyl do pokoju ${code}`);
      break;
    }
    case 'leave':
      leave(peer);
      break;
    case 'ping':
      send(peer, { t: 'pong' });
      break;
    default: {
      const r = peer.room && rooms.get(peer.room);
      if (!r) return;
      const other = r.host === peer ? r.guest : r.host;
      if (other) sendText(other, text);
    }
  }
}

function leave(peer) {
  const code = peer.room;
  if (!code) return;
  peer.room = null;
  const r = rooms.get(code);
  if (!r) return;
  rooms.delete(code);
  const other = r.host === peer ? r.guest : r.host;
  if (other) { other.room = null; send(other, { t: 'peer_left' }); }
  console.log(`[serwer] pokoj ${code} zamkniety`);
}

function drop(peer) {
  if (!clients.has(peer)) return;
  clients.delete(peer);
  leave(peer);
}

// podtrzymanie połączeń (darmowe hostingi zamykają ciche połączenia)
setInterval(() => {
  for (const p of clients) {
    if (!p.alive) { p.socket.destroy(); drop(p); continue; }
    p.alive = false;
    sendFrame(p, 0x9, Buffer.alloc(0));
  }
}, 25000);

server.listen(PORT, () => console.log(`[serwer] Bilard VR: nasluchuje na porcie ${PORT}`));
