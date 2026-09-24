// Talkie Cloud signaling server
// talkie.html의 signaling-server.js와 별도로 배포되는 독립 서버입니다.
// (Render + GitHub 별도 리포에 올려서 사용)
//
// 이 앱에는 "고정 채널" 개념이 없습니다. 클라이언트가 입력한 PIN(6자리) 또는
// 보안 비밀번호(10~20자) 자체가 곧 방(room) id가 되고, 그 방은 인원이
// 0명이 되는 순간 완전히 사라집니다(휘발성). 서버는 어떤 파일도 다루지
// 않으며 오직 WebRTC 시그널(offer/answer/ice)만 같은 방 구성원끼리 중계합니다.
//
// 프로토콜 (talkie 본가와 동일한 형태를 재사용):
//   C->S {type:'join', password:<string>}
//   S->C {type:'welcome', id}
//   C->S {type:'enter-channel', channel:<room id>}
//   C->S {type:'secret-auth', password:<string>}   // '3.비밀 연결' — TALKIE_CLOUD_SECRET 값과 일치해야 입장
//   S->C {type:'channel-welcome', channel, id, peers:[{id},...]}
//   S->C {type:'channel-full', channel}
//   S->C {type:'secret-error'}                      // 비밀번호 불일치
//   S->C {type:'peer-joined'|'peer-left', channel, id}
//   C->S {type:'leave-channel'}
//   C/S  {type:'signal', to|from, kind:'offer'|'answer'|'ice', payload}
//
// TALKIE_CLOUD_PASSWORD: 지정하면 최초 'join' 단계에서 이 값과 일치해야만
// 접속을 허용합니다(비워두면 누구나 접속 가능 — 실제 매칭은 방 코드가
// 담당하므로 기본값은 개방).
//
// TALKIE_CLOUD_SECRET: '3.비밀 연결' 화면에서 요구하는 공용 비밀번호입니다.
// Render 대시보드의 Environment 탭에 이 이름으로 값을 설정하세요(예: qwer4258).
// 설정하지 않으면 기본값 'qwer4258'이 사용됩니다. 이 비밀번호를 아는 사람은
// 누구나 같은 고정된 비밀 클라우드 방에 함께 입장하게 됩니다.

const http = require('http');
const { WebSocketServer } = require('ws');

const JOIN_PASSWORD = process.env.TALKIE_CLOUD_PASSWORD || null;
const SECRET_PASSWORD = process.env.TALKIE_CLOUD_SECRET || 'qwer4258';
const SECRET_ROOM = '__SECRET_ROOM__';
const PORT = process.env.PORT || 10001;
const ROOM_CAP = 12; // NICK_POOL 길이와 반드시 일치시킬 것

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Talkie Cloud signaling server OK');
});
const wss = new WebSocketServer({ server });

let nextId = 1;
const clients = new Map(); // id -> {ws, room: string|null}

function send(ws, obj) { try { ws.send(JSON.stringify(obj)); } catch (e) {} }
function roomMembers(room) {
  const ids = [];
  clients.forEach((c, id) => { if (c.room === room) ids.push(id); });
  return ids;
}
function broadcastToRoomExcept(room, exceptId, obj) {
  const msg = JSON.stringify(obj);
  clients.forEach((c, id) => { if (id !== exceptId && c.room === room) { try { c.ws.send(msg); } catch (e) {} } });
}
function leaveRoom(id) {
  const c = clients.get(id);
  if (!c || !c.room) return;
  const room = c.room;
  c.room = null;
  broadcastToRoomExcept(room, id, { type: 'peer-left', channel: room, id });
}
function enterRoom(ws, myId, me, room) {
  if (me.room === room) {
    const peers = roomMembers(room).filter((id) => id !== myId).map((id) => ({ id }));
    send(ws, { type: 'channel-welcome', channel: room, id: myId, peers });
    return;
  }
  const existing = roomMembers(room);
  if (existing.length >= ROOM_CAP) { send(ws, { type: 'channel-full', channel: room }); return; }
  if (me.room) leaveRoom(myId);
  me.room = room;
  const peers = existing.map((id) => ({ id }));
  send(ws, { type: 'channel-welcome', channel: room, id: myId, peers });
  broadcastToRoomExcept(room, myId, { type: 'peer-joined', channel: room, id: myId });
}

wss.on('connection', (ws) => {
  let authed = false, myId = null;

  ws.on('message', (raw) => {
    let data;
    try { data = JSON.parse(raw); } catch (e) { return; }

    if (!authed) {
      if (data.type !== 'join') return;
      if (JOIN_PASSWORD && data.password !== JOIN_PASSWORD) { send(ws, { type: 'auth-error' }); ws.close(); return; }
      authed = true;
      myId = String(nextId++);
      clients.set(myId, { ws, room: null });
      send(ws, { type: 'welcome', id: myId });
      return;
    }

    const me = clients.get(myId);
    if (!me) return;

    if (data.type === 'enter-channel' && typeof data.channel === 'string' && data.channel) {
      enterRoom(ws, myId, me, data.channel);
      return;
    }

    if (data.type === 'secret-auth') {
      if (typeof data.password !== 'string' || data.password !== SECRET_PASSWORD) {
        send(ws, { type: 'secret-error' });
        return;
      }
      enterRoom(ws, myId, me, SECRET_ROOM);
      return;
    }

    if (data.type === 'leave-channel') { leaveRoom(myId); return; }

    if (data.type === 'signal' && data.to) {
      const target = clients.get(data.to);
      if (target && me.room && target.room === me.room) {
        send(target.ws, { type: 'signal', from: myId, kind: data.kind, payload: data.payload });
      }
      return;
    }

    if (data.type === 'ping') { send(ws, { type: 'pong' }); return; }
  });

  ws.on('close', () => {
    if (myId && clients.has(myId)) { leaveRoom(myId); clients.delete(myId); }
  });
  ws.on('error', () => {});
});

server.listen(PORT, () => console.log('Talkie Cloud signaling server listening on', PORT));
