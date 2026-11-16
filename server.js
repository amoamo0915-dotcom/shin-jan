'use strict';
const http=require('http'),fs=require('fs'),path=require('path'),crypto=require('crypto');
const {Server}=require('socket.io');
const root=path.join(__dirname,'public');
const mime={'.html':'text/html; charset=utf-8','.js':'application/javascript; charset=utf-8','.css':'text/css; charset=utf-8'};
const server=http.createServer((req,res)=>{let pathname;try{pathname=decodeURIComponent(new URL(req.url,'http://localhost').pathname)}catch{return res.writeHead(400).end()}if(pathname==='/')pathname='/index.html';const file=path.resolve(root,'.'+pathname);if(!file.startsWith(root+path.sep))return res.writeHead(403).end();fs.readFile(file,(err,data)=>{if(err)return res.writeHead(404).end('Not found');res.writeHead(200,{'Content-Type':mime[path.extname(file)]||'application/octet-stream','Cache-Control':'no-store'}).end(data)})});
const io=new Server(server,{maxHttpBufferSize:20000,cors:{origin:false}});
const rooms=new Map(), queue=[];let nextRoom=1;
const safeName=v=>String(v||'プレイヤー').trim().slice(0,12)||'プレイヤー';
const makePlayer=(socket,name)=>({id:socket.id,name:safeName(name),cpu:false,score:0,hand:null});
const cpu=(n)=>({id:'cpu-'+crypto.randomUUID(),name:'CPU '+n,cpu:true,score:0,hand:null});
const publicRoom=r=>({id:r.id,mode:r.mode,label:r.label,host:r.host,status:r.status,players:r.players.map(p=>({id:p.id,name:p.name,cpu:p.cpu,score:p.score,submitted:p.hand!==null})),game:r.game,round:r.round,draws:r.draws,result:r.result,resultsAt:r.resultsAt});
const emit=r=>io.to(r.id).emit('state',publicRoom(r));
const fail=(s,msg)=>s.emit('errorMessage',msg);
function clearQueue(id){let i=queue.indexOf(id);if(i>=0)queue.splice(i,1)}
function drop(socket,notify=true){clearQueue(socket.id);let id=socket.data.room;if(!id)return;socket.leave(id);socket.data.room=null;let r=rooms.get(id);if(!r)return;let idx=r.players.findIndex(p=>p.id===socket.id);if(idx<0)return;if(r.status==='waiting'){r.players.splice(idx,1);if(!r.players.some(p=>!p.cpu)){rooms.delete(id);return}if(r.host===socket.id)r.host=r.players.find(p=>!p.cpu).id;if(notify)emit(r)}else if(r.status==='playing'){let old=r.players[idx];r.players[idx]={...old,id:'cpu-'+crypto.randomUUID(),name:old.name+' (CPU)',cpu:true,hand:old.hand};if(notify){emit(r);maybeResolve(r)}}}
function cpuHand(r,p){let chance=r.draws===2?.34:.19;if(p.score>=10)chance=.14;let n=Math.random();if(n<chance)return 3;n=(n-chance)/(1-chance);return n<.43?0:n<.73?1:2}
function start(r){if(r.status!=='waiting'||(r.mode==='open'&&r.players.length!==5)||(r.mode==='private'&&(r.players.length<5||r.players.length>8)))return false;r.status='playing';r.game=1;r.round=1;r.draws=0;r.result=null;r.resultsAt=null;r.players.forEach(p=>{p.score=0;p.hand=p.cpu?cpuHand(r,p):null});emit(r);maybeResolve(r);return true}
function maybeResolve(r) {
  if (r.status !== 'playing' || r.result || r.players.some(p => p.hand === null)) return;
  const hands = r.players.map(p => p.hand);
  const delta = Array(r.players.length).fill(0);
  const four = hands.filter(h => h === 3).length;
  let summary = '', finished = false;
  if (four > 0) {
    finished = true;
    if (four === 1) {
      const idx = hands.indexOf(3);
      delta.forEach((_,i) => { if(i !== idx) delta[i] = 1; });
      summary = r.players[idx].name + 'のフォー独占！ ほかの' + (r.players.length-1) + '人に1ポイント。';
    } else {
      hands.forEach((h,i) => { if(h === 3) delta[i] = four; });
      summary = 'フォーが' + four + '人被り！ フォー勢はそれぞれ' + four + 'ポイント。';
    }
  } else {
    const distinct = [...new Set(hands)];
    if (distinct.length === 1 || distinct.length === 3) {
      r.draws++;
      if (r.draws === 3) {
        delta.fill(10); finished = true;
        summary = '3連続あいこ！ 全員に10ポイント！';
      } else summary = 'あいこ！ ' + r.draws + '連続。';
    } else {
      const [a,b] = distinct;
      const loser = (a===0&&b===1)||(a===1&&b===2)||(a===2&&b===0) ? b : a;
      hands.forEach((h,i) => { if(h === loser) delta[i] = [0,2,5][h]; });
      summary = ['グー','チョキ','パー'][loser] + 'の負け！ 負けた人に' + [0,2,5][loser] + 'ポイント。';
      finished = true;
    }
  }
  r.players.forEach((p,i) => p.score += delta[i]);
  const ended = r.players.some(p => p.score >= 13);
  r.result = {hands,delta,summary,finished,ended};
  r.resultsAt = Date.now(); emit(r);
  setTimeout(() => {
    if(r.status !== 'playing' || !r.result) return;
    if(ended) {r.status='ended'; emit(r); return;}
    if(finished) {r.game++; r.round=1; r.draws=0;} else r.round++;
    r.result=null; r.resultsAt=null;
    r.players.forEach(p => p.hand=p.cpu?cpuHand(r,p):null);
    emit(r); maybeResolve(r);
  },5500);
}

function createRoom(socket,mode,label,pass,name){drop(socket);let id='room-'+nextRoom++;let r={id,mode,label,pass,host:socket.id,status:'waiting',players:[makePlayer(socket,name)],game:1,round:1,draws:0,result:null,resultsAt:null};rooms.set(id,r);socket.join(id);socket.data.room=id;emit(r);return r}
function matchQueue(){while(queue.length>=5){let ids=queue.splice(0,5),sockets=ids.map(id=>io.sockets.sockets.get(id)).filter(s=>s&&s.connected&&!s.data.room);if(sockets.length!==5){sockets.forEach(s=>queue.push(s.id));break}let r=createRoom(sockets[0],'open','オープンマッチ',null,sockets[0].data.name);for(let s of sockets.slice(1)){r.players.push(makePlayer(s,s.data.name));s.join(r.id);s.data.room=r.id}start(r)}}
io.on('connection',s=>{
 s.on('openMatch',data=>{if(s.data.room||queue.includes(s.id))return fail(s,'すでに参加しています');s.data.name=safeName(data?.name);queue.push(s.id);s.emit('queued',{count:queue.length});for(let id of queue){let other=io.sockets.sockets.get(id);other?.emit('queued',{count:queue.length})}matchQueue()});
 s.on('cancelQueue',()=>{clearQueue(s.id);s.emit('queueCancelled')});
 s.on('createPrivate',data=>{if(s.data.room||queue.includes(s.id))return fail(s,'先に現在の参加を終了してください');let label=String(data?.label||'').trim().slice(0,24),pass=String(data?.pass||'');if(!label||pass.length<4||pass.length>32)return fail(s,'ルーム名と4〜32文字の合言葉を入力してください');let exists=[...rooms.values()].some(r=>r.mode==='private'&&r.label===label&&r.status==='waiting');if(exists)return fail(s,'同名の待機中ルームがあります。別の名前にしてください');createRoom(s,'private',label,pass,safeName(data?.name))});
 s.on('joinPrivate',data=>{if(s.data.room||queue.includes(s.id))return fail(s,'先に現在の参加を終了してください');let r=[...rooms.values()].find(r=>r.mode==='private'&&r.label===String(data?.label||'').trim()&&r.status==='waiting');if(!r||r.pass!==String(data?.pass||''))return fail(s,'ルームが見つからないか、合言葉が違います');if(r.players.length>=8)return fail(s,'満員です（最大8人）');r.players.push(makePlayer(s,data?.name));s.join(r.id);s.data.room=r.id;emit(r)});
 s.on('setCpu',count=>{let r=rooms.get(s.data.room);if(!r||r.mode!=='private'||r.host!==s.id||r.status!=='waiting')return fail(s,'ホストだけがCPUを変更できます');let n=Number(count);if(!Number.isInteger(n)||n<0||n>7)return fail(s,'CPUは0〜7人にしてください');let humans=r.players.filter(p=>!p.cpu);if(humans.length+n>8)return fail(s,'合計8人を超えています');r.players=[...humans,...Array.from({length:n},(_,i)=>cpu(i+1))];emit(r)});
 s.on('startPrivate',()=>{let r=rooms.get(s.data.room);if(!r||r.mode!=='private'||r.host!==s.id)return fail(s,'ホストだけが開始できます');if(!start(r))fail(s,'CPUを含めて5〜8人になったら開始できます')});
 s.on('hand',hand=>{let r=rooms.get(s.data.room);if(!r||r.status!=='playing'||r.result)return;let p=r.players.find(p=>p.id===s.id);if(!p||p.hand!==null||!Number.isInteger(hand)||hand<0||hand>3)return;p.hand=hand;emit(r);maybeResolve(r)});
 s.on('leave',()=>{drop(s);s.emit('left')});s.on('disconnect',()=>drop(s));
});
const port=process.env.PORT||3000;server.listen(port,()=>console.log('しんたろうジャンケン http://localhost:'+port));
