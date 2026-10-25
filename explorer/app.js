'use strict';
(() => {
  const $ = (selector) => document.querySelector(selector);
  const icons = {
    grid: '<rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/>',
    box: '<path d="m12 3 9 5v8l-9 5-9-5V8zM3 8l9 5 9-5M12 13v8M7.5 5.5l9 5"/>',
    arrows: '<path d="M4 7h16m-5-5 5 5-5 5M20 17H4m5-5-5 5 5 5"/>',
    layers: '<path d="m12 3 10 5-10 5L2 8zM2 12l10 5 10-5M2 16l10 5 10-5"/>',
    network: '<circle cx="12" cy="5" r="3"/><circle cx="5" cy="18" r="3"/><circle cx="19" cy="18" r="3"/><path d="m10.5 7.5-4 8m7-8 4 8M8 18h8"/>',
    search: '<circle cx="10.5" cy="10.5" r="7.5"/><path d="m16 16 5 5"/>',
    refresh: '<path d="M20 7v5h-5M4 17v-5h5M6.1 6.1A8 8 0 0 1 20 12M4 12a8 8 0 0 0 13.9 5.9"/>',
    clock: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
    check: '<path d="m5 12 4 4L19 6"/>',
    copy: '<rect x="8" y="8" width="12" height="12" rx="2"/><path d="M16 8V4H4v12h4"/>',
    arrow: '<path d="M5 12h14m-5-5 5 5-5 5"/>',
  };
  const icon = (name) => `<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${icons[name] || icons.box}</svg>`;
  document.querySelectorAll('[data-icon]').forEach(el => { el.innerHTML = icon(el.dataset.icon); });
  const esc = (value) => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const num = (value) => Number(value ?? 0).toLocaleString('ko-KR');
  const short = (value, n = 10) => value && value.length > n * 2 ? `${value.slice(0,n)}…${value.slice(-n)}` : value || '—';
  const date = (time) => time ? new Date(time).toLocaleString('ko-KR', {hour12:false}) : '제네시스';
  const ago = (time) => {
    if (!time) return '제네시스';
    const seconds = Math.max(0, Math.floor((Date.now() - time) / 1000));
    if (seconds < 60) return '방금 전';
    if (seconds < 3600) return `${Math.floor(seconds / 60)}분 전`;
    if (seconds < 86400) return `${Math.floor(seconds / 3600)}시간 전`;
    return `${num(Math.floor(seconds / 86400))}일 전`;
  };
  const bytes = (value) => value < 1024 ? `${num(value)} B` : value < 1048576 ? `${(value/1024).toFixed(1)} KB` : `${(value/1048576).toFixed(2)} MB`;
  const path = (type, value) => `#/${type}/${encodeURIComponent(value)}`;
  const txLink = (id) => `<a class="mono hash-link" href="${path('tx',id)}" title="${esc(id)}">${esc(short(id))}</a>`;
  const nsLink = (ns) => `<a class="namespace-tag" href="${path('namespace',ns)}">${esc(ns)}</a>`;
  const copy = (value) => `<button class="copy-button" data-copy="${esc(value)}" aria-label="복사" title="복사">${icon('copy')}</button>`;
  const hash = (value) => `<span class="mono hash-value">${esc(value || '—')}</span>${value ? copy(value) : ''}`;
  const loading = '<div class="loading"><span class="spinner"></span>체인 데이터를 불러오고 있습니다.</div>';
  const empty = (label='아직 기록이 없습니다.') => `<div class="empty-state">${icon('layers')}<p>${esc(label)}</p></div>`;
  const apiBase = new URL('../', location.href).pathname;
  let requestVersion = 0, snapshot = null, activeDownload = null, lastPage = null;
  let pendingRequests = 0;
  const waiters = [];
  async function api(endpoint) {
    if (pendingRequests >= 2) await new Promise(resolve => waiters.push(resolve));
    pendingRequests++;
    try {
      for (let attempt = 0; attempt < 4; attempt++) {
        let response;
        try { response = await fetch(apiBase + endpoint, {signal:AbortSignal.timeout(15000), cache:'no-store'}); }
        catch { throw new Error('노드에 연결할 수 없습니다. 잠시 후 다시 시도해 주세요.'); }
        const data = await response.json();
        if (response.ok) return data;
        if (response.status === 503 && attempt < 3) { await new Promise(resolve => setTimeout(resolve, 400 * (attempt + 1))); continue; }
        const error = new Error(data.indexing ? '기록을 색인하고 있습니다. 잠시 후 새로고침해 주세요.' : response.status === 404 ? '일치하는 기록을 찾지 못했습니다.' : response.status === 400 ? '입력한 검색 값이나 페이지 주소를 확인해 주세요.' : '노드가 요청을 처리하지 못했습니다. 잠시 후 다시 시도해 주세요.');
        error.status = response.status; throw error;
      }
    } finally { pendingRequests--; const next = waiters.shift(); if (next) next(); }
  }
  function connection(ok) {
    $('#connection').innerHTML = `<i class="status-dot ${ok ? '' : 'offline'}"></i>${ok ? '노드 연결됨' : '연결 끊김'}`;
    $('#connection').classList.toggle('is-offline', !ok);
  }
  async function refreshStatus() {
    try {
      const [network, status] = await Promise.all([api('network/info'), api('status')]);
      snapshot = {...status, ...network}; connection(true);
      $('#updated').textContent = `${new Date().toLocaleTimeString('ko-KR', {hour12:false})} 업데이트 · 15초마다 갱신`;
      const sync = status.sync || {};
      $('#notice').innerHTML = sync.halted || sync.last_error ? '<div class="notice warning">노드 동기화에 문제가 있습니다. 표시된 기록이 최신 상태가 아닐 수 있습니다.</div>' : '';
      return snapshot;
    } catch (error) {
      connection(false);
      $('#notice').innerHTML = '<div class="notice warning">실시간 연결이 끊겼습니다. 다시 연결될 때까지 이전 데이터가 표시될 수 있습니다.</div>';
      throw error;
    }
  }
  function heading(title, subtitle, actions = '') {
    return `<div class="section-heading"><div><div class="eyebrow">EXPLORE TOMEI</div><h1>${esc(title)}</h1><p>${esc(subtitle)}</p></div><div class="actions">${actions}</div></div>`;
  }
  function panel(title, subtitle, body, link = '') {
    return `<section class="panel"><div class="panel-header"><div><h2>${title}</h2>${subtitle ? `<p>${subtitle}</p>` : ''}</div>${link}</div>${body}</section>`;
  }
  function txTable(items) {
    if (!items.length) return empty('확정된 트랜잭션이 없습니다.');
    return `<div class="table-wrap"><table><thead><tr><th>트랜잭션 ID</th><th>네임스페이스</th><th>블록</th><th>상태</th><th>기록 시각</th></tr></thead><tbody>${items.map(tx => `<tr><td>${txLink(tx.id)}</td><td>${nsLink(tx.namespace)}</td><td><a class="mono hash-link" href="${path('block',tx.height)}">#${num(tx.height)}</a></td><td><span class="status-badge">${icon('check')} 확정</span></td><td class="time-cell" title="${esc(date(tx.timestamp))}">${esc(ago(tx.timestamp))}</td></tr>`).join('')}</tbody></table></div>`;
  }
  function blockRows(items) {
    if (!items.length) return empty();
    return `<div class="block-list">${items.map(block => `<div class="block-row"><div class="block-symbol">${icon('box')}</div><div class="block-main"><a class="block-height mono" href="${path('block',block.height)}">#${num(block.height)}</a><div class="muted" title="${esc(block.hash)}">${esc(short(block.hash,8))}</div></div><div class="block-meta"><span>${num(block.tx_count)} <span class="muted">Txns</span></span><small title="${esc(date(block.timestamp))}">${ago(block.timestamp)}</small></div><a class="row-arrow" href="${path('block',block.height)}" aria-label="블록 ${block.height} 자세히">${icon('arrow')}</a></div>`).join('')}</div>`;
  }
  function stats(data) {
    const cards = [
      ['box','최신 블록 높이',`#${num(data.height)}`,`${num(data.height + 1)}개 블록 · 제네시스 포함`],
      ['arrows','누적 트랜잭션',num(data.total_txs),'체인에 확정된 공증 기록'],
      ['clock','대기 트랜잭션',num(data.mempool_pending),'블록 포함 대기 중'],
      ['network','블록 생산 방식','Single authority',data.role === 'authority' ? '현재 노드 · Authority' : '현재 노드 · Follower'],
    ];
    return `<section class="stats-grid" aria-label="체인 통계">${cards.map(([i,label,value,note]) => `<div class="stat-card"><div class="stat-label">${label}<span class="stat-icon">${icon(i)}</span></div><div class="stat-value ${i==='network' ? 'text-value' : ''}">${value}</div><div class="stat-note">${note}</div></div>`).join('')}</section>`;
  }
  async function home() {
    const [blocks, txs] = await Promise.all([api('explorer/api/blocks?limit=6'), api('explorer/api/transactions?limit=6')]);
    const ordered = [...blocks.items].reverse();
    const max = Math.max(1, ...ordered.map(b=>b.tx_count));
    const activity = `<div class="activity-summary"><span class="muted">최근 ${ordered.length}개 블록의 트랜잭션</span><strong>${num(ordered.reduce((sum,b)=>sum+b.tx_count,0))}<small> Txns</small></strong></div><div class="activity-bars">${ordered.map(b=>`<a class="activity-column" href="${path('block',b.height)}" title="블록 #${b.height}: ${b.tx_count}개 트랜잭션"><span class="activity-bar" data-bar-height="${b.tx_count/max*100}"></span><span class="mono">#${b.height}</span></a>`).join('')}</div><div class="network-note">${icon('clock')} 새로운 공증이 제출되면 블록이 생성됩니다.</div>`;
    return `${stats(snapshot)}<div class="dashboard-grid">${panel('최근 블록','차곡차곡 쌓이는 체인의 기록',blockRows(blocks.items),'<a class="text-link" href="#/blocks">전체 보기 →</a>')}${panel('온체인 활동','최근 블록별 공증 기록',activity,'<span class="live-label"><i class="status-dot"></i>LIVE</span>')}</div>${panel('최근 트랜잭션','체인에 새겨진 최신 데이터',txTable(txs.items),'<a class="text-link" href="#/transactions">전체 보기 →</a>')}<div class="summary-strip"><span>${icon('layers')} 앱의 기록을 한곳에서</span><p>네임스페이스별로 공증 이력을 모아 확인하세요.</p><a href="#/namespaces" class="text-link">네임스페이스 탐색 →</a></div>`;
  }
  function pagination(route, data, cursor) {
    const previous = cursor ? `<a class="button secondary small" href="#/${route}">최신으로</a>` : '';
    const next = data.next_cursor !== null ? `<a class="button secondary small" href="#/${route}?cursor=${encodeURIComponent(data.next_cursor)}">이전 기록 →</a>` : '<span class="muted">마지막 기록입니다</span>';
    return `<div class="pagination"><span class="muted">총 ${num(data.total)}개 · 이번 페이지 ${num(data.items.length)}개</span><div class="actions">${previous}${next}</div></div>`;
  }
  async function blocksPage(params) {
    const cursor = params.get('cursor');
    const data = await api(`explorer/api/blocks?limit=20${cursor ? `&before=${encodeURIComponent(cursor)}` : ''}`);
    return heading('블록','체인에 연결된 블록과 포함된 공증 기록을 살펴보세요.') + panel('전체 블록',`제네시스를 포함한 ${num(data.total)}개 블록`,data.items.length ? `<div class="table-wrap"><table><thead><tr><th>블록 높이</th><th>블록 해시</th><th>트랜잭션</th><th>데이터 크기</th><th>기록 시각</th></tr></thead><tbody>${data.items.map(b=>`<tr><td><a class="mono hash-link" href="${path('block',b.height)}">#${num(b.height)}</a>${b.height === 0 ? ' <span class="namespace-tag">Genesis</span>' : ''}</td><td><a class="mono hash-link" href="${path('block',b.height)}" title="${esc(b.hash)}">${esc(short(b.hash))}</a></td><td>${num(b.tx_count)}</td><td class="mono muted">${bytes(b.payload_bytes)}</td><td class="time-cell" title="${esc(date(b.timestamp))}">${ago(b.timestamp)}</td></tr>`).join('')}</tbody></table></div>` : empty()) + pagination('blocks',data,cursor);
  }
  async function transactionsPage(params, namespace) {
    const cursor = params.get('cursor');
    const data = await api(`explorer/api/transactions?limit=20${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}${namespace !== undefined ? `&namespace=${encodeURIComponent(namespace)}` : ''}`);
    const route = namespace === undefined ? 'transactions' : `namespace/${encodeURIComponent(namespace)}`;
    let intro = heading(namespace === undefined ? '트랜잭션' : namespace, namespace === undefined ? '확정된 트랜잭션과 공증 데이터를 확인하세요.' : '이 네임스페이스의 공증 이력입니다.');
    if (namespace !== undefined) {
      const info = await api(`ns/${encodeURIComponent(namespace)}`);
      intro = '<a class="back-link" href="#/namespaces">← 네임스페이스 목록</a>' + intro + `<div class="summary-strip"><span>누적 공증 <strong>${num(info.count)}</strong></span><p>최근 블록 <a class="hash-link mono" href="${path('block',info.last_height)}">#${num(info.last_height)}</a></p><span>최근 트랜잭션 ${txLink(info.last_tx_id)}</span></div>`;
    }
    return intro + panel('공증 기록', `${num(data.total)}개 트랜잭션`,txTable(data.items)) + pagination(route,data,cursor);
  }
  async function namespacesPage(params) {
    const cursor = params.get('cursor');
    const data = await api(`explorer/api/namespaces?limit=20${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`);
    return heading('네임스페이스','애플리케이션별로 모아 보는 공증 이력.') + `<div class="namespace-grid">${data.items.map(ns=>`<a class="namespace-card" href="${path('namespace',ns.namespace)}"><div class="namespace-top"><span class="block-symbol">${icon('layers')}</span>${icon('arrow')}</div><h2>${esc(ns.namespace)}</h2><p>확정된 공증 기록</p><div class="namespace-count">${num(ns.count)}<small> 트랜잭션</small></div><div class="namespace-bottom"><span>최근 블록</span><strong class="mono">#${num(ns.last_height)}</strong></div></a>`).join('') || empty('아직 네임스페이스가 없습니다.')}</div>` + pagination('namespaces',data,cursor);
  }
  const detail = (label,value) => `<div class="detail-row"><dt>${label}</dt><dd>${value}</dd></div>`;
  const jsonPanel = (title,value) => panel(title,'',`<div class="code-panel"><pre>${esc(JSON.stringify(value,null,2))}</pre></div>`);
  function downloadAction() { return '<button class="button secondary small" data-download>JSON 내려받기 ↓</button>'; }
  async function blockPage(height, version) {
    if (!/^\d+$/.test(height)) throw new Error('블록 높이는 0 이상의 정수여야 합니다.');
    const data = await api(`network/block/${height}`);
    const b = data.block, cp = data.checkpoint, h = b.header;
    if (version === requestVersion) activeDownload = {name:`tomei-block-${h.index}.json`,data};
    const txs = b.transactions.map(tx=>({...tx,height:h.index}));
    const navigation = `<div class="detail-nav">${h.index > 0 ? `<a class="button secondary small" href="${path('block',h.index-1)}">← 이전 블록</a>` : ''}${h.index < snapshot.height ? `<a class="button secondary small" href="${path('block',h.index+1)}">다음 블록 →</a>` : ''}</div>`;
    return '<a class="back-link" href="#/blocks">← 블록 목록</a>' + heading(`블록 #${num(h.index)}`,h.index === 0 ? 'tomei 체인의 시작, 제네시스 블록입니다.' : '확정된 블록의 데이터와 authority 체크포인트입니다.',downloadAction()) + panel('블록 정보', '', `<dl class="details-grid">${detail('블록 높이',`<strong class="mono">${num(h.index)}</strong>`)}${detail('블록 해시',hash(b.hash))}${detail('이전 블록 해시',h.index ? `<a class="mono hash-link hash-value" href="${path('block',h.index-1)}">${esc(h.prev_block_hash)}</a>${copy(h.prev_block_hash)}` : '제네시스 · 이전 블록 없음')}${detail('머클 루트',hash(h.merkle_root))}${detail('기록 시각',esc(date(h.timestamp)))}${detail('트랜잭션',`${num(b.transactions.length)}개`)}</dl>`) + navigation + panel('포함된 트랜잭션',`${num(b.transactions.length)}개 공증 기록`,txTable(txs)) + panel('Authority 체크포인트','노드가 제공하는 서명 정보입니다. 이 화면에서는 암호학적 서명 검증을 수행하지 않습니다.',`<dl class="details-grid">${detail('공개키',hash(cp.authority_public_key))}${detail('블록 다이제스트',hash(cp.block_digest))}${detail('Ed25519 서명',hash(cp.signature))}</dl>`);
  }
  async function txPage(id, version) {
    const data = await api(`tx/${encodeURIComponent(id)}`);
    if (version === requestVersion) activeDownload = {name:`tomei-tx-${id}.json`,data};
    if (data.status === 'pending') return heading('트랜잭션','블록에 포함되기를 기다리고 있습니다.') + panel('대기 중','',`<dl class="details-grid">${detail('트랜잭션 ID',hash(id))}${detail('상태','멤풀 대기 중')}</dl>`);
    const tx = data.tx;
    let payload; try { payload = JSON.stringify(JSON.parse(tx.payload),null,2); } catch { payload = tx.payload; }
    return '<a class="back-link" href="#/transactions">← 트랜잭션 목록</a>' + heading('트랜잭션 상세','체인에 확정된 공증 데이터입니다.',downloadAction()) + panel('트랜잭션 정보','',`<dl class="details-grid">${detail('트랜잭션 ID',hash(tx.id))}${detail('상태','<span class="status-badge">'+icon('check')+' 확정</span>')}${detail('포함 블록',`<a class="mono hash-link" href="${path('block',data.height)}">#${num(data.height)}</a>`)}${detail('네임스페이스',nsLink(tx.namespace))}${detail('제출 시각',esc(date(tx.timestamp)))}${detail('데이터 크기',bytes(new TextEncoder().encode(tx.payload).length))}${detail('애플리케이션 서명',tx.signature ? hash(tx.signature)+'<p class="muted">앱이 제출한 값이며, 코어에서 제출자 서명을 검증하지 않습니다.</p>' : '<span class="muted">제공되지 않음</span>')}</dl>`) + panel('공증 데이터 (Payload)','저장된 문자열을 표시합니다. JSON 데이터는 읽기 쉽게 정렬합니다.',`<div class="payload-toolbar"><button class="button secondary small" data-copy="${esc(tx.payload)}">원문 복사 ${icon('copy')}</button></div><div class="code-panel"><pre>${esc(payload)}</pre></div>`) + panel('태그',`${tx.tags.length}개`,tx.tags.length ? `<div class="table-wrap"><table><thead><tr><th>키</th><th>값</th></tr></thead><tbody>${tx.tags.map(([k,v])=>`<tr><td class="mono">${esc(k)}</td><td class="mono wrap">${esc(v)}</td></tr>`).join('')}</tbody></table></div>` : empty('이 트랜잭션에는 태그가 없습니다.'));
  }
  function networkPage() {
    const s = snapshot, sync = s.sync || {};
    activeDownload = {name:'tomei-network.json',data:s};
    return heading('네트워크','노드의 현재 상태와 체인 식별 정보를 확인하세요.',downloadAction()) + stats(s) + `<div class="network-grid">${panel('체인 식별 정보','공개 실험 네트워크',`<dl class="details-grid">${detail('체인 ID',hash(s.chain_id))}${detail('제네시스 해시',hash(s.genesis_hash))}${detail('Authority 공개키',hash(s.authority_public_key))}${detail('최신 블록 해시',`<a class="mono hash-link hash-value" href="${path('block',s.height)}">${esc(s.tip_hash)}</a>${copy(s.tip_hash)}`)}${detail('프로토콜 버전',`v${num(s.protocol_version)}`)}</dl>`)}${panel('노드 상태','',`<dl class="details-grid">${detail('노드 역할',esc(s.role))}${detail('블록 생산', '단일 authority')}${detail('복제 방식','서명 검증 후 순차 복제')}${detail('동기화',s.role === 'authority' ? 'Authority · 로컬 블록 생산' : sync.halted ? '중단됨' : sync.last_error ? '재시도 중' : sync.remote_height === s.height ? '최신 상태' : '동기화 중')}${detail('연결된 피어',esc(sync.active_peer || '—'))}${detail('최근 동기화',sync.last_success_ms ? esc(date(sync.last_success_ms)) : '—')}${detail('조회 API',`<a class="hash-link wrap" href="${esc(apiBase)}network/info" target="_blank" rel="noopener noreferrer">${esc(location.origin + apiBase)}network/info ↗</a>`)}</dl>`)}</div><div class="notice"><strong>tomei는 데이터 공증을 위한 실험용 네트워크입니다.</strong><p>블록 생산은 하나의 authority가 담당하며, follower가 서명과 블록 연결을 검증하고 보관합니다. 다중 검증자 합의나 보상 토큰을 제공하지 않습니다.</p></div>`;
  }
  async function render(background = false) {
    const version = ++requestVersion;
    const route = (location.hash || '#/').slice(1);
    const [pathname, query] = route.split('?');
    let segments;
    try { segments = pathname.split('/').filter(Boolean).map(decodeURIComponent); } catch { segments = ['invalid']; }
    const params = new URLSearchParams(query || '');
    const type = segments[0] || 'home';
    const labels = {home:'대시보드',blocks:'블록',block:'블록 상세',transactions:'트랜잭션',tx:'트랜잭션 상세',namespaces:'네임스페이스',namespace:'네임스페이스 상세',network:'네트워크'};
    const nav = {block:'blocks',tx:'transactions',namespace:'namespaces'}[type] || type;
    $('#breadcrumb').textContent = labels[type] || '페이지';
    document.title = `${labels[type] || '탐색'} · tomei Explorer`;
    document.querySelectorAll('[data-nav]').forEach(el=>{const active = el.dataset.nav === nav; el.classList.toggle('active',active); if(active) el.setAttribute('aria-current','page'); else el.removeAttribute('aria-current');});
    $('.hero').classList.toggle('compact',type !== 'home');
    if (!background) { $('#view').innerHTML = loading; activeDownload = null; }
    $('#refresh').disabled = true;
    try {
      await refreshStatus();
      if (version !== requestVersion) return;
      let html;
      if (type === 'home') html = await home();
      else if (type === 'blocks') html = await blocksPage(params);
      else if (type === 'transactions') html = await transactionsPage(params);
      else if (type === 'namespaces') html = await namespacesPage(params);
      else if (type === 'namespace' && segments[1]) html = await transactionsPage(params,segments[1]);
      else if (type === 'block' && segments[1] !== undefined) html = await blockPage(segments[1], version);
      else if (type === 'tx' && segments[1]) html = await txPage(segments[1], version);
      else if (type === 'network') html = networkPage();
      else throw new Error('요청한 페이지를 찾을 수 없습니다.');
      if (version !== requestVersion) return;
      $('#view').innerHTML = html;
      document.querySelectorAll('[data-bar-height]').forEach(el => el.style.setProperty('--bar-height', `${Number(el.dataset.barHeight)}%`));
      lastPage = location.hash;
    } catch (error) {
      if (version !== requestVersion) return;
      if (!background || lastPage !== location.hash) $('#view').innerHTML = `<div class="error-state">${icon('search')}<h2>${esc(error.message)}</h2><p>블록 높이, 64자리 해시 또는 정확한 네임스페이스를 확인해 주세요.</p><div class="actions"><button class="button" data-retry>다시 시도</button><a class="button secondary" href="#/">대시보드</a></div></div>`;
    } finally { if (version === requestVersion) $('#refresh').disabled = false; }
  }
  let searching = false;
  $('#search-form').addEventListener('submit',async event=>{
    event.preventDefault();
    const q = $('#search-input').value.trim();
    if (!q || searching) return;
    searching = true; const button = $('#search-form button'); button.disabled = true;
    $('#search-message').textContent = '기록을 찾고 있습니다…';
    try {
      const result = await api(`explorer/api/search?q=${encodeURIComponent(q)}`);
      const target = result.type === 'block' ? path('block',result.height) : result.type === 'transaction' ? path('tx',result.id) : path('namespace',result.namespace);
      $('#search-message').textContent = ''; if (location.hash === target) render(); else location.hash = target;
    } catch(error) { $('#search-message').textContent = error.message; }
    finally { searching=false; button.disabled=false; }
  });
  let toastTimer;
  function toast(text) { $('#toast').textContent=text; $('#toast').hidden=false; clearTimeout(toastTimer); toastTimer=setTimeout(()=>{$('#toast').hidden=true;},2500); }
  document.addEventListener('click',async event=>{
    if (event.target.closest('.skip-link')) { event.preventDefault(); $('#main').focus(); $('#main').scrollIntoView(); return; }
    const c = event.target.closest('[data-copy]');
    if (c) { try { await navigator.clipboard.writeText(c.dataset.copy); toast('클립보드에 복사했습니다.'); } catch { toast('복사하지 못했습니다. 텍스트를 선택해 복사해 주세요.'); } }
    if (event.target.closest('[data-retry]')) render();
    if (event.target.closest('[data-download]') && activeDownload) {
      const blob = new Blob([JSON.stringify(activeDownload.data,null,2)], {type:'application/json'});
      const url = URL.createObjectURL(blob), link=document.createElement('a'); link.href=url; link.download=activeDownload.name; link.click(); setTimeout(()=>URL.revokeObjectURL(url),1000);
    }
  });
  document.addEventListener('keydown',event=>{if(event.key==='/' && !/INPUT|TEXTAREA|SELECT/.test(event.target.tagName) && !event.ctrlKey && !event.metaKey){event.preventDefault();$('#search-input').focus();}});
  $('#refresh').addEventListener('click',()=>render());
  window.addEventListener('hashchange',()=>{render(); window.scrollTo({top:0,behavior:'instant'});});
  setInterval(()=>{
    if (document.hidden || searching || $('#refresh').disabled) return;
    if (['','#/','#/network'].includes(location.hash)) render(true);
    else refreshStatus().catch(() => {});
  },15000);
  render();
})();
