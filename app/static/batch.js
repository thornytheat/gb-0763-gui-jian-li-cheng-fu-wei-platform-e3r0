const statusLine = document.querySelector('#operation');
const channel = document.querySelector('#ingest-message')?.dataset.channel;

async function submit(path, payload) {
  const response = await fetch(path, {
    method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload),
  });
  const result = await response.json();
  if (!response.ok) throw new Error(result.message || '服务暂不可用');
  return result;
}

function handle(form, action) {
  form?.addEventListener('submit', async event => {
    event.preventDefault();
    const button = form.querySelector('button');
    button.disabled = true;
    if (statusLine) statusLine.textContent = '正在核验…';
    try { await action(); }
    catch (error) { if (statusLine) statusLine.textContent = error.message; }
    finally { button.disabled = false; }
  });
}

function short(value, length = 12) { return value.slice(0, length); }

function renderBoards(data) {
  const latest = data.latest;
  const trainBoard = document.querySelector('#train-board');
  if (trainBoard) {
    trainBoard.innerHTML = latest && latest.trains.length
      ? latest.trains.map(t => `<p><b>${t.run}</b> · 可用里程 <b>${t.usable_mm}</b> mm · 校准区间 ${t.span_count} 段 · 已复位缺陷 ${t.projected_count} 条 · ${t.chains.join('、') || '无有效区间'}</p>`).join('')
      : '<p>等待首批报文，采信后即可先行给出可用里程。</p>';
  }
  const conflictBoard = document.querySelector('#conflict-board');
  if (conflictBoard) {
    conflictBoard.innerHTML = latest && latest.conflicts.length
      ? latest.conflicts.map(r => `<details><summary>行 ${r.sources.join('、')} · ${r.label}</summary><p>来源报文：${r.message_refs.map(mid => `<a href="/api/batches/${data.channel}/messages/${mid}/raw"><code>${short(mid)}</code></a>`).join('、')}</p></details>`).join('')
      : '<p>暂无同编号异值冲突。</p>';
  }
  const messageBoard = document.querySelector('#message-board');
  if (messageBoard) {
    messageBoard.innerHTML = data.messages.length
      ? data.messages.map(item => `<p>#${item.seq} <a href="/api/batches/${data.channel}/messages/${item.message_id}/raw"><code>${short(item.message_id)}</code></a> · ${item.accepted} 采信 / ${item.isolated} 隔离 · <small>${item.received_at}</small></p>`).join('')
      : '<p>尚无报文入档。</p>';
  }
  const resetBoard = document.querySelector('#reset-board');
  if (resetBoard) {
    resetBoard.innerHTML = data.resets.length
      ? data.resets.map(item => `<p><a href="/work/resets/${item.reset_id}"><code>${short(item.reset_id)}</code></a> · 第 ${item.seq} 次复位 · 报文 ${item.message_count} 份 · 冲突 ${item.conflict_count} 组 · <small>${item.created_at}</small></p>`).join('')
      : '<p>尚无复位修订。</p>';
  }
  const label = document.querySelector('#batch-channel');
  if (label) label.textContent = `通道 ${data.channel}`;
}

async function refresh() {
  if (!channel || document.hidden) return;
  const response = await fetch(`/api/batches/${encodeURIComponent(channel)}`);
  if (response.ok) renderBoards(await response.json());
}

const ingest = document.querySelector('#ingest-message');
handle(ingest, async () => {
  const text = document.querySelector('#message-csv').value;
  const result = await submit(`/api/batches/${encodeURIComponent(channel)}/messages`,
    text ? {csv: text} : {sample: 'dirty'});
  document.querySelector('#message-csv').value = '';
  if (statusLine) statusLine.textContent = `第 ${result.seq} 次复位已生成，可用里程已更新`;
  await refresh();
});

const save = document.querySelector('#save-run');
handle(save, async () => {
  const text = document.querySelector('#csv').value;
  const result = await submit('/api/runs', text ? {csv: text} : {sample: save.dataset.sample});
  const address = new URL('/work/issue', location.origin);
  address.search = new URLSearchParams({run: result.run_id, sample: save.dataset.sample});
  location.assign(address);
});

if (channel) {
  setInterval(refresh, 5000);
  document.addEventListener('visibilitychange', refresh);
}
