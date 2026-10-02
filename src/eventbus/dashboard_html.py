"""HTML dashboard for the event bus monitor."""

from __future__ import annotations

dashboard_html = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Event Bus Monitor</title>
<style>
  :root {
    --bg: #0d1117;
    --surface: #161b22;
    --border: #30363d;
    --text: #e6edf3;
    --muted: #8b949e;
    --accent: #58a6ff;
    --green: #3fb950;
    --orange: #d29922;
    --font: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: var(--font);
    background: var(--bg);
    color: var(--text);
    height: 100vh;
    display: flex;
    flex-direction: column;
  }

  /* Header / Toolbar */
  .toolbar {
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    padding: 12px 20px;
    display: flex;
    align-items: center;
    gap: 16px;
    flex-wrap: wrap;
  }
  .toolbar h1 {
    font-size: 18px;
    font-weight: 600;
    white-space: nowrap;
  }
  .toolbar h1 span { color: var(--accent); }

  .status-dot {
    width: 10px; height: 10px;
    border-radius: 50%;
    background: var(--muted);
    flex-shrink: 0;
  }
  .status-dot.connected { background: var(--green); box-shadow: 0 0 6px var(--green); }

  .count-badge {
    font-size: 11px;
    color: var(--muted);
    padding: 4px 8px;
    background: var(--bg);
    border-radius: 10px;
    border: 1px solid var(--border);
  }

  /* Events list */
  .events-container {
    flex: 1;
    overflow-y: auto;
    padding: 0;
  }
  .events-container::-webkit-scrollbar { width: 8px; }
  .events-container::-webkit-scrollbar-track { background: transparent; }
  .events-container::-webkit-scrollbar-thumb {
    background: var(--border);
    border-radius: 4px;
  }

  .event-row {
    display: flex;
    align-items: flex-start;
    padding: 10px 20px;
    border-bottom: 1px solid var(--border);
    animation: fadeIn 0.2s ease;
    font-size: 13px;
  }
  .event-row:hover { background: rgba(88, 166, 255, 0.05); }

  @keyframes fadeIn {
    from { opacity: 0; transform: translateY(-4px); }
    to { opacity: 1; transform: translateY(0); }
  }

  .event-name {
    font-family: "SF Mono", "Fira Code", "Cascadia Code", monospace;
    font-weight: 600;
    color: var(--accent);
    min-width: 200px;
    flex-shrink: 0;
  }

  .event-source {
    font-size: 11px;
    color: var(--muted);
    min-width: 100px;
    max-width: 120px;
    flex-shrink: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .event-payload {
    flex: 1;
    font-family: "SF Mono", "Fira Code", "Cascadia Code", monospace;
    font-size: 12px;
    color: var(--muted);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 60%;
  }
  .event-payload .key { color: var(--orange); }
  .event-payload .val { color: var(--green); }

  .event-ts {
    font-size: 11px;
    color: var(--muted);
    min-width: 140px;
    flex-shrink: 0;
    text-align: left;
  }

  .empty-state {
    display: flex;
    align-items: center;
    justify-content: center;
    height: 100%;
    color: var(--muted);
    font-size: 14px;
    flex-direction: column;
    gap: 12px;
  }
  .empty-state .icon { font-size: 32px; }
</style>
</head>
<body>
<div class="toolbar">
  <h1>⚡ <span>EventBus</span> Monitor</h1>
  <div class="status-dot" id="statusDot" title="Connecting..."></div>
  <span id="statusText" style="font-size:12px;color:var(--muted)">Connecting...</span>
  <span class="count-badge" id="countBadge">0 events</span>
  <button class="btn danger" id="clearBtn" title="Clear memory only" style="margin-left:auto;">🗑 Clear (Memory Only)</button>
  <button class="btn danger" id="purgeBtn" title="Purge all events from database and memory" style="margin-right:10px;">💥 Purge All</button>
</div>
<div class="events-container" id="eventsContainer">
  <div class="empty-state">
    <div class="icon">📡</div>
    <div>Waiting for events...</div>
  </div>
</div>

<script>
(function() {
  const statusDot = document.getElementById('statusDot');
  const statusText = document.getElementById('statusText');
  const countBadge = document.getElementById('countBadge');
  const eventsContainer = document.getElementById('eventsContainer');
  const clearBtn = document.getElementById('clearBtn');
  const purgeBtn = document.getElementById('purgeBtn');

  let totalEvents = 0;
  let cursor = 0;
  let maxEvents = 50;
  let renderedIds = new Set();
  let isPolling = false;

  function poll() {
    if (isPolling) return;
    isPolling = true;
    
    fetch('/events?cursor=' + cursor + '&limit=50')
      .then(function(r) { return r.json(); })
      .then(function(data) {
        if (data.events && data.events.length > 0) {
          // Count only new unique events for the counter
          let newCount = 0;
          data.events.forEach(function(e) {
            if (!renderedIds.has(e.id)) {
              renderedIds.add(e.id);
              renderEvent(e);
              newCount++;
            }
          });
          totalEvents += newCount;
          updateCount();
          
          cursor = data.next_cursor || cursor;
          statusDot.className = 'status-dot connected';
          statusText.textContent = 'Connected — polling';
        } else if (data.done === true) {
          statusDot.className = 'status-dot connected';
          statusText.textContent = 'Connected — up to date';
        }
        
        isPolling = false;
        setTimeout(poll, 1000);
      })
      .catch(function() {
        statusDot.className = 'status-dot disconnected';
        statusText.textContent = 'Polling failed — retrying...';
        isPolling = false;
        setTimeout(poll, 3000);
      });
  }

  function updateCount() {
    countBadge.textContent = totalEvents + ' events';
  }

  function formatPayload(payload) {
    if (!payload || typeof payload !== 'object') return String(payload);
    return Object.entries(payload).map(function(kv) {
      return '<span class="key">' + escHtml(kv[0]) + '</span>:' +
             ' <span class="val">' + escHtml(String(kv[1])) + '</span>';
    }).join(', ');
  }

  function escHtml(str) {
    return str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }

  function formatTs(ts) {
    var d = new Date(ts * 1000);
    var date = d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
    var time = d.toLocaleTimeString();
    return date + ' ' + time;
  }

  function renderEvent(event) {
    // Remove empty state if present
    var empty = eventsContainer.querySelector('.empty-state');
    if (empty) empty.remove();

    var row = document.createElement('div');
    row.className = 'event-row';
    row.innerHTML =
      '<div class="event-name">' + escHtml(event.subject) + '</div>' +
      '<div class="event-source" title="Source: ' + escHtml(String(event.source || '')) + '">' + escHtml(String(event.source || '')) + '</div>' +
      '<div class="event-payload">' + formatPayload(event.payload) + '</div>' +
      '<div class="event-ts">' + formatTs(event.ts) + '</div>';

    eventsContainer.insertBefore(row, eventsContainer.firstChild);

    // Keep only last 50 events
    while (eventsContainer.children.length > maxEvents) {
      eventsContainer.removeChild(eventsContainer.lastChild);
    }
  }

  function clearEvents() {
    eventsContainer.innerHTML = '<div class="empty-state"><div class="icon">📡</div><div>Events cleared</div></div>';
    totalEvents = 0;
    cursor = 0;
    renderedIds.clear();
    updateCount();
  }

  async function purgeAll() {
    if (!confirm('This will delete ALL events from the database and clear memory. Continue?')) return;

    try {
      const resp = await fetch('/events', { method: 'DELETE' });
      const data = await resp.json();

      if (data.success) {
        clearEvents();
        statusText.textContent = 'Purged - ' + (data.message || '');
      } else {
        alert('Error: ' + (data.error || 'Unknown error'));
      }
    } catch (e) {
      alert('Failed to purge: ' + e.message);
    }
  }

  clearBtn.addEventListener('click', function() {
    clearEvents();
  });

  purgeBtn.addEventListener('click', function() {
    purgeAll();
  });

  poll();
})();
</script>
</body>
</html>
"""
