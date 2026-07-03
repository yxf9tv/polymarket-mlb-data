// ── State ────────────────────────────────────────────────────────────
const state = {
  liveStatus: null, liveSummary: null, liveConsensus: [], liveGames: [],
  liveTypes: [],
  historical: null, traders: [],
  pollTimer: null,
};

const TEAM_MAP = {
  nyy:"NYY", bos:"BOS", lad:"LAD", sd:"SD", hou:"HOU", det:"DET",
  tex:"TEX", tor:"TOR", chc:"CHC", mil:"MIL", atl:"ATL", sf:"SF",
  nym:"NYM", phi:"PHI", ari:"ARI", tb:"TB", oak:"OAK", laa:"LAA",
  sea:"SEA", col:"COL", min:"MIN", kc:"KC", cws:"CWS", cle:"CLE",
  cin:"CIN", pit:"PIT", stl:"STL", mia:"MIA", wsh:"WSH", bal:"BAL",
};

function fmtSlug(slug) {
  if (!slug) return "-";
  let p = slug.split("-");
  if (p[0]==="mlb" && p.length>=5) {
    let t1=TEAM_MAP[p[1]]||p[1].toUpperCase(), t2=TEAM_MAP[p[2]]||p[2].toUpperCase();
    let label=`${t1} @ ${t2}`;
    let rest=p.slice(4).join("-");
    if (rest.includes("total")) { let parts=rest.split("-"); let idx=parts.indexOf("total"); let line=parts[idx+1]||""; return `${label} O/U ${line}`; }
    if (rest.includes("spread")) { let parts=rest.split("-"); let idx=parts.indexOf("spread"); let side=parts[idx+1]||""; let line=parts[idx+2]||""; let team=side==="home"?t2:t1; return `${label} ${team} ${line}`; }
    if (rest.includes("nrfi")) return `${label} NRFI`;
    return `${label} ML`;
  }
  if (slug.startsWith("will-")) return slug.replace(/-/g," ").replace(/\s+/g," ").replace(/^will /i,"").replace(/\b\w/g,c=>c.toUpperCase());
  return slug;
}

function fmtWallet(w) { return w ? `${w.slice(0,6)}...${w.slice(-4)}` : "-"; }
function fmtDollar(n) { return n==null||isNaN(n)?"-":"$"+(n>=0?n.toLocaleString("en-US",{maximumFractionDigits:0}):"-"+Math.abs(n).toLocaleString("en-US",{maximumFractionDigits:0})); }
function fmtPct(n) { return n==null||isNaN(n)?"-":(n*100).toFixed(0)+"%"; }
function fmtNum(n) { return n==null||isNaN(n)?"-":n.toLocaleString(); }

function convColor(c) { return c>=0.8?"var(--green)":c>=0.5?"var(--yellow)":"var(--orange)"; }
function convBar(c) { return `<div class="conv-bar-wrap"><div class="conv-bar" style="width:${(c*100).toFixed(0)}%;background:${convColor(c)}"></div></div>`; }

function fmtDepth(n) {
  if (n==null||isNaN(n)) return "-";
  if (n>=1000000) return "$"+(n/1000000).toFixed(1)+"M";
  if (n>=1000) return "$"+(n/1000).toFixed(1)+"K";
  return "$"+n.toFixed(0);
}

function renderDepthBars(ob) {
  if (!ob || !ob.outcomes) return "";
  let outcomes = ob.outcomes;
  let entries = Object.entries(outcomes).map(([name, d]) => ({
    name, total: (d.bid_volume||0) + (d.ask_volume||0)
  }));
  let maxTotal = Math.max(...entries.map(e=>e.total), 1);
  let bars = entries.map(e => {
    let pct = (e.total / maxTotal * 100).toFixed(0);
    let color = e.total > 0 ? "var(--green)" : "var(--text2)";
    return `<div class="ob-row"><span class="ob-label">${e.name}</span><div class="ob-track"><div class="ob-fill" style="width:${pct}%;background:${color}"></div></div><span class="ob-val">${fmtDepth(e.total)}</span></div>`;
  }).join("");
  let scoreHtml = ob.depth_imbalance != null
    ? `<div class="ob-score">Smart $: ${(ob.depth_imbalance*100).toFixed(0)}%</div>` : "";
  return `<div class="ob-section">${bars}${scoreHtml}</div>`;
}

function marketTypeTag(t) {
  const m={"moneyline":"ml","total":"total","spread":"spread","futures":"futures","nrfi":"nrfi"};
  return `<span class="tag tag-${m[t]||"other"}">${t||"?"}</span>`;
}

function fmtDate(d) {
  if (!d) return "";
  let parts=d.split("-");
  if (parts.length<3) return d;
  return `${parts[1]}/${parts[2]}`;
}

function fmtDateFull(d) {
  if (!d) return "";
  let parts=d.split("-");
  if (parts.length<3) return d;
  const months=["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
  return `${months[parseInt(parts[1])-1]||parts[1]} ${parseInt(parts[2])}, ${parts[0]}`;
}

async function api(path) {
  try { let r=await fetch(path); if (!r.ok) throw new Error(r.statusText); return await r.json(); }
  catch(e) { console.error("API error:",path,e); return null; }
}

function switchTab(name) {
  document.querySelectorAll(".tab").forEach(t=>t.classList.toggle("active",t.dataset.tab===name));
  document.querySelectorAll(".tab-content").forEach(c=>c.classList.toggle("active",c.id==="tab-"+name));
  if (name==="live") renderLive();
  if (name==="overview") { renderSignals(); renderOverview(); }
  if (name==="traders") renderTraders();
  if (name==="tracking") renderTracking();
  if (name==="backtest") renderBacktest();
}

function switchOTab(name) {
  document.querySelectorAll(".sub-tab").forEach(t=>t.classList.toggle("active",t.dataset.otab===name));
  document.querySelectorAll(".otab-content").forEach(c=>c.classList.toggle("active",c.id==="otab-"+name));
  if (name==="signals") renderSignals();
  if (name==="games") renderGames();
}

// ── Init ─────────────────────────────────────────────────────────────
async function init() {
  let [status, types, traders] = await Promise.all([
    api("/api/live"), api("/api/live/market-types"), api("/api/traders?limit=200"),
  ]);
  state.liveStatus = status; state.liveTypes = types||[]; state.traders = traders||[];
  updateLiveHeader();
  if (status && status.status==="ok") {
    let [summary, consensus, games] = await Promise.all([
      api("/api/live/summary"), api("/api/live/consensus?limit=500&min_traders=1"),
      api("/api/live/games"),
    ]);
    state.liveSummary = summary; state.liveConsensus = consensus||[]; state.liveGames = games||[];
  }
  renderLiveTypeFilter();
  renderLive();
  renderOverview();
  renderTraders();
  renderRecommendations();
}

function renderRecommendations() {
  api("/api/recommendations").then(data=>{
    if (!data || data.error) {
      document.getElementById("recPanel").innerHTML="";
      return;
    }
    let bets=data.bets||[], fades=data.fades||[];
    if (!bets.length && !fades.length) {
      document.getElementById("recPanel").innerHTML = data.notice
        ? `<div class="rec-panel"><div class="rec-header"><strong>\uD83D\uDCCB Betting Slip</strong></div>
           <div style="color:var(--text2);font-size:13px;line-height:1.5">${data.notice}</div></div>`
        : "";
      return;
    }
    let html=`<div class="rec-panel"><div class="rec-header"><strong>\uD83D\uDCCB Betting Slip</strong> <span class="date-count">${data.generated_at.slice(11,19)} UTC</span></div>`;
    if (bets.length) {
      html+=`<div class="rec-section"><div class="rec-section-title">\u2705 BET (${bets.length})</div>`;
      for (let r of bets) {
        html+=`<div class="rec-row">
          <span class="rec-market">${fmtSlug(r.slug)}</span>
          <span class="rec-outcome">${r.predicted_outcome}</span>
          <span class="rec-edge">\uD83D\uDFE2 +${r.expected_roi}%</span>
        </div>`;
      }
      html+=`</div>`;
    }
    if (fades.length) {
      html+=`<div class="rec-section"><div class="rec-section-title">\uD83D\uDD04 FADE (${fades.length})</div>`;
      for (let r of fades) {
        html+=`<div class="rec-row">
          <span class="rec-market">${fmtSlug(r.slug)}</span>
          <span class="rec-outcome">\u2192 ${r.fade_outcome||"opposite"}</span>
          <span class="rec-edge">\uD83D\uDFE1 +${r.expected_roi}%</span>
        </div>`;
      }
      html+=`</div>`;
    }
    html+=`</div>`;
    document.getElementById("recPanel").innerHTML=html;
  });
}

function updateLiveHeader() {
  let s=state.liveStatus;
  let el=document.getElementById("liveIndicator"), lu=document.getElementById("lastUpdated"), dr=document.getElementById("dataRange");
  if (!s || s.status!=="ok") {
    el.textContent="\u26A0 No live data";
    el.style.color="var(--red)";
    lu.textContent="Run 'Poll Now' or python3 scripts/poll_live.py";
    dr.textContent="";
    return;
  }
  let age=s.age_seconds;
  let mins=Math.floor(age/60);
  el.textContent=mins<5?"\u25CF Live":mins<60?`\u25CF ${mins}m ago`:mins<1440?`\u25CF ${Math.floor(mins/60)}h ago`:"\u25CB Stale";
  el.style.color=mins<60?"var(--green)":mins<1440?"var(--yellow)":"var(--red)";
  lu.textContent=`Polled ${s.generated_at?s.generated_at.slice(11,19):"?"} UTC`;
  let sm=state.liveSummary;
  dr.textContent=sm?`${sm.data_start_date||"?"} \u2192 ${sm.data_end_date||"?"}  \u00B7 ${sm.active_markets_with_data||0} with data \u00B7 ${sm.total_open_markets||0} open \u00B7 ${sm.active_games||0} active games`:"";
}

// ── Live Tab ─────────────────────────────────────────────────────────
function renderLive() {
  let dateFilter=document.getElementById("liveDate").value;
  let type=document.getElementById("liveType").value;
  let minT=parseInt(document.getElementById("liveMinT").value)||1;
  let search=document.getElementById("liveSearch").value.toLowerCase();

  function localDate(d){let y=d.getFullYear(),m=d.getMonth()+1,dd=d.getDate();return y+'-'+(m<10?'0':'')+m+'-'+(dd<10?'0':'')+dd;}
  // Use server-provided NY today date, not browser local time
  let todayStr=state.liveStatus&&state.liveStatus.today ? state.liveStatus.today : localDate(new Date());
  let maxDate="";
  if (dateFilter==="today") { maxDate=todayStr; }
  else if (dateFilter==="tomorrow") { let d=new Date(); d.setDate(d.getDate()+1); maxDate=localDate(d); }
  else if (dateFilter==="3") { let d=new Date(); d.setDate(d.getDate()+3); maxDate=localDate(d); }
  else if (dateFilter==="7") { let d=new Date(); d.setDate(d.getDate()+7); maxDate=localDate(d); }

  // Build lookup: consensus markets keyed by condition_id
  let cmap={};
  for (let m of state.liveConsensus) cmap[m.condition_id||m.slug]=m;

  // Filter games by date
  let filteredGames = state.liveGames.filter(g=>{
    let gd=g.game_date||"";
    if (dateFilter) {
      if (!gd||gd<todayStr) return false;
      if (maxDate&&gd>maxDate) return false;
    }
    return true;
  });

  // Sort games by date ascending (soonest first)
  filteredGames.sort((a,b)=>(a.game_date||"").localeCompare(b.game_date||""));

  let sigCount=0, gameDays=new Set(), traderSet=new Set();

  // Collect all markets with data into a flat summary list
  let allMarkets = [];
  for (let g of filteredGames) {
    if (search) { let slug=g.event_slug.toLowerCase(); if (!slug.includes(search)) continue; }
    let ms = g.all_markets||[];
    if (type) ms = ms.filter(m=>m.market_type===type);
    if (search) { let s=search; ms = ms.filter(m=>m.slug.toLowerCase().includes(s)); }
    for (let m of ms) {
      if (m.has_trader_data && m.unique_traders >= minT) {
        allMarkets.push({...m, game_date: g.game_date});
      }
    }
  }
  allMarkets.sort((a,b)=>((b.conviction||0)-(a.conviction||0)));

  // Summary section: all signals in a compact table
  let summaryHtml = "";
  if (allMarkets.length) {
    summaryHtml = `<div class="summary-section">
      <h2 class="summary-heading">All Live Signals <span class="date-count">${allMarkets.length} markets</span></h2>
      <div class="table-wrap"><table>
        <thead><tr><th>Market</th><th>Date</th><th>Type</th><th>Lean</th><th>Frac</th><th>Conviction</th><th>Depth</th><th>Traders</th><th>Trades</th></tr></thead>
        ${allMarkets.map(m => {
          let gd=m.game_date||"";
          let isToday=gd===todayStr;
          let dateLabel=isToday ? '<span style="color:var(--green)">TODAY</span>' : fmtDate(gd);
          let frac=m.top_weighted_fraction||0;
          let fc=frac>=0.7?"text-green":frac>=0.5?"text-yellow":"text-red";
          let ob=m.orderbook;
          let depthPct=ob&&ob.depth_imbalance!=null?ob.depth_imbalance:null;
          let depthLabel=depthPct!=null?fmtPct(depthPct):"-";
          let dc=depthPct>=0.6?"text-green":depthPct>=0.5?"text-yellow":"text-red";
          return `<tr>
            <td style="max-width:220px;overflow:hidden;text-overflow:ellipsis">${fmtSlug(m.slug)}</td>
            <td>${dateLabel}</td>
            <td>${marketTypeTag(m.market_type)}</td>
            <td class="${fc}" style="font-weight:500">${m.top_outcome||"?"}</td>
            <td class="${fc}">${fmtPct(frac)}</td>
            <td>${convBar(m.conviction||0)} ${fmtPct(m.conviction||0)}</td>
            <td class="${dc}">${depthLabel}</td>
            <td>${m.unique_traders||0}</td>
            <td>${m.total_trade_events||0}</td>
          </tr>`;
        }).join("")}
      </table></div>
    </div>`;
  }

  let html=summaryHtml;
  if (summaryHtml) html+=`<div style="border-top:1px solid var(--border);margin:20px 0 16px"></div>`;
  for (let g of filteredGames) {
    if (search) {
      let slug=g.event_slug.toLowerCase();
      if (!slug.includes(search)) continue;
    }
    let gd=g.game_date||"";
    let isToday=gd===todayStr;
    let label=isToday?"\uD83D\uDD34 TODAY":fmtDateFull(gd);

    // Build market list
    let markets=g.all_markets||[];
    if (type) markets=markets.filter(m=>m.market_type===type);
    if (search) markets=markets.filter(m=>m.slug.toLowerCase().includes(search));

    if (!markets.length) continue;

    html+=`<div class="date-section"><h2 class="date-heading" style="${isToday?'color:var(--green)':''}">${label} <span class="date-count">${g.event_slug.split('-').slice(1,3).map(t=>TEAM_MAP[t]||t.toUpperCase()).join(' @ ')}</span></h2><div class="signal-cards">`;
    for (let m of markets) {
      let hasData=m.has_trader_data;
      let conv=m.conviction||0, tr=m.unique_traders||0, ev=m.total_trade_events||0;
      let top=m.top_outcome||"", frac=m.top_weighted_fraction||0;
      let fc=frac>=0.7?"text-green":frac>=0.5?"text-yellow":"text-red";
      let fmtSlugged=fmtSlug(m.slug);

      if (hasData && tr>=minT && conv>=0) {
        sigCount++;
        if (gd) gameDays.add(gd);
        if (tr) traderSet.add(tr);
      }

      if (hasData) {
        html+=`<div class="signal-card">
          <div class="sc-header">${marketTypeTag(m.market_type)} <span class="sc-slug" title="${m.slug}">${fmtSlugged}</span></div>
          <div class="sc-body">
            <div class="sc-lean ${fc}"><span class="sc-label">Lean</span> ${top||"?"}</div>
            <div class="sc-frac ${fc}"><span class="sc-label">Frac</span> ${fmtPct(frac)}</div>
            <div class="sc-conv">${convBar(conv)} ${fmtPct(conv)}</div>
            <div class="sc-traders"><span class="sc-label">Traders</span> ${tr}</div>
            <div class="sc-events"><span class="sc-label">Trades</span> ${ev}</div>
            ${renderDepthBars(m.orderbook)}
          </div>
        </div>`;
      } else {
        html+=`<div class="signal-card" style="opacity:0.5">
          <div class="sc-header">${marketTypeTag(m.market_type)} <span class="sc-slug" title="${m.slug}">${fmtSlugged}</span></div>
          <div class="sc-body" style="text-align:center;color:var(--text2);padding:10px 0;grid-column:1/-1">
            Awaiting trader activity
          </div>
        </div>`;
      }
    }
    html+=`</div></div>`;
  }
  document.getElementById("liveGrid").innerHTML=html||'<div style="padding:40px;text-align:center;color:var(--text2)">No games found. Try expanding the date range.</div>';

  document.getElementById("liveStats").innerHTML=`
    <span class="live-stat"><span class="ls-num">${sigCount}</span> signals</span>
    <span class="live-stat"><span class="ls-num">${gameDays.size}</span> game days</span>
  `;
}

function renderLiveTypeFilter() {
  let sel=document.getElementById("liveType");
  sel.innerHTML='<option value="">All types</option>'+state.liveTypes.map(t=>`<option value="${t}">${t}</option>`).join("");
}

// ── Poll ─────────────────────────────────────────────────────────────
async function triggerPoll() {
  let btn=document.getElementById("btnRefresh");
  let ind=document.getElementById("liveIndicator");
  btn.disabled=true; btn.textContent="\u23F3 Polling...";
  ind.textContent="\u23F3 Polling..."; ind.style.color="var(--yellow)";
  let result=await api("/api/live/poll");
  if (result && result.status==="ok") {
    await reloadLiveData();
    btn.textContent="\u2713 Polled";
    setTimeout(()=>{btn.disabled=false; btn.textContent="\u21bb Poll Now";},2000);
  } else if (result && result.status==="poll already in progress") {
    ind.textContent="\u23F3 Poll running... waiting";
    // Retry every 5s until generated_at changes (i.e. the running poll wrote new data)
    let before=state.liveStatus&&state.liveStatus.generated_at;
    let waited=0;
    let check = setInterval(async ()=>{
      let s=await api("/api/live");
      if (s && s.generated_at && s.generated_at!==before) {
        clearInterval(check);
        await reloadLiveData();
        btn.textContent="\u2713 Polled";
        setTimeout(()=>{btn.disabled=false; btn.textContent="\u21bb Poll Now";},2000);
        return;
      }
      waited+=5;
      if (waited>=900) {
        clearInterval(check);
        btn.disabled=false; btn.textContent="\u21bb Poll Now";
        ind.textContent="\u26A0 Poll timed out"; ind.style.color="var(--red)";
      }
    }, 5000);
  } else {
    btn.disabled=false; btn.textContent="\u21bb Poll Now";
    ind.textContent="\u26A0 Poll failed"; ind.style.color="var(--red)";
  }
}

async function reloadLiveData() {
  let [status, summary, consensus, games, types] = await Promise.all([
    api("/api/live"), api("/api/live/summary"), api("/api/live/consensus?limit=500&min_traders=1"),
    api("/api/live/games"), api("/api/live/market-types"),
  ]);
  state.liveStatus=status; state.liveSummary=summary; state.liveConsensus=consensus||[];
  state.liveGames=games||[]; state.liveTypes=types||[];
  updateLiveHeader();
  renderLiveTypeFilter();
  renderLive();
  renderRecommendations();
}

// ── Overview (Historical) ────────────────────────────────────────────
async function renderOverview() {
  let summary = await api("/api/summary");
  if (!summary) { document.getElementById("statsRow").innerHTML='<div style="color:var(--text2)">No historical data</div>'; return; }
  document.getElementById("statsRow").innerHTML = `
    <div class="stat-card"><div class="num">${fmtNum(summary.unique_markets)}</div><div class="label">Markets</div></div>
    <div class="stat-card"><div class="num">${fmtNum(summary.unique_games)}</div><div class="label">Games</div></div>
    <div class="stat-card"><div class="num">${fmtNum(summary.total_trade_events)}</div><div class="label">Trade Events</div></div>
    <div class="stat-card"><div class="num">${fmtNum(summary.unique_traders_in_trades)}</div><div class="label">Active Traders</div></div>`;
}

// ── Signals (Historical) ─────────────────────────────────────────────
function renderSignals() {
  let type=document.getElementById("sigType").value;
  let days=document.getElementById("sigDays").value;
  let minT=document.getElementById("sigMinTraders").value;
  let minC=parseInt(document.getElementById("sigMinConv").value);
  let search=document.getElementById("sigSearch").value;
  document.getElementById("sigMinConvLabel").textContent=minC+"%";
  let params=new URLSearchParams({min_traders:minT, min_conviction:(minC/100), limit:100});
  if (type) params.set("market_type",type);
  if (days) params.set("days",days);
  if (search) params.set("search",search);
  api("/api/consensus?"+params.toString()).then(data=>{
    let list=data||[];
    document.getElementById("signalsTable").innerHTML = list.length
      ? `<table><thead><tr><th>Market</th><th>Date</th><th>Type</th><th>Lean</th><th>Frac</th><th>Conviction</th><th>Traders</th><th>Events</th></tr></thead>${
        list.map(m=>{
          let slug=m.slug||"", date=m.last_trade_date||slug.match(/\d{4}-\d{2}-\d{2}/)?.[0]||"";
          let top=m.top_outcome||"?", frac=m.top_weighted_fraction||0, conv=m.conviction||0;
          let t=m.unique_traders||0, ev=m.total_trade_events||0;
          let fc=frac>0.7?"text-green":frac>0.5?"text-yellow":"text-red";
          return `<tr><td style="max-width:240px;overflow:hidden;text-overflow:ellipsis">${fmtSlug(slug)}</td>
            <td>${date}</td><td>${marketTypeTag(m.market_type)}</td>
            <td class="${fc}" style="font-weight:500">${top}</td><td class="${fc}">${fmtPct(frac)}</td>
            <td>${convBar(conv)} ${fmtPct(conv)}</td><td>${t}</td><td>${ev}</td></tr>`;
        }).join("")
      }</table>`
      : '<div style="padding:20px;text-align:center;color:var(--text2)">No signals match filters</div>';
  });
  // Load types
  api("/api/market-types").then(types=>{
    if (!types) return;
    let sel=document.getElementById("sigType");
    sel.innerHTML='<option value="">All Types</option>'+types.map(t=>`<option value="${t}">${t}</option>`).join("");
  });
}

// ── Games (Historical) ──────────────────────────────────────────────
function renderGames() {
  let search=document.getElementById("gameSearch").value.toLowerCase();
  api("/api/games").then(games=>{
    let filtered=games.filter(g=>!search||g.event_slug.toLowerCase().includes(search));
    document.getElementById("gameList").innerHTML=filtered.length
      ? filtered.map(g=>{
        let ml=g.moneyline, tot=g.total, sp=g.spread;
        let mls=ml?`<div class="game-market"><span class="label">${fmtSlug(ml.slug)}</span><span class="value ${ml.top_weighted_fraction>0.7?'text-green':'text-yellow'}">${ml.top_outcome} ${fmtPct(ml.top_weighted_fraction)} (${ml.unique_traders}t)</span></div>`:"";
        let tots=tot?`<div class="game-market"><span class="label">${fmtSlug(tot.slug)}</span><span class="value ${tot.top_weighted_fraction>0.7?'text-green':'text-yellow'}">${tot.top_outcome} ${fmtPct(tot.top_weighted_fraction)} (${tot.unique_traders}t)</span></div>`:"";
        let sps=sp?`<div class="game-market"><span class="label">${fmtSlug(sp.slug)}</span><span class="value ${sp.top_weighted_fraction>0.7?'text-green':'text-yellow'}">${sp.top_outcome} ${fmtPct(sp.top_weighted_fraction)} (${sp.unique_traders}t)</span></div>`:"";
        let extra=(g.top_markets||[]).filter(m=>m!==ml&&m!==tot&&m!==sp).slice(0,3).map(m=>
          `<div class="game-market"><span class="label">${fmtSlug(m.slug)}</span><span class="value">${m.top_outcome} ${fmtPct(m.top_weighted_fraction)} (${m.unique_traders}t)</span></div>`
        ).join("");
        return `<div class="game-card"><h3>${fmtSlug(g.event_slug)}</h3>
          <div class="game-meta">${g.market_count} markets \u00B7 ${g.total_trade_events} trades \u00B7 ${g.unique_traders} traders</div>
          <div class="game-markets">${mls}${tots}${sps}${extra}</div></div>`;
      }).join("")
      : '<div style="padding:20px;text-align:center;color:var(--text2)">No games found</div>';
  });
}

// ── Traders ──────────────────────────────────────────────────────────
function renderTraders() {
  let tier=document.getElementById("trTier").value;
  let sort=document.getElementById("trSort").value;
  let url=`/api/traders?limit=200&sort=${sort}`;
  if (tier) url+=`&tier=${tier}`;
  api(url).then(traders=>{
    if (!traders||!traders.length) { document.getElementById("tradersTable").innerHTML='<div style="padding:20px;text-align:center;color:var(--text2)">No traders</div>'; return; }
    document.getElementById("tradersTable").innerHTML = `<table>
      <thead><tr><th>Wallet</th><th>BB PnL</th><th>Trades</th><th>WR</th><th>Sharpe</th><th>Human</th><th>Flags</th></tr></thead>
      ${traders.map(t=>`<tr>
        <td><span class="wallet-short">${fmtWallet(t.wallet)}</span></td>
        <td class="text-right ${t.baseball_pnl_15d>0?'text-green':'text-red'}">${fmtDollar(t.baseball_pnl_15d)}</td>
        <td class="text-right">${fmtNum(t.baseball_trades_15d)}</td>
        <td>${fmtPct(t.win_rate)}</td>
        <td>${t.sharpe_ratio!=null?t.sharpe_ratio.toFixed(2):"-"}</td>
        <td>${t.human_likeness_score||"-"}</td>
        <td>${(t.behavioral_flags||[]).length?t.behavioral_flags.slice(0,2).map(f=>`<span class="tag tag-flag">${f}</span>`).join(""):`<span class="tag tag-clean">clean</span>`}</td>
      </tr>`).join("")}
    </table>`;
  });
}

// ── Tracking ─────────────────────────────────────────────────────────
function renderTracking() {
  api("/api/tracking").then(data=>{
    if (!data || data.error) {
      document.getElementById("trackingGrid").innerHTML='<div style="padding:40px;text-align:center;color:var(--text2)">No tracking data yet.</div>';
      return;
    }
    let stats=data.stats||{};
    let total=stats.total_tracked||0, resolved=stats.resolved||0;
    let correct=stats.correct||0, incorrect=stats.incorrect||0;
    let acc=stats.accuracy||0;

    document.getElementById("trackingHeader").innerHTML=`
      <div class="stats-row">
        <div class="stat-card"><div class="num">${total}</div><div class="label">Tracked Games</div></div>
        <div class="stat-card"><div class="num">${resolved}</div><div class="label">Resolved</div></div>
        <div class="stat-card ${acc>=0.5?'text-green':'text-red'}"><div class="num">${fmtPct(acc)}</div><div class="label">Accuracy</div></div>
      </div>`;

    let games=data.games||[];
    if (!games.length) {
      document.getElementById("trackingGrid").innerHTML='<div style="padding:40px;text-align:center;color:var(--text2)">No games tracked yet. Snapshots are taken automatically on game day.</div>';
      return;
    }

    // Reverse-chronological
    let sorted=[...games].sort((a,b)=>(b.game_date||"").localeCompare(a.game_date||""));

    let html="";
    for (let g of sorted) {
      let marker=g.resolved
        ? '<span style="color:var(--green)">\u2713 Resolved</span>'
        : '<span style="color:var(--yellow)">\u23F3 Pending</span>';
      html+=`<div class="game-card"><h3>${fmtSlug(g.event_slug)} <span style="font-weight:400;font-size:0.85em">${g.game_date}</span></h3>
        <div class="game-meta">${marker} \u00B7 Snapshot: ${(g.snapshot_at||"").slice(11,19)} UTC</div>
        <div style="margin-top:8px">`;
      for (let [mk, m] of Object.entries(g.markets||{})) {
        let resultIcon, resultClass;
        if (m.correct===true) { resultIcon="\u2713"; resultClass="text-green"; }
        else if (m.correct===false) { resultIcon="\u2717"; resultClass="text-red"; }
        else { resultIcon="\u23F3"; resultClass="text-yellow"; }
        let actual=m.actual_outcome||"pending";
        html+=`<div class="game-market">
          <span class="label">${marketTypeTag(m.market_type)} ${fmtSlug(m.slug)}</span>
          <span class="value">Predicted: <strong>${m.predicted_outcome}</strong> (${fmtPct(m.conviction)})</span>
          <span class="value ${resultClass}">\u2192 ${resultIcon} ${actual}</span>
        </div>`;
      }
      html+=`</div></div>`;
    }
    document.getElementById("trackingGrid").innerHTML=html;
  });
}

// ── Backtest ─────────────────────────────────────────────────────────
function renderBacktest() {
  api("/api/backtest").then(data=>{
    if (!data || data.error) {
      document.getElementById("btHeader").innerHTML=`
        <div style="background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:24px;margin-bottom:16px">
          <p style="color:var(--text2);margin-bottom:8px">No backtest results yet.</p>
          <p>Run <code style="background:var(--bg3);padding:2px 6px;border-radius:4px">python3 scripts/backtest.py</code> from the project root to generate them.</p>
        </div>`;
      return;
    }

    let stats=data.stats||{};
    let s=stats;
    let total=s.total_markets||0, resolved=s.resolved||0;
    let correct=s.correct||0, incorrect=s.incorrect||0;
    let accuracy=s.accuracy||0, pnl=s.simulated_pnl||0, roi=s.simulated_roi||0;

    document.getElementById("btHeader").innerHTML=`
      <div style="margin-bottom:12px;color:var(--text2);font-size:13px">Data range: ${data.data_range.start} to ${data.data_range.end}  \u00B7  conviction \u2265 ${fmtPct(data.threshold)}</div>
      <div class="stats-row">
        <div class="stat-card"><div class="num">${total}</div><div class="label">Markets Tested</div></div>
        <div class="stat-card"><div class="num">${resolved}</div><div class="label">Resolved</div></div>
        <div class="stat-card ${accuracy>=0.5?'text-green':'text-red'}"><div class="num">${fmtPct(accuracy)}</div><div class="label">Accuracy</div></div>
        <div class="stat-card ${pnl>=0?'text-green':'text-red'}"><div class="num">${pnl>=0?'+':''}${pnl}</div><div class="label">Sim. PnL ($1 stakes)</div></div>
        <div class="stat-card ${roi>=0?'text-green':'text-red'}"><div class="num">${roi>=0?'+':''}${roi}%</div><div class="label">ROI</div></div>
      </div>`;

    let bt=stats.by_threshold||{};
    let thresholdRows=Object.entries(bt).map(([t,d])=>{
      let acc=d.resolved?fmtPct(d.accuracy):'-';
      let cls=d.accuracy>=0.5?'text-green':'text-red';
      return `<tr><td>\u2265 ${(parseFloat(t)*100).toFixed(0)}%</td>
        <td class="text-right">${d.total}</td>
        <td class="text-right">${d.resolved}</td>
        <td class="text-right text-green">${d.correct}</td>
        <td class="text-right text-red">${d.incorrect}</td>
        <td class="text-right ${cls}">${acc}</td></tr>`;
    }).join("");

    document.getElementById("btThresholds").innerHTML=`
      <div class="bt-card">
        <h3 class="bt-heading">Accuracy by Conviction Threshold</h3>
        <div class="table-wrap"><table>
          <thead><tr><th>Threshold</th><th>Total</th><th>Resolved</th><th>Correct</th><th>Incorrect</th><th>Accuracy</th></tr></thead>
          ${thresholdRows}
        </table></div>
      </div>`;

    let byType=stats.by_market_type||{};
    let typeRows=Object.entries(byType).sort(([,a],[,b])=>b.total-a.total).map(([mt,d])=>{
      let acc=d.resolved?fmtPct(d.accuracy):'-';
      let cls=d.accuracy>=0.5?'text-green':'text-red';
      return `<tr><td>${marketTypeTag(mt)} ${mt}</td>
        <td class="text-right">${d.total}</td>
        <td class="text-right">${d.resolved}</td>
        <td class="text-right text-green">${d.correct}</td>
        <td class="text-right text-red">${d.incorrect}</td>
        <td class="text-right ${cls}">${acc}</td></tr>`;
    }).join("");

    document.getElementById("btByType").innerHTML=`
      <div class="bt-card">
        <h3 class="bt-heading">Accuracy by Market Type</h3>
        <div class="table-wrap"><table>
          <thead><tr><th>Market Type</th><th>Total</th><th>Resolved</th><th>Correct</th><th>Incorrect</th><th>Accuracy</th></tr></thead>
          ${typeRows}
        </table></div>
      </div>`;

    let bands=stats.by_conviction_band||{};
    let bandRows=Object.entries(bands).map(([b,d])=>{
      let acc=d.resolved?fmtPct(d.accuracy):'-';
      let cls=d.accuracy>=0.5?'text-green':'text-red';
      return `<tr><td>${b}</td>
        <td class="text-right">${d.total}</td>
        <td class="text-right">${d.resolved}</td>
        <td class="text-right text-green">${d.correct}</td>
        <td class="text-right text-red">${d.incorrect}</td>
        <td class="text-right ${cls}">${acc}</td></tr>`;
    }).join("");

    document.getElementById("btBands").innerHTML=`
      <div class="bt-card">
        <h3 class="bt-heading">Accuracy by Conviction Band</h3>
        <div class="table-wrap"><table>
          <thead><tr><th>Band</th><th>Total</th><th>Resolved</th><th>Correct</th><th>Incorrect</th><th>Accuracy</th></tr></thead>
          ${bandRows}
        </table></div>
      </div>`;

    let predictions=data.predictions||[];
    let detailRows=predictions.filter(p=>p.correct!=null)
      .sort((a,b)=>Math.abs(b.conviction||0)-Math.abs(a.conviction||0))
      .map(p=>{
      let icon=p.correct?'\u2713':'\u2717';
      let cls=p.correct?'text-green':'text-red';
      return `<tr>
        <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis" title="${p.slug}">${fmtSlug(p.slug)}</td>
        <td>${p.game_date||'-'}</td>
        <td>${marketTypeTag(p.market_type)}</td>
        <td style="font-weight:500">${p.predicted_outcome}</td>
        <td>${p.actual_outcome||'?'}</td>
        <td>${convBar(p.conviction)} ${fmtPct(p.conviction)}</td>
        <td>${p.unique_traders||0}</td>
        <td class="${cls}" style="font-size:18px;text-align:center">${icon}</td>
      </tr>`;
    }).join("");

    document.getElementById("btDetails").innerHTML=`
      <div class="bt-card">
        <h3 class="bt-heading">Individual Predictions <span class="date-count">${predictions.filter(p=>p.correct!=null).length} resolved</span></h3>
        <div class="table-wrap" style="max-height:600px;overflow-y:auto"><table>
          <thead><tr><th>Market</th><th>Date</th><th>Type</th><th>Predicted</th><th>Actual</th><th>Conviction</th><th>Traders</th><th>Result</th></tr></thead>
          ${detailRows||'<tr><td colspan="8" style="text-align:center;color:var(--text2)">No resolved predictions yet</td></tr>'}
        </table></div>
      </div>`;
  });
}
init();
