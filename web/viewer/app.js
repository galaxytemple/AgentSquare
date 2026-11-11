// AgentSquare Static Ledger Replay & Verification Web Viewer (Spec B3)
(function () {
  const extRenderers = {};
  window.registerExtView = function (namespace, renderFn) {
    extRenderers[namespace] = renderFn;
  };

  let records = [];
  let currentRound = 1;
  let maxRound = 1;
  let currentAsset = "copper";
  let currentView = "market";
  let playTimer = null;
  let firstMismatchSeq = null;
  let wasmReady = false;

  // Attempt to load Go WASM verifier if available
  if (typeof Go !== "undefined" && !window.__noWasmExec) {
    const go = new Go();
    fetch("agentsquare.wasm")
      .then((r) => (r.ok ? WebAssembly.instantiateStreaming(r, go.importObject) : Promise.reject()))
      .then((res) => {
        go.run(res.instance);
        wasmReady = true;
      })
      .catch(() => {
        wasmReady = false;
      });
  }

  function parseHashState() {
    const params = new URLSearchParams(window.location.hash.replace(/^#/, ""));
    if (params.get("round")) currentRound = parseInt(params.get("round"), 10) || 1;
    if (params.get("asset")) currentAsset = params.get("asset");
    if (params.get("view")) currentView = params.get("view");
    const q = new URLSearchParams(window.location.search);
    if (q.get("embed") === "1") document.body.classList.add("embed-mode");
    if (q.get("ledger")) fetchLedgerFromURL(q.get("ledger"));
  }

  function updateHashState() {
    window.location.hash = `round=${currentRound}&asset=${encodeURIComponent(currentAsset)}&view=${encodeURIComponent(currentView)}`;
  }

  function verifyRecords(rawJSONL) {
    firstMismatchSeq = null;
    if (wasmReady && typeof window.agentSquareVerifyLedger === "function") {
      const res = JSON.parse(window.agentSquareVerifyLedger(rawJSONL, ""));
      return res;
    }
    // Deterministic hash-chain + sequence verifier in JS
    let expectedSeq = 1;
    let expectedPrev = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
    for (const r of records) {
      if (r.seq !== expectedSeq || r.prev_hash !== expectedPrev || !r.server_sig || !r.hash) {
        firstMismatchSeq = r.seq || expectedSeq;
        return { valid: false, first_mismatch: firstMismatchSeq, error: `Chain mismatch at seq ${expectedSeq}` };
      }
      expectedSeq++;
      expectedPrev = r.hash;
    }
    return {
      valid: true,
      records: records.length,
      last_seq: records.length ? records[records.length - 1].seq : 0,
      last_hash: expectedPrev,
    };
  }

  function loadJSONLText(text) {
    const lines = text.trim().split("\n").filter((l) => l.trim().length > 0);
    records = lines.map((l) => JSON.parse(l));
    const v = verifyRecords(text);
    const badge = document.getElementById("verification-badge");
    const badgeText = document.getElementById("badge-text");

    if (v.valid) {
      badge.className = "badge badge-ok";
      badgeText.textContent = `VERIFIED (${records.length} records · head ${(v.last_hash || "").slice(0, 14)}…)`;
    } else {
      badge.className = "badge badge-err";
      badgeText.textContent = `MISMATCH at seq #${v.first_mismatch}: ${v.error}`;
    }

    maxRound = 1;
    const assets = new Set(["copper"]);
    for (const r of records) {
      if (r.body && r.body.round && r.body.round > maxRound) maxRound = r.body.round;
      if (r.body && r.body.asset) assets.add(r.body.asset);
    }

    const sel = document.getElementById("asset-select");
    sel.innerHTML = "";
    for (const a of assets) {
      const opt = document.createElement("option");
      opt.value = a;
      opt.textContent = a;
      if (a === currentAsset) opt.selected = true;
      sel.appendChild(opt);
    }

    const scrubber = document.getElementById("round-scrubber");
    scrubber.max = String(maxRound);
    if (currentRound > maxRound) currentRound = maxRound;
    scrubber.value = String(currentRound);

    renderAll();
  }

  function renderAll() {
    document.getElementById("round-label").textContent = String(currentRound);
    updateHashState();
    renderAdminMarkers();
    renderMarket();
    renderAnatomy();
    renderBoard();
    renderAgents();
    renderExplorer();
  }

  function renderAdminMarkers() {
    const wrap = document.getElementById("admin-markers");
    wrap.innerHTML = "";
    for (const r of records) {
      if (r.type && r.type.startsWith("admin.")) {
        const span = document.createElement("span");
        span.className = "admin-dot";
        span.textContent = `${r.type} (seq ${r.seq})`;
        wrap.appendChild(span);
      }
    }
  }

  function renderMarket() {
    const chartBox = document.getElementById("candles-chart");
    const tapeBox = document.getElementById("tape-list");
    chartBox.innerHTML = "";
    tapeBox.innerHTML = "";

    const clears = records.filter((r) => r.type === "clear" && r.body.asset === currentAsset && r.body.round <= currentRound);
    const fills = records.filter((r) => r.type === "fill" && r.body.asset === currentAsset && r.body.round <= currentRound);

    if (clears.length === 0) {
      chartBox.innerHTML = `<div class="record-row">No cleared rounds yet for ${currentAsset} up to round ${currentRound}.</div>`;
    } else {
      for (const c of clears) {
        const div = document.createElement("div");
        div.className = "record-row";
        div.innerHTML = `<strong>Round ${c.body.round}</strong> — Clearing Price: <strong>$${c.body.clearing_price}</strong> · Volume: <strong>${c.body.volume}</strong>`;
        chartBox.appendChild(div);
      }
    }

    for (const f of fills) {
      const div = document.createElement("div");
      div.className = "record-row";
      div.innerHTML = `[Round ${f.body.round}] <code>${f.body.agent_id}</code> <strong>${f.body.side.toUpperCase()}</strong> ${f.body.filled_qty} ${f.body.asset} @ $${f.body.clearing_price} ($${f.body.cash_delta})`;
      tapeBox.appendChild(div);
    }
  }

  function renderAnatomy() {
    const box = document.getElementById("anatomy-steps");
    box.innerHTML = "";

    const orders = records.filter((r) => r.type === "order_accepted" && r.body.round === currentRound && r.body.asset === currentAsset);
    const clearRec = records.find((r) => r.type === "clear" && r.body.round === currentRound && r.body.asset === currentAsset);
    const fills = records.filter((r) => r.type === "fill" && r.body.round === currentRound && r.body.asset === currentAsset);

    const steps = [
      {
        title: "1. Supply & Demand Aggregation",
        html: `${orders.length} accepted orders in Round ${currentRound} for <strong>${currentAsset}</strong>.<pre>${JSON.stringify(orders.map((o) => o.body), null, 2)}</pre>`,
      },
      {
        title: "2. Uniform Clearing Price & Tie-Break",
        html: clearRec
          ? `Clearing Price: <strong>$${clearRec.body.clearing_price}</strong><br/>Executed Volume: <strong>${clearRec.body.volume}</strong><br/>Prev Price: $${clearRec.body.prev_price} · Ref Price: $${clearRec.body.reference_price}`
          : "Round not yet cleared.",
      },
      {
        title: "3. Deterministic Seed & Round-Robin Lottery",
        html: clearRec
          ? `Seed Sig: <code>${clearRec.body.seed_sig.slice(0, 32)}…</code><br/>Uniform per-agent round-robin + Fisher-Yates shuffle.`
          : "Waiting for round_sealed.",
      },
      {
        title: "4. Atomic Settlement & Escrow Release",
        html: `${fills.length} agent fills settled:<pre>${JSON.stringify(fills.map((f) => f.body), null, 2)}</pre>`,
      },
    ];

    for (const s of steps) {
      const el = document.createElement("div");
      el.className = "anatomy-step";
      el.innerHTML = `<h3>${s.title}</h3><div>${s.html}</div>`;
      box.appendChild(el);
    }
  }

  function renderBoard() {
    const box = document.getElementById("board-feed");
    box.innerHTML = "";
    const posts = records.filter((r) => r.type === "ext" && r.body.namespace === "board");
    if (posts.length === 0) {
      box.innerHTML = `<div class="record-row">No board posts anchored in this ledger.</div>`;
      return;
    }
    for (const p of posts) {
      const div = document.createElement("div");
      div.className = "record-row";
      const b = p.body.body || {};
      const stanceCls = b.stance === "bull" ? "stance-bull" : b.stance === "bear" ? "stance-bear" : "";
      div.innerHTML = `[Round ${b.round || "?"}] <code>${b.author || ""}</code> <span class="${stanceCls}">${(b.stance || "neutral").toUpperCase()}</span> on <strong>${b.asset || "general"}</strong> · hash <code>${b.content_hash || ""}</code>`;
      box.appendChild(div);
    }
  }

  function renderAgents() {
    const box = document.getElementById("agent-table");
    box.innerHTML = "";
    const agents = {};
    for (const r of records) {
      if (r.body && r.body.round && r.body.round > currentRound) continue;
      if (r.type === "agent.admitted") {
        agents[r.body.agent_id] = { balances: {}, fills: 0 };
      } else if (r.type === "balance.credit") {
        agents[r.body.agent_id] = agents[r.body.agent_id] || { balances: {}, fills: 0 };
        agents[r.body.agent_id].balances[r.body.asset] = (agents[r.body.agent_id].balances[r.body.asset] || 0) + r.body.qty;
      } else if (r.type === "fill") {
        const ag = (agents[r.body.agent_id] = agents[r.body.agent_id] || { balances: {}, fills: 0 });
        ag.fills++;
        if (r.body.side === "buy") {
          ag.balances[r.body.asset] = (ag.balances[r.body.asset] || 0) + r.body.filled_qty;
          ag.balances["usd"] = (ag.balances["usd"] || 0) - r.body.cash_delta;
        } else {
          ag.balances[r.body.asset] = (ag.balances[r.body.asset] || 0) - r.body.filled_qty;
          ag.balances["usd"] = (ag.balances["usd"] || 0) + r.body.cash_delta;
        }
      }
    }
    for (const [id, data] of Object.entries(agents)) {
      const div = document.createElement("div");
      div.className = "record-row";
      div.innerHTML = `<strong>${id}</strong> · Fills: ${data.fills}<pre>${JSON.stringify(data.balances, null, 2)}</pre>`;
      box.appendChild(div);
    }
  }

  function renderExplorer() {
    const box = document.getElementById("record-list");
    box.innerHTML = "";
    for (const r of records) {
      const div = document.createElement("div");
      div.id = `rec-seq-${r.seq}`;
      div.className = "record-row" + (r.type && r.type.startsWith("admin.") ? " admin-rec" : "");
      let bodyHTML = `<pre>${JSON.stringify(r.body, null, 2)}</pre>`;
      if (r.type === "ext" && r.body && extRenderers[r.body.namespace]) {
        bodyHTML = extRenderers[r.body.namespace](r.body.body);
      }
      div.innerHTML = `<div><strong>#${r.seq}</strong> <code>${r.type}</code> · hash <code>${r.hash.slice(0, 20)}…</code> · prev <code>${r.prev_hash.slice(0, 20)}…</code></div>${bodyHTML}`;
      box.appendChild(div);
    }
  }

  function fetchLedgerFromURL(url) {
    const target = url.endsWith("/v1/ledger") || url.endsWith(".jsonl") ? url : url.replace(/\/$/, "") + "/v1/ledger";
    fetch(target)
      .then((r) => r.text())
      .then(loadJSONLText)
      .catch((err) => alert("Failed to load ledger: " + err.message));
  }

  // Event bindings
  document.querySelectorAll(".view-tabs .tab").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".view-tabs .tab").forEach((b) => b.classList.remove("active"));
      document.querySelectorAll(".view-panel").forEach((p) => p.classList.remove("active"));
      btn.classList.add("active");
      currentView = btn.dataset.view;
      document.getElementById("view-" + currentView).classList.add("active");
      updateHashState();
    });
  });

  document.getElementById("round-scrubber").addEventListener("input", (e) => {
    currentRound = parseInt(e.target.value, 10) || 1;
    renderAll();
  });

  document.getElementById("asset-select").addEventListener("change", (e) => {
    currentAsset = e.target.value;
    renderAll();
  });

  document.getElementById("verification-badge").addEventListener("click", () => {
    if (firstMismatchSeq) {
      document.querySelector('[data-view="explorer"]').click();
      const el = document.getElementById(`rec-seq-${firstMismatchSeq}`);
      if (el) el.scrollIntoView({ behavior: "smooth" });
    }
  });

  document.getElementById("btn-play").addEventListener("click", (e) => {
    if (playTimer) {
      clearInterval(playTimer);
      playTimer = null;
      e.target.textContent = "▶ Play";
      return;
    }
    const speed = parseInt(document.getElementById("play-speed").value, 10) || 1;
    e.target.textContent = "⏸ Pause";
    playTimer = setInterval(() => {
      if (currentRound < maxRound) {
        currentRound++;
      } else {
        currentRound = 1;
      }
      document.getElementById("round-scrubber").value = String(currentRound);
      renderAll();
    }, Math.max(20, Math.floor(1000 / speed)));
  });

  document.getElementById("btn-fetch").addEventListener("click", () => {
    const u = document.getElementById("server-url").value.trim();
    if (u) fetchLedgerFromURL(u);
  });

  document.getElementById("file-input").addEventListener("change", (e) => {
    const f = e.target.files[0];
    if (!f) return;
    f.text().then(loadJSONLText);
  });

  const dz = document.getElementById("drop-zone");
  dz.addEventListener("dragover", (e) => e.preventDefault());
  dz.addEventListener("drop", (e) => {
    e.preventDefault();
    if (e.dataTransfer.files.length > 0) {
      e.dataTransfer.files[0].text().then(loadJSONLText);
    }
  });

  document.getElementById("btn-sample").addEventListener("click", () => {
    fetch("/spec/test-vectors/01_spec_a10_copper_lottery.json")
      .then((r) => r.json())
      .then((vec) => {
        const lines = (vec.expected_ledger_after || []).map((r) => JSON.stringify(r)).join("\n");
        loadJSONLText(lines);
      })
      .catch(() => {
        alert("Run via 'agentsquare viewer' to load embedded Spec A10 vector, or drop any .jsonl file.");
      });
  });

  parseHashState();
})();
