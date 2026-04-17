/* MANV-Dispatch Dashboard
 * Leaflet map + Bootstrap filter UI for Krankenhäuser
 */
(function () {
  "use strict";

  const SK_COLORS = { SK1: "#b71c1c", SK2: "#f57c00", SK3: "#fbc02d", keine: "#757575" };

  const OCC_COLORS = {
    frei:       { bg: "#2e7d32", label: "frei" },
    mittel:     { bg: "#f9a825", label: "mittel" },
    voll:       { bg: "#e65100", label: "voll" },
    uebervoll:  { bg: "#b71c1c", label: "über" },
    unbekannt:  { bg: "#757575", label: "?" },
  };

  const state = {
    hub: null,
    kliniken: [],
    occupancy: {},       // {kh_id: {status, fill_pct}}
    colorMode: "sk",     // "sk" | "occupancy"
    map: null,
    cluster: null,
    hubMarker: null,
    hubRadiusCircle: null,
    markerById: new Map(),
    transports: [],
    batch: null,
    routesLayer: null,
    showRoutes: false,
    filters: {
      sk: { 1: true, 2: true, 3: true },
      radiusKm: 100,
      minBetten: 0,
      bundesland: "",
      traegerArt: "",
      onlyUni: false,
      flags: new Set(),
      search: "",
    },
  };

  // ---- utils ----
  function haversine(lat1, lon1, lat2, lon2) {
    const R = 6371;
    const toRad = d => (d * Math.PI) / 180;
    const dLat = toRad(lat2 - lat1);
    const dLon = toRad(lon2 - lon1);
    const a = Math.sin(dLat / 2) ** 2 +
              Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
    return 2 * R * Math.asin(Math.sqrt(a));
  }

  function skClass(sk_max) {
    if (sk_max === "SK1") return "sk1";
    if (sk_max === "SK2") return "sk2";
    if (sk_max === "SK3") return "sk3";
    return "sk-none";
  }

  function escapeHtml(s) {
    if (s == null) return "";
    return String(s).replace(/[&<>"']/g, c => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
    })[c]);
  }

  // ---- data load ----
  async function loadData() {
    const [hubsResp, khResp, optsResp] = await Promise.all([
      fetch("/api/hubs").then(r => r.json()),
      fetch("/api/krankenhaeuser").then(r => r.json()),
      fetch("/api/filter-options").then(r => r.json()),
    ]);
    state.hub = hubsResp[0];
    state.kliniken = khResp;
    return optsResp;
  }

  // ---- map ----
  function initMap() {
    const hub = state.hub;
    const map = L.map("map", { zoomControl: true }).setView([hub.lat, hub.lon], 7);
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution: "© OpenStreetMap",
      maxZoom: 18,
    }).addTo(map);

    // Hub marker (big pulsing pin)
    const hubIcon = L.divIcon({
      className: "",
      html: `<div class="hub-pin"><i class="bi bi-hospital-fill"></i></div>`,
      iconSize: [46, 46],
      iconAnchor: [23, 23],
    });
    const hubMarker = L.marker([hub.lat, hub.lon], { icon: hubIcon, zIndexOffset: 1000 })
      .addTo(map)
      .bindPopup(`
        <div class="kh-popup">
          <h6><i class="bi bi-geo-alt-fill text-danger"></i> ${escapeHtml(hub.name)}</h6>
          <div class="popup-meta">${escapeHtml(hub.ort)} · ${escapeHtml(hub.bundesland || "")}</div>
          <div class="popup-meta mt-1"><strong>${hub.kapazitaet_pro_tag} Verletzte/Tag</strong> · 72 h Vorlauf</div>
          <div class="popup-reason">${escapeHtml(hub.beschreibung || "")}</div>
        </div>`);

    // Radius circle
    const radiusCircle = L.circle([hub.lat, hub.lon], {
      radius: state.filters.radiusKm * 1000,
      color: "#c62828",
      weight: 1.5,
      fillOpacity: 0.04,
      dashArray: "4,6",
    }).addTo(map);

    // Marker cluster for hospitals
    const cluster = L.markerClusterGroup({
      chunkedLoading: true,
      showCoverageOnHover: false,
      maxClusterRadius: 50,
    });
    map.addLayer(cluster);

    state.map = map;
    state.cluster = cluster;
    state.hubMarker = hubMarker;
    state.hubRadiusCircle = radiusCircle;
  }

  function iconForKH(k) {
    if (k.ausgeschlossen) {
      return L.divIcon({
        className: "",
        html: `<div class="kh-pin excluded">✕</div>`,
        iconSize: [22, 22], iconAnchor: [11, 11],
      });
    }
    if (state.colorMode === "occupancy") {
      const occ = state.occupancy[k.id];
      const c = OCC_COLORS[occ?.status || "unbekannt"];
      const fill = occ?.fill_pct != null ? `${occ.fill_pct}` : "–";
      return L.divIcon({
        className: "",
        html: `<div class="kh-pin" style="background:${c.bg};color:#fff;">${fill}</div>`,
        iconSize: [22, 22], iconAnchor: [11, 11],
      });
    }
    const sk = skClass(k.sk_max);
    const label = (k.sk_max || "").replace("SK", "") || "–";
    return L.divIcon({
      className: "",
      html: `<div class="kh-pin ${sk}">${label}</div>`,
      iconSize: [22, 22], iconAnchor: [11, 11],
    });
  }

  function makeMarker(k) {
    const marker = L.marker([k.lat, k.lon], { icon: iconForKH(k) });
    marker.bindPopup(() => popupHtml(k), { maxWidth: 340 });
    marker.on("popupopen", e => {
      const btn = e.popup.getElement().querySelector("button[data-kh-id]");
      if (btn) {
        btn.addEventListener("click", () => toggleExclude(Number(btn.dataset.khId)));
      }
    });
    marker._kh = k;
    return marker;
  }

  function refreshMarkerIcons() {
    state.markerById.forEach((m, id) => {
      const k = state.kliniken.find(x => x.id === id);
      if (k) m.setIcon(iconForKH(k));
    });
  }

  function wireMapModeToggle() {
    const btn = document.getElementById("btn-map-mode");
    if (!btn) return;
    btn.addEventListener("click", () => {
      state.colorMode = state.colorMode === "sk" ? "occupancy" : "sk";
      btn.innerHTML = state.colorMode === "occupancy"
        ? '<i class="bi bi-droplet-fill"></i> Belegung'
        : '<i class="bi bi-droplet"></i> SK-Stufe';
      if (state.colorMode === "occupancy") loadOccupancy();
      refreshMarkerIcons();
    });
  }

  async function loadOccupancy() {
    try {
      const arr = await fetch("/api/krankenhaeuser/occupancy").then(r => r.json());
      state.occupancy = {};
      arr.forEach(o => { state.occupancy[o.id] = o; });
      if (state.colorMode === "occupancy") refreshMarkerIcons();
    } catch (e) { console.warn("occupancy load", e); }
  }

  function popupHtml(k) {
    const dist = haversine(state.hub.lat, state.hub.lon, k.lat, k.lon);
    const skMax = k.sk_max || "keine";
    const skCls = skClass(k.sk_max);
    const flags = [];
    const addFlag = (cond, label) => { if (cond) flags.push(`<span class="badge bg-secondary">${label}</span>`); };
    addFlag(k.hat_intensivmedizin, "ITS");
    addFlag(k.hat_notaufnahme, "Notaufnahme");
    addFlag(k.hat_bg_zulassung, "BG");
    addFlag(k.hat_radiologie, "Radiologie");
    addFlag(k.hat_onkologie, "Onkologie");
    addFlag(k.hat_psychiatrie, "Psychiatrie");
    addFlag(k.hat_geriatrie, "Geriatrie");
    addFlag(k.hat_dialyse, "Dialyse");
    if (k.universitaet) flags.push(`<span class="badge bg-info text-dark">Uni: ${escapeHtml(k.universitaet)}</span>`);

    const addr = [k.strasse, k.hausnummer].filter(Boolean).join(" ");
    const place = [k.plz, k.ort].filter(Boolean).join(" ");

    const excludeBtn = k.ausgeschlossen
      ? `<button class="btn btn-sm btn-success w-100" data-action="include" data-kh-id="${k.id}">
           <i class="bi bi-check-circle-fill"></i> Wieder aktivieren
         </button>`
      : `<button class="btn btn-sm btn-outline-danger w-100" data-action="exclude" data-kh-id="${k.id}">
           <i class="bi bi-x-circle-fill"></i> Ausschließen (defekt/nicht erreichbar)
         </button>`;

    const excludedBanner = k.ausgeschlossen
      ? `<div class="alert alert-danger py-1 px-2 small mb-2 mt-2">
           <i class="bi bi-exclamation-octagon-fill"></i> Ausgeschlossen
           ${k.ausschluss_grund ? `— ${escapeHtml(k.ausschluss_grund)}` : ""}
         </div>` : "";

    return `
      <div class="kh-popup">
        <h6>${escapeHtml(k.name)}</h6>
        <div class="popup-meta">${escapeHtml(addr)}${addr && place ? ", " : ""}${escapeHtml(place)}</div>
        ${excludedBanner}
        <div class="mt-1 d-flex align-items-center gap-2">
          <span class="badge sk-badge ${skCls}">${escapeHtml(skMax)}</span>
          <span class="popup-meta">
            <i class="bi bi-geo"></i> ${dist.toFixed(1)} km zum Hub
            ${k.betten ? ` · <strong>${k.betten}</strong> Betten` : ""}
          </span>
        </div>
        ${flags.length ? `<div class="popup-flags">${flags.join("")}</div>` : ""}
        ${k.telefon ? `<div class="popup-meta mt-2"><i class="bi bi-telephone"></i> ${escapeHtml(k.telefon)}</div>` : ""}
        ${k.website ? `<div class="popup-meta"><i class="bi bi-globe"></i> <a href="${escapeHtml(k.website)}" target="_blank" rel="noopener">${escapeHtml(k.website)}</a></div>` : ""}
        <div class="mt-2">${excludeBtn}</div>
      </div>`;
  }

  async function toggleExclude(khId) {
    const grund = prompt("Grund für den Ausschluss (z.B. 'Stromausfall', 'nicht erreichbar'):", "nicht erreichbar");
    if (grund === null) return;  // user cancelled
    const resp = await fetch(`/api/krankenhaus/${khId}/toggle-exclude`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ grund: grund || "manuell ausgeschlossen" }),
    });
    const j = await resp.json();
    // State in geladenem Array updaten
    const kh = state.kliniken.find(k => k.id === khId);
    if (kh) {
      kh.ausgeschlossen = j.ausgeschlossen;
      kh.ausschluss_grund = j.ausschluss_grund;
    }
    // Marker neu rendern für die Klinik
    const oldMarker = state.markerById.get(khId);
    if (oldMarker && kh) {
      state.cluster.removeLayer(oldMarker);
      const newMarker = makeMarker(kh);
      state.markerById.set(khId, newMarker);
      state.cluster.addLayer(newMarker);
      setTimeout(() => newMarker.openPopup(), 100);
    }
  }

  function buildAllMarkers() {
    state.markerById.clear();
    const markers = [];
    for (const k of state.kliniken) {
      if (k.lat == null || k.lon == null) continue;
      const m = makeMarker(k);
      state.markerById.set(k.id, m);
      markers.push(m);
    }
    state.cluster.addLayers(markers);
    renderKPIs();
  }

  // ---- filtering ----
  function passesFilters(k) {
    const f = state.filters;

    const skOk =
      (f.sk[1] && k.kann_sk1) ||
      (f.sk[2] && k.kann_sk2) ||
      (f.sk[3] && k.kann_sk3);
    if (!skOk) return false;

    if ((k.betten ?? 0) < f.minBetten) return false;

    const dist = haversine(state.hub.lat, state.hub.lon, k.lat, k.lon);
    if (dist > f.radiusKm) return false;

    if (f.bundesland && k.bundesland !== f.bundesland) return false;
    if (f.traegerArt && k.traeger_art !== f.traegerArt) return false;

    if (f.onlyUni) {
      const isUni = Boolean(k.universitaet) ||
        (k.lehrkrankenhaus && String(k.lehrkrankenhaus).toLowerCase().startsWith("ja"));
      if (!isUni) return false;
    }

    for (const flag of f.flags) {
      if (!k[flag]) return false;
    }

    if (f.search) {
      const needle = f.search.toLowerCase();
      const hay = `${k.name || ""} ${k.ort || ""} ${k.plz || ""}`.toLowerCase();
      if (!hay.includes(needle)) return false;
    }
    return true;
  }

  function applyFilters() {
    const f = state.filters;
    state.hubRadiusCircle.setRadius(f.radiusKm * 1000);

    const visible = [];
    const keepMarkers = [];
    const removeMarkers = [];

    for (const k of state.kliniken) {
      if (k.lat == null || k.lon == null) continue;
      const m = state.markerById.get(k.id);
      if (!m) continue;
      if (passesFilters(k)) {
        visible.push(k);
        keepMarkers.push(m);
      } else {
        removeMarkers.push(m);
      }
    }

    state.cluster.clearLayers();
    state.cluster.addLayers(keepMarkers);

    updateStats(visible);
    updateTable(visible);
  }

  function updateStats(visible) {
    const sk1 = visible.filter(k => k.kann_sk1).length;
    const sk2 = visible.filter(k => k.kann_sk2).length;
    const sk3 = visible.filter(k => k.kann_sk3).length;
    document.getElementById("stats-visible").textContent = visible.length.toLocaleString("de-DE");
    document.getElementById("stats-total").textContent = state.kliniken.length.toLocaleString("de-DE");
    document.getElementById("stats-sk1").textContent = sk1;
    document.getElementById("stats-sk2").textContent = sk2;
    document.getElementById("stats-sk3").textContent = sk3;
  }

  function updateTable(visible) {
    const tbody = document.querySelector("#klinik-table tbody");
    const withDist = visible.map(k => ({
      k,
      dist: haversine(state.hub.lat, state.hub.lon, k.lat, k.lon),
    })).sort((a, b) => a.dist - b.dist).slice(0, 50);

    tbody.innerHTML = withDist.map(({ k, dist }) => {
      const skCls = skClass(k.sk_max);
      const flagIcons = [
        [k.hat_intensivmedizin, "ITS"],
        [k.hat_notaufnahme, "NA"],
        [k.hat_bg_zulassung, "BG"],
        [k.hat_radiologie, "Röntgen"],
      ].filter(([on]) => on).map(([, l]) => `<span class="badge bg-light text-dark border">${l}</span>`).join(" ");
      return `
        <tr data-id="${k.id}">
          <td><span class="badge sk-badge ${skCls}">${escapeHtml(k.sk_max || "–")}</span></td>
          <td class="text-truncate" style="max-width: 260px;" title="${escapeHtml(k.name)}">${escapeHtml(k.name)}</td>
          <td>${escapeHtml(k.ort || "")}</td>
          <td class="text-end">${dist.toFixed(1)}</td>
          <td class="text-end">${k.betten ?? "–"}</td>
          <td><div class="d-flex gap-1 flex-wrap">${flagIcons}</div></td>
        </tr>`;
    }).join("");

    tbody.querySelectorAll("tr").forEach(tr => {
      tr.addEventListener("click", () => {
        const id = Number(tr.dataset.id);
        const m = state.markerById.get(id);
        if (m) {
          state.map.setView(m.getLatLng(), 13, { animate: true });
          m.openPopup();
        }
      });
    });
  }

  // ---- UI wiring ----
  function wireFilters(opts) {
    // Bundesland
    const bSel = document.getElementById("flt-bundesland");
    for (const b of opts.bundeslaender) {
      const o = document.createElement("option");
      o.value = b; o.textContent = b;
      bSel.appendChild(o);
    }
    // Trägerart
    const tSel = document.getElementById("flt-traeger");
    for (const t of opts.traeger_arten) {
      const o = document.createElement("option");
      o.value = t; o.textContent = t;
      tSel.appendChild(o);
    }
    // Max betten
    const bettenInput = document.getElementById("flt-betten");
    if (opts.max_betten) {
      bettenInput.max = Math.ceil(opts.max_betten / 100) * 100;
    }

    const bindChange = (sel, handler) => {
      document.querySelectorAll(sel).forEach(el => {
        el.addEventListener("change", handler);
        if (el.type === "range" || el.type === "text") el.addEventListener("input", handler);
      });
    };

    bindChange('[id^="flt-sk"]', e => {
      const sk = Number(e.target.dataset.sk);
      state.filters.sk[sk] = e.target.checked;
      applyFilters();
    });

    document.getElementById("flt-radius").addEventListener("input", e => {
      state.filters.radiusKm = Number(e.target.value);
      document.getElementById("flt-radius-val").textContent = e.target.value;
      applyFilters();
    });

    document.getElementById("flt-betten").addEventListener("input", e => {
      state.filters.minBetten = Number(e.target.value);
      document.getElementById("flt-betten-val").textContent = e.target.value;
      applyFilters();
    });

    document.getElementById("flt-bundesland").addEventListener("change", e => {
      state.filters.bundesland = e.target.value;
      applyFilters();
    });

    document.getElementById("flt-traeger").addEventListener("change", e => {
      state.filters.traegerArt = e.target.value;
      applyFilters();
    });

    document.getElementById("flt-uni").addEventListener("change", e => {
      state.filters.onlyUni = e.target.checked;
      applyFilters();
    });

    let searchTimer;
    document.getElementById("flt-search").addEventListener("input", e => {
      clearTimeout(searchTimer);
      searchTimer = setTimeout(() => {
        state.filters.search = e.target.value.trim();
        applyFilters();
      }, 150);
    });

    document.querySelectorAll("#flag-checks input[data-flag]").forEach(cb => {
      cb.addEventListener("change", e => {
        const flag = e.target.dataset.flag;
        if (e.target.checked) state.filters.flags.add(flag);
        else state.filters.flags.delete(flag);
        applyFilters();
      });
    });

    document.getElementById("btn-reset").addEventListener("click", () => {
      document.getElementById("flt-sk1").checked = true;
      document.getElementById("flt-sk2").checked = true;
      document.getElementById("flt-sk3").checked = true;
      document.getElementById("flt-radius").value = 100;
      document.getElementById("flt-radius-val").textContent = "100";
      document.getElementById("flt-betten").value = 0;
      document.getElementById("flt-betten-val").textContent = "0";
      document.getElementById("flt-bundesland").value = "";
      document.getElementById("flt-traeger").value = "";
      document.getElementById("flt-uni").checked = false;
      document.getElementById("flt-search").value = "";
      document.querySelectorAll("#flag-checks input[data-flag]").forEach(cb => (cb.checked = false));
      state.filters = {
        sk: { 1: true, 2: true, 3: true },
        radiusKm: 100, minBetten: 0,
        bundesland: "", traegerArt: "",
        onlyUni: false, flags: new Set(), search: "",
      };
      applyFilters();
    });
  }

  // ================= Dispatch / Simulation =================

  function wireDispatch() {
    const occSlider = document.getElementById("sim-occupancy");
    const occVal = document.getElementById("sim-occupancy-val");
    occSlider.addEventListener("input", e => { occVal.textContent = e.target.value; });

    document.getElementById("btn-sim-apply").addEventListener("click", async () => {
      const percent = Number(occSlider.value);
      setDispatchResult(`<span class="text-body-secondary">Setze Grundbelegung ${percent} %…</span>`);
      const resp = await fetch("/api/simulation/occupancy", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ percent }),
      });
      const j = await resp.json();
      renderBelegungTotals(j);
      setDispatchResult(`<span class="text-success"><i class="bi bi-check-circle"></i> Grundbelegung ${percent} % auf ${j.updated} Kliniken gesetzt.</span>`);
    });

    document.getElementById("btn-sim-reset").addEventListener("click", async () => {
      const resp = await fetch("/api/simulation/reset", { method: "POST" });
      const j = await resp.json();
      renderBelegungTotals(j);
      setDispatchResult(`<span class="text-body-secondary">Belegung zurückgesetzt (${j.cleared} Kliniken).</span>`);
    });

    // Dropzone
    const dz = document.getElementById("dropzone");
    const fi = document.getElementById("file-input");
    dz.addEventListener("click", () => fi.click());
    dz.addEventListener("dragover", e => { e.preventDefault(); dz.classList.add("dz-active"); });
    dz.addEventListener("dragleave", () => dz.classList.remove("dz-active"));
    dz.addEventListener("drop", e => {
      e.preventDefault(); dz.classList.remove("dz-active");
      if (e.dataTransfer.files.length) handleUpload(e.dataTransfer.files[0]);
    });
    fi.addEventListener("change", e => {
      if (e.target.files.length) handleUpload(e.target.files[0]);
    });

    document.getElementById("btn-dispatch").addEventListener("click", handleDispatch);
    document.getElementById("btn-batch-reset").addEventListener("click", handleBatchReset);
    document.getElementById("btn-toggle-routes").addEventListener("click", toggleRoutes);

    const btnManual = document.getElementById("btn-add-manual");
    if (btnManual) btnManual.addEventListener("click", handleManualAdd);

    // Initial totals
    fetch("/api/simulation/status").then(r => r.json()).then(renderBelegungTotals);
  }

  function renderBelegungTotals(t) {
    if (!t || !t.kapazitaet) return;
    const el = document.getElementById("sim-totals");
    const c = t.kapazitaet, b = t.belegung, f = t.frei;
    if (el) {
      el.innerHTML = `
        <div>Frei: <strong>${(f.sk1 || 0).toLocaleString("de-DE")}</strong> SK1 ·
                    <strong>${(f.sk2 || 0).toLocaleString("de-DE")}</strong> SK2 ·
                    <strong>${(f.sk3 || 0).toLocaleString("de-DE")}</strong> SK3</div>
        <div class="text-body-secondary">Belegt/Gesamt:
          ${(b.sk1 || 0)}/${(c.sk1 || 0)} ·
          ${(b.sk2 || 0)}/${(c.sk2 || 0)} ·
          ${(b.sk3 || 0)}/${(c.sk3 || 0)}
        </div>`;
    }
    // KPI-Card "Freie Betten"
    const kpiFrei = document.getElementById("kpi-frei");
    if (kpiFrei) {
      const totalFrei = (f.sk1 || 0) + (f.sk2 || 0) + (f.sk3 || 0);
      kpiFrei.textContent = totalFrei.toLocaleString("de-DE");
    }
  }

  function renderKPIs() {
    // Kliniken-KPI
    const el = document.getElementById("kpi-kliniken");
    if (el) {
      const sk1 = state.kliniken.filter(k => k.kann_sk1).length;
      const sk2 = state.kliniken.filter(k => k.kann_sk2).length;
      const sk3 = state.kliniken.filter(k => k.kann_sk3).length;
      el.textContent = state.kliniken.length.toLocaleString("de-DE");
      document.getElementById("kpi-sk1").textContent = sk1;
      document.getElementById("kpi-sk2").textContent = sk2;
      document.getElementById("kpi-sk3").textContent = sk3;
    }
  }

  function renderBatchKPI(batch, transports) {
    const t = document.getElementById("kpi-transports");
    if (t) t.textContent = (transports?.length || 0).toLocaleString("de-DE");
    const sub = document.getElementById("kpi-transport-sub");
    if (sub) sub.textContent = transports?.length ? "geplant · SK1→SK2→SK3" : "noch keine";

    const bt = document.getElementById("kpi-batch-total");
    const bs = document.getElementById("kpi-batch-sub");
    if (batch && bt && bs) {
      bt.textContent = batch.total;
      bs.innerHTML = `SK1: ${batch.sk1} · SK2: ${batch.sk2} · SK3: ${batch.sk3}`;
    }
  }

  /** Lädt bereits vorhandene Batches/Fahrten beim Dashboard-Start und
   *  aktualisiert damit die KPI-Kacheln + Karten-Routes. */
  async function loadExistingActivity() {
    try {
      const [batches, fahrten] = await Promise.all([
        fetch("/api/batches").then(r => r.json()),
        fetch("/api/fahrten").then(r => r.json()),
      ]);

      // KPI: Transportaufträge = Summe aller Fahrten
      const tEl = document.getElementById("kpi-transports");
      const sEl = document.getElementById("kpi-transport-sub");
      if (tEl) tEl.textContent = fahrten.length.toLocaleString("de-DE");
      if (sEl) {
        const byVehicle = fahrten.reduce((acc, f) => {
          acc[f.transportmittel] = (acc[f.transportmittel] || 0) + 1;
          return acc;
        }, {});
        const parts = ["RTW", "KTW", "BTW", "Taxi"]
          .map(v => byVehicle[v] ? `${byVehicle[v]} ${v}` : null)
          .filter(Boolean);
        sEl.textContent = fahrten.length ? parts.join(" · ") : "noch keine";
      }

      // KPI: aktueller Batch = neuester wartend, sonst neuester verteilt
      const pending = batches.find(b => b.status === "uploaded");
      const latest = pending || batches[0];
      const bt = document.getElementById("kpi-batch-total");
      const bs = document.getElementById("kpi-batch-sub");
      if (bt && bs) {
        if (latest) {
          bt.textContent = latest.total;
          const statusLabel = latest.status === "dispatched" ? "verteilt" : "wartet auf Verteilung";
          bs.innerHTML = `${latest.filename} — SK1:${latest.sk1} · SK2:${latest.sk2} · SK3:${latest.sk3} <span class="text-body-secondary">· ${statusLabel}</span>`;
        } else {
          bt.textContent = "—";
          bs.textContent = "kein Batch aktiv";
        }
      }

      // Letzten wartenden Batch automatisch als Dispatch-Target übernehmen,
      // damit der "Verteilen"-Button funktioniert.
      if (pending) {
        state.batch = {
          batch_id: pending.id, filename: pending.filename, hub: pending.hub_name,
          total: pending.total, sk1: pending.sk1, sk2: pending.sk2, sk3: pending.sk3,
        };
        showBatchPanel(state.batch);
      } else if (latest && latest.status === "dispatched") {
        // Für die Transporte-Tabelle: letzten dispatched Batch laden
        state.batch = {
          batch_id: latest.id, filename: latest.filename, hub: latest.hub_name,
          total: latest.total, sk1: latest.sk1, sk2: latest.sk2, sk3: latest.sk3,
        };
        // Transporte laden für die Map-Routes + Tabelle
        const resp = await fetch(`/api/transports?batch_id=${latest.id}`);
        state.transports = await resp.json();
        renderTransportTable();
      }
    } catch (err) {
      console.warn("loadExistingActivity failed:", err);
    }
  }

  function setDispatchResult(html) {
    document.getElementById("dispatch-result").innerHTML = html;
  }

  async function handleManualAdd() {
    const statusEl = document.getElementById("man-status");
    const id = document.getElementById("man-id").value.trim();
    const sk = document.querySelector('input[name="man-sk"]:checked')?.value;
    if (!sk) return;
    statusEl.innerHTML = `<span class="text-body-secondary"><i class="bi bi-hourglass-split"></i> Speichere…</span>`;
    try {
      const r = await fetch("/api/patients/manual", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sk, external_id: id || null, hub_name: state.hub?.name }),
      });
      const j = await r.json();
      if (!r.ok) throw new Error(j.error || "Fehler");
      statusEl.innerHTML = `<span class="text-success"><i class="bi bi-check-circle"></i> ${j.external_id} erfasst (Batch #${j.batch_id}, ${j.batch_total} insg.)</span>`;
      // Batch-Panel für diesen Manuell-Batch zeigen
      state.batch = { batch_id: j.batch_id, filename: `Manuell (${j.batch_total})`, hub: state.hub?.name, total: j.batch_total, sk1: 0, sk2: 0, sk3: 0 };
      // Neu laden für korrekte Counts
      const list = await fetch("/api/batches").then(r => r.json());
      const b = list.find(x => x.id === j.batch_id);
      if (b) state.batch = { batch_id: b.id, filename: b.filename, hub: b.hub_name, total: b.total, sk1: b.sk1, sk2: b.sk2, sk3: b.sk3 };
      showBatchPanel(state.batch);
      document.getElementById("man-id").value = "";
    } catch (err) {
      statusEl.innerHTML = `<span class="text-danger">${escapeHtml(err.message)}</span>`;
    }
  }

  async function handleUpload(file) {
    const dzStatus = document.getElementById("dropzone-status");
    dzStatus.innerHTML = `<span class="text-body-secondary"><i class="bi bi-hourglass-split"></i> Lade ${file.name}…</span>`;
    const form = new FormData();
    form.append("file", file);
    form.append("hub", state.hub?.name || "Hub Süd");
    try {
      const resp = await fetch("/api/batch/upload", { method: "POST", body: form });
      const j = await resp.json();
      if (!resp.ok) throw new Error(j.error || "Upload fehlgeschlagen");
      state.batch = j;
      dzStatus.innerHTML = `<span class="text-success"><i class="bi bi-check-circle"></i> ${j.total} Patienten geladen</span>`;
      showBatchPanel(j);
    } catch (err) {
      dzStatus.innerHTML = `<span class="text-danger">${escapeHtml(err.message)}</span>`;
    }
  }

  function showBatchPanel(b) {
    document.getElementById("batch-panel").classList.remove("d-none");
    document.getElementById("batch-filename").textContent = b.filename || "Upload";
    document.getElementById("batch-summary").innerHTML =
      `${b.total} Patienten — <span class="text-danger">SK1: ${b.sk1}</span> · ` +
      `<span style="color:#f57c00">SK2: ${b.sk2}</span> · SK3: ${b.sk3} · Hub: ${escapeHtml(b.hub || "")}`;
    renderBatchKPI(b, state.transports);
  }

  async function handleDispatch() {
    if (!state.batch) return;
    const btn = document.getElementById("btn-dispatch");
    btn.disabled = true;
    btn.innerHTML = `<span class="spinner-border spinner-border-sm"></span> Verteile…`;
    try {
      const resp = await fetch(`/api/batch/${state.batch.batch_id}/dispatch`, { method: "POST" });
      const j = await resp.json();
      if (!resp.ok) throw new Error(j.error || "Dispatch fehlgeschlagen");
      setDispatchResult(`
        <div class="alert alert-success py-2 small mb-0">
          <i class="bi bi-check-circle-fill"></i> Verteilung abgeschlossen<br>
          <strong>${j.assigned}</strong> zugewiesen · <strong>${j.unassigned}</strong> offen<br>
          Ø Entfernung: <strong>${j.avg_distanz_km || "–"} km</strong>
        </div>`);
      await loadTransports();
      fetch("/api/simulation/status").then(r => r.json()).then(renderBelegungTotals);
      loadExistingActivity();
      // Tab auf Transporte
      new bootstrap.Tab(document.querySelector('[data-bs-target="#tab-transporte"]')).show();
    } catch (err) {
      setDispatchResult(`<div class="alert alert-danger py-2 small mb-0">${escapeHtml(err.message)}</div>`);
    } finally {
      btn.disabled = false;
      btn.innerHTML = `<i class="bi bi-send-check"></i> Verteilen`;
    }
  }

  async function handleBatchReset() {
    if (!state.batch) return;
    await fetch(`/api/batch/${state.batch.batch_id}/reset`, { method: "POST" });
    state.transports = [];
    renderTransportTable();
    clearRoutes();
    fetch("/api/simulation/status").then(r => r.json()).then(renderBelegungTotals);
    setDispatchResult(`<span class="text-body-secondary">Zuweisung zurückgesetzt.</span>`);
  }

  async function loadTransports() {
    if (!state.batch) return;
    const resp = await fetch(`/api/transports?batch_id=${state.batch.batch_id}`);
    state.transports = await resp.json();
    renderTransportTable();
    renderBatchKPI(state.batch, state.transports);
    if (state.showRoutes) renderRoutes();
  }

  function renderTransportTable() {
    const tbody = document.querySelector("#transport-table tbody");
    document.getElementById("tab-transport-count").textContent = state.transports.length;
    if (!state.transports.length) {
      tbody.innerHTML = `<tr><td colspan="7" class="text-center text-body-secondary py-3">Noch keine Transportaufträge</td></tr>`;
      return;
    }
    tbody.innerHTML = state.transports.map(t => {
      const skCls = "sk" + (t.sk || "").replace("SK", "");
      return `
        <tr data-kh="${t.ziel.id}">
          <td><span class="badge sk-badge ${skCls}">${escapeHtml(t.sk || "")}</span></td>
          <td>
            <a href="/patients/${t.patient_id}" class="font-monospace small text-decoration-none" onclick="event.stopPropagation()">
              ${escapeHtml(t.patient_external_id || ("#" + t.patient_id))}
            </a>
          </td>
          <td class="text-truncate" style="max-width: 240px;" title="${escapeHtml(t.ziel.name || "")}">
            <a href="/krankenhaus/${t.ziel.id}" class="text-decoration-none text-body" onclick="event.stopPropagation()">
              ${escapeHtml(t.ziel.name || "")}
            </a>
          </td>
          <td>${escapeHtml(t.ziel.ort || "")}</td>
          <td class="text-end">${t.distanz_km ? t.distanz_km.toFixed(1) : "–"}</td>
          <td class="text-end">${t.dauer_min ? Math.round(t.dauer_min) : "–"}</td>
          <td>
            <a href="/transports/${t.id}" class="badge bg-danger text-decoration-none" onclick="event.stopPropagation()">
              <i class="bi bi-file-earmark-text"></i> #${t.id}
            </a>
          </td>
        </tr>`;
    }).join("");

    tbody.querySelectorAll("tr[data-kh]").forEach(tr => {
      tr.addEventListener("click", () => {
        const id = Number(tr.dataset.kh);
        const m = state.markerById.get(id);
        if (m) {
          state.map.setView(m.getLatLng(), 12, { animate: true });
          m.openPopup();
        }
      });
    });
  }

  function toggleRoutes() {
    state.showRoutes = !state.showRoutes;
    const btn = document.getElementById("btn-toggle-routes");
    if (state.showRoutes) {
      renderRoutes();
      btn.classList.add("active");
    } else {
      clearRoutes();
      btn.classList.remove("active");
    }
  }

  function renderRoutes() {
    clearRoutes();
    if (!state.transports.length) return;
    const layer = L.layerGroup().addTo(state.map);
    for (const t of state.transports) {
      if (!t.hub.lat || !t.ziel.lat) continue;
      const color = t.sk === "SK1" ? "#b71c1c" : t.sk === "SK2" ? "#f57c00" : "#fbc02d";
      L.polyline([[t.hub.lat, t.hub.lon], [t.ziel.lat, t.ziel.lon]], {
        color, weight: 2, opacity: 0.55, dashArray: "4,4",
      }).addTo(layer);
    }
    state.routesLayer = layer;
  }

  function clearRoutes() {
    if (state.routesLayer) {
      state.map.removeLayer(state.routesLayer);
      state.routesLayer = null;
    }
  }

  // ---- init ----
  async function init() {
    try {
      const opts = await loadData();
      if (!state.hub) {
        console.error("Kein Hub in DB.");
        return;
      }
      initMap();
      buildAllMarkers();
      wireFilters(opts);
      wireDispatch();
      wireMapModeToggle();
      applyFilters();
      loadExistingActivity();
      loadOccupancy();
      // Bei Dispatch/Reset automatisch Belegung neu laden, zusätzlich alle 30s Polling
      setInterval(loadOccupancy, 30000);
    } catch (err) {
      console.error("Dashboard init failed:", err);
      alert("Fehler beim Laden der Daten. Siehe Konsole.");
    }
  }

  document.addEventListener("DOMContentLoaded", init);
})();
