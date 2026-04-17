/* MANV-Dispatch Dashboard
 * Leaflet map + Bootstrap filter UI for Krankenhäuser
 */
(function () {
  "use strict";

  const SK_COLORS = { SK1: "#b71c1c", SK2: "#f57c00", SK3: "#fbc02d", keine: "#757575" };

  const state = {
    hub: null,
    kliniken: [],
    map: null,
    cluster: null,
    hubMarker: null,
    hubRadiusCircle: null,
    markerById: new Map(),
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

  function makeMarker(k) {
    const sk = skClass(k.sk_max);
    const label = (k.sk_max || "").replace("SK", "") || "–";
    const icon = L.divIcon({
      className: "",
      html: `<div class="kh-pin ${sk}">${label}</div>`,
      iconSize: [22, 22],
      iconAnchor: [11, 11],
    });
    const marker = L.marker([k.lat, k.lon], { icon });
    marker.bindPopup(() => popupHtml(k), { maxWidth: 340 });
    marker._kh = k;
    return marker;
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

    return `
      <div class="kh-popup">
        <h6>${escapeHtml(k.name)}</h6>
        <div class="popup-meta">${escapeHtml(addr)}${addr && place ? ", " : ""}${escapeHtml(place)}</div>
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
      </div>`;
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
      applyFilters();
    } catch (err) {
      console.error("Dashboard init failed:", err);
      alert("Fehler beim Laden der Daten. Siehe Konsole.");
    }
  }

  document.addEventListener("DOMContentLoaded", init);
})();
