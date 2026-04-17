# MANV-Dispatch · Dokumentation & Demo-Guide

> **Feuerwehr Hackathon 2026 · Team 01 · "Gesundheitslagebild"**
> Verteilung von ~250 Verletzten/Tag aus einem Hub auf Krankenhäuser
> mit Live-Kapazitätsbild, SK-Triage-Logik und intelligenter Transport-Bündelung.

**Live-System:** http://185.181.169.154
**Repo:** https://github.com/FeuerwehrHackathon2024/2026-gesundheitslagebild-2

---

## 1. Problem & Idee

- Im MANV-Fall (Massenanfall von Verletzten) kommen **~250 Verletzte/Tag** an einem
  zentralen **Hub Süd (Ulm)** an.
- 72 h Vorwarnung — die Leitstelle muss **verteilen, transportieren, koordinieren**.
- **Triage nach SK1/SK2/SK3** (vital / schwer stabil / leicht).
- Aufenthaltsdauer: SK1 = 5 Tage, SK2 = 3 Tage, SK3 = 1 Tag.

**Ziel:** Eine Software, die Leitstelle + Rettungsdienst + Krankenhäuser
koordiniert, Kapazitäten und Fahrten in Echtzeit abbildet und das Lagebild
visualisiert.

---

## 2. Datenbasis

| Quelle | Inhalt | Menge |
|---|---|---|
| Amtlicher Qualitätsbericht 2024 | Strukturdaten mit IK, Fachabteilungen, Notfallstufe | 2.273 Zeilen |
| QraGo-DB (Google Places + LLM-Enrichment) | Geocoding, Kapazität, Importance-Score | 3.728 Zeilen |
| **Dedup-Pipeline** | Geo-Matching + Name-Normalisierung + MANV-Filter | **2.081 eindeutige Akut-Kliniken** |
| IVENA-Beispiel-XLSX | 124 Patienten aus Live-System (Brand-Fall 5.3.2026) | 124 |

Dedup-Script: `scripts/deduplicate_krankenhaeuser.py`
Ausgabe: `data/krankenhaeuser_dedup.csv` (wird beim App-Start automatisch geseeded).

---

## 3. Architektur

```
┌──────────────────────────────────────────────────────────────┐
│                    Browser (Bootstrap + Leaflet + Chart.js)  │
└──────────────────────────────────────────────────────────────┘
                           │ HTTPS/HTTP
┌──────────────────────────▼──────────────────────────┐
│  Flask 3 + SQLAlchemy + Gunicorn (4 Worker)         │
│  ┌─────────────────────────────────────────────┐    │
│  │ Routes: /, /patients, /fahrten, /krankenhaus│    │
│  │         /belegung, /adt, /timecapsule, /traffic │
│  │ API:    /api/* (JSON)                       │    │
│  │ Dispatch-Engine │ HL7-Ingest │ Time-Capsule │    │
│  └─────────────────────────────────────────────┘    │
└─────────────┬───────────────────────┬────────────────┘
              │                       │
      ┌───────▼─────────┐   ┌─────────▼─────────────┐
      │ Postgres 16      │   │ HERE Maps API v7/v8   │
      │ (Docker-Volume)  │   │  - Routing (für Fahrten)
      │                  │   │  - Traffic Flow       │
      └──────────────────┘   │  - Incidents          │
                             └───────────────────────┘
```

**Stack**: Python 3.12, Flask, SQLAlchemy, Postgres, Docker-Compose,
Bootstrap 5, Leaflet + MarkerCluster, Chart.js, ReportLab (PDF),
openpyxl (XLSX), psycopg (Postgres), requests (HERE), flexpolyline.

---

## 4. Feature-Matrix (wann welcher Meilenstein)

| Bereich | Seite(n) | Kurzbeschreibung |
|---|---|---|
| **Datenpipeline** | — | 2.273 + 3.728 → Dedup → 2.081 Kliniken mit Geo + SK-Flags |
| **Dashboard** | `/` | Deutschland-Karte (MarkerCluster), Filter-Sidebar, KPI-Cards, Dispatch-Panel |
| **Karten-Filter** | `/` | SK-Stufe, Radius 10–1000 km, Bundesland, Trägerart, 8 Versorgungs-Flags, Uni-Toggle, Suche |
| **Karten-Belegung** | `/` | Toggle oben rechts: Farben nach **SK-Stufe** ↔ **Live-Auslastung** (grün/gelb/orange/rot) |
| **Klinik ausschließen** | Popup im Dashboard | Mit Grund-Eingabe, Marker wird schwarz mit ✕, Dispatch ignoriert |
| **Simulator** | `/simulator` | XLSX-Generator im IVENA-Format, **ODER** „Direkt laden + verteilen" |
| **XLSX-Upload** | `/` Dispatch-Panel | Dropzone für IVENA-Listen + manuelle Patientenerfassung |
| **Belegungs-Simulation** | `/` Dispatch-Panel | Slider 0–100 % Grundbelegung, per Klinik randomisiert |
| **Verteilungs-Engine** | `/api/batch/<id>/dispatch` | SK1→SK2→SK3, SK-Kompatibilität, Expanding-Radius (10/30/50/70/100/…) |
| **Fahrzeug-Bündelung** | Dispatch | RTW=1, KTW=1, BTW=2, Taxi=2 — pro (Klinik, Mittel) werden Chunks gebildet |
| **HERE-Routing** | Dispatch | 1 API-Call pro Ziel-Klinik, Ergebnis für alle Fahrten dorthin geteilt |
| **IVENA-Matching** | `/ivena-matching` | Referenztabelle SK ↔ MANV-Kategorie ↔ Farbe ↔ T-Code ↔ Transportmittel |
| **Patienten** | `/patients` · `/patients/<id>` | Liste (Filter + CSV-Export) + Detailansicht (Timeline, Ziel, Karte) |
| **Fahrten** | `/fahrten` · `/fahrten/<id>` | Übersicht pro Fahrzeug, Detail mit Route, Fahrtanweisungen, Status-Workflow |
| **Transportauftrag** | `/transports/<id>` | Einzel-Detail mit PDF-Export, Druck, Share-Link |
| **Klinik-Übersicht** | `/krankenhaeuser` | Card-Grid aller Kliniken, klickbar, mit Live-Auslastungs-Badge |
| **Klinik-Sicht** | `/krankenhaus/<id>` | Info + Belegungs-Bars + **eingehende Patienten nach Ankunftszeit** (für Empfangs-Team, druckbar) |
| **Belegung (aggregiert)** | `/belegung` | Tabelle Grund- vs. Dispatch-Belegung + Auslastungs-% pro Klinik |
| **ADT Live-Feed** | `/adt` | HL7 v2.5 A01/A03/A08-Events, Simulator-Buttons, Live-Tabelle (Polling 2,5 s) |
| **HL7-Generator-Script** | `scripts/hl7_adt_generator.py` | CLI: `--event A01 -n 50 --post URL` — simuliert externen Connector (i-engineers-Stil) |
| **HL7-Parser** | `hackathon/hl7_ingest.py` | Akzeptiert PV1-10 **und** PV1-19 SK, FA-Code-Mapping (ITS→SK1, UNF→SK2, ORT→SK3 …) |
| **Time-Capsule** | `/timecapsule` | Mehrtägige Simulation (1-14 Tage), stündliche Snapshots, Chart.js-Timeline, Tages-Kacheln, Top-Kliniken, Radius-Ring-Nutzung |
| **Verkehrslage** | `/traffic` | HERE Flow + Incidents (Baustellen/Unfälle/Sperrungen) als Karten-Overlay mit Jam-Factor-Farben |
| **Verläufe** | `/batches` | Alle Uploads/Simulationen, Status, Löschen |
| **Globaler Reset** | Navbar-Button | Löscht Patienten / Fahrten / ADT-Events / Belegung |

---

## 5. Die wichtigsten Zahlen für die Demo

- **Kliniken:** 2.081 (davon 2.046 geokodiert)
- **SK1-fähig:** 778 · **SK2-fähig:** 1.268 · **SK3-fähig:** 2.046
- **Geschätzte Gesamtkapazität:** ~19.700 SK1 · ~144.000 SK2 · ~208.000 SK3
- **Test-Run** (IVENA-Datei, 124 Patienten): 124 zugewiesen, 0 offen, Ø 0,5 km
- **Bündelung:** 124 Pat → **84 Fahrten** (RTW 11 · KTW 31 · Taxi 42 mit je 2 Pat)
- **HERE-Calls:** statt 124 × nur ~20 (einmal pro Ziel-Klinik, Bündel-Caching)
- **Time-Capsule** (7 Tage, 250 Pat/Tag, Bayern): Peak-Auslastung SK1 ~70 %, stündliche Snapshots

---

## 6. Demo-Drehbuch (20 Minuten)

### Schritt 1 – Problem einordnen (2 Min)
1. `/info` öffnen: Projekt-Überblick, Regeln, Datenbasis
2. Kurzer Ausflug in `/ivena-matching`: SK↔IVENA↔Transportmittel

### Schritt 2 – Daten & Karte (3 Min)
1. `/krankenhaeuser` — 2.081 Kliniken, Filter auf Bayern + SK1-fähig → ~150
2. Klick auf eine Klinik → `/krankenhaus/<id>` mit Belegung + eingehende Patienten
3. Zurück auf `/` — Karte mit Cluster, Filter live (z. B. Radius 50 km, nur SK1)

### Schritt 3 – Leitstellen-Workflow (5 Min)
1. Im Dispatch-Panel:
   - Slider auf **60 % Grundbelegung** → Anwenden (Karte-Modus "Belegung" umschalten → einige Kliniken werden orange/rot)
   - Option A: IVENA-XLSX (`data/IVENA_Fw_Hackathon.xlsx`) in die Dropzone ziehen
   - Option B: Simulator `/simulator` → *Preset 250/Tag* → **"Direkt laden + verteilen"**
2. Verteilen drücken → Toast: "124 zugewiesen, Ø 0,5 km, 84 Fahrten"
3. Tab "Transportaufträge" → Liste, Routen auf Karte einblenden
4. Klick auf eine Zeile → `/transports/<id>` → **PDF-Export** demonstrieren

### Schritt 4 – Fahrten & Status (2 Min)
1. `/fahrten` — Gebündelte Fahrten, KPI-Cards (RTW/KTW/BTW/Taxi)
2. In Detail einer Fahrt → Status klicken „Unterwegs" → „Angekommen"
3. Patient darin öffnen → `/patients/<id>` → Timeline Sichtung → Transport → Entlassung

### Schritt 5 – Empfangs-Sicht (1 Min)
1. `/krankenhaus/<id>` einer eingehenden Klinik → "Welche Patienten kommen wann?"
2. Druck-Button → druckfreundliche Liste fürs Empfangs-Team

### Schritt 6 – Live-ADT-Feed (3 Min)
1. `/adt` → Simulator-Button "50× Aufnahme (A01)" → Live-Tabelle füllt sich
2. "50× Entlassung (A03)" → Belegung sinkt sofort
3. Zurück zu `/` → Karte-Modus "Belegung" → Klinik-Marker haben neue Farben
4. Erklärung: externer Connector (i-engineers) kann mit
   `python3 scripts/hl7_adt_generator.py --post http://185.181.169.154/api/adt/ingest`
   direkt einspeisen

### Schritt 7 – Time-Capsule (2 Min)
1. `/timecapsule` → 7 Tage · 250 Pat/Tag · Bayern · 60 % Grundbelegung
2. Starten → nach ~20 s sieht man: Tages-Kacheln mit Peak-Auslastungen,
   %-Chart, Top-Kliniken-Tabelle, Radius-Ring-Verteilung
3. Scrubber anwerfen → Animation zeigt Entwicklung über die Tage

### Schritt 8 – Verkehrslage (1 Min)
1. `/traffic` → Zentrum Ulm, Radius 5 km Flow + 15 km Incidents
2. Baustellen und Jam-Factor live von HERE
3. Kombination: bei Dispatch-Routing berücksichtigt HERE die aktuelle Lage

### Schritt 9 – Abschluss (1 Min)
1. Navbar → **Reset** (Knopf mit Warning-Icon rechts oben)
2. Reset: alles zurück auf Grundbelegung
3. Bereit für nächsten Einsatz

---

## 7. Externe Simulation / Integration

### HL7 ADT-Generator (CLI)

```bash
# 50 Aufnahmen an VM senden
python3 scripts/hl7_adt_generator.py --event A01 -n 50 \
  --post http://185.181.169.154/api/adt/ingest

# Kontinuierliche Last simulieren (1 msg/s)
python3 scripts/hl7_adt_generator.py --mix -n 500 --rate 1 \
  --post http://185.181.169.154/api/adt/ingest
```

### Nachrichten-Format (HL7 v2.5)
```
MSH|^~\&|MANV-SIM|BWKUlm^Bundeswehrkrankenhaus|MANV-DISPATCH|…|ADT^A01|…
EVN|A01|20260417150000
PID|1||P123456^^^^PI||Müller-0042^Max||19800505|M
PV1|1|I|ITS^Bett12^Z1|…|SK1|…|20260417150000|
```
- SK wird aus **PV1-19** (unser Generator) oder **PV1-10** (Kollegen-Script) gelesen
- FA-Codes werden über Mapping-Tabelle (`ivena_mapping.py`) zu MANV-SK übersetzt

---

## 8. Deployment (VM 185.181.169.154)

```bash
ssh ubuntu@185.181.169.154
cd ~/hackathon_krankenhaus
cat .env              # SECRET_KEY, POSTGRES_*, HERE_API_KEY
sudo docker compose up -d --build
sudo docker compose logs -f web
```

Services: **web** (Gunicorn, Port 80) + **db** (Postgres 16) + Auto-Seed
(2.081 Kliniken + Hub "Hub Süd" in Ulm) beim ersten Start.

**.env-Variablen:**
- `SECRET_KEY` — Flask-Sessions
- `POSTGRES_USER / PASSWORD / DB`
- `HERE_API_KEY` — Routing + Traffic (optional; fallback = Haversine)
- `WEB_PORT` — Host-Port (default 80)

---

## 9. Was aktuell nicht läuft / bewusst nicht im Scope

- **Auth / Mehrbenutzer** — kein Login, alle Sessions gleichberechtigt (Hackathon-Prototyp)
- **Echtzeit-Push** (WebSocket/SSE) — Map-Refresh erfolgt per 30-s-Polling
- **Mehrere Hubs** — aktuell nur "Hub Süd", Architektur unterstützt es aber
- **Kapazitäts-Override pro Station** — nur komplette Klinik kann ausgeschlossen werden
- **Real-Traffic in Dispatch-Entscheidung** — Traffic-Layer visualisiert, beeinflusst aber die Auswahl nicht automatisch

## 10. Kontakt & Quellen

- Amtlicher Qualitätsbericht 2024 (Bundesweites Krankenhausverzeichnis)
- HERE Developer (Routing v8, Traffic v7)
- IVENA: https://www.ivena-hessen.de
- Team-VM: `185.181.169.154` — SSH-Key `team01` (nicht im Repo)
