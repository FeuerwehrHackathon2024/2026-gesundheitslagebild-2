# 2026-gesundheitslagebild — MANV-Dispatch

Prototyp aus dem Feuerwehr-Hackathon 2026: Verteilung von Verletzten aus
Hub Süd (Ulm) auf Krankenhäuser unter Berücksichtigung von Sichtungs-
kategorien (SK1–3), Versorgungsfähigkeit und Bettenbelegung.

---

## Flask + SQLAlchemy Skeleton

This project now follows a generic Flask app-factory structure with SQLAlchemy and Flask-Migrate.

## Project layout

- `app.py`: thin entrypoint for local runs.
- `hackathon/__init__.py`: app factory (`create_app`).
- `hackathon/config.py`: central config class.
- `hackathon/extensions.py`: extension instances (`db`, `migrate`).
- `hackathon/models.py`: SQLAlchemy models.
- `hackathon/routes.py`: blueprint routes.
- `templates/`, `static/`: basic UI assets.
- `smoke_test.py`: tiny runtime check.

## Quick start

```bash
poetry install
poetry run python app.py
```

## Initialize database migrations

```bash
poetry run flask --app app db init
poetry run flask --app app db migrate -m "initial"
poetry run flask --app app db upgrade
```

## Run smoke test

```bash
poetry run python smoke_test.py
```

---

## Kontext: MANV-Dispatch-Prototyp

Verteilung von Verletzten aus **Hub Süd** auf Krankenhäuser mit 72-h-Vorlauf.

- ~250 Verletzte/Tag am Hub
- 72 h Vorwarnung vor Ankunft
- Triage nach SK1–3 (Sichtungskategorien):
  - **SK1** = vital gefährdet (Schockraum/OP/Intensivmedizin)
  - **SK2** = schwer verletzt, stabil (Notaufnahme/OP)
  - **SK3** = leicht verletzt (Grundversorgung)
- Leitstelle priorisiert Transporte, Kliniken melden freie Kapazität

## Daten

Hauptdatei: **`data/krankenhaeuser_merged.csv`** — 4.420 Kliniken · 69 Spalten

| Block | Felder (Auszug) |
|---|---|
| Stamm | `ik`, `standortnummer`, `name`, `chain_name` |
| Adresse + Geo | `strasse`, `plz`, `ort`, `bundesland`, `lat`, `lon` |
| Kontakt | `telefon`, `email`, `website` |
| Kapazität | `betten`, `vollstationaere_fallzahl`, `ambulante_fallzahl` |
| Versorgung | `hat_intensivmedizin`, `hat_notaufnahme`, `hat_bg_zulassung`, `hat_radiologie`, ... |
| Fachabteilungen | `fachabteilungen`, `apparative_ausstattung` |
| **SK-Klassifikation** | `kann_sk1`, `kann_sk2`, `kann_sk3`, `sk_max`, `sk_begruendung` |
| **SK-Kapazität (Schätzung)** | `kapazitaet_sk1_geschaetzt`, `kapazitaet_sk2_geschaetzt`, `kapazitaet_sk3_geschaetzt` |
| Meta | `merge_source`, `source`, `importance_score`, `priority_bucket` |

### SK-Verteilung

| Stufe | Kliniken | Betten (geschätzt) |
|---|---|---|
| SK1 | 1.120 | ~19.700 |
| SK2 | 1.765 | ~144.000 |
| SK3 | 3.960 | ~208.000 |

Herkunft (`merge_source`): `csv+db` (1.562) · `db_only` (2.200) · `csv_only` (658)

### Datenquellen

1. `data/krankenhaus_daten_2024.csv` — amtlicher Qualitätsbericht 2024 (2.273 Häuser mit IK)
2. `data/krankenhaeuser_db.csv` — Export aus interner Markt-DB mit Geocoding + Scoring (3.728 Häuser, nicht versioniert)
3. `scripts/merge_krankenhaeuser.py` — Merge per `(ik, standortnummer)`
4. `scripts/add_sk_capability.py` — SK1/2/3-Ableitung aus Flags + Fachabteilungen + Uniklinik-Indikatoren

## Infrastruktur

- **Team-VM**: `185.181.169.154` · user `ubuntu` · Ubuntu 24.04 · 4 CPU / 8 GB RAM
- Zugang über SSH-Key `team01` (nicht im Repo, gesichert beim Teamleiter)
