"""Seed der Krankenhaus-Tabelle aus data/krankenhaeuser_merged.csv."""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import inspect
from sqlalchemy.exc import OperationalError, ProgrammingError, DatabaseError

from .config import BASE_DIR
from .extensions import db
from .models import Hub, Krankenhaus

log = logging.getLogger(__name__)

_DEDUP_PATH = BASE_DIR / "data" / "krankenhaeuser_dedup.csv"
_MERGED_PATH = BASE_DIR / "data" / "krankenhaeuser_merged.csv"
# Bevorzuge dedup-CSV wenn vorhanden (2.081 echte Akut-Kliniken),
# Fallback auf ungemerged (4.420 Zeilen, enthält Duplikate).
CSV_PATH = _DEDUP_PATH if _DEDUP_PATH.exists() else _MERGED_PATH

_TRUE = {"true", "ja", "1", "t", "yes", "y"}
_FALSE = {"false", "nein", "0", "f", "no", "n"}
_NULLISH = {"", "nan", "none", "null", "na"}


def _clean(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if s.lower() in _NULLISH:
        return None
    return s


def _as_bool(v: Any) -> bool | None:
    s = _clean(v)
    if s is None:
        return None
    low = s.lower()
    if low in _TRUE:
        return True
    if low in _FALSE:
        return False
    return None


def _as_int(v: Any) -> int | None:
    s = _clean(v)
    if s is None:
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _as_float(v: Any) -> float | None:
    s = _clean(v)
    if s is None:
        return None
    try:
        return float(s)
    except ValueError:
        return None


_COLUMN_CASTERS = {
    col.name: (
        _as_bool if isinstance(col.type, db.Boolean)
        else _as_int if isinstance(col.type, db.Integer)
        else _as_float if isinstance(col.type, db.Float)
        else _clean
    )
    for col in Krankenhaus.__table__.columns
}


def _row_from_csv(raw: dict[str, str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for col_name, cast in _COLUMN_CASTERS.items():
        if col_name in raw:
            out[col_name] = cast(raw[col_name])
    return out


def _valid_rows(source: Iterable[dict[str, str]]) -> Iterable[dict[str, Any]]:
    # IK ist nicht PK (db_only-Häuser haben keine). Dedupe nur wenn IK vorhanden.
    seen_ik: set[tuple[str, str | None]] = set()
    for raw in source:
        row = _row_from_csv(raw)
        if not row.get("name"):
            continue
        ik = row.get("ik")
        if ik:
            key = (ik, row.get("standortnummer"))
            if key in seen_ik:
                continue
            seen_ik.add(key)
        yield row


def _ensure_table_exists() -> None:
    """Legt die Tabelle an wenn sie fehlt (Fallback für Setup ohne Migration)."""
    inspector = inspect(db.engine)
    if not inspector.has_table(Krankenhaus.__tablename__):
        log.info("Tabelle '%s' existiert nicht — lege sie an.", Krankenhaus.__tablename__)
        db.create_all()


def seed_krankenhaus(force: bool = False, csv_path: Path | None = None) -> int:
    """Importiert die Krankenhäuser. Gibt die Anzahl eingefügter Zeilen zurück."""
    path = csv_path or CSV_PATH
    if not path.exists():
        log.warning("Seed-CSV nicht gefunden: %s", path)
        return 0

    _ensure_table_exists()

    if not force and db.session.query(Krankenhaus).first() is not None:
        current = db.session.query(Krankenhaus).count()
        log.info("Krankenhaus-Tabelle bereits befüllt (%d Einträge) — Seed übersprungen.", current)
        return 0

    if force:
        deleted = db.session.query(Krankenhaus).delete()
        db.session.commit()
        log.info("Force-Reseed: %d bestehende Einträge gelöscht.", deleted)

    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        rows = list(_valid_rows(reader))

    if not rows:
        log.warning("Keine validen Zeilen in %s.", path)
        return 0

    db.session.bulk_insert_mappings(Krankenhaus, rows)
    db.session.commit()
    log.info("Seed abgeschlossen: %d Krankenhäuser aus %s importiert.", len(rows), path.name)
    return len(rows)


def seed_hubs_if_empty() -> int:
    """Legt den initialen Hub Süd (Ulm) an, wenn noch kein Hub existiert."""
    _ensure_table_exists()
    if db.session.query(Hub).count() > 0:
        return 0
    hub = Hub(
        name="Hub Süd",
        lat=48.4220,
        lon=9.9520,
        ort="Ulm",
        bundesland="Baden-Württemberg",
        kapazitaet_pro_tag=250,
        beschreibung=(
            "Eingangspunkt Süddeutschland — Universitätsklinikum Ulm, "
            "Trauma-Level-1-Zentrum. 72 h Vorlauf zur Patientenankunft, "
            "~250 Verletzte/Tag."
        ),
    )
    db.session.add(hub)
    db.session.commit()
    log.info("Hub 'Hub Süd' initial angelegt (Ulm).")
    return 1


def seed_if_empty(app) -> None:
    """In create_app() aufzurufen: füllt Tabellen nur wenn leer."""
    with app.app_context():
        try:
            _ensure_table_exists()
        except Exception as exc:  # noqa: BLE001
            app.logger.warning("DB nicht erreichbar beim Auto-Seed: %s", exc)
            return

        try:
            count = db.session.query(Krankenhaus).count()
        except (OperationalError, ProgrammingError, DatabaseError):
            db.session.rollback()
            count = 0

        if count == 0:
            try:
                inserted = seed_krankenhaus(force=False)
                if inserted:
                    app.logger.info("Auto-Seed: %d Krankenhäuser importiert.", inserted)
            except Exception as exc:  # noqa: BLE001
                db.session.rollback()
                app.logger.error("Krankenhaus-Seed fehlgeschlagen: %s", exc)

        try:
            seed_hubs_if_empty()
        except (OperationalError, ProgrammingError, DatabaseError) as exc:
            db.session.rollback()
            app.logger.warning("Hub-Seed übersprungen: %s", exc)
