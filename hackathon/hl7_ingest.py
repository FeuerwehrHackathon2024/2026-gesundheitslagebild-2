"""HL7 v2 ADT Parser + Verarbeitung.

Akzeptiert A01 (Aufnahme), A03 (Entlassung), A08 (Update) und verändert damit
die live KrankenhausBelegung-Tabelle.

Da unsere Krankenhaus-DB keine IK-zu-HL7-Facility-Mapping enthält, mapt der
Parser die `Sending Facility` (MSH-4) fuzzy auf den Klinik-Namen:
  - exakter Name-Match
  - Substring-Match (IGNORE CASE)
  - Fallback: zufällige Klinik in Reichweite des Hubs
"""

from __future__ import annotations

import logging
import random
from datetime import datetime
from difflib import SequenceMatcher

from .extensions import db
from .ivena_mapping import map_ivena_to_sk
from .models import AdtEvent, Krankenhaus, KrankenhausBelegung

log = logging.getLogger(__name__)

SEGMENT_SEP_CANDIDATES = ("\r\n", "\r", "\n")


def _split_segments(raw: str) -> list[list[str]]:
    text = raw
    for sep in ("\r\n", "\r"):
        text = text.replace(sep, "\n")
    return [seg.split("|") for seg in text.split("\n") if seg.strip()]


def _parse_ts(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value[:14].ljust(14, "0"), "%Y%m%d%H%M%S")
    except ValueError:
        return None


def parse_hl7_adt(raw: str) -> dict:
    """Parst eine MSH/EVN/PID/PV1-Nachricht in ein Dict.

    Rückgabe-Schlüssel: event, patient_id, sending_facility, station, bed,
                         sk (von PV1-19), admit_ts, discharge_ts, raw.
    """
    segments = _split_segments(raw)
    by_name: dict[str, list[str]] = {}
    for seg in segments:
        if not seg:
            continue
        by_name[seg[0]] = seg

    msh = by_name.get("MSH", [])
    evn = by_name.get("EVN", [])
    pid = by_name.get("PID", [])
    pv1 = by_name.get("PV1", [])

    # MSH-9: ADT^A01
    event = ""
    if len(msh) > 8:
        parts = msh[8].split("^") if msh[8] else []
        if len(parts) >= 2:
            event = parts[1].strip()
    if not event and len(evn) > 1:
        event = evn[1].strip()

    sending_facility = msh[3] if len(msh) > 3 else ""

    patient_id = ""
    if len(pid) > 3 and pid[3]:
        patient_id = pid[3].split("^")[0]

    station = bed = ""
    if len(pv1) > 3 and pv1[3]:
        loc = pv1[3].split("^")
        station = loc[0] if len(loc) > 0 else ""
        bed = loc[1] if len(loc) > 1 else ""

    # PV1-19 (Visit Number) wird im Generator als SK-Speicher benutzt.
    sk_raw = pv1[19] if len(pv1) > 19 else ""
    sk = map_ivena_to_sk(sk_raw)

    admit_ts = _parse_ts(pv1[44]) if len(pv1) > 44 else None
    discharge_ts = _parse_ts(pv1[45]) if len(pv1) > 45 else None

    return {
        "event": event,
        "patient_id": patient_id,
        "sending_facility": sending_facility,
        "station": station,
        "bed": bed,
        "sk": sk,
        "admit_ts": admit_ts,
        "discharge_ts": discharge_ts,
        "raw": raw,
    }


# ---------------- Facility → Klinik Mapping ----------------

_FACILITY_CACHE: dict[str, int | None] = {}


def _sim(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def resolve_krankenhaus(sending_facility: str) -> Krankenhaus | None:
    if not sending_facility:
        return None
    if sending_facility in _FACILITY_CACHE:
        kh_id = _FACILITY_CACHE[sending_facility]
        return db.session.get(Krankenhaus, kh_id) if kh_id else None

    # Extrahiere Name-Komponente (MSH-4 = "FacId^Name")
    parts = sending_facility.split("^")
    candidates = [p.strip() for p in parts if p.strip()]

    # 1. exakter Substring-Match
    for cand in candidates:
        kh = (
            Krankenhaus.query
            .filter(Krankenhaus.lat.isnot(None), Krankenhaus.ausgeschlossen == False)  # noqa: E712
            .filter(Krankenhaus.name.ilike(f"%{cand}%"))
            .first()
        )
        if kh:
            _FACILITY_CACHE[sending_facility] = kh.id
            return kh

    # 2. Fuzzy: hole 200 Kliniken und score
    best = None
    best_score = 0.0
    for kh in (
        Krankenhaus.query
        .filter(Krankenhaus.lat.isnot(None), Krankenhaus.ausgeschlossen == False)  # noqa: E712
        .limit(2000)
        .all()
    ):
        for cand in candidates:
            s = _sim(cand, kh.name or "")
            if s > best_score:
                best_score, best = s, kh
    if best and best_score > 0.5:
        _FACILITY_CACHE[sending_facility] = best.id
        return best

    _FACILITY_CACHE[sending_facility] = None
    return None


def _random_target_kh() -> Krankenhaus | None:
    """Fallback: zufällige Klinik mit Belegungs-Datensatz und Kapazität."""
    bel_ids = [
        row[0] for row in db.session.query(KrankenhausBelegung.krankenhaus_id)
        .filter((KrankenhausBelegung.kapazitaet_sk1 +
                 KrankenhausBelegung.kapazitaet_sk2 +
                 KrankenhausBelegung.kapazitaet_sk3) > 0).all()
    ]
    if not bel_ids:
        return None
    kh_id = random.choice(bel_ids)
    return db.session.get(Krankenhaus, kh_id)


def apply_event(parsed: dict) -> AdtEvent:
    """Persistiert das Event und updated die KrankenhausBelegung.

    A01 (Aufnahme):   belegung[sk] += 1
    A03 (Entlassung): belegung[sk] -= 1 (min 0)
    A08 (Update):     keine Mengen-Änderung, nur Event-Log
    """
    ev_type = (parsed.get("event") or "").upper()
    sk = parsed.get("sk")
    kh = resolve_krankenhaus(parsed.get("sending_facility") or "")
    if kh is None:
        kh = _random_target_kh()

    # Falls SK fehlt, würfel eine aus (damit das Simulator-Mapping weiter klappt)
    if not sk:
        sk = random.choices(["SK1", "SK2", "SK3"], weights=[1, 2, 4])[0]

    note = None
    processed = True
    if kh is None:
        processed = False
        note = "Kein Krankenhaus zuordenbar"

    event = AdtEvent(
        event_type=ev_type,
        patient_hl7_id=parsed.get("patient_id"),
        sk=sk,
        krankenhaus_id=kh.id if kh else None,
        sending_facility_raw=parsed.get("sending_facility"),
        station=parsed.get("station"),
        admit_ts=parsed.get("admit_ts"),
        discharge_ts=parsed.get("discharge_ts"),
        raw_message=(parsed.get("raw") or "")[:8000],
        processed_ok=processed,
        process_note=note,
    )
    db.session.add(event)

    if kh is not None:
        # Belegung-Datensatz anlegen wenn noch nicht vorhanden
        bel = db.session.get(KrankenhausBelegung, kh.id)
        if bel is None:
            bel = KrankenhausBelegung(
                krankenhaus_id=kh.id,
                kapazitaet_sk1=kh.kapazitaet_sk1_geschaetzt or 0,
                kapazitaet_sk2=kh.kapazitaet_sk2_geschaetzt or 0,
                kapazitaet_sk3=kh.kapazitaet_sk3_geschaetzt or 0,
            )
            db.session.add(bel)
            db.session.flush()

        col = f"belegung_{sk.lower()}"
        cap_col = f"kapazitaet_{sk.lower()}"
        current = getattr(bel, col) or 0
        cap = getattr(bel, cap_col) or 0

        if ev_type == "A01":
            new = current + 1
            if cap and new > cap:
                note = (note or "") + f" Kapazität {sk} überschritten ({new}/{cap})"
                event.process_note = note.strip()
            setattr(bel, col, new)
        elif ev_type == "A03":
            setattr(bel, col, max(current - 1, 0))
        elif ev_type == "A08":
            # Optional: falls admit_ts/discharge_ts dabei: als Info loggen
            pass

    db.session.commit()
    return event
