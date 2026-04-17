"""Time-Capsule-Simulator.

Simuliert mehrere Tage MANV-Betrieb: Patientenströme am Hub, Aufnahmen in
Kliniken (A01), planmäßige Entlassungen (A03 nach Aufenthaltsdauer) und
erzeugt dichte ADT-Events + Belegungs-Snapshots. Das Ergebnis wird als
Timeline mit Chart.js im UI visualisiert.

Es gibt zwei Modi:
  - "batch" / synchron: die gesamte Simulation läuft auf dem Server ab,
     Snapshots werden in DB gespeichert und per API ausgeliefert.
  - "live" / streaming: Frontend pollt Events in echtzeit-artig.

Dieses Modul implementiert den batch-Modus, den wir für outstanding
Visualisierung im UI brauchen.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable

from .dispatch import (
    SK_AUFENTHALT_TAGE,
    TRANSPORTMITTEL_KAPAZITAET,
    haversine_km,
)
from .extensions import db
from .ivena_mapping import SK_TRANSPORTMITTEL as IVENA_TRANSPORTMITTEL
from .models import (
    AdtEvent,
    Fahrt,
    Hub,
    Krankenhaus,
    KrankenhausBelegung,
    Patient,
    PatientenBatch,
    TransportAuftrag,
)


@dataclass
class CapsuleParams:
    days: int = 5
    patients_per_day: int = 250
    sk_distribution: tuple[float, float, float] = (0.12, 0.28, 0.60)  # SK1, SK2, SK3
    start_date: datetime | None = None
    grundbelegung_prozent: int = 60
    seed: int | None = 42
    bundesland: str | None = None  # wenn gesetzt: nur Kliniken dieses Landes


def _ensure_belegung(kh: Krankenhaus) -> KrankenhausBelegung:
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
    return bel


def _reset_simulation_state():
    """Alles, was die Capsule erzeugt, vorher leeren."""
    db.session.query(AdtEvent).delete()
    db.session.query(TransportAuftrag).delete()
    db.session.query(Fahrt).delete()
    db.session.query(Patient).delete()
    db.session.query(PatientenBatch).delete()
    # Belegung nicht komplett leeren, aber belegung_sk* auf 0 setzen
    for row in db.session.query(KrankenhausBelegung).all():
        row.belegung_sk1 = 0
        row.belegung_sk2 = 0
        row.belegung_sk3 = 0
    db.session.commit()


def _bundesland_kh_ids(bundesland: str | None) -> set[int] | None:
    if not bundesland:
        return None
    ids = {row[0] for row in db.session.query(Krankenhaus.id)
           .filter(Krankenhaus.bundesland == bundesland).all()}
    return ids


def _set_grundbelegung(prozent: int, rng: random.Random, bundesland_ids: set[int] | None = None):
    q = db.session.query(KrankenhausBelegung)
    for row in q.all():
        if bundesland_ids is not None and row.krankenhaus_id not in bundesland_ids:
            continue
        row.vorbelegung_prozent = prozent
        for sk in ("sk1", "sk2", "sk3"):
            cap = getattr(row, f"kapazitaet_{sk}") or 0
            jitter = rng.uniform(-10, 10)
            pct = max(0.0, min(100.0, prozent + jitter))
            setattr(row, f"belegung_{sk}", int(round(cap * pct / 100.0)))
    db.session.commit()


def _pick_target_kh(sk: str, hub: Hub, rng: random.Random,
                    bundesland: str | None = None) -> tuple[Krankenhaus, KrankenhausBelegung, float] | None:
    """Wähle eine Klinik mit freier Kapazität für diese SK-Stufe.

    Strategie: nach Distanz zum Hub sortieren, erste mit Kapazität nehmen.
    Bei bundesland-Filter werden nur Kliniken des Bundeslands betrachtet.
    """
    from .dispatch import SK_KANN_COLS
    from sqlalchemy import or_

    ors = [getattr(Krankenhaus, c) == True for c in SK_KANN_COLS[sk]]  # noqa: E712
    q = (
        db.session.query(Krankenhaus, KrankenhausBelegung)
        .join(KrankenhausBelegung, KrankenhausBelegung.krankenhaus_id == Krankenhaus.id)
        .filter(Krankenhaus.lat.isnot(None))
        .filter(Krankenhaus.ausgeschlossen == False)  # noqa: E712
        .filter(or_(*ors))
    )
    if bundesland:
        q = q.filter(Krankenhaus.bundesland == bundesland)
    candidates = q.all()
    ranked = []
    for kh, bel in candidates:
        d = haversine_km(hub.lat, hub.lon, kh.lat, kh.lon)
        ranked.append((kh, bel, d))
    ranked.sort(key=lambda t: t[2])
    for kh, bel, d in ranked:
        if bel.frei(sk) > 0:
            return kh, bel, d
    return None


def run_capsule(params: CapsuleParams) -> dict:
    """Führt die mehrtägige Simulation durch und schreibt alles in die DB.

    Liefert eine Zusammenfassung + Snapshots, die das UI direkt plotten kann.
    """
    rng = random.Random(params.seed) if params.seed is not None else random.Random()
    start = params.start_date or datetime.now().replace(hour=6, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=params.days)

    hub = Hub.query.filter_by(name="Hub Süd").first() or Hub.query.first()
    if hub is None:
        return {"error": "Kein Hub in DB"}

    _reset_simulation_state()
    # Wichtig: Belegungs-Rows für alle bayrischen Kliniken sicherstellen,
    # bevor wir die Grundbelegung setzen (sonst greift der Bundesland-Filter
    # ins Leere, wenn noch nie gedispatched wurde).
    for kh in (db.session.query(Krankenhaus)
               .filter(Krankenhaus.lat.isnot(None))
               .all()):
        _ensure_belegung(kh)
    db.session.commit()

    bundesland_ids = _bundesland_kh_ids(params.bundesland)
    _set_grundbelegung(params.grundbelegung_prozent, rng, bundesland_ids)

    # Globaler Batch für alle Capsule-Patienten
    batch = PatientenBatch(
        filename=f"TimeCapsule {start.strftime('%Y-%m-%d')} ({params.days}d)",
        hub_id=hub.id, hub_name=hub.name,
        total=0, sk1=0, sk2=0, sk3=0, status="dispatched",
        dispatched_at=datetime.utcnow(),
    )
    db.session.add(batch)
    db.session.flush()

    snapshot_interval = timedelta(hours=1)
    next_snapshot = start
    snapshots: list[dict] = []

    # Verfolgen, wann welcher Patient entlassen werden soll (für A03)
    pending_discharges: list[tuple[datetime, int, str]] = []  # (when, kh_id, sk)

    # Pro Tag: Patienten verteilt über 8..22 Uhr
    total_patients = 0
    unassigned = 0
    events_a01 = events_a03 = 0
    admits_per_day: dict[int, int] = {}
    discharges_per_day: dict[int, int] = {}

    sim_time = start
    patient_idx = 0
    for day in range(params.days):
        day_start = start + timedelta(days=day)
        peak = day_start.replace(hour=14)  # Peak am Nachmittag
        day_count = int(params.patients_per_day * rng.uniform(0.85, 1.15))

        # Zeiten so verteilen, dass um Peak herum Spitze ist
        arrival_times = []
        for _ in range(day_count):
            base_min = rng.normalvariate(14 * 60, 180)  # min ~ N(14:00, 3h)
            base_min = max(7 * 60, min(22 * 60, base_min))
            arrival_times.append(day_start + timedelta(minutes=base_min))
        arrival_times.sort()

        # SK-Verteilung
        sk_pool = (["SK1"] * int(day_count * params.sk_distribution[0])
                   + ["SK2"] * int(day_count * params.sk_distribution[1])
                   + ["SK3"] * int(day_count * params.sk_distribution[2]))
        while len(sk_pool) < day_count:
            sk_pool.append("SK3")
        rng.shuffle(sk_pool)

        for arr_dt, sk in zip(arrival_times, sk_pool):
            patient_idx += 1
            total_patients += 1

            # Snapshot wenn fällig
            while next_snapshot <= arr_dt and next_snapshot <= end:
                snapshots.append(_capture_snapshot(next_snapshot, bundesland_ids))
                next_snapshot += snapshot_interval

            # Auch Entlassungen durchführen die bis arr_dt passieren
            while pending_discharges and pending_discharges[0][0] <= arr_dt:
                d_time, d_khid, d_sk = pending_discharges.pop(0)
                bel = db.session.get(KrankenhausBelegung, d_khid)
                if bel:
                    col = f"belegung_{d_sk.lower()}"
                    setattr(bel, col, max(getattr(bel, col) - 1, 0))
                db.session.add(AdtEvent(
                    event_type="A03", sk=d_sk,
                    patient_hl7_id=f"TC-DIS-{d_khid}-{d_time.strftime('%m%d%H%M')}",
                    krankenhaus_id=d_khid,
                    sending_facility_raw="TimeCapsule",
                    discharge_ts=d_time,
                    processed_ok=True,
                    created_at=d_time,
                ))
                events_a03 += 1
                d_day = (d_time - start).days
                discharges_per_day[d_day] = discharges_per_day.get(d_day, 0) + 1

            # Patient finden/erstellen
            p = Patient(
                batch_id=batch.id,
                external_id=f"TC_{arr_dt.strftime('%m%d')}_{patient_idx:04d}",
                sk=sk,
                datum=arr_dt.date(),
                eingangssichtung=arr_dt,
                transportbereit=arr_dt + timedelta(minutes=rng.randint(3, 15)),
                quelle=hub.name,
            )
            db.session.add(p)
            db.session.flush()
            batch.total += 1
            setattr(batch, sk.lower(), getattr(batch, sk.lower()) + 1)

            pick = _pick_target_kh(sk, hub, rng, params.bundesland)
            if pick is None:
                p.status = "unassigned"
                unassigned += 1
                continue
            kh, bel, dist = pick

            # Aufnehmen
            col = f"belegung_{sk.lower()}"
            setattr(bel, col, (getattr(bel, col) or 0) + 1)
            p.assigned_krankenhaus_id = kh.id
            p.assigned_at = arr_dt
            p.aufenthaltsdauer_tage = SK_AUFENTHALT_TAGE[sk]
            p.distanz_km = dist
            p.status = "assigned"

            # Entlassung für später vormerken
            aufenthalt = SK_AUFENTHALT_TAGE[sk]
            discharge_dt = arr_dt + timedelta(days=aufenthalt)
            pending_discharges.append((discharge_dt, kh.id, sk))
            pending_discharges.sort(key=lambda t: t[0])

            # ADT A01
            db.session.add(AdtEvent(
                event_type="A01", sk=sk, patient_hl7_id=p.external_id,
                krankenhaus_id=kh.id,
                sending_facility_raw="TimeCapsule",
                admit_ts=arr_dt,
                processed_ok=True,
                created_at=arr_dt,
            ))
            events_a01 += 1
            admits_per_day[day] = admits_per_day.get(day, 0) + 1

        # Am Tagesende committen, damit Session nicht zu fett wird
        db.session.commit()

    # Restliche Snapshots + noch anstehende Entlassungen nach Zeitraum-Ende
    while next_snapshot <= end:
        snapshots.append(_capture_snapshot(next_snapshot, bundesland_ids))
        next_snapshot += snapshot_interval

    while pending_discharges and pending_discharges[0][0] <= end:
        d_time, d_khid, d_sk = pending_discharges.pop(0)
        bel = db.session.get(KrankenhausBelegung, d_khid)
        if bel:
            col = f"belegung_{d_sk.lower()}"
            setattr(bel, col, max(getattr(bel, col) - 1, 0))
        db.session.add(AdtEvent(
            event_type="A03", sk=d_sk,
            patient_hl7_id=f"TC-DIS-{d_khid}-{d_time.strftime('%m%d%H%M')}",
            krankenhaus_id=d_khid,
            sending_facility_raw="TimeCapsule",
            discharge_ts=d_time, processed_ok=True,
            created_at=d_time,
        ))
        events_a03 += 1
        d_day = (d_time - start).days
        if 0 <= d_day < params.days:
            discharges_per_day[d_day] = discharges_per_day.get(d_day, 0) + 1

    db.session.commit()

    daily_summary = _compute_daily_summary(
        snapshots, start, params.days, admits_per_day, discharges_per_day,
    )

    # Peak-Metriken über gesamte Simulation
    peak_pct = {"SK1": 0.0, "SK2": 0.0, "SK3": 0.0}
    peak_when = {"SK1": None, "SK2": None, "SK3": None}
    for s in snapshots:
        for sk in ("SK1", "SK2", "SK3"):
            if s["pct"][sk] > peak_pct[sk]:
                peak_pct[sk] = s["pct"][sk]
                peak_when[sk] = s["t"]

    return {
        "batch_id": batch.id,
        "days": params.days,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "total_patients": total_patients,
        "unassigned": unassigned,
        "events_a01": events_a01,
        "events_a03": events_a03,
        "peak_pct": peak_pct,
        "peak_when": peak_when,
        "daily_summary": daily_summary,
        "snapshots": snapshots,
    }


def _capture_snapshot(at: datetime, bundesland_ids: set[int] | None = None) -> dict:
    """Summiert aktuelle Belegung für Chart-Zeile. Optional auf Bundesland-Scope beschränkt."""
    from sqlalchemy import func
    q = db.session.query(
        func.sum(KrankenhausBelegung.kapazitaet_sk1),
        func.sum(KrankenhausBelegung.kapazitaet_sk2),
        func.sum(KrankenhausBelegung.kapazitaet_sk3),
        func.sum(KrankenhausBelegung.belegung_sk1),
        func.sum(KrankenhausBelegung.belegung_sk2),
        func.sum(KrankenhausBelegung.belegung_sk3),
    )
    if bundesland_ids is not None:
        q = q.filter(KrankenhausBelegung.krankenhaus_id.in_(bundesland_ids))
    agg = q.first()
    cap1, cap2, cap3, bel1, bel2, bel3 = [x or 0 for x in agg]
    def _pct(b, c):
        return round(b / c * 100, 1) if c else 0.0
    return {
        "t": at.isoformat(),
        "cap": {"SK1": cap1, "SK2": cap2, "SK3": cap3},
        "bel": {"SK1": bel1, "SK2": bel2, "SK3": bel3},
        "frei": {"SK1": max(cap1 - bel1, 0),
                 "SK2": max(cap2 - bel2, 0),
                 "SK3": max(cap3 - bel3, 0)},
        "pct": {"SK1": _pct(bel1, cap1),
                "SK2": _pct(bel2, cap2),
                "SK3": _pct(bel3, cap3)},
    }


def _compute_daily_summary(snapshots: list[dict], start: datetime, days: int,
                           admits_per_day: dict, discharges_per_day: dict) -> list[dict]:
    """Aggregiert die stündlichen Snapshots zu Tageskacheln."""
    out = []
    for day_i in range(days):
        day_start = (start + timedelta(days=day_i)).replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)
        day_snaps = [s for s in snapshots
                     if day_start.isoformat() <= s["t"] < day_end.isoformat()]
        if not day_snaps:
            continue
        # Peak = höchste Gesamt-Auslastung am Tag
        peak = {}
        closing = day_snaps[-1]  # letzter Snapshot des Tages
        for sk in ("SK1", "SK2", "SK3"):
            pmax = max(s["pct"][sk] for s in day_snaps)
            peak_snap = next(s for s in day_snaps if s["pct"][sk] == pmax)
            peak[sk] = {
                "pct_peak": pmax,
                "bel_peak": peak_snap["bel"][sk],
                "bel_close": closing["bel"][sk],
                "pct_close": closing["pct"][sk],
                "cap": closing["cap"][sk],
            }
        out.append({
            "day": day_i + 1,
            "date": day_start.date().isoformat(),
            "label": day_start.strftime("%a %d.%m."),
            "aufnahmen": admits_per_day.get(day_i, 0),
            "entlassungen": discharges_per_day.get(day_i, 0),
            "peak": peak,
        })
    return out
