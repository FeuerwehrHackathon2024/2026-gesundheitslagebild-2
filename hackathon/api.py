"""JSON-API für das Dashboard."""

from __future__ import annotations

import logging

from flask import Blueprint, abort, jsonify, request

from .dispatch import (
    dispatch_batch,
    init_belegung_rows,
    parse_ivena_xlsx,
    reset_dispatch,
    simulate_occupancy,
    reset_belegung,
)
from .extensions import db
from .models import (
    Hub,
    Krankenhaus,
    KrankenhausBelegung,
    Patient,
    PatientenBatch,
    TransportAuftrag,
)

log = logging.getLogger(__name__)

api_bp = Blueprint("api", __name__, url_prefix="/api")


_KRANKENHAUS_COMPACT_FIELDS = (
    "id", "ik", "name", "lat", "lon",
    "strasse", "hausnummer", "plz", "ort", "bundesland",
    "betten", "anzahl_fachabteilungen",
    "sk_max", "kann_sk1", "kann_sk2", "kann_sk3",
    "kapazitaet_sk1_geschaetzt", "kapazitaet_sk2_geschaetzt", "kapazitaet_sk3_geschaetzt",
    "hat_intensivmedizin", "hat_notaufnahme", "hat_bg_zulassung", "hat_radiologie",
    "hat_onkologie", "hat_psychiatrie", "hat_geriatrie", "hat_dialyse",
    "traeger_art", "traeger_name", "universitaet", "lehrkrankenhaus",
    "telefon", "website",
    "ausgeschlossen", "ausschluss_grund",
)


@api_bp.post("/krankenhaus/<int:kh_id>/toggle-exclude")
def toggle_exclude(kh_id: int):
    kh = db.session.get(Krankenhaus, kh_id)
    if kh is None:
        abort(404)
    payload = request.get_json(silent=True) or {}
    kh.ausgeschlossen = not kh.ausgeschlossen
    kh.ausschluss_grund = (payload.get("grund") or "manuell ausgeschlossen") if kh.ausgeschlossen else None
    db.session.commit()
    return jsonify({
        "id": kh.id,
        "name": kh.name,
        "ausgeschlossen": kh.ausgeschlossen,
        "ausschluss_grund": kh.ausschluss_grund,
    })


@api_bp.get("/krankenhaus/ausgeschlossen")
def list_ausgeschlossen():
    rows = (
        Krankenhaus.query
        .filter(Krankenhaus.ausgeschlossen == True)  # noqa: E712
        .order_by(Krankenhaus.name.asc())
        .all()
    )
    return jsonify([{
        "id": k.id,
        "name": k.name,
        "ort": k.ort,
        "plz": k.plz,
        "sk_max": k.sk_max,
        "ausschluss_grund": k.ausschluss_grund,
    } for k in rows])


def _row_to_compact(k: Krankenhaus) -> dict:
    return {f: getattr(k, f) for f in _KRANKENHAUS_COMPACT_FIELDS}


@api_bp.get("/hubs")
def list_hubs():
    hubs = Hub.query.order_by(Hub.id.asc()).all()
    return jsonify([h.to_dict() for h in hubs])


@api_bp.get("/krankenhaeuser")
def list_krankenhaeuser():
    q = (
        Krankenhaus.query
        .filter(Krankenhaus.lat.isnot(None), Krankenhaus.lon.isnot(None))
        .order_by(Krankenhaus.sk_max.asc().nulls_last() if hasattr(Krankenhaus.sk_max.asc(), "nulls_last") else Krankenhaus.sk_max.asc())
    )
    return jsonify([_row_to_compact(k) for k in q.all()])


@api_bp.get("/krankenhaeuser/<int:krankenhaus_id>")
def get_krankenhaus(krankenhaus_id: int):
    k = db.session.get(Krankenhaus, krankenhaus_id)
    if k is None:
        abort(404)
    return jsonify({
        col.name: getattr(k, col.name)
        for col in Krankenhaus.__table__.columns
    })


@api_bp.get("/filter-options")
def filter_options():
    """Dropdown-/Slider-Optionen für das Dashboard-Filter-UI."""
    bundeslaender = [
        row[0] for row in db.session.query(Krankenhaus.bundesland)
        .filter(Krankenhaus.bundesland.isnot(None))
        .distinct().order_by(Krankenhaus.bundesland.asc()).all()
        if row[0]
    ]
    traeger_arten = [
        row[0] for row in db.session.query(Krankenhaus.traeger_art)
        .filter(Krankenhaus.traeger_art.isnot(None))
        .distinct().order_by(Krankenhaus.traeger_art.asc()).all()
        if row[0]
    ]
    max_betten = db.session.query(db.func.max(Krankenhaus.betten)).scalar() or 0
    return jsonify({
        "bundeslaender": bundeslaender,
        "traeger_arten": traeger_arten,
        "max_betten": int(max_betten),
    })


@api_bp.get("/belegung")
def list_belegung():
    """Belegungs-Übersicht pro Klinik: Grundbelegung + Dispatch + Frei.

    Dispatch-Zuweisungen werden aus Patient-Tabelle aggregiert, damit man sieht,
    was aus der Simulation kommt und was durch die aktuelle Verteilung dazu kam.
    """
    # Dispatch-Zuweisungen pro Klinik+SK
    assignments = (
        db.session.query(
            Patient.assigned_krankenhaus_id,
            Patient.sk,
            db.func.count(Patient.id),
        )
        .filter(Patient.status == "assigned")
        .group_by(Patient.assigned_krankenhaus_id, Patient.sk)
        .all()
    )
    dispatch_by_kh: dict[int, dict[str, int]] = {}
    for kh_id, sk, cnt in assignments:
        if kh_id is None:
            continue
        dispatch_by_kh.setdefault(kh_id, {"SK1": 0, "SK2": 0, "SK3": 0})[sk] = cnt

    out = []
    rows = (
        db.session.query(KrankenhausBelegung, Krankenhaus)
        .join(Krankenhaus, Krankenhaus.id == KrankenhausBelegung.krankenhaus_id)
        .filter(Krankenhaus.lat.isnot(None))
        .order_by(Krankenhaus.name.asc())
    )
    for bel, kh in rows:
        dispatch = dispatch_by_kh.get(kh.id, {"SK1": 0, "SK2": 0, "SK3": 0})
        grundbelegung = {
            "SK1": max((bel.belegung_sk1 or 0) - dispatch["SK1"], 0),
            "SK2": max((bel.belegung_sk2 or 0) - dispatch["SK2"], 0),
            "SK3": max((bel.belegung_sk3 or 0) - dispatch["SK3"], 0),
        }
        out.append({
            "id": kh.id,
            "name": kh.name,
            "ort": kh.ort,
            "plz": kh.plz,
            "sk_max": kh.sk_max,
            "ausgeschlossen": kh.ausgeschlossen,
            "kapazitaet": {
                "SK1": bel.kapazitaet_sk1 or 0,
                "SK2": bel.kapazitaet_sk2 or 0,
                "SK3": bel.kapazitaet_sk3 or 0,
            },
            "grundbelegung": grundbelegung,
            "dispatch": dispatch,
            "belegung_total": {
                "SK1": bel.belegung_sk1 or 0,
                "SK2": bel.belegung_sk2 or 0,
                "SK3": bel.belegung_sk3 or 0,
            },
            "frei": {
                "SK1": max((bel.kapazitaet_sk1 or 0) - (bel.belegung_sk1 or 0), 0),
                "SK2": max((bel.kapazitaet_sk2 or 0) - (bel.belegung_sk2 or 0), 0),
                "SK3": max((bel.kapazitaet_sk3 or 0) - (bel.belegung_sk3 or 0), 0),
            },
            "vorbelegung_prozent": bel.vorbelegung_prozent or 0,
        })
    # Kliniken mit Dispatch-Einträgen oder Betten > 0 bevorzugt zeigen
    out.sort(key=lambda x: (
        -(x["dispatch"]["SK1"] + x["dispatch"]["SK2"] + x["dispatch"]["SK3"]),
        -(x["kapazitaet"]["SK1"] + x["kapazitaet"]["SK2"] + x["kapazitaet"]["SK3"]),
    ))
    return jsonify(out)


@api_bp.get("/stats")
def stats():
    total = Krankenhaus.query.filter(Krankenhaus.lat.isnot(None)).count()
    sk1 = Krankenhaus.query.filter(Krankenhaus.lat.isnot(None), Krankenhaus.kann_sk1 == True).count()
    sk2 = Krankenhaus.query.filter(Krankenhaus.lat.isnot(None), Krankenhaus.kann_sk2 == True).count()
    sk3 = Krankenhaus.query.filter(Krankenhaus.lat.isnot(None), Krankenhaus.kann_sk3 == True).count()
    return jsonify({"total": total, "sk1": sk1, "sk2": sk2, "sk3": sk3})


# ===================== Belegungs-Simulation =====================

@api_bp.post("/simulation/occupancy")
def set_occupancy():
    """Setze simulierte Grundbelegung (0..100 %) pro Klinik."""
    payload = request.get_json(silent=True) or {}
    try:
        percent = int(payload.get("percent", 0))
    except (TypeError, ValueError):
        return jsonify({"error": "percent muss Integer 0..100 sein"}), 400
    try:
        result = simulate_occupancy(percent, seed=payload.get("seed", 42))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    totals = _belegung_totals()
    return jsonify({**result, **totals})


@api_bp.post("/simulation/reset")
def reset_occupancy():
    cleared = reset_belegung()
    return jsonify({"cleared": cleared, **_belegung_totals()})


@api_bp.get("/simulation/status")
def occupancy_status():
    return jsonify(_belegung_totals())


def _belegung_totals() -> dict:
    rows = db.session.query(KrankenhausBelegung).all()
    cap = {"sk1": 0, "sk2": 0, "sk3": 0}
    bel = {"sk1": 0, "sk2": 0, "sk3": 0}
    for r in rows:
        for sk in ("sk1", "sk2", "sk3"):
            cap[sk] += getattr(r, f"kapazitaet_{sk}") or 0
            bel[sk] += getattr(r, f"belegung_{sk}") or 0
    frei = {sk: max(cap[sk] - bel[sk], 0) for sk in cap}
    return {
        "kliniken_mit_belegung": len(rows),
        "kapazitaet": cap,
        "belegung": bel,
        "frei": frei,
    }


# ===================== Batch Upload + Dispatch =====================

@api_bp.post("/batch/upload")
def batch_upload():
    """Upload einer IVENA-XLSX → Patienten-Batch anlegen."""
    f = request.files.get("file")
    if f is None:
        return jsonify({"error": "Keine Datei in 'file' hochgeladen"}), 400

    default_hub = request.form.get("hub") or "Hub Süd"
    hub_obj = Hub.query.filter_by(name=default_hub).first() or Hub.query.first()
    if hub_obj is None:
        return jsonify({"error": "Kein Hub in DB"}), 500

    try:
        raw = f.read()
        rows = parse_ivena_xlsx(raw, default_hub_name=hub_obj.name)
    except Exception as exc:  # noqa: BLE001
        log.exception("XLSX parse failed")
        return jsonify({"error": f"XLSX konnte nicht gelesen werden: {exc}"}), 400

    if not rows:
        return jsonify({"error": "Keine gültigen Patientenzeilen in XLSX gefunden"}), 400

    batch = PatientenBatch(
        filename=f.filename,
        hub_id=hub_obj.id,
        hub_name=hub_obj.name,
        total=len(rows),
        sk1=sum(1 for r in rows if r["sk"] == "SK1"),
        sk2=sum(1 for r in rows if r["sk"] == "SK2"),
        sk3=sum(1 for r in rows if r["sk"] == "SK3"),
    )
    db.session.add(batch)
    db.session.flush()

    for r in rows:
        p = Patient(
            batch_id=batch.id,
            external_id=r["external_id"],
            sk=r["sk"],
            datum=r["datum"],
            eingangssichtung=r["eingangssichtung"],
            transportbereit=r["transportbereit"],
            quelle=r["quelle"],
        )
        db.session.add(p)
    db.session.commit()

    return jsonify({
        "batch_id": batch.id,
        "filename": batch.filename,
        "hub": hub_obj.name,
        "total": batch.total,
        "sk1": batch.sk1, "sk2": batch.sk2, "sk3": batch.sk3,
    })


@api_bp.post("/batch/<int:batch_id>/dispatch")
def batch_dispatch(batch_id: int):
    batch = db.session.get(PatientenBatch, batch_id)
    if batch is None:
        abort(404)
    hub = db.session.get(Hub, batch.hub_id) if batch.hub_id else Hub.query.first()
    if hub is None:
        return jsonify({"error": "Kein Hub gefunden"}), 500

    # Belegungs-Tabelle sicherstellen
    init_belegung_rows()

    # Falls schon dispatched → erst reseten
    if batch.status == "dispatched":
        reset_dispatch(batch)

    result = dispatch_batch(batch, hub)

    return jsonify({
        "batch_id": result.batch_id,
        "assigned": result.assigned,
        "unassigned": result.unassigned,
        "transports": result.transport_count,
        "avg_distanz_km": result.avg_distanz_km,
    })


@api_bp.post("/batch/<int:batch_id>/reset")
def batch_reset(batch_id: int):
    batch = db.session.get(PatientenBatch, batch_id)
    if batch is None:
        abort(404)
    deleted = reset_dispatch(batch)
    return jsonify({"deleted_transports": deleted, "batch_id": batch.id})


@api_bp.get("/batches")
def list_batches():
    rows = PatientenBatch.query.order_by(PatientenBatch.uploaded_at.desc()).all()
    return jsonify([{
        "id": b.id,
        "filename": b.filename,
        "hub_name": b.hub_name,
        "total": b.total,
        "sk1": b.sk1, "sk2": b.sk2, "sk3": b.sk3,
        "status": b.status,
        "uploaded_at": b.uploaded_at.isoformat() if b.uploaded_at else None,
        "dispatched_at": b.dispatched_at.isoformat() if b.dispatched_at else None,
    } for b in rows])


@api_bp.delete("/batches/<int:batch_id>")
def delete_batch(batch_id: int):
    batch = db.session.get(PatientenBatch, batch_id)
    if batch is None:
        abort(404)
    reset_dispatch(batch)
    db.session.delete(batch)
    db.session.commit()
    return jsonify({"deleted": batch_id})


# ===================== Transporte =====================

# ===================== Patienten =====================

@api_bp.get("/patients")
def list_patients():
    batch_id = request.args.get("batch_id", type=int)
    q = Patient.query.order_by(
        db.case((Patient.sk == "SK1", 1), (Patient.sk == "SK2", 2), else_=3),
        Patient.eingangssichtung.asc().nulls_last(),
    )
    if batch_id:
        q = q.filter(Patient.batch_id == batch_id)
    out = []
    for p in q.all():
        kh = p.assigned_krankenhaus
        out.append({
            "id": p.id,
            "batch_id": p.batch_id,
            "external_id": p.external_id,
            "sk": p.sk,
            "datum": p.datum.isoformat() if p.datum else None,
            "eingangssichtung": p.eingangssichtung.isoformat() if p.eingangssichtung else None,
            "transportbereit": p.transportbereit.isoformat() if p.transportbereit else None,
            "quelle": p.quelle,
            "status": p.status,
            "aufenthaltsdauer_tage": p.aufenthaltsdauer_tage,
            "distanz_km": p.distanz_km,
            "ziel": {
                "id": kh.id if kh else None,
                "name": kh.name if kh else None,
                "ort": kh.ort if kh else None,
                "plz": kh.plz if kh else None,
                "bundesland": kh.bundesland if kh else None,
            } if kh else None,
            "assigned_at": p.assigned_at.isoformat() if p.assigned_at else None,
            "note": p.note,
        })
    return jsonify(out)


# ===================== Simulator → direkt laden =====================

@api_bp.post("/batch/from-simulator")
def batch_from_simulator():
    """Erzeugt einen Patienten-Batch direkt aus Simulator-Parametern (ohne XLSX-Umweg)."""
    from datetime import datetime as _dt
    from .simulator import SimInput, generate_patients

    payload = request.get_json(silent=True) or {}
    try:
        start_date = _dt.fromisoformat(payload.get("start_date", ""))
    except ValueError:
        return jsonify({"error": "start_date ungültig"}), 400

    hub_name = str(payload.get("hub_name") or "Hub Süd")
    hub_obj = Hub.query.filter_by(name=hub_name).first() or Hub.query.first()
    if hub_obj is None:
        return jsonify({"error": "Kein Hub in DB"}), 500

    seed = payload.get("seed")
    inp = SimInput(
        sk1=int(payload.get("sk1", 0)),
        sk2=int(payload.get("sk2", 0)),
        sk3=int(payload.get("sk3", 0)),
        start_date=start_date,
        days=int(payload.get("days", 1)),
        hub_name=hub_obj.name,
        start_hour=int(payload.get("start_hour", 8)),
        end_hour=int(payload.get("end_hour", 20)),
        seed=int(seed) if seed not in (None, "") else None,
    )
    if inp.total == 0:
        return jsonify({"error": "Mindestens ein SK-Wert > 0"}), 400

    rows = generate_patients(inp)
    batch = PatientenBatch(
        filename=f"simulator_{inp.total}p_{inp.days}t.xlsx",
        hub_id=hub_obj.id,
        hub_name=hub_obj.name,
        total=len(rows),
        sk1=inp.sk1, sk2=inp.sk2, sk3=inp.sk3,
    )
    db.session.add(batch)
    db.session.flush()

    for r in rows:
        datum = r["Datum"]
        sichtung = r.get("Eingangssichtung um")
        transport = r.get("Transportbereitschaft gesetzt um")
        from datetime import datetime as _dt2, date as _date
        if isinstance(datum, _date) and sichtung is not None and not isinstance(sichtung, _dt2):
            sichtung = _dt2.combine(datum, sichtung)
        if isinstance(datum, _date) and transport is not None and not isinstance(transport, _dt2):
            transport = _dt2.combine(datum, transport)
        p = Patient(
            batch_id=batch.id,
            external_id=r["ID"],
            sk=r["SK"],
            datum=datum,
            eingangssichtung=sichtung,
            transportbereit=transport,
            quelle=r["Quelle"],
        )
        db.session.add(p)
    db.session.commit()

    return jsonify({
        "batch_id": batch.id,
        "filename": batch.filename,
        "hub": hub_obj.name,
        "total": batch.total,
        "sk1": batch.sk1, "sk2": batch.sk2, "sk3": batch.sk3,
    })


# ===================== Globaler Reset =====================

@api_bp.post("/reset/all")
def reset_all():
    """Löscht alle Batches, Patienten, Transporte und setzt Belegung auf 0."""
    t_count = db.session.query(TransportAuftrag).delete()
    p_count = db.session.query(Patient).delete()
    b_count = db.session.query(PatientenBatch).delete()
    cleared = reset_belegung()
    db.session.commit()
    return jsonify({
        "transports_deleted": t_count,
        "patients_deleted": p_count,
        "batches_deleted": b_count,
        "belegung_reset": cleared,
    })


# ===================== Transporte =====================

@api_bp.get("/transports")
def list_transports():
    batch_id = request.args.get("batch_id", type=int)
    q = TransportAuftrag.query.order_by(
        db.case((TransportAuftrag.sk == "SK1", 1), (TransportAuftrag.sk == "SK2", 2), else_=3),
        TransportAuftrag.distanz_km.asc(),
    )
    if batch_id:
        q = q.filter(TransportAuftrag.batch_id == batch_id)
    out = []
    for t in q.all():
        kh = t.krankenhaus
        out.append({
            "id": t.id,
            "batch_id": t.batch_id,
            "patient_id": t.patient_id,
            "patient_external_id": t.patient.external_id if t.patient else None,
            "sk": t.sk,
            "hub": {"lat": t.hub_lat, "lon": t.hub_lon},
            "ziel": {
                "id": t.krankenhaus_id,
                "name": kh.name if kh else None,
                "ort": kh.ort if kh else None,
                "lat": t.ziel_lat, "lon": t.ziel_lon,
            },
            "distanz_km": t.distanz_km,
            "dauer_min": t.dauer_min,
            "status": t.status,
            "erzeugt_am": t.erzeugt_am.isoformat() if t.erzeugt_am else None,
        })
    return jsonify(out)
