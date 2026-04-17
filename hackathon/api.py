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
from .here_traffic import traffic_flow, traffic_incidents
from .hl7_ingest import apply_event, parse_hl7_adt
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
def get_krankenhaus_full(krankenhaus_id: int):
    """Vollständiger Row-Dump einer Klinik (alle DB-Spalten) — für Debugging/Export."""
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


@api_bp.get("/krankenhaeuser/occupancy")
def krankenhaeuser_occupancy():
    """Liefert pro Klinik den aktuellen Belegungszustand (nur die mit Geo).

    Für die Dashboard-Map um Marker nach Auslastung einzufärben.
    Return: [{id, fill_pct, status}] — status ∈ 'frei' | 'mittel' | 'voll' | 'uebervoll' | 'unbekannt'
    """
    rows = (
        db.session.query(Krankenhaus, KrankenhausBelegung)
        .outerjoin(KrankenhausBelegung, KrankenhausBelegung.krankenhaus_id == Krankenhaus.id)
        .filter(Krankenhaus.lat.isnot(None))
        .all()
    )
    out = []
    for kh, bel in rows:
        if bel is None or (not bel.kapazitaet_sk1 and not bel.kapazitaet_sk2 and not bel.kapazitaet_sk3):
            out.append({"id": kh.id, "fill_pct": None, "status": "unbekannt"})
            continue
        cap = (bel.kapazitaet_sk1 or 0) + (bel.kapazitaet_sk2 or 0) + (bel.kapazitaet_sk3 or 0)
        used = (bel.belegung_sk1 or 0) + (bel.belegung_sk2 or 0) + (bel.belegung_sk3 or 0)
        if cap == 0:
            out.append({"id": kh.id, "fill_pct": None, "status": "unbekannt"})
            continue
        pct = round(used / cap * 100)
        if pct >= 100:
            status = "uebervoll"
        elif pct >= 85:
            status = "voll"
        elif pct >= 60:
            status = "mittel"
        else:
            status = "frei"
        out.append({"id": kh.id, "fill_pct": pct, "status": status,
                    "used": used, "cap": cap})
    return jsonify(out)


# ===================== Fahrt-Status Workflow =====================

@api_bp.post("/fahrten/<int:fahrt_id>/status")
def update_fahrt_status(fahrt_id: int):
    """Setzt den Status einer Fahrt (und aller zugehörigen Transportaufträge)."""
    payload = request.get_json(silent=True) or {}
    new_status = str(payload.get("status", "")).strip().lower()
    if new_status not in ("geplant", "unterwegs", "abgeschlossen"):
        return jsonify({"error": "status muss geplant|unterwegs|abgeschlossen sein"}), 400
    f = db.session.get(Fahrt, fahrt_id)
    if f is None:
        abort(404)
    f.status = new_status
    # zugehörige Transportaufträge spiegeln
    db.session.query(TransportAuftrag).filter(
        TransportAuftrag.fahrt_id == f.id
    ).update({"status": new_status})
    db.session.commit()
    return jsonify({"id": f.id, "status": f.status})


# ===================== PDF-Export Transportauftrag =====================

@api_bp.get("/transports/<int:transport_id>/pdf")
def transport_pdf(transport_id: int):
    from datetime import datetime as _dt
    from io import BytesIO
    from flask import Response as _Response
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    )

    t = db.session.get(TransportAuftrag, transport_id)
    if t is None:
        abort(404)
    kh = t.krankenhaus
    p = t.patient
    f = t.fahrt

    def fmt_dt(d):
        return d.strftime("%d.%m.%Y %H:%M") if d else "—"

    sk_color = {"SK1": colors.HexColor("#b71c1c"),
                "SK2": colors.HexColor("#f57c00"),
                "SK3": colors.HexColor("#fbc02d")}.get(t.sk, colors.black)
    sk_textcolor = colors.white if t.sk in ("SK1", "SK2") else colors.black

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=1.8 * cm, rightMargin=1.8 * cm,
        topMargin=1.5 * cm, bottomMargin=1.5 * cm,
    )
    styles = getSampleStyleSheet()
    story = []

    title_style = ParagraphStyle(
        "Title", parent=styles["Heading1"], fontSize=18, textColor=colors.HexColor("#c62828"),
    )
    story.append(Paragraph(f"Transportauftrag #{t.id}", title_style))
    story.append(Paragraph(
        f"MANV-Dispatch · Hub Süd (Ulm) · erzeugt {fmt_dt(t.erzeugt_am)}",
        styles["Normal"],
    ))
    story.append(Spacer(1, 0.4 * cm))

    # Kopfzeile: SK-Badge + Transportmittel
    header_data = [[
        Paragraph(f"<b>{t.sk or '–'}</b>", ParagraphStyle(
            "sk", parent=styles["Normal"], textColor=sk_textcolor,
            backColor=sk_color, alignment=1, fontSize=16,
        )),
        Paragraph(f"<b>{t.transportmittel or '—'}</b>", ParagraphStyle(
            "tm", parent=styles["Normal"], fontSize=16, alignment=1,
        )),
        Paragraph(f"<b>Status:</b> {t.status or 'geplant'}<br/>"
                  f"<b>Fahrt-Code:</b> {f.fahrt_code if f else '—'}",
                  styles["Normal"]),
    ]]
    header_tbl = Table(header_data, colWidths=[2.5 * cm, 3 * cm, 10.5 * cm])
    header_tbl.setStyle(TableStyle([
        ("BOX", (0, 0), (-1, -1), 0.5, colors.lightgrey),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ]))
    story.append(header_tbl)
    story.append(Spacer(1, 0.5 * cm))

    # Zeitplan
    zeit_data = [
        ["Abfahrt am Hub", fmt_dt(t.abfahrt)],
        ["Ankunft in Klinik", fmt_dt(t.ankunft)],
        ["Strecke",
         f"{round(t.distanz_km, 1) if t.distanz_km else '—'} km · "
         f"{round(t.dauer_min) if t.dauer_min else '—'} min"
         f"  ({t.routing_source or '—'})"],
    ]
    zeit_tbl = Table(zeit_data, colWidths=[5 * cm, 11 * cm])
    zeit_tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (0, -1), colors.whitesmoke),
        ("BOX", (0, 0), (-1, -1), 0.5, colors.lightgrey),
        ("INNERGRID", (0, 0), (-1, -1), 0.3, colors.lightgrey),
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))
    story.append(zeit_tbl)
    story.append(Spacer(1, 0.4 * cm))

    # Start + Ziel nebeneinander
    start_text = "<b>Start (Hub)</b><br/>"
    if t.hub:
        start_text += f"{t.hub.name}<br/>{t.hub.ort or ''}<br/>"
    if t.hub_lat is not None:
        start_text += f"GPS: {t.hub_lat:.4f}, {t.hub_lon:.4f}"

    ziel_text = "<b>Ziel (Klinik)</b><br/>"
    if kh:
        ziel_text += f"{kh.name}<br/>"
        addr = " ".join(filter(None, [kh.strasse, kh.hausnummer]))
        place = " ".join(filter(None, [kh.plz, kh.ort]))
        if addr:
            ziel_text += f"{addr}<br/>"
        if place:
            ziel_text += f"{place}<br/>"
        if kh.telefon:
            ziel_text += f"Tel: {kh.telefon}<br/>"
    if t.ziel_lat is not None:
        ziel_text += f"GPS: {t.ziel_lat:.4f}, {t.ziel_lon:.4f}"

    route_tbl = Table(
        [[Paragraph(start_text, styles["Normal"]),
          Paragraph(ziel_text, styles["Normal"])]],
        colWidths=[8 * cm, 8 * cm],
    )
    route_tbl.setStyle(TableStyle([
        ("BOX", (0, 0), (-1, -1), 0.5, colors.lightgrey),
        ("INNERGRID", (0, 0), (-1, -1), 0.3, colors.lightgrey),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ]))
    story.append(route_tbl)
    story.append(Spacer(1, 0.4 * cm))

    # Patient
    story.append(Paragraph("<b>Patient</b>", styles["Heading3"]))
    pat_rows = []
    if p:
        pat_rows.extend([
            ["Patient-ID", p.external_id or f"#{p.id}"],
            ["Sichtungskategorie", f"{t.sk or '—'}"],
            ["Herkunft", p.quelle or "—"],
            ["Eingangssichtung", fmt_dt(p.eingangssichtung)],
            ["Transportbereit", fmt_dt(p.transportbereit)],
            ["Aufenthaltsdauer", f"{p.aufenthaltsdauer_tage or '–'} Tage"],
        ])
    if f and f.anzahl_patienten > 1:
        mitfahrer = [other.patient.external_id or f"#{other.patient_id}"
                     for other in f.transporte if other.id != t.id]
        if mitfahrer:
            pat_rows.append(["Weitere Patienten in Fahrt", ", ".join(mitfahrer)])
    if pat_rows:
        pat_tbl = Table(pat_rows, colWidths=[5 * cm, 11 * cm])
        pat_tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (0, -1), colors.whitesmoke),
            ("BOX", (0, 0), (-1, -1), 0.5, colors.lightgrey),
            ("INNERGRID", (0, 0), (-1, -1), 0.3, colors.lightgrey),
            ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ]))
        story.append(pat_tbl)

    story.append(Spacer(1, 0.6 * cm))
    story.append(Paragraph(
        f"<i>Dokument erzeugt am {_dt.now().strftime('%d.%m.%Y %H:%M')} · "
        f"MANV-Dispatch Prototyp · Hackathon 2026</i>",
        ParagraphStyle("foot", parent=styles["Normal"], textColor=colors.grey, fontSize=8),
    ))

    doc.build(story)
    pdf_bytes = buf.getvalue()

    filename = f"transportauftrag_{t.id}_{t.sk or 'unknown'}.pdf"
    return _Response(
        pdf_bytes, mimetype="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ===================== HL7 ADT Live-Feed =====================

@api_bp.post("/adt/ingest")
def adt_ingest():
    """Nimmt eine HL7 v2 ADT-Nachricht entgegen und verarbeitet sie.

    Akzeptierte Content-Types:
      - application/hl7-v2 (Rohtext)
      - text/plain
      - application/json {"message": "..."}
    """
    ct = request.content_type or ""
    raw = ""
    if "json" in ct.lower():
        payload = request.get_json(silent=True) or {}
        raw = payload.get("message", "")
    else:
        raw = request.get_data(as_text=True)

    if not raw or not raw.strip():
        return jsonify({"error": "Leere HL7-Nachricht"}), 400

    parsed = parse_hl7_adt(raw)
    if not parsed.get("event"):
        return jsonify({"error": "Kein ADT-Event in MSH/EVN gefunden"}), 400

    event = apply_event(parsed)
    return jsonify({
        "event_id": event.id,
        "event_type": event.event_type,
        "sk": event.sk,
        "krankenhaus_id": event.krankenhaus_id,
        "krankenhaus": event.krankenhaus.name if event.krankenhaus else None,
        "processed_ok": event.processed_ok,
        "note": event.process_note,
    }), 200 if event.processed_ok else 202


@api_bp.get("/adt/events")
def adt_events():
    """Letzte Events (für Live-Feed im UI)."""
    limit = min(request.args.get("limit", 50, type=int), 500)
    since_id = request.args.get("since_id", type=int)
    q = AdtEvent.query.order_by(AdtEvent.id.desc())
    if since_id:
        q = q.filter(AdtEvent.id > since_id).order_by(AdtEvent.id.asc())
    events = q.limit(limit).all()
    if since_id:
        events = list(reversed(events))  # newest-first for client
    events.sort(key=lambda e: e.id, reverse=True)
    return jsonify([{
        "id": e.id,
        "event_type": e.event_type,
        "sk": e.sk,
        "patient_hl7_id": e.patient_hl7_id,
        "krankenhaus_id": e.krankenhaus_id,
        "krankenhaus_name": e.krankenhaus.name if e.krankenhaus else None,
        "sending_facility": e.sending_facility_raw,
        "station": e.station,
        "created_at": e.created_at.isoformat() if e.created_at else None,
        "processed_ok": e.processed_ok,
        "note": e.process_note,
    } for e in events])


@api_bp.get("/adt/stats")
def adt_stats():
    from sqlalchemy import func
    total = db.session.query(func.count(AdtEvent.id)).scalar()
    by_type = dict(
        db.session.query(AdtEvent.event_type, func.count(AdtEvent.id))
        .group_by(AdtEvent.event_type).all()
    )
    return jsonify({
        "total": total,
        "by_type": {k: v for k, v in by_type.items() if k},
        "unresolved": db.session.query(func.count(AdtEvent.id))
                        .filter(AdtEvent.processed_ok == False).scalar() or 0,  # noqa: E712
    })


# ===================== Time-Capsule =====================

@api_bp.post("/timecapsule/run")
def timecapsule_run():
    """Startet eine mehrtägige Time-Capsule-Simulation (synchron).

    Body: {days?: int, patients_per_day?: int, grundbelegung_prozent?: int,
           seed?: int, sk_distribution?: [sk1, sk2, sk3]}
    """
    from datetime import datetime as _dt
    from .timecapsule import CapsuleParams, run_capsule

    payload = request.get_json(silent=True) or {}
    start_str = payload.get("start")
    start_dt = None
    if start_str:
        try:
            start_dt = _dt.fromisoformat(start_str)
        except ValueError:
            return jsonify({"error": "start ungültig"}), 400

    dist = payload.get("sk_distribution")
    sk_dist = tuple(dist) if isinstance(dist, list) and len(dist) == 3 else (0.12, 0.28, 0.60)

    bundesland = payload.get("bundesland") or None
    params = CapsuleParams(
        days=max(1, min(int(payload.get("days", 5)), 14)),
        patients_per_day=max(1, min(int(payload.get("patients_per_day", 250)), 2000)),
        sk_distribution=sk_dist,
        start_date=start_dt,
        grundbelegung_prozent=max(0, min(int(payload.get("grundbelegung_prozent", 60)), 100)),
        seed=payload.get("seed"),
        bundesland=bundesland if bundesland else None,
    )
    result = run_capsule(params)
    return jsonify(result)


@api_bp.post("/adt/simulate")
def adt_simulate():
    """Triggert eine interne Live-Simulation: erzeugt N ADT-Events server-seitig."""
    import sys
    sys.path.insert(0, "scripts")
    try:
        from hl7_adt_generator import build_message, stream_messages  # noqa: F401
    except ImportError:
        return jsonify({"error": "Generator-Script nicht verfügbar"}), 500

    payload = request.get_json(silent=True) or {}
    event_type = payload.get("event")   # A01|A03|A08|None(=mix)
    count = max(1, min(int(payload.get("count", 10)), 500))

    applied = []
    for i in range(1, count + 1):
        msg = build_message(event_type or
                            __import__("random").choices(["A01", "A08", "A03"],
                                                         weights=[5, 3, 2])[0], i)
        parsed = parse_hl7_adt(msg)
        ev = apply_event(parsed)
        applied.append({"id": ev.id, "event": ev.event_type, "sk": ev.sk,
                        "krankenhaus": ev.krankenhaus.name if ev.krankenhaus else None})

    return jsonify({"generated": count, "events": applied[-20:]})


# ===================== HERE Traffic =====================

@api_bp.get("/traffic/flow")
def api_traffic_flow():
    lat = request.args.get("lat", type=float, default=48.422)
    lon = request.args.get("lon", type=float, default=9.952)
    radius = request.args.get("radius", type=int, default=5000)
    radius = max(500, min(radius, 20000))
    return jsonify(traffic_flow(lat, lon, radius_m=radius))


@api_bp.get("/traffic/incidents")
def api_traffic_incidents():
    lat = request.args.get("lat", type=float, default=48.422)
    lon = request.args.get("lon", type=float, default=9.952)
    radius = request.args.get("radius", type=int, default=15000)
    radius = max(500, min(radius, 50000))
    return jsonify(traffic_incidents(lat, lon, radius_m=radius))


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

@api_bp.post("/patients/manual")
def add_manual_patient():
    """Einzelnen Patienten manuell erfassen. Fügt ihn einem 'Manuell'-Batch hinzu.

    Body: { sk: 'SK1'|'SK2'|'SK3' (oder IVENA-Varianten),
            external_id?: '...', quelle?: 'Hub Süd', eingangssichtung?: ISO, transportbereit?: ISO,
            hub_name?: 'Hub Süd' }
    """
    from datetime import datetime as _dt
    from .ivena_mapping import map_ivena_to_sk

    payload = request.get_json(silent=True) or {}
    sk = map_ivena_to_sk(payload.get("sk"))
    if not sk:
        return jsonify({"error": "SK-Stufe ungültig (erwartet SK1/SK2/SK3 oder IVENA-Variante)"}), 400

    hub_name = str(payload.get("hub_name") or payload.get("quelle") or "Hub Süd")
    hub = Hub.query.filter_by(name=hub_name).first() or Hub.query.first()
    if hub is None:
        return jsonify({"error": "Kein Hub in DB"}), 500

    # Manuell-Batch finden oder anlegen (pro Tag ein Batch)
    today_str = _dt.utcnow().strftime("%Y-%m-%d")
    batch_name = f"Manuell {today_str}"
    batch = PatientenBatch.query.filter_by(filename=batch_name).first()
    if batch is None:
        batch = PatientenBatch(
            filename=batch_name, hub_id=hub.id, hub_name=hub.name,
            total=0, sk1=0, sk2=0, sk3=0, status="uploaded",
        )
        db.session.add(batch)
        db.session.flush()

    def _parse_dt(v):
        if not v:
            return None
        try:
            return _dt.fromisoformat(v)
        except ValueError:
            return None

    sichtung = _parse_dt(payload.get("eingangssichtung"))
    transport = _parse_dt(payload.get("transportbereit")) or sichtung

    ext_id = payload.get("external_id") or f"MAN_{_dt.utcnow().strftime('%H%M%S')}_{batch.total + 1:03d}"
    patient = Patient(
        batch_id=batch.id,
        external_id=ext_id,
        sk=sk,
        datum=sichtung.date() if sichtung else _dt.utcnow().date(),
        eingangssichtung=sichtung,
        transportbereit=transport,
        quelle=hub.name,
    )
    db.session.add(patient)
    # Batch-Counter updaten
    batch.total += 1
    if sk == "SK1": batch.sk1 += 1
    elif sk == "SK2": batch.sk2 += 1
    elif sk == "SK3": batch.sk3 += 1
    db.session.commit()

    return jsonify({
        "patient_id": patient.id,
        "external_id": patient.external_id,
        "sk": patient.sk,
        "batch_id": batch.id,
        "batch_total": batch.total,
    })


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
    """Löscht alle Batches, Patienten, Transporte, Fahrten und setzt Belegung auf 0."""
    t_count = db.session.query(TransportAuftrag).delete()
    f_count = db.session.query(Fahrt).delete()
    p_count = db.session.query(Patient).delete()
    b_count = db.session.query(PatientenBatch).delete()
    cleared = reset_belegung()
    db.session.commit()
    return jsonify({
        "transports_deleted": t_count,
        "fahrten_deleted": f_count,
        "patients_deleted": p_count,
        "batches_deleted": b_count,
        "belegung_reset": cleared,
    })


# ===================== Fahrten =====================

@api_bp.get("/fahrten")
def list_fahrten():
    batch_id = request.args.get("batch_id", type=int)
    q = Fahrt.query.order_by(
        db.case((Fahrt.transportmittel == "RTW", 1),
                (Fahrt.transportmittel == "KTW", 2), else_=3),
        Fahrt.abfahrt.asc().nulls_last(),
    )
    if batch_id:
        q = q.filter(Fahrt.batch_id == batch_id)
    out = []
    for f in q.all():
        patienten = [{
            "patient_id": t.patient_id,
            "external_id": t.patient.external_id if t.patient else None,
            "sk": t.sk,
            "position": t.bundle_position,
        } for t in f.transporte]
        kh = f.krankenhaus
        out.append({
            "id": f.id,
            "fahrt_code": f.fahrt_code,
            "transportmittel": f.transportmittel,
            "kapazitaet": f.kapazitaet,
            "anzahl_patienten": f.anzahl_patienten,
            "abfahrt": f.abfahrt.isoformat() if f.abfahrt else None,
            "ankunft": f.ankunft.isoformat() if f.ankunft else None,
            "distanz_km": f.distanz_km,
            "dauer_min": f.dauer_min,
            "routing_source": f.routing_source,
            "status": f.status,
            "hub": {"lat": f.hub_lat, "lon": f.hub_lon, "name": f.hub.name if f.hub else None},
            "ziel": None if not kh else {
                "id": kh.id, "name": kh.name, "ort": kh.ort,
                "lat": f.ziel_lat, "lon": f.ziel_lon,
            },
            "patienten": patienten,
        })
    return jsonify(out)


@api_bp.get("/fahrten/<int:fahrt_id>")
def get_fahrt(fahrt_id: int):
    import json as _json
    f = db.session.get(Fahrt, fahrt_id)
    if f is None:
        abort(404)
    actions = None
    if f.here_instructions_json:
        try:
            actions = _json.loads(f.here_instructions_json)
        except Exception:  # noqa: BLE001
            actions = None
    geojson = None
    if f.route_geojson:
        try:
            geojson = _json.loads(f.route_geojson)
        except Exception:  # noqa: BLE001
            geojson = None
    patienten = []
    for t in f.transporte:
        p = t.patient
        patienten.append({
            "patient_id": t.patient_id,
            "external_id": p.external_id if p else None,
            "sk": t.sk,
            "position": t.bundle_position,
            "quelle": p.quelle if p else None,
            "aufenthaltsdauer_tage": p.aufenthaltsdauer_tage if p else None,
        })
    kh = f.krankenhaus
    return jsonify({
        "id": f.id,
        "fahrt_code": f.fahrt_code,
        "transportmittel": f.transportmittel,
        "kapazitaet": f.kapazitaet,
        "abfahrt": f.abfahrt.isoformat() if f.abfahrt else None,
        "ankunft": f.ankunft.isoformat() if f.ankunft else None,
        "distanz_km": f.distanz_km,
        "dauer_min": f.dauer_min,
        "routing_source": f.routing_source,
        "status": f.status,
        "hub": {"lat": f.hub_lat, "lon": f.hub_lon, "name": f.hub.name if f.hub else None},
        "ziel": None if not kh else {
            "id": kh.id, "name": kh.name, "ort": kh.ort, "plz": kh.plz,
            "strasse": kh.strasse, "hausnummer": kh.hausnummer,
            "telefon": kh.telefon,
            "lat": f.ziel_lat, "lon": f.ziel_lon,
        },
        "patienten": patienten,
        "route_geojson": geojson,
        "actions": actions,
        "here_polyline": f.here_polyline,
    })


# ===================== Detail-Endpoints =====================

def _patient_to_dict(p: Patient) -> dict:
    kh = p.assigned_krankenhaus
    return {
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
        "ziel": None if not kh else {
            "id": kh.id, "name": kh.name, "ort": kh.ort, "plz": kh.plz,
            "strasse": kh.strasse, "hausnummer": kh.hausnummer,
            "bundesland": kh.bundesland, "lat": kh.lat, "lon": kh.lon,
            "telefon": kh.telefon, "website": kh.website,
            "betten": kh.betten, "sk_max": kh.sk_max,
        },
        "assigned_at": p.assigned_at.isoformat() if p.assigned_at else None,
        "note": p.note,
    }


@api_bp.get("/patients/<int:patient_id>")
def get_patient(patient_id: int):
    p = db.session.get(Patient, patient_id)
    if p is None:
        abort(404)
    out = _patient_to_dict(p)
    # Zugehörigen Transportauftrag
    t = TransportAuftrag.query.filter_by(patient_id=p.id).first()
    if t:
        out["transport"] = {
            "id": t.id,
            "transportmittel": t.transportmittel,
            "abfahrt": t.abfahrt.isoformat() if t.abfahrt else None,
            "ankunft": t.ankunft.isoformat() if t.ankunft else None,
            "distanz_km": t.distanz_km,
            "dauer_min": t.dauer_min,
            "routing_source": t.routing_source,
            "status": t.status,
        }
    return jsonify(out)


@api_bp.get("/transports/<int:transport_id>")
def get_transport(transport_id: int):
    import json as _json
    t = db.session.get(TransportAuftrag, transport_id)
    if t is None:
        abort(404)
    kh = t.krankenhaus
    p = t.patient
    actions = None
    if t.here_instructions_json:
        try:
            actions = _json.loads(t.here_instructions_json)
        except Exception:  # noqa: BLE001
            actions = None
    geojson = None
    if t.route_geojson:
        try:
            geojson = _json.loads(t.route_geojson)
        except Exception:  # noqa: BLE001
            geojson = None
    return jsonify({
        "id": t.id,
        "status": t.status,
        "sk": t.sk,
        "transportmittel": t.transportmittel,
        "abfahrt": t.abfahrt.isoformat() if t.abfahrt else None,
        "ankunft": t.ankunft.isoformat() if t.ankunft else None,
        "distanz_km": t.distanz_km,
        "dauer_min": t.dauer_min,
        "routing_source": t.routing_source,
        "hub": {"lat": t.hub_lat, "lon": t.hub_lon,
                "name": t.hub.name if t.hub else None,
                "ort": t.hub.ort if t.hub else None},
        "ziel": None if not kh else {
            "id": kh.id, "name": kh.name, "ort": kh.ort, "plz": kh.plz,
            "strasse": kh.strasse, "hausnummer": kh.hausnummer,
            "bundesland": kh.bundesland,
            "lat": t.ziel_lat, "lon": t.ziel_lon,
            "telefon": kh.telefon, "website": kh.website,
        },
        "patient": None if not p else {
            "id": p.id, "external_id": p.external_id,
            "sk": p.sk, "quelle": p.quelle,
            "eingangssichtung": p.eingangssichtung.isoformat() if p.eingangssichtung else None,
            "transportbereit": p.transportbereit.isoformat() if p.transportbereit else None,
            "aufenthaltsdauer_tage": p.aufenthaltsdauer_tage,
        },
        "route_geojson": geojson,
        "actions": actions,
        "here_polyline": t.here_polyline,
        "erzeugt_am": t.erzeugt_am.isoformat() if t.erzeugt_am else None,
    })


@api_bp.get("/krankenhaus/<int:kh_id>")
def get_krankenhaus(kh_id: int):
    kh = db.session.get(Krankenhaus, kh_id)
    if kh is None:
        abort(404)
    bel = db.session.get(KrankenhausBelegung, kh_id)
    return jsonify({
        "id": kh.id, "name": kh.name,
        "strasse": kh.strasse, "hausnummer": kh.hausnummer,
        "plz": kh.plz, "ort": kh.ort, "bundesland": kh.bundesland,
        "lat": kh.lat, "lon": kh.lon,
        "telefon": kh.telefon, "email": kh.email, "website": kh.website,
        "betten": kh.betten, "sk_max": kh.sk_max,
        "kann_sk1": kh.kann_sk1, "kann_sk2": kh.kann_sk2, "kann_sk3": kh.kann_sk3,
        "hat_intensivmedizin": kh.hat_intensivmedizin,
        "hat_notaufnahme": kh.hat_notaufnahme,
        "hat_bg_zulassung": kh.hat_bg_zulassung,
        "hat_radiologie": kh.hat_radiologie,
        "universitaet": kh.universitaet,
        "traeger_name": kh.traeger_name,
        "fachabteilungen": kh.fachabteilungen,
        "apparative_ausstattung": kh.apparative_ausstattung,
        "ausgeschlossen": kh.ausgeschlossen,
        "ausschluss_grund": kh.ausschluss_grund,
        "belegung": None if not bel else {
            "kapazitaet_sk1": bel.kapazitaet_sk1, "kapazitaet_sk2": bel.kapazitaet_sk2, "kapazitaet_sk3": bel.kapazitaet_sk3,
            "belegung_sk1": bel.belegung_sk1, "belegung_sk2": bel.belegung_sk2, "belegung_sk3": bel.belegung_sk3,
            "frei_sk1": bel.frei("SK1"), "frei_sk2": bel.frei("SK2"), "frei_sk3": bel.frei("SK3"),
            "vorbelegung_prozent": bel.vorbelegung_prozent,
        },
    })


@api_bp.get("/krankenhaus/<int:kh_id>/incoming")
def get_krankenhaus_incoming(kh_id: int):
    """Patienten die bei dieser Klinik ankommen, sortiert nach Ankunftszeit."""
    q = (
        db.session.query(Patient, TransportAuftrag)
        .outerjoin(TransportAuftrag, TransportAuftrag.patient_id == Patient.id)
        .filter(Patient.assigned_krankenhaus_id == kh_id)
        .order_by(TransportAuftrag.ankunft.asc().nulls_last(),
                  db.case((Patient.sk == "SK1", 1), (Patient.sk == "SK2", 2), else_=3))
    )
    out = []
    for p, t in q.all():
        out.append({
            "patient_id": p.id,
            "external_id": p.external_id,
            "sk": p.sk,
            "quelle": p.quelle,
            "aufenthaltsdauer_tage": p.aufenthaltsdauer_tage,
            "eingangssichtung": p.eingangssichtung.isoformat() if p.eingangssichtung else None,
            "transport_id": t.id if t else None,
            "transportmittel": t.transportmittel if t else None,
            "abfahrt": t.abfahrt.isoformat() if t and t.abfahrt else None,
            "ankunft": t.ankunft.isoformat() if t and t.ankunft else None,
            "distanz_km": t.distanz_km if t else p.distanz_km,
            "dauer_min": t.dauer_min if t else None,
        })
    return jsonify(out)


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
