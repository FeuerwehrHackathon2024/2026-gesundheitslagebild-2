"""JSON-API für das Dashboard."""

from __future__ import annotations

from flask import Blueprint, abort, jsonify

from .extensions import db
from .models import Hub, Krankenhaus


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
)


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


@api_bp.get("/stats")
def stats():
    total = Krankenhaus.query.filter(Krankenhaus.lat.isnot(None)).count()
    sk1 = Krankenhaus.query.filter(Krankenhaus.lat.isnot(None), Krankenhaus.kann_sk1 == True).count()
    sk2 = Krankenhaus.query.filter(Krankenhaus.lat.isnot(None), Krankenhaus.kann_sk2 == True).count()
    sk3 = Krankenhaus.query.filter(Krankenhaus.lat.isnot(None), Krankenhaus.kann_sk3 == True).count()
    return jsonify({"total": total, "sk1": sk1, "sk2": sk2, "sk3": sk3})
