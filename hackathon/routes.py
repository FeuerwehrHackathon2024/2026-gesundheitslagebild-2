from datetime import datetime

from flask import Blueprint, Response, jsonify, render_template, request

from .simulator import SimInput, build_xlsx


main_bp = Blueprint("main", __name__)


@main_bp.get("/")
def dashboard() -> str:
    return render_template("dashboard.html")


@main_bp.get("/simulator")
def simulator_page() -> str:
    return render_template("simulator.html")


@main_bp.get("/batches")
def batches_page() -> str:
    return render_template("batches.html")


@main_bp.get("/patients")
def patients_page() -> str:
    return render_template("patients.html")


@main_bp.get("/patients/<int:patient_id>")
def patient_detail_page(patient_id: int):
    return render_template("patient_detail.html", patient_id=patient_id)


@main_bp.get("/transports/<int:transport_id>")
def transport_detail_page(transport_id: int):
    return render_template("transport_detail.html", transport_id=transport_id)


@main_bp.get("/fahrten")
def fahrten_page() -> str:
    return render_template("fahrten.html")


@main_bp.get("/fahrten/<int:fahrt_id>")
def fahrt_detail_page(fahrt_id: int):
    return render_template("fahrt_detail.html", fahrt_id=fahrt_id)


@main_bp.get("/krankenhaus/<int:kh_id>")
def krankenhaus_detail_page(kh_id: int):
    return render_template("krankenhaus_detail.html", kh_id=kh_id)


@main_bp.get("/belegung")
def belegung_page() -> str:
    return render_template("belegung.html")


@main_bp.get("/ivena-matching")
def ivena_matching_page() -> str:
    from .ivena_mapping import (
        IVENA_SK_LOOKUP, SK_FARBE, IVENA_VERSORGUNGSSTUFE_TO_SK,
        IVENA_FACHBEREICH_TO_SK, SK_TRANSPORTMITTEL, TRANSPORTMITTEL_KATALOG,
    )
    by_sk: dict[str, list[str]] = {"SK1": [], "SK2": [], "SK3": []}
    for src, dst in IVENA_SK_LOOKUP.items():
        by_sk.setdefault(dst, []).append(src)
    return render_template(
        "ivena_matching.html",
        sk_farbe=SK_FARBE,
        sk_transport=SK_TRANSPORTMITTEL,
        transportmittel_katalog=TRANSPORTMITTEL_KATALOG,
        ivena_by_sk=by_sk,
        versorgungsstufe=IVENA_VERSORGUNGSSTUFE_TO_SK,
        fachbereich=IVENA_FACHBEREICH_TO_SK,
    )


@main_bp.get("/info")
def info_page() -> str:
    return render_template("info.html")


@main_bp.get("/adt")
def adt_page() -> str:
    return render_template("adt.html")


@main_bp.get("/timecapsule")
def timecapsule_page() -> str:
    return render_template("timecapsule.html")


@main_bp.post("/simulator/generate")
def simulator_generate():
    payload = request.get_json(silent=True) or {}
    try:
        start_date = datetime.fromisoformat(payload.get("start_date", ""))
    except ValueError:
        return jsonify({"error": "start_date ungültig (ISO-Format erwartet)"}), 400

    seed = payload.get("seed")
    inp = SimInput(
        sk1=int(payload.get("sk1", 0)),
        sk2=int(payload.get("sk2", 0)),
        sk3=int(payload.get("sk3", 0)),
        start_date=start_date,
        days=int(payload.get("days", 1)),
        hub_name=str(payload.get("hub_name") or "Hub Süd"),
        start_hour=int(payload.get("start_hour", 8)),
        end_hour=int(payload.get("end_hour", 20)),
        seed=int(seed) if seed is not None and seed != "" else None,
    )
    if inp.total == 0:
        return jsonify({"error": "Mindestens ein SK-Wert > 0 erforderlich"}), 400

    xlsx_bytes = build_xlsx(inp)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"manv_sim_{inp.total}p_{inp.days}t_{stamp}.xlsx"
    return Response(
        xlsx_bytes,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
