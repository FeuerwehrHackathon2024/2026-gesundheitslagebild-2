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
