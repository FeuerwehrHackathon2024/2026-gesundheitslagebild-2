from flask import Blueprint, render_template


main_bp = Blueprint("main", __name__)


@main_bp.get("/")
def dashboard() -> str:
    return render_template("dashboard.html")
