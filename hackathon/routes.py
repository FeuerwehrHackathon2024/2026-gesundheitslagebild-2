from flask import Blueprint, render_template

from .models import Krankenhaus, User


main_bp = Blueprint("main", __name__)


@main_bp.get("/")
def index() -> str:
    return render_template(
        "index.html",
        user_count=User.query.count(),
        krankenhaus_count=Krankenhaus.query.count(),
    )
