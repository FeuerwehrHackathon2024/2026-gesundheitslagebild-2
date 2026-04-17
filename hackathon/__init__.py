from flask import Flask

from .config import Config
from .extensions import db, migrate


def create_app(config_class: type[Config] = Config) -> Flask:
    app = Flask(__name__, template_folder="../templates", static_folder="../static")
    app.config.from_object(config_class)

    db.init_app(app)
    migrate.init_app(app, db)

    from .routes import main_bp

    app.register_blueprint(main_bp)

    from . import models  # noqa: F401

    from .seed import seed_if_empty, seed_krankenhaus

    @app.cli.command("seed-krankenhaus")
    def _seed_cmd():
        """Krankenhaus-CSV in die DB importieren (nur wenn leer)."""
        n = seed_krankenhaus(force=False)
        click_echo = app.cli.echo if hasattr(app.cli, "echo") else print
        click_echo(f"Eingefügt: {n}")

    @app.cli.command("reseed-krankenhaus")
    def _reseed_cmd():
        """Tabelle leeren und komplett neu aus CSV befüllen."""
        n = seed_krankenhaus(force=True)
        print(f"Reseeded: {n}")

    if not app.config.get("TESTING"):
        seed_if_empty(app)

    return app
