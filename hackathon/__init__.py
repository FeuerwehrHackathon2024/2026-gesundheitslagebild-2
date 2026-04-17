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

    # Import models after extension initialization so SQLAlchemy can register metadata.
    from . import models  # noqa: F401

    return app

