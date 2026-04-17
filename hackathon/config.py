from pathlib import Path
import os


BASE_DIR = Path(__file__).resolve().parent.parent


class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-key")
    SQLALCHEMY_DATABASE_URI = os.getenv(
        "DATABASE_URL",
        f"sqlite:///{BASE_DIR / 'db.sqlite3'}",
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False

