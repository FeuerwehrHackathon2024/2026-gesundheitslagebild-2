# Flask + SQLAlchemy Skeleton

This project now follows a generic Flask app-factory structure with SQLAlchemy and Flask-Migrate.

## Project layout

- `app.py`: thin entrypoint for local runs.
- `hackathon/__init__.py`: app factory (`create_app`).
- `hackathon/config.py`: central config class.
- `hackathon/extensions.py`: extension instances (`db`, `migrate`).
- `hackathon/models.py`: SQLAlchemy models.
- `hackathon/routes.py`: blueprint routes.
- `templates/`, `static/`: basic UI assets.
- `smoke_test.py`: tiny runtime check.

## Quick start

```bash
poetry install
poetry run python app.py
```

## Initialize database migrations

```bash
poetry run flask --app app db init
poetry run flask --app app db migrate -m "initial"
poetry run flask --app app db upgrade
```

## Run smoke test

```bash
poetry run python smoke_test.py
```

