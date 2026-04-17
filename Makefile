.PHONY: help up down restart build logs ps shell reseed clean local-dev

help:
	@echo "Make-Targets:"
	@echo "  up        - docker compose up -d --build (App starten)"
	@echo "  down      - docker compose down"
	@echo "  restart   - App-Container neustarten (DB bleibt)"
	@echo "  build     - Image neu bauen"
	@echo "  logs      - Logs der App folgen"
	@echo "  ps        - Container-Status"
	@echo "  shell     - Bash im App-Container"
	@echo "  reseed    - Krankenhaus-Tabelle per CLI neu befüllen"
	@echo "  clean     - Container + Volumes löschen (DANGER: löscht DB!)"
	@echo "  local-dev - lokales SQLite löschen und App direkt starten"

up:
	docker compose up -d --build

down:
	docker compose down

restart:
	docker compose restart web

build:
	docker compose build --no-cache

logs:
	docker compose logs -f web

ps:
	docker compose ps

shell:
	docker compose exec web bash

reseed:
	docker compose exec web flask --app app reseed-krankenhaus

clean:
	docker compose down -v

local-dev:
	rm -f db.sqlite3
	poetry run python app.py
