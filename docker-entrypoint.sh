#!/bin/sh
set -e

# Seed the database once before gunicorn workers start.
# Retries while waiting for Postgres to become available.
echo "[entrypoint] Running startup DB init..."
python -c "
import logging, sys, time
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
from sqlalchemy.exc import OperationalError

for attempt in range(1, 20):
    try:
        from hackathon import create_app
        app = create_app()
        print(f'[entrypoint] Init OK on attempt {attempt}.')
        sys.exit(0)
    except OperationalError as e:
        print(f'[entrypoint] DB not ready (attempt {attempt}/20): {e}')
        time.sleep(2)
    except Exception as e:
        print(f'[entrypoint] Unexpected error: {type(e).__name__}: {e}')
        raise
print('[entrypoint] Giving up waiting for DB.')
sys.exit(1)
"

echo "[entrypoint] Starting gunicorn..."
exec "$@"
