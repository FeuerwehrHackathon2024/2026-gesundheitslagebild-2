"""Minimaler HERE Maps v8 Routing-Client.

Nutzt `HERE_API_KEY` aus der Umgebung. Fällt sauber auf Haversine-Schätzung
zurück, wenn kein Key gesetzt oder das API nicht erreichbar ist.
"""

from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass
from typing import Any

import requests
try:
    import flexpolyline as fp
except Exception:  # pragma: no cover
    fp = None

log = logging.getLogger(__name__)

HERE_BASE = "https://router.hereapi.com/v8/routes"
HERE_API_KEY = os.environ.get("HERE_API_KEY", "").strip()


@dataclass
class RouteResult:
    source: str                 # "here" | "haversine"
    distance_km: float
    duration_min: float
    polyline: str | None        # HERE flexpolyline
    geojson: dict | None        # {type: LineString, coordinates: [[lon,lat], ...]}
    actions: list[dict] | None  # Navigation-Schritte


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def _fallback(origin, dest) -> RouteResult:
    lat1, lon1 = origin
    lat2, lon2 = dest
    d_km = _haversine_km(lat1, lon1, lat2, lon2)
    # Grobe Fahrzeit: Luftlinie × 1.3 (Umwegfaktor) bei Ø 70 km/h
    duration_min = (d_km * 1.3) / 70.0 * 60.0
    return RouteResult(
        source="haversine",
        distance_km=round(d_km, 2),
        duration_min=round(duration_min, 1),
        polyline=None,
        geojson={"type": "LineString", "coordinates": [[lon1, lat1], [lon2, lat2]]},
        actions=None,
    )


def fetch_route(
    origin: tuple[float, float],
    dest: tuple[float, float],
    transport_mode: str = "car",
    timeout: float = 5.0,
) -> RouteResult:
    """Holt Route via HERE v8. Bei Fehler → Haversine-Fallback."""
    if not HERE_API_KEY:
        return _fallback(origin, dest)

    params = {
        "transportMode": transport_mode,
        "origin": f"{origin[0]},{origin[1]}",
        "destination": f"{dest[0]},{dest[1]}",
        "return": "polyline,summary,actions,instructions",
        "apikey": HERE_API_KEY,
    }
    try:
        resp = requests.get(HERE_BASE, params=params, timeout=timeout)
        if resp.status_code != 200:
            log.warning("HERE route HTTP %s: %s", resp.status_code, resp.text[:200])
            return _fallback(origin, dest)
        data = resp.json()
    except Exception as exc:  # noqa: BLE001
        log.warning("HERE route error: %s", exc)
        return _fallback(origin, dest)

    routes = data.get("routes") or []
    if not routes:
        return _fallback(origin, dest)
    route = routes[0]
    sections = route.get("sections") or []
    if not sections:
        return _fallback(origin, dest)

    total_distance = 0
    total_duration = 0
    all_coords: list[list[float]] = []
    all_actions: list[dict] = []
    poly_combined = []
    for sec in sections:
        summary = sec.get("summary", {})
        total_distance += int(summary.get("length", 0))
        total_duration += int(summary.get("duration", 0))
        poly = sec.get("polyline")
        if poly and fp is not None:
            try:
                coords_latlon = fp.decode(poly)  # [(lat, lon), ...]
                all_coords.extend([[lon, lat] for (lat, lon) in coords_latlon])
                poly_combined.append(poly)
            except Exception as exc:  # noqa: BLE001
                log.warning("flexpolyline decode: %s", exc)
        for act in sec.get("actions", []) or []:
            all_actions.append({
                "action": act.get("action"),
                "instruction": act.get("instruction"),
                "duration": act.get("duration"),
                "length": act.get("length"),
            })

    return RouteResult(
        source="here",
        distance_km=round(total_distance / 1000.0, 2),
        duration_min=round(total_duration / 60.0, 1),
        polyline=poly_combined[0] if poly_combined else None,
        geojson={"type": "LineString", "coordinates": all_coords} if all_coords else None,
        actions=all_actions,
    )
