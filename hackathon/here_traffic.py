"""HERE Traffic API v7 Client — Flow + Incidents.

Docs: https://developer.here.com/documentation/traffic-api/api-reference.html

Liefert:
  - `traffic_flow(lat, lon, radius_m)` → Segmente mit Speed, Jam-Factor, Confidence
  - `traffic_incidents(lat, lon, radius_m)` → Unfälle / Baustellen / Sperrungen

Falls kein HERE_API_KEY gesetzt ist, gibt der Client leere Ergebnisse zurück
(damit das UI nicht abschmiert).
"""

from __future__ import annotations

import logging
import os
from typing import Any

import requests

log = logging.getLogger(__name__)

BASE_FLOW = "https://data.traffic.hereapi.com/v7/flow"
BASE_INC = "https://data.traffic.hereapi.com/v7/incidents"


def _key() -> str:
    return os.environ.get("HERE_API_KEY", "").strip()


def _coords_from_shape(shape: dict) -> list[list[float]]:
    """Shape aus HERE-Response in GeoJSON-LineString-Koordinaten [[lon,lat], ...] wandeln."""
    out: list[list[float]] = []
    for link in shape.get("links", []):
        pts = link.get("points") or []
        for p in pts:
            lat = p.get("lat")
            lng = p.get("lng")
            if lat is not None and lng is not None:
                out.append([lng, lat])
    return out


def traffic_flow(lat: float, lon: float, radius_m: int = 5000, timeout: float = 6.0) -> dict:
    """Aktuelle Verkehrslage im Umkreis.

    Rückgabe: {"segments": [{geometry, speed, free_flow, jam_factor, traversability}], "count": N}
    """
    key = _key()
    if not key:
        return {"segments": [], "count": 0, "error": "HERE_API_KEY fehlt"}
    params = {
        "in": f"circle:{lat},{lon};r={int(radius_m)}",
        "locationReferencing": "shape",
        "apikey": key,
    }
    try:
        r = requests.get(BASE_FLOW, params=params, timeout=timeout)
        if r.status_code != 200:
            return {"segments": [], "count": 0,
                    "error": f"HERE Flow HTTP {r.status_code}",
                    "detail": r.text[:300]}
        data = r.json()
    except Exception as exc:  # noqa: BLE001
        log.warning("HERE flow error: %s", exc)
        return {"segments": [], "count": 0, "error": str(exc)}

    segments = []
    for res in (data.get("results") or []):
        cf = res.get("currentFlow", {})
        loc = res.get("location", {})
        coords = _coords_from_shape(loc.get("shape", {}))
        if len(coords) < 2:
            continue
        segments.append({
            "coords": coords,
            "speed": cf.get("speed"),
            "speed_uncapped": cf.get("speedUncapped"),
            "free_flow": cf.get("freeFlow"),
            "jam_factor": cf.get("jamFactor"),
            "confidence": cf.get("confidence"),
            "traversability": cf.get("traversability"),
            "description": (loc.get("description", {}) or {}).get("value")
                           if isinstance(loc.get("description"), dict)
                           else (str(loc.get("description")) if loc.get("description") else None),
        })
    return {"segments": segments, "count": len(segments)}


def traffic_incidents(lat: float, lon: float, radius_m: int = 10000, timeout: float = 6.0) -> dict:
    """Verkehrsstörungen (Unfälle, Baustellen, Sperrungen) im Umkreis."""
    key = _key()
    if not key:
        return {"incidents": [], "count": 0, "error": "HERE_API_KEY fehlt"}
    params = {
        "in": f"circle:{lat},{lon};r={int(radius_m)}",
        "locationReferencing": "shape",
        "apikey": key,
    }
    try:
        r = requests.get(BASE_INC, params=params, timeout=timeout)
        if r.status_code != 200:
            return {"incidents": [], "count": 0,
                    "error": f"HERE Incidents HTTP {r.status_code}",
                    "detail": r.text[:300]}
        data = r.json()
    except Exception as exc:  # noqa: BLE001
        log.warning("HERE incidents error: %s", exc)
        return {"incidents": [], "count": 0, "error": str(exc)}

    incidents = []
    for res in (data.get("results") or []):
        inc = res.get("incidentDetails", {})
        loc = res.get("location", {})
        coords = _coords_from_shape(loc.get("shape", {}))
        if not coords:
            continue
        incidents.append({
            "id": inc.get("id"),
            "type": inc.get("type"),
            "description": (inc.get("description") or {}).get("value")
                           or (inc.get("summary") or {}).get("value"),
            "road_closed": inc.get("roadClosed"),
            "criticality": inc.get("criticality"),
            "start_time": inc.get("startTime"),
            "end_time": inc.get("endTime"),
            "coords": coords,
            "lat": coords[0][1] if coords else None,
            "lon": coords[0][0] if coords else None,
        })
    return {"incidents": incidents, "count": len(incidents)}
