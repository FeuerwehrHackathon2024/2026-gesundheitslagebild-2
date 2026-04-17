"""IVENA → SK Mapping.

IVENA (Interdisziplinärer Versorgungsnachweis) nutzt im MANV-Fall
Sichtungskategorien nach SKN/MANV-Standard. Daneben gibt es im regulären
IVENA-Betrieb Versorgungsstufen und Fachbereichs-Ampeln.

Dieses Modul stellt eine zentrale Zuordnung zur Verfügung:
1. IVENA MANV-Kategorie ↔ SK1/SK2/SK3 (1:1 Abbildung)
2. Farbcode (rot/gelb/grün/blau) ↔ SK
3. Römische Ziffern (I/II/III/IV) ↔ SK
4. Zusätzliche Synonyme / IVENA-Klartext
5. IVENA-Versorgungsstufen ↔ welche SK eine Klinik versorgen kann
6. IVENA-Fachbereiche ↔ welche SK bevorzugt dort ankommen
"""

from __future__ import annotations

# Direkte SK-Zuweisungen (eingehend — aus IVENA-XLSX oder Leitstellen-Input)
# Die Strings links kommen typischerweise in der "SK"-Spalte einer IVENA-Liste vor.
IVENA_SK_LOOKUP: dict[str, str] = {
    # SK I / rot / lebensgefährlich
    "sk1": "SK1", "sk 1": "SK1", "sk i": "SK1", "ski": "SK1", "i": "SK1",
    "kat1": "SK1", "kat 1": "SK1", "kat i": "SK1",
    "rot": "SK1", "red": "SK1", "t1": "SK1",
    "lebensgefahr": "SK1", "akut lebensbedrohlich": "SK1", "vital bedroht": "SK1",
    "notfall": "SK1", "schockraum": "SK1",

    # SK II / gelb / schwer verletzt
    "sk2": "SK2", "sk 2": "SK2", "sk ii": "SK2", "skii": "SK2", "ii": "SK2",
    "kat2": "SK2", "kat 2": "SK2", "kat ii": "SK2",
    "gelb": "SK2", "yellow": "SK2", "t2": "SK2",
    "schwer verletzt": "SK2", "dringlich": "SK2", "urgent": "SK2",

    # SK III / grün / leicht verletzt
    "sk3": "SK3", "sk 3": "SK3", "sk iii": "SK3", "skiii": "SK3", "iii": "SK3",
    "kat3": "SK3", "kat 3": "SK3", "kat iii": "SK3",
    "gruen": "SK3", "grün": "SK3", "green": "SK3", "t3": "SK3",
    "leicht verletzt": "SK3", "leicht": "SK3", "unverletzt": "SK3",

    # SK IV / blau / abwartend / palliativ → mappen wir auf SK3 (leichteste
    # Stufe, da wir keine separate Palliativ-Versorgung modellieren)
    "sk4": "SK3", "sk iv": "SK3", "iv": "SK3", "kat iv": "SK3",
    "blau": "SK3", "schwarz": "SK3", "palliativ": "SK3", "exitus": "SK3",
}


# Farb-Definition der Sichtungskategorien (für UI + Legende)
SK_FARBE = {
    "SK1": {"farbe": "rot", "hex": "#b71c1c", "kuerzel": "T1"},
    "SK2": {"farbe": "gelb", "hex": "#f57c00", "kuerzel": "T2"},
    "SK3": {"farbe": "grün", "hex": "#fbc02d", "kuerzel": "T3"},
}


# IVENA-Versorgungsstufen (auf Klinik-Seite, nicht Patient):
# definiert, welche SK-Stufen in welcher Klinik-Kategorie versorgt werden können.
# Reihenfolge aufsteigend: 1 = Basis, 4 = Maximalversorger.
IVENA_VERSORGUNGSSTUFE_TO_SK = {
    "Basisnotfallversorgung (Stufe 1)": ["SK3"],
    "Erweiterte Notfallversorgung (Stufe 2)": ["SK2", "SK3"],
    "Umfassende Notfallversorgung (Stufe 3)": ["SK1", "SK2", "SK3"],
    "Maximalversorger / Uniklinik (Stufe 4)": ["SK1", "SK2", "SK3"],
    "Ohne Notfallversorgung": [],
}


# IVENA-Fachbereichs-Ampeln → welche SK-Patienten landen typischerweise dort.
# (Grobe Heuristik, keine klinische Leitlinie.)
IVENA_FACHBEREICH_TO_SK = {
    "Schockraum": ["SK1"],
    "Intensivstation": ["SK1"],
    "Neurochirurgie": ["SK1"],
    "Traumatologie / Unfallchirurgie": ["SK1", "SK2"],
    "Herzkatheter": ["SK1"],
    "Stroke Unit": ["SK1"],
    "Allgemeinchirurgie": ["SK2", "SK3"],
    "Innere Medizin": ["SK2", "SK3"],
    "Orthopädie": ["SK2", "SK3"],
    "HNO": ["SK3"],
    "Augenklinik": ["SK3"],
    "Ambulanz / Notfallpraxis": ["SK3"],
}


# Transportmittel-Katalog — Kapazität laut Vorgabe
TRANSPORTMITTEL_KATALOG = {
    "RTW":  {"name": "Rettungswagen",           "besatzung": "RettAss + NotArzt",        "kapazitaet": 1, "symbol": "bi-bandaid-fill"},
    "KTW":  {"name": "Krankentransportwagen",   "besatzung": "Rettungssanitäter",        "kapazitaet": 1, "symbol": "bi-ambulance"},
    "BTW":  {"name": "Behindertentransportwagen", "besatzung": "qualifizierter Fahrer", "kapazitaet": 2, "symbol": "bi-universal-access"},
    "Taxi": {"name": "Taxi / Patiententransport", "besatzung": "Fahrer",                 "kapazitaet": 2, "symbol": "bi-car-front-fill"},
}

# Transportmittel-Empfehlung je SK (Default-Zuordnung vom Hub zur Klinik)
#   SK1 → RTW (medizinisch notwendig, 1 Patient)
#   SK2 → KTW (qualifizierte Versorgung, 1 Patient — BTW als Alternative möglich)
#   SK3 → Taxi (mobil, 2 Patienten bündelbar — BTW ebenfalls)
SK_TRANSPORTMITTEL = {
    "SK1": {"code": "RTW",  "name": TRANSPORTMITTEL_KATALOG["RTW"]["name"],
            "besatzung": TRANSPORTMITTEL_KATALOG["RTW"]["besatzung"], "prio": 1,
            "alternativen": []},
    "SK2": {"code": "KTW",  "name": TRANSPORTMITTEL_KATALOG["KTW"]["name"],
            "besatzung": TRANSPORTMITTEL_KATALOG["KTW"]["besatzung"], "prio": 2,
            "alternativen": ["BTW"]},
    "SK3": {"code": "Taxi", "name": TRANSPORTMITTEL_KATALOG["Taxi"]["name"],
            "besatzung": TRANSPORTMITTEL_KATALOG["Taxi"]["besatzung"], "prio": 3,
            "alternativen": ["BTW"]},
}


def map_ivena_to_sk(value) -> str | None:
    """Normalisiert eine IVENA-Angabe auf 'SK1' / 'SK2' / 'SK3'.

    Gibt None zurück, wenn keine Zuordnung möglich.
    """
    if value is None:
        return None
    s = str(value).strip().lower()
    if not s:
        return None
    # Direkter Treffer
    hit = IVENA_SK_LOOKUP.get(s)
    if hit:
        return hit
    # Entferne Leer-/Sonderzeichen für robusteren Match
    s_norm = "".join(ch for ch in s if ch.isalnum())
    for key, val in IVENA_SK_LOOKUP.items():
        if "".join(ch for ch in key if ch.isalnum()) == s_norm:
            return val
    # Heuristik: enthält "1"/"2"/"3" am Ende
    if s_norm.endswith("1"):
        return "SK1"
    if s_norm.endswith("2"):
        return "SK2"
    if s_norm.endswith("3"):
        return "SK3"
    return None


def sk_to_ivena_summary(sk: str) -> dict:
    """Liefert eine IVENA-Ansicht (Farbe, römisch, T-Code, Transportmittel) für eine SK-Stufe."""
    sk = (sk or "").upper()
    farbe = SK_FARBE.get(sk, {})
    roman = {"SK1": "I", "SK2": "II", "SK3": "III"}.get(sk)
    return {
        "sk": sk,
        "roman": roman,
        "farbe": farbe.get("farbe"),
        "hex": farbe.get("hex"),
        "t_code": farbe.get("kuerzel"),
        "transportmittel": SK_TRANSPORTMITTEL.get(sk),
    }
