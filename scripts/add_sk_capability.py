"""
Reichert krankenhaeuser_merged.csv um SK1/SK2/SK3-Versorgungsfähigkeit an.

Sichtungskategorien (MANV):
  SK1 = vital gefährdet  -> braucht Schockraum / OP / Intensivmedizin / CT
  SK2 = schwer verletzt  -> stationäre Versorgung, OP-fähig, Notaufnahme
  SK3 = leicht verletzt  -> ambulant/basic, Sichtung & Erstversorgung

Ableitung (primär aus amtlichen Flags, Fallback aus Heuristiken):

  kann_sk1 wenn:
    (hat_intensivmedizin AND (hat_notaufnahme OR chirurgische Fachabteilung))
    ODER Universitätsklinik / universitätsangebunden + >=300 Betten
    ODER Name enthält "Universitätsklinik|Uniklinik|Klinikum ... Zentral"
    ODER importance_score >= 80 UND >=300 Betten

  kann_sk2 wenn:
    kann_sk1
    ODER hat_notaufnahme ODER hat_bg_zulassung ODER hat_intensivmedizin
    ODER chirurgische Fachabteilung + Betten >= 50
    ODER Name enthält "Krankenhaus|Klinikum|Klinik|Hospital" + Betten >= 50

  kann_sk3 wenn:
    kann_sk2
    ODER Betten >= 1
    ODER name enthält "Krankenhaus|Klinik|Hospital|Medizin..."
    ODER Fachabteilungen vorhanden
"""

import pandas as pd
import numpy as np
import re

IN_PATH  = "/Users/christianruff/hackathon/krankenhaeuser_merged.csv"
OUT_PATH = "/Users/christianruff/hackathon/krankenhaeuser_merged.csv"

df = pd.read_csv(IN_PATH, dtype=str, keep_default_na=False, na_values=[""])

def to_bool(s):
    if isinstance(s, pd.Series):
        return s.astype(str).str.strip().str.lower().isin(["true", "ja", "1", "t"])
    return False

betten = pd.to_numeric(df["betten"], errors="coerce")
importance = pd.to_numeric(df.get("importance_score", pd.Series([np.nan]*len(df))), errors="coerce")
fachabt = df["fachabteilungen"].fillna("").str.lower()
name    = df["name"].fillna("").str.lower()
chain   = df.get("chain_name", pd.Series([""]*len(df))).fillna("").str.lower()

hat_intensiv     = to_bool(df.get("hat_intensivmedizin", pd.Series()))
hat_notaufnahme  = to_bool(df.get("hat_notaufnahme", pd.Series()))
hat_bg           = to_bool(df.get("hat_bg_zulassung", pd.Series()))
hat_radio        = to_bool(df.get("hat_radiologie", pd.Series()))

# Chirurgische Fachabteilung vorhanden?
chirurg_keywords = ["chirurg", "unfallchirurg", "unfall-", "orthopäd", "traumato"]
has_chirurgie = fachabt.apply(lambda t: any(k in t for k in chirurg_keywords))

# Uniklinik / Maximalversorger?
uni_name_patterns = re.compile(
    r"(universit[aä]tsklinik|uniklinik|universit[aä]tsmedizin|"
    r"charité|charite|klinikum\s+(der|uni|universit[aä]t)|"
    r"maximalversorg|zentralklinik(um)?|bg[ -]unfallklinik|"
    r"universit[aä]tskrankenhaus)"
)
is_uni_name = name.str.contains(uni_name_patterns, regex=True, na=False)
is_uni_affil = df["universitaet"].fillna("").str.strip().ne("") if "universitaet" in df else pd.Series([False]*len(df))

# Krankenhaus-ähnlicher Name (für Fallback bei db_only ohne Flags)
hospital_name_re = re.compile(r"(krankenhaus|klinikum|klinik\b|hospital|krankenh\.|med\.\s*zentrum)")
looks_like_hospital = name.str.contains(hospital_name_re, regex=True, na=False)

# === SK1 ===
kann_sk1 = (
    (hat_intensiv & (hat_notaufnahme | has_chirurgie))
    | (is_uni_affil & (betten.fillna(0) >= 300))
    | is_uni_name
    | ((importance.fillna(0) >= 80) & (betten.fillna(0) >= 300))
)

# === SK2 ===
kann_sk2 = (
    kann_sk1
    | hat_notaufnahme
    | hat_bg
    | hat_intensiv
    | (has_chirurgie & (betten.fillna(0) >= 50))
    | (looks_like_hospital & (betten.fillna(0) >= 50))
)

# === SK3 ===
kann_sk3 = (
    kann_sk2
    | (betten.fillna(0) >= 1)
    | looks_like_hospital
    | fachabt.ne("")
)

df["kann_sk1"] = kann_sk1
df["kann_sk2"] = kann_sk2
df["kann_sk3"] = kann_sk3

df["sk_max"] = np.where(kann_sk1, "SK1",
               np.where(kann_sk2, "SK2",
               np.where(kann_sk3, "SK3", "keine")))

# Grobe Kapazitätsschätzung pro SK-Stufe (für Simulation):
#   SK1-Betten ~ 5 % der Betten (ITS-Anteil), min 2 wenn SK1-fähig
#   SK2-Betten ~ 30 % (Normalstation OP-Bereich)
#   SK3-Betten ~ 40 % (leichte Fälle / ambulant / Rest)
b = betten.fillna(0)
df["kapazitaet_sk1_geschaetzt"] = np.where(kann_sk1, np.maximum((b * 0.05).round().astype(int), 2), 0)
df["kapazitaet_sk2_geschaetzt"] = np.where(kann_sk2, np.maximum((b * 0.30).round().astype(int), 5), 0)
df["kapazitaet_sk3_geschaetzt"] = np.where(kann_sk3, np.maximum((b * 0.40).round().astype(int), 5 if kann_sk3.any() else 0), 0)

# Begründung für Transparenz
def begruendung(row):
    reasons = []
    if row["kann_sk1"]:
        if row.get("hat_intensivmedizin", "").lower() in ("true","ja") and (
            row.get("hat_notaufnahme","").lower() in ("true","ja") or any(k in str(row.get("fachabteilungen","")).lower() for k in chirurg_keywords)
        ):
            reasons.append("ITS+Chirurgie/Notaufnahme")
        if str(row.get("universitaet","")).strip():
            reasons.append(f"Uniklinik ({row['universitaet']})")
        if any(p in str(row.get("name","")).lower() for p in ["universitätsklinik","uniklinik","universitätsmedizin","charité"]):
            reasons.append("Name: Uniklinik")
    elif row["kann_sk2"]:
        if row.get("hat_notaufnahme","").lower() in ("true","ja"): reasons.append("Notaufnahme")
        if row.get("hat_bg_zulassung","").lower() in ("true","ja"): reasons.append("BG-Zulassung")
        if row.get("hat_intensivmedizin","").lower() in ("true","ja"): reasons.append("Intensivmedizin")
        b = pd.to_numeric(row.get("betten"), errors="coerce")
        if pd.notna(b) and b >= 50: reasons.append(f"{int(b)} Betten")
    elif row["kann_sk3"]:
        reasons.append("Grundversorgung")
    return "; ".join(reasons) if reasons else ""

df["sk_begruendung"] = df.apply(begruendung, axis=1)

df.to_csv(OUT_PATH, index=False)

# Stats
print(f"Geschrieben: {OUT_PATH}  (Zeilen: {len(df)}, Spalten: {len(df.columns)})")
print("\n=== SK-Verteilung (wie viele Kliniken pro Stufe) ===")
print(f"  kann_sk1:  {df['kann_sk1'].sum():>5d} / {len(df)}")
print(f"  kann_sk2:  {df['kann_sk2'].sum():>5d} / {len(df)}")
print(f"  kann_sk3:  {df['kann_sk3'].sum():>5d} / {len(df)}")
print(f"\n=== sk_max ===")
print(df["sk_max"].value_counts())

print("\n=== Beispiele SK1 (Top-10 nach Betten) ===")
sk1 = df[df["kann_sk1"]].copy()
sk1["_betten"] = pd.to_numeric(sk1["betten"], errors="coerce").fillna(0)
print(sk1.sort_values("_betten", ascending=False)[["name","ort","betten","sk_begruendung"]].head(10).to_string(index=False))

print("\n=== Geschätzte Gesamtkapazität ===")
print(f"  SK1: {df['kapazitaet_sk1_geschaetzt'].sum()} Betten")
print(f"  SK2: {df['kapazitaet_sk2_geschaetzt'].sum()} Betten")
print(f"  SK3: {df['kapazitaet_sk3_geschaetzt'].sum()} Betten")
