import pandas as pd
import numpy as np

CSV_PATH = "/Users/christianruff/hackathon/krankenhaus_daten_2024.csv"
DB_PATH  = "/Users/christianruff/hackathon/krankenhaeuser_db.csv"
OUT_PATH = "/Users/christianruff/hackathon/krankenhaeuser_merged.csv"

csv = pd.read_csv(CSV_PATH, dtype=str, keep_default_na=False, na_values=[""])
db  = pd.read_csv(DB_PATH,  dtype=str, keep_default_na=False, na_values=[""])

# Key-Normalisierung (beide Seiten auf object/str, NaN konsistent)
for df in (csv, db):
    for col in ("ik", "standortnummer"):
        df[col] = df[col].astype("object").where(df[col].notna(), None)
        df[col] = df[col].map(lambda v: str(v).strip() if v is not None and str(v).strip() else None)

# CSV hat leere Spalte fachabteilung_schluessel — löschen.
# Die AA-Codes stehen in apparative_ausstattung.
if "fachabteilung_schluessel" in csv.columns:
    csv = csv.drop(columns=["fachabteilung_schluessel"])

# Merge per (ik, standortnummer). DB- und CSV-Zeilen ohne IK bleiben als db_only/csv_only erhalten.
merged = csv.merge(
    db,
    how="outer",
    on=["ik", "standortnummer"],
    suffixes=("_csv", "_db"),
    indicator="merge_source",
)
merged["merge_source"] = merged["merge_source"].map({
    "both": "csv+db", "left_only": "csv_only", "right_only": "db_only"
})

def coalesce(col_csv, col_db, new_col):
    if col_csv in merged and col_db in merged:
        merged[new_col] = merged[col_csv].where(merged[col_csv].notna(), merged[col_db])
    elif col_csv in merged:
        merged[new_col] = merged[col_csv]
    elif col_db in merged:
        merged[new_col] = merged[col_db]

# Coalesce der gemeinsamen Felder (CSV bevorzugt)
coalesce("name_csv", "name_db", "name")
coalesce("strasse", "street", "strasse")
coalesce("hausnummer", "house_number", "hausnummer")
coalesce("plz", "postal_code", "plz")
coalesce("ort", "city", "ort")
coalesce("telefon", "phone", "telefon")
coalesce("email_csv", "email_db", "email")
coalesce("url_internetseite", "website", "website")
coalesce("betten_csv", "betten_db", "betten")
coalesce("traeger_name_csv", "traeger_name_db", "traeger_name")
coalesce("traeger_art_csv", "traeger_art_db", "traeger_art")
coalesce("universitaet_csv", "universitaet_db", "universitaet")
coalesce("lehrkrankenhaus_csv", "lehrkrankenhaus_db", "lehrkrankenhaus")
coalesce("notfallstufe_csv", "notfallstufe_db", "notfallstufe")
coalesce("anzahl_standorte_csv", "anzahl_standorte_db", "anzahl_standorte")
coalesce("anzahl_fachabteilungen_csv", "anzahl_fachabteilungen_db", "anzahl_fachabteilungen")
coalesce("fachabteilungen_csv", "fachabteilungen_db", "fachabteilungen")
coalesce("vollstationaere_fallzahl_csv", "vollstationaere_fallzahl_db", "vollstationaere_fallzahl")
coalesce("teilstationaere_fallzahl_csv", "teilstationaere_fallzahl_db", "teilstationaere_fallzahl")
coalesce("ambulante_fallzahl_csv", "ambulante_fallzahl_db", "ambulante_fallzahl")

# "hat_*"-Flags: CSV ("Ja"/"Nein") priorisiert, DB (bool) als Fallback
def merge_hat(key):
    col_csv = f"hat_{key}_csv"
    col_db  = f"hat_{key}_db"
    if col_csv in merged:
        csv_v = merged[col_csv].str.lower().map({"ja": True, "nein": False})
    else:
        csv_v = pd.Series([np.nan]*len(merged))
    if col_db in merged:
        db_v = merged[col_db].map({"t": True, "f": False, "True": True, "False": False, "true": True, "false": False})
    else:
        db_v = pd.Series([np.nan]*len(merged))
    merged[f"hat_{key}"] = csv_v.where(csv_v.notna(), db_v)

for key in ["dialyse","onkologie","geriatrie","intensivmedizin","notaufnahme","psychiatrie","bg_zulassung","radiologie"]:
    merge_hat(key)

# CSV-only Felder übernehmen
csv_only_keep = {
    "apparative_ausstattung": "apparative_ausstattung",
    "staeb_fallzahl":         "staeb_fallzahl",
    "notdienst":              "notdienst",
    "notdienstpraxis":        "notdienstpraxis",
    "hat_darzt":              "hat_darzt",
    "url_zugang":             "url_zugang",
    "url_weitere_infos":      "url_weitere_infos",
    "fax":                    "fax",
}
for src, dst in csv_only_keep.items():
    if src in merged:
        merged[dst] = merged[src]

# DB-only Felder übernehmen
db_only_keep = {
    "lat": "lat", "lon": "lon",
    "federal_state": "bundesland", "country": "country",
    "name_norm": "name_norm", "chain_name": "chain_name",
    "rating": "rating", "reviews": "reviews", "opening_hours_present": "opening_hours_present",
    "importance_score": "importance_score", "priority_bucket": "priority_bucket", "confidence": "confidence",
    "estimated_rides_per_year": "estimated_rides_per_year",
    "estimated_rides_per_year_db": "estimated_rides_per_year_db",
    "estimated_rides_per_year_llm": "estimated_rides_per_year_llm",
    "allocated_exact_db": "allocated_exact_db",
    "target_tier": "target_tier", "target_classification": "target_classification",
    "segment": "segment", "sector": "sector",
    "source": "source", "source_id": "source_id",
    "id": "facility_enriched_id",
}
for src, dst in db_only_keep.items():
    if src in merged and dst not in merged:
        merged[dst] = merged[src]

# Finale Spaltenreihenfolge
final_cols = [
    "ik","standortnummer","name","name_norm","chain_name",
    "strasse","hausnummer","plz","ort","bundesland","country",
    "lat","lon",
    "telefon","fax","email","website","url_zugang","url_weitere_infos",
    "anzahl_standorte","betten",
    "vollstationaere_fallzahl","teilstationaere_fallzahl","ambulante_fallzahl","staeb_fallzahl",
    "traeger_name","traeger_art","universitaet","lehrkrankenhaus",
    "notfallstufe","notdienst","notdienstpraxis",
    "hat_dialyse","hat_onkologie","hat_geriatrie","hat_intensivmedizin","hat_notaufnahme",
    "hat_psychiatrie","hat_bg_zulassung","hat_radiologie","hat_darzt",
    "anzahl_fachabteilungen","fachabteilungen","apparative_ausstattung",
    "rating","reviews","opening_hours_present",
    "importance_score","priority_bucket","confidence",
    "estimated_rides_per_year","estimated_rides_per_year_db","estimated_rides_per_year_llm","allocated_exact_db",
    "target_tier","target_classification",
    "segment","sector","source","source_id","facility_enriched_id",
    "merge_source",
]
final_cols = [c for c in final_cols if c in merged.columns]

out = merged[final_cols]
out.to_csv(OUT_PATH, index=False)

print(f"Geschrieben: {OUT_PATH}")
print(f"Zeilen: {len(out)}")
print(f"Spalten: {len(out.columns)}")
print("\nVerteilung merge_source:")
print(out["merge_source"].value_counts())
print("\nAbdeckung wichtiger Felder:")
for c in ["ik","lat","lon","betten","fachabteilungen","apparative_ausstattung",
          "importance_score","telefon","email","website","traeger_name"]:
    if c in out.columns:
        print(f"  {c:<32s}: {out[c].notna().sum():>5d} / {len(out)}")
