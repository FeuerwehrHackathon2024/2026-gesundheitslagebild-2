"""Dedupliziert die merged Krankenhaus-CSV.

Strategie (mehrstufig):
  1. Gleiche (name_norm, PLZ) → 1 Eintrag (Priorität csv+db > csv_only_IK > csv_only > db_only)
  2. Gleiche (name_norm, ort) für Zeilen ohne PLZ → 1 Eintrag
  3. db_only → dropped, wenn (name_norm, PLZ) bereits in csv+db existiert
  4. Gleiche IK + gleicher name_norm (Mehrfachstandorte-Artefakte) → 1 Eintrag

Kanonische Felder werden aus dem besten (höchstpriorisierten) Eintrag
übernommen. Fehlende Felder werden aus anderen Duplikaten gefüllt ("merge best").

Output: data/krankenhaeuser_dedup.csv
"""

from __future__ import annotations

import math
import re
from pathlib import Path

import pandas as pd

IN_PATH = Path(__file__).resolve().parent.parent / "data" / "krankenhaeuser_merged.csv"
OUT_PATH = Path(__file__).resolve().parent.parent / "data" / "krankenhaeuser_dedup.csv"


def normalize_name(s: str) -> str:
    if pd.isna(s) or not s:
        return ""
    s = str(s).lower()
    s = re.sub(r"\b(ggmbh|gmbh|ag|e\.?v\.?|kg|gbr|ohg|co\.?|kgaa|mbh)\b", "", s)
    s = re.sub(r"\b(st\.?|sankt)\b", "st", s)
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def haversine_km(lat1, lon1, lat2, lon2):
    if any(pd.isna(x) for x in (lat1, lon1, lat2, lon2)):
        return math.inf
    R = 6371.0
    rad = math.pi / 180
    dlat = (lat2 - lat1) * rad
    dlon = (lon2 - lon1) * rad
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1 * rad) * math.cos(lat2 * rad) * math.sin(dlon / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def priority(row) -> int:
    ms = row.get("merge_source") or ""
    ik = row.get("ik")
    if ms == "csv+db":
        return 0
    if ms == "csv_only" and isinstance(ik, str) and ik.strip():
        return 1
    if ms == "csv_only":
        return 2
    return 3  # db_only oder Unbekannt


def merge_group(group: pd.DataFrame) -> pd.Series:
    """Wählt aus einer Gruppe die beste Zeile und füllt leere Felder aus den anderen."""
    group = group.sort_values("_prio")
    base = group.iloc[0].copy()
    for _, other in group.iloc[1:].iterrows():
        for col in group.columns:
            if col.startswith("_"):
                continue
            cur = base.get(col)
            alt = other.get(col)
            if (pd.isna(cur) or cur in ("", "nan")) and pd.notna(alt) and alt not in ("", "nan"):
                base[col] = alt
    base["duplicate_count"] = len(group)
    return base


def main():
    df = pd.read_csv(IN_PATH, dtype=str, keep_default_na=False, na_values=[""])
    total_in = len(df)
    print(f"Eingang: {total_in} Zeilen")

    # Normalisierung
    df["_nn"] = df["name"].apply(normalize_name)
    df["_plz"] = df["plz"].fillna("").str.strip()
    df["_ort"] = df["ort"].fillna("").str.lower().str.strip()
    df["_prio"] = df.apply(priority, axis=1)

    # Dedup-Key: (name_norm, PLZ) wenn beide da, sonst (name_norm, ort)
    def make_key(r):
        if not r["_nn"]:
            return None
        if r["_plz"]:
            return ("plz", r["_nn"], r["_plz"])
        if r["_ort"]:
            return ("ort", r["_nn"], r["_ort"])
        return None

    df["_key"] = df.apply(make_key, axis=1)

    without_key = df[df["_key"].isna()]
    with_key = df[df["_key"].notna()].copy()

    # Gruppieren + mergen
    merged_rows = []
    for key, group in with_key.groupby("_key"):
        if len(group) == 1:
            merged_rows.append(group.iloc[0])
        else:
            merged_rows.append(merge_group(group))

    merged_df = pd.DataFrame(merged_rows).reset_index(drop=True)
    # Zeilen ohne Key (keine PLZ/Ort/Name) → so wie sie sind dranhängen
    final = pd.concat([merged_df, without_key], ignore_index=True)

    # Zusätzlich: IK-Duplikate bei identischem name_norm (Konzern-Artefakte)
    ik_dup_mask = (
        final["ik"].notna() & final["_nn"].ne("") &
        final.duplicated(subset=["ik", "_nn"], keep=False)
    )
    ik_dup_groups = final[ik_dup_mask].groupby(["ik", "_nn"])
    drop_idx = []
    for (ik, nn), g in ik_dup_groups:
        g_sorted = g.sort_values("_prio")
        drop_idx.extend(g_sorted.index[1:].tolist())
    if drop_idx:
        final = final.drop(index=drop_idx).reset_index(drop=True)
    print(f"Nach IK-Dup-Drop: {len(final)} Zeilen")

    # ---------- Geo-Matching: db_only vs csv+db/csv_only ----------
    # Für jeden db_only-Eintrag: liegt er <500m von einem IK-Eintrag und
    # hat Name-Ähnlichkeit (oder gleiche PLZ)? → droppen.
    from difflib import SequenceMatcher

    def name_sim(a: str, b: str) -> float:
        if not a or not b:
            return 0.0
        return SequenceMatcher(None, a, b).ratio()

    final["lat_f"] = pd.to_numeric(final["lat"], errors="coerce")
    final["lon_f"] = pd.to_numeric(final["lon"], errors="coerce")
    official = final[final["merge_source"].isin(["csv+db", "csv_only"])].copy()
    official = official[official["lat_f"].notna()]

    # Spatial-Bucket: round lat/lon auf 0.02 (~2km) für schnelles Nachbarschafts-Lookup
    def bucket(lat, lon):
        return (round(lat * 50) / 50, round(lon * 50) / 50)

    buckets: dict = {}
    for idx, row in official.iterrows():
        b = bucket(row["lat_f"], row["lon_f"])
        buckets.setdefault(b, []).append((idx, row["lat_f"], row["lon_f"], row["_nn"], row["_plz"]))

    db_only_idx = final.index[final["merge_source"] == "db_only"]
    drop_geo = []
    for idx in db_only_idx:
        row = final.loc[idx]
        lat, lon = row["lat_f"], row["lon_f"]
        if pd.isna(lat):
            continue
        nn = row["_nn"]
        plz = row["_plz"]
        for dlat in (-0.02, 0, 0.02):
            for dlon in (-0.02, 0, 0.02):
                for cand_idx, c_lat, c_lon, c_nn, c_plz in buckets.get(
                    (round((lat + dlat) * 50) / 50, round((lon + dlon) * 50) / 50), []
                ):
                    d = haversine_km(lat, lon, c_lat, c_lon)
                    if d > 0.5:
                        continue
                    # Name-Ähnlichkeit ODER gleiche PLZ
                    if (plz and plz == c_plz) or name_sim(nn, c_nn) > 0.55:
                        drop_geo.append(idx)
                        break
                else:
                    continue
                break
            if drop_geo and drop_geo[-1] == idx:
                break

    drop_geo = list(set(drop_geo))
    if drop_geo:
        final = final.drop(index=drop_geo).reset_index(drop=True)
    print(f"Nach Geo-Matching (db_only vs csv): {len(final)} (−{len(drop_geo)} db_only-Duplikate)")

    # ---------- db_only: Nicht-MANV-Kliniken droppen ----------
    # Kriterien für Drop: keine Betten UND keine SK1/SK2-Fähigkeit UND
    # (kein typischer Krankenhaus-Name ODER segment != Krankenhaus)
    def is_nonmanv(row) -> bool:
        if row.get("merge_source") != "db_only":
            return False
        name_lower = str(row.get("name") or "").lower()
        # Reha/MVZ/Praxis/Hospiz → explizit raus (nie MANV-fähig)
        if any(kw in name_lower for kw in
               ["rehabilitation", "reha-", "reha ", " reha", "hospiz", "mvz",
                "tagesklinik", "praxis", "senioren", "pflegeheim", "altenheim",
                "ambulanz", "zahnklinik", "zahnarzt", "dialyse"]):
            return True
        # Krankenhaus-Indikatoren
        is_hospital_name = any(
            kw in name_lower for kw in
            ["krankenhaus", "klinikum", "hospital", "universitätsklinik", "uniklinik"]
        ) or name_lower.startswith("klinik ") or " klinik " in name_lower
        has_betten = False
        try:
            has_betten = int(float(row.get("betten") or 0)) > 0
        except (TypeError, ValueError):
            pass
        can_sk1 = str(row.get("kann_sk1") or "").lower() in ("true", "1", "ja")
        # Nur behalten wenn: Krankenhaus-Name ODER Betten > 0 ODER SK1-fähig
        return not (is_hospital_name or has_betten or can_sk1)

    mask_drop = final.apply(is_nonmanv, axis=1)
    final = final.loc[~mask_drop].reset_index(drop=True)
    print(f"Nach Nicht-MANV-Filter: {len(final)} (−{mask_drop.sum()})")

    # ---------- IK-Konsolidierung: 1 Eintrag pro IK ----------
    # Mehrfach-Standorte (same IK, different stnr) → behalte bester Eintrag
    ik_groups = final[final["ik"].notna() & final["ik"].str.strip().ne("")].groupby("ik")
    drop_ik = []
    for ik, g in ik_groups:
        if len(g) <= 1:
            continue
        g_sorted = g.sort_values(
            by=["_prio", "betten"],
            ascending=[True, False],
            key=lambda col: pd.to_numeric(col, errors="coerce") if col.name == "betten" else col,
        )
        drop_ik.extend(g_sorted.index[1:].tolist())
    if drop_ik:
        final = final.drop(index=drop_ik).reset_index(drop=True)
    print(f"Nach IK-Konsolidierung (1 Eintrag/IK): {len(final)} (−{len(drop_ik)})")

    # ---------- db_only untereinander: (name_norm, plz) ----------
    db_mask = final["merge_source"] == "db_only"
    db_rows = final[db_mask & final["_nn"].ne("")]
    db_dup_groups = db_rows.groupby(["_nn", "_plz"])
    drop_intra = []
    for _, g in db_dup_groups:
        if len(g) <= 1:
            continue
        # Behalte Eintrag mit höchstem importance_score (oder ersten)
        g_sorted = g.assign(
            _score=pd.to_numeric(g["importance_score"], errors="coerce").fillna(0)
        ).sort_values("_score", ascending=False)
        drop_intra.extend(g_sorted.index[1:].tolist())
    if drop_intra:
        final = final.drop(index=drop_intra).reset_index(drop=True)
    print(f"Nach db_only intra-dedup: {len(final)} (−{len(drop_intra)})")

    # Hilfsspalten wieder weg (außer duplicate_count)
    drop_cols = [c for c in final.columns if c.startswith("_") or c in ("lat_f", "lon_f")]
    final = final.drop(columns=drop_cols)
    if "duplicate_count" not in final.columns:
        final["duplicate_count"] = 1
    else:
        final["duplicate_count"] = final["duplicate_count"].fillna(1).astype(int)

    final.to_csv(OUT_PATH, index=False)

    # Stats
    total_out = len(final)
    print(f"Ausgang: {total_out} Zeilen (−{total_in - total_out} Duplikate)")
    print(f"\nNach merge_source:")
    print(final["merge_source"].value_counts().to_string())
    print(f"\nNach sk_max:")
    print(final["sk_max"].fillna("-").value_counts().to_string())
    print(f"\nGeo-Abdeckung: {final['lat'].notna().sum()} / {total_out}")
    multi = (pd.to_numeric(final["duplicate_count"], errors="coerce") > 1).sum()
    print(f"Davon aus >=2 Quell-Zeilen gemerged: {multi}")
    print(f"\nGeschrieben: {OUT_PATH}")


if __name__ == "__main__":
    main()
