#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
hl7_kapazitaet.py — Interaktive Kapazitätsberechnung aus HL7 ADT-Nachrichten
=============================================================================

Ablauf (interaktiv):

  1. Das Skript fragt nach den Kapazitäten von SK1, SK2 und SK3.
  2. Das Skript fragt: "Daten einlesen? (j/n)".
  3. Bei 'j' werden die HL7-Dateien aus dem aktuellen Verzeichnis eingelesen
     (Default: adt_a01.hl7, adt_a08.hl7, adt_a03.hl7 — alternativ über
     CLI-Optionen anpassbar).
  4. Nach *jeder einzelnen* verarbeiteten Nachricht wird eine Zeile mit
     der aktuellen Belegung in die Verlaufsdatei geschrieben.
  5. Am Ende erscheint eine Zusammenfassung auf der Konsole.

SK-Ermittlung aus PV1-10 (Hospital Service):
  - Wenn PV1-10.1 einen Code SK1/SK2/SK3 enthält, wird dieser direkt genommen.
  - Sonst wird das Fachabteilungs-Kürzel (KAR/ORT/VIS/UNF/ONK/PNE/AOP ...)
    über FA_TO_SK auf eine SK gemappt. Diese Tabelle ist im Skript leicht
    anzupassen.

AUFRUF
------
  python3 hl7_kapazitaet.py
  python3 hl7_kapazitaet.py --a01 adt_a01.hl7 --a08 adt_a08.hl7 --a03 adt_a03.hl7
  python3 hl7_kapazitaet.py --output belegung.csv --format csv
  python3 hl7_kapazitaet.py --output belegung.txt --format text

Erzeugen passender HL7-Dateien aus dem ADT-Generator:
  python3 hl7_adt_generator.py --event A01 -n 50 --output adt_a01.hl7
  python3 hl7_adt_generator.py --event A08 -n 50 --output adt_a08.hl7
  python3 hl7_adt_generator.py --event A03 -n 50 --output adt_a03.hl7
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from typing import Optional


# ─── Konfiguration: Fachabteilung → SK-Mapping ────────────────────────────────
# Passen Sie diese Tabelle bei Bedarf an. Fachabteilungs-Kürzel sind die aus
# PV1-10 des hl7_adt_generator.py (FA-Code "KAR", "ORT", "VIS", "UNF", "ONK",
# "PNE", "AOP").

FA_TO_SK: dict[str, str] = {
    # Normalstation → SK1
    "KAR": "SK1",   # Kardiologie
    "ORT": "SK1",   # Orthopädie
    "VIS": "SK1",   # Viszeralchirurgie
    "ONK": "SK1",   # Onkologie
    "AOP": "SK1",   # Ambulantes OP-Zentrum
    # Intermediate Care → SK2
    "UNF": "SK2",   # Unfallchirurgie (oft IMC-pflichtig)
    "PNE": "SK2",   # Pneumologie (enthält IMC-Fälle im Generator)
    # Intensivstation → SK3  (bei Bedarf hier Codes ergänzen)
    # "ITS": "SK3",
}


# ─── HL7-Parser (minimal, nur die Felder die für die Belegung zählen) ─────────

FIELD_SEP = "|"
COMP_SEP = "^"


def _get(fields: list, idx: int, default: str = "") -> str:
    return fields[idx] if idx < len(fields) else default


@dataclass
class Event:
    trigger: str          # "A01" | "A08" | "A03"
    patient_id: str
    fall: str
    sk_code: Optional[str]  # finale SK-Zuordnung (oder None wenn nicht ermittelbar)


def parse_message(msg: str) -> Event:
    """Extrahiert aus einer HL7-Nachricht Trigger, Patient-ID, Fall und SK."""
    # Segment-Separator tolerant: \r, \n, \r\n
    segments = msg.replace("\r\n", "\n").replace("\r", "\n").strip().split("\n")
    seg: dict[str, str] = {}
    for s in segments:
        if len(s) >= 3 and s[3:4] == FIELD_SEP:
            seg[s[:3]] = s

    msh = seg.get("MSH", "").split(FIELD_SEP)
    pid = seg.get("PID", "").split(FIELD_SEP)
    pv1 = seg.get("PV1", "").split(FIELD_SEP)

    trigger = ""
    msh9 = _get(msh, 8)
    if COMP_SEP in msh9:
        parts = msh9.split(COMP_SEP)
        if len(parts) > 1:
            trigger = parts[1]

    patient_id = _get(pid, 3).split(COMP_SEP)[0]
    fall = _get(pv1, 19)

    # SK-Ermittlung aus PV1-10
    pv1_10 = _get(pv1, 10)
    sk_code: Optional[str] = None
    if pv1_10:
        code = pv1_10.split(COMP_SEP)[0].strip().upper()
        if code in ("SK1", "SK2", "SK3"):
            sk_code = code                    # bereits SK-kodiert
        elif code in FA_TO_SK:
            sk_code = FA_TO_SK[code]          # Fachabteilung → SK mappen

    return Event(trigger=trigger, patient_id=patient_id,
                 fall=fall, sk_code=sk_code)


def split_messages(text: str) -> list[str]:
    """Teilt einen Rohtext mit mehreren HL7-Nachrichten in Einzelnachrichten."""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    # Bevorzugte Trennung: Leerzeile
    if "\n\n" in text:
        return [blk.strip() for blk in text.split("\n\n") if blk.strip()]
    # Fallback: jede Zeile, die mit "MSH|" beginnt, startet eine neue Nachricht
    lines = text.split("\n")
    msgs: list[list[str]] = []
    current: list[str] = []
    for ln in lines:
        if ln.startswith("MSH|"):
            if current:
                msgs.append(current)
            current = [ln]
        elif current:
            current.append(ln)
    if current:
        msgs.append(current)
    return ["\n".join(m).strip() for m in msgs if m]


# ─── Tracker: hält den Zustand und schreibt pro Event eine Zeile ──────────────

class Tracker:
    def __init__(self, capacities: dict[str, int]):
        self.capacities = dict(capacities)
        self.belegt = {k: 0 for k in capacities}
        self._patient_sk: dict[str, str] = {}   # Patient → aktuell belegte SK
        self.warnings: list[str] = []
        self.step = 0

    def process(self, ev: Event) -> str:
        """Wendet das Event an und liefert einen knappen Status-Kommentar."""
        self.step += 1
        pid = ev.patient_id
        if not pid:
            return "Nachricht ohne Patient-ID übersprungen"

        current = self._patient_sk.get(pid)
        comment = ""

        if ev.trigger == "A01":
            if ev.sk_code is None or ev.sk_code not in self.capacities:
                self.warnings.append(
                    f"[Schritt {self.step}] A01 {pid}: SK '{ev.sk_code}' nicht erkannt"
                )
                return f"A01 {pid}: SK unbekannt → übersprungen"
            if current is not None:
                self.warnings.append(
                    f"[Schritt {self.step}] A01 für bereits aufgenommenen Patienten {pid}"
                )
                self.belegt[current] = max(0, self.belegt[current] - 1)
            self.belegt[ev.sk_code] += 1
            self._patient_sk[pid] = ev.sk_code
            comment = f"A01 {pid} → {ev.sk_code}  (+1)"
            self._check_over(ev.sk_code)

        elif ev.trigger == "A08":
            if ev.sk_code is None or ev.sk_code not in self.capacities:
                return f"A08 {pid}: kein SK-Effekt"
            if current is None:
                self.belegt[ev.sk_code] += 1
                self._patient_sk[pid] = ev.sk_code
                self.warnings.append(
                    f"[Schritt {self.step}] A08 unbekannter Patient {pid} → als Aufnahme gewertet"
                )
                comment = f"A08 {pid} → {ev.sk_code} (als Aufnahme; +1)"
                self._check_over(ev.sk_code)
            elif current != ev.sk_code:
                self.belegt[current] = max(0, self.belegt[current] - 1)
                self.belegt[ev.sk_code] += 1
                self._patient_sk[pid] = ev.sk_code
                comment = f"A08 {pid}: {current} → {ev.sk_code}"
                self._check_over(ev.sk_code)
            else:
                comment = f"A08 {pid}: keine SK-Änderung"

        elif ev.trigger == "A03":
            if current is None:
                self.warnings.append(
                    f"[Schritt {self.step}] A03 für unbekannten Patienten {pid}"
                )
                return f"A03 {pid}: unbekannt → übersprungen"
            self.belegt[current] = max(0, self.belegt[current] - 1)
            del self._patient_sk[pid]
            comment = f"A03 {pid} ← {current}  (-1)"

        else:
            comment = f"{ev.trigger} {pid}: ignoriert"

        return comment

    def _check_over(self, sk: str) -> None:
        if self.belegt[sk] > self.capacities[sk]:
            self.warnings.append(
                f"[Schritt {self.step}] Überbelegung {sk}: "
                f"{self.belegt[sk]} / {self.capacities[sk]}"
            )

    def snapshot_row(self) -> dict:
        """Eine Zeile für die Verlaufsdatei (als dict, vor Formatierung)."""
        row = {"schritt": self.step}
        for sk in self.capacities:
            kap = self.capacities[sk]
            bel = self.belegt[sk]
            frei = kap - bel
            pct = (bel / kap * 100.0) if kap else 0.0
            row[f"{sk}_belegt"] = bel
            row[f"{sk}_frei"] = frei
            row[f"{sk}_auslastung_pct"] = round(pct, 1)
        row["patienten_im_haus"] = len(self._patient_sk)
        return row


# ─── Verlaufsausgabe-Formate ──────────────────────────────────────────────────

def write_header(fh, capacities: dict[str, int], fmt: str) -> None:
    if fmt == "csv":
        cols = ["schritt", "trigger", "patient_id", "kommentar"]
        for sk in capacities:
            cols += [f"{sk}_belegt", f"{sk}_frei", f"{sk}_auslastung_pct"]
        cols.append("patienten_im_haus")
        fh.write(";".join(cols) + "\n")
    else:
        fh.write("Kapazitäts-Verlauf aus HL7 ADT-Nachrichten\n")
        fh.write("=" * 78 + "\n")
        fh.write("Konfigurierte Kapazitäten:  " +
                 "  ".join(f"{k}={v}" for k, v in capacities.items()) + "\n")
        fh.write("=" * 78 + "\n\n")
        header_line = f"{'Nr.':>4} {'Trigger':<7} {'Patient':<8} {'Kommentar':<32}"
        for sk in capacities:
            header_line += f" {sk:>7}"
        fh.write(header_line + "\n")
        fh.write("-" * len(header_line) + "\n")


def write_step(fh, tracker: Tracker, ev: Event, comment: str, fmt: str) -> None:
    row = tracker.snapshot_row()
    if fmt == "csv":
        parts = [str(row["schritt"]), ev.trigger, ev.patient_id, comment]
        for sk in tracker.capacities:
            parts += [str(row[f"{sk}_belegt"]),
                      str(row[f"{sk}_frei"]),
                      f"{row[f'{sk}_auslastung_pct']:.1f}"]
        parts.append(str(row["patienten_im_haus"]))
        fh.write(";".join(parts) + "\n")
    else:
        line = (f"{row['schritt']:>4} {ev.trigger:<7} {ev.patient_id:<8} "
                f"{comment:<32}")
        for sk in tracker.capacities:
            line += f" {row[f'{sk}_belegt']:>3}/{tracker.capacities[sk]:<3}"
        fh.write(line + "\n")


def write_footer(fh, tracker: Tracker, fmt: str) -> None:
    if fmt == "csv":
        return
    fh.write("\n")
    fh.write("=" * 78 + "\n")
    fh.write("Endstand\n")
    fh.write("=" * 78 + "\n")
    for sk, kap in tracker.capacities.items():
        bel = tracker.belegt[sk]
        pct = (bel / kap * 100.0) if kap else 0.0
        flag = "   ÜBERBELEGT" if bel > kap else ""
        fh.write(f"  {sk}:  {bel:>3} / {kap:<3}  belegt "
                 f"({pct:>5.1f} %){flag}\n")
    fh.write(f"\nPatienten im Haus: {sum(tracker.belegt.values())}\n")
    if tracker.warnings:
        fh.write("\nWarnungen:\n")
        for w in tracker.warnings:
            fh.write(f"  · {w}\n")


# ─── Interaktive Eingaben ─────────────────────────────────────────────────────

def ask_int(prompt: str, default: Optional[int] = None) -> int:
    while True:
        hint = f" [{default}]" if default is not None else ""
        raw = input(f"{prompt}{hint}: ").strip()
        if not raw and default is not None:
            return default
        try:
            v = int(raw)
            if v < 0:
                print("  Bitte einen Wert ≥ 0 eingeben.")
                continue
            return v
        except ValueError:
            print("  Ungültige Zahl. Bitte erneut eingeben.")


def ask_yes_no(prompt: str, default: bool = True) -> bool:
    suffix = " (j/n) [j]" if default else " (j/n) [n]"
    while True:
        raw = input(prompt + suffix + ": ").strip().lower()
        if not raw:
            return default
        if raw in ("j", "ja", "y", "yes"):
            return True
        if raw in ("n", "nein", "no"):
            return False
        print("  Bitte 'j' oder 'n' eingeben.")


# ─── Hauptablauf ──────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Interaktive SK-Belegung aus HL7 ADT-Nachrichten.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--a01", default="adt_a01.hl7",
                    help="HL7-Datei mit A01-Nachrichten (Default: adt_a01.hl7)")
    ap.add_argument("--a08", default="adt_a08.hl7",
                    help="HL7-Datei mit A08-Nachrichten (Default: adt_a08.hl7)")
    ap.add_argument("--a03", default="adt_a03.hl7",
                    help="HL7-Datei mit A03-Nachrichten (Default: adt_a03.hl7)")
    ap.add_argument("-o", "--output", default="belegung_verlauf.txt",
                    help="Verlaufs-Ausgabedatei (Default: belegung_verlauf.txt)")
    ap.add_argument("-f", "--format", choices=["text", "csv"], default="text",
                    help="Ausgabeformat (Default: text)")
    args = ap.parse_args()

    print("=" * 66)
    print("  HL7 Kapazitätsberechnung SK1 / SK2 / SK3")
    print("=" * 66)
    print()

    # 1) Kapazitäten abfragen
    print("Bitte die Kapazitäten (Anzahl Betten) pro Servicekategorie eingeben:")
    sk1 = ask_int("  Kapazität SK1 (Normalstation)", default=30)
    sk2 = ask_int("  Kapazität SK2 (Intermediate Care)", default=12)
    sk3 = ask_int("  Kapazität SK3 (Intensivstation)", default=6)
    capacities = {"SK1": sk1, "SK2": sk2, "SK3": sk3}

    print()
    print("Konfigurierte Kapazitäten:")
    for k, v in capacities.items():
        print(f"  {k}: {v}")
    print()

    # 2) Einlesen bestätigen
    print("Erwartete Eingabedateien:")
    for label, path in (("A01", args.a01), ("A08", args.a08), ("A03", args.a03)):
        exists = "✓" if os.path.isfile(path) else "✗ nicht gefunden"
        print(f"  {label}: {path}   [{exists}]")
    print()

    if not ask_yes_no("Daten einlesen und verarbeiten?"):
        print("Abgebrochen.")
        return 0

    # 3) Dateien einlesen und in chronologische Reihenfolge sortieren
    all_events: list[Event] = []
    for path in (args.a01, args.a08, args.a03):
        if not os.path.isfile(path):
            print(f"WARNUNG: {path} nicht gefunden, übersprungen.")
            continue
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            raw = f.read()
        msgs = split_messages(raw)
        parsed = [parse_message(m) for m in msgs]
        print(f"  {path}: {len(parsed)} Nachrichten gelesen")
        all_events.extend(parsed)

    if not all_events:
        print("Keine Nachrichten gefunden. Bitte HL7-Dateien bereitstellen.")
        return 1

    # A01 vor A08 vor A03 ist die fachlich korrekte Reihenfolge damit
    # Patienten erst aufgenommen, dann aktualisiert, dann entlassen werden.
    order = {"A01": 0, "A08": 1, "A03": 2}
    all_events.sort(key=lambda e: order.get(e.trigger, 99))

    print(f"\nGesamt: {len(all_events)} Nachrichten werden verarbeitet.")
    print(f"Verlauf wird geschrieben nach: {args.output}\n")

    # 4) Verarbeiten und nach jeder Nachricht eine Zeile schreiben
    tracker = Tracker(capacities)
    with open(args.output, "w", encoding="utf-8") as fh:
        write_header(fh, capacities, args.format)
        for ev in all_events:
            comment = tracker.process(ev)
            write_step(fh, tracker, ev, comment, args.format)
        write_footer(fh, tracker, args.format)

    # 5) Konsolen-Zusammenfassung
    print("Endstand:")
    print("-" * 50)
    for sk, kap in capacities.items():
        bel = tracker.belegt[sk]
        pct = (bel / kap * 100.0) if kap else 0.0
        flag = "  ÜBERBELEGT" if bel > kap else ""
        print(f"  {sk}:  {bel:>3} / {kap:<3}  ({pct:>5.1f} %){flag}")
    print("-" * 50)
    print(f"Patienten im Haus: {sum(tracker.belegt.values())}")
    if tracker.warnings:
        print(f"\n{len(tracker.warnings)} Warnung(en). "
              f"Details in {args.output}.")

    print(f"\nVerlauf geschrieben: {args.output}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nAbgebrochen.")
        sys.exit(130)
