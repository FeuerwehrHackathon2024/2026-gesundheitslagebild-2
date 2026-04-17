"""HL7 v2.5 ADT-Nachrichten-Generator für die MANV-Dispatch-Simulation.

Erzeugt realistische ADT-Events (A01/A03/A08) und kann sie entweder auf
stdout ausgeben oder direkt an den Dispatch-Server senden (/api/adt/ingest).

Anwendungsbeispiele
-------------------
# 50 Aufnahmen auf stdout
python3 hl7_adt_generator.py --event A01 -n 50

# 50 Entlassungen direkt an lokale Instanz senden
python3 hl7_adt_generator.py --event A03 -n 50 --post http://localhost/api/adt/ingest

# Mix / kontinuierliche Last (realistischer Stream)
python3 hl7_adt_generator.py --mix -n 200 --rate 5 --post http://185.181.169.154/api/adt/ingest
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from datetime import datetime, timedelta
from typing import Iterable
from urllib import request as urlreq
from urllib.error import URLError

SEGMENT_SEP = "\r"
FIELD_SEP = "|"
COMPONENT_SEP = "^"

EVENTS = {
    "A01": "Aufnahme",
    "A03": "Entlassung",
    "A08": "Update Patient Information",
}

SK_CODES = ["SK1", "SK2", "SK3"]
STATIONEN = ["ITS", "CHIR", "INNERE", "OP", "AMB", "NOTFALL", "KARDIO", "NEURO"]
GESCHLECHTER = ["M", "F"]

# Fallback-Krankenhausliste, wenn kein Remote aufgelöst wird.
# Id-Nummern entsprechen IK-artigen Codes. Der Server mappt fuzzy.
DEFAULT_SENDING_FACILITIES = [
    ("BWKUlm", "Bundeswehrkrankenhaus Ulm"),
    ("UniKlinUlm", "Universitätsklinikum Ulm"),
    ("CharitBer", "Charité Berlin"),
    ("LMU", "LMU München"),
    ("UKL", "Universitätsklinikum Leipzig"),
]


def _ts(dt: datetime | None = None) -> str:
    return (dt or datetime.now()).strftime("%Y%m%d%H%M%S")


def _rnd_date_past(max_days: int = 5) -> datetime:
    return datetime.now() - timedelta(
        days=random.randint(0, max_days),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )


def _first_last(idx: int) -> tuple[str, str]:
    firsts = ["Max", "Anna", "Lena", "Tim", "Jan", "Mia", "Tom", "Lisa", "Paul", "Emma"]
    lasts = ["Müller", "Schmidt", "Weber", "Fischer", "Wagner", "Becker", "Schulz", "Hoffmann"]
    return random.choice(firsts), random.choice(lasts) + f"-{idx:04d}"


def build_message(event: str, idx: int, facility: tuple[str, str] | None = None,
                  pid: str | None = None, sk: str | None = None) -> str:
    now = _ts()
    msg_ctrl = f"MSG{random.randint(100000, 999999)}"
    fac_id, fac_name = facility or random.choice(DEFAULT_SENDING_FACILITIES)
    first, last = _first_last(idx)
    gender = random.choice(GESCHLECHTER)
    birth = _rnd_date_past(365 * 60 + random.randint(0, 365 * 20)).strftime("%Y%m%d")
    station = random.choice(STATIONEN)
    sk = sk or random.choices(SK_CODES, weights=[1, 2, 4], k=1)[0]  # mehr leichte Fälle
    pid = pid or f"P{random.randint(100000, 999999)}"

    msh = FIELD_SEP.join([
        "MSH", COMPONENT_SEP + "~\\&",
        "MANV-SIM",
        fac_id + COMPONENT_SEP + fac_name,
        "MANV-DISPATCH", "MANV-CENTRAL",
        now,
        "",
        f"ADT{COMPONENT_SEP}{event}",
        msg_ctrl,
        "P", "2.5",
    ])
    evn = FIELD_SEP.join(["EVN", event, now])
    pid_seg = FIELD_SEP.join([
        "PID", "1", "", pid + COMPONENT_SEP * 4 + "PI",
        "", last + COMPONENT_SEP + first,
        "", birth, gender,
    ])

    # PV1 je nach Event
    discharge_ts = now if event == "A03" else ""
    admit_ts = now if event == "A01" else _ts(_rnd_date_past(3))
    pv1 = FIELD_SEP.join([
        "PV1", "1",
        "I",                      # Inpatient
        station + COMPONENT_SEP + f"Bett{random.randint(1, 40)}" + COMPONENT_SEP + f"Z{random.randint(1, 4)}",
        "",                       # Admission Type
        "",                       # Preadmit Number
        "",                       # Prior Patient Location
        "",                       # Attending Doctor
        "",                       # Referring Doctor
        "",                       # Consulting Doctor
        "",                       # Hospital Service
        "",                       # Temporary Location
        "",                       # Preadmit Test Indicator
        "",                       # Re-admission Indicator
        "",                       # Admit Source
        "",                       # Ambulatory Status
        "",                       # VIP Indicator
        "",                       # Admitting Doctor
        "",                       # Patient Type
        sk,                       # Visit Number → missbraucht für SK (nicht Standard, aber simpel)
        "",                       # Financial Class
        "",                       # Charge Price Indicator
        "",                       # Courtesy Code
        "",                       # Credit Rating
        "",                       # Contract Code
        "",                       # Contract Effective Date
        "",                       # Contract Amount
        "",                       # Contract Period
        "",                       # Interest Code
        "",                       # Transfer to Bad Debt Code
        "",                       # Transfer to Bad Debt Date
        "",                       # Bad Debt Agency Code
        "",                       # Bad Debt Transfer Amount
        "",                       # Bad Debt Recovery Amount
        "",                       # Delete Account Indicator
        "",                       # Delete Account Date
        "",                       # Discharge Disposition
        "",                       # Discharged to Location
        "",                       # Diet Type
        "",                       # Servicing Facility
        "",                       # Bed Status
        "",                       # Account Status
        "",                       # Pending Location
        "",                       # Prior Temporary Location
        admit_ts,                 # Admit Date/Time
        discharge_ts,             # Discharge Date/Time
    ])
    return SEGMENT_SEP.join([msh, evn, pid_seg, pv1]) + SEGMENT_SEP


def stream_messages(
    count: int,
    event: str | None = None,
    mix: bool = False,
    mix_weights: dict[str, int] | None = None,
) -> Iterable[str]:
    mix_weights = mix_weights or {"A01": 5, "A08": 3, "A03": 2}
    for i in range(1, count + 1):
        ev = event
        if mix or ev is None:
            ev = random.choices(list(mix_weights.keys()), weights=list(mix_weights.values()))[0]
        yield build_message(ev, i)


def post(url: str, message: str, timeout: float = 4.0) -> tuple[int, str]:
    req = urlreq.Request(
        url, data=message.encode("utf-8"),
        headers={"Content-Type": "application/hl7-v2", "Accept": "application/json"},
        method="POST",
    )
    try:
        with urlreq.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", errors="ignore")
    except URLError as e:
        return 0, str(e)


def main():
    ap = argparse.ArgumentParser(description="HL7 ADT Simulator")
    ap.add_argument("--event", choices=list(EVENTS.keys()), help="Fester Event-Typ")
    ap.add_argument("--mix", action="store_true", help="Mix aus A01/A08/A03 (Default-Gewichtung)")
    ap.add_argument("-n", "--count", type=int, default=50, help="Anzahl Nachrichten")
    ap.add_argument("--post", metavar="URL", help="Endpunkt, an den gepostet wird")
    ap.add_argument("--rate", type=float, default=0.0,
                    help="Nachrichten pro Sekunde (0 = so schnell wie möglich)")
    args = ap.parse_args()

    if not args.event and not args.mix:
        args.mix = True

    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    success = 0
    for i, msg in enumerate(stream_messages(args.count, args.event, args.mix), 1):
        if args.post:
            code, body = post(args.post, msg)
            ok = 200 <= code < 300
            sys.stderr.write(
                f"[{i}/{args.count}] {'OK' if ok else 'ERR'} {code} {body[:80]}\n"
            )
            success += 1 if ok else 0
        else:
            sys.stdout.write(msg.replace(SEGMENT_SEP, "\n"))
            sys.stdout.write("\n---\n")
        if interval > 0:
            time.sleep(interval)

    if args.post:
        sys.stderr.write(f"Fertig. {success}/{args.count} erfolgreich.\n")


if __name__ == "__main__":
    main()
