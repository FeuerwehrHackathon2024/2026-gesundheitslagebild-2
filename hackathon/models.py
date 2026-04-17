from datetime import datetime

from .extensions import db


class Krankenhaus(db.Model):
    __tablename__ = "krankenhaus"
    __table_args__ = (
        db.Index("ix_krankenhaus_ik", "ik"),
        db.Index("ix_krankenhaus_plz", "plz"),
        db.Index("ix_krankenhaus_sk_max", "sk_max"),
        db.Index("ix_krankenhaus_geo", "lat", "lon"),
    )

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    ik = db.Column(db.String(32), nullable=True)
    standortnummer = db.Column(db.String(32), nullable=True)
    name = db.Column(db.Text, nullable=False)
    name_norm = db.Column(db.Text, nullable=True)
    chain_name = db.Column(db.Text, nullable=True)
    strasse = db.Column(db.Text, nullable=True)
    hausnummer = db.Column(db.String(64), nullable=True)
    plz = db.Column(db.String(16), nullable=True)
    ort = db.Column(db.Text, nullable=True)
    bundesland = db.Column(db.String(128), nullable=True)
    country = db.Column(db.String(128), nullable=True)
    lat = db.Column(db.Float, nullable=True)
    lon = db.Column(db.Float, nullable=True)
    telefon = db.Column(db.String(128), nullable=True)
    email = db.Column(db.Text, nullable=True)
    website = db.Column(db.Text, nullable=True)
    url_zugang = db.Column(db.Text, nullable=True)
    url_weitere_infos = db.Column(db.Text, nullable=True)
    anzahl_standorte = db.Column(db.Integer, nullable=True)
    betten = db.Column(db.Integer, nullable=True)
    vollstationaere_fallzahl = db.Column(db.Integer, nullable=True)
    teilstationaere_fallzahl = db.Column(db.Integer, nullable=True)
    ambulante_fallzahl = db.Column(db.Integer, nullable=True)
    staeb_fallzahl = db.Column(db.Integer, nullable=True)
    traeger_name = db.Column(db.Text, nullable=True)
    traeger_art = db.Column(db.String(128), nullable=True)
    universitaet = db.Column(db.Text, nullable=True)
    lehrkrankenhaus = db.Column(db.String(64), nullable=True)
    notfallstufe = db.Column(db.String(128), nullable=True)
    notdienst = db.Column(db.String(32), nullable=True)
    notdienstpraxis = db.Column(db.String(32), nullable=True)
    hat_dialyse = db.Column(db.Boolean, nullable=True)
    hat_onkologie = db.Column(db.Boolean, nullable=True)
    hat_geriatrie = db.Column(db.Boolean, nullable=True)
    hat_intensivmedizin = db.Column(db.Boolean, nullable=True)
    hat_notaufnahme = db.Column(db.Boolean, nullable=True)
    hat_psychiatrie = db.Column(db.Boolean, nullable=True)
    hat_bg_zulassung = db.Column(db.Boolean, nullable=True)
    hat_radiologie = db.Column(db.Boolean, nullable=True)
    hat_darzt = db.Column(db.String(32), nullable=True)
    anzahl_fachabteilungen = db.Column(db.Integer, nullable=True)
    fachabteilungen = db.Column(db.Text, nullable=True)
    apparative_ausstattung = db.Column(db.Text, nullable=True)
    rating = db.Column(db.String(64), nullable=True)
    reviews = db.Column(db.Integer, nullable=True)
    opening_hours_present = db.Column(db.Boolean, nullable=True)
    importance_score = db.Column(db.Float, nullable=True)
    priority_bucket = db.Column(db.String(32), nullable=True)
    confidence = db.Column(db.Float, nullable=True)
    estimated_rides_per_year = db.Column(db.Integer, nullable=True)
    estimated_rides_per_year_db = db.Column(db.Integer, nullable=True)
    estimated_rides_per_year_llm = db.Column(db.Integer, nullable=True)
    allocated_exact_db = db.Column(db.Boolean, nullable=True)
    target_tier = db.Column(db.String(32), nullable=True)
    target_classification = db.Column(db.String(64), nullable=True)
    segment = db.Column(db.String(64), nullable=True)
    sector = db.Column(db.String(64), nullable=True)
    source = db.Column(db.String(64), nullable=True)
    source_id = db.Column(db.String(64), nullable=True)
    facility_enriched_id = db.Column(db.Integer, nullable=True)
    merge_source = db.Column(db.String(64), nullable=True)
    kann_sk1 = db.Column(db.Boolean, nullable=True)
    kann_sk2 = db.Column(db.Boolean, nullable=True)
    kann_sk3 = db.Column(db.Boolean, nullable=True)
    sk_max = db.Column(db.String(16), nullable=True)
    kapazitaet_sk1_geschaetzt = db.Column(db.Integer, nullable=True)
    kapazitaet_sk2_geschaetzt = db.Column(db.Integer, nullable=True)
    kapazitaet_sk3_geschaetzt = db.Column(db.Integer, nullable=True)
    sk_begruendung = db.Column(db.Text, nullable=True)
    # Vom User ausgeschlossen (defekt/nicht erreichbar) — wird im Dispatch ignoriert
    ausgeschlossen = db.Column(db.Boolean, default=False, nullable=False)
    ausschluss_grund = db.Column(db.Text, nullable=True)


# Backward-compatible alias for previous model name.
Krankenhaeuser = Krankenhaus


class Hub(db.Model):
    __tablename__ = "hub"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), nullable=False, unique=True)
    lat = db.Column(db.Float, nullable=False)
    lon = db.Column(db.Float, nullable=False)
    ort = db.Column(db.String(128), nullable=True)
    bundesland = db.Column(db.String(64), nullable=True)
    kapazitaet_pro_tag = db.Column(db.Integer, nullable=True)
    beschreibung = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "lat": self.lat,
            "lon": self.lon,
            "ort": self.ort,
            "bundesland": self.bundesland,
            "kapazitaet_pro_tag": self.kapazitaet_pro_tag,
            "beschreibung": self.beschreibung,
        }


class PatientenBatch(db.Model):
    """Ein Upload einer Patientenliste (XLSX)."""
    __tablename__ = "patienten_batch"

    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(255), nullable=True)
    hub_id = db.Column(db.Integer, db.ForeignKey("hub.id"), nullable=True)
    hub_name = db.Column(db.String(128), nullable=True)
    total = db.Column(db.Integer, default=0)
    sk1 = db.Column(db.Integer, default=0)
    sk2 = db.Column(db.Integer, default=0)
    sk3 = db.Column(db.Integer, default=0)
    status = db.Column(db.String(32), default="uploaded")  # uploaded | dispatched
    uploaded_at = db.Column(db.DateTime, default=datetime.utcnow)
    dispatched_at = db.Column(db.DateTime, nullable=True)
    patients = db.relationship("Patient", backref="batch", lazy="dynamic",
                               cascade="all, delete-orphan")


class Patient(db.Model):
    __tablename__ = "patient"
    __table_args__ = (
        db.Index("ix_patient_batch_status", "batch_id", "status"),
        db.Index("ix_patient_sk", "sk"),
    )

    id = db.Column(db.Integer, primary_key=True)
    batch_id = db.Column(db.Integer, db.ForeignKey("patienten_batch.id"), nullable=False)
    external_id = db.Column(db.String(64), nullable=True)
    sk = db.Column(db.String(8), nullable=False)  # SK1 / SK2 / SK3
    datum = db.Column(db.Date, nullable=True)
    eingangssichtung = db.Column(db.DateTime, nullable=True)
    transportbereit = db.Column(db.DateTime, nullable=True)
    quelle = db.Column(db.String(128), nullable=True)
    assigned_krankenhaus_id = db.Column(db.Integer, db.ForeignKey("krankenhaus.id"), nullable=True)
    assigned_at = db.Column(db.DateTime, nullable=True)
    aufenthaltsdauer_tage = db.Column(db.Integer, nullable=True)
    distanz_km = db.Column(db.Float, nullable=True)
    status = db.Column(db.String(32), default="pending")  # pending | assigned | unassigned
    note = db.Column(db.Text, nullable=True)

    assigned_krankenhaus = db.relationship("Krankenhaus")


class KrankenhausBelegung(db.Model):
    """Aktuelle/simulierte Bettenbelegung pro Klinik + SK-Stufe."""
    __tablename__ = "krankenhaus_belegung"

    krankenhaus_id = db.Column(db.Integer, db.ForeignKey("krankenhaus.id"), primary_key=True)
    kapazitaet_sk1 = db.Column(db.Integer, default=0)
    kapazitaet_sk2 = db.Column(db.Integer, default=0)
    kapazitaet_sk3 = db.Column(db.Integer, default=0)
    belegung_sk1 = db.Column(db.Integer, default=0)
    belegung_sk2 = db.Column(db.Integer, default=0)
    belegung_sk3 = db.Column(db.Integer, default=0)
    vorbelegung_prozent = db.Column(db.Integer, default=0)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    krankenhaus = db.relationship("Krankenhaus", backref=db.backref("belegung", uselist=False))

    def frei(self, sk: str) -> int:
        cap = getattr(self, f"kapazitaet_{sk.lower()}")
        bel = getattr(self, f"belegung_{sk.lower()}")
        return max(cap - bel, 0)


class TransportAuftrag(db.Model):
    __tablename__ = "transport_auftrag"
    __table_args__ = (
        db.Index("ix_transport_patient", "patient_id"),
        db.Index("ix_transport_created", "erzeugt_am"),
    )

    id = db.Column(db.Integer, primary_key=True)
    patient_id = db.Column(db.Integer, db.ForeignKey("patient.id"), nullable=False)
    batch_id = db.Column(db.Integer, db.ForeignKey("patienten_batch.id"), nullable=True)
    hub_id = db.Column(db.Integer, db.ForeignKey("hub.id"), nullable=True)
    hub_lat = db.Column(db.Float, nullable=True)
    hub_lon = db.Column(db.Float, nullable=True)
    krankenhaus_id = db.Column(db.Integer, db.ForeignKey("krankenhaus.id"), nullable=False)
    ziel_lat = db.Column(db.Float, nullable=True)
    ziel_lon = db.Column(db.Float, nullable=True)
    sk = db.Column(db.String(8), nullable=True)
    distanz_km = db.Column(db.Float, nullable=True)
    dauer_min = db.Column(db.Float, nullable=True)
    route_geojson = db.Column(db.Text, nullable=True)
    status = db.Column(db.String(32), default="geplant")  # geplant | unterwegs | abgeschlossen
    erzeugt_am = db.Column(db.DateTime, default=datetime.utcnow)

    patient = db.relationship("Patient")
    krankenhaus = db.relationship("Krankenhaus")
    hub = db.relationship("Hub")


class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self) -> str:
        return f"<User {self.username}>"

