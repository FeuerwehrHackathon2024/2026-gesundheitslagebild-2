from datetime import datetime

from .extensions import db


class Krankenhaus(db.Model):
    __tablename__ = "krankenhaus"

    ik = db.Column(db.String(32), primary_key=True)
    standortnummer = db.Column(db.String(32), nullable=True)
    name = db.Column(db.String(255), nullable=False)
    name_norm = db.Column(db.String(255), nullable=True)
    chain_name = db.Column(db.String(255), nullable=True)
    strasse = db.Column(db.String(255), nullable=True)
    hausnummer = db.Column(db.String(32), nullable=True)
    plz = db.Column(db.String(16), nullable=True)
    ort = db.Column(db.String(255), nullable=True)
    bundesland = db.Column(db.String(128), nullable=True)
    country = db.Column(db.String(128), nullable=True)
    lat = db.Column(db.Float, nullable=True)
    lon = db.Column(db.Float, nullable=True)
    telefon = db.Column(db.String(64), nullable=True)
    email = db.Column(db.String(255), nullable=True)
    website = db.Column(db.String(512), nullable=True)
    url_zugang = db.Column(db.String(512), nullable=True)
    url_weitere_infos = db.Column(db.String(512), nullable=True)
    anzahl_standorte = db.Column(db.Integer, nullable=True)
    betten = db.Column(db.Integer, nullable=True)
    vollstationaere_fallzahl = db.Column(db.Integer, nullable=True)
    teilstationaere_fallzahl = db.Column(db.Integer, nullable=True)
    ambulante_fallzahl = db.Column(db.Integer, nullable=True)
    staeb_fallzahl = db.Column(db.Integer, nullable=True)
    traeger_name = db.Column(db.String(255), nullable=True)
    traeger_art = db.Column(db.String(128), nullable=True)
    universitaet = db.Column(db.String(255), nullable=True)
    lehrkrankenhaus = db.Column(db.String(32), nullable=True)
    notfallstufe = db.Column(db.String(64), nullable=True)
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
    rating = db.Column(db.String(32), nullable=True)
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


# Backward-compatible alias for previous model name.
Krankenhaeuser = Krankenhaus


class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self) -> str:
        return f"<User {self.username}>"

