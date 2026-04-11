"""
NIHSA Backend — SQLAlchemy Models
Windows / SQLite compatible (no geoalchemy2 / PostGIS required).

Geometry columns are stored as plain strings:
  - POINT  → "lat,lng"  e.g. "7.800,6.740"
  - POLYGON → WKT string (can upgrade to PostGIS later)

To migrate to PostGIS in production:
  1. Install geoalchemy2 and PostgreSQL + PostGIS
  2. Replace all Column(String, ...) geometry fields with Column(Geometry(...))
"""

from sqlalchemy import (
    Column, String, Integer, Float, Boolean, Text, DateTime,
    ForeignKey, Enum, JSON, Index, UniqueConstraint
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import enum
import uuid
from database import Base          # ← plain "database", not "app.database"


def gen_uuid():
    return str(uuid.uuid4())


# ── ENUMS ──────────────────────────────────────────────────────────────────────

class UserRole(str, enum.Enum):
    CITIZEN     = "citizen"
    VANGUARD    = "vanguard"
    RESEARCHER  = "researcher"
    GOVERNMENT  = "government"
    NIHSA_STAFF = "nihsa_staff"
    SUB_ADMIN   = "sub_admin"
    ADMIN       = "admin"

class VanguardRank(str, enum.Enum):
    TRAINEE          = "trainee"
    FIELD_VANGUARD   = "field_vanguard"
    SENIOR_VANGUARD  = "senior_vanguard"
    COORDINATOR      = "coordinator"
    STATE_LEAD       = "state_lead"

class RiskLevel(str, enum.Enum):
    NORMAL   = "NORMAL"
    WATCH    = "WATCH"
    MEDIUM   = "MEDIUM"
    HIGH     = "HIGH"
    CRITICAL = "CRITICAL"

class VerificationStatus(str, enum.Enum):
    PENDING   = "pending"
    AI_REVIEW = "ai_review"
    VERIFIED  = "verified"
    REJECTED  = "rejected"
    ESCALATED = "escalated"

class AlertChannel(str, enum.Enum):
    PUSH  = "push"
    SMS   = "sms"
    EMAIL = "email"
    ALL   = "all"


# ── USERS ──────────────────────────────────────────────────────────────────────

class User(Base):
    __tablename__ = "users"

    id            = Column(String, primary_key=True, default=gen_uuid)
    name          = Column(String(120), nullable=False)
    email         = Column(String(255), unique=True, index=True)
    phone_number  = Column(String(20), unique=True, index=True)
    password_hash = Column(String(255), nullable=False)
    role          = Column(Enum(UserRole), default=UserRole.CITIZEN, nullable=False)
    sub_admin_scope = Column(String(60))   # e.g. "surface_water", "forecast", "reports"
    state         = Column(String(60))
    lga           = Column(String(100))
    location_wkt  = Column(String(60))      # "lat,lng" e.g. "7.800,6.740"
    is_active     = Column(Boolean, default=True)
    is_verified   = Column(Boolean, default=False)
    push_token    = Column(String(512))
    language      = Column(String(10), default="en")
    created_at    = Column(DateTime(timezone=True), server_default=func.now())
    last_login    = Column(DateTime(timezone=True))
    profile_pic   = Column(String(512))

    flood_reports    = relationship("FloodReport", back_populates="user", foreign_keys="FloodReport.user_id")
    vanguard_profile = relationship("VanguardProfile", back_populates="user", uselist=False)
    alert_subs       = relationship("AlertSubscription", back_populates="user")
    chat_messages    = relationship("VanguardChatMessage", back_populates="user")

    __table_args__ = (
        Index("idx_users_state_lga", "state", "lga"),
    )


class VanguardProfile(Base):
    __tablename__ = "vanguard_profiles"

    id                 = Column(String, primary_key=True, default=gen_uuid)
    user_id            = Column(String, ForeignKey("users.id", ondelete="CASCADE"), unique=True)
    rank               = Column(Enum(VanguardRank), default=VanguardRank.TRAINEE)
    zone               = Column(String(100))
    assigned_lga       = Column(String(100))
    reports_count      = Column(Integer, default=0)
    verified_reports   = Column(Integer, default=0)
    accuracy_score     = Column(Float, default=0.0)
    is_on_duty         = Column(Boolean, default=False)
    last_checkin       = Column(DateTime(timezone=True))
    equipment          = Column(JSON)
    training_completed = Column(Boolean, default=False)
    joined_at          = Column(DateTime(timezone=True), server_default=func.now())

    user = relationship("User", back_populates="vanguard_profile")


# ── GEOGRAPHY ──────────────────────────────────────────────────────────────────

class RiverBasin(Base):
    __tablename__ = "river_basins"

    id            = Column(String, primary_key=True, default=gen_uuid)
    basin_code    = Column(String(50), unique=True, nullable=False, index=True)
    name          = Column(String(120), nullable=False)
    river_system  = Column(String(120))
    area_km2      = Column(Float)
    geometry_wkt  = Column(Text)            # WKT MULTIPOLYGON string
    states        = Column(JSON)
    transboundary = Column(Boolean, default=False)
    countries     = Column(JSON)
    created_at    = Column(DateTime(timezone=True), server_default=func.now())

    forecasts = relationship("FloodForecast", back_populates="basin")
    gauges    = relationship("RiverGauge", back_populates="basin")
    alerts    = relationship("FloodAlert", back_populates="basin")


class NigeriaLGA(Base):
    __tablename__ = "nigeria_lgas"

    id           = Column(Integer, primary_key=True)
    lga_code     = Column(String(20), unique=True)
    lga_name     = Column(String(120), nullable=False)
    state_name   = Column(String(60), nullable=False, index=True)
    geometry_wkt = Column(Text)             # WKT MULTIPOLYGON string
    population   = Column(Integer)
    area_km2     = Column(Float)


# ── HYDROLOGICAL MONITORING ────────────────────────────────────────────────────

class RiverGauge(Base):
    __tablename__ = "river_gauges"

    station_id   = Column(String, primary_key=True, default=gen_uuid)
    station_code = Column(String(20), unique=True, nullable=False)
    station_name = Column(String(120), nullable=False)
    river_name   = Column(String(120))
    basin_id     = Column(String, ForeignKey("river_basins.id"))
    lat          = Column(Float)            # replaces Geometry POINT
    lng          = Column(Float)
    state        = Column(String(60), index=True)
    lga          = Column(String(100))
    elevation_m  = Column(Float)
    datum_m      = Column(Float)
    warning_level= Column(Float)
    danger_level = Column(Float)
    is_active    = Column(Boolean, default=True)
    operator     = Column(String(120))
    installed_at = Column(DateTime(timezone=True))
    last_serviced= Column(DateTime(timezone=True))
    telemetry    = Column(Boolean, default=False)
    metadata_json= Column(JSON)

    basin    = relationship("RiverBasin", back_populates="gauges")
    readings = relationship("GaugeReading", back_populates="station", cascade="all, delete-orphan")


class GaugeReading(Base):
    __tablename__ = "gauge_readings"

    id          = Column(String, primary_key=True, default=gen_uuid)
    station_id  = Column(String, ForeignKey("river_gauges.station_id", ondelete="CASCADE"), nullable=False, index=True)
    water_level = Column(Float, nullable=False)
    discharge   = Column(Float)
    rainfall_mm = Column(Float)
    temperature = Column(Float)
    risk_level  = Column(Enum(RiskLevel), default=RiskLevel.NORMAL)
    source      = Column(String(50), default="manual")   # manual | telemetry | satellite
    recorded_at = Column(DateTime(timezone=True), nullable=False, index=True)
    created_at  = Column(DateTime(timezone=True), server_default=func.now())
    created_by  = Column(String, ForeignKey("users.id"))
    notes       = Column(Text)

    station = relationship("RiverGauge", back_populates="readings")

    __table_args__ = (
        Index("idx_reading_station_time", "station_id", "recorded_at"),
    )


# ── FLOOD FORECASTS ────────────────────────────────────────────────────────────

class FloodForecast(Base):
    __tablename__ = "flood_forecasts"

    id            = Column(String, primary_key=True, default=gen_uuid)
    basin_id      = Column(String, ForeignKey("river_basins.id"), nullable=False, index=True)
    forecast_date = Column(DateTime(timezone=True), nullable=False)
    horizon_days  = Column(Integer, default=7)
    model_version = Column(String(50))
    stage         = Column(Integer, default=1)     # LSTM Stage 1 or 2
    # Q05 / Q50 / Q95 stored as JSON arrays: [day1, day2, ..., day7]
    q05           = Column(JSON)
    q50           = Column(JSON)
    q95           = Column(JSON)
    peak_q50      = Column(Float)
    peak_date     = Column(DateTime(timezone=True))
    risk_level    = Column(Enum(RiskLevel), default=RiskLevel.NORMAL)
    nse_score     = Column(Float)
    created_at    = Column(DateTime(timezone=True), server_default=func.now())

    basin = relationship("RiverBasin", back_populates="forecasts")

    __table_args__ = (
        Index("idx_forecast_basin_date", "basin_id", "forecast_date"),
    )


# ── FLOOD ALERTS ───────────────────────────────────────────────────────────────

class FloodAlert(Base):
    __tablename__ = "flood_alerts"

    id          = Column(String, primary_key=True, default=gen_uuid)
    basin_id    = Column(String, ForeignKey("river_basins.id"))
    title       = Column(String(200), nullable=False)
    message     = Column(Text, nullable=False)
    level       = Column(Enum(RiskLevel), nullable=False)
    state       = Column(String(60), index=True)
    lgas        = Column(JSON)                 # list of affected LGA names
    start_date  = Column(DateTime(timezone=True))
    end_date    = Column(DateTime(timezone=True))
    is_active   = Column(Boolean, default=True)
    is_published= Column(Boolean, default=False)
    channels    = Column(JSON, default=["push"])
    issued_by   = Column(String, ForeignKey("users.id"))
    created_at  = Column(DateTime(timezone=True), server_default=func.now())
    updated_at  = Column(DateTime(timezone=True), onupdate=func.now())
    lat = Column(Float, nullable=True)
    lng = Column(Float, nullable=True)
  
    basin = relationship("RiverBasin", back_populates="alerts")

    __table_args__ = (
        Index("idx_alert_state_active", "state", "is_active"),
    )


# ── FLOOD REPORTS (citizen / vanguard) ────────────────────────────────────────

class FloodReport(Base):
    __tablename__ = "flood_reports"

    id              = Column(String, primary_key=True, default=gen_uuid)
    user_id         = Column(String, ForeignKey("users.id"), nullable=True, index=True)
    description     = Column(Text, nullable=False)
    water_depth_m   = Column(Float)
    state           = Column(String(60), index=True)
    lga             = Column(String(100))
    address         = Column(Text)
    lat             = Column(Float)             # replaces Geometry POINT
    lng             = Column(Float)
    media_urls      = Column(JSON, default=[])
    status          = Column(Enum(VerificationStatus), default=VerificationStatus.PENDING)
    verified_by     = Column(String, ForeignKey("users.id"))
    verified_at     = Column(DateTime(timezone=True))
    ai_confidence   = Column(Float)
    rejection_reason= Column(Text)
    risk_level      = Column(Enum(RiskLevel), default=RiskLevel.MEDIUM)
    submitted_at    = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    updated_at      = Column(DateTime(timezone=True), onupdate=func.now())

    user = relationship("User", back_populates="flood_reports", foreign_keys=[user_id])

    __table_args__ = (
        Index("idx_report_state_status", "state", "status"),
        Index("idx_report_submitted", "submitted_at"),
    )


# ── ALERT SUBSCRIPTIONS ────────────────────────────────────────────────────────

class AlertSubscription(Base):
    __tablename__ = "alert_subscriptions"

    id             = Column(String, primary_key=True, default=gen_uuid)
    user_id        = Column(String, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    state          = Column(String(60))
    lga            = Column(String(100))
    min_risk_level = Column(Enum(RiskLevel), default=RiskLevel.WATCH)
    channels       = Column(JSON, default=["push"])
    is_active      = Column(Boolean, default=True)
    created_at     = Column(DateTime(timezone=True), server_default=func.now())

    user = relationship("User", back_populates="alert_subs")

    __table_args__ = (
        UniqueConstraint("user_id", "state", "lga", name="uq_alert_sub"),
    )


# ── VANGUARD CHAT ──────────────────────────────────────────────────────────────

class VanguardChannel(Base):
    __tablename__ = "vanguard_channels"

    id          = Column(String, primary_key=True, default=gen_uuid)
    channel_key = Column(String(80), unique=True, nullable=False)
    name        = Column(String(150), nullable=False)
    description = Column(Text)
    state       = Column(String(60))
    risk_level  = Column(Enum(RiskLevel), default=RiskLevel.NORMAL)
    is_active   = Column(Boolean, default=True)
    is_command  = Column(Boolean, default=False)
    created_at  = Column(DateTime(timezone=True), server_default=func.now())

    messages = relationship("VanguardChatMessage", back_populates="channel", cascade="all, delete-orphan")


class VanguardChatMessage(Base):
    __tablename__ = "vanguard_chat_messages"

    id           = Column(String, primary_key=True, default=gen_uuid)
    channel_id   = Column(String, ForeignKey("vanguard_channels.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id      = Column(String, ForeignKey("users.id"), nullable=False)
    message      = Column(Text, nullable=False)
    message_type = Column(String(20), default="text")
    location_wkt = Column(String(60))       # "lat,lng" string
    media_url    = Column(String(512))
    is_ai        = Column(Boolean, default=False)
    reactions    = Column(JSON, default=[])
    is_pinned    = Column(Boolean, default=False)
    created_at   = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    edited_at    = Column(DateTime(timezone=True))

    channel = relationship("VanguardChannel", back_populates="messages")
    user    = relationship("User", back_populates="chat_messages")

    __table_args__ = (
        Index("idx_chat_channel_time", "channel_id", "created_at"),
    )


# ── SATELLITE & REMOTE SENSING ────────────────────────────────────────────────

class SatelliteFloodExtent(Base):
    __tablename__ = "satellite_flood_extents"

    id               = Column(String, primary_key=True, default=gen_uuid)
    acquisition_date = Column(DateTime(timezone=True), nullable=False, index=True)
    satellite        = Column(String(50))
    orbit_pass       = Column(String(20))
    flood_geometry_wkt = Column(Text)       # WKT MULTIPOLYGON
    area_km2         = Column(Float)
    states           = Column(JSON)
    confidence       = Column(Float)
    image_url        = Column(String(512))
    voice_url     = Column(String(500), nullable=True)
    video_url     = Column(String(500), nullable=True)
    source           = Column(String(100))
    processed_at     = Column(DateTime(timezone=True), server_default=func.now())


# ── RESERVOIRS & DAMS ─────────────────────────────────────────────────────────

class Reservoir(Base):
    __tablename__ = "reservoirs"

    id                = Column(String, primary_key=True, default=gen_uuid)
    name              = Column(String(120), nullable=False)
    dam_name          = Column(String(120))
    lat               = Column(Float)
    lng               = Column(Float)
    state             = Column(String(60))
    river             = Column(String(120))
    full_supply_level = Column(Float)
    dead_level        = Column(Float)
    flood_gate_level  = Column(Float)
    total_capacity_mm3= Column(Float)
    operator          = Column(String(120))
    is_active         = Column(Boolean, default=True)

    level_readings = relationship("ReservoirReading", back_populates="reservoir", cascade="all, delete-orphan")


class ReservoirReading(Base):
    __tablename__ = "reservoir_readings"

    id             = Column(String, primary_key=True, default=gen_uuid)
    reservoir_id   = Column(String, ForeignKey("reservoirs.id", ondelete="CASCADE"), nullable=False, index=True)
    water_level    = Column(Float)
    storage_mm3    = Column(Float)
    inflow_m3s     = Column(Float)
    outflow_m3s    = Column(Float)
    spillway_open  = Column(Boolean, default=False)
    spillway_gates = Column(Integer, default=0)
    recorded_at    = Column(DateTime(timezone=True), nullable=False, index=True)

    reservoir = relationship("Reservoir", back_populates="level_readings")


# ── EVACUATION CENTERS ────────────────────────────────────────────────────────

class EvacuationCenter(Base):
    __tablename__ = "evacuation_centers"

    id                = Column(String, primary_key=True, default=gen_uuid)
    name              = Column(String(150), nullable=False)
    lat               = Column(Float)
    lng               = Column(Float)
    state             = Column(String(60), index=True)
    lga               = Column(String(100))
    address           = Column(Text)
    capacity          = Column(Integer)
    current_occupancy = Column(Integer, default=0)
    is_active         = Column(Boolean, default=True)
    managed_by        = Column(String(120))
    contact_phone     = Column(String(20))
    has_water         = Column(Boolean, default=False)
    has_food          = Column(Boolean, default=False)
    has_medical       = Column(Boolean, default=False)
    updated_at        = Column(DateTime(timezone=True), onupdate=func.now())


# ── HISTORICAL FLOOD EVENTS ───────────────────────────────────────────────────

class HistoricalFloodEvent(Base):
    __tablename__ = "historical_flood_events"

    id                = Column(String, primary_key=True, default=gen_uuid)
    event_name        = Column(String(200))
    year              = Column(Integer, nullable=False, index=True)
    start_date        = Column(DateTime(timezone=True))
    end_date          = Column(DateTime(timezone=True))
    peak_date         = Column(DateTime(timezone=True))
    basin_id          = Column(String, ForeignKey("river_basins.id"))
    states_affected   = Column(JSON)
    peak_discharge    = Column(Float)
    peak_level        = Column(Float)
    area_flooded_km2  = Column(Float)
    deaths            = Column(Integer)
    displaced         = Column(Integer)
    economic_loss_ngn = Column(Float)
    cause             = Column(String(200))
    flood_geometry_wkt= Column(Text)        # WKT MULTIPOLYGON
    data_source       = Column(String(200))
    notes             = Column(Text)

    __table_args__ = (
        Index("idx_hist_year", "year"),
    )


# ── AUDIT LOG ─────────────────────────────────────────────────────────────────

class AuditLog(Base):
    __tablename__ = "audit_logs"

    id          = Column(String, primary_key=True, default=gen_uuid)
    user_id     = Column(String, ForeignKey("users.id"))
    action      = Column(String(100), nullable=False)
    resource    = Column(String(100))
    resource_id = Column(String(200))
    ip_address  = Column(String(45))
    user_agent  = Column(String(512))
    details     = Column(JSON)
    created_at  = Column(DateTime(timezone=True), server_default=func.now(), index=True)

    __table_args__ = (
        Index("idx_audit_user_time", "user_id", "created_at"),
        Index("idx_audit_action", "action"),
    )


# ── MAP LAYERS ─────────────────────────────────────────────────────────────────

class MapLayer(Base):
    __tablename__ = "map_layers"

    id              = Column(String, primary_key=True, default=gen_uuid)
    name            = Column(String(200), nullable=False)
    group_key       = Column(String(50), nullable=False)  # surface_water|groundwater|water_quality|coastal_marine|forecast
    layer_key       = Column(String(100), nullable=False)
    description     = Column(Text, default="")
    layer_type      = Column(String(20), default="toggle")  # toggle|atlas|geojson|wms
    source_url      = Column(String(512), default="")
    icon            = Column(String(20), default="🗺️")
    display_order   = Column(Integer, default=0)
    is_active       = Column(Boolean, default=True)
    default_visible = Column(Boolean, default=False)
    meta            = Column(JSON, default={})
    created_at      = Column(DateTime(timezone=True), server_default=func.now())
    updated_at      = Column(DateTime(timezone=True), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("layer_key", name="uq_map_layer_key"),
        Index("idx_map_layer_group", "group_key"),
    )
