"""NIHSA Backend — Pydantic Schemas"""

from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List
from datetime import datetime
from enum import Enum


# ── ENUMS (mirror models.py exactly) ──────────────────────────────────────────
class RiskLevel(str, Enum):
    NORMAL   = "NORMAL"
    WATCH    = "WATCH"
    MEDIUM   = "MEDIUM"
    HIGH     = "HIGH"
    CRITICAL = "CRITICAL"

class UserRole(str, Enum):
    CITIZEN     = "citizen"
    VANGUARD    = "vanguard"
    RESEARCHER  = "researcher"
    GOVERNMENT  = "government"
    NIHSA_STAFF = "nihsa_staff"
    SUB_ADMIN   = "sub_admin"
    ADMIN       = "admin"

class VerificationStatus(str, Enum):
    PENDING   = "pending"
    AI_REVIEW = "ai_review"
    VERIFIED  = "verified"
    REJECTED  = "rejected"
    ESCALATED = "escalated"


# ── AUTH ───────────────────────────────────────────────────────────────────────
class UserRegister(BaseModel):
    name: str = Field(..., min_length=2, max_length=120)
    email: Optional[EmailStr] = None
    phone_number: Optional[str] = Field(None, pattern=r"^\+?[0-9]{10,15}$")
    password: str = Field(..., min_length=8)
    state: Optional[str] = None
    lga: Optional[str] = None
    # role intentionally omitted — always defaults to CITIZEN on register

class UserLogin(BaseModel):
    identifier: str   # email or phone number
    password: str

class UserOut(BaseModel):
    id: str
    name: str
    email: Optional[str] = None
    phone_number: Optional[str] = None
    role: UserRole
    sub_admin_scope: Optional[str] = None
    state: Optional[str] = None
    lga: Optional[str] = None
    is_verified: bool = False
    created_at: datetime
    class Config:
        from_attributes = True

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserOut

class UserUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=2, max_length=120)
    state: Optional[str] = None
    lga: Optional[str] = None


# ── GAUGE STATIONS ─────────────────────────────────────────────────────────────
class GaugeOut(BaseModel):
    """Matches RiverGauge model — uses lat/lng not latitude/longitude."""
    station_id: str
    station_code: Optional[str] = None
    station_name: str
    river_name: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    state: Optional[str] = None
    lga: Optional[str] = None
    warning_level: Optional[float] = None
    danger_level: Optional[float] = None
    is_active: bool = True
    telemetry: bool = False
    class Config:
        from_attributes = True

class GaugeCreate(BaseModel):
    station_code: Optional[str] = None   # auto-generated if not provided
    station_name: str = Field(..., min_length=1, max_length=120)
    river_name: Optional[str] = None
    river: Optional[str] = None          # alias from admin panel
    lat: float
    lng: float
    state: Optional[str] = None
    lga: Optional[str] = None
    warning_level: Optional[float] = None
    danger_level: Optional[float] = None
    is_active: bool = True
    telemetry: bool = False

    def model_post_init(self, __context):
        # Accept "river" as alias for "river_name"
        if self.river and not self.river_name:
            object.__setattr__(self, "river_name", self.river)
        # Auto-generate station_code if not provided
        if not self.station_code:
            import uuid
            object.__setattr__(self, "station_code", "G" + str(uuid.uuid4())[:6].upper())

class GaugeReadingCreate(BaseModel):
    station_id: Optional[str] = None    # accepted in body (admin panel)
    water_level: float = Field(..., ge=0, le=30, description="Metres above datum")
    flow_rate: Optional[float] = None   # alias used by admin panel
    discharge: Optional[float] = Field(None, ge=0, description="m³/s")
    rainfall_mm: Optional[float] = Field(None, ge=0)
    temperature: Optional[float] = None
    source: str = "manual"
    recorded_at: Optional[datetime] = None
    notes: Optional[str] = None

    def model_post_init(self, __context):
        if self.flow_rate and not self.discharge:
            object.__setattr__(self, "discharge", self.flow_rate)

class GaugeReadingOut(BaseModel):
    """Matches GaugeReading model — uses station_id not gauge_id."""
    id: str
    station_id: str
    water_level: float
    discharge: Optional[float] = None
    rainfall_mm: Optional[float] = None
    risk_level: RiskLevel = RiskLevel.NORMAL
    source: str = "manual"
    recorded_at: datetime
    notes: Optional[str] = None
    class Config:
        from_attributes = True


# ── FLOOD REPORTS ──────────────────────────────────────────────────────────────
class FloodReportCreate(BaseModel):
    """Matches FloodReport model — uses lat/lng, status, submitted_at."""
    lat: float = Field(..., ge=4.0, le=14.0)
    lng: float = Field(..., ge=2.5, le=15.0)
    state: Optional[str] = None
    lga: Optional[str] = None
    address: Optional[str] = None
    description: str = Field(..., min_length=10, max_length=2000)
    water_depth_m: Optional[float] = Field(None, ge=0, le=20)
    media_urls: Optional[List[str]] = []

# Alias so reports.py can use either name
ReportCreate = FloodReportCreate

class FloodReportOut(BaseModel):
    id: str
    user_id: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    state: Optional[str] = None
    lga: Optional[str] = None
    address: Optional[str] = None
    description: str
    water_depth_m: Optional[float] = None
    media_urls: Optional[List[str]] = []
    status: VerificationStatus = VerificationStatus.PENDING
    risk_level: RiskLevel = RiskLevel.MEDIUM
    submitted_at: datetime
    class Config:
        from_attributes = True

# Alias so reports.py can use either name
ReportOut = FloodReportOut

class FloodReportVerify(BaseModel):
    status: VerificationStatus
    rejection_reason: Optional[str] = None
    risk_level: Optional[RiskLevel] = None

class FloodReportPage(BaseModel):
    items: List[FloodReportOut]
    total: int
    page: int
    pages: int


# ── FLOOD ALERTS ───────────────────────────────────────────────────────────────
class AlertCreate(BaseModel):
    """Matches FloodAlert model fields exactly."""
    basin_id: Optional[str] = None
    title: str = Field(..., min_length=5, max_length=200)
    message: str = Field(..., min_length=10)
    level: RiskLevel                          # model field is "level"
    state: Optional[str] = None
    lgas: Optional[List[str]] = []
    is_published: bool = False

class AlertUpdate(BaseModel):
    title: Optional[str] = None
    message: Optional[str] = None
    level: Optional[RiskLevel] = None
    state: Optional[str] = None
    lgas: Optional[List[str]] = None
    is_active: Optional[bool] = None
    is_published: Optional[bool] = None

class AlertOut(BaseModel):
    """Matches FloodAlert model — level not risk_level, created_at not issued_at."""
    id: str
    title: str
    message: str
    level: RiskLevel
    state: Optional[str] = None
    lgas: Optional[List[str]] = []
    is_active: bool = True
    is_published: bool = False
    created_at: datetime
    lat: Optional[float] = None   
    lng: Optional[float] = None 
    
    class Config:
        from_attributes = True

class AlertPage(BaseModel):
    items: List[AlertOut]
    total: int
    page: int
    pages: int


# ── FORECASTS ──────────────────────────────────────────────────────────────────
class ForecastCreate(BaseModel):
    model_config = {"protected_namespaces": ()}
    """Used by forecast router POST endpoint."""
    basin_id: str
    forecast_date: datetime
    horizon_days: int = 7
    q05: Optional[List[float]] = None
    q50: Optional[List[float]] = None
    q95: Optional[List[float]] = None
    peak_q50: Optional[float] = None
    peak_date: Optional[datetime] = None
    risk_level: RiskLevel = RiskLevel.NORMAL
    nse_score: Optional[float] = None
    stage: int = 1
    model_version: Optional[str] = None

class ForecastOut(BaseModel):
    id: str
    basin_id: str
    forecast_date: datetime
    horizon_days: int = 7
    q05: Optional[List[float]] = None
    q50: Optional[List[float]] = None
    q95: Optional[List[float]] = None
    peak_q50: Optional[float] = None
    peak_date: Optional[datetime] = None
    risk_level: RiskLevel = RiskLevel.NORMAL
    nse_score: Optional[float] = None
    model_name: Optional[str] = None     # computed/alias field — not in DB
    created_at: datetime
    class Config:
        from_attributes = True
        protected_namespaces = ()


# ── VANGUARD CHAT ──────────────────────────────────────────────────────────────
class ChannelOut(BaseModel):
    id: str
    channel_key: str
    name: str
    description: Optional[str] = None
    state: Optional[str] = None
    risk_level: RiskLevel = RiskLevel.NORMAL
    is_active: bool = True
    is_command: bool = False
    created_at: datetime
    class Config:
        from_attributes = True

class ChatMessageCreate(BaseModel):
    message: str = Field(..., min_length=1, max_length=2000)
    message_type: str = "text"
    media_url: Optional[str] = None
    # lat/lng optional for location-tagged messages
    lat: Optional[float] = None
    lng: Optional[float] = None

class ChatMessageOut(BaseModel):
    """Matches VanguardChatMessage model."""
    id: str
    channel_id: str
    user_id: str
    user_name: str     # populated from join in router
    user_role: str     # populated from join in router
    message: str
    message_type: str = "text"
    media_url: Optional[str] = None
    is_ai: bool = False
    reactions: List[str] = []
    is_pinned: bool = False
    created_at: datetime
    class Config:
        from_attributes = True

class ChatPage(BaseModel):
    items: List[ChatMessageOut]
    total: int


# ── DASHBOARD ──────────────────────────────────────────────────────────────────
class DashboardStats(BaseModel):
    """Matches exactly what dashboard router returns."""
    active_alerts: int
    critical_alerts: int
    gauges_online: int
    gauges_critical: int
    reports_today: int
    reports_pending: int
    total_stations: int = 0
    active_vanguards: int = 0
    basins_monitored: int = 70
    last_updated: datetime

class DashboardSummary(BaseModel):
    active_alerts: int
    critical_alerts: int
    active_basins: int
    gauges_online: int
    reports_today: int
    reports_week: int
    displaced_persons_estimate: int
    last_updated: datetime

class BasinStatusOut(BaseModel):
    basin_id: str
    basin_name: str
    risk_level: RiskLevel
    flood_probability: float
    latest_discharge: Optional[float] = None
    latest_level: Optional[float] = None
    active_alert: Optional[str] = None


# ── MISC ───────────────────────────────────────────────────────────────────────
class MessageResponse(BaseModel):
    message: str
    success: bool = True

class NearbyFloods(BaseModel):
    latitude: float = Field(..., ge=4.0, le=14.0)
    longitude: float = Field(..., ge=2.5, le=15.0)
    radius_km: float = Field(default=50, ge=1, le=500)

# ── GAUGE UPDATE (used by PATCH /gauges/{station_id}) ─────────────────────────
class GaugeUpdate(BaseModel):
    station_name: Optional[str] = None
    river_name: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    state: Optional[str] = None
    lga: Optional[str] = None
    warning_level: Optional[float] = None
    danger_level: Optional[float] = None
    is_active: Optional[bool] = None
    telemetry: Optional[bool] = None

# ── MAP LAYERS ─────────────────────────────────────────────────────────────────

class MapLayerOut(BaseModel):
    id: str
    name: str
    group_key: str
    layer_key: str
    description: str = ""
    layer_type: str = "toggle"
    source_url: str = ""
    icon: str = "🗺️"
    display_order: int = 0
    is_active: bool = True
    default_visible: bool = False
    meta: dict = {}

    class Config:
        from_attributes = True

class MapLayerCreate(BaseModel):
    name: str
    group_key: str
    layer_key: str
    description: str = ""
    layer_type: str = "toggle"
    source_url: str = ""
    icon: str = "🗺️"
    display_order: int = 0
    is_active: bool = True
    default_visible: bool = False
    meta: dict = {}

class MapLayerUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    layer_type: Optional[str] = None
    source_url: Optional[str] = None
    icon: Optional[str] = None
    display_order: Optional[int] = None
    is_active: Optional[bool] = None
    default_visible: Optional[bool] = None
    meta: Optional[dict] = None
