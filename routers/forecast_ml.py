"""
forecast_ml.py — NFFS Integration Router
─────────────────────────────────────────
Reads outputs from the National Flood Forecasting System at:
    D:\\AI Flood forecast 2026\\data\\processed\\

Falls back to simulation data when real files are not yet present.
The app NEVER calls NFFS directly — it only reads the output files.

NFFS alert levels:  NONE | WATCH | WARNING | SEVERE | EXTREME
App risk levels:    NORMAL | WATCH | MEDIUM | HIGH | CRITICAL

Mapping:
    NONE    → NORMAL
    WATCH   → WATCH
    WARNING → MEDIUM
    SEVERE  → HIGH
    EXTREME → CRITICAL
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict

from fastapi import APIRouter, HTTPException

logger = logging.getLogger("nihsa.forecast_ml")
router = APIRouter()

# ── NFFS output directory ──────────────────────────────────────────────────────
NFFS_ROOT       = Path(os.getenv("NFFS_ROOT", r"C:\Users\DELL\Documents\nffs"))
DIR_ALERTS      = NFFS_ROOT / "data" / "processed" / "alerts"
DIR_PREDICTIONS = NFFS_ROOT / "data" / "processed" / "predictions"
DIR_HBV         = NFFS_ROOT / "data" / "processed" / "hbv"
DIR_IMPACT      = NFFS_ROOT / "data" / "processed" / "impact"
DIR_CONFIG      = NFFS_ROOT / "config"
DIR_BULLETINS   = NFFS_ROOT / "results" / "bulletins"

# ── Nigerian state lookup by nearest capital centroid ─────────────────────────
# (lat, lon, state_name) — used when NFFS data has no state field
_NG_STATES = [
    (12.45,  4.20, "Kebbi"),
    (13.07,  5.25, "Sokoto"),
    (12.17,  6.67, "Zamfara"),
    (12.99,  7.60, "Katsina"),
    (11.99,  8.52, "Kano"),
    (11.75,  9.34, "Jigawa"),
    (11.75, 11.96, "Yobe"),
    (11.83, 13.16, "Borno"),
    (10.29, 11.17, "Gombe"),
    ( 9.21, 12.48, "Adamawa"),
    ( 8.90, 11.37, "Taraba"),
    (10.31,  9.84, "Bauchi"),
    ( 9.92,  8.89, "Plateau"),
    (10.52,  7.44, "Kaduna"),
    ( 9.61,  6.56, "Niger"),
    ( 9.05,  7.39, "FCT Abuja"),
    ( 8.49,  8.52, "Nasarawa"),
    ( 7.73,  8.52, "Benue"),
    ( 7.80,  6.75, "Kogi"),
    ( 8.50,  4.55, "Kwara"),
    ( 7.16,  3.35, "Ogun"),
    ( 6.45,  3.40, "Lagos"),
    ( 7.38,  3.93, "Oyo"),
    ( 7.76,  4.56, "Osun"),
    ( 7.63,  5.22, "Ekiti"),
    ( 7.25,  5.19, "Ondo"),
    ( 6.34,  5.63, "Edo"),
    ( 6.19,  6.74, "Delta"),
    ( 6.21,  7.07, "Anambra"),
    ( 6.44,  7.49, "Enugu"),
    ( 6.33,  8.12, "Ebonyi"),
    ( 5.48,  7.03, "Imo"),
    ( 5.54,  7.49, "Abia"),
    ( 5.87,  8.60, "Cross River"),
    ( 5.03,  7.93, "Akwa Ibom"),
    ( 4.82,  7.03, "Rivers"),
    ( 4.92,  6.27, "Bayelsa"),
]

def _lookup_state(lat: float, lon: float) -> str:
    """Return Nigerian state name by nearest capital centroid."""
    if not lat and not lon:
        return ""
    best, best_d = "", float("inf")
    for slat, slon, name in _NG_STATES:
        d = (lat - slat) ** 2 + (lon - slon) ** 2
        if d < best_d:
            best_d = d
            best = name
    return best


# ── Alert level mapping NFFS → App ────────────────────────────────────────────
NFFS_TO_APP = {
    "NONE":    "NORMAL",
    "WATCH":   "WATCH",
    "WARNING": "MEDIUM",
    "SEVERE":  "HIGH",
    "EXTREME": "CRITICAL",
}
NFFS_PRIORITY = ["NONE","WATCH","WARNING","SEVERE","EXTREME"]

# ── Plain-English public messages ─────────────────────────────────────────────
PUBLIC_MESSAGES = {
    "NONE": {
        "emoji": "✅",
        "headline": "No flood risk at this time",
        "body": "River levels are within normal range. Continue to monitor updates during rainy season.",
        "action": "No action needed. Stay informed.",
    },
    "WATCH": {
        "emoji": "👀",
        "headline": "Rivers are rising. Stay alert.",
        "body": "Water levels are above normal. Flooding is possible if rains continue.",
        "action": "Monitor NIHSA updates closely. Avoid unnecessary travel near rivers and streams.",
    },
    "WARNING": {
        "emoji": "⚠️",
        "headline": "Flooding likely in low-lying areas",
        "body": "River levels are rising significantly. Low-lying areas and farmland near the river are at risk.",
        "action": "Move valuables to higher ground. Avoid crossing rivers or flood-prone roads.",
    },
    "SEVERE": {
        "emoji": "🔴",
        "headline": "Significant flooding expected",
        "body": "River levels are well above warning thresholds. Flooding of riverbank communities is expected.",
        "action": "Riverbank communities should prepare to evacuate. Move people, livestock and valuables now.",
    },
    "EXTREME": {
        "emoji": "🚨",
        "headline": "Life-threatening flooding",
        "body": "Extreme flood event in progress. Water levels are at or near record highs.",
        "action": "EVACUATE NOW if you are near the river. Do not wait. Move to designated evacuation centres.",
    },
}

# ── Lagdo cascade downstream stations ─────────────────────────────────────────
LAGDO_DOWNSTREAM = {"makurdi","lokoja","ibi","umaisha","wuroboki"}

# ── Simulation baseline (same station IDs as NIHSA database) ──────────────────
SIM_BASINS = [
    {"station_id":"G001","station_name":"Lokoja",   "lat":7.800,"lon":6.749,"river":"Niger",   "state":"Kogi",     "sim_level":"SEVERE"},
    {"station_id":"G002","station_name":"Makurdi",  "lat":7.746,"lon":8.532,"river":"Benue",   "state":"Benue",    "sim_level":"WARNING"},
    {"station_id":"G003","station_name":"Onitsha",  "lat":6.163,"lon":6.775,"river":"Niger",   "state":"Anambra",  "sim_level":"WARNING"},
    {"station_id":"G004","station_name":"Kainji",   "lat":9.852,"lon":4.616,"river":"Niger",   "state":"Niger",    "sim_level":"WATCH"},
    {"station_id":"G005","station_name":"Jebba",    "lat":9.129,"lon":4.818,"river":"Niger",   "state":"Kwara",    "sim_level":"WATCH"},
    {"station_id":"G006","station_name":"Baro",     "lat":8.583,"lon":6.383,"river":"Niger",   "state":"Niger",    "sim_level":"WATCH"},
    {"station_id":"G007","station_name":"Umaisha",  "lat":8.003,"lon":7.185,"river":"Benue",   "state":"Nasarawa", "sim_level":"NONE"},
    {"station_id":"G008","station_name":"Wuroboki", "lat":8.022,"lon":10.201,"river":"Taraba", "state":"Taraba",   "sim_level":"NONE"},
]


def _is_numeric(v) -> bool:
    try: float(str(v)); return True
    except: return False


def _latest_alerts_file() -> Optional[Path]:
    if not DIR_ALERTS.exists():
        return None
    files = list(DIR_ALERTS.glob("*_alerts.json"))
    if not files:
        return None
    # Sort by modification time (most recently written file wins)
    return max(files, key=lambda p: p.stat().st_mtime)


def _read_nffs_alerts() -> Optional[List[Dict]]:
    f = _latest_alerts_file()
    if not f:
        return None
    try:
        data = json.loads(f.read_text(encoding="utf-8"))
        if not isinstance(data, list) or len(data) == 0:
            return None
        return data
    except Exception as e:
        logger.warning(f"Could not read NFFS alerts: {e}")
        return None


def _read_ensemble(station_id: str) -> Optional[List[Dict]]:
    p = DIR_PREDICTIONS / f"{station_id}_ensemble.csv"
    if not p.exists():
        return None
    try:
        import csv
        rows = []
        with open(p, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                rows.append({
                    "horizon": int(float(row.get("horizon", len(rows)+1))),
                    "q05": round(float(row.get("q05", 0)), 1),
                    "q50": round(float(row.get("q50", 0)), 1),
                    "q95": round(float(row.get("q95", 0)), 1),
                })
        return rows[:7] if rows else None
    except Exception as e:
        logger.warning(f"Could not read ensemble for {station_id}: {e}")
        return None


def _build_days_from_ensemble(forecast_days: List[Dict]) -> tuple:
    """Convert ensemble q50 series into thermometer bars. Returns (days, peak_day)."""
    labels = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
    max_q  = max((d.get("q50", 0) for d in forecast_days), default=1) or 1
    days   = []
    for i, d in enumerate(forecast_days[:7]):
        intensity = (d.get("q50", 0) / max_q) * 10
        days.append({
            "label":     labels[i],
            "intensity": round(intensity, 1),
            "status":    "high" if intensity > 6.5 else "elevated" if intensity > 3.5 else "calm",
            "q50":       d.get("q50"),
            "q05":       d.get("q05"),
            "q95":       d.get("q95"),
        })
    peak_day = max(range(len(days)), key=lambda i: days[i]["intensity"]) + 1
    return days, peak_day


def _build_sim_days(station_id: str, level: str) -> tuple:
    """Deterministic simulation thermometer. Returns (days, peak_day)."""
    seed  = sum(ord(c) for c in station_id)
    peak  = 2 + (seed % 4)
    base  = {"NONE":0.12,"WATCH":0.35,"WARNING":0.58,"SEVERE":0.80,"EXTREME":0.96}.get(level, 0.12)
    labels = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
    days  = []
    for i in range(7):
        if i < peak:
            t = base * (i + 1) / (peak + 1)
        elif i == peak:
            t = base
        else:
            t = base * max(0.08, 1 - (i - peak) * 0.35)
        intensity = round(t * 10, 1)
        days.append({
            "label":     labels[i],
            "intensity": intensity,
            "status":    "high" if intensity > 6.5 else "elevated" if intensity > 3.5 else "calm",
        })
    return days, peak + 1


def _build_impact(station_id: str, level: str, raw_impact: Optional[Dict] = None) -> Dict:
    """Return impact dict. Uses real NFFS impact data when available."""
    if level == "NONE":
        return {}
    if raw_impact:
        return raw_impact
    # Simulation
    seed = sum(ord(c) for c in station_id)
    m    = {"WATCH":0.25,"WARNING":0.55,"SEVERE":1.0,"EXTREME":1.6}.get(level, 0)
    return {
        "population_at_risk":  round((60000  + seed % 140000) * m),
        "schools_affected":    round((3      + seed % 18)     * m),
        "health_facilities":   round((1      + seed % 7)      * m),
        "farmland_ha":         round((900    + seed % 4100)   * m),
        "roads_km":            round((12     + seed % 58)     * m),
    }


def _sanitize_state(val) -> str:
    """Clean up NaN/None strings that come from Python pandas serialization."""
    s = str(val or "").strip()
    return "" if s.lower() in ("nan", "none", "null", "") else s


def _build_response(alert: Dict, forecast_days: Optional[List[Dict]] = None, is_live: bool = False) -> Dict:
    """Convert raw NFFS alert → full app response with public-English fields."""
    nffs_level = alert.get("level", "NONE")
    app_level  = NFFS_TO_APP.get(nffs_level, "NORMAL")
    msg        = PUBLIC_MESSAGES.get(nffs_level, PUBLIC_MESSAGES["NONE"])
    station_id = str(alert.get("station_id", ""))

    # 7-day thermometer
    if forecast_days:
        days, peak_day = _build_days_from_ensemble(forecast_days)
    else:
        days, peak_day = _build_sim_days(station_id, nffs_level)

    # Lagdo cascade flag
    is_lagdo = alert.get("lagdo_cascade", False) or (
        alert.get("station_name","").lower() in LAGDO_DOWNSTREAM
        and nffs_level in ("SEVERE","EXTREME")
    )

    # State: use data if present, otherwise geo-lookup from lat/lon
    raw_lat = float(alert.get("lat", 0) or 0)
    raw_lon = float(alert.get("lon", 0) or 0)
    state = _sanitize_state(alert.get("state", "")) or _lookup_state(raw_lat, raw_lon)

    # Real NFFS impact data (only use if at least one field is non-zero)
    nffs_impact = {
        "population_at_risk":  int(float(alert.get("population_at_risk",  0) or 0)),
        "schools_affected":    int(float(alert.get("schools_flooded",      0) or 0)),
        "health_facilities":   int(float(alert.get("health_facilities_flooded", 0) or 0)),
        "farmland_ha":         round(float(alert.get("farmland_flooded_ha", 0) or 0)),
        "communities_flooded": int(float(alert.get("communities_flooded",  0) or 0)),
    }
    raw_impact = nffs_impact if any(v > 0 for v in nffs_impact.values()) else None

    return {
        # Identity
        "station_id":    station_id,
        "station_name":  str(alert.get("station_name", station_id) or "").strip() or station_id,
        "river":         _sanitize_state(alert.get("river", "")),
        "state":         state,
        "lat":           raw_lat,
        "lon":           raw_lon,
        # Alert levels
        "nffs_level":    nffs_level,
        "app_level":     app_level,
        "priority":      NFFS_PRIORITY.index(nffs_level) if nffs_level in NFFS_PRIORITY else 0,
        # Technical (admin/engineers only)
        "q50": float(alert.get("q50", 0)),
        "q05": float(alert.get("q05", 0)),
        "q95": float(alert.get("q95", 0)),
        "threshold_watch":   alert.get("threshold_watch"),
        "threshold_warning": alert.get("threshold_warning"),
        "threshold_severe":  alert.get("threshold_severe"),
        "threshold_extreme": alert.get("threshold_extreme"),
        "threshold_source":  alert.get("threshold_source", "simulation"),
        # Public-facing
        "emoji":    msg["emoji"],
        "headline": msg["headline"],
        "body":     msg["body"],
        "action":   msg["action"],
        "peak_day": peak_day,
        "days":     days,
        "impact":   _build_impact(station_id, nffs_level, raw_impact),
        "lagdo_cascade": is_lagdo,
        # Metadata
        "data_source":   "nffs_live" if is_live else "simulation",
        "forecast_date": str(alert.get("date", datetime.utcnow().date())),
    }


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/status")
def forecast_status():
    """Is NFFS producing real output, or are we in simulation mode?"""
    f = _latest_alerts_file()
    if f:
        mtime     = datetime.fromtimestamp(f.stat().st_mtime)
        # Use local time for both to avoid UTC/local mismatch
        age_hours = (datetime.now() - mtime).total_seconds() / 3600
        return {
            "mode":         "live",
            "file":         f.name,
            "path":         str(f),
            "last_updated": mtime.isoformat(),
            "age_hours":    round(age_hours, 1),
            "stale":        age_hours > 26,
        }
    return {
        "mode":     "simulation",
        "file":     None,
        "path":     str(DIR_ALERTS),
        "last_updated": None,
        "note":     f"No NFFS output at {DIR_ALERTS}. Run: python src/run_all.py --mode forecast-weekly",
    }


@router.get("/alerts")
def get_all_alerts():
    """All station alerts — plain English, public-facing."""
    raw = _read_nffs_alerts()
    is_live = raw is not None

    if not is_live:
        raw = []
        for b in SIM_BASINS:
            lvl = b["sim_level"]
            seed = sum(ord(c) for c in b["station_id"])
            base = {"NONE":120,"WATCH":380,"WARNING":1200,"SEVERE":4800,"EXTREME":9500}.get(lvl, 120)
            bseed = 200 + (seed % 400)
            raw.append({
                **{k:v for k,v in b.items() if k != "sim_level"},
                "level": lvl,
                "q50": round(base * (1 + (seed % 30)/100), 1),
                "q05": round(base * 0.65, 1),
                "q95": round(base * 1.55, 1),
                "threshold_watch":   round(bseed * 1.5, 1),
                "threshold_warning": round(bseed * 2.5, 1),
                "threshold_severe":  round(bseed * 4.0, 1),
                "threshold_extreme": round(bseed * 6.5, 1),
                "threshold_source":  "simulation",
            })

    # Deduplicate by station_id — keep highest-priority entry for each
    seen: Dict[str, Dict] = {}
    for a in raw:
        sid = str(a.get("station_id", ""))
        existing = seen.get(sid)
        if existing is None:
            seen[sid] = a
        else:
            # Keep the one with higher alert level (priority)
            level_order = {"NONE":0,"WATCH":1,"WARNING":2,"SEVERE":3,"EXTREME":4}
            if level_order.get(str(a.get("level","NONE")),0) > level_order.get(str(existing.get("level","NONE")),0):
                seen[sid] = a

    results = []
    for a in seen.values():
        fc = _read_ensemble(str(a.get("station_id","")))
        results.append(_build_response(a, fc, is_live=is_live))

    results.sort(key=lambda x: x["priority"], reverse=True)

    return {
        "data_source":   "nffs_live" if is_live else "simulation",
        "forecast_date": datetime.utcnow().strftime("%Y-%m-%d"),
        "station_count": len(results),
        "alerts":        results,
    }


@router.get("/alerts/{station_id}")
def get_station_alert(station_id: str):
    """Single station — full 7-day detail."""
    raw = _read_nffs_alerts()
    alert = None

    if raw:
        alert = next((a for a in raw if str(a.get("station_id")) == station_id), None)

    if not alert:
        b = next((b for b in SIM_BASINS if b["station_id"] == station_id), None)
        if not b:
            raise HTTPException(status_code=404, detail=f"Station {station_id} not found")
        lvl  = b["sim_level"]
        seed = sum(ord(c) for c in station_id)
        base = {"NONE":120,"WATCH":380,"WARNING":1200,"SEVERE":4800,"EXTREME":9500}.get(lvl, 120)
        bseed = 200 + (seed % 400)
        alert = {
            **{k:v for k,v in b.items() if k != "sim_level"},
            "level": lvl,
            "q50": round(base * (1 + (seed%30)/100), 1),
            "q05": round(base * 0.65, 1),
            "q95": round(base * 1.55, 1),
            "threshold_watch":   round(bseed*1.5,1),
            "threshold_warning": round(bseed*2.5,1),
            "threshold_severe":  round(bseed*4.0,1),
            "threshold_extreme": round(bseed*6.5,1),
            "threshold_source":  "simulation",
        }

    fc = _read_ensemble(station_id)
    is_live = raw is not None
    return _build_response(alert, fc, is_live=is_live)


@router.get("/summary")
def get_summary():
    """Counts, lagdo status, top alerts — for admin ML panel."""
    data   = get_all_alerts()
    alerts = data["alerts"]

    counts = {lvl: 0 for lvl in NFFS_PRIORITY}
    for a in alerts:
        counts[a["nffs_level"]] = counts.get(a["nffs_level"], 0) + 1

    return {
        "data_source":   data["data_source"],
        "forecast_date": data["forecast_date"],
        "total_stations": len(alerts),
        "level_counts":  counts,
        "active_alerts": sum(v for k,v in counts.items() if k != "NONE"),
        "lagdo_active":  any(a["lagdo_cascade"] for a in alerts),
        "top_alerts":    [a for a in alerts if a["priority"] > 0][:5],
        "impact":        _read_impact_totals(),
    }


def _read_impact_totals() -> Dict:
    p = DIR_IMPACT / "impact_summary.csv"
    if not p.exists():
        return {}
    try:
        import csv
        t = {"population_at_risk":0,"schools_affected":0,"farmland_ha":0}
        with open(p, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                t["population_at_risk"] += int(float(row.get("communities_flooded",0))) * 5000
                t["schools_affected"]   += int(float(row.get("schools_flooded",0)))
                t["farmland_ha"]        += int(float(row.get("farmland_flooded_ha",0)))
        return t
    except:
        return {}


@router.get("/bulletin")
def get_bulletin():
    """Latest WMO plain-text bulletin."""
    if not DIR_BULLETINS.exists():
        return {"bulletin": None}
    files = sorted(DIR_BULLETINS.glob("*_bulletin.txt"), reverse=True)
    if not files:
        return {"bulletin": None}
    try:
        return {"bulletin": files[0].read_text(encoding="utf-8"),
                "date": files[0].stem.replace("_bulletin",""),
                "file": files[0].name}
    except:
        return {"bulletin": None}


@router.get("/thresholds")
def get_thresholds():
    """Flood frequency thresholds — for admin engineers."""
    p = DIR_CONFIG / "alert_thresholds.csv"
    if not p.exists():
        return {"data_source": "not_available", "thresholds": []}
    try:
        import csv
        rows = []
        with open(p, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                rows.append({k: (float(v) if _is_numeric(v) else v) for k,v in row.items()})
        return {"data_source": "nffs_config", "thresholds": rows}
    except Exception as e:
        return {"data_source": "error", "error": str(e), "thresholds": []}
