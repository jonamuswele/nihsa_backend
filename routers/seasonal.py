"""
seasonal.py — NFFS Seasonal Flood Outlook Router
─────────────────────────────────────────────────
Serves the richer seasonal (S12/S13) outputs produced by the NFFS:

  GET /api/seasonal/summary           — peak season stats + flood extent km²
  GET /api/seasonal/periods           — list of available periods with metadata
  GET /api/seasonal/{period}/discharge— all-station discharge/risk table for a season
  GET /api/seasonal/{period}/atrisk/{layer}
                                       — per-feature at-risk list (CSV → JSON)
  GET /api/seasonal/atlas             — atlas URL + available maps
  GET /api/seasonal/animation         — animation HTML URL

Data is read directly from NFFS output files (no database required).
"""

import csv
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse, FileResponse

router = APIRouter()
logger = logging.getLogger("nihsa.seasonal")

# ── NFFS paths ─────────────────────────────────────────────────────────────────
NFFS_ROOT    = Path(os.getenv("NFFS_ROOT", r"C:\Users\DELL\Documents\nffs"))
DIR_SEASONAL = NFFS_ROOT / "data" / "processed" / "seasonal"
DIR_ATLAS    = NFFS_ROOT / "results" / "atlas"
DIR_EXPORTS  = DIR_ATLAS / "atrisk_exports"

SEASONS = {
    "P1_Apr_Jun": "April – June 2026",
    "P2_Jul_Sep": "July – September 2026",
    "P3_Oct_Nov": "October – November 2026",
    "P4_Annual":  "Full Year 2026",
}

VALID_LAYERS = [
    "communities", "health", "education", "roads", "railways",
    "markets", "idp_camps", "population", "farmland",
]

SEASON_COLORS = {
    "P1_Apr_Jun": "#f39c12",
    "P2_Jul_Sep": "#e74c3c",
    "P3_Oct_Nov": "#8e44ad",
    "P4_Annual":  "#2c3e50",
}

LAYER_ICONS = {
    "communities": "🏘️", "health": "🏥", "education": "🏫",
    "roads": "🛣️", "railways": "🚂", "markets": "🛒",
    "idp_camps": "⛺", "population": "👥", "farmland": "🌾",
}


def _read_discharge_csv(period_key: str) -> list:
    p = DIR_SEASONAL / f"{period_key}_discharge.csv"
    if not p.exists():
        return []
    try:
        rows = []
        with open(p, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                rows.append({
                    "station_id":   row.get("station_id", ""),
                    "name":         row.get("name", ""),
                    "lat":          float(row.get("lat", 0) or 0),
                    "lon":          float(row.get("lon", 0) or 0),
                    "state":        row.get("state", ""),
                    "area_km2":     float(row.get("area_km2", 0) or 0),
                    "q_peak":       float(row.get("q_peak", 0) or 0),
                    "q_mean":       float(row.get("q_mean", 0) or 0),
                    "q_p50":        float(row.get("q_p50", 0) or 0),
                    "q_p05":        float(row.get("q_p05", 0) or 0),
                    "q_p95":        float(row.get("q_p95", 0) or 0),
                    "flood_days":   int(float(row.get("flood_days", 0) or 0)),
                    "risk":         row.get("risk", "NONE"),
                    "priority":     int(float(row.get("priority", 0) or 0)),
                    "color":        row.get("color", "#27ae60"),
                    "lt_mean":      float(row.get("lt_mean", 0) or 0),
                    "source":       row.get("source", "climatological"),
                })
        return sorted(rows, key=lambda x: -x["priority"])
    except Exception as e:
        logger.warning(f"discharge CSV read error: {e}")
        return []


def _read_atrisk_csv(period_key: str, layer: str) -> list:
    p = DIR_EXPORTS / f"{period_key}_{layer}_atrisk.csv"
    if not p.exists():
        return []
    try:
        rows = []
        with open(p, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                rows.append({
                    "name":       row.get("name", ""),
                    "lat":        float(row.get("lat", 0) or 0),
                    "lon":        float(row.get("lon", 0) or 0),
                    "depth_m":    float(row.get("depth_m", 0) or 0),
                    "depth_zone": row.get("depth_zone", "watch"),
                    "state":      row.get("state", ""),
                    "lga":        row.get("lga", ""),
                    "population": int(float(row.get("population", 0) or 0)),
                })
        return rows
    except Exception as e:
        logger.warning(f"atrisk CSV read error: {e}")
        return []


def _load_summary_json() -> dict:
    p = DIR_SEASONAL / "seasonal_summary.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.get("/summary")
def seasonal_summary():
    """
    National seasonal flood outlook summary — all 4 periods.
    Shows station counts, risk distribution, peak discharge, and flood extent.
    """
    summary = _load_summary_json()

    result = {
        "year":          2026,
        "generated":     datetime.utcnow().isoformat(),
        "data_source":   "nffs_seasonal" if summary else "unavailable",
        "periods":       {},
    }

    for pk, plabel in SEASONS.items():
        ps = summary.get(pk, {})
        rc = ps.get("risk_counts", {})
        n_active = sum(v for k, v in rc.items() if k != "NONE")
        result["periods"][pk] = {
            "label":         plabel,
            "color":         SEASON_COLORS[pk],
            "start":         ps.get("start", ""),
            "end":           ps.get("end", ""),
            "total_stations":ps.get("stations", 0),
            "hbv_stations":  ps.get("hbv_count", 0),
            "active_alerts": n_active,
            "risk_counts":   rc,
            "peak_q":        ps.get("peak_q", 0),
            "peak_station":  ps.get("peak_station", ""),
            "atlas_url":     f"/api/seasonal/atlas/{pk}",
        }

    return result


@router.get("/periods")
def list_periods():
    """List all available seasonal periods with metadata."""
    summary = _load_summary_json()
    out = []
    for pk, plabel in SEASONS.items():
        csv_path = DIR_SEASONAL / f"{pk}_discharge.csv"
        exports  = list(DIR_EXPORTS.glob(f"{pk}_*_atrisk.csv")) if DIR_EXPORTS.exists() else []
        ps = summary.get(pk, {})
        out.append({
            "period_key":    pk,
            "label":         plabel,
            "color":         SEASON_COLORS[pk],
            "start":         ps.get("start", ""),
            "end":           ps.get("end", ""),
            "has_discharge": csv_path.exists(),
            "has_exports":   len(exports) > 0,
            "export_layers": len(exports),
            "peak_q":        ps.get("peak_q", 0),
        })
    return {"periods": out}


@router.get("/{period}/discharge")
def get_period_discharge(
    period: str,
    state: Optional[str] = None,
    min_risk: Optional[int] = Query(default=None, ge=0, le=4),
    limit: int = Query(default=100, le=500),
    offset: int = Query(default=0, ge=0),
):
    """
    All-station discharge and risk data for a seasonal period.
    Optionally filter by state or minimum risk priority.
    """
    if period not in SEASONS:
        raise HTTPException(status_code=404, detail=f"Period '{period}' not found. "
                            f"Valid: {list(SEASONS.keys())}")
    rows = _read_discharge_csv(period)
    if not rows:
        raise HTTPException(status_code=503,
                            detail=f"Discharge data for {period} not yet available. Run NFFS S12 first.")

    if state:
        rows = [r for r in rows if state.lower() in r["state"].lower()]
    if min_risk is not None:
        rows = [r for r in rows if r["priority"] >= min_risk]

    total = len(rows)
    page  = rows[offset: offset + limit]

    return {
        "period":   period,
        "label":    SEASONS[period],
        "total":    total,
        "offset":   offset,
        "limit":    limit,
        "stations": page,
    }


@router.get("/{period}/atrisk/{layer}")
def get_period_atrisk(
    period: str,
    layer:  str,
    state:  Optional[str] = None,
    depth_zone: Optional[str] = None,
    limit:  int = Query(default=200, le=1000),
    offset: int = Query(default=0, ge=0),
):
    """
    Per-feature at-risk list for a period × layer combination.
    Returns individual feature records with depth, state, LGA, coordinates.

    layer: communities | health | education | roads | railways |
           markets | idp_camps | population | farmland
    """
    if period not in SEASONS:
        raise HTTPException(status_code=404, detail=f"Period '{period}' not found")
    if layer not in VALID_LAYERS:
        raise HTTPException(status_code=404,
                            detail=f"Layer '{layer}' not valid. Choose from: {VALID_LAYERS}")

    rows = _read_atrisk_csv(period, layer)
    if not rows:
        raise HTTPException(status_code=404,
                            detail=f"No at-risk data for {period}/{layer}. "
                                   "Run NFFS S13b first.")

    # Filters
    if state:
        rows = [r for r in rows if state.lower() in r["state"].lower()]
    if depth_zone:
        rows = [r for r in rows if r["depth_zone"] == depth_zone]

    total = len(rows)
    page  = rows[offset: offset + limit]

    # Summary stats
    zones = {}
    for r in rows:
        z = r["depth_zone"]
        zones[z] = zones.get(z, 0) + 1

    return {
        "period":     period,
        "label":      SEASONS[period],
        "layer":      layer,
        "layer_icon": LAYER_ICONS.get(layer, "📌"),
        "total":      total,
        "offset":     offset,
        "limit":      limit,
        "depth_zone_counts": zones,
        "features":   page,
    }


@router.get("/{period}/atrisk-summary")
def get_period_atrisk_summary(period: str):
    """
    Quick count of at-risk features per layer for a period — for dashboard cards.
    """
    if period not in SEASONS:
        raise HTTPException(status_code=404, detail=f"Period '{period}' not found")

    # Load master summary CSV
    summary_path = DIR_EXPORTS / "atrisk_summary.csv"
    if not summary_path.exists():
        raise HTTPException(status_code=503,
                            detail="At-risk summary not available. Run NFFS S13b first.")
    try:
        rows = {}
        with open(summary_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("period", "").strip() == period:
                    layer = row["layer"]
                    rows[layer] = {
                        "layer":     layer,
                        "icon":      LAYER_ICONS.get(layer, "📌"),
                        "n_atrisk":  int(float(row.get("n_atrisk", 0))),
                        "n_severe":  int(float(row.get("n_severe", 0))),
                        "max_depth": float(row.get("max_depth", 0)),
                        "csv_url":   f"/api/seasonal/{period}/atrisk/{layer}",
                        "download_csv": f"/nffs/exports/{period}_{layer}_atrisk.csv",
                        "download_shp": f"/nffs/exports/{period}_{layer}_atrisk.shp",
                    }
        return {
            "period": period,
            "label":  SEASONS.get(period, period),
            "layers": rows,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/atlas")
def atlas_index():
    """Atlas metadata + map URL list."""
    idx = DIR_ATLAS / "index.html"
    maps = []
    for pk in SEASONS:
        for layer in list(VALID_LAYERS) + ["flood_extent"]:
            mf = DIR_ATLAS / f"{pk}_{layer}_map.html"
            if mf.exists():
                maps.append({
                    "period":    pk,
                    "layer":     layer,
                    "icon":      LAYER_ICONS.get(layer, "💧"),
                    "label":     SEASONS[pk],
                    "map_url":   f"/nffs/atlas/{pk}_{layer}_map.html",
                    "table_url": f"/nffs/atlas/{pk}_{layer}_table.csv",
                })
    return {
        "atlas_index_url": "/nffs/atlas/index.html",
        "animation_url":   "/nffs/atlas/flood_animation.html",
        "downloads_url":   "/nffs/atlas/downloads.html",
        "total_maps":      len(maps),
        "maps":            maps,
    }


@router.get("/atlas/{period}")
def atlas_period(period: str):
    """Maps available for a specific period."""
    if period not in SEASONS:
        raise HTTPException(status_code=404, detail=f"Period '{period}' not found")
    maps = []
    for layer in list(VALID_LAYERS) + ["flood_extent"]:
        mf = DIR_ATLAS / f"{period}_{layer}_map.html"
        if mf.exists():
            maps.append({
                "layer":     layer,
                "icon":      LAYER_ICONS.get(layer, "💧"),
                "map_url":   f"/nffs/atlas/{period}_{layer}_map.html",
                "table_url": f"/nffs/atlas/{period}_{layer}_table.csv",
                "atrisk_url":f"/api/seasonal/{period}/atrisk/{layer}" if layer != "flood_extent" else None,
            })
    return {
        "period":   period,
        "label":    SEASONS[period],
        "maps":     maps,
    }
