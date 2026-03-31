"""Map Layer definitions — admin-managed, publicly readable.
All data uploads are CSV only. Each layer has a downloadable template
showing exactly what columns are required.
"""

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from fastapi.responses import Response
from sqlalchemy.orm import Session
from typing import List
from pathlib import Path
from datetime import datetime, timezone
import json, csv, io, os

import models, schemas
from database import get_db
from auth_utils import require_role

# ── Paths ─────────────────────────────────────────────────────────────────────
_NFFS_ROOT   = Path(os.getenv("NFFS_ROOT", r"C:\Users\DELL\Documents\nffs"))
_GEOJSON_DIR = _NFFS_ROOT / "results" / "atlas" / "geojson"
_ATLAS_DIR   = _NFFS_ROOT / "results" / "atlas"

# Maps layer_key → canonical GeoJSON filename on disk
_KEY_TO_FILE = {
    # Annual forecast
    "fc_flood_extent": "flood_extent.geojson",
    "fc_population":   "population.geojson",
    "fc_communities":  "communities.geojson",
    "fc_health":       "health.geojson",
    "fc_schools":      "schools.geojson",
    "fc_farmland":     "farmland.geojson",
    "fc_roads":        "roads.geojson",
    # Surface water (admin-uploadable satellite / field data)
    "sw_satellite":       "sw_satellite.geojson",
    "sw_station_updates": "sw_station_updates.geojson",
    # Weekly forecast (same structure, weekly_ prefix)
    "fw_flood_extent": "weekly_flood_extent.geojson",
    "fw_population":   "weekly_population.geojson",
    "fw_communities":  "weekly_communities.geojson",
    "fw_health":       "weekly_health.geojson",
    "fw_schools":      "weekly_schools.geojson",
    "fw_farmland":     "weekly_farmland.geojson",
    "fw_roads":        "weekly_roads.geojson",
}

# ── CSV Templates ─────────────────────────────────────────────────────────────
# Each entry: columns, description, 3 sample data rows, validation notes
_CSV_TEMPLATES = {
    "fc_flood_extent": {
        "columns": "risk_zone,state,lga,lat,lon",
        "description": "Flood risk zone locations. One row per location within a flood zone.",
        "sample": [
            "high,Kogi,Lokoja,7.8069,6.7420",
            "severe,Anambra,Onitsha,6.1444,6.7836",
            "watch,Niger,Baro,8.6167,6.4167",
        ],
        "notes": "risk_zone must be: watch, medium, high, severe, or extreme",
    },
    "fc_population": {
        "columns": "name,state,lga,lat,lon,depth",
        "description": "Population centres at annual flood risk.",
        "sample": [
            "Onu Nwokwo,Ebonyi,Ohaukwu,6.5047,7.9650,1.2",
            "Alioma Umuakpu,Ebonyi,Ohaukwu,6.4941,7.9887,0.8",
            "Tsamiya Layin Makera,Yobe,Bade,12.9021,11.0481,2.0",
        ],
        "notes": "depth = expected flood depth in metres (leave blank if unknown)",
    },
    "fc_communities": {
        "columns": "name,state,lga,lat,lon,depth",
        "description": "Communities/settlements at annual flood risk.",
        "sample": [
            "Tsamiya Layin Makera,Yobe,Bade,12.9021,11.0481,1.5",
            "Mallam Madori,Jigawa,Hadejia,12.4667,10.0333,0.9",
            "Geidam,Yobe,Geidam,12.8944,11.9278,2.1",
        ],
        "notes": "depth = expected flood depth in metres (leave blank if unknown)",
    },
    "fc_health": {
        "columns": "name,state,lga,lat,lon,facility_type,depth",
        "description": "Health facilities (clinics, hospitals) at annual flood risk.",
        "sample": [
            "Meleri Primary Health Care,Borno,Mobbar,13.2167,13.1500,PHC,0.7",
            "Bade General Hospital,Yobe,Bade,12.9021,11.0481,Hospital,1.2",
            "Lagos Island General Hospital,Lagos,Lagos Island,6.4541,3.3947,Hospital,0.5",
        ],
        "notes": "facility_type: PHC, Clinic, Hospital, Health Centre (optional)",
    },
    "fc_schools": {
        "columns": "name,state,lga,lat,lon,school_type,depth",
        "description": "Schools and educational institutions at annual flood risk.",
        "sample": [
            "Gill Educational Center,Niger,Mariga,10.3833,5.4167,Primary,1.0",
            "Kano Government Secondary School,Kano,Kano Municipal,12.0022,8.5920,Secondary,0.6",
            "University of Lagos,Lagos,Lagos Island,6.5158,3.3955,University,0.3",
        ],
        "notes": "school_type: Primary, Secondary, University, Polytechnic (optional)",
    },
    "fc_farmland": {
        "columns": "name,state,lga,lat,lon,crop_type,area_ha,depth",
        "description": "Farmland parcels at annual flood risk.",
        "sample": [
            "Ayedade Farm,Osun,Ayedade,7.5833,4.3333,Rice,12.5,0.8",
            "Ohaukwu Fields,Ebonyi,Ohaukwu,6.5000,7.9800,Cassava,8.2,1.1",
            "Bade Irrigation Scheme,Yobe,Bade,12.9021,11.0481,Millet,45.0,2.0",
        ],
        "notes": "area_ha = farm area in hectares; crop_type and area_ha are optional",
    },
    "fc_roads": {
        "columns": "name,state,lga,lat,lon,road_class,depth",
        "description": "Roads and bridges at annual flood risk.",
        "sample": [
            "Sapele - Warri Road,Delta,,5.8904,5.6781,Federal,0.9",
            "Lagos - Ibadan Expressway,Lagos,,6.6018,3.3515,Federal,0.4",
            "Lokoja Bridge,Kogi,Lokoja,7.8069,6.7420,State,1.5",
        ],
        "notes": "road_class: Federal, State, LGA (optional); lga can be blank for roads",
    },
    # Weekly versions use identical templates
    "fw_flood_extent": None,  # resolved dynamically to fc_flood_extent template
    "fw_population":   None,
    "fw_communities":  None,
    "fw_health":       None,
    "fw_schools":      None,
    "fw_farmland":     None,
    "fw_roads":        None,
}
    "sw_station_updates": {
        "columns": "station_name,river,state,lga,lat,lon,level_m,flow_m3s,status,notes",
        "description": "Station condition updates — new readings, field observations, or situation reports.",
        "sample": [
            "Lokoja Station,Niger,Kogi,Lokoja,7.8069,6.7420,4.2,1850,WARNING,Rising fast after upstream release",
            "Onitsha Station,Niger,Anambra,Onitsha,6.1444,6.7836,5.1,2340,SEVERE,Banks overtopped on eastern side",
            "Baro Station,Niger,Niger,Baro,8.6167,6.4167,2.8,640,WATCH,Steady rise since yesterday",
        ],
        "notes": (
            "status must be one of: NORMAL, WATCH, WARNING, SEVERE, EXTREME. "
            "level_m = current river level in metres. flow_m3s = discharge in cubic metres per second. "
            "notes = free text field observation (max 200 chars). All columns except lat/lon are optional."
        ),
    },
# Weekly layers share templates with their annual counterparts
for _wk in ["fw_flood_extent","fw_population","fw_communities","fw_health","fw_schools","fw_farmland","fw_roads"]:
    _CSV_TEMPLATES[_wk] = _CSV_TEMPLATES[_wk.replace("fw_","fc_")]

# ── Generic fallback template (used for groundwater, water quality, coastal & any custom geojson layer) ──
_GENERIC_TEMPLATE = {
    "columns": "name,state,lga,lat,lon",
    "description": "Generic spatial data layer. Any additional columns are accepted and stored as properties.",
    "sample": [
        "Location A,Kogi,Lokoja,7.8069,6.7420",
        "Location B,Anambra,Onitsha,6.1444,6.7836",
        "Location C,Niger,Baro,8.6167,6.4167",
    ],
    "notes": "Required columns: lat and lon (decimal degrees, Nigeria bounds). All other columns are stored as properties.",
}
# Pre-assign generic templates for non-forecast geojson layers that have no specific template
for _gk in [
    "gw_levels", "gw_aquifer", "gw_recharge",
    "wq_index", "wq_turbidity", "wq_contamination",
    "cm_coastal_risk", "cm_storm_surge", "cm_erosion", "cm_mangrove",
    "sw_satellite",
]:
    if _gk not in _CSV_TEMPLATES:
        _CSV_TEMPLATES[_gk] = _GENERIC_TEMPLATE

# ── CSV → GeoJSON conversion ───────────────────────────────────────────────────
_RISK_ZONES = ["watch", "medium", "high", "severe", "extreme"]


def _csv_to_fc(content_bytes: bytes, layer_key: str) -> dict:
    """
    Convert uploaded CSV to GeoJSON FeatureCollection.
    All layers map to Point features using lat/lon columns.
    flood_extent layers also set a risk_zone property used for polygon-style colouring.
    """
    text = content_bytes.decode("utf-8", errors="ignore").lstrip("\ufeff")  # strip BOM
    reader = csv.DictReader(io.StringIO(text))

    # Normalise header names to lower-case for matching
    raw_rows = list(reader)
    if not raw_rows:
        return {"type": "FeatureCollection", "features": []}

    features = []
    skipped = 0
    for row in raw_rows:
        # Case-insensitive lat/lon lookup
        norm = {k.strip().lower(): v.strip() for k, v in row.items()}
        lat_raw = norm.get("lat") or norm.get("latitude")
        lon_raw = norm.get("lon") or norm.get("longitude") or norm.get("lng")

        if not lat_raw or not lon_raw:
            skipped += 1
            continue
        try:
            lat, lon = float(lat_raw), float(lon_raw)
        except ValueError:
            skipped += 1
            continue

        # Skip implausible coordinates (Nigeria bounding box: ~4N-14N, 3E-15E)
        if not (2.0 <= lat <= 16.0 and 2.0 <= lon <= 16.0):
            skipped += 1
            continue

        # Build properties from all other columns
        exclude = {"lat","latitude","lon","longitude","lng"}
        props = {k: v for k, v in norm.items() if k not in exclude and v != ""}

        # Validate risk_zone for flood extent layers
        if "flood_extent" in layer_key:
            zone = props.get("risk_zone", "").lower()
            if zone not in _RISK_ZONES:
                props["risk_zone"] = "watch"  # default
            else:
                props["risk_zone"] = zone

        # Convert numeric-looking strings to numbers
        for k, v in props.items():
            try:
                props[k] = float(v) if "." in v else int(v)
            except (ValueError, TypeError):
                pass

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": props,
        })

    return {"type": "FeatureCollection", "features": features, "_skipped": skipped}


# ── Default layer catalogue ────────────────────────────────────────────────────
_DEFAULT_LAYERS = [
    # ── Surface Water ──────────────────────────────────────────────────────────
    {"group_key":"surface_water","layer_key":"stations","name":"River Gauge Stations",
     "description":"358 NIHSA real-time river level monitoring stations","icon":"📍",
     "layer_type":"toggle","display_order":1,"is_active":True,"default_visible":True,"source_url":"","meta":{}},
    {"group_key":"surface_water","layer_key":"alerts","name":"Active Flood Alerts",
     "description":"Published flood warnings from the NIHSA alert system","icon":"⚠️",
     "layer_type":"toggle","display_order":2,"is_active":True,"default_visible":True,"source_url":"","meta":{}},
    {"group_key":"surface_water","layer_key":"reports","name":"Citizen Flood Reports",
     "description":"Verified field reports submitted by citizens and vanguards","icon":"💧",
     "layer_type":"toggle","display_order":3,"is_active":True,"default_visible":False,"source_url":"","meta":{}},
    {"group_key":"surface_water","layer_key":"sw_satellite","name":"Satellite Flood Extent",
     "description":"Near-real-time satellite-derived flood inundation extent","icon":"🛰️",
     "layer_type":"geojson_fc","display_order":4,"is_active":False,"default_visible":False,"source_url":"","meta":{}},
    {"group_key":"surface_water","layer_key":"sw_station_updates","name":"Station Situation Updates",
     "description":"Latest field observations and condition updates per gauge station","icon":"📡",
     "layer_type":"geojson_fc","display_order":5,"is_active":True,"default_visible":False,"source_url":"","meta":{}},
    # ── Groundwater ────────────────────────────────────────────────────────────
    {"group_key":"groundwater","layer_key":"gw_levels","name":"Groundwater Levels",
     "description":"Monitoring borehole water table depth across Nigeria","icon":"🔵",
     "layer_type":"toggle","display_order":1,"is_active":False,"default_visible":False,"source_url":"","meta":{}},
    {"group_key":"groundwater","layer_key":"gw_aquifer","name":"Aquifer Zones",
     "description":"Major aquifer classification and recharge zones","icon":"🗺️",
     "layer_type":"geojson","display_order":2,"is_active":False,"default_visible":False,"source_url":"","meta":{}},
    {"group_key":"groundwater","layer_key":"gw_recharge","name":"Recharge Areas",
     "description":"Groundwater recharge risk during flood season","icon":"♻️",
     "layer_type":"geojson","display_order":3,"is_active":False,"default_visible":False,"source_url":"","meta":{}},
    # ── Water Quality ──────────────────────────────────────────────────────────
    {"group_key":"water_quality","layer_key":"wq_index","name":"Water Quality Index",
     "description":"Composite WQI at major monitoring stations","icon":"🧪",
     "layer_type":"toggle","display_order":1,"is_active":False,"default_visible":False,"source_url":"","meta":{}},
    {"group_key":"water_quality","layer_key":"wq_turbidity","name":"Turbidity / Sediment",
     "description":"Suspended sediment and turbidity levels in rivers","icon":"🌊",
     "layer_type":"toggle","display_order":2,"is_active":False,"default_visible":False,"source_url":"","meta":{}},
    {"group_key":"water_quality","layer_key":"wq_contamination","name":"Contamination Risk Zones",
     "description":"Post-flood contamination risk from industrial sources","icon":"⚗️",
     "layer_type":"geojson","display_order":3,"is_active":False,"default_visible":False,"source_url":"","meta":{}},
    # ── Coastal & Marine ───────────────────────────────────────────────────────
    {"group_key":"coastal_marine","layer_key":"cm_coastal_risk","name":"Coastal Flood Risk",
     "description":"Storm surge and tidal flood risk zones along the coast","icon":"🏖️",
     "layer_type":"geojson","display_order":1,"is_active":False,"default_visible":False,"source_url":"","meta":{}},
    {"group_key":"coastal_marine","layer_key":"cm_storm_surge","name":"Storm Surge Zones",
     "description":"Atlantic storm surge inundation extents by return period","icon":"🌀",
     "layer_type":"geojson","display_order":2,"is_active":False,"default_visible":False,"source_url":"","meta":{}},
    {"group_key":"coastal_marine","layer_key":"cm_erosion","name":"Coastal Erosion Risk",
     "description":"Shoreline erosion vulnerability and recession rates","icon":"⛰️",
     "layer_type":"geojson","display_order":3,"is_active":False,"default_visible":False,"source_url":"","meta":{}},
    {"group_key":"coastal_marine","layer_key":"cm_mangrove","name":"Mangrove Buffer Zones",
     "description":"Mangrove extent providing natural coastal protection","icon":"🌿",
     "layer_type":"geojson","display_order":4,"is_active":False,"default_visible":False,"source_url":"","meta":{}},
    # ── Annual Forecast (AFO 2026) ─────────────────────────────────────────────
    {"group_key":"forecast","layer_key":"fc_animation","name":"Flood Animation 2026",
     "description":"Monthly animated river flood extent Jan-Dec 2026","icon":"🎬",
     "layer_type":"atlas","source_url":"flood_animation.html","display_order":1,"is_active":True,"default_visible":False,"meta":{}},
    {"group_key":"forecast","layer_key":"fc_flood_extent","name":"Flood Extent & Depth",
     "description":"Annual inundation extent by risk zone","icon":"💧",
     "layer_type":"geojson_fc","display_order":2,"is_active":True,"default_visible":False,"source_url":"geojson/flood_extent.geojson","meta":{}},
    {"group_key":"forecast","layer_key":"fc_population","name":"Population at Risk",
     "description":"People living in flood-prone zones by LGA","icon":"👥",
     "layer_type":"geojson_fc","display_order":3,"is_active":True,"default_visible":False,"source_url":"geojson/population.geojson","meta":{}},
    {"group_key":"forecast","layer_key":"fc_communities","name":"Communities at Risk",
     "description":"Settlements exposed to annual flooding","icon":"🏘️",
     "layer_type":"geojson_fc","display_order":4,"is_active":True,"default_visible":False,"source_url":"geojson/communities.geojson","meta":{}},
    {"group_key":"forecast","layer_key":"fc_health","name":"Health Facilities at Risk",
     "description":"Clinics, health centres and hospitals in flood zones","icon":"🏥",
     "layer_type":"geojson_fc","display_order":5,"is_active":True,"default_visible":False,"source_url":"geojson/health.geojson","meta":{}},
    {"group_key":"forecast","layer_key":"fc_schools","name":"Schools at Risk",
     "description":"Schools and educational facilities in flood zones","icon":"🏫",
     "layer_type":"geojson_fc","display_order":6,"is_active":True,"default_visible":False,"source_url":"geojson/schools.geojson","meta":{}},
    {"group_key":"forecast","layer_key":"fc_farmland","name":"Farmland Exposure",
     "description":"Agricultural land at risk of seasonal flooding","icon":"🌾",
     "layer_type":"geojson_fc","display_order":7,"is_active":True,"default_visible":False,"source_url":"geojson/farmland.geojson","meta":{}},
    {"group_key":"forecast","layer_key":"fc_roads","name":"Road Network at Risk",
     "description":"Roads and bridges vulnerable to flood damage","icon":"🛣️",
     "layer_type":"geojson_fc","display_order":8,"is_active":True,"default_visible":False,"source_url":"geojson/roads.geojson","meta":{}},
    # ── Weekly Forecast ────────────────────────────────────────────────────────
    {"group_key":"forecast_weekly","layer_key":"fw_flood_extent","name":"Flood Extent & Depth",
     "description":"Weekly inundation extent by risk zone","icon":"💧",
     "layer_type":"geojson_fc","display_order":1,"is_active":True,"default_visible":False,"source_url":"geojson/weekly_flood_extent.geojson","meta":{}},
    {"group_key":"forecast_weekly","layer_key":"fw_population","name":"Population at Risk",
     "description":"People living in flood-prone zones (this week)","icon":"👥",
     "layer_type":"geojson_fc","display_order":2,"is_active":True,"default_visible":False,"source_url":"geojson/weekly_population.geojson","meta":{}},
    {"group_key":"forecast_weekly","layer_key":"fw_communities","name":"Communities at Risk",
     "description":"Settlements exposed to flooding this week","icon":"🏘️",
     "layer_type":"geojson_fc","display_order":3,"is_active":True,"default_visible":False,"source_url":"geojson/weekly_communities.geojson","meta":{}},
    {"group_key":"forecast_weekly","layer_key":"fw_health","name":"Health Facilities at Risk",
     "description":"Health facilities in flood zones this week","icon":"🏥",
     "layer_type":"geojson_fc","display_order":4,"is_active":True,"default_visible":False,"source_url":"geojson/weekly_health.geojson","meta":{}},
    {"group_key":"forecast_weekly","layer_key":"fw_schools","name":"Schools at Risk",
     "description":"Schools in flood zones this week","icon":"🏫",
     "layer_type":"geojson_fc","display_order":5,"is_active":True,"default_visible":False,"source_url":"geojson/weekly_schools.geojson","meta":{}},
    {"group_key":"forecast_weekly","layer_key":"fw_farmland","name":"Farmland Exposure",
     "description":"Agricultural land at risk this week","icon":"🌾",
     "layer_type":"geojson_fc","display_order":6,"is_active":True,"default_visible":False,"source_url":"geojson/weekly_farmland.geojson","meta":{}},
    {"group_key":"forecast_weekly","layer_key":"fw_roads","name":"Road Network at Risk",
     "description":"Roads and bridges at risk this week","icon":"🛣️",
     "layer_type":"geojson_fc","display_order":7,"is_active":True,"default_visible":False,"source_url":"geojson/weekly_roads.geojson","meta":{}},
]

router = APIRouter()


# ── Public endpoints ──────────────────────────────────────────────────────────

@router.get("", response_model=List[schemas.MapLayerOut])
def list_map_layers(db: Session = Depends(get_db)):
    """Public: all active layer definitions sorted by group and display order."""
    return (
        db.query(models.MapLayer)
        .filter(models.MapLayer.is_active == True)
        .order_by(models.MapLayer.group_key, models.MapLayer.display_order)
        .all()
    )


@router.get("/all", response_model=List[schemas.MapLayerOut])
def list_all_map_layers(
    db: Session = Depends(get_db),
    _user=Depends(require_role(models.UserRole.NIHSA_STAFF, models.UserRole.ADMIN, models.UserRole.SUB_ADMIN)),
):
    """Admin: all layers including inactive."""
    return (
        db.query(models.MapLayer)
        .order_by(models.MapLayer.group_key, models.MapLayer.display_order)
        .all()
    )


@router.get("/{layer_id}/template")
def download_csv_template(
    layer_id: str,
    db: Session = Depends(get_db),
    _user=Depends(require_role(models.UserRole.NIHSA_STAFF, models.UserRole.ADMIN, models.UserRole.SUB_ADMIN)),
):
    """
    Download a CSV template for a specific layer.
    Returns a ready-to-fill CSV with correct headers, 3 sample rows, and a notes row.
    """
    layer = db.query(models.MapLayer).filter(models.MapLayer.id == layer_id).first()
    if not layer:
        raise HTTPException(status_code=404, detail="Layer not found")

    tpl = _CSV_TEMPLATES.get(layer.layer_key)
    if not tpl:
        if layer.layer_type in ("geojson", "geojson_fc"):
            tpl = _GENERIC_TEMPLATE
        else:
            raise HTTPException(
                status_code=400,
                detail=f"No CSV template defined for layer '{layer.layer_key}'. "
                       "This layer type does not support CSV upload."
            )

    buf = io.StringIO()
    buf.write(f"# Layer: {layer.name}\n")
    buf.write(f"# Description: {tpl['description']}\n")
    buf.write(f"# Notes: {tpl['notes']}\n")
    buf.write(f"# DO NOT edit the header row. Remove all lines starting with #.\n")
    buf.write(tpl["columns"] + "\n")
    for row in tpl["sample"]:
        buf.write(row + "\n")

    filename = f"{layer.layer_key}_template.csv"
    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ── Admin endpoints ───────────────────────────────────────────────────────────

@router.post("/seed", status_code=201)
def seed_map_layers(
    db: Session = Depends(get_db),
    _user=Depends(require_role(models.UserRole.ADMIN)),
):
    """Idempotent seed of the default layer catalogue."""
    added = 0
    for d in _DEFAULT_LAYERS:
        exists = db.query(models.MapLayer).filter(
            models.MapLayer.layer_key == d["layer_key"]
        ).first()
        if not exists:
            db.add(models.MapLayer(**d))
            added += 1
    db.commit()
    return {"seeded": added}


@router.post("", response_model=schemas.MapLayerOut, status_code=201)
def create_map_layer(
    body: schemas.MapLayerCreate,
    db: Session = Depends(get_db),
    _user=Depends(require_role(models.UserRole.NIHSA_STAFF, models.UserRole.ADMIN)),
):
    if db.query(models.MapLayer).filter(
        models.MapLayer.layer_key == body.layer_key
    ).first():
        raise HTTPException(
            status_code=409, detail=f"layer_key '{body.layer_key}' already exists"
        )
    layer = models.MapLayer(**body.model_dump())
    db.add(layer)
    db.commit()
    db.refresh(layer)
    return layer


@router.put("/{layer_id}", response_model=schemas.MapLayerOut)
@router.patch("/{layer_id}", response_model=schemas.MapLayerOut)
def update_map_layer(
    layer_id: str,
    body: schemas.MapLayerUpdate,
    db: Session = Depends(get_db),
    _user=Depends(require_role(models.UserRole.NIHSA_STAFF, models.UserRole.ADMIN, models.UserRole.SUB_ADMIN)),
):
    layer = db.query(models.MapLayer).filter(
        models.MapLayer.id == layer_id
    ).first()
    if not layer:
        raise HTTPException(status_code=404, detail="Layer not found")
    for k, v in body.model_dump(exclude_none=True).items():
        setattr(layer, k, v)
    db.commit()
    db.refresh(layer)
    return layer


@router.delete("/{layer_id}", status_code=204)
def delete_map_layer(
    layer_id: str,
    db: Session = Depends(get_db),
    _user=Depends(require_role(models.UserRole.ADMIN)),
):
    layer = db.query(models.MapLayer).filter(
        models.MapLayer.id == layer_id
    ).first()
    if not layer:
        raise HTTPException(status_code=404, detail="Layer not found")
    db.delete(layer)
    db.commit()


@router.post("/sync-files")
def sync_existing_files(
    db: Session = Depends(get_db),
    _user=Depends(require_role(models.UserRole.NIHSA_STAFF, models.UserRole.ADMIN, models.UserRole.SUB_ADMIN)),
):
    """Scan geojson dir and register any files that exist but aren't recorded in meta."""
    _GEOJSON_DIR.mkdir(parents=True, exist_ok=True)
    updated = []
    layers = db.query(models.MapLayer).all()
    for layer in layers:
        meta = dict(layer.meta or {})
        if meta.get("data_file"):
            continue
        out_name = _KEY_TO_FILE.get(layer.layer_key, f"{layer.layer_key}.geojson")
        out_path = _GEOJSON_DIR / out_name
        if not out_path.exists():
            continue
        try:
            data = json.loads(out_path.read_text(encoding="utf-8"))
            n_features = len(data.get("features", []))
            size_kb = out_path.stat().st_size // 1024
            meta.update({
                "data_file":       out_name,
                "feature_count":   n_features,
                "file_size_kb":    size_kb,
                "uploaded_at":     datetime.now(timezone.utc).isoformat(),
                "source_filename": out_name,
            })
            layer.meta = meta
            layer.source_url = f"geojson/{out_name}"
            updated.append(layer.layer_key)
        except Exception:
            continue
    db.commit()
    return {"synced": len(updated), "layers": updated}


@router.post("/{layer_id}/upload")
async def upload_layer_csv(
    layer_id: str,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    _user=Depends(require_role(models.UserRole.NIHSA_STAFF, models.UserRole.ADMIN, models.UserRole.SUB_ADMIN)),
):
    """
    Upload a CSV file to populate a map layer.

    Required columns depend on the layer (download the template for exact format).
    All layers must have  lat  and  lon  columns.

    The CSV is converted to GeoJSON and served on the public map.
    A validation summary (rows accepted / skipped) is returned so the admin
    can confirm the data loaded correctly before publishing.
    """
    layer = db.query(models.MapLayer).filter(
        models.MapLayer.id == layer_id
    ).first()
    if not layer:
        raise HTTPException(status_code=404, detail="Layer not found")

    # CSV only
    filename = file.filename or ""
    if not filename.lower().endswith(".csv"):
        raise HTTPException(
            status_code=400,
            detail=(
                "Only CSV files are accepted. "
                "Download the template from the '⬇ Template' button to see the required format."
            ),
        )

    # Check template exists for this layer (fall back to generic template for geojson-type layers)
    tpl = _CSV_TEMPLATES.get(layer.layer_key)
    if not tpl:
        if layer.layer_type in ("geojson", "geojson_fc"):
            tpl = _GENERIC_TEMPLATE
        else:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Layer '{layer.name}' (type: {layer.layer_type}) does not support CSV upload. "
                    "Only geojson and geojson_fc layer types accept CSV files."
                ),
            )

    content = await file.read()
    if not content.strip():
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    _GEOJSON_DIR.mkdir(parents=True, exist_ok=True)

    # Convert CSV → GeoJSON
    geojson = _csv_to_fc(content, layer.layer_key)
    n_features = len(geojson.get("features", []))
    n_skipped  = geojson.pop("_skipped", 0)

    if n_features == 0:
        # Build a helpful error message showing what columns were found vs expected
        try:
            text = content.decode("utf-8", errors="ignore").lstrip("\ufeff")
            found_cols = text.splitlines()[0].strip() if text.strip() else "(empty)"
        except Exception:
            found_cols = "(could not read)"
        raise HTTPException(
            status_code=400,
            detail=(
                f"No valid rows found. 0 features produced.\n"
                f"Columns found in your file: {found_cols}\n"
                f"Expected columns: {tpl['columns']}\n"
                f"Make sure lat and lon columns are present and contain decimal numbers."
            ),
        )

    # Save GeoJSON to disk
    out_name = _KEY_TO_FILE.get(layer.layer_key, f"{layer.layer_key}.geojson")
    out_path  = _GEOJSON_DIR / out_name
    out_path.write_text(json.dumps(geojson), encoding="utf-8")
    size_kb = out_path.stat().st_size // 1024

    # Update layer meta in DB
    meta = dict(layer.meta or {})
    meta.update({
        "data_file":       out_name,
        "feature_count":   n_features,
        "file_size_kb":    size_kb,
        "rows_skipped":    n_skipped,
        "uploaded_at":     datetime.now(timezone.utc).isoformat(),
        "source_filename": filename,
    })
    layer.meta       = meta
    layer.source_url = f"geojson/{out_name}"
    db.commit()
    db.refresh(layer)

    return {
        "status":        "ok",
        "layer_key":     layer.layer_key,
        "feature_count": n_features,
        "rows_skipped":  n_skipped,
        "size_kb":       size_kb,
        "message": (
            f"{n_features:,} features loaded successfully"
            + (f" ({n_skipped} rows skipped — missing or invalid coordinates)" if n_skipped else "")
        ),
    }
