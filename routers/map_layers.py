"""map_layers.py - Updated for R2 storage"""

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from fastapi.responses import Response
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timezone
import json

from database import get_db
import models, schemas
from auth_utils import require_role
from r2_storage import (
    upload_layer_file, get_layer_file, delete_layer_file,
    list_all_layer_files, LAYER_TO_FOLDER
)

router = APIRouter()

# CSV Templates for each layer (same as before)
_CSV_TEMPLATES = {
    "fc_flood_extent": {
        "columns": "risk_zone,state,lga,lat,lon",
        "description": "Flood risk zone locations. One row per location.",
        "sample": ["high,Kogi,Lokoja,7.8069,6.7420"],
        "notes": "risk_zone must be: watch, medium, high, severe, or extreme"
    },
    "fc_population": {
        "columns": "name,state,lga,lat,lon,population",
        "description": "Population centres at annual flood risk.",
        "sample": ["Onu Nwokwo,Ebonyi,Ohaukwu,6.5047,7.9650,12500"],
        "notes": "population is optional but recommended"
    },
    "fc_communities": {
        "columns": "name,state,lga,lat,lon,depth",
        "description": "Communities/settlements at annual flood risk.",
        "sample": ["Tsamiya Layin Makera,Yobe,Bade,12.9021,11.0481,1.5"],
        "notes": "depth = expected flood depth in metres"
    },
    "fc_health": {
        "columns": "name,state,lga,lat,lon,facility_type,depth",
        "description": "Health facilities at annual flood risk.",
        "sample": ["Meleri PHC,Borno,Mobbar,13.2167,13.1500,PHC,0.7"],
        "notes": "facility_type: PHC, Clinic, Hospital, Health Centre"
    },
    "fc_schools": {
        "columns": "name,state,lga,lat,lon,school_type,depth",
        "description": "Schools at annual flood risk.",
        "sample": ["Gill Educational Center,Niger,Mariga,10.3833,5.4167,Primary,1.0"],
        "notes": "school_type: Primary, Secondary, University"
    },
    "fc_farmland": {
        "columns": "name,state,lga,lat,lon,crop_type,area_ha,depth",
        "description": "Farmland at annual flood risk.",
        "sample": ["Ayedade Farm,Osun,Ayedade,7.5833,4.3333,Rice,12.5,0.8"],
        "notes": "area_ha = farm area in hectares"
    },
    "fc_roads": {
        "columns": "name,state,lga,lat,lon,road_class,depth",
        "description": "Roads at annual flood risk.",
        "sample": ["Sapele-Warri Road,Delta,,5.8904,5.6781,Federal,0.9"],
        "notes": "road_class: Federal, State, LGA"
    },
}

# Generic template for other layers
_GENERIC_TEMPLATE = {
    "columns": "name,state,lga,lat,lon",
    "description": "Generic spatial data layer",
    "sample": ["Location A,Kogi,Lokoja,7.8069,6.7420"],
    "notes": "Required columns: lat and lon"
}

# Fill in weekly templates (same as annual)
for wk in ["fw_flood_extent", "fw_population", "fw_communities", 
           "fw_health", "fw_schools", "fw_farmland", "fw_roads"]:
    base = wk.replace("fw_", "fc_")
    if base in _CSV_TEMPLATES:
        _CSV_TEMPLATES[wk] = _CSV_TEMPLATES[base]


# ========== PUBLIC ENDPOINTS ==========

@router.get("", response_model=List[schemas.MapLayerOut])
def list_map_layers(db: Session = Depends(get_db)):
    """Public: all active layer definitions"""
    layers = db.query(models.MapLayer).filter(
        models.MapLayer.is_active == True
    ).order_by(models.MapLayer.group_key, models.MapLayer.display_order).all()
    
    # Enhance with R2 file info if available
    r2_files = list_all_layer_files()
    for layer in layers:
        if layer.layer_key in r2_files:
            # Update source_url to point to R2
            layer.source_url = r2_files[layer.layer_key]["url"]
            # Add metadata
            if not layer.meta:
                layer.meta = {}
            layer.meta["r2_available"] = True
            layer.meta["r2_last_modified"] = r2_files[layer.layer_key]["last_modified"].isoformat()
    
    return layers


@router.get("/all", response_model=List[schemas.MapLayerOut])
def list_all_map_layers(
    db: Session = Depends(get_db),
    _user=Depends(require_role(models.UserRole.NIHSA_STAFF, models.UserRole.ADMIN, models.UserRole.SUB_ADMIN)),
):
    """Admin: all layers including inactive"""
    layers = db.query(models.MapLayer).order_by(
        models.MapLayer.group_key, models.MapLayer.display_order
    ).all()
    
    r2_files = list_all_layer_files()
    for layer in layers:
        if layer.layer_key in r2_files:
            layer.source_url = r2_files[layer.layer_key]["url"]
    
    return layers


@router.get("/{layer_id}/status")
def get_layer_status(
    layer_id: str,
    db: Session = Depends(get_db),
    _user=Depends(require_role(models.UserRole.NIHSA_STAFF, models.UserRole.ADMIN, models.UserRole.SUB_ADMIN)),
):
    """Check if a layer has data uploaded to R2"""
    layer = db.query(models.MapLayer).filter(models.MapLayer.id == layer_id).first()
    if not layer:
        raise HTTPException(status_code=404, detail="Layer not found")
    
    r2_file = get_layer_file(layer.layer_key)
    
    return {
        "layer_key": layer.layer_key,
        "name": layer.name,
        "has_data": r2_file is not None,
        "feature_count": r2_file["metadata"].get("feature_count") if r2_file else None,
        "last_modified": r2_file["last_modified"] if r2_file else None,
        "url": r2_file["url"] if r2_file else None
    }


@router.get("/{layer_id}/template")
def download_csv_template(
    layer_id: str,
    db: Session = Depends(get_db),
    _user=Depends(require_role(models.UserRole.NIHSA_STAFF, models.UserRole.ADMIN, models.UserRole.SUB_ADMIN)),
):
    """Download CSV template for a layer"""
    layer = db.query(models.MapLayer).filter(models.MapLayer.id == layer_id).first()
    if not layer:
        raise HTTPException(status_code=404, detail="Layer not found")
    
    tpl = _CSV_TEMPLATES.get(layer.layer_key, _GENERIC_TEMPLATE)
    
    buf = io.StringIO()
    buf.write(f"# Layer: {layer.name}\n")
    buf.write(f"# Description: {tpl['description']}\n")
    buf.write(f"# Notes: {tpl['notes']}\n")
    buf.write(f"# DO NOT edit the header row.\n")
    buf.write(tpl["columns"] + "\n")
    for row in tpl["sample"]:
        buf.write(row + "\n")
    
    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{layer.layer_key}_template.csv"'},
    )


# ========== ADMIN ENDPOINTS ==========

@router.post("/{layer_id}/upload", status_code=201)
async def upload_layer_data(
    layer_id: str,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    _user=Depends(require_role(models.UserRole.NIHSA_STAFF, models.UserRole.ADMIN, models.UserRole.SUB_ADMIN)),
):
    """
    Upload a CSV or GeoJSON file for a map layer.
    - CSV files are automatically converted to GeoJSON
    - GeoJSON files are used as-is
    - Files overwrite previous uploads
    - Stored in Cloudflare R2, not on Render
    """
    layer = db.query(models.MapLayer).filter(models.MapLayer.id == layer_id).first()
    if not layer:
        raise HTTPException(status_code=404, detail="Layer not found")
    
    # Validate file type
    filename = file.filename or ""
    if not (filename.lower().endswith('.csv') or 
            filename.lower().endswith('.geojson') or 
            filename.lower().endswith('.json')):
        raise HTTPException(
            status_code=400,
            detail="Only CSV, GeoJSON, or JSON files are supported."
        )
    
    # Read file content
    content = await file.read()
    if not content.strip():
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")
    
    # Upload to R2
    try:
        result = upload_layer_file(layer.layer_key, content, filename)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")
    
    # Update the layer's source_url in the database to point to R2
    layer.source_url = result["public_url"]
    
    # Update metadata
    meta = dict(layer.meta or {})
    meta.update({
        "r2_key": result["r2_key"],
        "feature_count": result["feature_count"],
        "rows_skipped": result["rows_skipped"],
        "file_size_bytes": result["file_size_bytes"],
        "original_filename": result["original_filename"],
        "uploaded_at": datetime.now(timezone.utc).isoformat(),
        "storage": "r2"
    })
    layer.meta = meta
    
    db.commit()
    db.refresh(layer)
    
    return {
        "success": True,
        "layer_key": layer.layer_key,
        "feature_count": result["feature_count"],
        "rows_skipped": result["rows_skipped"],
        "file_size_kb": round(result["file_size_bytes"] / 1024, 1),
        "public_url": result["public_url"],
        "message": f"Successfully uploaded {result['feature_count']:,} features to R2."
    }


@router.delete("/{layer_id}/data", status_code=204)
def delete_layer_data(
    layer_id: str,
    db: Session = Depends(get_db),
    _user=Depends(require_role(models.UserRole.ADMIN)),
):
    """Delete the data file for a map layer from R2"""
    layer = db.query(models.MapLayer).filter(models.MapLayer.id == layer_id).first()
    if not layer:
        raise HTTPException(status_code=404, detail="Layer not found")
    
    success = delete_layer_file(layer.layer_key)
    if not success:
        raise HTTPException(status_code=404, detail="No data file found for this layer")
    
    # Clear the source_url in database
    layer.source_url = ""
    if layer.meta:
        layer.meta.pop("r2_key", None)
        layer.meta.pop("feature_count", None)
    
    db.commit()
    return


@router.post("/sync-r2")
def sync_r2_files(
    db: Session = Depends(get_db),
    _user=Depends(require_role(models.UserRole.ADMIN)),
):
    """Sync database layer records with existing R2 files"""
    r2_files = list_all_layer_files()
    updated = []
    
    layers = db.query(models.MapLayer).all()
    for layer in layers:
        if layer.layer_key in r2_files:
            # Update source_url to point to R2
            layer.source_url = r2_files[layer.layer_key]["url"]
            
            # Update metadata
            meta = dict(layer.meta or {})
            meta["r2_available"] = True
            meta["r2_last_modified"] = r2_files[layer.layer_key]["last_modified"].isoformat()
            layer.meta = meta
            
            updated.append(layer.layer_key)
    
    db.commit()
    
    return {
        "synced": len(updated),
        "layers": updated,
        "total_in_r2": len(r2_files)
    }


# ========== DEFAULT LAYERS (same as before) ==========

_DEFAULT_LAYERS = [
    # Surface Water
    {"group_key":"surface_water","layer_key":"stations","name":"River Gauge Stations",
     "description":"358 NIHSA real-time river level monitoring stations","icon":"📍",
     "layer_type":"toggle","display_order":1,"is_active":True,"default_visible":True},
    {"group_key":"surface_water","layer_key":"alerts","name":"Active Flood Alerts",
     "description":"Published flood warnings from the NIHSA alert system","icon":"⚠️",
     "layer_type":"toggle","display_order":2,"is_active":True,"default_visible":True},
    {"group_key":"surface_water","layer_key":"reports","name":"Citizen Flood Reports",
     "description":"Verified field reports","icon":"💧",
     "layer_type":"toggle","display_order":3,"is_active":True,"default_visible":False},
    {"group_key":"surface_water","layer_key":"sw_satellite","name":"Satellite Flood Extent",
     "description":"Satellite-derived flood inundation","icon":"🛰️",
     "layer_type":"geojson_fc","display_order":4,"is_active":True,"default_visible":False},
    {"group_key":"surface_water","layer_key":"sw_station_updates","name":"Station Updates",
     "description":"Field observations per gauge","icon":"📡",
     "layer_type":"geojson_fc","display_order":5,"is_active":True,"default_visible":False},
    
    # Annual Forecast
    {"group_key":"forecast","layer_key":"fc_flood_extent","name":"Flood Extent & Depth",
     "description":"Annual inundation extent","icon":"💧",
     "layer_type":"geojson_fc","display_order":2,"is_active":True,"default_visible":False},
    {"group_key":"forecast","layer_key":"fc_population","name":"Population at Risk",
     "description":"People in flood zones","icon":"👥",
     "layer_type":"geojson_fc","display_order":3,"is_active":True,"default_visible":False},
    {"group_key":"forecast","layer_key":"fc_communities","name":"Communities at Risk",
     "description":"Settlements exposed","icon":"🏘️",
     "layer_type":"geojson_fc","display_order":4,"is_active":True,"default_visible":False},
    {"group_key":"forecast","layer_key":"fc_health","name":"Health Facilities at Risk",
     "description":"Clinics in flood zones","icon":"🏥",
     "layer_type":"geojson_fc","display_order":5,"is_active":True,"default_visible":False},
    {"group_key":"forecast","layer_key":"fc_schools","name":"Schools at Risk",
     "description":"Schools in flood zones","icon":"🏫",
     "layer_type":"geojson_fc","display_order":6,"is_active":True,"default_visible":False},
    {"group_key":"forecast","layer_key":"fc_farmland","name":"Farmland Exposure",
     "description":"Agricultural land at risk","icon":"🌾",
     "layer_type":"geojson_fc","display_order":7,"is_active":True,"default_visible":False},
    {"group_key":"forecast","layer_key":"fc_roads","name":"Road Network at Risk",
     "description":"Roads vulnerable","icon":"🛣️",
     "layer_type":"geojson_fc","display_order":8,"is_active":True,"default_visible":False},
    
    # Weekly Forecast (same structure)
    {"group_key":"forecast_weekly","layer_key":"fw_flood_extent","name":"Weekly Flood Extent",
     "description":"Current week inundation","icon":"💧",
     "layer_type":"geojson_fc","display_order":1,"is_active":True,"default_visible":False},
    {"group_key":"forecast_weekly","layer_key":"fw_population","name":"Weekly Population at Risk",
     "icon":"👥","layer_type":"geojson_fc","display_order":2,"is_active":True,"default_visible":False},
    {"group_key":"forecast_weekly","layer_key":"fw_communities","name":"Weekly Communities at Risk",
     "icon":"🏘️","layer_type":"geojson_fc","display_order":3,"is_active":True,"default_visible":False},
    {"group_key":"forecast_weekly","layer_key":"fw_health","name":"Weekly Health at Risk",
     "icon":"🏥","layer_type":"geojson_fc","display_order":4,"is_active":True,"default_visible":False},
    {"group_key":"forecast_weekly","layer_key":"fw_schools","name":"Weekly Schools at Risk",
     "icon":"🏫","layer_type":"geojson_fc","display_order":5,"is_active":True,"default_visible":False},
    {"group_key":"forecast_weekly","layer_key":"fw_farmland","name":"Weekly Farmland at Risk",
     "icon":"🌾","layer_type":"geojson_fc","display_order":6,"is_active":True,"default_visible":False},
    {"group_key":"forecast_weekly","layer_key":"fw_roads","name":"Weekly Roads at Risk",
     "icon":"🛣️","layer_type":"geojson_fc","display_order":7,"is_active":True,"default_visible":False},
    
    # Groundwater
    {"group_key":"groundwater","layer_key":"gw_levels","name":"Groundwater Levels",
     "icon":"🔵","layer_type":"geojson_fc","display_order":1,"is_active":True,"default_visible":False},
    {"group_key":"groundwater","layer_key":"gw_aquifer","name":"Aquifer Zones",
     "icon":"🗺️","layer_type":"geojson_fc","display_order":2,"is_active":True,"default_visible":False},
    {"group_key":"groundwater","layer_key":"gw_recharge","name":"Recharge Areas",
     "icon":"♻️","layer_type":"geojson_fc","display_order":3,"is_active":True,"default_visible":False},
    
    # Water Quality
    {"group_key":"water_quality","layer_key":"wq_index","name":"Water Quality Index",
     "icon":"🧪","layer_type":"geojson_fc","display_order":1,"is_active":True,"default_visible":False},
    {"group_key":"water_quality","layer_key":"wq_turbidity","name":"Turbidity",
     "icon":"🌊","layer_type":"geojson_fc","display_order":2,"is_active":True,"default_visible":False},
    {"group_key":"water_quality","layer_key":"wq_contamination","name":"Contamination Risk",
     "icon":"⚗️","layer_type":"geojson_fc","display_order":3,"is_active":True,"default_visible":False},
    
    # Coastal & Marine
    {"group_key":"coastal_marine","layer_key":"cm_coastal_risk","name":"Coastal Flood Risk",
     "icon":"🏖️","layer_type":"geojson_fc","display_order":1,"is_active":True,"default_visible":False},
    {"group_key":"coastal_marine","layer_key":"cm_storm_surge","name":"Storm Surge Zones",
     "icon":"🌀","layer_type":"geojson_fc","display_order":2,"is_active":True,"default_visible":False},
    {"group_key":"coastal_marine","layer_key":"cm_erosion","name":"Coastal Erosion",
     "icon":"⛰️","layer_type":"geojson_fc","display_order":3,"is_active":True,"default_visible":False},
    {"group_key":"coastal_marine","layer_key":"cm_mangrove","name":"Mangrove Zones",
     "icon":"🌿","layer_type":"geojson_fc","display_order":4,"is_active":True,"default_visible":False},
]

import io  # Add at top of file if not already


@router.post("/seed", status_code=201)
def seed_map_layers(
    db: Session = Depends(get_db),
    _user=Depends(require_role(models.UserRole.ADMIN)),
):
    """Idempotent seed of the default layer catalogue"""
    added = 0
    for d in _DEFAULT_LAYERS:
        exists = db.query(models.MapLayer).filter(
            models.MapLayer.layer_key == d["layer_key"]
        ).first()
        if not exists:
            # Add missing fields with defaults
            full_d = {
                "name": d["name"],
                "group_key": d["group_key"],
                "layer_key": d["layer_key"],
                "description": d.get("description", ""),
                "layer_type": d.get("layer_type", "toggle"),
                "source_url": d.get("source_url", ""),
                "icon": d.get("icon", "🗺️"),
                "display_order": d.get("display_order", 0),
                "is_active": d.get("is_active", True),
                "default_visible": d.get("default_visible", False),
                "meta": d.get("meta", {})
            }
            db.add(models.MapLayer(**full_d))
            added += 1
    db.commit()
    return {"seeded": added}
