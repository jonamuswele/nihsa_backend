"""map_layers.py - Complete working version with R2 storage"""

import os
import json
import csv
import io
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from fastapi.responses import Response
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timezone
from pathlib import Path

from database import get_db
import models, schemas
from auth_utils import require_role

router = APIRouter()

# ==============================================
# SIMPLE R2 STORAGE - NO IMPORT ERRORS
# ==============================================

# R2 Configuration from environment
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY_ID", "")
R2_SECRET_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "nihsamedia")
R2_CUSTOM_DOMAIN = os.getenv("R2_CUSTOM_DOMAIN", "")
USE_R2 = os.getenv("USE_R2", "false").lower() == "true"

# Initialize R2 client safely
r2_client = None
if USE_R2 and R2_ACCOUNT_ID and R2_ACCESS_KEY and R2_SECRET_KEY:
    try:
        import boto3
        from botocore.config import Config
        r2_client = boto3.client(
            's3',
            endpoint_url=f'https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com',
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
            config=Config(signature_version='s3v4'),
            region_name='auto'
        )
        print(f"✅ R2 initialized for bucket: {R2_BUCKET_NAME}")
    except Exception as e:
        print(f"⚠️ R2 init failed: {e}")
        USE_R2 = False

# Map layer_key to R2 path
R2_MAP_PREFIX = "map-layers/"
LAYER_PATHS = {
    "fc_flood_extent": f"{R2_MAP_PREFIX}forecast/flood_extent.geojson",
    "fc_population": f"{R2_MAP_PREFIX}forecast/population.geojson",
    "fc_communities": f"{R2_MAP_PREFIX}forecast/communities.geojson",
    "fc_health": f"{R2_MAP_PREFIX}forecast/health.geojson",
    "fc_schools": f"{R2_MAP_PREFIX}forecast/schools.geojson",
    "fc_farmland": f"{R2_MAP_PREFIX}forecast/farmland.geojson",
    "fc_roads": f"{R2_MAP_PREFIX}forecast/roads.geojson",
    "fw_flood_extent": f"{R2_MAP_PREFIX}forecast_weekly/flood_extent.geojson",
    "fw_population": f"{R2_MAP_PREFIX}forecast_weekly/population.geojson",
    "fw_communities": f"{R2_MAP_PREFIX}forecast_weekly/communities.geojson",
    "fw_health": f"{R2_MAP_PREFIX}forecast_weekly/health.geojson",
    "fw_schools": f"{R2_MAP_PREFIX}forecast_weekly/schools.geojson",
    "fw_farmland": f"{R2_MAP_PREFIX}forecast_weekly/farmland.geojson",
    "fw_roads": f"{R2_MAP_PREFIX}forecast_weekly/roads.geojson",
    "sw_satellite": f"{R2_MAP_PREFIX}surface_water/satellite.geojson",
    "sw_station_updates": f"{R2_MAP_PREFIX}surface_water/station_updates.geojson",
    "gw_levels": f"{R2_MAP_PREFIX}groundwater/levels.geojson",
    "gw_aquifer": f"{R2_MAP_PREFIX}groundwater/aquifer.geojson",
    "gw_recharge": f"{R2_MAP_PREFIX}groundwater/recharge.geojson",
    "wq_index": f"{R2_MAP_PREFIX}water_quality/index.geojson",
    "wq_turbidity": f"{R2_MAP_PREFIX}water_quality/turbidity.geojson",
    "wq_contamination": f"{R2_MAP_PREFIX}water_quality/contamination.geojson",
    "cm_coastal_risk": f"{R2_MAP_PREFIX}coastal_marine/coastal_risk.geojson",
    "cm_storm_surge": f"{R2_MAP_PREFIX}coastal_marine/storm_surge.geojson",
    "cm_erosion": f"{R2_MAP_PREFIX}coastal_marine/erosion.geojson",
    "cm_mangrove": f"{R2_MAP_PREFIX}coastal_marine/mangrove.geojson",
}


def get_public_url(key: str) -> str:
    if R2_CUSTOM_DOMAIN:
        return f"https://{R2_CUSTOM_DOMAIN}/{key}"
    return f"https://{R2_BUCKET_NAME}.r2.dev/{key}"


def csv_to_geojson(content_bytes: bytes) -> tuple:
    """Convert CSV to GeoJSON. Returns (geojson_str, feature_count, skipped)"""
    text = content_bytes.decode("utf-8", errors="ignore").lstrip("\ufeff")
    reader = csv.DictReader(io.StringIO(text))
    
    features = []
    skipped = 0
    
    for row in reader:
        norm = {k.strip().lower(): v.strip() for k, v in row.items()}
        
        lat = norm.get("lat") or norm.get("latitude")
        lon = norm.get("lon") or norm.get("longitude") or norm.get("lng")
        
        if not lat or not lon:
            skipped += 1
            continue
        
        try:
            lat_val = float(lat)
            lon_val = float(lon)
        except ValueError:
            skipped += 1
            continue
        
        # Validate Nigeria bounds
        if not (4.0 <= lat_val <= 14.0) or not (2.5 <= lon_val <= 15.0):
            skipped += 1
            continue
        
        # Build properties
        props = {}
        for k, v in norm.items():
            if k not in ["lat", "latitude", "lon", "longitude", "lng"] and v:
                try:
                    props[k] = float(v) if "." in v else int(v)
                except ValueError:
                    props[k] = v
        
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon_val, lat_val]},
            "properties": props
        })
    
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    
    return json.dumps(geojson), len(features), skipped


# ==============================================
# CSV TEMPLATES
# ==============================================

CSV_TEMPLATES = {
    "fc_flood_extent": {
        "columns": "risk_zone,state,lga,lat,lon",
        "sample": ["high,Kogi,Lokoja,7.8069,6.7420"]
    },
    "fc_population": {
        "columns": "name,state,lga,lat,lon,population",
        "sample": ["Lokoja Town,Kogi,Lokoja,7.8069,6.7420,12500"]
    },
    "fc_communities": {
        "columns": "name,state,lga,lat,lon,depth",
        "sample": ["Ganaja Village,Kogi,Lokoja,7.8200,6.7350,1.5"]
    },
}

GENERIC_TEMPLATE = {
    "columns": "name,state,lga,lat,lon",
    "sample": ["Location Name,State,LGA,7.8069,6.7420"]
}


# ==============================================
# PUBLIC ENDPOINTS
# ==============================================

@router.get("", response_model=List[schemas.MapLayerOut])
def list_map_layers(db: Session = Depends(get_db)):
    """Public: all active layer definitions"""
    layers = db.query(models.MapLayer).filter(
        models.MapLayer.is_active == True
    ).order_by(models.MapLayer.group_key, models.MapLayer.display_order).all()
    
    # Update source_url for layers that have R2 files
    for layer in layers:
        if layer.layer_key in LAYER_PATHS and USE_R2 and r2_client:
            layer.source_url = get_public_url(LAYER_PATHS[layer.layer_key])
    
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
    
    for layer in layers:
        if layer.layer_key in LAYER_PATHS and USE_R2 and r2_client:
            layer.source_url = get_public_url(LAYER_PATHS[layer.layer_key])
    
    return layers


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
    
    tpl = CSV_TEMPLATES.get(layer.layer_key, GENERIC_TEMPLATE)
    
    content = f"# Template for {layer.name}\n"
    content += f"# {layer.description}\n"
    content += f"# Required columns: {tpl['columns']}\n"
    content += f"# Coordinates must be within Nigeria (lat: 4-14, lon: 2.5-15)\n"
    content += tpl["columns"] + "\n"
    for row in tpl["sample"]:
        content += row + "\n"
    
    return Response(
        content=content,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{layer.layer_key}_template.csv"'},
    )


# ==============================================
# ADMIN UPLOAD ENDPOINT - THE IMPORTANT ONE
# ==============================================

@router.post("/{layer_id}/upload", status_code=201)
async def upload_layer_data(
    layer_id: str,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    _user=Depends(require_role(models.UserRole.NIHSA_STAFF, models.UserRole.ADMIN, models.UserRole.SUB_ADMIN)),
):
    """Upload CSV or GeoJSON file for a map layer to R2"""
    
    # 1. Find the layer
    layer = db.query(models.MapLayer).filter(models.MapLayer.id == layer_id).first()
    if not layer:
        raise HTTPException(status_code=404, detail="Layer not found")
    
    # 2. Check if we have R2 configured
    if not USE_R2 or not r2_client:
        raise HTTPException(
            status_code=503, 
            detail="R2 storage is not configured. Please set USE_R2=true and R2 credentials."
        )
    
    # 3. Get the R2 path for this layer
    r2_path = LAYER_PATHS.get(layer.layer_key)
    if not r2_path:
        raise HTTPException(
            status_code=400,
            detail=f"No storage path configured for layer type: {layer.layer_key}"
        )
    
    # 4. Read file
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")
    
    filename = file.filename or ""
    is_csv = filename.lower().endswith('.csv')
    is_geojson = filename.lower().endswith('.geojson') or filename.lower().endswith('.json')
    
    if not (is_csv or is_geojson):
        raise HTTPException(
            status_code=400,
            detail="Only CSV or GeoJSON files are supported"
        )
    
    # 5. Convert CSV to GeoJSON if needed
    if is_csv:
        geojson_str, feature_count, skipped = csv_to_geojson(content)
        final_content = geojson_str.encode('utf-8')
        content_type = "application/geo+json"
    else:
        # Validate GeoJSON
        try:
            geojson_data = json.loads(content.decode('utf-8'))
            feature_count = len(geojson_data.get('features', []))
            skipped = 0
            final_content = content
            content_type = "application/geo+json"
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail=f"Invalid GeoJSON: {e}")
    
    if feature_count == 0:
        raise HTTPException(
            status_code=400,
            detail="No valid features found in file. Check lat/lon columns."
        )
    
    # 6. Upload to R2
    try:
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=r2_path,
            Body=final_content,
            ContentType=content_type,
            Metadata={
                'original_filename': filename,
                'layer_key': layer.layer_key,
                'feature_count': str(feature_count),
                'uploaded_at': datetime.now(timezone.utc).isoformat()
            }
        )
        print(f"✅ Uploaded to R2: {r2_path}")
    except Exception as e:
        print(f"❌ R2 upload error: {e}")
        raise HTTPException(status_code=500, detail=f"R2 upload failed: {str(e)}")
    
    # 7. Update layer metadata in database
    public_url = get_public_url(r2_path)
    layer.source_url = public_url
    
    meta = dict(layer.meta or {})
    meta.update({
        "r2_path": r2_path,
        "feature_count": feature_count,
        "rows_skipped": skipped,
        "original_filename": filename,
        "uploaded_at": datetime.now(timezone.utc).isoformat(),
        "storage": "r2"
    })
    layer.meta = meta
    
    db.commit()
    db.refresh(layer)
    
    # 8. Return success response
    return {
        "success": True,
        "layer_key": layer.layer_key,
        "feature_count": feature_count,
        "rows_skipped": skipped,
        "size_kb": round(len(final_content) / 1024, 1),
        "public_url": public_url,
        "message": f"Successfully uploaded {feature_count:,} features to R2"
    }


# ==============================================
# DELETE ENDPOINT
# ==============================================

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
    
    r2_path = LAYER_PATHS.get(layer.layer_key)
    if not r2_path or not USE_R2 or not r2_client:
        raise HTTPException(status_code=404, detail="No data file found")
    
    try:
        r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=r2_path)
        print(f"✅ Deleted from R2: {r2_path}")
    except Exception as e:
        print(f"❌ R2 delete error: {e}")
    
    layer.source_url = ""
    if layer.meta:
        layer.meta.pop("r2_path", None)
        layer.meta.pop("feature_count", None)
    
    db.commit()
    return


# ==============================================
# DEFAULT LAYERS SEED
# ==============================================

DEFAULT_LAYERS = [
    # Surface Water
    {"group_key":"surface_water","layer_key":"stations","name":"River Gauge Stations",
     "description":"358 NIHSA real-time river level monitoring stations","icon":"📍",
     "layer_type":"toggle","display_order":1,"is_active":True,"default_visible":True},
    {"group_key":"surface_water","layer_key":"alerts","name":"Active Flood Alerts",
     "description":"Published flood warnings","icon":"⚠️","layer_type":"toggle",
     "display_order":2,"is_active":True,"default_visible":True},
    {"group_key":"surface_water","layer_key":"reports","name":"Citizen Flood Reports",
     "description":"Verified field reports","icon":"💧","layer_type":"toggle",
     "display_order":3,"is_active":True,"default_visible":False},
    
    # Annual Forecast
    {"group_key":"forecast","layer_key":"fc_flood_extent","name":"Flood Extent & Depth",
     "description":"Annual inundation extent","icon":"💧","layer_type":"geojson_fc",
     "display_order":2,"is_active":True,"default_visible":False},
    {"group_key":"forecast","layer_key":"fc_population","name":"Population at Risk",
     "description":"People in flood zones","icon":"👥","layer_type":"geojson_fc",
     "display_order":3,"is_active":True,"default_visible":False},
    {"group_key":"forecast","layer_key":"fc_communities","name":"Communities at Risk",
     "description":"Settlements exposed","icon":"🏘️","layer_type":"geojson_fc",
     "display_order":4,"is_active":True,"default_visible":False},
    
    # Weekly Forecast
    {"group_key":"forecast_weekly","layer_key":"fw_flood_extent","name":"Weekly Flood Extent",
     "description":"Current week inundation","icon":"💧","layer_type":"geojson_fc",
     "display_order":1,"is_active":True,"default_visible":False},
]


@router.post("/seed", status_code=201)
def seed_map_layers(
    db: Session = Depends(get_db),
    _user=Depends(require_role(models.UserRole.ADMIN)),
):
    """Seed default layers"""
    added = 0
    for d in DEFAULT_LAYERS:
        exists = db.query(models.MapLayer).filter(
            models.MapLayer.layer_key == d["layer_key"]
        ).first()
        if not exists:
            db.add(models.MapLayer(**d))
            added += 1
    db.commit()
    return {"seeded": added}
