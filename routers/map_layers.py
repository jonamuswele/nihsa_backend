"""map_layers.py - Complete working version with R2 storage"""


import os
import json
import csv
import io
import uuid
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from fastapi.responses import Response
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timezone

from database import get_db
import models, schemas
from auth_utils import require_role

router = APIRouter()

@router.post("/test-upload")
async def test_upload_only(file: UploadFile = File(...)):
    """Simple test upload - no database, no R2"""
    print(f"🔵 TEST UPLOAD CALLED! File: {file.filename}")
    content = await file.read()
    print(f"🔵 File size: {len(content)} bytes")
    return {
        "success": True, 
        "filename": file.filename,
        "size_bytes": len(content)
    }
    
@router.get("/ping")
def ping():
    print("✅ PING endpoint called!")
    return {"status": "alive", "message": "Map layers router is working!"}

# R2 Configuration
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY_ID", "")
R2_SECRET_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "nihsamedia")
R2_CUSTOM_DOMAIN = os.getenv("R2_CUSTOM_DOMAIN", "")
USE_R2 = os.getenv("USE_R2", "false").lower() == "true"

print("=" * 50)
print("🔍 ENVIRONMENT VARIABLES CHECK:")
print(f"USE_R2 = {os.getenv('USE_R2', 'NOT SET')}")
print(f"R2_ACCOUNT_ID = {os.getenv('R2_ACCOUNT_ID', 'NOT SET')[:10] if os.getenv('R2_ACCOUNT_ID') else 'NOT SET'}...")
print(f"R2_ACCESS_KEY_ID = {'SET' if os.getenv('R2_ACCESS_KEY_ID') else 'NOT SET'}")
print(f"R2_SECRET_ACCESS_KEY = {'SET' if os.getenv('R2_SECRET_ACCESS_KEY') else 'NOT SET'}")
print(f"R2_BUCKET_NAME = {os.getenv('R2_BUCKET_NAME', 'NOT SET')}")
print("=" * 50)

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
        print(f"✅ R2 ready for map layers")
    except Exception as e:
        print(f"⚠️ R2 init failed: {e}")


def get_public_url(key: str) -> str:
    if R2_CUSTOM_DOMAIN:
        return f"https://{R2_CUSTOM_DOMAIN}/{key}"
    return f"https://{R2_BUCKET_NAME}.r2.dev/{key}"


def csv_to_geojson(content_bytes: bytes):
    """Convert CSV to GeoJSON"""
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
            if not (4 <= lat_val <= 14) or not (2.5 <= lon_val <= 15):
                skipped += 1
                continue
        except ValueError:
            skipped += 1
            continue

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

    return json.dumps({"type": "FeatureCollection", "features": features}), len(features), skipped


# ==============================================
# DEFAULT LAYERS - THIS IS WHAT WAS MISSING!
# ==============================================

DEFAULT_LAYERS = [
    # Surface Water
    {"group_key": "surface_water", "layer_key": "stations", "name": "River Gauge Stations",
     "description": "358 NIHSA real-time river level monitoring stations", "icon": "📍",
     "layer_type": "toggle", "display_order": 1, "is_active": True, "default_visible": True},
    {"group_key": "surface_water", "layer_key": "alerts", "name": "Active Flood Alerts",
     "description": "Published flood warnings", "icon": "⚠️", "layer_type": "toggle",
     "display_order": 2, "is_active": True, "default_visible": True},
    {"group_key": "surface_water", "layer_key": "reports", "name": "Citizen Flood Reports",
     "description": "Verified field reports", "icon": "💧", "layer_type": "toggle",
     "display_order": 3, "is_active": True, "default_visible": False},

    # Annual Forecast
    {"group_key": "forecast", "layer_key": "fc_flood_extent", "name": "Flood Extent & Depth",
     "description": "Annual inundation extent", "icon": "💧", "layer_type": "geojson_fc",
     "display_order": 1, "is_active": True, "default_visible": False},
    {"group_key": "forecast", "layer_key": "fc_population", "name": "Population at Risk",
     "description": "People in flood zones", "icon": "👥", "layer_type": "geojson_fc",
     "display_order": 2, "is_active": True, "default_visible": False},
    {"group_key": "forecast", "layer_key": "fc_communities", "name": "Communities at Risk",
     "description": "Settlements exposed", "icon": "🏘️", "layer_type": "geojson_fc",
     "display_order": 3, "is_active": True, "default_visible": False},
    {"group_key": "forecast", "layer_key": "fc_health", "name": "Health Facilities at Risk",
     "description": "Clinics in flood zones", "icon": "🏥", "layer_type": "geojson_fc",
     "display_order": 4, "is_active": True, "default_visible": False},
    {"group_key": "forecast", "layer_key": "fc_schools", "name": "Schools at Risk",
     "description": "Schools in flood zones", "icon": "🏫", "layer_type": "geojson_fc",
     "display_order": 5, "is_active": True, "default_visible": False},
    {"group_key": "forecast", "layer_key": "fc_farmland", "name": "Farmland Exposure",
     "description": "Agricultural land at risk", "icon": "🌾", "layer_type": "geojson_fc",
     "display_order": 6, "is_active": True, "default_visible": False},
    {"group_key": "forecast", "layer_key": "fc_roads", "name": "Road Network at Risk",
     "description": "Roads vulnerable", "icon": "🛣️", "layer_type": "geojson_fc",
     "display_order": 7, "is_active": True, "default_visible": False},

    # Weekly Forecast
    {"group_key": "forecast_weekly", "layer_key": "fw_flood_extent", "name": "Weekly Flood Extent",
     "description": "Current week inundation", "icon": "💧", "layer_type": "geojson_fc",
     "display_order": 1, "is_active": True, "default_visible": False},
    {"group_key": "forecast_weekly", "layer_key": "fw_population", "name": "Weekly Population at Risk",
     "icon": "👥", "layer_type": "geojson_fc", "display_order": 2, "is_active": True, "default_visible": False},
    {"group_key": "forecast_weekly", "layer_key": "fw_communities", "name": "Weekly Communities at Risk",
     "icon": "🏘️", "layer_type": "geojson_fc", "display_order": 3, "is_active": True, "default_visible": False},
    {"group_key": "forecast_weekly", "layer_key": "fw_health", "name": "Weekly Health at Risk",
     "icon": "🏥", "layer_type": "geojson_fc", "display_order": 4, "is_active": True, "default_visible": False},
    {"group_key": "forecast_weekly", "layer_key": "fw_schools", "name": "Weekly Schools at Risk",
     "icon": "🏫", "layer_type": "geojson_fc", "display_order": 5, "is_active": True, "default_visible": False},
    {"group_key": "forecast_weekly", "layer_key": "fw_farmland", "name": "Weekly Farmland at Risk",
     "icon": "🌾", "layer_type": "geojson_fc", "display_order": 6, "is_active": True, "default_visible": False},
    {"group_key": "forecast_weekly", "layer_key": "fw_roads", "name": "Weekly Roads at Risk",
     "icon": "🛣️", "layer_type": "geojson_fc", "display_order": 7, "is_active": True, "default_visible": False},

    # Groundwater
    {"group_key": "groundwater", "layer_key": "gw_levels", "name": "Groundwater Levels",
     "icon": "🔵", "layer_type": "geojson_fc", "display_order": 1, "is_active": True, "default_visible": False},
    {"group_key": "groundwater", "layer_key": "gw_aquifer", "name": "Aquifer Zones",
     "icon": "🗺️", "layer_type": "geojson_fc", "display_order": 2, "is_active": True, "default_visible": False},
    {"group_key": "groundwater", "layer_key": "gw_recharge", "name": "Recharge Areas",
     "icon": "♻️", "layer_type": "geojson_fc", "display_order": 3, "is_active": True, "default_visible": False},

    # Water Quality
    {"group_key": "water_quality", "layer_key": "wq_index", "name": "Water Quality Index",
     "icon": "🧪", "layer_type": "geojson_fc", "display_order": 1, "is_active": True, "default_visible": False},
    {"group_key": "water_quality", "layer_key": "wq_turbidity", "name": "Turbidity",
     "icon": "🌊", "layer_type": "geojson_fc", "display_order": 2, "is_active": True, "default_visible": False},
    {"group_key": "water_quality", "layer_key": "wq_contamination", "name": "Contamination Risk",
     "icon": "⚗️", "layer_type": "geojson_fc", "display_order": 3, "is_active": True, "default_visible": False},

    # Coastal & Marine
    {"group_key": "coastal_marine", "layer_key": "cm_coastal_risk", "name": "Coastal Flood Risk",
     "icon": "🏖️", "layer_type": "geojson_fc", "display_order": 1, "is_active": True, "default_visible": False},
    {"group_key": "coastal_marine", "layer_key": "cm_storm_surge", "name": "Storm Surge Zones",
     "icon": "🌀", "layer_type": "geojson_fc", "display_order": 2, "is_active": True, "default_visible": False},
    {"group_key": "coastal_marine", "layer_key": "cm_erosion", "name": "Coastal Erosion",
     "icon": "⛰️", "layer_type": "geojson_fc", "display_order": 3, "is_active": True, "default_visible": False},
    {"group_key": "coastal_marine", "layer_key": "cm_mangrove", "name": "Mangrove Zones",
     "icon": "🌿", "layer_type": "geojson_fc", "display_order": 4, "is_active": True, "default_visible": False},
]


# ==============================================
# ENDPOINTS
# ==============================================

@router.get("", response_model=List[schemas.MapLayerOut])
def list_map_layers(db: Session = Depends(get_db)):
    """Public: all active layer definitions"""
    return db.query(models.MapLayer).filter(
        models.MapLayer.is_active == True
    ).order_by(models.MapLayer.group_key, models.MapLayer.display_order).all()


@router.get("/all", response_model=List[schemas.MapLayerOut])
def list_all_map_layers(
        db: Session = Depends(get_db),
        _user=Depends(require_role(models.UserRole.NIHSA_STAFF, models.UserRole.ADMIN, models.UserRole.SUB_ADMIN)),
):
    """Admin: all layers including inactive"""
    return db.query(models.MapLayer).order_by(
        models.MapLayer.group_key, models.MapLayer.display_order
    ).all()


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

    template = f"""# Template for {layer.name}
# Required columns: lat, lon
# Optional columns: name, state, lga, risk_zone, depth, population, etc.
# Coordinates must be within Nigeria (lat: 4-14, lon: 2.5-15)
lat,lon,name,state,lga
7.8069,6.7420,Sample Location,Kogi,Lokoja
8.5321,7.7462,Another Location,Benue,Makurdi
"""
    return Response(
        content=template,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{layer.layer_key}_template.csv"'},
    )


@router.post("/{layer_id}/upload", status_code=201)
async def upload_layer_data(
        layer_id: str,
        file: UploadFile = File(...),
        db: Session = Depends(get_db),
        _user=Depends(require_role(models.UserRole.NIHSA_STAFF, models.UserRole.ADMIN, models.UserRole.SUB_ADMIN)),
):
    
    print("=" * 60)
    print("🚨 UPLOAD FUNCTION WAS CALLED! 🚨")
    print(f"Layer ID: {layer_id}")
    print("=" * 60)
    
    """Upload CSV file for a map layer to R2"""

    layer = db.query(models.MapLayer).filter(models.MapLayer.id == layer_id).first()
    if not layer:
        raise HTTPException(status_code=404, detail="Layer not found")

    if not USE_R2 or not r2_client:
        raise HTTPException(status_code=503, detail="R2 storage not configured")

    content = await file.read()
    filename = file.filename or ""

    if not filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")

    # Convert CSV to GeoJSON
    geojson_str, feature_count, skipped = csv_to_geojson(content)

    if feature_count == 0:
        raise HTTPException(status_code=400, detail="No valid features found. Check lat/lon columns.")

    # Upload to R2
    file_key = f"map-layers/{layer.layer_key}.geojson"
    r2_client.put_object(
        Bucket=R2_BUCKET_NAME,
        Key=file_key,
        Body=geojson_str.encode('utf-8'),
        ContentType="application/geo+json"
    )

    public_url = get_public_url(file_key)

    # Update database
    layer.source_url = public_url
    layer.meta = {
        "feature_count": feature_count,
        "rows_skipped": skipped,
        "uploaded_at": datetime.now(timezone.utc).isoformat(),
        "filename": filename
    }
    db.commit()

    return {
        "success": True,
        "feature_count": feature_count,
        "rows_skipped": skipped,
        "size_kb": round(len(geojson_str) / 1024, 1),
        "public_url": public_url,
        "message": f"Successfully uploaded {feature_count:,} features"
    }


@router.post("/seed", status_code=201)
def seed_map_layers(
        db: Session = Depends(get_db),
        _user=Depends(require_role(models.UserRole.ADMIN)),
):
    """Seed default layers into database"""
    added = 0
    for d in DEFAULT_LAYERS:
        exists = db.query(models.MapLayer).filter(
            models.MapLayer.layer_key == d["layer_key"]
        ).first()
        if not exists:
            layer = models.MapLayer(
                id=str(uuid.uuid4()),
                **d
            )
            db.add(layer)
            added += 1
    db.commit()
    return {"seeded": added, "total": len(DEFAULT_LAYERS)}
