"""Simple Map Layers Router - Uses same pattern as reports"""

import os
import json
import csv
import io
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from fastapi.responses import Response
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime
import boto3
from botocore.config import Config

from database import get_db
import models, schemas
from auth_utils import require_role

router = APIRouter()

# R2 Configuration (same as reports system)
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY_ID", "")
R2_SECRET_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "nihsamedia")
USE_R2 = os.getenv("USE_R2", "false").lower() == "true"

# Initialize R2 client (same as reports)
r2_client = None
if USE_R2 and R2_ACCOUNT_ID and R2_ACCESS_KEY and R2_SECRET_KEY:
    r2_client = boto3.client(
        's3',
        endpoint_url=f'https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com',
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='auto'
    )
    print(f"✅ R2 ready for map layers")

# Simple CSV to GeoJSON converter
def csv_to_geojson(content_bytes: bytes):
    text = content_bytes.decode("utf-8", errors="ignore").lstrip("\ufeff")
    reader = csv.DictReader(io.StringIO(text))
    
    features = []
    for row in reader:
        lat = row.get('lat') or row.get('latitude')
        lon = row.get('lon') or row.get('longitude') or row.get('lng')
        
        if not lat or not lon:
            continue
        
        try:
            lat_val = float(lat)
            lon_val = float(lon)
            if not (4 <= lat_val <= 14) or not (2.5 <= lon_val <= 15):
                continue
        except:
            continue
        
        props = {k: v for k, v in row.items() if k not in ['lat', 'latitude', 'lon', 'longitude', 'lng']}
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon_val, lat_val]},
            "properties": props
        })
    
    return json.dumps({"type": "FeatureCollection", "features": features}), len(features)


@router.post("/{layer_id}/upload")
async def upload_layer_file(
    layer_id: str,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: models.User = Depends(require_role(models.UserRole.NIHSA_STAFF, models.UserRole.ADMIN))
):
    """Upload CSV/GeoJSON for a map layer - SAME PATTERN AS REPORTS"""
    
    # Get the layer
    layer = db.query(models.MapLayer).filter(models.MapLayer.id == layer_id).first()
    if not layer:
        raise HTTPException(404, "Layer not found")
    
    # Read file
    content = await file.read()
    filename = file.filename or ""
    
    # Convert if CSV
    if filename.endswith('.csv'):
        geojson_str, feature_count = csv_to_geojson(content)
        content = geojson_str.encode('utf-8')
        content_type = "application/geo+json"
    else:
        feature_count = len(json.loads(content.decode()).get('features', []))
        content_type = "application/geo+json"
    
    # Generate R2 key (same pattern as reports)
    file_key = f"map-layers/{layer.layer_key}.geojson"
    
    # Upload to R2 (same as reports)
    if USE_R2 and r2_client:
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=file_key,
            Body=content,
            ContentType=content_type
        )
        public_url = f"https://{R2_BUCKET_NAME}.r2.dev/{file_key}"
    else:
        # Fallback to local storage
        os.makedirs("map-layers", exist_ok=True)
        with open(f"map-layers/{layer.layer_key}.geojson", "wb") as f:
            f.write(content)
        public_url = f"/map-layers/{layer.layer_key}.geojson"
    
    # Save URL to database (SAME AS REPORTS!)
    layer.source_url = public_url
    layer.meta = {
        "feature_count": feature_count,
        "uploaded_at": datetime.utcnow().isoformat(),
        "filename": filename
    }
    db.commit()
    
    return {
        "success": True,
        "feature_count": feature_count,
        "url": public_url,
        "message": f"Uploaded {feature_count} features"
    }


@router.get("/{layer_id}/template")
def download_template(layer_id: str, db: Session = Depends(get_db)):
    """Download CSV template"""
    layer = db.query(models.MapLayer).filter(models.MapLayer.id == layer_id).first()
    if not layer:
        raise HTTPException(404, "Layer not found")
    
    template = f"""# Template for {layer.name}
# Required columns: lat, lon
# Optional columns: name, state, lga, risk_zone, depth, population, etc.
lat,lon,name,state,lga
7.8069,6.7420,Sample Location,Kogi,Lokoja
8.5321,7.7462,Another Location,Benue,Makurdi
"""
    return Response(content=template, media_type="text/csv")


@router.get("", response_model=List[schemas.MapLayerOut])
def list_layers(db: Session = Depends(get_db)):
    """List all layers with their source_url from database"""
    return db.query(models.MapLayer).filter(models.MapLayer.is_active == True).all()
