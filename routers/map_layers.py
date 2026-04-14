"""map_layers.py - Complete working version with R2 storage + Shapefile support"""

import os
import json
import csv
import io
import uuid
import zipfile
import tempfile
import shapefile
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from fastapi.responses import Response
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timezone
from pydantic import BaseModel
from database import get_db
import models, schemas
from auth_utils import require_role

router = APIRouter()

@router.post("/test-upload")
async def test_upload_only(file: UploadFile = File(...)):
    content = await file.read()
    return {"success": True, "filename": file.filename, "size_bytes": len(content)}

@router.get("/ping")
def ping():
    return {"status": "alive", "message": "Map layers router is working!"}

# ── R2 Configuration ───────────────────────────────────────────────────────────
R2_ACCOUNT_ID  = os.getenv("R2_ACCOUNT_ID", "")
R2_ACCESS_KEY  = os.getenv("R2_ACCESS_KEY_ID", "")
R2_SECRET_KEY  = os.getenv("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "nihsamedia")
R2_CUSTOM_DOMAIN = os.getenv("R2_CUSTOM_DOMAIN", "")
USE_R2 = os.getenv("USE_R2", "false").lower() == "true"

print("=" * 50)
print(f"USE_R2 = {os.getenv('USE_R2', 'NOT SET')}")
print(f"R2_ACCOUNT_ID = {R2_ACCOUNT_ID[:10] if R2_ACCOUNT_ID else 'NOT SET'}...")
print(f"R2_ACCESS_KEY_ID = {'SET' if R2_ACCESS_KEY else 'NOT SET'}")
print(f"R2_SECRET_ACCESS_KEY = {'SET' if R2_SECRET_KEY else 'NOT SET'}")
print(f"R2_BUCKET_NAME = {R2_BUCKET_NAME}")
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
        print("✅ R2 client initialized for bucket:", R2_BUCKET_NAME)
        print("✅ R2 ready for map layers")
    except Exception as e:
        print(f"⚠️ R2 init failed: {e}")


def get_public_url(key: str) -> str:
    if R2_CUSTOM_DOMAIN:
        return f"https://{R2_CUSTOM_DOMAIN}/{key}"
    return f"https://{R2_BUCKET_NAME}.r2.dev/{key}"


# ── CSV → GeoJSON ──────────────────────────────────────────────────────────────
def csv_to_geojson(content_bytes: bytes):
    text = content_bytes.decode("utf-8", errors="ignore").lstrip("\ufeff")
    reader = csv.DictReader(io.StringIO(text))
    features, skipped = [], 0
    for row in reader:
        norm = {k.strip().lower(): v.strip() for k, v in row.items()}
        lat = norm.get("lat") or norm.get("latitude")
        lon = norm.get("lon") or norm.get("longitude") or norm.get("lng")
        if not lat or not lon:
            skipped += 1; continue
        try:
            lat_val, lon_val = float(lat), float(lon)
            if not (4 <= lat_val <= 14) or not (2.5 <= lon_val <= 15):
                skipped += 1; continue
        except ValueError:
            skipped += 1; continue
        props = {}
        for k, v in norm.items():
            if k not in ["lat","latitude","lon","longitude","lng"] and v:
                try:    props[k] = float(v) if "." in v else int(v)
                except: props[k] = v
        features.append({"type":"Feature","geometry":{"type":"Point","coordinates":[lon_val,lat_val]},"properties":props})
    return json.dumps({"type":"FeatureCollection","features":features}), len(features), skipped


# ── Shapefile ZIP → GeoJSON ────────────────────────────────────────────────────
def shp_to_geojson(content_bytes: bytes):
    """
    Convert a Shapefile ZIP (.shp + .shx + .dbf + optional .prj) to GeoJSON.
    Handles both flat ZIPs and ZIPs with a single subfolder.
    Supports: Point, Polyline, Polygon, MultiPoint (and their Z/M variants).
    Returns: (geojson_string, feature_count, skipped_count)
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, 'upload.zip')
        with open(zip_path, 'wb') as f:
            f.write(content_bytes)

        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(tmpdir)

        # Find .shp recursively — handles subfolders
        shp_files = []
        for root, dirs, files in os.walk(tmpdir):
            for fname in files:
                if fname.lower().endswith('.shp'):
                    shp_files.append(os.path.join(root, fname))

        if not shp_files:
            raise ValueError("No .shp file found. ZIP must contain .shp, .shx, and .dbf files at root or one subfolder deep.")

        shp_path = shp_files[0]

        try:
            sf = shapefile.Reader(shp_path, encoding='utf-8')
        except Exception:
            sf = shapefile.Reader(shp_path, encoding='latin1')

        fields = [f[0] for f in sf.fields[1:]]
        features, skipped = [], 0

        for shape_record in sf.iterShapeRecords():
            shape  = shape_record.shape
            record = shape_record.record

            props = {}
            for j, field_name in enumerate(fields):
                val = record[j]
                if val is not None and val != '':
                    if isinstance(val, bytes):
                        val = val.decode('utf-8', errors='ignore')
                    props[field_name] = val

            geom = None
            st = shape.shapeType

            if st in (1, 11, 21):   # Point / PointZ / PointM
                if shape.points:
                    geom = {"type":"Point","coordinates":[shape.points[0][0], shape.points[0][1]]}

            elif st in (3, 13, 23): # PolyLine / PolyLineZ / PolyLineM
                parts = list(shape.parts) + [len(shape.points)]
                lines = [[[p[0],p[1]] for p in shape.points[parts[j]:parts[j+1]]]
                         for j in range(len(shape.parts))]
                geom = {"type":"LineString","coordinates":lines[0]} if len(lines)==1 \
                    else {"type":"MultiLineString","coordinates":lines}

            elif st in (5, 15, 25): # Polygon / PolygonZ / PolygonM
                parts = list(shape.parts) + [len(shape.points)]
                rings = [[[p[0],p[1]] for p in shape.points[parts[j]:parts[j+1]]]
                         for j in range(len(shape.parts))]
                geom = {"type":"Polygon","coordinates":rings} if len(rings)==1 \
                    else {"type":"MultiPolygon","coordinates":[[r] for r in rings]}

            elif st in (8, 18, 28): # MultiPoint / MultiPointZ / MultiPointM
                geom = {"type":"MultiPoint","coordinates":[[p[0],p[1]] for p in shape.points]}

            if geom and shape.points:
                features.append({"type":"Feature","geometry":geom,"properties":props})
            else:
                skipped += 1

        return json.dumps({"type":"FeatureCollection","features":features}), len(features), skipped


# ── Default layer definitions ──────────────────────────────────────────────────
DEFAULT_LAYERS = [
    {"group_key":"surface_water","layer_key":"stations","name":"River Gauge Stations",
     "description":"358 NIHSA real-time river level monitoring stations","icon":"📍",
     "layer_type":"toggle","display_order":1,"is_active":True,"default_visible":True},
    {"group_key":"surface_water","layer_key":"alerts","name":"Active Flood Alerts",
     "description":"Published flood warnings","icon":"⚠️","layer_type":"toggle",
     "display_order":2,"is_active":True,"default_visible":True},
    {"group_key":"surface_water","layer_key":"reports","name":"Citizen Flood Reports",
     "description":"Verified field reports","icon":"💧","layer_type":"toggle",
     "display_order":3,"is_active":True,"default_visible":False},
    {"group_key":"forecast","layer_key":"fc_flood_extent","name":"Flood Extent & Depth",
     "description":"Annual inundation extent","icon":"💧","layer_type":"geojson_fc",
     "display_order":1,"is_active":True,"default_visible":False},
    {"group_key":"forecast","layer_key":"fc_population","name":"Population at Risk",
     "description":"People in flood zones","icon":"👥","layer_type":"geojson_fc",
     "display_order":2,"is_active":True,"default_visible":False},
    {"group_key":"forecast","layer_key":"fc_communities","name":"Communities at Risk",
     "description":"Settlements exposed","icon":"🏘️","layer_type":"geojson_fc",
     "display_order":3,"is_active":True,"default_visible":False},
    {"group_key":"forecast","layer_key":"fc_health","name":"Health Facilities at Risk",
     "description":"Clinics in flood zones","icon":"🏥","layer_type":"geojson_fc",
     "display_order":4,"is_active":True,"default_visible":False},
    {"group_key":"forecast","layer_key":"fc_schools","name":"Schools at Risk",
     "description":"Schools in flood zones","icon":"🏫","layer_type":"geojson_fc",
     "display_order":5,"is_active":True,"default_visible":False},
    {"group_key":"forecast","layer_key":"fc_farmland","name":"Farmland Exposure",
     "description":"Agricultural land at risk","icon":"🌾","layer_type":"geojson_fc",
     "display_order":6,"is_active":True,"default_visible":False},
    {"group_key":"forecast","layer_key":"fc_roads","name":"Road Network at Risk",
     "description":"Roads vulnerable","icon":"🛣️","layer_type":"geojson_fc",
     "display_order":7,"is_active":True,"default_visible":False},
    {"group_key":"forecast_weekly","layer_key":"fw_flood_extent","name":"Weekly Flood Extent",
     "description":"Current week inundation","icon":"💧","layer_type":"geojson_fc",
     "display_order":1,"is_active":True,"default_visible":False},
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
    {"group_key":"groundwater","layer_key":"gw_levels","name":"Groundwater Levels",
     "icon":"🔵","layer_type":"geojson_fc","display_order":1,"is_active":True,"default_visible":False},
    {"group_key":"groundwater","layer_key":"gw_aquifer","name":"Aquifer Zones",
     "icon":"🗺️","layer_type":"geojson_fc","display_order":2,"is_active":True,"default_visible":False},
    {"group_key":"groundwater","layer_key":"gw_recharge","name":"Recharge Areas",
     "icon":"♻️","layer_type":"geojson_fc","display_order":3,"is_active":True,"default_visible":False},
    {"group_key":"water_quality","layer_key":"wq_index","name":"Water Quality Index",
     "icon":"🧪","layer_type":"geojson_fc","display_order":1,"is_active":True,"default_visible":False},
    {"group_key":"water_quality","layer_key":"wq_turbidity","name":"Turbidity",
     "icon":"🌊","layer_type":"geojson_fc","display_order":2,"is_active":True,"default_visible":False},
    {"group_key":"water_quality","layer_key":"wq_contamination","name":"Contamination Risk",
     "icon":"⚗️","layer_type":"geojson_fc","display_order":3,"is_active":True,"default_visible":False},
    {"group_key":"coastal_marine","layer_key":"cm_coastal_risk","name":"Coastal Flood Risk",
     "icon":"🏖️","layer_type":"geojson_fc","display_order":1,"is_active":True,"default_visible":False},
    {"group_key":"coastal_marine","layer_key":"cm_storm_surge","name":"Storm Surge Zones",
     "icon":"🌀","layer_type":"geojson_fc","display_order":2,"is_active":True,"default_visible":False},
    {"group_key":"coastal_marine","layer_key":"cm_erosion","name":"Coastal Erosion",
     "icon":"⛰️","layer_type":"geojson_fc","display_order":3,"is_active":True,"default_visible":False},
    {"group_key":"coastal_marine","layer_key":"cm_mangrove","name":"Mangrove Zones",
     "icon":"🌿","layer_type":"geojson_fc","display_order":4,"is_active":True,"default_visible":False},
]


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.get("", response_model=List[schemas.MapLayerOut])
def list_map_layers(db: Session = Depends(get_db)):
    return db.query(models.MapLayer).filter(
        models.MapLayer.is_active == True
    ).order_by(models.MapLayer.group_key, models.MapLayer.display_order).all()


@router.get("/all", response_model=List[schemas.MapLayerOut])
def list_all_map_layers(
    db: Session = Depends(get_db),
    _user=Depends(require_role(models.UserRole.NIHSA_STAFF, models.UserRole.ADMIN, models.UserRole.SUB_ADMIN)),
):
    return db.query(models.MapLayer).order_by(
        models.MapLayer.group_key, models.MapLayer.display_order
    ).all()


@router.get("/{layer_id}/template")
def download_csv_template(
    layer_id: str,
    db: Session = Depends(get_db),
    _user=Depends(require_role(models.UserRole.NIHSA_STAFF, models.UserRole.ADMIN, models.UserRole.SUB_ADMIN)),
):
    layer = db.query(models.MapLayer).filter(models.MapLayer.id == layer_id).first()
    if not layer:
        raise HTTPException(status_code=404, detail="Layer not found")
    template = f"# Template for {layer.name}\n# Required: lat, lon\nlat,lon,name,state,lga\n7.8069,6.7420,Sample Location,Kogi,Lokoja\n8.5321,7.7462,Another Location,Benue,Makurdi\n"
    return Response(content=template, media_type="text/csv",
                    headers={"Content-Disposition": f'attachment; filename="{layer.layer_key}_template.csv"'})


class FileUploadRequest(BaseModel):
    filename: str
    content: str
    encoding: str = 'utf8'   # 'utf8' for CSV, 'base64' for ZIP


@router.post("/{layer_id}/upload", status_code=201)
async def upload_layer_data(
    layer_id: str,
    request: FileUploadRequest,
    db: Session = Depends(get_db),
):
    import base64 as _b64

    filename = request.filename.lower()

    layer = db.query(models.MapLayer).filter(models.MapLayer.id == layer_id).first()
    if not layer:
        raise HTTPException(status_code=404, detail="Layer not found")

    if not USE_R2 or not r2_client:
        raise HTTPException(status_code=503, detail="R2 storage not configured")

    # Decode: ZIP → base64 binary, CSV → UTF-8 text
    if request.encoding == 'base64' or filename.endswith('.zip'):
        try:
            content = _b64.b64decode(request.content)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid base64 content for ZIP file")
    else:
        content = request.content.encode('utf-8')

    # Convert to GeoJSON
    if filename.endswith('.csv'):
        geojson_str, feature_count, skipped = csv_to_geojson(content)
        source_type = 'csv'
    elif filename.endswith('.zip'):
        try:
            geojson_str, feature_count, skipped = shp_to_geojson(content)
            source_type = 'shapefile'
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Shapefile processing error: {str(e)}")
    else:
        raise HTTPException(status_code=400, detail="Only CSV (.csv) and Shapefile ZIP (.zip) are supported")

    if feature_count == 0:
        raise HTTPException(status_code=400, detail="No valid features found. Check file format and coordinates.")

    # Upload to R2
    file_key = f"map-layers/{layer.layer_key}.geojson"
    r2_client.put_object(
        Bucket=R2_BUCKET_NAME,
        Key=file_key,
        Body=geojson_str.encode('utf-8'),
        ContentType="application/geo+json"
    )

    public_url = get_public_url(file_key)

    layer.source_url = public_url
    layer.meta = {
        "feature_count": feature_count,
        "rows_skipped": skipped,
        "uploaded_at": datetime.now(timezone.utc).isoformat(),
        "filename": request.filename,
        "source_type": source_type,
        "file_size_kb": round(len(request.content) / 1024, 1),
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


@router.put("/{layer_id}", response_model=schemas.MapLayerOut)
@router.patch("/{layer_id}", response_model=schemas.MapLayerOut)
def update_map_layer(
    layer_id: str,
    body: schemas.MapLayerUpdate,
    db: Session = Depends(get_db),
    _user=Depends(require_role(models.UserRole.NIHSA_STAFF, models.UserRole.ADMIN, models.UserRole.SUB_ADMIN)),
):
    layer = db.query(models.MapLayer).filter(models.MapLayer.id == layer_id).first()
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
    layer = db.query(models.MapLayer).filter(models.MapLayer.id == layer_id).first()
    if not layer:
        raise HTTPException(status_code=404, detail="Layer not found")
    db.delete(layer)
    db.commit()


@router.post("", response_model=schemas.MapLayerOut, status_code=201)
def create_map_layer(
    body: schemas.MapLayerCreate,
    db: Session = Depends(get_db),
    _user=Depends(require_role(models.UserRole.NIHSA_STAFF, models.UserRole.ADMIN, models.UserRole.SUB_ADMIN)),
):
    if db.query(models.MapLayer).filter(models.MapLayer.layer_key == body.layer_key).first():
        raise HTTPException(status_code=409, detail=f"layer_key '{body.layer_key}' already exists")
    layer = models.MapLayer(id=str(uuid.uuid4()), **body.model_dump())
    db.add(layer)
    db.commit()
    db.refresh(layer)
    return layer


@router.post("/seed", status_code=201)
def seed_map_layers(
    db: Session = Depends(get_db),
    _user=Depends(require_role(models.UserRole.ADMIN)),
):
    added = 0
    for d in DEFAULT_LAYERS:
        if not db.query(models.MapLayer).filter(models.MapLayer.layer_key == d["layer_key"]).first():
            db.add(models.MapLayer(id=str(uuid.uuid4()), **d))
            added += 1
    db.commit()
    return {"seeded": added, "total": len(DEFAULT_LAYERS)}
