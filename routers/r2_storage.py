"""r2_storage.py - Cloudflare R2 storage for map layers"""

import os
import json
import csv
import io
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from typing import Optional, Dict, Any, Tuple
from pathlib import Path
import logging

logger = logging.getLogger("nihsa.r2_storage")

# R2 Configuration
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY_ID", "")
R2_SECRET_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "nihsamedia")
R2_CUSTOM_DOMAIN = os.getenv("R2_CUSTOM_DOMAIN", "")
USE_R2 = os.getenv("USE_R2", "true").lower() == "true"

# Directory structure mapping layer_key → R2 folder path
LAYER_TO_FOLDER = {
    # Annual Forecast
    "fc_flood_extent": "forecast/flood_extent.geojson",
    "fc_population": "forecast/population.geojson",
    "fc_communities": "forecast/communities.geojson",
    "fc_health": "forecast/health.geojson",
    "fc_schools": "forecast/schools.geojson",
    "fc_farmland": "forecast/farmland.geojson",
    "fc_roads": "forecast/roads.geojson",
    
    # Weekly Forecast
    "fw_flood_extent": "forecast_weekly/flood_extent.geojson",
    "fw_population": "forecast_weekly/population.geojson",
    "fw_communities": "forecast_weekly/communities.geojson",
    "fw_health": "forecast_weekly/health.geojson",
    "fw_schools": "forecast_weekly/schools.geojson",
    "fw_farmland": "forecast_weekly/farmland.geojson",
    "fw_roads": "forecast_weekly/roads.geojson",
    
    # Surface Water
    "sw_satellite": "surface_water/satellite.geojson",
    "sw_station_updates": "surface_water/station_updates.geojson",
    
    # Groundwater
    "gw_levels": "groundwater/levels.geojson",
    "gw_aquifer": "groundwater/aquifer.geojson",
    "gw_recharge": "groundwater/recharge.geojson",
    
    # Water Quality
    "wq_index": "water_quality/index.geojson",
    "wq_turbidity": "water_quality/turbidity.geojson",
    "wq_contamination": "water_quality/contamination.geojson",
    
    # Coastal & Marine
    "cm_coastal_risk": "coastal_marine/coastal_risk.geojson",
    "cm_storm_surge": "coastal_marine/storm_surge.geojson",
    "cm_erosion": "coastal_marine/erosion.geojson",
    "cm_mangrove": "coastal_marine/mangrove.geojson",
}

# Reverse mapping for URL generation
FOLDER_TO_LAYER = {v: k for k, v in LAYER_TO_FOLDER.items()}

# Initialize R2 client
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
    logger.info(f"✅ R2 client initialized for bucket: {R2_BUCKET_NAME}")
else:
    logger.warning("⚠️ R2 not configured - map layer uploads will not work")


def ensure_bucket_exists():
    """Create bucket if it doesn't exist"""
    if not r2_client:
        return False
    try:
        r2_client.head_bucket(Bucket=R2_BUCKET_NAME)
        logger.info(f"Bucket {R2_BUCKET_NAME} exists")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            try:
                r2_client.create_bucket(Bucket=R2_BUCKET_NAME)
                logger.info(f"Created bucket: {R2_BUCKET_NAME}")
            except Exception as create_err:
                logger.error(f"Failed to create bucket: {create_err}")
                return False
        else:
            logger.error(f"Bucket error: {e}")
            return False
    return True


def get_public_url(key: str) -> str:
    """Get public URL for a file in R2"""
    if R2_CUSTOM_DOMAIN:
        return f"https://{R2_CUSTOM_DOMAIN}/{key}"
    return f"https://{R2_BUCKET_NAME}.r2.dev/{key}"


def csv_to_geojson(csv_content: bytes, layer_key: str) -> Tuple[Dict, int, int]:
    """
    Convert CSV to GeoJSON FeatureCollection.
    Returns (geojson_dict, feature_count, skipped_rows)
    """
    text = csv_content.decode("utf-8", errors="ignore").lstrip("\ufeff")
    reader = csv.DictReader(io.StringIO(text))
    
    features = []
    skipped = 0
    
    for row_num, row in enumerate(reader, start=2):
        # Normalize keys (case-insensitive)
        norm = {k.strip().lower(): v.strip() for k, v in row.items()}
        
        # Find lat/lon (accept various column names)
        lat_raw = norm.get("lat") or norm.get("latitude")
        lon_raw = norm.get("lon") or norm.get("longitude") or norm.get("lng")
        
        if not lat_raw or not lon_raw:
            skipped += 1
            continue
            
        try:
            lat = float(lat_raw)
            lon = float(lon_raw)
        except ValueError:
            skipped += 1
            continue
        
        # Validate Nigeria bounds
        if not (4.0 <= lat <= 14.0) or not (2.5 <= lon <= 15.0):
            skipped += 1
            continue
        
        # Build properties (exclude coordinate columns)
        exclude = {"lat", "latitude", "lon", "longitude", "lng"}
        properties = {k: v for k, v in norm.items() if k not in exclude and v}
        
        # Convert numeric strings to numbers where possible
        for k, v in properties.items():
            try:
                if "." in v:
                    properties[k] = float(v)
                else:
                    properties[k] = int(v)
            except (ValueError, TypeError):
                pass
        
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat]
            },
            "properties": properties
        })
    
    geojson = {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "layer_key": layer_key,
            "feature_count": len(features),
            "generated_at": None  # Will be set by caller
        }
    }
    
    return geojson, len(features), skipped


def validate_geojson(content: bytes) -> Tuple[Dict, int, int]:
    """
    Validate and parse GeoJSON file.
    Returns (geojson_dict, feature_count, 0)
    """
    try:
        data = json.loads(content.decode("utf-8"))
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid GeoJSON: {e}")
    
    if data.get("type") != "FeatureCollection":
        raise ValueError("GeoJSON must be a FeatureCollection")
    
    features = data.get("features", [])
    feature_count = len(features)
    
    return data, feature_count, 0


def upload_layer_file(layer_key: str, file_content: bytes, filename: str) -> Dict[str, Any]:
    """
    Upload a file (CSV or GeoJSON) for a map layer to R2.
    Overwrites existing file if it exists.
    
    Returns metadata about the upload.
    """
    if not r2_client:
        raise Exception("R2 storage is not configured. Set USE_R2=true and R2 credentials.")
    
    # Get the R2 key (path) for this layer
    r2_key = LAYER_TO_FOLDER.get(layer_key)
    if not r2_key:
        raise ValueError(f"Unknown layer_key: {layer_key}. No storage path configured.")
    
    # Ensure bucket exists
    ensure_bucket_exists()
    
    # Determine file type and convert if needed
    is_geojson = filename.lower().endswith('.geojson') or filename.lower().endswith('.json')
    is_csv = filename.lower().endswith('.csv')
    
    if not (is_geojson or is_csv):
        raise ValueError(f"Unsupported file type: {filename}. Use .geojson, .json, or .csv")
    
    # Process the file
    if is_geojson:
        geojson_data, feature_count, skipped = validate_geojson(file_content)
        final_content = json.dumps(geojson_data, ensure_ascii=False).encode('utf-8')
        content_type = "application/geo+json"
    else:  # CSV
        geojson_data, feature_count, skipped = csv_to_geojson(file_content, layer_key)
        final_content = json.dumps(geojson_data, ensure_ascii=False).encode('utf-8')
        content_type = "application/geo+json"
    
    if feature_count == 0:
        raise ValueError(f"No valid features found in {filename}. Check your data format.")
    
    # Upload to R2 (overwrites if exists)
    try:
        r2_client.put_object(
            Bucket=R2_BUCKET_NAME,
            Key=r2_key,
            Body=final_content,
            ContentType=content_type,
            Metadata={
                'original_filename': filename,
                'layer_key': layer_key,
                'feature_count': str(feature_count),
                'skipped_rows': str(skipped),
                'uploaded_at': None  # Will be set in DB
            }
        )
        logger.info(f"Uploaded {r2_key} to R2 bucket {R2_BUCKET_NAME}")
    except Exception as e:
        logger.error(f"R2 upload failed: {e}")
        raise Exception(f"Failed to upload to R2: {str(e)}")
    
    # Generate public URL
    public_url = get_public_url(r2_key)
    
    return {
        "layer_key": layer_key,
        "r2_key": r2_key,
        "public_url": public_url,
        "feature_count": feature_count,
        "rows_skipped": skipped,
        "file_size_bytes": len(final_content),
        "original_filename": filename
    }


def get_layer_file(layer_key: str) -> Optional[Dict]:
    """
    Retrieve a layer file from R2.
    Returns the GeoJSON data and metadata, or None if not found.
    """
    if not r2_client:
        return None
    
    r2_key = LAYER_TO_FOLDER.get(layer_key)
    if not r2_key:
        return None
    
    try:
        response = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=r2_key)
        content = response['Body'].read()
        geojson_data = json.loads(content.decode('utf-8'))
        
        return {
            "exists": True,
            "geojson": geojson_data,
            "metadata": response.get('Metadata', {}),
            "last_modified": response.get('LastModified'),
            "url": get_public_url(r2_key)
        }
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return None
        logger.error(f"R2 get error: {e}")
        return None


def delete_layer_file(layer_key: str) -> bool:
    """Delete a layer file from R2"""
    if not r2_client:
        return False
    
    r2_key = LAYER_TO_FOLDER.get(layer_key)
    if not r2_key:
        return False
    
    try:
        r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=r2_key)
        logger.info(f"Deleted {r2_key} from R2")
        return True
    except Exception as e:
        logger.error(f"R2 delete error: {e}")
        return False


def list_all_layer_files() -> Dict[str, Dict]:
    """List all map layer files currently in R2"""
    if not r2_client:
        return {}
    
    result = {}
    try:
        # List all objects in the bucket
        paginator = r2_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=R2_BUCKET_NAME):
            for obj in page.get('Contents', []):
                key = obj['Key']
                # Find which layer this belongs to
                if key in FOLDER_TO_LAYER:
                    layer_key = FOLDER_TO_LAYER[key]
                    result[layer_key] = {
                        "r2_key": key,
                        "size_bytes": obj['Size'],
                        "last_modified": obj['LastModified'],
                        "url": get_public_url(key)
                    }
    except Exception as e:
        logger.error(f"R2 list error: {e}")
    
    return result
