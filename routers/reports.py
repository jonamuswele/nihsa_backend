"""
Reports Router — Citizen / Vanguard Flood Reports
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timedelta

from pydantic import BaseModel

from database import get_db
import models, schemas
from auth_utils import get_current_user, get_current_user_optional, require_government

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from urllib.parse import urlparse

router = APIRouter()

R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY_ID", "")
R2_SECRET_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "")
R2_CUSTOM_DOMAIN = os.getenv("R2_CUSTOM_DOMAIN", "")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "nihsa-flood-reports")
USE_R2 = os.getenv("USE_R2", "false").lower() == "true"


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
    print(f"✅ R2 client initialized for bucket: {R2_BUCKET_NAME}")
else:
    print("⚠️ R2 not configured - using local storage")

@router.get("", response_model=List[schemas.FloodReportOut])
def list_reports(
    state: Optional[str] = None,
    verified_only: bool = False,
    status: Optional[str] = None,
    hours: int = Query(720, le=8760),
    limit: int = Query(100, le=500),
    offset: int = 0,
    db: Session = Depends(get_db),
):
    since = datetime.utcnow() - timedelta(hours=hours)
    q = (
        db.query(models.FloodReport)
        .filter(models.FloodReport.submitted_at >= since)
    )
    if state:
        q = q.filter(models.FloodReport.state.ilike(f"%{state}%"))
    if verified_only:
        q = q.filter(models.FloodReport.status == models.VerificationStatus.VERIFIED)
    if status and status.upper() != 'ALL':
        try:
            q = q.filter(models.FloodReport.status == models.VerificationStatus[status.upper()])
        except KeyError:
            pass
    return q.order_by(models.FloodReport.submitted_at.desc()).offset(offset).limit(limit).all()


@router.post("", response_model=schemas.FloodReportOut, status_code=201)
def create_report(
    body: schemas.FloodReportCreate,
    db: Session = Depends(get_db),
    current_user: Optional[models.User] = Depends(get_current_user_optional),
):
    report = models.FloodReport(
        lat=body.lat,
        lng=body.lng,
        state=body.state,
        lga=body.lga,
        address=body.address,
        description=body.description,
        water_depth_m=body.water_depth_m,
        media_urls=body.media_urls or [],
        user_id=current_user.id if current_user else None,
    )
    db.add(report)
    db.commit()
    db.refresh(report)
    return report


@router.get("/{report_id}", response_model=schemas.FloodReportOut)
def get_report(report_id: str, db: Session = Depends(get_db)):
    r = db.query(models.FloodReport).filter(models.FloodReport.id == report_id).first()
    if not r:
        raise HTTPException(status_code=404, detail="Report not found")
    return r


@router.patch("/{report_id}/verify", response_model=schemas.FloodReportOut)
def verify_report(
    report_id: str,
    body: schemas.FloodReportVerify,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(require_government),
):
    report = db.query(models.FloodReport).filter(models.FloodReport.id == report_id).first()
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")

    prev_status = report.status
    report.status = body.status
    report.verified_by = current_user.id
    report.verified_at = datetime.utcnow()
    if body.rejection_reason:
        report.rejection_reason = body.rejection_reason
    if body.risk_level:
        report.risk_level = body.risk_level

    # If rejected, delete media files from R2
    if body.status == models.VerificationStatus.REJECTED and prev_status != models.VerificationStatus.REJECTED:
        if report.media_urls and USE_R2 and r2_client:
            for media_url in report.media_urls:
                try:
                    parsed = urlparse(media_url)
                    file_key = parsed.path.lstrip('/')
                    r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=file_key)
                    print(f"Deleted from R2: {file_key}")
                except Exception as e:
                    print(f"Failed to delete {media_url}: {e}")

    # ── Auto-create a public alert when a report is verified ──────────────────
    if (body.status == models.VerificationStatus.VERIFIED
            and prev_status != models.VerificationStatus.VERIFIED):
        _level_map = {
            "CRITICAL": models.RiskLevel.CRITICAL,
            "HIGH":     models.RiskLevel.HIGH,
            "MEDIUM":   models.RiskLevel.MEDIUM,
            "WATCH":    models.RiskLevel.WATCH,
            "NORMAL":   models.RiskLevel.NORMAL,
        }
        report_level = (report.risk_level or "MEDIUM")
        alert_level = _level_map.get(
            report_level.upper() if hasattr(report_level, "upper") else report_level.value.upper(),
            models.RiskLevel.MEDIUM,
        )
        location_label = (
            report.address
            or (f"{report.state}, {report.lga}" if report.state and report.lga else None)
            or report.state
            or "Unknown Location"
        )
        alert = models.FloodAlert(
            title=f"Verified Flood Report — {location_label}",
            message=...,
            level=alert_level,
            state=report.state,
            lgas=[report.lga] if report.lga else [],
            lat=report.lat,      
            lng=report.lng,      
            is_active=True,
            is_published=True,
            issued_by=current_user.id,
        )
        db.add(alert)

    db.commit()
    db.refresh(report)
    return report


@router.delete("/{report_id}", status_code=204)
def delete_report(
    report_id: str,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(require_government),
):
    report = db.query(models.FloodReport).filter(models.FloodReport.id == report_id).first()
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")

    # Delete media files from R2 before deleting the report
    if report.media_urls and USE_R2 and r2_client:
        for media_url in report.media_urls:
            try:
                parsed = urlparse(media_url)
                file_key = parsed.path.lstrip('/')
                r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=file_key)
                print(f"Deleted from R2: {file_key}")
            except Exception as e:
                print(f"Failed to delete {media_url}: {e}")

    db.delete(report)
    db.commit()


# ── Media upload endpoint (photo + voice + video) ─────────────────────────────
from fastapi import UploadFile, File, Form
import uuid as _uuid
import os as _os

MEDIA_DIR = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), "media")
_os.makedirs(MEDIA_DIR, exist_ok=True)


@router.post("/media", status_code=201)
async def create_report_with_media(
    address: str = Form(""),
    lat: float = Form(9.082),
    lng: float = Form(8.675),
    water_depth_m: float = Form(0.0),
    description: str = Form("Unknown flood event"),
    state: str = Form(""),
    lga: str = Form(""),
    image: UploadFile = File(None),
    voice: UploadFile = File(None),
    video: UploadFile = File(None),
    db: Session = Depends(get_db),
    current_user: Optional[models.User] = Depends(get_current_user_optional),
):
    _ALLOWED_EXT = {'jpg', 'jpeg', 'png', 'gif', 'webp', 'mp4', 'webm', 'mov', 'mp3', 'wav', 'ogg', 'm4a'}

    async def save_to_r2(file: UploadFile, prefix: str) -> Optional[str]:
        """Upload to Cloudflare R2 and return public URL (async)"""
        if not file or not file.filename or not r2_client:
            return None

        ext = file.filename.rsplit('.', 1)[-1].lower() if '.' in file.filename else ''
        if ext not in _ALLOWED_EXT:
            return None

        file_key = f"reports/{prefix}_{_uuid.uuid4().hex[:12]}.{ext}"

        content_type = {
            'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png',
            'gif': 'image/gif', 'webp': 'image/webp',
            'mp4': 'video/mp4', 'webm': 'video/webm', 'mov': 'video/quicktime',
            'mp3': 'audio/mpeg', 'wav': 'audio/wav', 'ogg': 'audio/ogg', 'm4a': 'audio/mp4'
        }.get(ext, 'application/octet-stream')

        try:
            content = await file.read()
            r2_client.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=file_key,
                Body=content,
                ContentType=content_type,
                Metadata={
                    'original_filename': file.filename,
                    'uploaded_by': current_user.id if current_user else 'anonymous',
                    'report_type': prefix
                }
            )

            # Use custom domain from environment variable
            CUSTOM_DOMAIN = os.getenv("R2_CUSTOM_DOMAIN", "")
            if CUSTOM_DOMAIN:
                return f"https://{CUSTOM_DOMAIN}/{file_key}"
            else:
                return f"https://{R2_BUCKET_NAME}.r2.dev/{file_key}"

        except Exception as e:
            import logging
            logging.getLogger("nihsa.reports").error(f"R2 upload failed: {e}")
            return None

    async def save_to_local(file: UploadFile, prefix: str) -> Optional[str]:
        """Fallback to local storage (async)"""
        if not file or not file.filename:
            return None
        ext = file.filename.rsplit('.', 1)[-1].lower() if '.' in file.filename else ''
        if ext not in _ALLOWED_EXT:
            return None
        name = f"{prefix}_{_uuid.uuid4().hex[:8]}.{ext}"
        path = os.path.join(MEDIA_DIR, name)
        content = await file.read()
        with open(path, "wb") as out:
            out.write(content)
        return f"/media/{name}"

    save_func = save_to_r2 if USE_R2 and r2_client else save_to_local

    media_urls = []
    for f, prefix in [(image, "img"), (voice, "voice"), (video, "video")]:
        if f:
            url = await save_func(f, prefix)
            if url:
                media_urls.append(url)

    report = models.FloodReport(
        lat=lat,
        lng=lng,
        address=address or None,
        state=state or None,
        lga=lga or None,
        description=description,
        water_depth_m=water_depth_m or None,
        media_urls=media_urls,
        status=models.VerificationStatus.PENDING,
        user_id=current_user.id if current_user else None,
    )
    db.add(report)
    db.commit()
    db.refresh(report)
    return {
        "id": report.id,
        "status": "pending",
        "media_urls": media_urls,
        "message": "Report received. Pending NIHSA verification."
    }


# Media deletion endpoint (optional, kept for compatibility)
class MediaDeleteRequest(BaseModel):
    file_key: str
    report_id: str


@router.post("/media/delete")
async def delete_media_file(
    body: MediaDeleteRequest,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(require_government),
):
    """Delete a media file from Cloudflare R2 (admin only)"""
    if not USE_R2 or not r2_client:
        return {"message": "R2 not configured, skipping file deletion"}

    try:
        r2_client.delete_object(Bucket=R2_BUCKET_NAME, Key=body.file_key)

        report = db.query(models.FloodReport).filter(models.FloodReport.id == body.report_id).first()
        if report and report.media_urls:
            url_to_remove = f"https://{R2_BUCKET_NAME}.r2.dev/{body.file_key}"
            if url_to_remove in report.media_urls:
                report.media_urls = [u for u in report.media_urls if u != url_to_remove]
                db.commit()

        return {"message": f"Deleted {body.file_key}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete: {str(e)}")
