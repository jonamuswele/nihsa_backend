"""
Reports Router — Citizen / Vanguard Flood Reports
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timedelta

from database import get_db
import models, schemas
from auth_utils import get_current_user, get_current_user_optional, require_government

router = APIRouter()


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
            message=(report.description or "A field flood report has been verified by NIHSA.")
                    + (f"\n\nLocation: {location_label}" if location_label else ""),
            level=alert_level,
            state=report.state,
            lgas=[report.lga] if report.lga else [],
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
    lat:  float = Form(9.082),
    lng:  float = Form(8.675),
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
    _ALLOWED_EXT = {'jpg','jpeg','png','gif','webp','mp4','webm','mov','mp3','wav','ogg','m4a'}
    def save_file(f: UploadFile, prefix: str) -> str:
        if not f or not f.filename:
            return None
        ext = f.filename.rsplit('.', 1)[-1].lower() if '.' in f.filename else ''
        if ext not in _ALLOWED_EXT:
            return None
        name = f"{prefix}_{_uuid.uuid4().hex[:8]}.{ext}"
        path = _os.path.join(MEDIA_DIR, name)
        with open(path, "wb") as out:
            out.write(f.file.read())
        return f"/media/{name}"

    media_urls = [u for u in [save_file(image, "img"), save_file(voice, "voice"), save_file(video, "video")] if u]

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
    return {"id": report.id, "status": "pending", "message": "Report received. Pending NIHSA verification."}
