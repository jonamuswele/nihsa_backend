"""
Alerts Router — National Flood Alerts
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime

from database import get_db
import models, schemas
from auth_utils import get_current_user, require_government

router = APIRouter()


@router.get("", response_model=List[schemas.AlertOut])
def list_alerts(
    active_only: bool = True,
    published_only: bool = False,
    risk_level: Optional[str] = None,
    state: Optional[str] = None,
    limit: int = Query(50, le=200),
    offset: int = 0,
    db: Session = Depends(get_db),
):
    q = db.query(models.FloodAlert)
    if active_only:
        q = q.filter(models.FloodAlert.is_active == True)
    if published_only:
        q = q.filter(models.FloodAlert.is_published == True)
    if risk_level:
        q = q.filter(models.FloodAlert.level == risk_level.upper())
    if state:
        q = q.filter(models.FloodAlert.state.ilike(f"%{state}%"))
    return q.order_by(models.FloodAlert.created_at.desc()).offset(offset).limit(limit).all()


@router.get("/{alert_id}", response_model=schemas.AlertOut)
def get_alert(alert_id: str, db: Session = Depends(get_db)):
    alert = db.query(models.FloodAlert).filter(models.FloodAlert.id == alert_id).first()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    return alert


@router.post("", response_model=schemas.AlertOut, status_code=201)
def create_alert(
    body: schemas.AlertCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(require_government),
):
    alert = models.FloodAlert(
        title=body.title,
        message=body.message,
        level=body.level,
        state=body.state,
        lgas=body.lgas,
        issued_by=current_user.id,
    )
    db.add(alert)
    db.commit()
    db.refresh(alert)
    return alert


@router.patch("/{alert_id}/publish", response_model=schemas.AlertOut)
def publish_alert(
    alert_id: str,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(require_government),
):
    alert = db.query(models.FloodAlert).filter(models.FloodAlert.id == alert_id).first()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    alert.is_published = True
    db.commit()
    db.refresh(alert)
    return alert


@router.patch("/{alert_id}/deactivate", response_model=schemas.AlertOut)
def deactivate_alert(
    alert_id: str,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(require_government),
):
    alert = db.query(models.FloodAlert).filter(models.FloodAlert.id == alert_id).first()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    alert.is_active = False
    db.commit()
    db.refresh(alert)
    return alert


@router.put("/{alert_id}", response_model=schemas.AlertOut)
@router.patch("/{alert_id}", response_model=schemas.AlertOut)
def update_alert(
    alert_id: str,
    body: schemas.AlertUpdate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(require_government),
):
    alert = db.query(models.FloodAlert).filter(models.FloodAlert.id == alert_id).first()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    for k, v in body.model_dump(exclude_none=True).items():
        if hasattr(alert, k):
            setattr(alert, k, v)
    db.commit()
    db.refresh(alert)
    return alert


@router.delete("/{alert_id}")
def delete_alert(
    alert_id: str,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(require_government),
):
    alert = db.query(models.FloodAlert).filter(models.FloodAlert.id == alert_id).first()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    db.delete(alert)
    db.commit()
    return {"message": f"Alert '{alert.title}' deleted"}


@router.post("/seed", include_in_schema=False)
def seed_alerts(db: Session = Depends(get_db), _: models.User = Depends(require_government)):
    sample = [
        {
            "title": "CRITICAL FLOOD WARNING — Lokoja",
            "message": "River Niger discharge at 12,400 m³/s. Critical flood stage reached. Immediate evacuation advised.",
            "level": models.RiskLevel.CRITICAL,
            "state": "Kogi State",
            "lgas": ["Lokoja", "Ajaokuta", "Ibaji"],
        },
        {
            "title": "HIGH FLOOD ALERT — Makurdi",
            "message": "Benue River at 8.9m — 2.4m above warning threshold. Flooding expected in 12–24 hours.",
            "level": models.RiskLevel.HIGH,
            "state": "Benue State",
            "lgas": ["Makurdi", "Gwer", "Agatu"],
        },
        {
            "title": "MEDIUM FLOOD WATCH — Lagos Coast",
            "message": "Atlantic storm surge warning. Coastal flooding risk elevated.",
            "level": models.RiskLevel.MEDIUM,
            "state": "Lagos State",
            "lgas": ["Lagos Island", "Badagry", "Epe"],
        },
    ]
    for s in sample:
        db.add(models.FloodAlert(**s))
    db.commit()
    return {"seeded": len(sample)}
