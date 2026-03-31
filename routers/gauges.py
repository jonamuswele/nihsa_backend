"""Gauges Router"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timedelta

from database import get_db
import models, schemas
from auth_utils import get_current_user, require_government

router = APIRouter()


# ── List all gauges (admin needs active_only=false by default) ─────────────────
@router.get("", response_model=List[schemas.GaugeOut])
def list_gauges(
    state: Optional[str] = None,
    active_only: bool = False,   # changed: admin needs to see all
    db: Session = Depends(get_db),
):
    q = db.query(models.RiverGauge)
    if active_only:
        q = q.filter(models.RiverGauge.is_active == True)
    if state:
        q = q.filter(models.RiverGauge.state.ilike(f"%{state}%"))
    return q.all()


@router.get("/readings", response_model=List[schemas.GaugeReadingOut])
def list_all_readings(
    limit: int = Query(100, le=500),
    db: Session = Depends(get_db),
):
    """Flat list of recent readings — used by admin Gauge Readings section."""
    return (
        db.query(models.GaugeReading)
        .order_by(models.GaugeReading.recorded_at.desc())
        .limit(limit)
        .all()
    )


@router.post("/readings", response_model=schemas.GaugeReadingOut, status_code=201)
def post_reading_flat(
    body: schemas.GaugeReadingCreate,
    station_id: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    _: models.User = Depends(require_government),
):
    """Admin panel posts readings — station_id from body or query param."""
    sid = station_id or body.station_id
    if not sid:
        raise HTTPException(status_code=422, detail="station_id required")
    gauge = db.query(models.RiverGauge).filter(models.RiverGauge.station_id == sid).first()
    station_id = sid  # use resolved value below
    if not gauge:
        raise HTTPException(status_code=404, detail="Gauge station not found")

    risk = models.RiskLevel.NORMAL
    if gauge.danger_level and body.water_level >= gauge.danger_level:
        risk = models.RiskLevel.CRITICAL
    elif gauge.warning_level and body.water_level >= gauge.warning_level * 0.9:
        risk = models.RiskLevel.HIGH
    elif gauge.warning_level and body.water_level >= gauge.warning_level * 0.7:
        risk = models.RiskLevel.MEDIUM

    reading = models.GaugeReading(
        station_id=sid,
        water_level=body.water_level,
        discharge=body.discharge,
        rainfall_mm=body.rainfall_mm,
        risk_level=risk,
        recorded_at=body.recorded_at or datetime.utcnow(),
        source=body.source,
        notes=body.notes,
    )
    db.add(reading)
    db.commit()
    db.refresh(reading)
    return reading


@router.get("/{station_id}", response_model=schemas.GaugeOut)
def get_gauge(station_id: str, db: Session = Depends(get_db)):
    g = db.query(models.RiverGauge).filter(models.RiverGauge.station_id == station_id).first()
    if not g:
        raise HTTPException(status_code=404, detail="Gauge station not found")
    return g


@router.post("", response_model=schemas.GaugeOut, status_code=201)
def create_gauge(body: schemas.GaugeCreate, db: Session = Depends(get_db), _: models.User = Depends(require_government)):
    # Check duplicate station_code
    if db.query(models.RiverGauge).filter(models.RiverGauge.station_code == body.station_code).first():
        raise HTTPException(status_code=400, detail="Station code already exists")
    gauge = models.RiverGauge(**body.model_dump())
    db.add(gauge)
    db.commit()
    db.refresh(gauge)
    return gauge


@router.put("/{station_id}", response_model=schemas.GaugeOut)
@router.patch("/{station_id}", response_model=schemas.GaugeOut)
def update_gauge(station_id: str, body: schemas.GaugeUpdate, db: Session = Depends(get_db), _: models.User = Depends(require_government)):
    g = db.query(models.RiverGauge).filter(models.RiverGauge.station_id == station_id).first()
    if not g:
        raise HTTPException(status_code=404, detail="Gauge not found")
    for k, v in body.model_dump(exclude_none=True).items():
        setattr(g, k, v)
    db.commit()
    db.refresh(g)
    return g


@router.delete("/{station_id}")
def delete_gauge(station_id: str, db: Session = Depends(get_db), _: models.User = Depends(require_government)):
    g = db.query(models.RiverGauge).filter(models.RiverGauge.station_id == station_id).first()
    if not g:
        raise HTTPException(status_code=404, detail="Gauge not found")
    db.delete(g)
    db.commit()
    return {"message": "Deleted"}


@router.get("/{station_id}/readings", response_model=List[schemas.GaugeReadingOut])
def get_station_readings(station_id: str, hours: int = Query(72, le=720), db: Session = Depends(get_db)):
    since = datetime.utcnow() - timedelta(hours=hours)
    return (
        db.query(models.GaugeReading)
        .filter(models.GaugeReading.station_id == station_id)
        .filter(models.GaugeReading.recorded_at >= since)
        .order_by(models.GaugeReading.recorded_at.desc())
        .limit(500).all()
    )


@router.post("/seed", include_in_schema=False)
def seed_gauges(db: Session = Depends(get_db), _: models.User = Depends(require_government)):
    stations = [
        {"station_id": "G001", "station_code": "G001", "station_name": "Lokoja",   "river_name": "Niger",  "lat": 7.79996, "lng": 6.74868, "state": "Kogi State",     "warning_level": 7.5, "danger_level": 9.0},
        {"station_id": "G002", "station_code": "G002", "station_name": "Makurdi",  "river_name": "Benue",  "lat": 7.74618, "lng": 8.53213, "state": "Benue State",    "warning_level": 6.0, "danger_level": 8.0},
        {"station_id": "G003", "station_code": "G003", "station_name": "Onitsha",  "river_name": "Niger",  "lat": 6.16250, "lng": 6.77548, "state": "Anambra State",  "warning_level": 5.5, "danger_level": 7.0},
        {"station_id": "G004", "station_code": "G004", "station_name": "Kainji",   "river_name": "Niger",  "lat": 9.85173, "lng": 4.61560, "state": "Niger State",    "warning_level": 4.5, "danger_level": 6.0},
        {"station_id": "G005", "station_code": "G005", "station_name": "Jebba",    "river_name": "Niger",  "lat": 9.12898, "lng": 4.81822, "state": "Kwara State",    "warning_level": 5.0, "danger_level": 7.0},
    ]
    added = 0
    for s in stations:
        if not db.query(models.RiverGauge).filter(models.RiverGauge.station_id == s["station_id"]).first():
            db.add(models.RiverGauge(**s, is_active=True))
            added += 1
    db.commit()
    return {"seeded": added}
