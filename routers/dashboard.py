"""
Dashboard Router — National Overview Statistics
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func
from datetime import datetime, timedelta

from database import get_db
import models, schemas

router = APIRouter()


@router.get("/stats", response_model=schemas.DashboardStats)
def get_stats(db: Session = Depends(get_db)):
    now = datetime.utcnow()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    active_alerts = db.query(models.FloodAlert).filter(
        models.FloodAlert.is_active == True
    ).count()

    critical_alerts = db.query(models.FloodAlert).filter(
        models.FloodAlert.is_active == True,
        models.FloodAlert.level.in_([models.RiskLevel.CRITICAL, models.RiskLevel.HIGH])
    ).count()

    gauges_online = db.query(models.RiverGauge).filter(
        models.RiverGauge.is_active == True
    ).count()

    gauges_critical = (
        db.query(models.GaugeReading)
        .filter(models.GaugeReading.risk_level == models.RiskLevel.CRITICAL)
        .filter(models.GaugeReading.recorded_at >= now - timedelta(hours=6))
        .count()
    )

    reports_today = db.query(models.FloodReport).filter(
        models.FloodReport.submitted_at >= today_start
    ).count()

    reports_pending = db.query(models.FloodReport).filter(
        models.FloodReport.status == models.VerificationStatus.PENDING
    ).count()

    total_stations = db.query(models.RiverGauge).count()
    active_vanguards = db.query(models.User).filter(
        models.User.role == models.UserRole.VANGUARD,
        models.User.is_active == True,
    ).count()

    return schemas.DashboardStats(
        active_alerts=active_alerts,
        critical_alerts=critical_alerts,
        gauges_online=gauges_online,
        gauges_critical=gauges_critical,
        reports_today=reports_today,
        reports_pending=reports_pending,
        total_stations=total_stations,
        active_vanguards=active_vanguards,
        basins_monitored=70,
        last_updated=now,
    )


@router.get("/map-data")
def get_map_data(db: Session = Depends(get_db)):
    """All geospatial data needed to render the national map."""
    alerts = db.query(models.FloodAlert).filter(
        models.FloodAlert.is_active == True,
        models.FloodAlert.is_published == True,
    ).all()

    gauges = db.query(models.RiverGauge).filter(
        models.RiverGauge.is_active == True
    ).all()

    reports = db.query(models.FloodReport).filter(
        models.FloodReport.status == models.VerificationStatus.VERIFIED,
        models.FloodReport.submitted_at >= datetime.utcnow() - timedelta(days=2),
        models.FloodReport.lat.isnot(None),
    ).limit(100).all()

    centres = db.query(models.EvacuationCenter).filter(
        models.EvacuationCenter.is_active == True
    ).all()

    return {
        "alerts": [
            {
                "id": a.id, "title": a.title, "level": a.level,
                "state": a.state, "lgas": a.lgas or [],
            }
            for a in alerts
        ],
        "gauges": [
            {
                "id": g.station_id, "name": g.station_name,
                "lat": g.lat, "lng": g.lng,
                "river": g.river_name, "state": g.state,
            }
            for g in gauges
        ],
        "reports": [
            {
                "id": r.id, "lat": r.lat, "lng": r.lng,
                "depth": r.water_depth_m, "state": r.state, "lga": r.lga,
            }
            for r in reports
        ],
        "centres": [
            {
                "id": c.id, "name": c.name, "lat": c.lat, "lng": c.lng,
                "capacity": c.capacity, "occupancy": c.current_occupancy,
            }
            for c in centres
        ],
    }


@router.get("/alerts/active", response_model=list)
def get_active_alerts(db: Session = Depends(get_db)):
    """Quick list of active published alerts for the ticker."""
    return db.query(models.FloodAlert).filter(
        models.FloodAlert.is_active == True,
        models.FloodAlert.is_published == True,
    ).order_by(models.FloodAlert.created_at.desc()).limit(10).all()
