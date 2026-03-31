"""
Forecast Router — AI Flood Forecast Model Outputs
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timedelta

from database import get_db
import models, schemas
from auth_utils import require_government

router = APIRouter()


@router.get("", response_model=List[schemas.ForecastOut])
def list_forecasts(
    basin_code: Optional[str] = None,
    risk_level: Optional[str] = None,
    lead_hours: int = Query(120, le=720),
    db: Session = Depends(get_db),
):
    until = datetime.utcnow() + timedelta(hours=lead_hours)
    q = (
        db.query(models.FloodForecast)
        .filter(models.FloodForecast.forecast_date <= until)
        .filter(models.FloodForecast.forecast_date >= datetime.utcnow())
    )
    if basin_code:
        basin = db.query(models.RiverBasin).filter(
            models.RiverBasin.basin_code == basin_code
        ).first()
        if basin:
            q = q.filter(models.FloodForecast.basin_id == basin.id)
    if risk_level:
        q = q.filter(models.FloodForecast.risk_level == risk_level.upper())
    return q.order_by(models.FloodForecast.forecast_date).limit(200).all()


@router.get("/basin/{basin_code}")
def get_basin_forecast(basin_code: str, db: Session = Depends(get_db)):
    basin = db.query(models.RiverBasin).filter(
        models.RiverBasin.basin_code == basin_code
    ).first()
    if not basin:
        raise HTTPException(status_code=404, detail="Basin not found")
    forecasts = (
        db.query(models.FloodForecast)
        .filter(models.FloodForecast.basin_id == basin.id)
        .filter(models.FloodForecast.forecast_date >= datetime.utcnow())
        .order_by(models.FloodForecast.forecast_date)
        .limit(10)
        .all()
    )
    return {
        "basin_code": basin.basin_code,
        "name": basin.name,
        "forecasts": [schemas.ForecastOut.model_validate(f) for f in forecasts],
    }


@router.post("", response_model=schemas.ForecastOut, status_code=201)
def ingest_forecast(
    body: schemas.ForecastCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(require_government),
):
    """Ingest LSTM model forecast output."""
    fc = models.FloodForecast(**body.model_dump())
    db.add(fc)
    db.commit()
    db.refresh(fc)
    return fc
