"""
NIHSA National Flood Intelligence Platform — Backend API
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from datetime import datetime
from fastapi import FastAPI, Depends
from fastapi.staticfiles import StaticFiles
from fastapi import WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import json
import asyncio
from typing import Dict, List
from pathlib import Path
from sqlalchemy.orm import Session
from database import engine, Base, SessionLocal
from database import get_db
from routers import auth, alerts, gauges, reports, forecast, chat, dashboard, vanguards, admin, forecast_ml, assistant, seasonal, map_layers
import models
from auth_utils import verify_password
from auth_utils import hash_password
import models as m
from sqlalchemy import text
from routers.map_layers import DEFAULT_LAYERS


class ConnectionManager:
    def __init__(self):
        self.channels: Dict[str, List[WebSocket]] = {}

    async def connect(self, ws: WebSocket, channel: str):
        await ws.accept()
        self.channels.setdefault(channel, []).append(ws)

    def disconnect(self, ws: WebSocket, channel: str):
        if channel in self.channels:
            self.channels[channel] = [w for w in self.channels[channel] if w != ws]

    async def broadcast(self, channel: str, message: dict):
        if channel in self.channels:
            dead = []
            for ws in self.channels[channel]:
                try:
                    await ws.send_json(message)
                except Exception:
                    dead.append(ws)
            for ws in dead:
                self.disconnect(ws, channel)

manager = ConnectionManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create all tables
    Base.metadata.create_all(bind=engine)
    
    with engine.connect() as conn:
        migrations = [
            "ALTER TABLE users ADD COLUMN sub_admin_scope VARCHAR(60)",
            "UPDATE map_layers SET layer_type='geojson_fc' WHERE layer_key='sw_satellite' AND layer_type='toggle'",
        ]
        for sql in migrations:
            try:
                conn.execute(text(sql))
                conn.commit()
            except Exception:
                pass  

    
    db = SessionLocal()
    try:
        default_admin_email = os.getenv("DEFAULT_ADMIN_EMAIL", "")
        default_admin_password = os.getenv("DEFAULT_ADMIN_PASSWORD", "")
        default_admin_name = os.getenv("DEFAULT_ADMIN_NAME", "System Administrator")
        
        if default_admin_email and default_admin_password:
            exists = db.query(m.User).filter(m.User.email == default_admin_email).first()
            if not exists:
                db.add(m.User(
                    name=default_admin_name,
                    email=default_admin_email,
                    phone_number=os.getenv("DEFAULT_ADMIN_PHONE", "08000000000"),
                    password_hash=hash_password(default_admin_password),
                    role=m.UserRole.ADMIN,
                    is_active=True,
                    is_verified=True,  
                ))
                print(f"Created admin user: {default_admin_email}")
        
        # Optional: Create a default coordinator (also from env)
        default_coordinator_email = os.getenv("DEFAULT_COORDINATOR_EMAIL", "")
        default_coordinator_password = os.getenv("DEFAULT_COORDINATOR_PASSWORD", "")
        
        if default_coordinator_email and default_coordinator_password:
            exists = db.query(m.User).filter(m.User.email == default_coordinator_email).first()
            if not exists:
                db.add(m.User(
                    name=os.getenv("DEFAULT_COORDINATOR_NAME", "NIHSA Coordinator"),
                    email=default_coordinator_email,
                    phone_number=os.getenv("DEFAULT_COORDINATOR_PHONE", "08000000001"),
                    password_hash=hash_password(default_coordinator_password),
                    role=m.UserRole.NIHSA_STAFF,
                    is_active=True,
                    is_verified=True,
                ))
                print(f"Created coordinator user: {default_coordinator_email}")
                
        db.commit()
    except Exception as e:
        print(f"Seed error: {str(e)}")
        db.rollback()
    finally:
        db.close()

    # Seed default map layers
    db2 = SessionLocal()
    try:
        
        for d in DEFAULT_LAYERS:
            exists = db2.query(m.MapLayer).filter(m.MapLayer.layer_key == d["layer_key"]).first()
            if not exists:
                db2.add(m.MapLayer(**d))
        db2.commit()
    except Exception as e:
        print(f"Map layer seed error: {str(e)}")
        db2.rollback()
    finally:
        db2.close()

    print("NIHSA API started — database ready")
    
    # Alert auto-delete runs every 6 hours
    async def _auto_delete():
        while True:
            await asyncio.sleep(21600)
            try:
                from datetime import datetime, timedelta
                from database import SessionLocal
                db = SessionLocal()
                cutoff = datetime.utcnow() - timedelta(days=30)
                n = db.query(m.FloodAlert).filter(
                    m.FloodAlert.created_at < cutoff, 
                    m.FloodAlert.is_active == False
                ).delete(synchronize_session=False)
                db.commit()
                db.close()
                if n:
                    print(f"Auto-purged {n} expired alerts")
            except:
                pass
    
    asyncio.create_task(_auto_delete())
    yield
    print("NIHSA API shutting down")

app = FastAPI(
    title="NIHSA Flood Intelligence API",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    lifespan=lifespan,
)

# Configure CORS for production
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "").split(",") if os.getenv("ALLOWED_ORIGINS") else []

# Always include these exact origins
ALLOWED_ORIGINS.extend([
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "https://nihsa-frontend.onrender.com",
    "*"
])

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve uploaded media files
MEDIA_DIR = os.getenv("MEDIA_DIR", "/app/media")
os.makedirs(MEDIA_DIR, exist_ok=True)
app.mount("/media", StaticFiles(directory=MEDIA_DIR), name="media")

# NFFS Atlas & Export static files
NFFS_ROOT = Path(os.getenv("NFFS_ROOT", "/app/nffs_data"))
ATLAS_DIR = NFFS_ROOT / "results" / "atlas"
EXPORT_DIR = ATLAS_DIR / "atrisk_exports"

# Create directories if they don't exist
ATLAS_DIR.mkdir(parents=True, exist_ok=True)
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

if ATLAS_DIR.exists():
    app.mount("/nffs/atlas", StaticFiles(directory=str(ATLAS_DIR)), name="nffs_atlas")
if EXPORT_DIR.exists():
    app.mount("/nffs/exports", StaticFiles(directory=str(EXPORT_DIR)), name="nffs_exports")

# Import routers
app.include_router(auth.router,      prefix="/api/auth",      tags=["Auth"])
app.include_router(alerts.router,    prefix="/api/alerts",    tags=["Alerts"])
app.include_router(gauges.router,    prefix="/api/gauges",    tags=["Gauges"])
app.include_router(reports.router,   prefix="/api/reports",   tags=["Reports"])
app.include_router(forecast.router,  prefix="/api/forecast",  tags=["Forecast"])
app.include_router(chat.router,      prefix="/api/chat",      tags=["Chat"])
app.include_router(dashboard.router,  prefix="/api/dashboard", tags=["Dashboard"])
app.include_router(vanguards.router,   prefix="/api/vanguards",    tags=["Vanguards"])
app.include_router(admin.router,       prefix="/api",              tags=["Admin"])
app.include_router(forecast_ml.router, prefix="/api/forecast/ml",  tags=["ML Forecast"])
app.include_router(seasonal.router,    prefix="/api/seasonal",     tags=["Seasonal Outlook"])
app.include_router(assistant.router,   prefix="/api/assistant",    tags=["Assistant"])
app.include_router(map_layers.router,  prefix="/api/map-layers",   tags=["Map Layers"])

@app.get("/api")
async def api_root():
    return {"status": "ok", "message": "NIHSA API is running", "version": "1.0.0"}
    
@app.get("/api/debug/admin-check")
async def check_admin(db: Session = Depends(get_db)):
    from auth_utils import verify_password
    
    admin = db.query(models.User).filter(
        models.User.email == "admin@nihsa.gov.ng"
    ).first()
    
    if not admin:
        return {"exists": False, "message": "Admin user not found in database"}
    
    # Test if password works
    password_works = verify_password("nihsa2026", admin.password_hash)
    
    return {
        "exists": True,
        "email": admin.email,
        "role": admin.role.value,
        "is_active": admin.is_active,
        "is_verified": admin.is_verified,
        "password_hash_starts": admin.password_hash[:20] + "...",
        "password_verification": "SUCCESS" if password_works else "FAILED",
        "id": admin.id
    }
    
@app.get("/api/db-test")
async def test_database():
    from database import SessionLocal
    from sqlalchemy import text
    
    try:
        db = SessionLocal()
        # Try to query the database
        result = db.execute(text("SELECT 1 as test"))
        db.close()
        return {
            "status": "connected", 
            "message": "Database connection successful!",
            "database_url": os.getenv("DATABASE_URL", "").replace("://", "://***@")[:50] + "..."
        }
    except Exception as e:
        return {
            "status": "failed", 
            "error": str(e),
            "message": "Database connection failed - check DATABASE_URL"
        }

@app.get("/api/tables-check")
async def check_tables():
    from database import SessionLocal
    from sqlalchemy import text
    
    try:
        db = SessionLocal()
        # Get list of all tables
        result = db.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """))
        tables = [row[0] for row in result.fetchall()]
        db.close()
        
        return {
            "status": "success",
            "table_count": len(tables),
            "tables": tables[:20],  # Show first 20 tables
            "expected_tables": ["users", "river_gauges", "flood_alerts", "flood_reports"]
        }
    except Exception as e:
        return {"status": "failed", "error": str(e)}

@app.get("/api/users-count")
async def count_users():
    from database import SessionLocal
    import models
    
    try:
        db = SessionLocal()
        count = db.query(models.User).count()
        db.close()
        return {
            "status": "success",
            "user_count": count,
            "message": f"Database has {count} users"
        }
    except Exception as e:
        return {"status": "failed", "error": str(e)}

@app.get("/")
async def root():
    return {"system": "NIHSA Flood Intelligence Platform", "status": "operational", "docs": "/api/docs"}

@app.get("/api/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


@app.websocket("/ws/chat/{channel_id}")
async def ws_chat(ws: WebSocket, channel_id: str):
    await manager.connect(ws, channel_id)
    try:
        while True:
            data = await ws.receive_text()
            msg = json.loads(data)
            await manager.broadcast(channel_id, {"type": "message", "channel": channel_id, **msg})
    except WebSocketDisconnect:
        manager.disconnect(ws, channel_id)

@app.websocket("/ws/gauges")
async def ws_gauges(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            await asyncio.sleep(30)
            await ws.send_json({"type": "gauge_update"})
    except WebSocketDisconnect:
        pass

@app.websocket("/ws/alerts")
async def ws_alerts(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            await asyncio.sleep(60)
            await ws.send_json({"type": "alert_ping"})
    except WebSocketDisconnect:
        pass
