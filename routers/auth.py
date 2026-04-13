import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import APIRouter, Depends, HTTPException, Form, Request
from sqlalchemy.orm import Session
from datetime import datetime
from typing import Optional, Union
import json
from database import get_db
import models, schemas
from auth_utils import (
    hash_password, 
    verify_password, 
    create_access_token,
    create_refresh_token,  # ✅ ADDED
    get_current_user,
    SECRET_KEY,            # ✅ ADDED
    ALGORITHM,             # ✅ ADDED
    ACCESS_TOKEN_EXPIRE_HOURS  # ✅ ADDED
)
from pydantic import BaseModel
from jose import JWTError, jwt  # ✅ ADDED

router = APIRouter()


class RefreshTokenRequest(BaseModel):
    refresh_token: str


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    user: schemas.UserOut
    expires_in: int = 86400  # 24 hours in seconds


@router.post("/refresh", response_model=TokenResponse)
def refresh_access_token(body: RefreshTokenRequest, db: Session = Depends(get_db)):
    """Get a new access token using a valid refresh token."""
    try:
        payload = jwt.decode(body.refresh_token, SECRET_KEY, algorithms=[ALGORITHM])
        
        # ✅ Verify it's a refresh token
        if payload.get("type") != "refresh":
            raise HTTPException(status_code=401, detail="Invalid token type")
        
        user_id = payload.get("sub")
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token payload")
        
        user = db.query(models.User).filter(models.User.id == str(user_id)).first()
        if not user or not user.is_active:
            raise HTTPException(status_code=401, detail="User not found or inactive")
        
        # Create new tokens
        access_token = create_access_token({"sub": str(user.id), "role": user.role.value, "scope": user.sub_admin_scope})
        refresh_token = create_refresh_token({"sub": str(user.id)})
        
        return TokenResponse(
            access_token=access_token,
            refresh_token=refresh_token,
            user=schemas.UserOut.from_orm(user),
            expires_in=ACCESS_TOKEN_EXPIRE_HOURS * 3600
        )
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired refresh token")


@router.post("/login", response_model=TokenResponse)
def login(
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
):
    user = db.query(models.User).filter(
        (models.User.email == username) | (models.User.phone_number == username)
    ).first()
    if not user or not verify_password(password, user.password_hash):
        raise HTTPException(status_code=401, detail="Incorrect email or password")
    if not user.is_active:
        raise HTTPException(status_code=403, detail="Account suspended")
    
    user.last_login = datetime.utcnow()
    db.commit()
    
    # ✅ Create both access and refresh tokens
    access_token = create_access_token({"sub": str(user.id), "role": user.role.value, "scope": user.sub_admin_scope})
    refresh_token = create_refresh_token({"sub": str(user.id)})
    
    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        user=schemas.UserOut.from_orm(user),
        expires_in=ACCESS_TOKEN_EXPIRE_HOURS * 3600
    )


@router.post("/register", response_model=TokenResponse, status_code=201)  # ✅ Changed to TokenResponse
def register(body: schemas.UserRegister, db: Session = Depends(get_db)):
    if body.email and db.query(models.User).filter(models.User.email == body.email).first():
        raise HTTPException(status_code=400, detail="Email already registered")
    if body.phone_number and db.query(models.User).filter(models.User.phone_number == body.phone_number).first():
        raise HTTPException(status_code=400, detail="Phone number already registered")
    
    user = models.User(
        name=body.name, 
        email=body.email, 
        phone_number=body.phone_number or None,
        password_hash=hash_password(body.password),
        role=models.UserRole.CITIZEN, 
        state=body.state, 
        lga=body.lga,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    
    # ✅ Create both access and refresh tokens
    access_token = create_access_token({"sub": str(user.id), "role": user.role.value})
    refresh_token = create_refresh_token({"sub": str(user.id)})
    
    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        user=schemas.UserOut.from_orm(user),
        expires_in=ACCESS_TOKEN_EXPIRE_HOURS * 3600
    )


@router.post("/seed-admins")
def seed_admins(db: Session = Depends(get_db)):
    created = []
    defaults = [
        {"email": "admin@nihsa.gov.ng",       "name": "NIHSA Administrator", "role": models.UserRole.ADMIN,       "phone": "08000000001", "password": "nihsa2026"},
        {"email": "coordinator@nihsa.gov.ng", "name": "NIHSA Coordinator",   "role": models.UserRole.NIHSA_STAFF, "phone": "08000000002", "password": "nihsa2026"},
    ]
    for u in defaults:
        if not db.query(models.User).filter(models.User.email == u["email"]).first():
            db.add(models.User(
                name=u["name"], 
                email=u["email"], 
                phone_number=u["phone"],
                password_hash=hash_password(u["password"]), 
                role=u["role"], 
                is_active=True
            ))
            created.append(u["email"])
    db.commit()
    return {"message": "Created: " + ", ".join(created) if created else "Already exist"}


@router.get("/me", response_model=schemas.UserOut)
def get_me(current_user: models.User = Depends(get_current_user)):
    return current_user


# ── Forgot Password (Resend email service) ─────────────────────────────────────

class ForgotPasswordRequest(BaseModel):
    email: str


class ResetPasswordRequest(BaseModel):
    email: str
    otp: str
    new_password: str


# In-memory OTP store (use Redis in production)
import secrets
import time as _time
_otp_store: dict = {}  # email -> (otp, expires_at)


@router.post("/forgot-password")
def forgot_password(body: ForgotPasswordRequest, db: Session = Depends(get_db)):
    """Send password reset OTP via Resend email service."""
    user = db.query(models.User).filter(models.User.email == body.email).first()
    # Always return success to prevent email enumeration
    if user:
        otp = str(secrets.randbelow(900000) + 100000)  # 6-digit OTP
        _otp_store[body.email] = (otp, _time.time() + 900)  # 15 min expiry

        # Send via Resend — set RESEND_API_KEY in your .env
        resend_key = os.getenv("RESEND_API_KEY")
        if resend_key:
            try:
                import urllib.request
                import json as _json
                payload = _json.dumps({
                    "from": "NIHSA Platform <noreply@nihsa.gov.ng>",
                    "to": [body.email],
                    "subject": "Your NIHSA Password Reset Code",
                    "html": f"""
                    <div style="font-family:sans-serif;max-width:480px;margin:0 auto;padding:32px;background:#f9f9f9;border-radius:12px">
                      <div style="text-align:center;margin-bottom:24px">
                        <div style="font-size:32px">🌊</div>
                        <h2 style="color:#0369a1;margin:8px 0">NIHSA Password Reset</h2>
                      </div>
                      <p style="color:#333">Your password reset code is:</p>
                      <div style="font-size:36px;font-weight:800;letter-spacing:8px;color:#0369a1;
                        text-align:center;padding:20px;background:#fff;border-radius:8px;margin:16px 0">
                        {otp}
                      </div>
                      <p style="color:#666;font-size:13px">This code expires in 15 minutes. If you did not request a reset, ignore this email.</p>
                      <p style="color:#999;font-size:11px;margin-top:24px">Nigeria Hydrological Services Agency · Utako, Abuja FCT</p>
                    </div>
                    """,
                }).encode()
                req = urllib.request.Request(
                    "https://api.resend.com/emails",
                    data=payload,
                    headers={"Authorization": f"Bearer {resend_key}", "Content-Type": "application/json"},
                )
                urllib.request.urlopen(req, timeout=10)
            except Exception as e:
                import logging
                logging.getLogger("nihsa.auth").warning(f"Resend email failed: {e}")
        else:
            # Log OTP to console for development (remove in production)
            import logging
            logging.getLogger("nihsa.auth").info(f"[DEV] OTP for {body.email}: {otp} (set RESEND_API_KEY to send email)")

    return {"message": "If that email is registered, a reset code has been sent."}


@router.post("/reset-password")
def reset_password(body: ResetPasswordRequest, db: Session = Depends(get_db)):
    """Verify OTP and set new password."""
    record = _otp_store.get(body.email)
    if not record:
        raise HTTPException(status_code=400, detail="No reset request found. Please request a new code.")
    otp, expires = record
    if _time.time() > expires:
        del _otp_store[body.email]
        raise HTTPException(status_code=400, detail="Reset code has expired. Please request a new one.")
    if body.otp != otp:
        raise HTTPException(status_code=400, detail="Invalid reset code.")

    user = db.query(models.User).filter(models.User.email == body.email).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")

    user.password_hash = hash_password(body.new_password[:72])
    db.commit()
    del _otp_store[body.email]
    return {"message": "Password reset successfully. You can now sign in."}


# ── Admin: User Management ─────────────────────────────────────────────────────

from auth_utils import require_admin, require_government


class RoleAssignRequest(BaseModel):
    role: str
    sub_admin_scope: Optional[str] = None


VALID_ROLES = {r.value for r in models.UserRole}
VALID_SCOPES = {"surface_water", "groundwater", "water_quality", "coastal_marine", "forecast", "forecast_weekly", "reports", "alerts", "vanguards"}


@router.get("/users")
def list_users(
    db: Session = Depends(get_db),
    _admin: models.User = Depends(require_government),
):
    users = db.query(models.User).order_by(models.User.created_at.desc()).all()
    return [schemas.UserOut.from_orm(u) for u in users]


@router.delete("/users/{user_id}")
def delete_user(
    user_id: str,
    db: Session = Depends(get_db),
    admin: models.User = Depends(require_admin),
):
    if user_id == admin.id:
        raise HTTPException(status_code=400, detail="Cannot delete your own account")
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Null out foreign key references to this user before deleting
    # to avoid FK constraint violations
    db.query(models.FloodAlert).filter(
        models.FloodAlert.issued_by == user_id
    ).update({"issued_by": None}, synchronize_session=False)

    db.query(models.FloodReport).filter(
        models.FloodReport.user_id == user_id
    ).update({"user_id": None}, synchronize_session=False)

    db.query(models.FloodReport).filter(
        models.FloodReport.verified_by == user_id
    ).update({"verified_by": None}, synchronize_session=False)

    db.query(models.VanguardChatMessage).filter(
        models.VanguardChatMessage.user_id == user_id
    ).update({"user_id": None}, synchronize_session=False)

    db.delete(user)
    db.commit()
    return {"message": f"User '{user.name}' deleted"}


@router.put("/users/{user_id}/role")
def assign_role(
    user_id: str,
    body: RoleAssignRequest,
    db: Session = Depends(get_db),
    admin: models.User = Depends(require_admin),
):
    if body.role not in VALID_ROLES:
        raise HTTPException(status_code=400, detail=f"Invalid role: {body.role}")
    if body.role == "sub_admin" and body.sub_admin_scope not in VALID_SCOPES:
        raise HTTPException(status_code=400, detail=f"Invalid scope: {body.sub_admin_scope}")
    if user_id == admin.id:
        raise HTTPException(status_code=400, detail="Cannot change your own role")

    user = db.query(models.User).filter(models.User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.role = models.UserRole(body.role)
    user.sub_admin_scope = body.sub_admin_scope if body.role == "sub_admin" else None
    db.commit()
    db.refresh(user)
    return schemas.UserOut.from_orm(user)
