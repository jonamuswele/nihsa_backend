"""Vanguards Router — User management for admin panel"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from pydantic import BaseModel

from database import get_db
import models
from auth_utils import hash_password, require_government

router = APIRouter()


class VanguardOut(BaseModel):
    user_id: str
    full_name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    role: str
    state: Optional[str] = None
    lga: Optional[str] = None
    is_active: bool = True

    class Config:
        from_attributes = True


class VanguardCreate(BaseModel):
    full_name: str
    email: str
    password: Optional[str] = None
    role: str = "vanguard"
    phone: Optional[str] = None
    state: Optional[str] = None
    lga: Optional[str] = None
    is_active: bool = True


class VanguardUpdate(BaseModel):
    full_name: Optional[str] = None
    email: Optional[str] = None
    role: Optional[str] = None
    phone: Optional[str] = None
    state: Optional[str] = None
    lga: Optional[str] = None
    is_active: Optional[bool] = None


def user_to_out(u: models.User) -> dict:
    return {
        "user_id": u.id,
        "full_name": u.name,
        "email": u.email,
        "phone": u.phone_number,
        "role": u.role.value if hasattr(u.role, 'value') else str(u.role),
        "state": u.state,
        "lga": u.lga,
        "is_active": u.is_active,
    }


@router.get("")
def list_vanguards(
    db: Session = Depends(get_db),
    filter: Optional[str] = None,   # verified | regular | marshal | nihsa_staff | all
    search: Optional[str] = None,
    _: models.User = Depends(require_government),
):
    """List users with optional filtering for admin panel."""
    q = db.query(models.User)

    if filter == 'verified':
        # Verified = any non-citizen role
        q = q.filter(models.User.role != models.UserRole.CITIZEN)
    elif filter == 'regular':
        q = q.filter(models.User.role == models.UserRole.CITIZEN)
    elif filter == 'marshal':
        q = q.filter(models.User.role == models.UserRole.VANGUARD)
    elif filter == 'nihsa_staff':
        q = q.filter(models.User.role.in_([
            models.UserRole.NIHSA_STAFF,
            models.UserRole.ADMIN,
            models.UserRole.GOVERNMENT,
        ]))
    # default 'all' — no filter

    if search:
        q = q.filter(
            models.User.name.ilike(f"%{search}%") |
            models.User.email.ilike(f"%{search}%")
        )

    users = q.order_by(models.User.name).all()
    return [user_to_out(u) for u in users]


@router.post("", status_code=201)
def create_vanguard(body: VanguardCreate, db: Session = Depends(get_db), _: models.User = Depends(require_government)):
    if db.query(models.User).filter(models.User.email == body.email).first():
        raise HTTPException(status_code=400, detail="Email already registered")

    role_map = {
        "vanguard": models.UserRole.VANGUARD,
        "coordinator": models.UserRole.NIHSA_STAFF,
        "nihsa_staff": models.UserRole.NIHSA_STAFF,
        "government": models.UserRole.GOVERNMENT,
        "admin": models.UserRole.ADMIN,
    }
    role = role_map.get(body.role.lower(), models.UserRole.VANGUARD)

    user = models.User(
        name=body.full_name,
        email=body.email,
        phone_number=body.phone or f"000{hash(body.email) % 10000000:07d}",
        password_hash=hash_password((body.password or "nihsa2026")[:72]),
        role=role,
        state=body.state,
        lga=body.lga,
        is_active=body.is_active,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user_to_out(user)


@router.put("/{user_id}")
@router.patch("/{user_id}")
def update_vanguard(user_id: str, body: VanguardUpdate, db: Session = Depends(get_db), _: models.User = Depends(require_government)):
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if body.full_name  is not None: user.name         = body.full_name
    if body.email      is not None: user.email        = body.email
    if body.phone      is not None: user.phone_number = body.phone
    if body.state      is not None: user.state        = body.state
    if body.lga        is not None: user.lga          = body.lga
    if body.is_active  is not None: user.is_active    = body.is_active
    if body.role is not None:
        role_map = {"vanguard": models.UserRole.VANGUARD, "coordinator": models.UserRole.NIHSA_STAFF,
                    "nihsa_staff": models.UserRole.NIHSA_STAFF, "government": models.UserRole.GOVERNMENT, "admin": models.UserRole.ADMIN}
        user.role = role_map.get(body.role.lower(), user.role)

    db.commit()
    db.refresh(user)
    return user_to_out(user)


@router.delete("/{user_id}")
def delete_vanguard(user_id: str, db: Session = Depends(get_db), _: models.User = Depends(require_government)):
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    db.delete(user)
    db.commit()
    return {"message": "User removed"}
