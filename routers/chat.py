"""
Vanguard Chat Router — state-based channels
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime

from database import get_db
import models, schemas
from auth_utils import get_current_user_optional, require_admin

router = APIRouter()

# All valid Nigeria channels: national + 37 states + FCT
VALID_CHANNELS = {
    # National command channel
    "national":     {"label": "National", "national": True},
    # States
    "abia":         {"label": "Abia State"},
    "adamawa":      {"label": "Adamawa State"},
    "akwa-ibom":    {"label": "Akwa Ibom State"},
    "anambra":      {"label": "Anambra State"},
    "bauchi":       {"label": "Bauchi State"},
    "bayelsa":      {"label": "Bayelsa State"},
    "benue":        {"label": "Benue State"},
    "borno":        {"label": "Borno State"},
    "cross-river":  {"label": "Cross River State"},
    "delta":        {"label": "Delta State"},
    "ebonyi":       {"label": "Ebonyi State"},
    "edo":          {"label": "Edo State"},
    "ekiti":        {"label": "Ekiti State"},
    "enugu":        {"label": "Enugu State"},
    "fct":          {"label": "FCT — Abuja"},
    "gombe":        {"label": "Gombe State"},
    "imo":          {"label": "Imo State"},
    "jigawa":       {"label": "Jigawa State"},
    "kaduna":       {"label": "Kaduna State"},
    "kano":         {"label": "Kano State"},
    "katsina":      {"label": "Katsina State"},
    "kebbi":        {"label": "Kebbi State"},
    "kogi":         {"label": "Kogi State"},
    "kwara":        {"label": "Kwara State"},
    "lagos":        {"label": "Lagos State"},
    "nasarawa":     {"label": "Nasarawa State"},
    "niger":        {"label": "Niger State"},
    "ogun":         {"label": "Ogun State"},
    "ondo":         {"label": "Ondo State"},
    "osun":         {"label": "Osun State"},
    "oyo":          {"label": "Oyo State"},
    "plateau":      {"label": "Plateau State"},
    "rivers":       {"label": "Rivers State"},
    "sokoto":       {"label": "Sokoto State"},
    "taraba":       {"label": "Taraba State"},
    "yobe":         {"label": "Yobe State"},
    "zamfara":      {"label": "Zamfara State"},
}


def _get_or_create_channel(channel_key: str, db: Session) -> models.VanguardChannel:
    ch = db.query(models.VanguardChannel).filter(
        models.VanguardChannel.channel_key == channel_key
    ).first()
    if not ch:
        info = VALID_CHANNELS.get(channel_key, {"label": channel_key})
        ch = models.VanguardChannel(
            channel_key=channel_key,
            name=info["label"],
            state=info["label"],
            is_command=False,
        )
        db.add(ch)
        db.commit()
        db.refresh(ch)
    return ch


@router.get("/channels")
def list_channels():
    return [
        {"id": key, "label": val["label"]}
        for key, val in VALID_CHANNELS.items()
    ]


@router.get("/{channel_id}/messages", response_model=List[schemas.ChatMessageOut])
def get_messages(
    channel_id: str,
    limit: int = Query(50, le=200),
    before: Optional[datetime] = None,
    db: Session = Depends(get_db),
):
    # Auto-create channel if not known; accept any channel string
    ch = _get_or_create_channel(channel_id, db)
    q = db.query(models.VanguardChatMessage).filter(
        models.VanguardChatMessage.channel_id == ch.id
    )
    if before:
        q = q.filter(models.VanguardChatMessage.created_at < before)
    msgs = q.order_by(models.VanguardChatMessage.created_at.desc()).limit(limit).all()

    result = []
    for m in reversed(msgs):
        user = db.query(models.User).filter(models.User.id == m.user_id).first()
        result.append(schemas.ChatMessageOut(
            id=m.id,
            channel_id=m.channel_id,
            user_id=m.user_id or "",
            user_name=user.name if user else "Anonymous",
            user_role=user.role.value if user else "vanguard",
            message=m.message,
            message_type=m.message_type,
            media_url=m.media_url,
            is_ai=m.is_ai,
            reactions=m.reactions or [],
            is_pinned=m.is_pinned,
            created_at=m.created_at,
        ))
    return result


@router.post("/{channel_id}/messages", response_model=schemas.ChatMessageOut, status_code=201)
def post_message(
    channel_id: str,
    body: schemas.ChatMessageCreate,
    db: Session = Depends(get_db),
    current_user: Optional[models.User] = Depends(get_current_user_optional),
):
    ch = _get_or_create_channel(channel_id, db)

    msg = models.VanguardChatMessage(
        channel_id=ch.id,
        user_id=current_user.id if current_user else None,
        message=body.message,
        message_type=body.message_type,
        media_url=body.media_url,
        location_wkt=f"{body.lat},{body.lng}" if body.lat and body.lng else None,
    )
    db.add(msg)
    db.commit()
    db.refresh(msg)

    return schemas.ChatMessageOut(
        id=msg.id,
        channel_id=msg.channel_id,
        user_id=msg.user_id or "",
        user_name=current_user.name if current_user else "Anonymous",
        user_role=current_user.role.value if current_user else "vanguard",
        message=msg.message,
        message_type=msg.message_type,
        media_url=msg.media_url,
        is_ai=msg.is_ai,
        reactions=msg.reactions or [],
        is_pinned=msg.is_pinned,
        created_at=msg.created_at,
    )


@router.delete("/{channel_id}/messages/{message_id}", status_code=204)
def delete_message(
    channel_id: str,
    message_id: str,
    db: Session = Depends(get_db),
    _admin: models.User = Depends(require_admin),
):
    """Admin-only: delete any chat message from any channel."""
    msg = db.query(models.VanguardChatMessage).filter(
        models.VanguardChatMessage.id == message_id
    ).first()
    if not msg:
        raise HTTPException(status_code=404, detail="Message not found")
    db.delete(msg)
    db.commit()
    return
