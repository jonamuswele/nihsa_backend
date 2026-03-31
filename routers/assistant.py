"""
NIHSA AI Assistant — Backend proxy for Anthropic API.
Keeps the API key server-side; never exposed to the browser.
Set ANTHROPIC_API_KEY in your environment / .env file.
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional
import urllib.request
import json as _json

router = APIRouter()

class _Msg(BaseModel):
    role: str
    content: str = Field(..., max_length=8000)

class AssistantRequest(BaseModel):
    system: str = Field(..., max_length=6000)
    messages: List[_Msg] = Field(..., max_items=20)
    model: str = "claude-sonnet-4-20250514"
    max_tokens: int = Field(default=1000, le=2000)


@router.post("/chat")
def assistant_chat(body: AssistantRequest):
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise HTTPException(
            status_code=503,
            detail="AI assistant is not configured. Contact the system administrator."
        )

    # Sanitise: only allow 'user'/'assistant' roles
    safe_msgs = [
        {"role": m.role if m.role in ("user", "assistant") else "user", "content": m.content}
        for m in body.messages
    ]

    payload = _json.dumps({
        "model": body.model,
        "max_tokens": body.max_tokens,
        "system": body.system,
        "messages": safe_msgs,
    }).encode()

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=payload,
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = _json.loads(resp.read())
        text = "".join(c.get("text", "") for c in data.get("content", []))
        return {"text": text or "I could not generate a response. Please try again."}
    except urllib.error.HTTPError as e:
        body_bytes = e.read()
        try:
            err = _json.loads(body_bytes).get("error", {}).get("message", "")
        except Exception:
            err = ""
        raise HTTPException(status_code=502, detail=f"AI service error: {err}" if err else "AI service temporarily unavailable.")
    except Exception:
        raise HTTPException(status_code=503, detail="AI service temporarily unavailable.")
