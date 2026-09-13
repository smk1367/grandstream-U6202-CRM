import ast
import json
import hashlib
import re
import threading
import socket
import logging
import os
import time
import secrets
from datetime import datetime, timedelta
from typing import Any

import requests
from fastapi import FastAPI, HTTPException, Query, UploadFile, File, Request
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, String, Integer, DateTime, Text, func, select, inspect, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("ucmcrm")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://crm:crm123@db:5432/ucmcrm")
UCM_CDR_URL = os.getenv("UCM_CDR_URL", "https://192.168.10.10:8443/cdrapi")
UCM_REC_URL = os.getenv("UCM_REC_URL", "https://192.168.10.10:8443/recapi")
UCM_USER = os.getenv("UCM_USER", "")
UCM_PASS = os.getenv("UCM_PASS", "")
VERIFY_TLS = os.getenv("UCM_VERIFY_TLS", "false").lower() == "true"
SYNC_INTERVAL = int(os.getenv("SYNC_INTERVAL_SECONDS", "60"))
UCM_API_URL = os.getenv("UCM_API_URL", "https://192.168.10.10:8089/api")
UCM_API_USER = os.getenv("UCM_API_USER", UCM_USER)
UCM_API_PASS = os.getenv("UCM_API_PASS", UCM_PASS)
LIVE_POLL_SECONDS = float(os.getenv("LIVE_POLL_SECONDS", "1"))

AUTH_USERS_RAW = os.getenv("CRM_USERS", "admin:admin123:full,viewer:viewer123:view")
AUTH_TOKENS: dict[str, dict[str, str]] = {}
AUTH_LOCK = threading.Lock()

def parse_users(raw: str):
    users = {}
    for item in raw.split(','):
        parts = item.strip().split(':')
        if len(parts) < 3:
            continue
        username, password, role = parts[0].strip(), parts[1], parts[2].strip().lower()
        extension = parts[3].strip() if len(parts) >= 4 else ''
        if username and password and role in {'full','view','sales','manager'}:
            users[username] = {'password': password, 'role': role, 'extension': extension}
    return users
CRM_USERS = parse_users(AUTH_USERS_RAW)

def auth_user(request: Request, required: str | None = None):
    token = request.headers.get('Authorization','').removeprefix('Bearer ').strip()
    if not token:
        # Core legacy CRM remains readable without login; management endpoints will require auth.
        return {'username':'guest','role':'full','extension':''}
    with AUTH_LOCK:
        user = AUTH_TOKENS.get(token)
    if not user:
        raise HTTPException(401, 'نشست ورود معتبر نیست')
    if required and required not in ({'full'} if required=='full' else {required}):
        role=user.get('role')
        if required=='manager' and role not in {'full','manager'}:
            raise HTTPException(403, 'دسترسی مدیریتی ندارید')
        if required=='sales' and role not in {'full','manager','sales'}:
            raise HTTPException(403, 'دسترسی فروش ندارید')
    return user

def require_management(request: Request):
    user=auth_user(request)
    if user.get('username')=='guest' or user.get('role') not in {'full','manager'}:
        raise HTTPException(403, 'ورود مدیریتی لازم است')
    return user

def require_sales_access(request: Request):
    user=auth_user(request)
    if user.get('username')=='guest' or user.get('role') not in {'full','manager','sales'}:
        raise HTTPException(403, 'ورود فروش لازم است')
    return user

# Latest real-time call event pushed by the UCM CDR Real-time Output feature.
LIVE_EVENT = {"seq": 0, "received_at": "", "active": False, "state": "", "call_key":"", "caller": "", "callee": "", "caller_name": "", "customer":"", "company":"", "answer_by": "", "direction": "", "disposition": "", "previous_extensions": [], "raw": {}}
LIVE_LOCK = threading.Lock()

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

class Base(DeclarativeBase):
    pass

class Contact(Base):
    __tablename__ = "contacts"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(200), default="")
    phone: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    company: Mapped[str] = mapped_column(String(200), default="")
    notes: Mapped[str] = mapped_column(Text, default="")
    phones: Mapped[str] = mapped_column(Text, default="[]")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    owner_extension: Mapped[str] = mapped_column(String(30), default="")
    customer_status: Mapped[str] = mapped_column(String(40), default="lead")
    score: Mapped[int] = mapped_column(Integer, default=50)
    next_followup: Mapped[str] = mapped_column(String(40), default="")
    followup_note: Mapped[str] = mapped_column(Text, default="")
    lost_reason: Mapped[str] = mapped_column(String(200), default="")

class Call(Base):
    __tablename__ = "calls"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    unique_id: Mapped[str] = mapped_column(String(200), unique=True, index=True)
    caller: Mapped[str] = mapped_column(String(100), default="")
    callee: Mapped[str] = mapped_column(String(100), default="")
    caller_name: Mapped[str] = mapped_column(String(200), default="")
    answer_by: Mapped[str] = mapped_column(String(100), default="")
    disposition: Mapped[str] = mapped_column(String(100), default="")
    start_time: Mapped[str] = mapped_column(String(80), default="")
    answer_time: Mapped[str] = mapped_column(String(80), default="")
    end_time: Mapped[str] = mapped_column(String(80), default="")
    call_time: Mapped[str] = mapped_column(String(40), default="")
    talk_time: Mapped[str] = mapped_column(String(40), default="")
    src_trunk: Mapped[str] = mapped_column(String(100), default="")
    dst_trunk: Mapped[str] = mapped_column(String(100), default="")
    channel: Mapped[str] = mapped_column(String(200), default="")
    dst_channel: Mapped[str] = mapped_column(String(200), default="")
    recordfiles: Mapped[str] = mapped_column(Text, default="")
    direction: Mapped[str] = mapped_column(String(30), default="unknown")
    raw_json: Mapped[str] = mapped_column(Text, default="{}")
    synced_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    outcome: Mapped[str] = mapped_column(String(100), default="")
    call_notes: Mapped[str] = mapped_column(Text, default="")

class FollowUp(Base):
    __tablename__ = "followups"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    contact_id: Mapped[int] = mapped_column(Integer, index=True)
    due_at: Mapped[str] = mapped_column(String(40), default="")
    note: Mapped[str] = mapped_column(Text, default="")
    status: Mapped[str] = mapped_column(String(30), default="open")
    owner_extension: Mapped[str] = mapped_column(String(30), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    completed_at: Mapped[str] = mapped_column(String(40), default="")

class Opportunity(Base):
    __tablename__ = "opportunities"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    contact_id: Mapped[int] = mapped_column(Integer, index=True)
    title: Mapped[str] = mapped_column(String(200), default="")
    stage: Mapped[str] = mapped_column(String(40), default="lead")
    value: Mapped[int] = mapped_column(Integer, default=0)
    owner_extension: Mapped[str] = mapped_column(String(30), default="")
    close_date: Mapped[str] = mapped_column(String(40), default="")
    lost_reason: Mapped[str] = mapped_column(String(200), default="")
    notes: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


def init_db(retries=30, delay=2):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            Base.metadata.create_all(engine)
            cols = {c["name"] for c in inspect(engine).get_columns("contacts")}
            contact_migrations = {
                "phones":"TEXT NOT NULL DEFAULT '[]'",
                "owner_extension":"VARCHAR(30) NOT NULL DEFAULT ''",
                "customer_status":"VARCHAR(40) NOT NULL DEFAULT 'lead'",
                "score":"INTEGER NOT NULL DEFAULT 50",
                "next_followup":"VARCHAR(40) NOT NULL DEFAULT ''",
                "followup_note":"TEXT NOT NULL DEFAULT ''",
                "lost_reason":"VARCHAR(200) NOT NULL DEFAULT ''",
            }
            with engine.begin() as conn:
                for col, spec in contact_migrations.items():
                    if col not in cols:
                        conn.execute(text(f"ALTER TABLE contacts ADD COLUMN {col} {spec}"))
                        log.info("database migration: contacts.%s added", col)
                call_cols = {c["name"] for c in inspect(engine).get_columns("calls")}
                for col, spec in {"outcome":"VARCHAR(100) NOT NULL DEFAULT ''", "call_notes":"TEXT NOT NULL DEFAULT ''"}.items():
                    if col not in call_cols:
                        conn.execute(text(f"ALTER TABLE calls ADD COLUMN {col} {spec}"))
                        log.info("database migration: calls.%s added", col)
            log.info("database ready")
            return
        except OperationalError as exc:
            last_error = exc
            log.warning("database not ready (attempt %s/%s): %s", attempt, retries, exc)
            time.sleep(delay)
    raise last_error

init_db()


UCM_API_SESSION = {"cookie": "", "expires": 0.0}
UCM_API_LOCK = threading.Lock()

def _ucm_api_post(payload: dict):
    r = requests.post(UCM_API_URL, json={"request": payload}, verify=VERIFY_TLS, timeout=4,
                      headers={"Content-Type":"application/json;charset=UTF-8", "Connection":"close"})
    r.raise_for_status()
    return r.json()

def _ucm_login_api() -> str:
    if not UCM_API_USER or not UCM_API_PASS:
        return ""
    with UCM_API_LOCK:
        if UCM_API_SESSION["cookie"] and UCM_API_SESSION["expires"] > time.time()+20:
            return UCM_API_SESSION["cookie"]
        ch = _ucm_api_post({"action":"challenge", "user":UCM_API_USER, "version":"1.0"})
        challenge = str(ch.get("response",{}).get("challenge", ""))
        if not challenge:
            raise RuntimeError(f"UCM challenge failed: {ch}")
        token = hashlib.md5((challenge + UCM_API_PASS).encode()).hexdigest()
        login = _ucm_api_post({"action":"login", "token":token, "user":UCM_API_USER,
                                "url":os.getenv("PUBLIC_API_URL", "")})
        cookie = str(login.get("response",{}).get("cookie", ""))
        if not cookie:
            raise RuntimeError(f"UCM login failed: {login}")
        UCM_API_SESSION.update(cookie=cookie, expires=time.time()+540)
        return cookie

def _ucm_api_action(action: str):
    cookie=_ucm_login_api()
    res=_ucm_api_post({"action":action,"cookie":cookie})
    if res.get("status") != 0:
        with UCM_API_LOCK:
            UCM_API_SESSION["cookie"]=""
        cookie=_ucm_login_api()
        res=_ucm_api_post({"action":action,"cookie":cookie})
    return res

def _find_contact_phone(phone: str):
    phone=clean_phone(phone)
    if not phone:
        return None
    db=SessionLocal()
    try:
        c=db.scalar(select(Contact).where(Contact.phone==phone))
        if c:
            return c
        for row in db.scalars(select(Contact)).all():
            if phone in split_phones(row.phones):
                return row
        return None
    finally:
        db.close()

def _previous_answered_extensions(phone: str) -> list[str]:
    phone = clean_phone(phone)
    if not phone:
        return []
    db = SessionLocal()
    try:
        rows = db.scalars(
            select(Call).where(
                Call.disposition.in_(["ANSWERED", "ANSWER"]),
                (Call.caller == phone) | (Call.callee == phone),
            ).order_by(Call.start_time.desc(), Call.id.desc()).limit(100)
        ).all()
        seen = []
        for row in rows:
            ext = clean_phone(row.answer_by)
            if ext and ext not in seen and not is_trunk(ext):
                seen.append(ext)
        return seen[:10]
    except Exception as exc:
        log.debug("previous extension lookup failed: %s", exc)
        return []
    finally:
        db.close()

def _set_live_from_channel(ch: dict, bridged=False):
    phone=str(ch.get("callernum") or ch.get("callerid1") or ch.get("callerid2") or "").strip()
    callee=str(ch.get("connectednum") or ch.get("callerid2") or "").strip()
    state=str(ch.get("state") or ("Up" if bridged else "Ringing"))
    inbound=bool(ch.get("inbound_trunk_name")) or (not bridged and str(ch.get("channel","")).startswith("trunk"))
    direction="inbound" if inbound else "outbound"
    contact=_find_contact_phone(phone)
    # Never query PostgreSQL while holding LIVE_LOCK: a slow DB query must not
    # block the live-call worker/browser state update.
    previous_extensions = _previous_answered_extensions(phone)
    with LIVE_LOCK:
        old_key=LIVE_EVENT.get("call_key")
        key=str(ch.get("uniqueid") or ch.get("uniqueid1") or ch.get("bridge_id") or ch.get("channel") or (phone+callee))
        # Update only when call identity/state changes so browser receives a real event.
        changed=(old_key != key or LIVE_EVENT.get("state") != state or LIVE_EVENT.get("active") is not True)
        LIVE_EVENT.update({"active":True,"call_key":key,"state":state,"caller":phone,
                           "callee":callee,"caller_name":str(ch.get("callername") or ch.get("name1") or (contact.name if contact else "")),
                           "customer":contact.name if contact else "","company":contact.company if contact else "",
                           "answer_by":str(ch.get("connectednum") or ch.get("channel") or ""),
                           "disposition":state,"direction":direction,"previous_extensions":previous_extensions,
                           "received_at":datetime.utcnow().isoformat()+"Z","raw":ch})
        if changed: LIVE_EVENT["seq"] += 1

def _clear_live_if_needed():
    with LIVE_LOCK:
        if LIVE_EVENT.get("active"):
            LIVE_EVENT.update({"active":False,"state":"ended","caller":"","callee":"","caller_name":"","customer":"","company":"","answer_by":"","direction":"","previous_extensions":[]})
            LIVE_EVENT["seq"] += 1

def _live_worker():
    log.info("UCM live call monitor started: %s", UCM_API_URL)
    while True:
        try:
            unbridged=_ucm_api_action("listUnBridgedChannels")
            bridged=_ucm_api_action("listBridgedChannels")
            uc=unbridged.get("response",{}).get("channel",[]) or []
            bc=bridged.get("response",{}).get("channel",[]) or []
            if uc:
                _set_live_from_channel(uc[0], bridged=False)
            elif bc:
                _set_live_from_channel(bc[0], bridged=True)
            else:
                _clear_live_if_needed()
        except Exception as exc:
            log.debug("UCM live API poll failed: %s", exc)
        time.sleep(LIVE_POLL_SECONDS)

app = FastAPI(title="UCM6202 CRM API", version="1.2.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
threading.Thread(target=_live_worker, daemon=True, name="ucm-live-monitor").start()


def _walk_values(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield str(k).lower(), v
            yield from _walk_values(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk_values(v)


def _first_value(obj, keys):
    wanted = {k.lower().replace("-", "_").replace(" ", "_") for k in keys}
    for k, v in _walk_values(obj):
        nk = k.replace("-", "_").replace(" ", "_")
        if nk in wanted and v not in (None, ""):
            return str(v)
    return ""


def _parse_live_body(raw: bytes, content_type: str):
    text_body = raw.decode("utf-8", errors="ignore").strip()
    if not text_body:
        return {}
    try:
        data = json.loads(text_body)
        return data if isinstance(data, (dict, list)) else {"value": data}
    except Exception:
        pass
    # UCM can be configured for XML output. XML is intentionally parsed without
    # requiring extra dependencies.
    if "xml" in (content_type or "").lower() or text_body.startswith("<"):
        try:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(text_body)
            def elem_dict(node):
                if list(node):
                    d = {}
                    for child in node:
                        d[child.tag] = elem_dict(child)
                    return d
                return node.text or ""
            return {root.tag: elem_dict(root)}
        except Exception:
            pass
    return {"raw": text_body}


@app.post("/api/cdr-realtime")
async def cdr_realtime(request: Request):
    """Receive Grandstream UCM CDR Real-time Output callbacks.

    The UCM can POST JSON/XML here as soon as a call report is available. Keeping
    this endpoint separate from the periodic CDR sync restores the live call card
    without changing the existing CDR/recording database logic.
    """
    raw = await request.body()
    payload = _parse_live_body(raw, request.headers.get("content-type", ""))
    caller = _first_value(payload, {"src", "caller", "caller_number", "callerid", "from"})
    callee = _first_value(payload, {"dst", "callee", "called", "callee_number", "to"})
    caller_name = _first_value(payload, {"caller_name", "callername", "name", "cid_name"})
    answer_by = _first_value(payload, {"answeredby", "answer_by", "answerby", "dstchannel_ext", "channel_ext"})
    disposition = _first_value(payload, {"disposition", "status", "state", "event"})
    direction = _first_value(payload, {"direction", "call_direction"}).lower()

    # Infer direction from trunk markers when the payload omits it.
    if not direction:
        src_trunk = _first_value(payload, {"src_trunk_name", "src_trunk", "from_trunk"})
        dst_trunk = _first_value(payload, {"dst_trunk_name", "dst_trunk", "to_trunk"})
        if src_trunk and not dst_trunk:
            direction = "inbound"
        elif dst_trunk and not src_trunk:
            direction = "outbound"

    with LIVE_LOCK:
        LIVE_EVENT["seq"] += 1
        LIVE_EVENT.update({
            "received_at": datetime.utcnow().isoformat() + "Z",
            "caller": caller,
            "callee": callee,
            "caller_name": caller_name,
            "answer_by": answer_by,
            "disposition": disposition,
            "direction": direction,
            "raw": payload,
        })
        event = dict(LIVE_EVENT)
    log.info("UCM LIVE EVENT payload: %s", event)
    return {"ok": True, "event": event}


@app.get("/api/live")
def live():
    with LIVE_LOCK:
        return dict(LIVE_EVENT)


def as_dict(v: Any) -> dict[str, Any]:
    return v if isinstance(v, dict) else {}


def segments_from_item(item: dict[str, Any]) -> list[dict[str, Any]]:
    # Simple CDR record.
    if "main_cdr" not in item:
        return [item]
    # Grouped session: keep main first, then every sub_cdr in numeric order.
    segs = []
    if isinstance(item.get("main_cdr"), dict):
        segs.append(item["main_cdr"])
    keys = sorted(
        [k for k, v in item.items() if k.startswith("sub_cdr_") and isinstance(v, dict)],
        key=lambda k: int(k.rsplit("_", 1)[-1]) if k.rsplit("_", 1)[-1].isdigit() else 9999,
    )
    segs.extend(item[k] for k in keys)
    return segs


def clean_phone(value: Any) -> str:
    s = str(value or "").strip()
    return s


def is_trunk(v: Any) -> bool:
    s = str(v or "").lower()
    return "trunk" in s or s.startswith("pjsip/trunk") or s.startswith("dahdi/")


def parse_numeric(v: Any) -> int:
    try:
        return int(float(str(v or "0")))
    except Exception:
        return 0


def classify_segments(segs: list[dict[str, Any]]) -> str:
    userfields = {str(s.get("userfield") or "").strip().lower() for s in segs}
    if "inbound" in userfields:
        return "inbound"
    if "external" in userfields:
        return "outbound"
    for s in segs:
        if s.get("src_trunk_name") and not s.get("dst_trunk_name"):
            return "inbound"
        if s.get("dst_trunk_name") and not s.get("src_trunk_name"):
            return "outbound"
    return "internal"


def choose_external_party(segs: list[dict[str, Any]], direction: str) -> tuple[str, str]:
    if direction == "inbound":
        for s in segs:
            if str(s.get("userfield") or "").lower() == "inbound":
                return clean_phone(s.get("src")), clean_phone(s.get("caller_name"))
        for s in segs:
            if s.get("src_trunk_name"):
                return clean_phone(s.get("src")), clean_phone(s.get("caller_name"))
    elif direction == "outbound":
        for s in reversed(segs):
            if str(s.get("userfield") or "").lower() == "external":
                return clean_phone(s.get("dst")), clean_phone(s.get("caller_name"))
        for s in reversed(segs):
            if s.get("dst_trunk_name"):
                return clean_phone(s.get("dst")), clean_phone(s.get("caller_name"))
    # Internal call or fallback.
    if segs:
        s = segs[-1]
        return clean_phone(s.get("dst")), clean_phone(s.get("caller_name"))
    return "", ""


def choose_answer_by(segs: list[dict[str, Any]], direction: str) -> str:
    candidates = []
    for s in segs:
        if str(s.get("disposition") or "").upper() != "ANSWERED":
            continue
        channel_ext = clean_phone(s.get("channel_ext"))
        dst_ext = clean_phone(s.get("dstchannel_ext"))
        if direction == "inbound":
            if dst_ext and not is_trunk(dst_ext):
                candidates.append(dst_ext)
        elif direction == "outbound":
            if channel_ext and not is_trunk(channel_ext):
                candidates.append(channel_ext)
        else:
            if dst_ext and not is_trunk(dst_ext):
                candidates.append(dst_ext)
            elif channel_ext and not is_trunk(channel_ext):
                candidates.append(channel_ext)
    return candidates[-1] if candidates else ""


def aggregate_recordfiles(segs: list[dict[str, Any]]) -> str:
    vals = []
    for s in segs:
        v = s.get("recordfiles")
        if v is None or v == "":
            continue
        if isinstance(v, list):
            vals.extend(str(x) for x in v)
        else:
            sv = str(v)
            # Some firmwares return a JSON list as a string.
            try:
                parsed = json.loads(sv)
                if isinstance(parsed, list):
                    vals.extend(str(x) for x in parsed)
                    continue
            except Exception:
                pass
            vals.append(sv)
    return "\n".join(dict.fromkeys(vals))


def flatten_cdr(item: dict[str, Any]) -> dict[str, Any]:
    segs = segments_from_item(item)
    direction = classify_segments(segs)
    caller, caller_name = choose_external_party(segs, direction)

    main = as_dict(item.get("main_cdr")) if "main_cdr" in item else as_dict(item)
    # For simple records, use the single record. For grouped sessions, main_cdr has reliable overall timing.
    start = str(main.get("start") or main.get("start_time") or "")
    answer = str(main.get("answer") or main.get("answer_time") or "")
    end = str(main.get("end") or main.get("end_time") or "")
    duration = str(main.get("duration") or "")
    billsec = str(main.get("billsec") or "")

    answered = any(str(s.get("disposition") or "").upper() == "ANSWERED" for s in segs)
    if answered:
        disposition = "ANSWERED"
    else:
        dispositions = [str(s.get("disposition") or "").strip() for s in segs if str(s.get("disposition") or "").strip()]
        disposition = next((d for d in reversed(dispositions) if d), "NO ANSWER")

    unique = str(item.get("cdr") or main.get("session") or main.get("uniqueid") or f"{start}-{caller}-{main.get('dst','')}")
    dst = clean_phone(main.get("dst"))
    src = clean_phone(main.get("src"))
    answer_by = choose_answer_by(segs, direction)

    if direction == "inbound":
        external_caller = caller
        internal_callee = answer_by or dst
    elif direction == "outbound":
        external_caller = src if src and not is_trunk(src) else (main.get("channel_ext") or src)
        internal_callee = caller
        caller = clean_phone(external_caller)
        dst = internal_callee
    else:
        caller = src
        dst = dst

    return {
        "unique_id": unique,
        "caller": clean_phone(caller),
        "callee": clean_phone(dst),
        "caller_name": caller_name,
        "answer_by": answer_by,
        "disposition": disposition,
        "start_time": start,
        "answer_time": answer,
        "end_time": end,
        "call_time": duration,
        "talk_time": billsec,
        "src_trunk": next((str(s.get("src_trunk_name") or "") for s in segs if s.get("src_trunk_name")), ""),
        "dst_trunk": next((str(s.get("dst_trunk_name") or "") for s in reversed(segs) if s.get("dst_trunk_name")), ""),
        "channel": next((str(s.get("channel") or "") for s in segs if s.get("channel")), ""),
        "dst_channel": next((str(s.get("dstchannel") or s.get("dst_channel") or "") for s in reversed(segs) if s.get("dstchannel") or s.get("dst_channel")), ""),
        "recordfiles": aggregate_recordfiles(segs),
        "direction": direction,
        "raw_json": json.dumps(item, ensure_ascii=False),
    }


def fetch_cdr(limit=1000, offset=0):
    params = {"format": "json", "numRecords": limit, "offset": offset}
    r = requests.get(
        UCM_CDR_URL,
        params=params,
        auth=requests.auth.HTTPDigestAuth(UCM_USER, UCM_PASS),
        verify=VERIFY_TLS,
        timeout=20,
    )
    r.raise_for_status()
    payload = r.json()
    return payload.get("cdr_root", payload if isinstance(payload, list) else [])


def sync_once() -> dict[str, int]:
    records = fetch_cdr()
    db = SessionLocal()
    inserted = 0
    updated = 0
    try:
        for raw in records:
            if not isinstance(raw, dict):
                continue
            x = flatten_cdr(raw)
            existing = db.scalar(select(Call).where(Call.unique_id == x["unique_id"]))
            if existing:
                for k, v in x.items():
                    if k != "unique_id":
                        setattr(existing, k, v)
                existing.synced_at = datetime.utcnow()
                updated += 1
            else:
                db.add(Call(**x))
                inserted += 1
        db.commit()
    finally:
        db.close()
    log.info("UCM sync: received=%s inserted=%s updated=%s", len(records), inserted, updated)
    return {"received": len(records), "inserted": inserted, "updated": updated}


def sync_loop():
    time.sleep(2)
    while True:
        try:
            sync_once()
        except Exception as exc:
            log.exception("automatic UCM sync failed: %s", exc)
        time.sleep(SYNC_INTERVAL)


@app.on_event("startup")
def startup_event():
    t = threading.Thread(target=sync_loop, name="ucm-sync", daemon=True)
    t.start()


@app.get("/api/health")
def health():
    return {"status": "ok", "ucm": UCM_CDR_URL, "sync_interval": SYNC_INTERVAL}

@app.post("/api/sync")
def sync():
    try:
        return sync_once()
    except Exception as e:
        log.exception("sync failed")
        raise HTTPException(502, f"UCM sync failed: {e}")

MISSED_DISPOSITIONS={"NO ANSWER","NOANSWER","BUSY","FAILED","CANCELLED","CHANUNAVAIL","CONGESTION","NO ANSWERED","NO ANSWERED","NOANSWERED","UNANSWERED","REJECTED"}

def normalized_disposition(v: Any) -> str:
    return " ".join(str(v or "").strip().upper().replace("_", " ").replace("-", " ").split())

def looks_external_number(v: Any) -> bool:
    s = "".join(ch for ch in str(v or "") if ch.isdigit())
    # Extensions are normally short (1-6 digits); mobile/landline/external numbers
    # are usually 7+ digits. This deliberately errs on the side of showing a missed
    # call instead of hiding a real customer call.
    return len(s) >= 7

def is_missed_call(x: Call) -> bool:
    d = normalized_disposition(x.disposition)
    if d in {"ANSWERED", "ANSWER"}:
        return False
    raw = (x.raw_json or "").lower()
    raw_inbound = any(k in raw for k in ("inbound", "src_trunk_name", "from_trunk", "in_trunk"))
    # Strong signal: an explicitly inbound, unanswered CDR.
    if x.direction == "inbound":
        return d in MISSED_DISPOSITIONS or d == "" or looks_external_number(x.caller)
    # Some UCM CDRs are classified as internal/unknown even for an external missed
    # call. Keep these when one side clearly looks like an external phone number.
    if x.direction in {"internal", "unknown", ""} and (looks_external_number(x.caller) or looks_external_number(x.callee) or raw_inbound):
        return d in MISSED_DISPOSITIONS or not d or "NO ANSWER" in d or "UNANSWER" in d
    # Last-resort compatibility for older rows with incorrect direction.
    return raw_inbound and (d in MISSED_DISPOSITIONS or not d)

@app.get("/api/dashboard")
def dashboard():
    db = SessionLocal()
    try:
        total = db.scalar(select(func.count()).select_from(Call)) or 0
        inbound = db.scalar(select(func.count()).select_from(Call).where(Call.direction == "inbound")) or 0
        outbound = db.scalar(select(func.count()).select_from(Call).where(Call.direction == "outbound")) or 0
        inbound_rows = db.scalars(select(Call).where(Call.direction == "inbound")).all()
        missed = sum(1 for x in inbound_rows if is_missed_call(x))
        contacts = db.scalar(select(func.count()).select_from(Contact)) or 0
        recent = db.scalars(select(Call).order_by(Call.start_time.desc(), Call.id.desc()).limit(15)).all()
        today=datetime.utcnow().date().isoformat()
        due_today=db.scalars(select(FollowUp).where(FollowUp.due_at.like(today+'%'), FollowUp.status=='open')).all()
        overdue=[]
        for f in db.scalars(select(FollowUp).where(FollowUp.status=='open')).all():
            if f.due_at and f.due_at < today:
                overdue.append(f)
        open_opps=db.scalar(select(func.count()).select_from(Opportunity).where(~Opportunity.stage.in_(['won','lost']))) or 0
        won_opps=db.scalar(select(func.count()).select_from(Opportunity).where(Opportunity.stage=='won')) or 0
        won_value=db.scalar(select(func.coalesce(func.sum(Opportunity.value),0)).where(Opportunity.stage=='won')) or 0
        return {"stats": {"contacts": contacts, "total": total, "inbound": inbound, "outbound": outbound, "missed": missed,
                           "followups_today":len(due_today), "followups_overdue":len(overdue), "open_opportunities":open_opps,
                           "won_opportunities":won_opps, "won_value":won_value},
                "recent": [call_json(x, db) for x in recent]}
    finally:
        db.close()

@app.get("/api/cdr")
def cdr(limit: int = Query(100, ge=1, le=1000), search: str = "", direction_filter: str = "", missed_only: bool = False):
    db = SessionLocal()
    try:
        q = select(Call).order_by(Call.start_time.desc(), Call.id.desc())
        if not missed_only:
            q = q.limit(limit)
        rows = db.scalars(q).all()
        if missed_only:
            # Do not limit before filtering: the missed calls may be mixed with many
            # answered/internal records in the newest CDR rows.
            rows = rows[:5000]
        if search:
            s = search.lower()
            rows = [r for r in rows if s in r.caller.lower() or s in r.callee.lower() or s in r.caller_name.lower()]
        if direction_filter:
            rows = [r for r in rows if r.direction == direction_filter]
        if missed_only:
            rows = [r for r in rows if is_missed_call(r)]
        return [call_json(x, db) for x in rows]
    finally:
        db.close()


STAGES = ['lead','contacted','negotiation','proposal','decision','won','lost']
CUSTOMER_STATUSES = {'lead':'سرنخ','contacted':'تماس گرفته شده','negotiation':'در حال مذاکره','proposal':'پیشنهاد قیمت','decision':'در انتظار تصمیم','customer':'مشتری','lost':'از دست رفته'}

@app.post('/api/login')
def login(payload: dict[str, Any]):
    username=str(payload.get('username','')).strip(); password=str(payload.get('password',''))
    user=CRM_USERS.get(username)
    if not user or not hmac_compare(password, user['password']):
        raise HTTPException(401, 'نام کاربری یا رمز عبور نادرست است')
    token=secrets.token_urlsafe(32)
    with AUTH_LOCK:
        AUTH_TOKENS[token]={'username':username,'role':user['role'],'extension':user.get('extension','')}
    return {'token':token,'user':AUTH_TOKENS[token]}

def hmac_compare(a,b):
    import hmac
    return hmac.compare_digest(str(a),str(b))

@app.get('/api/me')
def me(request: Request):
    return auth_user(request)

@app.get('/api/management/dashboard')
def management_dashboard(request: Request, start: str = '', end: str = ''):
    user=require_management(request)
    db=SessionLocal()
    try:
        where=[]
        if start: where.append(Call.start_time >= start)
        if end: where.append(Call.start_time <= end+' 23:59:59')
        calls=db.scalars(select(Call).where(*where) if where else select(Call)).all()
        stats={}
        for c in calls:
            ext=report_extension(c)
            if not ext: continue
            d=stats.setdefault(ext, {'extension':ext,'total':0,'answered':0,'missed':0,'talk_seconds':0,'sales':0,'sales_value':0})
            d['total']+=1
            if normalized_disposition(c.disposition) in {'ANSWERED','ANSWER'}:
                d['answered']+=1
                try: d['talk_seconds']+=int(float(c.talk_time or 0))
                except: pass
            elif is_missed_call(c): d['missed']+=1
        for ext,d in stats.items():
            d['answer_rate']=round((d['answered']/d['total']*100),1) if d['total'] else 0
            won=db.scalar(select(func.count()).select_from(Opportunity).where(Opportunity.owner_extension==ext,Opportunity.stage=='won')) or 0
            val=db.scalar(select(func.coalesce(func.sum(Opportunity.value),0)).where(Opportunity.owner_extension==ext,Opportunity.stage=='won')) or 0
            d['sales']=won; d['sales_value']=val
        opportunities=db.scalars(select(Opportunity).order_by(Opportunity.updated_at.desc()).limit(200)).all()
        return {'rows':sorted(stats.values(), key=lambda x:(x['sales_value'],x['answered']), reverse=True),
                'pipeline':{stage:db.scalar(select(func.count()).select_from(Opportunity).where(Opportunity.stage==stage)) or 0 for stage in STAGES},
                'pipeline_value':{stage:db.scalar(select(func.coalesce(func.sum(Opportunity.value),0)).where(Opportunity.stage==stage)) or 0 for stage in STAGES},
                'lost_reasons':lost_reason_report(db,start,end),
                'hourly':hourly_report(db,start,end),
                'opportunities':[opportunity_json(x,db) for x in opportunities]}
    finally: db.close()

def report_extension(c: Call) -> str:
    if c.answer_by and not is_trunk(c.answer_by) and len(''.join(ch for ch in str(c.answer_by) if ch.isdigit())) <= 6:
        return c.answer_by
    raw={}
    try:
        raw=json.loads(c.raw_json or '{}')
    except Exception:
        raw={}
    candidates=[]
    for key in ('dstchannel_ext','channel_ext','dst','connectednum','connected_num'):
        v=_first_value(raw,{key}) if raw else ''
        if v and not is_trunk(v):
            digits=''.join(ch for ch in str(v) if ch.isdigit())
            if 1 <= len(digits) <= 6:
                candidates.append(digits)
    # Prefer inbound destination for missed calls.
    return candidates[0] if candidates else ''

def lost_reason_report(db,start='',end=''):
    q=select(Opportunity).where(Opportunity.stage=='lost')
    if start: q=q.where(Opportunity.updated_at >= datetime.fromisoformat(start+'T00:00:00'))
    if end: q=q.where(Opportunity.updated_at <= datetime.fromisoformat(end+'T23:59:59'))
    out={}
    for o in db.scalars(q).all(): out[o.lost_reason or 'نامشخص']=out.get(o.lost_reason or 'نامشخص',0)+1
    return [{'reason':k,'count':v} for k,v in sorted(out.items(), key=lambda kv:-kv[1])]

def hourly_report(db,start='',end=''):
    q=select(Call)
    if start: q=q.where(Call.start_time >= start)
    if end: q=q.where(Call.start_time <= end+' 23:59:59')
    hours=[0]*24
    for c in db.scalars(q).all():
        st=str(c.start_time or '')
        try: h=int(st[11:13]); hours[h]+=1
        except: pass
    return [{'hour':i,'calls':n} for i,n in enumerate(hours)]

@app.get('/api/internal-reports')
def internal_reports(request: Request, start: str='', end: str=''):
    return management_dashboard(request,start,end)['rows']

def opportunity_json(o, db):
    c=db.get(Contact,o.contact_id)
    return {'id':o.id,'contact_id':o.contact_id,'customer':c.name if c else '', 'phone':c.phone if c else '', 'title':o.title,
            'stage':o.stage,'value':o.value,'owner_extension':o.owner_extension,'close_date':o.close_date,'lost_reason':o.lost_reason,'notes':o.notes}

class FollowUpIn(BaseModel):
    contact_id:int
    due_at:str
    note:str=''
    owner_extension:str=''

@app.get('/api/followups')
def list_followups(request:Request,status_filter:str='open'):
    user=require_sales_access(request); db=SessionLocal()
    try:
        q=select(FollowUp).order_by(FollowUp.due_at)
        if status_filter: q=q.where(FollowUp.status==status_filter)
        if user['role']=='sales' and user.get('extension'):
            q=q.where(FollowUp.owner_extension==user['extension'])
        rows=db.scalars(q).all()
        return [{'id':x.id,'contact_id':x.contact_id,'due_at':x.due_at,'note':x.note,'status':x.status,'owner_extension':x.owner_extension} for x in rows]
    finally: db.close()

@app.post('/api/followups')
def create_followup(data:FollowUpIn, request:Request):
    user=require_sales_access(request); db=SessionLocal()
    try:
        ext=data.owner_extension.strip() or user.get('extension','')
        f=FollowUp(contact_id=data.contact_id,due_at=data.due_at,note=data.note,owner_extension=ext)
        db.add(f)
        c=db.get(Contact,data.contact_id)
        if c:
            c.next_followup=data.due_at; c.followup_note=data.note; c.owner_extension=ext or c.owner_extension
        db.commit(); db.refresh(f)
        return {'id':f.id}
    finally: db.close()

@app.post('/api/followups/{followup_id}/complete')
def complete_followup(followup_id:int, request:Request):
    user=require_sales_access(request); db=SessionLocal()
    try:
        f=db.get(FollowUp,followup_id)
        if not f: raise HTTPException(404,'پیگیری پیدا نشد')
        if user['role']=='sales' and user.get('extension') and f.owner_extension!=user['extension']: raise HTTPException(403,'دسترسی ندارید')
        f.status='done'; f.completed_at=datetime.utcnow().isoformat(); db.commit(); return {'ok':True}
    finally: db.close()

class OpportunityIn(BaseModel):
    contact_id:int
    title:str=''
    stage:str='lead'
    value:int=0
    owner_extension:str=''
    close_date:str=''
    lost_reason:str=''
    notes:str=''

@app.get('/api/opportunities')
def list_opportunities(request:Request, stage:str=''):
    user=require_sales_access(request); db=SessionLocal()
    try:
        q=select(Opportunity).order_by(Opportunity.updated_at.desc())
        if stage: q=q.where(Opportunity.stage==stage)
        if user['role']=='sales' and user.get('extension'): q=q.where(Opportunity.owner_extension==user['extension'])
        return [opportunity_json(x,db) for x in db.scalars(q).all()]
    finally: db.close()

@app.post('/api/opportunities')
def create_opportunity(data:OpportunityIn, request:Request):
    user=require_sales_access(request); db=SessionLocal()
    try:
        ext=data.owner_extension.strip() or user.get('extension','')
        o=Opportunity(contact_id=data.contact_id,title=data.title,stage=data.stage if data.stage in STAGES else 'lead',value=int(data.value or 0),owner_extension=ext,close_date=data.close_date,lost_reason=data.lost_reason,notes=data.notes)
        db.add(o); db.commit(); db.refresh(o); return opportunity_json(o,db)
    finally: db.close()

@app.put('/api/opportunities/{opportunity_id}')
def update_opportunity(opportunity_id:int, data:OpportunityIn, request:Request):
    user=require_sales_access(request); db=SessionLocal()
    try:
        o=db.get(Opportunity,opportunity_id)
        if not o: raise HTTPException(404,'فرصت فروش پیدا نشد')
        if user['role']=='sales' and user.get('extension') and o.owner_extension!=user['extension']: raise HTTPException(403,'دسترسی ندارید')
        o.contact_id=data.contact_id; o.title=data.title; o.stage=data.stage if data.stage in STAGES else 'lead'; o.value=int(data.value or 0); o.close_date=data.close_date; o.lost_reason=data.lost_reason; o.notes=data.notes
        db.commit(); db.refresh(o); return opportunity_json(o,db)
    finally: db.close()

@app.post('/api/calls/{call_id}/outcome')
def call_outcome(call_id:int, payload:dict[str,Any], request:Request):
    user=require_sales_access(request); db=SessionLocal()
    try:
        c=db.get(Call,call_id)
        if not c: raise HTTPException(404,'تماس پیدا نشد')
        c.outcome=str(payload.get('outcome','')).strip(); c.call_notes=str(payload.get('notes','')).strip(); db.commit(); return {'ok':True,'outcome':c.outcome}
    finally: db.close()

@app.get('/api/action-center')
def action_center(request:Request):
    user=require_sales_access(request); db=SessionLocal()
    try:
        today=datetime.utcnow().date().isoformat()
        q=select(FollowUp).where(FollowUp.status=='open').order_by(FollowUp.due_at)
        if user['role']=='sales' and user.get('extension'): q=q.where(FollowUp.owner_extension==user['extension'])
        items=[]
        for f in db.scalars(q).all():
            c=db.get(Contact,f.contact_id)
            items.append({'id':f.id,'contact_id':f.contact_id,'customer':c.name if c else '', 'phone':c.phone if c else '', 'due_at':f.due_at,'note':f.note,'overdue':bool(f.due_at and f.due_at < today)})
        return {'items':items,'overdue':sum(1 for x in items if x['overdue']),'today':sum(1 for x in items if x['due_at'].startswith(today))}
    finally: db.close()

def split_phones(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        vals = value
    else:
        raw = str(value).replace("\r", "\n")
        for sep in [";", ",", "|", "\t"]:
            raw = raw.replace(sep, "\n")
        vals = raw.split("\n")
    out = []
    for v in vals:
        s = str(v or "").strip()
        if s and s not in out:
            out.append(s)
    return out

def contact_json(x: Contact) -> dict[str, Any]:
    try:
        extra = json.loads(x.phones or "[]")
        if not isinstance(extra, list):
            extra = split_phones(extra)
    except Exception:
        extra = split_phones(x.phones)
    phones = []
    for p in [x.phone, *extra]:
        p = str(p or "").strip()
        if p and p not in phones:
            phones.append(p)
    return {"id":x.id,"name":x.name,"phone":x.phone,"phones":phones,"company":x.company,"notes":x.notes,
            "owner_extension":x.owner_extension,"customer_status":x.customer_status,"score":x.score,
            "next_followup":x.next_followup,"followup_note":x.followup_note,"lost_reason":x.lost_reason}

@app.get("/api/contacts")
def contacts(search: str = "", request: Request = None):
    user = auth_user(request) if request is not None else {'role':'full','extension':''}
    db = SessionLocal()
    try:
        q=select(Contact).order_by(Contact.name)
        if user.get('role')=='sales' and user.get('extension'):
            q=q.where(Contact.owner_extension==user['extension'])
        rows = db.scalars(q).all()
        if search:
            ss = search.lower()
            rows = [x for x in rows if ss in x.name.lower() or ss in x.phone.lower() or ss in (x.phones or "").lower() or ss in x.company.lower()]
        return [contact_json(x) for x in rows]
    finally: db.close()

class ContactIn(BaseModel):
    name: str
    phone: str
    phones: list[str] = []
    company: str = ""
    notes: str = ""
    owner_extension: str = ""
    customer_status: str = "lead"
    score: int = 50
    next_followup: str = ""
    followup_note: str = ""
    lost_reason: str = ""

def save_contact(data: ContactIn, contact_id: int | None = None):
    primary = str(data.phone or "").strip()
    if not primary:
        raise HTTPException(422, "شماره اصلی الزامی است")
    extras = []
    for p in data.phones:
        p = str(p or "").strip()
        if p and p != primary and p not in extras:
            extras.append(p)
    db = SessionLocal()
    try:
        row = db.get(Contact, contact_id) if contact_id is not None else db.scalar(select(Contact).where(Contact.phone == primary))
        if contact_id is not None and row is None:
            raise HTTPException(404, "مخاطب پیدا نشد")
        owner = db.scalar(select(Contact).where(Contact.phone == primary))
        if owner and row and owner.id != row.id:
            raise HTTPException(409, "این شماره اصلی قبلاً برای مخاطب دیگری ثبت شده است")
        if owner and row is None:
            row = owner
        if row is None:
            row = Contact(name=data.name.strip(), phone=primary, company=data.company.strip(), notes=data.notes.strip(), phones=json.dumps(extras, ensure_ascii=False),
                          owner_extension=data.owner_extension.strip(), customer_status=data.customer_status.strip() or 'lead',
                          score=max(0,min(100,int(data.score or 0))), next_followup=data.next_followup.strip(),
                          followup_note=data.followup_note.strip(), lost_reason=data.lost_reason.strip())
        else:
            row.name = data.name.strip(); row.phone = primary; row.company = data.company.strip(); row.notes = data.notes.strip(); row.phones = json.dumps(extras, ensure_ascii=False)
            row.owner_extension = data.owner_extension.strip(); row.customer_status = data.customer_status.strip() or 'lead'; row.score=max(0,min(100,int(data.score or 0)))
            row.next_followup=data.next_followup.strip(); row.followup_note=data.followup_note.strip(); row.lost_reason=data.lost_reason.strip()
        db.add(row); db.commit(); db.refresh(row)
        return contact_json(row)
    finally: db.close()

@app.post("/api/contacts")
def add_contact(data: ContactIn):
    return save_contact(data)

@app.put("/api/contacts/{contact_id}")
def update_contact(contact_id: int, data: ContactIn):
    return save_contact(data, contact_id)

@app.delete("/api/contacts/{contact_id}")
def delete_contact(contact_id: int):
    db = SessionLocal()
    try:
        row = db.get(Contact, contact_id)
        if not row: raise HTTPException(404, "مخاطب پیدا نشد")
        db.delete(row); db.commit(); return {"ok": True}
    finally: db.close()

@app.post("/api/contacts/import")
def import_contacts(file: UploadFile = File(...)):
    import csv, io, re
    name = (file.filename or "").lower()
    raw = file.file.read()
    records=[]
    try:
        if name.endswith(".xlsx"):
            from openpyxl import load_workbook
            wb = load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
            ws = wb.active
            rows = list(ws.iter_rows(values_only=True))
            if not rows:
                return {"inserted":0,"updated":0,"errors":[],"total_rows":0}
            headers=[str(x or "").replace("\ufeff","").strip().lower().replace(" ","_") for x in rows[0]]
            records=[dict(zip(headers,r)) for r in rows[1:] if any(v not in (None,"") for v in r)]
        else:
            txt=raw.decode("utf-8-sig", errors="replace")
            sample=txt[:8192]
            # Support comma, semicolon, TAB and pipe-delimited files, including .txt/.tsv.
            try:
                dialect=csv.Sniffer().sniff(sample, delimiters=",;\t|")
                delim=dialect.delimiter
            except Exception:
                delim="\t" if "\t" in sample else ","
            reader=csv.DictReader(io.StringIO(txt), delimiter=delim)
            records=list(reader)
    except Exception as exc:
        raise HTTPException(400, f"خطا در خواندن فایل: {exc}")

    def norm_key(k):
        s=str(k or "").replace("\ufeff","").strip().lower()
        s=s.replace(" ","_").replace("-","_")
        return s

    def norm_digits(v):
        s=str(v or "").strip()
        # Excel may parse a phone as a number and append .0
        if re.fullmatch(r"[0-9]+\.0", s): s=s[:-2]
        return s

    def pick(rec,*keys):
        norm={norm_key(k):v for k,v in rec.items()}
        wanted={norm_key(k) for k in keys}
        for k,v in norm.items():
            if k in wanted and v not in (None,""):
                return norm_digits(v)
        return ""

    inserted=updated=0; errors=[]; db=SessionLocal()
    try:
        for idx,rec in enumerate(records,start=2):
            try:
                name_v=pick(rec,"name","نام","نام_مشتری","customer","customer_name")
                phone_v=pick(rec,"phone","شماره","شماره_تلفن","mobile","mobile_1","phone_1")
                allnums=[]
                for key,val in rec.items():
                    k=norm_key(key)
                    if ("phone" in k or "mobile" in k or "شماره" in k) and val not in (None,""):
                        allnums.extend(split_phones(norm_digits(val)))
                allnums=list(dict.fromkeys([p for p in allnums if p]))
                if not phone_v and allnums: phone_v=allnums[0]
                # Make the explicit phone column the primary phone and keep remaining
                # phone/mobile columns as additional numbers.
                nums=list(dict.fromkeys([phone_v, *allnums])) if phone_v else allnums
                nums=[p for p in nums if p]
                if not nums:
                    errors.append({"row":idx,"error":"شماره تلفن ندارد"}); continue
                phone_v=nums[0]; extras=[p for p in nums[1:] if p!=phone_v]
                company=pick(rec,"company","شرکت")
                notes=pick(rec,"notes","یادداشت","توضیحات")
                owner=pick(rec,"owner_extension","داخلی","مسئول","sales_extension")
                status_v=pick(rec,"customer_status","status","وضعیت") or 'lead'
                try: score_v=int(float(pick(rec,"score","امتیاز") or 50))
                except: score_v=50
                next_fu=pick(rec,"next_followup","پیگیری_بعدی","پیگیری")
                fu_note=pick(rec,"followup_note","توضیح_پیگیری")

                row=db.scalar(select(Contact).where(Contact.phone==phone_v))
                if row:
                    row.name=name_v or row.name
                    row.company=company or row.company
                    row.notes=notes or row.notes
                    row.owner_extension=owner or row.owner_extension
                    row.customer_status=status_v or row.customer_status
                    row.score=max(0,min(100,score_v))
                    row.next_followup=next_fu or row.next_followup
                    row.followup_note=fu_note or row.followup_note
                    try: old=json.loads(row.phones or "[]")
                    except Exception: old=split_phones(row.phones)
                    row.phones=json.dumps(list(dict.fromkeys([*old,*extras])),ensure_ascii=False)
                    updated+=1
                else:
                    # A secondary number belonging to another contact must not create a
                    # second contact with duplicate business data.
                    owner=None
                    for candidate in db.scalars(select(Contact)).all():
                        if phone_v in contact_json(candidate)["phones"]:
                            owner=candidate; break
                    if owner:
                        owner.name=name_v or owner.name
                        owner.company=company or owner.company
                        owner.notes=notes or owner.notes
                        old=contact_json(owner)["phones"][1:]
                        owner.phones=json.dumps(list(dict.fromkeys([*old,*extras])),ensure_ascii=False)
                        updated+=1
                    else:
                        db.add(Contact(name=name_v,phone=phone_v,company=company,notes=notes,phones=json.dumps(extras,ensure_ascii=False)))
                        inserted+=1
            except Exception as exc:
                errors.append({"row":idx,"error":str(exc)})
        db.commit()
    finally:
        db.close()
    return {"inserted":inserted,"updated":updated,"errors":errors,"total_rows":len(records)}

@app.get("/api/customer/{phone}")
def customer(phone: str):
    db = SessionLocal()
    try:
        contact = db.scalar(select(Contact).where(Contact.phone == phone))
        if not contact:
            for c in db.scalars(select(Contact)).all():
                if phone in split_phones(c.phones): contact = c; break
        if contact:
            from sqlalchemy import or_
            conditions=[(Call.caller == p) | (Call.callee == p) for p in contact_json(contact)["phones"]]
            rows=db.scalars(select(Call).where(or_(*conditions)).order_by(Call.start_time.desc(), Call.id.desc()).limit(100)).all()
        else:
            rows=db.scalars(select(Call).where((Call.caller == phone) | (Call.callee == phone)).order_by(Call.start_time.desc(), Call.id.desc()).limit(100)).all()
        return {"contact": None if not contact else contact_json(contact), "calls": [call_json(x,db) for x in rows]}
    finally: db.close()

@app.get("/api/recording")
def recording(filedir: str, filename: str):
    r = requests.get(UCM_REC_URL, params={"filedir": filedir, "filename": filename}, auth=requests.auth.HTTPDigestAuth(UCM_USER, UCM_PASS), verify=VERIFY_TLS, timeout=30)
    if r.status_code != 200:
        raise HTTPException(r.status_code, "Recording unavailable")
    return {"content_type": r.headers.get("content-type", "audio/wav"), "size": len(r.content)}

@app.get("/api/recording/raw")
def recording_raw(filedir: str = "monitor", filename: str = ""):
    from fastapi.responses import Response
    if not filename:
        raise HTTPException(400, "filename is required")
    params={"filedir": filedir or "monitor", "filename": filename}
    r = requests.get(UCM_REC_URL, params=params, auth=requests.auth.HTTPDigestAuth(UCM_USER,UCM_PASS), verify=VERIFY_TLS, timeout=30)
    if r.status_code != 200:
        raise HTTPException(r.status_code, f"Recording unavailable ({r.text[:200]})")
    ctype=(r.headers.get("content-type") or "application/octet-stream").split(";")[0].strip()
    # Some UCM firmwares return octet-stream for WAV; browsers still need an audio MIME.
    if ctype in {"application/octet-stream","binary/octet-stream"} and filename.lower().endswith(".wav"):
        ctype="audio/wav"
    return Response(content=r.content, media_type=ctype, headers={"Content-Disposition": f'inline; filename="{os.path.basename(filename)}"', "Accept-Ranges":"bytes"})

@app.get("/api/click-to-call")
def click_to_call(number: str):
    return {"status": "ready", "number": number, "message": "Click-to-Call endpoint reserved for UCM control API integration."}


def call_json(x: Call, db):
    phone = x.caller if x.direction == "inbound" else x.callee
    if x.direction == "internal": phone = x.caller or x.callee
    contact = db.scalar(select(Contact).where(Contact.phone == phone))
    if not contact and phone:
        for c in db.scalars(select(Contact)).all():
            if phone in split_phones(c.phones): contact = c; break
    recordings=[]
    raw_values=[]
    try:
        parsed=json.loads(x.recordfiles or "[]")
        if isinstance(parsed,list): raw_values=[str(v) for v in parsed]
        elif isinstance(parsed,dict):
            for key in ("filename","recordfile","recordfiles","file","files"):
                val=parsed.get(key)
                if isinstance(val,list): raw_values.extend(str(v) for v in val)
                elif val: raw_values.append(str(val))
    except Exception:
        raw_values=[]
    if not raw_values:
        raw_values=split_phones(x.recordfiles)
    for raw in raw_values:
        raw=str(raw or "").strip()
        if not raw: continue
        # Normalize common UCM formats: monitor/file.wav, /monitor/file.wav, file.wav,
        # monitor@file.wav, and URL/query-style values.
        filedir="monitor"; filename=raw
        if "filedir=" in raw and "filename=" in raw:
            try:
                from urllib.parse import parse_qs, urlparse
                q=parse_qs(urlparse(raw).query); filedir=q.get("filedir",["monitor"])[0]; filename=q.get("filename",[raw])[0]
            except Exception: pass
        elif "@" in raw:
            parts=[p.strip(" /") for p in raw.split("@") if p.strip(" /")]
            if len(parts)>=2: filedir,filename=parts[0],parts[-1]
        elif "/" in raw:
            parts=[p for p in raw.split("/") if p]
            if len(parts)>=2: filedir,filename=parts[-2],parts[-1]
        filename=filename.strip("/ ")
        if not filename: continue
        recordings.append({"label":filename,"filedir":filedir or "monitor","filename":filename,"url":f"/api/recording/raw?filedir={requests.utils.quote(filedir or "monitor",safe="")}&filename={requests.utils.quote(filename,safe="")}"})
    return {
        "id": x.id,
        "unique_id": x.unique_id,
        "caller": x.caller,
        "callee": x.callee,
        "caller_name": contact.name if contact else x.caller_name,
        "customer": contact.name if contact else "",
        "answer_by": x.answer_by,
        "disposition": x.disposition,
        "start_time": x.start_time,
        "answer_time": x.answer_time,
        "end_time": x.end_time,
        "call_time": x.call_time,
        "talk_time": x.talk_time,
        "direction": x.direction,
        "recordfiles": x.recordfiles,
        "recordings": recordings,
        "outcome": x.outcome,
        "call_notes": x.call_notes,
        "src_trunk": x.src_trunk,
        "dst_trunk": x.dst_trunk,
    }
