import ast
import json
import logging
import os
import threading
import time
from collections import deque
from threading import Lock
from .live_listener import start_live_listener
from datetime import datetime
from typing import Any

import requests
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, String, Integer, DateTime, Text, func, select
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
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

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


def init_db(retries=30, delay=2):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            Base.metadata.create_all(engine)
            log.info("database ready")
            return
        except OperationalError as exc:
            last_error = exc
            log.warning("database not ready (attempt %s/%s): %s", attempt, retries, exc)
            time.sleep(delay)
    raise last_error

init_db()

app = FastAPI(title="UCM6202 CRM API", version="1.2.0")


# =========================================================
# REAL-TIME UCM EVENTS
# =========================================================

LIVE_EVENTS = deque(maxlen=100)
LIVE_CALLS = {}
LIVE_LOCK = Lock()

def handle_live_event(event):
    payload = event.get("payload") or {}

    if isinstance(payload, dict):
        normalized = payload.get("normalized") or {}

        if normalized:
            session = normalized.get("session") or ""

            live_call = {
                "received_at": event.get("received_at"),
                "source_ip": event.get("source_ip"),
                **normalized,
            }

            with LIVE_LOCK:
                LIVE_EVENTS.appendleft(event)

                if session:
                    LIVE_CALLS[session] = live_call

                    # بیشتر از 50 تماس زنده نگه ندار
                    if len(LIVE_CALLS) > 50:
                        oldest = next(iter(LIVE_CALLS))
                        LIVE_CALLS.pop(oldest, None)

            log.info(
                "Live call stored: session=%s caller=%s type=%s dst=%s ext=%s",
                session,
                normalized.get("caller_number"),
                normalized.get("call_type"),
                normalized.get("destination"),
                normalized.get("extension"),
            )
            return

    with LIVE_LOCK:
        LIVE_EVENTS.appendleft(event)

    log.info("Live event stored")

@app.on_event("startup")
def start_ucm_live_listener():
    start_live_listener(handle_live_event)


@app.get("/api/live")
def get_live_events():
    with LIVE_LOCK:
        events = list(LIVE_EVENTS)

    return {
        "status": "ok",
        "events": events,
    }


@app.get("/api/live")
def get_live_events():
    with LIVE_LOCK:
        events = list(LIVE_EVENTS)
        calls = list(LIVE_CALLS.values())

    return {
        "status": "ok",
        "events": events,
        "calls": calls,
    }
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


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

@app.get("/api/dashboard")
def dashboard():
    db = SessionLocal()
    try:
        total = db.scalar(select(func.count()).select_from(Call)) or 0
        inbound = db.scalar(select(func.count()).select_from(Call).where(Call.direction == "inbound")) or 0
        outbound = db.scalar(select(func.count()).select_from(Call).where(Call.direction == "outbound")) or 0
        missed = db.scalar(
            select(func.count()).select_from(Call).where(
                (Call.direction == "inbound") & (Call.disposition.in_(["NO ANSWER", "BUSY", "FAILED", "CANCELLED"]))
            )
        ) or 0
        contacts = db.scalar(select(func.count()).select_from(Contact)) or 0
        recent = db.scalars(select(Call).order_by(Call.start_time.desc(), Call.id.desc()).limit(15)).all()
        return {"stats": {"contacts": contacts, "total": total, "inbound": inbound, "outbound": outbound, "missed": missed}, "recent": [call_json(x, db) for x in recent]}
    finally:
        db.close()

@app.get("/api/cdr")
def cdr(limit: int = Query(100, ge=1, le=1000), search: str = "", direction_filter: str = ""):
    db = SessionLocal()
    try:
        q = select(Call).order_by(Call.start_time.desc(), Call.id.desc()).limit(limit)
        rows = db.scalars(q).all()
        if search:
            s = search.lower()
            rows = [r for r in rows if s in r.caller.lower() or s in r.callee.lower() or s in r.caller_name.lower()]
        if direction_filter:
            rows = [r for r in rows if r.direction == direction_filter]
        return [call_json(x, db) for x in rows]
    finally:
        db.close()

@app.get("/api/contacts")
def contacts(search: str = ""):
    db = SessionLocal()
    try:
        rows = db.scalars(select(Contact).order_by(Contact.name)).all()
        if search:
            s = search.lower(); rows = [x for x in rows if s in x.name.lower() or s in x.phone.lower()]
        return [{"id":x.id,"name":x.name,"phone":x.phone,"company":x.company,"notes":x.notes} for x in rows]
    finally: db.close()

class ContactIn(BaseModel):
    name: str
    phone: str
    company: str = ""
    notes: str = ""

@app.post("/api/contacts")
def add_contact(data: ContactIn):
    db = SessionLocal()
    try:
        row = db.scalar(select(Contact).where(Contact.phone == data.phone))
        if row:
            row.name = data.name
            row.company = data.company
            row.notes = data.notes
            db.commit(); db.refresh(row)
        else:
            row = Contact(**data.model_dump()); db.add(row); db.commit(); db.refresh(row)
        return {"id": row.id, "name": row.name, "phone": row.phone, "company": row.company, "notes": row.notes}
    finally: db.close()

@app.get("/api/customer/{phone}")
def customer(phone: str):
    db = SessionLocal()
    try:
        contact = db.scalar(select(Contact).where(Contact.phone == phone))
        rows = db.scalars(select(Call).where((Call.caller == phone) | (Call.callee == phone)).order_by(Call.start_time.desc(), Call.id.desc()).limit(100)).all()
        return {"contact": None if not contact else {"id":contact.id,"name":contact.name,"phone":contact.phone,"company":contact.company,"notes":contact.notes}, "calls": [call_json(x,db) for x in rows]}
    finally: db.close()

@app.get("/api/recording")
def recording(filedir: str, filename: str):
    r = requests.get(UCM_REC_URL, params={"filedir": filedir, "filename": filename}, auth=requests.auth.HTTPDigestAuth(UCM_USER, UCM_PASS), verify=VERIFY_TLS, timeout=30)
    if r.status_code != 200:
        raise HTTPException(r.status_code, "Recording unavailable")
    return {"content_type": r.headers.get("content-type", "audio/wav"), "size": len(r.content)}

@app.get("/api/recording/raw")
def recording_raw(filedir: str, filename: str):
    from fastapi.responses import Response
    r = requests.get(UCM_REC_URL, params={"filedir": filedir, "filename": filename}, auth=requests.auth.HTTPDigestAuth(UCM_USER, UCM_PASS), verify=VERIFY_TLS, timeout=30)
    if r.status_code != 200:
        raise HTTPException(r.status_code, "Recording unavailable")
    return Response(content=r.content, media_type=r.headers.get("content-type", "audio/wav"))

@app.get("/api/click-to-call")
def click_to_call(number: str):
    return {"status": "ready", "number": number, "message": "Click-to-Call endpoint reserved for UCM control API integration."}


def call_json(x: Call, db):
    phone = x.caller if x.direction == "inbound" else x.callee
    if x.direction == "internal":
        phone = x.caller or x.callee
    contact = db.scalar(select(Contact).where(Contact.phone == phone))
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
        "src_trunk": x.src_trunk,
        "dst_trunk": x.dst_trunk,
    }
