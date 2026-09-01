import csv
import io
import json
import logging
import socket
import threading
from datetime import datetime
from typing import Any, Callable

log = logging.getLogger("ucmcrm.live")

HOST = "0.0.0.0"
PORT = 10000

# ساختار CDR که UCM در CDR Real-time Output می‌فرستد
CDR_FIELDS = [
    "AcctId",
    "accountcode",
    "src",
    "dst",
    "dcontext",
    "clid",
    "channel",
    "dstchannel",
    "lastapp",
    "lastdata",
    "start",
    "answer",
    "end",
    "duration",
    "billsec",
    "disposition",
    "amaflags",
    "uniqueid",
    "userfield",
    "channel_ext",
    "dstchannel_ext",
    "service",
    "caller_name",
    "recordfiles",
    "dstanswer",
    "chanext",
    "dstchanext",
    "session",
    "action_owner",
    "action_type",
    "src_trunk_name",
    "dst_trunk_name",
]


def parse_cdr_csv(text: str) -> dict[str, Any] | None:
    """
    Parse one Grandstream UCM6202 real-time CDR CSV row.
    """
    text = text.strip().strip("\x00")

    if not text:
        return None

    try:
        reader = csv.reader(
            io.StringIO(text),
            delimiter=",",
            quotechar="'",
            skipinitialspace=False,
        )
        row = next(reader, None)
    except Exception:
        log.exception("CSV parsing failed")
        return None

    if not row:
        return None

    row = [str(x).strip() for x in row]

    # بعضی نسخه‌ها ممکن است یک ستون اضافه/کم داشته باشند
    data: dict[str, Any] = {}

    for idx, field in enumerate(CDR_FIELDS):
        data[field] = row[idx] if idx < len(row) else ""

    # اگر ستون‌های اضافه بودند، نگهشان می‌داریم
    if len(row) > len(CDR_FIELDS):
        data["_extra"] = row[len(CDR_FIELDS):]

    return data


def normalize_live_cdr(data: dict[str, Any]) -> dict[str, Any]:
    """
    اطلاعات CDR خام را به ساختار قابل استفاده برای CRM تبدیل می‌کند.
    """
    src = (data.get("src") or "").strip()
    dst = (data.get("dst") or "").strip()
    userfield = (data.get("userfield") or "").strip()
    context = (data.get("dcontext") or "").strip()
    channel_ext = (data.get("channel_ext") or "").strip()
    dstchannel_ext = (data.get("dstchannel_ext") or "").strip()
    disposition = (data.get("disposition") or "").strip()
    session = (data.get("session") or "").strip()
    recordfiles = (data.get("recordfiles") or "").strip()
    action_type = (data.get("action_type") or "").strip()
    src_trunk = (data.get("src_trunk_name") or "").strip()
    dst_trunk = (data.get("dst_trunk_name") or "").strip()

    # نوع تماس
    if userfield.lower() == "inbound":
        call_type = "inbound"
    elif userfield.lower() == "external":
        call_type = "outbound"
    elif userfield.lower() == "internal":
        call_type = "internal"
    else:
        if "ext-did" in context:
            call_type = "inbound"
        elif "outbound-route" in context:
            call_type = "outbound"
        elif src_trunk or dst_trunk:
            call_type = "external"
        else:
            call_type = "unknown"

    # برای تماس ورودی، src شماره تماس‌گیرنده است.
    # برای تماس خروجی، dst شماره مقصد است.
    if call_type == "inbound":
        caller_number = src
        destination = dst
    elif call_type == "outbound":
        caller_number = src
        destination = dst
    else:
        caller_number = src
        destination = dst

    # داخلی را از channel_ext / dstchannel_ext استخراج می‌کنیم
    extension = ""

    if call_type == "inbound":
        # trunk -> IVR/RingGroup/Extension
        if dstchannel_ext and not dstchannel_ext.startswith("trunk"):
            extension = dstchannel_ext
        elif dst and dst.isdigit():
            extension = dst
    elif call_type == "outbound":
        # Extension -> trunk
        if channel_ext and not channel_ext.startswith("trunk"):
            extension = channel_ext

    # وضعیت
    if disposition.upper() == "ANSWERED":
        status = "answered"
    elif disposition.upper() == "NO ANSWER":
        status = "no_answer"
    elif disposition.upper() == "BUSY":
        status = "busy"
    elif disposition.upper() == "FAILED":
        status = "failed"
    else:
        status = (disposition or "unknown").lower().replace(" ", "_")

    # duration / billsec
    try:
        duration = int(float(data.get("duration") or 0))
    except Exception:
        duration = 0

    try:
        billsec = int(float(data.get("billsec") or 0))
    except Exception:
        billsec = 0

    return {
        "account_id": data.get("AcctId", ""),
        "src": src,
        "dst": dst,
        "caller_number": caller_number,
        "destination": destination,
        "caller_name": data.get("caller_name", ""),
        "clid": data.get("clid", ""),
        "call_type": call_type,
        "status": status,
        "disposition": disposition,
        "extension": extension,
        "channel_ext": channel_ext,
        "dstchannel_ext": dstchannel_ext,
        "context": context,
        "action_type": action_type,
        "session": session,
        "channel": data.get("channel", ""),
        "dstchannel": data.get("dstchannel", ""),
        "start": data.get("start", ""),
        "answer": data.get("answer", ""),
        "end": data.get("end", ""),
        "duration": duration,
        "billsec": billsec,
        "recordfiles": recordfiles,
        "src_trunk_name": src_trunk,
        "dst_trunk_name": dst_trunk,
        "raw_cdr": data,
    }


def decode_payload(data: bytes) -> Any:
    text = data.decode("utf-8", errors="replace").strip()

    if not text:
        return {}

    # JSON
    try:
        return json.loads(text)
    except Exception:
        pass

    # Grandstream Real-Time CDR CSV
    parsed = parse_cdr_csv(text)
    if parsed:
        return {
            "type": "cdr",
            "data": parsed,
            "normalized": normalize_live_cdr(parsed),
        }

    # key=value / key:value
    result = {}

    for line in text.replace("\r", "\n").split("\n"):
        line = line.strip()

        if not line:
            continue

        if "=" in line:
            key, value = line.split("=", 1)
            result[key.strip()] = value.strip()

        elif ":" in line:
            key, value = line.split(":", 1)
            result[key.strip()] = value.strip()

    if result:
        return result

    return {"raw": text}


def handle_client(
    conn: socket.socket,
    addr,
    on_event: Callable[[dict], None],
):
    try:
        conn.settimeout(30)
        chunks = []

        while True:
            try:
                data = conn.recv(4096)
            except socket.timeout:
                break

            if not data:
                break

            chunks.append(data)

            # معمولاً UCM یک اتصال برای هر CDR باز می‌کند.
            # اگر اتصال بسته نشده باشد، برای جلوگیری از انتظار بی‌مورد
            # با رسیدن newline هم می‌توانیم ادامه دهیم.
            current = b"".join(chunks)

            stripped = current.strip()

            if stripped.startswith(b"{") and stripped.endswith(b"}"):
                break

            if stripped.startswith(b"[") and stripped.endswith(b"]"):
                break

        raw = b"".join(chunks)

        if not raw:
            log.warning(
                "Empty live connection from %s:%s",
                addr[0],
                addr[1],
            )
            return

        payload = decode_payload(raw)

        event = {
            "received_at": datetime.now().isoformat(),
            "source_ip": addr[0],
            "payload": payload,
            "raw": raw.decode("utf-8", errors="replace").strip(),
        }

        if isinstance(payload, dict) and payload.get("normalized"):
            log.info(
                "UCM LIVE CALL: type=%s caller=%s dst=%s ext=%s status=%s session=%s",
                payload["normalized"].get("call_type"),
                payload["normalized"].get("caller_number"),
                payload["normalized"].get("destination"),
                payload["normalized"].get("extension"),
                payload["normalized"].get("status"),
                payload["normalized"].get("session"),
            )

        log.info("UCM LIVE EVENT: %s", event)

        on_event(event)

        try:
            conn.sendall(b"OK\r\n")
        except Exception:
            pass

    except Exception:
        log.exception("Live client error")

    finally:
        try:
            conn.close()
        except Exception:
            pass


def start_live_listener(on_event: Callable[[dict], None]):
    def server_loop():
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        server.bind((HOST, PORT))
        server.listen(20)

        log.info(
            "Live UCM TCP listener started on %s:%s",
            HOST,
            PORT,
        )

        while True:
            try:
                conn, addr = server.accept()

                log.info(
                    "UCM live connection from %s:%s",
                    addr[0],
                    addr[1],
                )

                threading.Thread(
                    target=handle_client,
                    args=(conn, addr, on_event),
                    daemon=True,
                ).start()

            except Exception:
                log.exception("Live listener error")

    threading.Thread(
        target=server_loop,
        name="ucm-live-listener",
        daemon=True,
    ).start()
