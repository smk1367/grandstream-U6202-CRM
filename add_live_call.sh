#!/bin/bash
set -e

echo "=== Backup ==="

cp backend/app/main.py backend/app/main.py.bak
cp frontend/index.html frontend/index.html.bak

echo "=== Creating live_listener.py ==="

cat > backend/app/live_listener.py <<'PY'
import json
import logging
import socket
import threading
from datetime import datetime
from typing import Any, Callable

log = logging.getLogger("ucmcrm.live")

HOST = "0.0.0.0"
PORT = 10000


def decode_payload(data: bytes) -> Any:
    text = data.decode("utf-8", errors="replace").strip()

    if not text:
        return {}

    # JSON
    try:
        return json.loads(text)
    except Exception:
        pass

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

            current = b"".join(chunks)

            # JSON معمولاً با این حالت بسته می‌شود
            stripped = current.strip()

            if stripped.startswith(b"{") and stripped.endswith(b"}"):
                break

            if stripped.startswith(b"[") and stripped.endswith(b"]"):
                break

            if b"\r\n\r\n" in current:
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
PY

echo "=== Patching main.py ==="

python3 <<'PY'
from pathlib import Path

p = Path("backend/app/main.py")
text = p.read_text()

# ---------------------------------------------------------
# Imports
# ---------------------------------------------------------

if "from collections import deque" not in text:
    marker = "import logging"
    if marker in text:
        text = text.replace(
            marker,
            "import logging\nfrom collections import deque\nfrom threading import Lock",
            1
        )
    else:
        text = (
            "from collections import deque\n"
            "from threading import Lock\n"
            + text
        )

if "from .live_listener import start_live_listener" not in text:
    if "from threading import Lock" in text:
        text = text.replace(
            "from threading import Lock",
            "from threading import Lock\nfrom .live_listener import start_live_listener",
            1
        )
    else:
        text = (
            "from .live_listener import start_live_listener\n"
            + text
        )

# ---------------------------------------------------------
# Live event storage
# ---------------------------------------------------------

live_code = r'''

# =========================================================
# REAL-TIME UCM EVENTS
# =========================================================

LIVE_EVENTS = deque(maxlen=100)
LIVE_LOCK = Lock()


def handle_live_event(event):
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


@app.delete("/api/live")
def clear_live_events():
    with LIVE_LOCK:
        LIVE_EVENTS.clear()

    return {
        "status": "ok"
    }

'''

if "def handle_live_event(event):" not in text:
    # Add after FastAPI app declaration
    marker = "app = FastAPI"

    pos = text.find(marker)

    if pos == -1:
        raise SystemExit(
            "ERROR: app = FastAPI not found in backend/app/main.py"
        )

    # Find end of that declaration line
    line_end = text.find("\n", pos)

    if line_end == -1:
        line_end = len(text)

    text = (
        text[:line_end + 1]
        + live_code
        + text[line_end + 1:]
    )

p.write_text(text)

# ---------------------------------------------------------
# Basic validation
# ---------------------------------------------------------

compile_text = compile(
    text,
    str(p),
    "exec"
)

print("main.py patched successfully")
PY

echo "=== Patching frontend/index.html ==="

python3 <<'PY'
from pathlib import Path

p = Path("frontend/index.html")
text = p.read_text()

css = r'''
/* =========================================================
   REAL-TIME UCM CALL POPUP
   ========================================================= */

#liveCallBox {
    display: none;
    position: fixed;
    top: 20px;
    right: 20px;
    z-index: 99999;
    width: min(430px, calc(100vw - 40px));
    background: #ffffff;
    border-radius: 18px;
    padding: 22px;
    box-shadow: 0 18px 60px rgba(0,0,0,.22);
    border: 2px solid #16a34a;
    direction: rtl;
}

#liveCallBox.show {
    display: block;
    animation: liveCallIn .25s ease-out;
}

@keyframes liveCallIn {
    from {
        opacity: 0;
        transform: translateY(-15px);
    }
    to {
        opacity: 1;
        transform: translateY(0);
    }
}

#liveCallBox .live-title {
    font-size: 20px;
    font-weight: 700;
    margin-bottom: 16px;
}

#liveCallBox .live-row {
    margin: 10px 0;
    font-size: 15px;
}

#liveCallBox .live-label {
    color: #667085;
    display: inline-block;
    min-width: 100px;
}

#liveCallClose {
    position: absolute;
    left: 14px;
    top: 10px;
    border: 0;
    background: transparent;
    font-size: 24px;
    cursor: pointer;
    color: #667085;
}

#liveCallStatus {
    color: #16a34a;
    font-weight: 700;
}
'''

html = r'''
<!-- =======================================================
     REAL-TIME UCM CALL
     ======================================================= -->

<div id="liveCallBox">

    <button id="liveCallClose" type="button" onclick="hideLiveCall()">
        ×
    </button>

    <div class="live-title">
        📞 تماس جدید
    </div>

    <div class="live-row">
        <span class="live-label">شماره تماس:</span>
        <strong id="liveCaller">-</strong>
    </div>

    <div class="live-row">
        <span class="live-label">نام:</span>
        <strong id="liveName">-</strong>
    </div>

    <div class="live-row">
        <span class="live-label">مقصد:</span>
        <strong id="liveDst">-</strong>
    </div>

    <div class="live-row">
        <span class="live-label">داخلی:</span>
        <strong id="liveExt">-</strong>
    </div>

    <div class="live-row">
        <span class="live-label">وضعیت:</span>
        <strong id="liveCallStatus">
            در حال زنگ خوردن...
        </strong>
    </div>

</div>
'''

js = r'''
<script>

/* =========================================================
   REAL-TIME UCM CALL MONITOR
   ========================================================= */

let lastLiveSignature = "";
let liveTimer = null;


function getPayloadValue(payload, keys) {

    if (!payload) {
        return "";
    }

    for (const key of keys) {

        if (
            payload[key] !== undefined &&
            payload[key] !== null &&
            String(payload[key]).trim() !== ""
        ) {
            return String(payload[key]).trim();
        }
    }

    return "";
}


function hideLiveCall() {

    const box = document.getElementById("liveCallBox");

    if (box) {
        box.classList.remove("show");
    }
}


function showLiveCall(event) {

    const payload = event.payload || {};

    const caller = getPayloadValue(payload, [
        "caller",
        "caller_number",
        "callerNumber",
        "src",
        "source",
        "from",
        "number",
        "channel_ext"
    ]);

    const dst = getPayloadValue(payload, [
        "callee",
        "callee_number",
        "calleeNumber",
        "dst",
        "destination",
        "to",
        "dstchannel_ext"
    ]);

    const name = getPayloadValue(payload, [
        "caller_name",
        "callerName",
        "name"
    ]);

    const ext = getPayloadValue(payload, [
        "ext",
        "extension",
        "channel_ext",
        "dstchannel_ext"
    ]);

    const status = getPayloadValue(payload, [
        "status",
        "state",
        "event",
        "action",
        "type"
    ]);

    document.getElementById("liveCaller").textContent =
        caller || "-";

    document.getElementById("liveName").textContent =
        name || "-";

    document.getElementById("liveDst").textContent =
        dst || "-";

    document.getElementById("liveExt").textContent =
        ext || "-";

    document.getElementById("liveCallStatus").textContent =
        status || "در حال زنگ خوردن...";

    document.getElementById("liveCallBox").classList.add("show");

    clearTimeout(liveTimer);

    liveTimer = setTimeout(
        hideLiveCall,
        30000
    );
}


async function checkLiveCalls() {

    try {

        const response = await fetch(
            "/api/live",
            {
                cache: "no-store"
            }
        );

        if (!response.ok) {
            return;
        }

        const data = await response.json();

        if (
            !data.events ||
            !Array.isArray(data.events) ||
            data.events.length === 0
        ) {
            return;
        }

        const event = data.events[0];

        const signature =
            String(event.received_at || "") +
            "|" +
            String(event.raw || "") +
            "|" +
            JSON.stringify(event.payload || {});

        if (signature === lastLiveSignature) {
            return;
        }

        lastLiveSignature = signature;

        showLiveCall(event);

    } catch (error) {

        console.debug(
            "UCM live monitor:",
            error
        );
    }
}


function startLiveMonitor() {

    if (liveTimer) {
        clearInterval(liveTimer);
    }

    /*
     * بررسی هر یک ثانیه
     * بنابراین تماس تقریباً لحظه‌ای روی داشبورد دیده می‌شود.
     */

    setInterval(
        checkLiveCalls,
        1000
    );

    checkLiveCalls();
}


if (document.readyState === "loading") {

    document.addEventListener(
        "DOMContentLoaded",
        startLiveMonitor
    );

} else {

    startLiveMonitor();
}

</script>
'''

# CSS داخل style
if "#liveCallBox" not in text:
    if "</style>" in text:
        text = text.replace(
            "</style>",
            css + "\n</style>",
            1
        )
    else:
        text = text.replace(
            "<head>",
            "<head><style>" + css + "</style>",
            1
        )

# HTML بعد از body
if 'id="liveCallBox"' not in text:
    if "<body>" in text:
        text = text.replace(
            "<body>",
            "<body>\n" + html,
            1
        )
    else:
        text = html + "\n" + text

# JS قبل از body
if "function checkLiveCalls()" not in text:
    if "</body>" in text:
        text = text.replace(
            "</body>",
            js + "\n</body>",
            1
        )
    else:
        text += "\n" + js

p.write_text(text)

print("index.html patched successfully")
PY

echo
echo "=== Python syntax check ==="

python3 -m py_compile backend/app/main.py backend/app/live_listener.py

echo
echo "=== Done ==="
echo "Backup files:"
echo "  backend/app/main.py.bak"
echo "  frontend/index.html.bak"
