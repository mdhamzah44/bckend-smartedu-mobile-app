"""
SmartEdu  —  Flask + SocketIO backend
Combines:
  • Real-time canvas / drawing / slide sync  (gevent SocketIO)
  • Full REST API for auth, courses, tests, polls, notes, admin
  • MongoDB (Atlas) persistence
  • Cloudinary file uploads
  • Resend transactional email (OTP + test results)
"""

import os
import time
import zlib
import uuid
import random
import logging
from functools import wraps
from datetime import datetime, timedelta, timezone

from flask import Flask, request, jsonify, session
from flask_cors import CORS
from flask_socketio import SocketIO, join_room, emit
from werkzeug.security import generate_password_hash, check_password_hash
from pymongo import MongoClient
import cloudinary
import cloudinary.uploader
import resend

# ─────────────────────────────────────────────────────────────────────────────
# APP & CONFIG
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.ERROR)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "smartedu-secret-key")
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=30)
app.config["SESSION_COOKIE_SECURE"]   = True
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"

CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)

socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="gevent",
    ping_timeout=60,
    ping_interval=20,
    max_http_buffer_size=80 * 1024 * 1024,
    compression_threshold=1024,
    logger=False,
    engineio_logger=False,
)

# ─────────────────────────────────────────────────────────────────────────────
# THIRD-PARTY SERVICES
# ─────────────────────────────────────────────────────────────────────────────

resend.api_key = os.environ.get("RESEND_API_KEY", "re_P827PnR8_B9n5Fhz3vxAjqp56ZxV8sBRk")

cloudinary.config(
    cloud_name = os.environ.get("CLOUDINARY_CLOUD", "dtiy0aqwb"),
    api_key    = os.environ.get("CLOUDINARY_KEY",   "559813745442773"),
    api_secret = os.environ.get("CLOUDINARY_SECRET","fkdtSGUU7xaSSXF_D6ybVjx6vmY"),
)

# ─────────────────────────────────────────────────────────────────────────────
# MONGODB
# ─────────────────────────────────────────────────────────────────────────────

MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb+srv://Vercel-Admin-atlas-claret-kettle:PGhZuRc6LeUN145C"
    "@atlas-claret-kettle.mqtmjmc.mongodb.net/?retryWrites=true&w=majority"
)

client = MongoClient(MONGO_URI)
try:
    client.admin.command("ping")
    print("✅  MongoDB connected")
except Exception as e:
    print("❌  MongoDB error:", e)

db = client["SmartEduDB"]

users_col          = db["users"]
classes_col        = db["classes"]
user_classes_col   = db["user_classes"]
comments_col       = db["comments"]
user_courses_col   = db["user_courses"]
courses_col        = db["courses"]
teachers_col       = db["teachers"]
reviews_col        = db["reviews"]
followers_col      = db["followers"]
notes_col          = db["notes"]
tests_col          = db["tests"]
test_attempts_col  = db["test_attempts"]
poll_sessions_col  = db["poll_sessions"]
announcements_col  = db["announcements"]
activity_log_col   = db["activity_log"]
otp_store_col = db["otp_store"]

ADMIN_ID = os.environ.get("ADMIN_ID", "83a39908-cb47-4589-8c58-46f388f3976d")

# ─────────────────────────────────────────────────────────────────────────────
# IN-MEMORY REAL-TIME STATE
# ─────────────────────────────────────────────────────────────────────────────

canvas_data:           dict[str, dict[int, bytes | None]] = {}
current_slide:         dict[str, int]   = {}
slide_meta:            dict[str, dict]  = {}
poll_state:            dict[str, dict]  = {}
hand_raise_state:      dict[str, dict]  = {}
voice_call_state:      dict[str, dict]  = {}
sid_map:               dict[str, dict]  = {}
last_canvas_broadcast: dict[str, float] = {}

CANVAS_BROADCAST_MIN_INTERVAL = 0.08   # 80 ms ≈ 12 fps


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS — compression, room init
# ─────────────────────────────────────────────────────────────────────────────

def compress_image(data_url: str) -> bytes:
    return zlib.compress(data_url.encode("utf-8"), level=1)

def decompress_image(data: bytes) -> str:
    return zlib.decompress(data).decode("utf-8")

def get_slide_image(room: str, slide: int):
    raw = canvas_data.get(room, {}).get(slide)
    return decompress_image(raw) if raw else None

def set_slide_image(room: str, slide: int, data_url):
    canvas_data.setdefault(room, {})[slide] = compress_image(data_url) if data_url else None

def ensure_room(room):
    canvas_data.setdefault(room, {0: None})
    current_slide.setdefault(room, 0)
    slide_meta.setdefault(room, {0: {"dark": False}})

def ensure_poll(room):
    poll_state.setdefault(room, {"poll_id": None, "active": False, "responses": {}, "_names": {}})

def ensure_hand(room):
    hand_raise_state.setdefault(room, {})

def ensure_voice(room):
    voice_call_state.setdefault(room, {"student_id": None, "teacher_socket_id": None, "student_socket_id": None})

def get_slide(room):
    return current_slide.get(room, 0)

def student_socket_for(room, user_id):
    info = hand_raise_state.get(room, {}).get(user_id)
    if info:
        return info.get("socket_id")
    voice = voice_call_state.get(room, {})
    if voice.get("student_id") == user_id:
        return voice.get("student_socket_id")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS — auth decorators
# ─────────────────────────────────────────────────────────────────────────────

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        # 1. Try Flask session (same-origin / web)
        if "user_id" in session:
            return f(*args, **kwargs)

        # 2. Try Authorization: Bearer <token> (mobile / cross-origin)
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header.split(" ", 1)[1].strip()
            user = users_col.find_one({"id": token})
            if user:
                # Populate session-like keys so route handlers work unchanged
                session["user_id"] = user["id"]
                session["role"]    = user.get("role", "Student")
                session["user_name"] = user.get("fullname", "")
                return f(*args, **kwargs)

        return jsonify({"error": "Unauthorized"}), 401
    return decorated

def role_required(required_role):
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            # 1. Try Flask session
            if "user_id" in session:
                if session.get("role") != required_role:
                    return jsonify({"error": "Forbidden"}), 403
                return f(*args, **kwargs)

            # 2. Try Authorization: Bearer <token>
            auth_header = request.headers.get("Authorization", "")
            if auth_header.startswith("Bearer "):
                token = auth_header.split(" ", 1)[1].strip()
                user = users_col.find_one({"id": token})
                if user:
                    if user.get("role") != required_role:
                        return jsonify({"error": "Forbidden"}), 403
                    session["user_id"]   = user["id"]
                    session["role"]      = user.get("role", "Student")
                    session["user_name"] = user.get("fullname", "")
                    return f(*args, **kwargs)

        return jsonify({"error": "Unauthorized"}), 401
        return decorated
    return decorator


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS — utilities
# ─────────────────────────────────────────────────────────────────────────────

def get_class_status(cls):
    try:
        class_dt = datetime.strptime(f"{cls['date']} {cls['time']}", "%Y-%m-%d %H:%M")
        diff_min = (class_dt - datetime.now()).total_seconds() / 60
        if diff_min > 0:
            return "upcoming"
        if -60 <= diff_min <= 0:
            return "live"
        return "completed"
    except Exception:
        return "upcoming"

def log_admin_action(action: str, admin_id: str = None):
    try:
        activity_log_col.insert_one({
            "action":    action,
            "admin_id":  admin_id or "system",
            "timestamp": datetime.utcnow(),
        })
    except Exception:
        pass

def serialize(doc: dict) -> dict:
    """Remove MongoDB _id and convert datetime to ISO string."""
    out = {k: v for k, v in doc.items() if k != "_id"}
    for k, v in out.items():
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat()
    return out


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS — email
# ─────────────────────────────────────────────────────────────────────────────

FROM_ADDR = "SmartEdu <noreply@emails.unacademy.store>"

def _otp_html(heading: str, otp: str) -> str:
    return f"""<div style="background:#070810;font-family:Arial,sans-serif;padding:20px;">
<div style="max-width:520px;margin:auto;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);border-radius:16px;overflow:hidden;">
<div style="padding:28px 20px;text-align:center;">
  <div style="width:44px;height:44px;border-radius:12px;background:linear-gradient(135deg,#4f7cff,#7c3aed);color:#fff;font-weight:800;font-size:18px;line-height:44px;text-align:center;margin:auto;">S</div>
  <h2 style="color:#f0f2ff;margin:14px 0 6px;">{heading}</h2>
</div>
<div style="padding:0 20px 24px;text-align:center;">
  <div style="background:rgba(79,124,255,.08);border:1px solid rgba(79,124,255,.2);border-radius:14px;padding:20px;">
    <div style="font-size:11px;letter-spacing:2px;color:#7c84a8;margin-bottom:10px;">YOUR OTP</div>
    <div style="font-size:32px;font-weight:800;letter-spacing:8px;color:#4f7cff;">{otp}</div>
  </div>
  <p style="font-size:13px;color:#7c84a8;margin-top:16px;">Expires in <b style="color:#ff6b6b;">5 minutes</b>. Do not share.</p>
</div>
</div></div>"""

def send_otp_email(to: str, otp: str):
    try:
        resend.Emails.send({"from": FROM_ADDR, "to": to,
            "subject": "Verify Your SmartEdu Account",
            "html": _otp_html("Verify Your Email", otp)})
    except Exception as e:
        logging.error(f"OTP email failed: {e}")

def send_login_otp_email(to: str, otp: str):
    try:
        resend.Emails.send({"from": FROM_ADDR, "to": to,
            "subject": "SmartEdu Login OTP",
            "html": _otp_html("Login OTP", otp)})
    except Exception as e:
        logging.error(f"Login OTP email failed: {e}")

def send_test_result_email(to: str, name: str, test_name: str, score: float, correct: int, wrong: int, total: int):
    skipped = total - correct - wrong
    try:
        resend.Emails.send({"from": FROM_ADDR, "to": to,
            "subject": f"Your Results: {test_name} — SmartEdu",
            "html": f"""<div style="background:#070810;font-family:Arial,sans-serif;padding:20px;">
<div style="max-width:520px;margin:auto;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);border-radius:16px;overflow:hidden;">
<div style="height:3px;background:linear-gradient(90deg,#4f7cff,#7c3aed,#06d6a0);"></div>
<div style="padding:28px 20px;text-align:center;">
  <h2 style="color:#f0f2ff;">Test Results</h2>
  <p style="color:#a78bfa;font-weight:700;">{test_name}</p>
  <p style="color:#7c84a8;">Hi {name}, here's how you did!</p>
</div>
<div style="padding:0 20px;text-align:center;">
  <div style="background:rgba(79,124,255,.08);border:1px solid rgba(79,124,255,.2);border-radius:14px;padding:20px;margin-bottom:16px;">
    <div style="font-size:11px;color:#7c84a8;margin-bottom:8px;">TOTAL SCORE</div>
    <div style="font-size:40px;font-weight:800;color:#4f7cff;">{score:+.1f}</div>
  </div>
  <table width="100%" style="border-collapse:collapse;">
    <tr>
      <td style="padding:12px;background:rgba(6,214,160,.08);border:1px solid rgba(6,214,160,.2);border-radius:10px;text-align:center;">
        <div style="font-size:11px;color:#7c84a8;">CORRECT</div>
        <div style="font-size:22px;font-weight:800;color:#06d6a0;">{correct}</div>
      </td>
      <td width="10"></td>
      <td style="padding:12px;background:rgba(255,107,107,.08);border:1px solid rgba(255,107,107,.2);border-radius:10px;text-align:center;">
        <div style="font-size:11px;color:#7c84a8;">WRONG</div>
        <div style="font-size:22px;font-weight:800;color:#ff6b6b;">{wrong}</div>
      </td>
      <td width="10"></td>
      <td style="padding:12px;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);border-radius:10px;text-align:center;">
        <div style="font-size:11px;color:#7c84a8;">SKIPPED</div>
        <div style="font-size:22px;font-weight:800;color:#7c84a8;">{skipped}</div>
      </td>
    </tr>
  </table>
</div>
<div style="padding:18px 20px;border-top:1px solid rgba(255,255,255,.08);text-align:center;">
  <p style="font-size:11px;color:#7c84a8;">© SmartEdu • Keep learning 🚀</p>
</div>
</div></div>"""})
    except Exception as e:
        logging.error(f"Result email failed: {e}")


# ═════════════════════════════════════════════════════════════════════════════
#  SOCKET.IO  —  REAL-TIME EVENTS
# ═════════════════════════════════════════════════════════════════════════════

# ── Join / Disconnect ─────────────────────────────────────────────────────────

@socketio.on("join-room")
def on_join_room(data):
    room    = data["class_id"]
    user_id = data.get("user_id") or request.sid
    join_room(room)
    ensure_room(room); ensure_poll(room); ensure_hand(room); ensure_voice(room)
    sid_map[request.sid] = {"class_id": room, "user_id": user_id}

    slide = current_slide[room]
    image = get_slide_image(room, slide)
    dark  = slide_meta.get(room, {}).get(slide, {}).get("dark", False)
    emit("load-canvas", {"image": image, "slide": slide, "dark": dark})
    emit("user-joined", {"user_id": request.sid}, room=room, include_self=False)

    poll = poll_state.get(room, {})
    if poll.get("active"):
        elapsed   = time.time() - poll.get("started_at", time.time())
        remaining = max(1, int(poll.get("timer", 30) - elapsed))
        emit("poll-start", {"poll_id": poll["poll_id"], "poll_type": poll["poll_type"],
                             "options": poll["options"], "timer": remaining,
                             "question_num": poll.get("question_num", 1)})

    for uid, info in hand_raise_state.get(room, {}).items():
        emit("hand-raise", {"user_id": uid, "name": info.get("name", "Student"), "raised": True})


@socketio.on("disconnect")
def on_disconnect():
    info = sid_map.pop(request.sid, None)
    if not info:
        return
    room    = info.get("class_id")
    user_id = info.get("user_id")
    if not room:
        return

    hands = hand_raise_state.get(room, {})
    if user_id in hands and hands[user_id].get("socket_id") == request.sid:
        del hands[user_id]
        socketio.emit("hand-raise", {"user_id": user_id, "name": "", "raised": False}, room=room)

    voice = voice_call_state.get(room, {})
    if voice.get("student_id") == user_id or voice.get("student_socket_id") == request.sid:
        teacher_sid = voice.get("teacher_socket_id")
        voice_call_state[room] = {"student_id": None, "teacher_socket_id": None, "student_socket_id": None}
        if teacher_sid:
            socketio.emit("voice-ended-by-student", {"student_id": user_id}, to=teacher_sid)


# ── WebRTC signaling ──────────────────────────────────────────────────────────

@socketio.on("offer")
def on_offer(data):
    emit("offer", {"offer": data["offer"], "from": request.sid}, to=data["to"])

@socketio.on("answer")
def on_answer(data):
    emit("answer", {"answer": data["answer"], "from": request.sid}, to=data["to"])

@socketio.on("ice-candidate")
def on_ice(data):
    emit("ice-candidate", {"candidate": data["candidate"], "from": request.sid}, to=data["to"])


# ── Drawing ───────────────────────────────────────────────────────────────────

@socketio.on("draw-start")
def on_draw_start(data):
    emit("draw-start", data, room=data["class_id"], include_self=False)

@socketio.on("draw")
def on_draw(data):
    emit("draw", data, room=data["class_id"], include_self=False)

@socketio.on("erase")
def on_erase(data):
    emit("erase", data, room=data["class_id"], include_self=False)

@socketio.on("draw-end")
def on_draw_end(data):
    emit("draw-end", {}, room=data["class_id"], include_self=False)


# ── Canvas image sync ─────────────────────────────────────────────────────────

@socketio.on("canvas-image")
def on_canvas_image(data):
    room  = data["class_id"]
    slide = data.get("slide", get_slide(room))
    image = data.get("image")
    ensure_room(room)
    set_slide_image(room, slide, image)
    now  = time.time()
    last = last_canvas_broadcast.get(room, 0)
    if now - last >= CANVAS_BROADCAST_MIN_INTERVAL:
        last_canvas_broadcast[room] = now
        emit("canvas-image-update", {"slide": slide, "image": image},
             room=room, include_self=False)

@socketio.on("canvas-bg")
def on_canvas_bg(data):
    room  = data["class_id"]
    slide = data.get("slide", get_slide(room))
    dark  = bool(data.get("dark", False))
    slide_meta.setdefault(room, {})[slide] = {"dark": dark}
    emit("canvas-bg", {"dark": dark, "slide": slide}, room=room, include_self=False)

@socketio.on("clear-canvas")
def on_clear_canvas(data):
    room  = data["class_id"]
    slide = get_slide(room)
    set_slide_image(room, slide, None)
    emit("clear-canvas", {}, room=room, include_self=False)


# ── Slides ────────────────────────────────────────────────────────────────────

def _emit_slide_changed(room, slide):
    image = get_slide_image(room, slide)
    dark  = slide_meta.get(room, {}).get(slide, {}).get("dark", False)
    emit("slide-changed", {"slide": slide, "image": image, "dark": dark}, room=room)

@socketio.on("add-slide")
def on_add_slide(data):
    room = data["class_id"]
    ensure_room(room)
    new_idx = len(canvas_data[room])
    canvas_data[room][new_idx] = None
    current_slide[room] = new_idx
    emit("slide-changed", {"slide": new_idx, "image": None, "dark": False}, room=room)

@socketio.on("add-slide-with-image")
def on_add_slide_with_image(data):
    room = data["class_id"]; image = data.get("image"); dark = bool(data.get("dark", False))
    ensure_room(room)
    new_idx = len(canvas_data[room])
    set_slide_image(room, new_idx, image)
    slide_meta.setdefault(room, {})[new_idx] = {"dark": dark}
    current_slide[room] = new_idx
    emit("slide-changed", {"slide": new_idx, "image": image, "dark": dark}, room=room)

@socketio.on("change-slide")
def on_change_slide(data):
    room = data["class_id"]
    ensure_room(room)
    slide = max(0, min(int(data["slide"]), len(canvas_data[room]) - 1))
    current_slide[room] = slide
    _emit_slide_changed(room, slide)

@socketio.on("delete-slide")
def on_delete_slide(data):
    room = data["class_id"]; idx = int(data.get("slide", 0))
    ensure_room(room)
    slides = canvas_data[room]
    if len(slides) <= 1:
        canvas_data[room] = {0: None}
        slide_meta.setdefault(room, {})[0] = {"dark": False}
        current_slide[room] = 0
        emit("slide-changed", {"slide": 0, "image": None, "dark": False}, room=room)
        return
    new_slides = {}; new_meta = {}; old_meta = slide_meta.get(room, {}); ni = 0
    for i in sorted(slides.keys()):
        if i != idx:
            new_slides[ni] = slides[i]; new_meta[ni] = old_meta.get(i, {"dark": False}); ni += 1
    canvas_data[room] = new_slides; slide_meta[room] = new_meta
    cur = current_slide[room]
    cur = max(0, cur - 1) if cur >= idx else cur
    current_slide[room] = cur
    _emit_slide_changed(room, cur)

@socketio.on("get-slides")
def on_get_slides(data):
    room = data["class_id"]; ensure_room(room)
    slides = canvas_data[room]
    ordered = [get_slide_image(room, i) for i in sorted(slides.keys())]
    emit("slides-list", {"slides": ordered, "current": current_slide.get(room, 0)})


# ── Upload progress ───────────────────────────────────────────────────────────

@socketio.on("upload-start")
def on_upload_start(data):
    emit("upload-start", {"label": data.get("label", "Loading…")},
         room=data["class_id"], include_self=False)

@socketio.on("upload-progress")
def on_upload_progress(data):
    emit("upload-progress", {"label": data.get("label", "Loading…"), "pct": data.get("pct", 0)},
         room=data["class_id"], include_self=False)


# ── Class ended ───────────────────────────────────────────────────────────────

@socketio.on("class-ended")
def on_class_ended(data):
    emit("class-ended", {}, room=data["class_id"], include_self=False)


# ── Polls ─────────────────────────────────────────────────────────────────────

@socketio.on("poll-start")
def on_poll_start(data):
    room = data["class_id"]; ensure_poll(room)
    poll_state[room] = {
        "poll_id": data["poll_id"], "poll_type": data.get("poll_type"),
        "options": data.get("options", []), "timer": data.get("timer", 30),
        "question_num": data.get("question_num", 1), "active": True,
        "started_at": time.time(), "responses": {}, "_names": {},
    }
    emit("poll-start", {"poll_id": data["poll_id"], "poll_type": data.get("poll_type"),
                        "options": data.get("options", []), "timer": data.get("timer", 30),
                        "question_num": data.get("question_num", 1)},
         room=room, include_self=False)

@socketio.on("poll-response")
def on_poll_response(data):
    room = data["class_id"]; ensure_poll(room)
    poll = poll_state[room]
    if not poll.get("active") or poll.get("poll_id") != data.get("poll_id"):
        return
    user_id = data.get("user_id") or request.sid
    answer  = data.get("answer"); name = data.get("name", "Student")
    poll["responses"][user_id] = answer; poll["_names"][user_id] = name
    emit("poll-response", {"poll_id": data["poll_id"], "user_id": user_id,
                           "answer": answer, "name": name, "total": len(poll["responses"])},
         room=room, include_self=True)

@socketio.on("poll-end")
def on_poll_end(data):
    room = data["class_id"]; ensure_poll(room)
    poll = poll_state[room]; poll["active"] = False; poll["correct"] = data.get("correct")
    merged = {**data.get("responses", {}), **poll.get("responses", {})}
    poll["responses"] = merged
    emit("poll-end", {"poll_id": data["poll_id"], "correct": data.get("correct"),
                      "responses": merged}, room=room, include_self=False)

@socketio.on("show-leaderboard")
def on_show_leaderboard(data):
    emit("show-leaderboard", {"leaderboard": data.get("leaderboard", [])},
         room=data["class_id"], include_self=False)


# ── Hand raise ────────────────────────────────────────────────────────────────

@socketio.on("hand-raise")
def on_hand_raise(data):
    room = data["class_id"]; ensure_hand(room)
    user_id = data.get("user_id") or request.sid
    raised  = bool(data.get("raised", True)); name = data.get("name", "Student")
    if raised:
        hand_raise_state[room][user_id] = {"name": name, "socket_id": request.sid, "raised_at": time.time()}
    else:
        hand_raise_state[room].pop(user_id, None)
    emit("hand-raise", {"user_id": user_id, "name": name, "raised": raised},
         room=room, include_self=False)

@socketio.on("hand-dismissed")
def on_hand_dismissed(data):
    room = data["class_id"]; user_id = data.get("user_id"); ensure_hand(room)
    info = hand_raise_state.get(room, {}).pop(user_id, None)
    student_sid = info.get("socket_id") if info else None
    if student_sid:
        emit("hand-dismissed", {"user_id": user_id}, to=student_sid)
    else:
        emit("hand-dismissed", {"user_id": user_id}, room=room, include_self=False)


# ── Voice call ────────────────────────────────────────────────────────────────

@socketio.on("voice-accept")
def on_voice_accept(data):
    room = data["class_id"]; student_id = data.get("student_id"); ensure_voice(room)
    student_socket_id = student_socket_for(room, student_id)
    voice_call_state[room] = {"student_id": student_id,
                              "teacher_socket_id": request.sid, "student_socket_id": student_socket_id}
    payload = {"student_id": student_id, "teacher_socket": request.sid}
    if student_socket_id:
        emit("voice-accept", payload, to=student_socket_id)
    else:
        emit("voice-accept", payload, room=room, include_self=False)

@socketio.on("voice-offer")
def on_voice_offer(data):
    room = data["class_id"]; voice = voice_call_state.get(room, {})
    teacher_sid = voice.get("teacher_socket_id")
    if teacher_sid:
        emit("voice-offer", {"student_id": data.get("student_id"), "offer": data["offer"]}, to=teacher_sid)

@socketio.on("voice-answer")
def on_voice_answer(data):
    room = data["class_id"]; student_id = data.get("student_id")
    voice = voice_call_state.get(room, {})
    student_sid = voice.get("student_socket_id") or student_socket_for(room, student_id)
    if student_sid:
        emit("voice-answer", {"student_id": student_id, "answer": data["answer"]}, to=student_sid)

@socketio.on("voice-ice")
def on_voice_ice(data):
    room = data["class_id"]; candidate = data.get("candidate"); student_id = data.get("student_id")
    from_teacher = bool(data.get("from_teacher", False))
    voice = voice_call_state.get(room, {})
    if from_teacher:
        sid = voice.get("student_socket_id") or student_socket_for(room, student_id)
        if sid: emit("voice-ice-student", {"candidate": candidate, "student_id": student_id}, to=sid)
    else:
        sid = voice.get("teacher_socket_id")
        if sid: emit("voice-ice-teacher", {"candidate": candidate, "student_id": student_id}, to=sid)

@socketio.on("voice-end")
def on_voice_end(data):
    room = data["class_id"]; student_id = data.get("student_id"); ensure_voice(room)
    voice = voice_call_state.get(room, {})
    student_sid = voice.get("student_socket_id") or student_socket_for(room, student_id)
    voice_call_state[room] = {"student_id": None, "teacher_socket_id": None, "student_socket_id": None}
    hand_raise_state.get(room, {}).pop(student_id, None)
    if student_sid:
        emit("voice-end", {"student_id": student_id}, to=student_sid)
    socketio.emit("hand-raise", {"user_id": student_id, "name": "", "raised": False}, room=room)

@socketio.on("voice-ended-by-student")
def on_voice_ended_by_student(data):
    room = data["class_id"]; student_id = data.get("student_id"); ensure_voice(room)
    voice = voice_call_state.get(room, {})
    teacher_sid = voice.get("teacher_socket_id")
    voice_call_state[room] = {"student_id": None, "teacher_socket_id": None, "student_socket_id": None}
    hand_raise_state.get(room, {}).pop(student_id, None)
    if teacher_sid:
        emit("voice-ended-by-student", {"student_id": student_id}, to=teacher_sid)
    socketio.emit("hand-raise", {"user_id": student_id, "name": "", "raised": False}, room=room)


# ═════════════════════════════════════════════════════════════════════════════
#  HTTP  —  REST API
# ═════════════════════════════════════════════════════════════════════════════

# ── Health ────────────────────────────────────────────────────────────────────

@app.route("/")
def health():
    return jsonify({"status": "ok", "rooms": len(canvas_data), "connections": len(sid_map)})




# ── Canvas PDF export ─────────────────────────────────────────────────────────

@app.route("/get-slides-pdf/<class_id>")
def get_slides_pdf(class_id):
    ensure_room(class_id)
    slides  = canvas_data.get(class_id, {})
    ordered = [get_slide_image(class_id, i) for i in sorted(slides.keys())]
    valid   = [s for s in ordered if s]
    return jsonify({"slides": valid, "total": len(valid)})


# ── Auth — register / verify / login ─────────────────────────────────────────

@app.route("/register", methods=["POST"])
def register():
    try:
        data     = request.get_json() or request.form
        fullname = (data.get("fullname") or "").strip()
        email    = (data.get("email") or "").strip().lower()
        password = data.get("password") or ""
        if not fullname or not email or not password:
            return jsonify({"error": "All fields required"}), 400
        if users_col.find_one({"email": email}):
            return jsonify({"error": "Email already registered"}), 409

        otp = str(random.randint(100000, 999999))
        session["pending_user"] = {
            "fullname": fullname, "email": email,
            "password": generate_password_hash(password),
            "role": "Student", "otp": otp,
            "otp_expiry": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
        }
        send_otp_email(email, otp)
        return jsonify({"message": "OTP sent", "requires_otp": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/verify-otp", methods=["POST"])
def verify_otp():
    try:
        data    = request.get_json() or request.form
        entered = str(data.get("otp", "")).strip()
        pending = session.get("pending_user")
        if not pending:
            return jsonify({"error": "No pending registration"}), 400

        expiry = datetime.fromisoformat(pending["otp_expiry"])
        if datetime.now(timezone.utc) > expiry:
            session.pop("pending_user", None)
            return jsonify({"error": "OTP expired"}), 400
        if entered != pending["otp"]:
            return jsonify({"error": "Invalid OTP"}), 400

        user_id = str(uuid.uuid4())
        users_col.insert_one({
            "id": user_id, "fullname": pending["fullname"],
            "email": pending["email"], "password": pending["password"],
            "role": pending["role"], "subscribed": "no",
            "created_at": datetime.now(timezone.utc),
        })
        if pending["role"] == "Teacher":
            teachers_col.insert_one({
                "teacher_id": str(uuid.uuid4()), "user_id": user_id,
                "fullname": pending["fullname"], "headline": "", "bio": "",
                "education": "", "experience": "", "languages": [],
                "specialization": "", "category": "", "courses": [],
                "free_classes": [], "rating": 0, "total_students": 0,
                "created_at": datetime.now(timezone.utc),
            })

        session.update({"user_id": user_id, "role": pending["role"], "user_name": pending["fullname"]})
        session.permanent = True
        session.pop("pending_user", None)

        user_out = {"id": user_id, "fullname": pending["fullname"],
                    "email": pending["email"], "role": pending["role"]}
        return jsonify({"user": user_out, "token": user_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/login", methods=["POST"])
def login():
    try:
        data     = request.get_json() or request.form
        email    = (data.get("email") or "").strip().lower()
        password = data.get("password") or ""
        if not email or not password:
            return jsonify({"error": "Email and password required"}), 400
        user = users_col.find_one({"email": email})
        if not user:
            return jsonify({"error": "User not found"}), 404
        if not check_password_hash(user["password"], password):
            return jsonify({"error": "Incorrect password"}), 401

        otp   = str(random.randint(100000, 999999))
        token = str(uuid.uuid4())          # ← client holds this
        expiry = datetime.now(timezone.utc) + timedelta(minutes=5)

        otp_store_col.update_one(
            {"token": token},
            {"$set": {
                "token":    token,
                "user_id":  user["id"],
                "fullname": user["fullname"],
                "email":    user["email"],
                "role":     user["role"],
                "otp":      otp,
                "expiry":   expiry,
                "type":     "login",
            }},
            upsert=True
        )
        send_login_otp_email(user["email"], otp)
        return jsonify({"requires_otp": True, "message": "OTP sent", "otp_token": token})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# /login-verify-otp — look up by token, not session
@app.route("/login-verify-otp", methods=["POST"])
def login_verify_otp():
    try:
        data      = request.get_json() or request.form
        entered   = str(data.get("otp", "")).strip()
        otp_token = str(data.get("otp_token", "")).strip()   # ← sent by client

        if not otp_token:
            return jsonify({"error": "Missing otp_token"}), 400

        record = otp_store_col.find_one({"token": otp_token, "type": "login"})
        if not record:
            return jsonify({"error": "No pending login"}), 400
        if datetime.now(timezone.utc) > record["expiry"].replace(tzinfo=timezone.utc):
            otp_store_col.delete_one({"token": otp_token})
            return jsonify({"error": "OTP expired"}), 400
        if entered != record["otp"]:
            return jsonify({"error": "Invalid OTP"}), 400

        # Clean up
        otp_store_col.delete_one({"token": otp_token})

        # Set server session (best-effort; mobile uses the returned token)
        session.update({
            "user_id":   record["user_id"],
            "role":      record["role"],
            "user_name": record["fullname"],
        })
        session.permanent = True

        user_out = {
            "id":       record["user_id"],
            "fullname": record["fullname"],
            "email":    record["email"],
            "role":     record["role"],
        }
        return jsonify({"user": user_out, "token": record["user_id"]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Student home data ─────────────────────────────────────────────────────────

@app.route("/api/student/home")
@login_required
def student_home():
    user_id = session["user_id"]
    today   = datetime.now().strftime("%Y-%m-%d")
    enrolled_class_ids = {e["class_id"] for e in user_classes_col.find({"user_id": user_id})}
    enrolled_course_ids = {e["course_id"] for e in user_courses_col.find({"user_id": user_id})}

    all_classes = list(classes_col.find({"class_id": {"$in": list(enrolled_class_ids)}}))
    today_classes    = [c for c in all_classes if c.get("date") == today]
    upcoming_classes = [c for c in all_classes if c.get("date", "") > today]

    def fmt_class(c):
        teacher = users_col.find_one({"id": c.get("teacher_id")})
        return {
            "class_id":     c["class_id"],
            "title":        c.get("subject", c.get("title", "")),
            "date":         c.get("date"),
            "time":         c.get("time"),
            "status":       get_class_status(c),
            "teacher_name": teacher.get("fullname") if teacher else "",
            "is_free":      c.get("is_free", False),
        }

    courses = []
    for cid in enrolled_course_ids:
        course = courses_col.find_one({"course_id": cid})
        if course:
            teacher = teachers_col.find_one({"teacher_id": course.get("teacher_id")})
            courses.append({
                "course_id":    cid,
                "title":        course.get("name"),
                "teacher_name": teacher.get("fullname") if teacher else "",
                "total_classes": course.get("total_classes", 0),
            })

    return jsonify({
        "today_classes":    [fmt_class(c) for c in today_classes],
        "upcoming_classes": [fmt_class(c) for c in upcoming_classes[:10]],
        "courses":          courses,
    })


# ── Student tests ─────────────────────────────────────────────────────────────

@app.route("/api/student/tests")
@login_required
def student_tests():
    user_id = session["user_id"]
    enrolled_course_ids = [e["course_id"] for e in user_courses_col.find({"user_id": user_id})]
    tests   = list(tests_col.find({"course_id": {"$in": enrolled_course_ids}}))
    out = []
    for t in tests:
        attempt  = test_attempts_col.find_one({"user_id": user_id, "test_id": t["test_id"]})
        st = t.get("start_time")
        out.append({
            "test_id":        t["test_id"],
            "name":           t.get("name"),
            "total_questions": len(t.get("questions", [])),
            "duration_minutes": t.get("duration"),
            "start_time":     st.isoformat() if hasattr(st, "isoformat") else str(st) if st else None,
            "attempted":      bool(attempt),
        })
    return jsonify({"tests": out})


@app.route("/api/test/<test_id>/start", methods=["POST"])
@login_required
def start_test(test_id):
    test = tests_col.find_one({"test_id": test_id})
    if not test:
        return jsonify({"error": "Test not found"}), 404
    st = test.get("start_time")
    qs = []
    for q in test.get("questions", []):
        qs.append({"id": q.get("id", ""), "question": q.get("question"),
                   "options": q.get("options"), "type": "mcq"})
    return jsonify({"test": {
        "test_id": test_id, "name": test.get("name"),
        "duration_minutes": test.get("duration"),
        "start_time": st.isoformat() if hasattr(st, "isoformat") else None,
        "questions": qs,
    }})


@app.route("/api/test/<test_id>/submit", methods=["POST"])
@login_required
def submit_test_api(test_id):
    user_id = session["user_id"]
    data    = request.get_json() or {}
    answers = data.get("answers", {})
    test    = tests_col.find_one({"test_id": test_id})
    if not test:
        return jsonify({"error": "Test not found"}), 404

    default_marks    = float(test.get("marks_per_q", 4))
    default_negative = float(test.get("negative_marks", 1))
    score = correct = wrong = 0

    for i, q in enumerate(test.get("questions", [])):
        key      = q.get("id") or str(i)
        user_ans = answers.get(key) or answers.get(str(i))
        if user_ans is None or str(user_ans) == "-1":
            continue
        q_marks    = float(q.get("marks", default_marks))
        q_negative = float(q.get("negative", default_negative))
        if int(user_ans) == q["correct"]:
            score += q_marks; correct += 1
        else:
            score -= q_negative; wrong += 1

    test_attempts_col.update_one(
        {"user_id": user_id, "test_id": test_id},
        {"$set": {"answers": answers, "score": score, "correct": correct,
                  "wrong": wrong, "submitted_at": datetime.utcnow()}},
        upsert=True
    )

    user = users_col.find_one({"id": user_id})
    if user and user.get("email"):
        send_test_result_email(
            to=user["email"], name=user.get("fullname", "Student"),
            test_name=test.get("name", "Test"), score=score,
            correct=correct, wrong=wrong, total=len(test.get("questions", []))
        )

    return jsonify({"score": score, "correct": correct, "wrong": wrong})


# ── Student results ───────────────────────────────────────────────────────────

@app.route("/api/student/results")
@login_required
def student_results():
    user_id  = session["user_id"]
    attempts = list(test_attempts_col.find({"user_id": user_id}))
    out = []
    for a in attempts:
        test = tests_col.find_one({"test_id": a["test_id"]})
        sa   = a.get("submitted_at")
        out.append({
            "test_id":        a["test_id"],
            "test_name":      test.get("name") if test else "Unknown",
            "score":          a.get("score", 0),
            "correct":        a.get("correct", 0),
            "wrong":          a.get("wrong", 0),
            "total_questions": len(test.get("questions", [])) if test else 0,
            "submitted_at":   sa.isoformat() if hasattr(sa, "isoformat") else None,
        })
    return jsonify({"results": out})


@app.route("/get-result/<test_id>")
@login_required
def get_result(test_id):
    user_id = session["user_id"]
    attempt = test_attempts_col.find_one({"user_id": user_id, "test_id": test_id})
    if not attempt:
        return jsonify({"attempted": False})
    return jsonify({"attempted": True, "score": attempt["score"],
                    "correct": attempt["correct"], "wrong": attempt["wrong"],
                    "answers": attempt["answers"]})


@app.route("/get-attempt/<test_id>/<user_id>")
def get_attempt(test_id, user_id):
    attempt = test_attempts_col.find_one({"test_id": test_id, "user_id": user_id})
    test    = tests_col.find_one({"test_id": test_id})
    if not attempt or not test:
        return jsonify([])
    answers = attempt.get("answers", {})
    result  = []
    for i, q in enumerate(test.get("questions", [])):
        ua = answers.get(q.get("id") or str(i), -1)
        result.append({"question": q.get("question"), "options": q.get("options"),
                        "correct": q.get("correct"), "marked": int(ua),
                        "is_correct": int(ua) == q.get("correct")})
    return jsonify(result)


# ── Student notes ─────────────────────────────────────────────────────────────

@app.route("/api/student/notes")
@login_required
def student_notes():
    user_id = session["user_id"]
    enrolled_course_ids = [e["course_id"] for e in user_courses_col.find({"user_id": user_id})]
    notes_raw = list(notes_col.find({"course_id": {"$in": enrolled_course_ids}}))
    out = []
    for n in notes_raw:
        course = courses_col.find_one({"course_id": n.get("course_id")})
        ca = n.get("created_at")
        out.append({"_id": str(n["_id"]), "title": n.get("title"),
                    "description": n.get("description"), "file_url": n.get("file_url"),
                    "course_name": course.get("name") if course else None,
                    "created_at": ca.isoformat() if hasattr(ca, "isoformat") else None})
    return jsonify(out)


# ── Student announcements ─────────────────────────────────────────────────────

@app.route("/api/student/announcements")
@login_required
def student_announcements():
    docs = list(announcements_col.find({}, {"_id": 0}).sort("created_at", -1).limit(30))
    for d in docs:
        ca = d.get("created_at")
        if hasattr(ca, "isoformat"):
            d["created_at"] = ca.isoformat()
    return jsonify({"announcements": docs})


# ── Student courses ───────────────────────────────────────────────────────────

@app.route("/api/student/courses")
@login_required
def student_courses():
    user_id = session["user_id"]
    enrolled = [e["course_id"] for e in user_courses_col.find({"user_id": user_id})]
    courses  = []
    for cid in enrolled:
        c = courses_col.find_one({"course_id": cid})
        if c:
            courses.append(serialize(c))
    return jsonify({"courses": courses})


@app.route("/api/enroll-course", methods=["POST"])
@login_required
def enroll_course():
    user_id   = session["user_id"]
    course_id = (request.get_json() or {}).get("course_id")
    if not course_id:
        return jsonify({"error": "course_id required"}), 400
    if not user_courses_col.find_one({"user_id": user_id, "course_id": course_id}):
        user_courses_col.insert_one({"user_id": user_id, "course_id": course_id,
                                     "enrolled_at": datetime.utcnow()})
        for c in classes_col.find({"course_id": course_id}):
            if not user_classes_col.find_one({"user_id": user_id, "class_id": c["class_id"]}):
                user_classes_col.insert_one({"user_id": user_id, "class_id": c["class_id"]})
    return jsonify({"message": "Enrolled"})


@app.route("/api/enroll-class", methods=["POST"])
@login_required
def enroll_class_api():
    user_id  = session["user_id"]
    class_id = (request.get_json() or {}).get("class_id")
    if not class_id:
        return jsonify({"error": "class_id required"}), 400
    if not user_classes_col.find_one({"user_id": user_id, "class_id": class_id}):
        user_classes_col.insert_one({"user_id": user_id, "class_id": class_id})
    return jsonify({"status": "enrolled"})


# ── Schedule by date ──────────────────────────────────────────────────────────

@app.route("/get-classes")
@login_required
def get_classes():
    user_id  = session["user_id"]
    date_str = request.args.get("date", datetime.now().strftime("%Y-%m-%d"))
    enrolled_ids = {e["class_id"] for e in user_classes_col.find({"user_id": user_id})}
    clss = list(classes_col.find({"class_id": {"$in": list(enrolled_ids)}, "date": date_str}))
    out  = []
    for c in clss:
        teacher = users_col.find_one({"id": c.get("teacher_id")})
        out.append({"type": "class", "class_id": c["class_id"],
                    "subject": c.get("subject", c.get("title", "")),
                    "date": c["date"], "time": c.get("time"),
                    "status": get_class_status(c),
                    "teacher_name": teacher.get("fullname") if teacher else "",
                    "duration": c.get("duration", 60)})

    enrolled_course_ids = [e["course_id"] for e in user_courses_col.find({"user_id": user_id})]
    tests = list(tests_col.find({"course_id": {"$in": enrolled_course_ids}}))
    for t in tests:
        st = t.get("start_time")
        if st and hasattr(st, "strftime") and st.strftime("%Y-%m-%d") == date_str:
            out.append({"type": "test", "test_id": t["test_id"],
                        "subject": t.get("name"), "date": date_str,
                        "time": st.strftime("%H:%M"), "duration": t.get("duration")})

    return jsonify(out)


# ── Recording ─────────────────────────────────────────────────────────────────

@app.route("/recording/<class_id>")
@login_required
def recording(class_id):
    user_id = session["user_id"]
    cls = classes_col.find_one({"class_id": class_id})
    if not cls:
        return jsonify({"error": "Class not found"}), 404
    if (cls.get("teacher_id") != user_id and
            not user_classes_col.find_one({"user_id": user_id, "class_id": class_id})):
        return jsonify({"error": "Access denied"}), 403
    return jsonify({"link": cls.get("link"), "subject": cls.get("subject", "Recording")})


# ── Public courses + teachers ─────────────────────────────────────────────────

@app.route("/api/courses")
def public_courses():
    courses = list(courses_col.find({}, {"_id": 0}))
    out = []
    for c in courses:
        teacher = teachers_col.find_one({"teacher_id": c.get("teacher_id")})
        c["teacher_name"] = teacher.get("fullname") if teacher else ""
        if hasattr(c.get("start_date"), "isoformat"):
            c["start_date"] = c["start_date"].isoformat()
        if hasattr(c.get("created_at"), "isoformat"):
            c["created_at"] = c["created_at"].isoformat()
        out.append(c)
    return jsonify({"courses": out})


@app.route("/api/course/<course_id>")
def public_course(course_id):
    course = courses_col.find_one({"course_id": course_id}, {"_id": 0})
    if not course:
        return jsonify({"error": "Not found"}), 404
    if hasattr(course.get("start_date"), "isoformat"):
        course["start_date"] = course["start_date"].isoformat()
    if hasattr(course.get("created_at"), "isoformat"):
        course["created_at"] = course["created_at"].isoformat()

    teacher = teachers_col.find_one({"teacher_id": course.get("teacher_id")})
    course["teacher_name"] = teacher.get("fullname") if teacher else ""

    clss  = [serialize(c) for c in classes_col.find({"course_id": course_id})]
    tests = [serialize(t) for t in tests_col.find({"course_id": course_id})]
    notes = [serialize(n) for n in notes_col.find({"course_id": course_id})]

    enrolled = False
    if "user_id" in session:
        enrolled = bool(user_courses_col.find_one({"user_id": session["user_id"], "course_id": course_id}))

    return jsonify({"course": course, "classes": clss, "tests": tests, "notes": notes, "enrolled": enrolled})


@app.route("/api/teachers")
def public_teachers():
    teachers = list(teachers_col.find({}, {"_id": 0}))
    for t in teachers:
        t.pop("password", None)
        if hasattr(t.get("created_at"), "isoformat"):
            t["created_at"] = t["created_at"].isoformat()
    return jsonify({"teachers": teachers})


@app.route("/api/teacher/<user_id_or_teacher_id>")
def public_teacher(user_id_or_teacher_id):
    teacher = (teachers_col.find_one({"user_id": user_id_or_teacher_id}) or
               teachers_col.find_one({"teacher_id": user_id_or_teacher_id}))
    if not teacher:
        return jsonify({"error": "Not found"}), 404

    user = users_col.find_one({"id": teacher["user_id"]}, {"_id": 0, "password": 0})
    reviews = list(reviews_col.find({"teacher_id": teacher["teacher_id"]}))
    for r in reviews:
        rev_user = users_col.find_one({"id": r.get("user_id")})
        r.pop("_id", None)
        r["name"] = rev_user.get("fullname") if rev_user else "Student"
        if hasattr(r.get("created_at"), "isoformat"):
            r["created_at"] = r["created_at"].isoformat()

    courses = list(courses_col.find({"teacher_id": teacher["teacher_id"]}, {"_id": 0}))

    followers_count = followers_col.count_documents({"teacher_id": teacher["teacher_id"]})
    is_following = False
    if "user_id" in session:
        is_following = bool(followers_col.find_one(
            {"follower_id": session["user_id"], "teacher_id": teacher["teacher_id"]}))

    return jsonify({
        "teacher": {**serialize(teacher), **(serialize(user) if user else {})},
        "reviews": reviews,
        "courses": [serialize(c) for c in courses],
        "followers": followers_count,
        "is_following": is_following,
    })


# ── Follow / Review ───────────────────────────────────────────────────────────

@app.route("/toggle-follow", methods=["POST"])
@login_required
def toggle_follow():
    user_id   = session["user_id"]
    teacher_id = (request.get_json() or {}).get("teacher_id")
    if not teacher_id:
        return jsonify({"error": "teacher_id required"}), 400
    teacher = teachers_col.find_one({"teacher_id": teacher_id})
    if not teacher or teacher["user_id"] == user_id:
        return jsonify({"error": "Invalid"}), 400
    existing = followers_col.find_one({"follower_id": user_id, "teacher_id": teacher_id})
    if existing:
        followers_col.delete_one({"_id": existing["_id"]})
        return jsonify({"status": "unfollowed"})
    followers_col.insert_one({"follower_id": user_id, "teacher_id": teacher_id})
    return jsonify({"status": "followed"})


@app.route("/add-review/<teacher_id>", methods=["POST"])
@login_required
def add_review(teacher_id):
    user_id = session["user_id"]
    data    = request.get_json() or request.form
    rating  = int(data.get("rating", 5))
    comment = data.get("comment", "")
    teacher = teachers_col.find_one({"teacher_id": teacher_id})
    if not teacher:
        return jsonify({"error": "Teacher not found"}), 404
    if teacher["user_id"] == user_id:
        return jsonify({"error": "Cannot review yourself"}), 400

    existing = reviews_col.find_one({"teacher_id": teacher_id, "user_id": user_id})
    if existing:
        reviews_col.update_one({"_id": existing["_id"]},
                               {"$set": {"rating": rating, "comment": comment, "updated_at": datetime.now()}})
    else:
        reviews_col.insert_one({"teacher_id": teacher_id, "user_id": user_id,
                                 "rating": rating, "comment": comment, "created_at": datetime.now()})

    reviews = list(reviews_col.find({"teacher_id": teacher_id}))
    avg = sum(r["rating"] for r in reviews) / len(reviews) if reviews else 0
    teachers_col.update_one({"teacher_id": teacher_id},
                             {"$set": {"rating": round(avg, 1)}})
    return jsonify({"message": "Review submitted"})


# ── Teacher — home, courses, classes ─────────────────────────────────────────

@app.route("/api/teacher/home")
@login_required
def teacher_home():
    user_id = session["user_id"]
    teacher = teachers_col.find_one({"user_id": user_id})
    teacher_id = teacher["teacher_id"] if teacher else user_id

    my_classes = list(classes_col.find({"teacher_id": teacher_id}))
    my_courses = list(courses_col.find({"teacher_id": teacher_id}, {"_id": 0}))

    out_classes = []
    for c in my_classes:
        out_classes.append({"class_id": c["class_id"], "title": c.get("subject", c.get("title", "")),
                             "date": c.get("date"), "time": c.get("time"),
                             "status": get_class_status(c)})

    return jsonify({"my_classes": out_classes,
                    "my_courses": [serialize(c) for c in my_courses]})


@app.route("/api/teacher/create-class", methods=["POST"])
@login_required
def teacher_create_class():
    user_id = session["user_id"]
    teacher = teachers_col.find_one({"user_id": user_id})
    teacher_id = teacher["teacher_id"] if teacher else user_id
    data = request.get_json() or {}

    title     = (data.get("title") or data.get("subject") or "").strip()
    date      = data.get("date", "").strip()
    time_val  = data.get("time", "").strip()
    if not title or not date or not time_val:
        return jsonify({"error": "title, date, time required"}), 400

    class_id = str(uuid.uuid4())
    classes_col.insert_one({
        "class_id":   class_id, "course_id": data.get("course_id"),
        "teacher_id": teacher_id, "subject": title,
        "date": date, "time": time_val,
        "description": data.get("description", ""),
        "is_free": bool(data.get("is_free", False)),
        "status": "upcoming", "created_at": datetime.now(),
    })
    return jsonify({"message": "Class created", "class_id": class_id})


@app.route("/api/teacher/class/<class_id>", methods=["PUT"])
@login_required
def teacher_update_class(class_id):
    data = request.get_json() or {}
    classes_col.update_one({"class_id": class_id},
                            {"$set": {"subject": data.get("title"), "date": data.get("date"),
                                      "time": data.get("time"), "link": data.get("link")}})
    return jsonify({"message": "Updated"})


@app.route("/api/teacher/class/<class_id>", methods=["DELETE"])
@login_required
def teacher_delete_class(class_id):
    classes_col.delete_one({"class_id": class_id})
    user_classes_col.delete_many({"class_id": class_id})
    return jsonify({"message": "Deleted"})


@app.route("/api/teacher/class/<class_id>/students")
@login_required
def teacher_class_students(class_id):
    enrolled = [e["user_id"] for e in user_classes_col.find({"class_id": class_id})]
    students = list(users_col.find({"id": {"$in": enrolled}}, {"_id": 0, "password": 0}))
    for s in students:
        s["user_id"] = s.get("id")
    return jsonify({"students": students})


@app.route("/api/teacher/create-test", methods=["POST"])
@login_required
def teacher_create_test():
    user_id = session["user_id"]
    data    = request.get_json() or {}
    test_id = str(uuid.uuid4())
    start_raw = data.get("start_time", "")
    start_time = None
    for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S"):
        try:
            start_time = datetime.strptime(start_raw, fmt); break
        except Exception:
            pass
    if not start_time:
        return jsonify({"error": "Invalid start_time format. Use YYYY-MM-DDTHH:MM"}), 400

    tests_col.insert_one({
        "test_id": test_id, "course_id": data.get("course_id"),
        "teacher_id": user_id, "name": data.get("name"),
        "duration": int(data.get("duration", 60)),
        "start_time": start_time,
        "marks_per_q": float(data.get("marks_per_q", 4)),
        "negative_marks": float(data.get("negative_marks", 1)),
        "questions": data.get("questions", []),
        "created_at": datetime.now(),
    })
    return jsonify({"message": "Test created", "test_id": test_id})


@app.route("/api/teacher/announcements")
@login_required
def teacher_announcements():
    docs = list(announcements_col.find({}, {"_id": 0}).sort("created_at", -1).limit(20))
    for d in docs:
        if hasattr(d.get("created_at"), "isoformat"):
            d["created_at"] = d["created_at"].isoformat()
    return jsonify({"announcements": docs})


@app.route("/api/teacher/announcement", methods=["POST"])
@login_required
def teacher_announcement():
    user_id = session["user_id"]
    data    = request.get_json() or {}
    title   = (data.get("title") or "").strip()
    body    = (data.get("body") or "").strip()
    if not title or not body:
        return jsonify({"error": "title and body required"}), 400
    announcements_col.insert_one({
        "announcement_id": str(uuid.uuid4()), "title": title, "body": body,
        "target": "all", "priority": "normal", "created_by": user_id,
        "created_at": datetime.utcnow(),
    })
    return jsonify({"message": "Announcement sent"})


# ── Teacher profile ───────────────────────────────────────────────────────────

@app.route("/teacher/profile")
@login_required
def teacher_profile_api():
    user_id = session["user_id"]
    teacher = teachers_col.find_one({"user_id": user_id}, {"_id": 0})
    user    = users_col.find_one({"id": user_id}, {"_id": 0, "password": 0})
    return jsonify({"teacher": {**(serialize(teacher) if teacher else {}),
                                **(serialize(user) if user else {})}})


@app.route("/update-teacher-profile", methods=["POST"])
@login_required
def update_teacher_profile():
    user_id = session["user_id"]
    data    = request.get_json() or {}
    users_col.update_one({"id": user_id}, {"$set": {"fullname": data.get("fullname")}})
    langs = data.get("languages", "")
    lang_list = [l.strip() for l in langs.split(",")] if langs else []
    teachers_col.update_one({"user_id": user_id}, {"$set": {
        "fullname": data.get("fullname"), "headline": data.get("headline"),
        "bio": data.get("bio"), "category": data.get("category"),
        "education": data.get("education"), "specialization": data.get("specialization"),
        "languages": lang_list, "phone": data.get("phone"),
    }})
    return jsonify({"message": "Profile updated"})


@app.route("/teacher/course/<course_id>")
@login_required
def teacher_course_detail(course_id):
    course  = courses_col.find_one({"course_id": course_id}, {"_id": 0})
    clss    = [serialize(c) for c in classes_col.find({"course_id": course_id})]
    tests   = [serialize(t) for t in tests_col.find({"course_id": course_id})]
    notes   = [serialize(n) for n in notes_col.find({"course_id": course_id})]
    return jsonify({"course": serialize(course) if course else {}, "classes": clss, "tests": tests, "notes": notes})


# ── Notes ─────────────────────────────────────────────────────────────────────

@app.route("/add-note", methods=["POST"])
@login_required
def add_note():
    user_id = session["user_id"]
    teacher = teachers_col.find_one({"user_id": user_id})
    data    = request.get_json() or {}
    title   = (data.get("title") or "").strip()
    if not title:
        return jsonify({"error": "Title required"}), 400
    notes_col.insert_one({
        "note_id": str(uuid.uuid4()), "title": title,
        "description": data.get("description", ""),
        "file_url": data.get("file_url", ""),
        "course_id": data.get("course_id"),
        "teacher_id": teacher["teacher_id"] if teacher else user_id,
        "created_at": datetime.now(),
    })
    return jsonify({"message": "Note added"})


@app.route("/delete-note/<note_id>", methods=["DELETE"])
@login_required
def delete_note(note_id):
    notes_col.delete_one({"note_id": note_id})
    return jsonify({"message": "Deleted"})


@app.route("/upload-note", methods=["POST"])
@login_required
def upload_note():
    try:
        title     = request.form.get("title")
        course_id = request.form.get("course_id")
        if "file" not in request.files:
            return jsonify({"error": "No file"}), 400
        result  = cloudinary.uploader.upload(request.files["file"], resource_type="auto", folder="notes")
        teacher = teachers_col.find_one({"user_id": session["user_id"]})
        notes_col.insert_one({"note_id": str(uuid.uuid4()), "title": title,
                               "file_url": result.get("secure_url"),
                               "course_id": course_id,
                               "teacher_id": teacher["teacher_id"] if teacher else session["user_id"],
                               "created_at": datetime.now()})
        return jsonify({"message": "Uploaded"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Comments (live chat) ──────────────────────────────────────────────────────

@app.route("/get-comments/<class_id>")
@login_required
def get_comments(class_id):
    docs = list(comments_col.find({"class_id": class_id}, {"_id": 0}).sort("created_at", 1).limit(200))
    for d in docs:
        if hasattr(d.get("created_at"), "isoformat"):
            d["time"] = d["created_at"].strftime("%H:%M")
    return jsonify(docs)


@app.route("/add-comment", methods=["POST"])
@login_required
def add_comment():
    user_id = session["user_id"]
    user    = users_col.find_one({"id": user_id})
    data    = request.get_json() or {}
    class_id = data.get("class_id")
    text     = (data.get("text") or "").strip()
    if not class_id or not text:
        return jsonify({"error": "class_id and text required"}), 400
    comments_col.insert_one({
        "class_id": class_id, "user_id": user_id,
        "name": user.get("fullname") if user else "Student",
        "role": user.get("role") if user else "Student",
        "text": text, "type": "message",
        "created_at": datetime.now(),
    })
    return jsonify({"message": "Sent"})


@app.route("/join-class", methods=["POST"])
@login_required
def join_class_chat():
    user_id  = session["user_id"]
    user     = users_col.find_one({"id": user_id})
    class_id = (request.get_json() or {}).get("class_id")
    if class_id and user:
        comments_col.insert_one({
            "class_id": class_id, "user_id": user_id,
            "name": user.get("fullname"), "role": user.get("role"),
            "text": f"{user.get('fullname')} joined", "type": "join",
            "created_at": datetime.now(),
        })
    return jsonify({"status": "ok"})


# ── Polls ─────────────────────────────────────────────────────────────────────

@app.route("/poll/save", methods=["POST"])
@login_required
def save_poll_session():
    try:
        data      = request.get_json() or {}
        class_id  = data.get("class_id"); correct = data.get("correct")
        responses = data.get("responses", {}); total = len(responses)
        correct_count = sum(1 for a in responses.values() if a == correct)
        db.poll_sessions.insert_one({
            "poll_id":         data.get("poll_id") or str(uuid.uuid4()),
            "class_id":        class_id, "teacher_id": session.get("user_id"),
            "poll_type":       data.get("poll_type"), "options": data.get("options", []),
            "correct":         correct, "responses": responses,
            "timer":           data.get("timer", 30), "question_num": data.get("question_num", 1),
            "total_responses": total, "correct_count": correct_count,
            "accuracy":        round(correct_count / total * 100, 1) if total > 0 else 0,
            "created_at":      datetime.now(),
        })
        return jsonify({"message": "Poll saved"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/poll/history/<class_id>")
@login_required
def poll_history(class_id):
    polls = list(db.poll_sessions.find({"class_id": class_id}, {"_id": 0}).sort("created_at", 1))
    for p in polls:
        if hasattr(p.get("created_at"), "isoformat"):
            p["created_at"] = p["created_at"].isoformat()
    return jsonify(polls)


@app.route("/poll/leaderboard/<class_id>")
@login_required
def poll_leaderboard(class_id):
    polls       = list(db.poll_sessions.find({"class_id": class_id}))
    leaderboard = {}
    for poll in polls:
        responses   = poll.get("responses", {})
        correct_idx = poll.get("correct")
        for uid, ans in responses.items():
            if uid not in leaderboard:
                leaderboard[uid] = {"user_id": uid, "name": uid, "score": 0, "correct": 0, "wrong": 0}
            if ans == correct_idx:
                leaderboard[uid]["correct"] += 1; leaderboard[uid]["score"] += 10
            else:
                leaderboard[uid]["wrong"] += 1

    for u in users_col.find({"id": {"$in": list(leaderboard.keys())}}, {"id": 1, "fullname": 1}):
        if u["id"] in leaderboard:
            leaderboard[u["id"]]["name"] = u["fullname"]

    return jsonify(sorted(leaderboard.values(), key=lambda x: x["score"], reverse=True))


# ── Subscription ──────────────────────────────────────────────────────────────

@app.route("/subscribe", methods=["POST"])
@login_required
def subscribe():
    user_id = session["user_id"]
    data    = request.get_json() or {}
    months  = int(data.get("plan", 1))
    expiry  = datetime.utcnow() + timedelta(days=30 * months)
    users_col.update_one({"id": user_id},
                          {"$set": {"subscribed": "yes", "subscription_expiry": expiry}})
    return jsonify({"message": f"Subscribed for {months} month(s)", "expiry": expiry.isoformat()})


@app.route("/create-order", methods=["POST"])
@login_required
def create_order():
    # Integrate your Razorpay key here
    data  = request.get_json() or {}
    plan  = int(data.get("plan", 1))
    price = {1: 29900, 3: 74900, 6: 129900}.get(plan, 29900)  # paise
    return jsonify({"payment_url": None, "amount": price, "currency": "INR",
                    "plan": plan, "message": "Integrate Razorpay order creation here"})


# ── Search ────────────────────────────────────────────────────────────────────

@app.route("/search-page")
def search_page():
    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify({"courses": [], "teachers": [], "tests": []})
    regex = {"$regex": q, "$options": "i"}
    courses  = list(courses_col.find({"name": regex}, {"_id": 0, "course_id": 1, "name": 1}).limit(10))
    teachers = list(teachers_col.find({"fullname": regex}, {"_id": 0, "teacher_id": 1, "fullname": 1, "user_id": 1}).limit(10))
    for t in teachers:
        t["id"] = t.get("user_id")
    tests = list(tests_col.find({"name": regex}, {"_id": 0, "test_id": 1, "name": 1}).limit(10))
    return jsonify({"courses": courses, "teachers": teachers, "tests": tests})


# ── Admin ─────────────────────────────────────────────────────────────────────

@app.route("/admin-analytics")
@role_required("Admin")
def admin_analytics():
    try:
        cat_data = {d["_id"] or "General": d["count"]
                    for d in courses_col.aggregate([{"$group": {"_id": "$category", "count": {"$sum": 1}}}])}
        return jsonify({
            "total_users":   users_col.count_documents({}),
            "total_courses": courses_col.count_documents({}),
            "total_classes": classes_col.count_documents({}),
            "pro_users":     users_col.count_documents({"subscribed": "yes"}),
            "free_users":    users_col.count_documents({"subscribed": {"$ne": "yes"}}),
            "students":      users_col.count_documents({"role": "Student"}),
            "teachers":      users_col.count_documents({"role": "Teacher"}),
            "admins":        users_col.count_documents({"role": "Admin"}),
            "total_tests":   tests_col.count_documents({}),
            "total_notes":   notes_col.count_documents({}),
            "total_attempts": test_attempts_col.count_documents({}),
            "category_breakdown": cat_data,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/admin-users-data")
@role_required("Admin")
def admin_users_data():
    users = list(users_col.find({}, {"_id": 0, "password": 0}))
    for u in users:
        u["id"] = u.get("id", "")
        for k, v in u.items():
            if hasattr(v, "isoformat"):
                u[k] = v.isoformat()
    return jsonify(users)


@app.route("/admin-update-user-role", methods=["POST"])
@role_required("Admin")
def admin_update_user_role():
    data = request.get_json() or {}
    uid  = data.get("user_id"); role = data.get("role")
    if not uid or not role:
        return jsonify({"error": "user_id and role required"}), 400
    users_col.update_one({"id": uid}, {"$set": {"role": role}})
    log_admin_action(f"Changed role of {uid} to {role}", session.get("user_id"))
    return jsonify({"message": "Role updated"})


@app.route("/admin-grant-pro", methods=["POST"])
@role_required("Admin")
def admin_grant_pro():
    uid    = (request.get_json() or {}).get("user_id")
    expiry = datetime.utcnow() + timedelta(days=30)
    users_col.update_one({"id": uid}, {"$set": {"subscribed": "yes", "subscription_expiry": expiry}})
    log_admin_action(f"Granted Pro to {uid}", session.get("user_id"))
    return jsonify({"message": "Pro granted"})


@app.route("/admin-revoke-pro", methods=["POST"])
@role_required("Admin")
def admin_revoke_pro():
    uid = (request.get_json() or {}).get("user_id")
    users_col.update_one({"id": uid}, {"$set": {"subscribed": "no"}})
    log_admin_action(f"Revoked Pro from {uid}", session.get("user_id"))
    return jsonify({"message": "Pro revoked"})


@app.route("/admin-delete-user/<user_id>", methods=["DELETE"])
@role_required("Admin")
def admin_delete_user(user_id):
    users_col.delete_one({"id": user_id})
    teachers_col.delete_one({"user_id": user_id})
    log_admin_action(f"Deleted user {user_id}", session.get("user_id"))
    return jsonify({"message": "User deleted"})


@app.route("/admin-data")
@role_required("Admin")
def admin_data():
    courses = list(courses_col.find({}, {"_id": 0}))
    for c in courses:
        for k, v in c.items():
            if hasattr(v, "isoformat"):
                c[k] = v.isoformat()
    return jsonify(courses)


@app.route("/admin-course-data/<course_id>")
@role_required("Admin")
def admin_course_data(course_id):
    course = courses_col.find_one({"course_id": course_id}, {"_id": 0})
    clss   = [serialize(c) for c in classes_col.find({"course_id": course_id})]
    tests  = [serialize(t) for t in tests_col.find({"course_id": course_id})]
    notes  = [serialize(n) for n in notes_col.find({"course_id": course_id})]
    return jsonify({"course": serialize(course) if course else {}, "classes": clss, "tests": tests, "notes": notes})


@app.route("/admin-update-course", methods=["POST"])
@role_required("Admin")
def admin_update_course():
    data = request.get_json() or {}
    courses_col.update_one({"course_id": data["course_id"]},
                            {"$set": {"name": data.get("name"), "desc": data.get("desc"),
                                      "category": data.get("category")}})
    return jsonify({"message": "Updated"})


@app.route("/add-course", methods=["POST"])
@login_required
def add_course():
    data      = request.get_json() or {}
    course_id = str(uuid.uuid4())
    courses_col.insert_one({"course_id": course_id, "name": data.get("name"),
                             "desc": data.get("desc"), "category": data.get("category"),
                             "teacher_id": session.get("user_id"),
                             "total_classes": 0, "created_at": datetime.now()})
    return jsonify({"message": "Course created", "course_id": course_id})


@app.route("/delete-course/<course_id>", methods=["DELETE"])
@role_required("Admin")
def delete_course(course_id):
    courses_col.delete_one({"course_id": course_id})
    class_ids = [c["class_id"] for c in classes_col.find({"course_id": course_id}, {"class_id": 1})]
    classes_col.delete_many({"course_id": course_id})
    if class_ids:
        user_classes_col.delete_many({"class_id": {"$in": class_ids}})
    notes_col.delete_many({"course_id": course_id})
    tests_col.delete_many({"course_id": course_id})
    user_courses_col.delete_many({"course_id": course_id})
    log_admin_action(f"Deleted course {course_id}", session.get("user_id"))
    return jsonify({"message": "Course and all related data deleted"})


@app.route("/delete-test/<test_id>", methods=["DELETE"])
@login_required
def delete_test(test_id):
    tests_col.delete_one({"test_id": test_id})
    test_attempts_col.delete_many({"test_id": test_id})
    log_admin_action(f"Deleted test {test_id}", session.get("user_id"))
    return jsonify({"message": "Test deleted"})


@app.route("/update-class-admin", methods=["POST"])
@role_required("Admin")
def update_class_admin():
    data = request.get_json() or {}
    classes_col.update_one({"class_id": data["class_id"]},
                            {"$set": {"subject": data.get("subject"), "date": data.get("date"),
                                      "time": data.get("time"), "link": data.get("link")}})
    return jsonify({"message": "Updated"})


@app.route("/admin-activity-log")
@role_required("Admin")
def admin_activity_log():
    docs = list(activity_log_col.find({}, {"_id": 0}).sort("timestamp", -1).limit(100))
    for d in docs:
        if hasattr(d.get("timestamp"), "isoformat"):
            d["timestamp"] = d["timestamp"].isoformat()
    return jsonify(docs)


@app.route("/admin-send-announcement", methods=["POST"])
@role_required("Admin")
def admin_send_announcement():
    data  = request.get_json() or {}
    title = (data.get("title") or "").strip()
    body  = (data.get("body") or "").strip()
    if not title or not body:
        return jsonify({"error": "title and body required"}), 400
    announcements_col.insert_one({
        "announcement_id": str(uuid.uuid4()), "title": title, "body": body,
        "target": data.get("target", "all"), "priority": data.get("priority", "normal"),
        "created_by": session.get("user_id"), "created_at": datetime.utcnow(),
    })
    return jsonify({"message": "Announcement sent"})


# ── Watchtime ─────────────────────────────────────────────────────────────────

@app.route("/update-watchtime", methods=["POST"])
@login_required
def update_watchtime():
    data     = request.get_json() or {}
    class_id = data.get("class_id"); seconds = data.get("seconds", 0)
    user_id  = session["user_id"]
    if not class_id or seconds <= 0:
        return jsonify({"message": "invalid"}), 400
    db.student_watchtime.update_one(
        {"user_id": user_id, "class_id": class_id},
        {"$inc": {"watched_seconds": seconds}, "$set": {"last_updated": datetime.now()}},
        upsert=True
    )
    return jsonify({"message": "updated"})


@app.route("/get-watchtime")
@login_required
def get_watchtime():
    user_id = session["user_id"]
    records = list(db.student_watchtime.find({"user_id": user_id}))
    total   = sum(r.get("watched_seconds", 0) for r in records)
    return jsonify({"total_seconds": total, "hours": total // 3600,
                    "minutes": (total % 3600) // 60, "classes_watched": len(records)})


# ─────────────────────────────────────────────────────────────────────────────
# ERROR HANDLERS
# ─────────────────────────────────────────────────────────────────────────────

@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Not found"}), 404

@app.errorhandler(500)
def server_error(e):
    return jsonify({"error": "Internal server error"}), 500


# ─────────────────────────────────────────────────────────────────────────────
# RUN
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    socketio.run(app, host="0.0.0.0", port=port, debug=False)
