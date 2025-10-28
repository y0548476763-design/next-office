import os, json, base64, asyncio, logging
from typing import Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Response, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import websockets

from utils_audio import ulaw_to_linear16, linear16_to_ulaw, resample_linear16

# ---------- Logging ----------
logger = logging.getLogger("voice-rt")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------- Env ----------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
REALTIME_MODEL = os.getenv("REALTIME_MODEL", "gpt-4o-realtime-preview")
VOICE = os.getenv("VOICE", "alloy")  # OpenAI voice name
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")

# ---------- FastAPI ----------
app = FastAPI(title="Twilio ↔ OpenAI Realtime Bridge")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/", response_class=HTMLResponse)
def index():
    return "<h1>Twilio ↔ OpenAI Realtime</h1><p>POST /twilio/voice — WebSocket /twilio/media — GET /health</p>"


@app.get("/health")
def health():
    return {"status": "ok"}


def _compute_media_ws(request: Request) -> str:
    """
    בונה את כתובת ה־wss ל־/twilio/media.
    אם הוגדר PUBLIC_BASE_URL – נשתמש בו; אחרת נבנה מה־headers של Cloud Run.
    """
    if PUBLIC_BASE_URL:
        base = PUBLIC_BASE_URL
    else:
        # Cloud Run מעביר x-forwarded-host + x-forwarded-proto
        host = request.headers.get("x-forwarded-host") or request.url.netloc or request.url.hostname
        scheme = request.headers.get("x-forwarded-proto", "https")
        base = f"{scheme}://{host}"

    # המרה ל־wss
    return base.replace("http://", "wss://").replace("https://", "wss://") + "/twilio/media"


@app.post("/twilio/voice")
async def twilio_voice(
    request: Request,
    CallSid: str = Form(...),
    From: str = Form(...),
    To: str = Form(...),
):
    """
    נקודת Twilio Voice Webhook — מחזירה TwiML שמחבר את השיחה ל־WebSocket שלנו.
    משתמשים ב-<Connect><Stream> (תקני ל-Media Streams).
    """
    media_ws_url = _compute_media_ws(request)
    logger.info(f"[TwiML] Incoming call From={From} To={To} CallSid={CallSid} → Stream={media_ws_url}")

    # שימוש ב-voice="alice" כדי להימנע משגיאות אימות קול. (Polly.* לא תמיד זמין כברירת מחדל)
    twiml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Say voice="alice">You are now connected to the A I assistant.</Say>
  <Connect>
    <Stream url="{media_ws_url}"/>
  </Connect>
</Response>
""".strip()

    return Response(content=twiml, media_type="application/xml")


# ---------- OpenAI Realtime ----------
async def openai_realtime_connect():
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not set")

    uri = f"wss://api.openai.com/v1/realtime?model={REALTIME_MODEL}"
    headers = [
        ("Authorization", f"Bearer {OPENAI_API_KEY}"),
        ("OpenAI-Beta", "realtime=v1"),
    ]

    ws = await websockets.connect(
        uri,
        extra_headers=headers,
        ping_interval=20,
        ping_timeout=20,
        max_size=None,
    )

    # תצורת סשן: קול, מודל, VAD בצד השרת, פורמטים של אודיו
    session_update = {
        "type": "session.update",
        "session": {
            "voice": VOICE,
            "modalities": ["audio"],
            "turn_detection": {"type": "server_vad", "silence_duration_ms": 700},
            "input_audio_format": {"type": "pcm16", "sample_rate": 16000},
            "output_audio_format": {"type": "pcm16", "sample_rate": 16000},
            "instructions": (
                "You are a helpful assistant on a phone call. "
                "Speak clearly and keep responses concise. "
                "If the caller is silent, proactively greet them."
            ),
        },
    }
    await ws.send(json.dumps(session_update))

    # ברכת פתיחה כדי שלא יהיה שקט בהתחלה
    initial_say = {
        "type": "response.create",
        "response": {
            "instructions": "Greet the caller briefly. Introduce yourself and ask how you can help.",
        },
    }
    await ws.send(json.dumps(initial_say))
    return ws


# ---------- Twilio <-> OpenAI bridge over WebSocket ----------
@app.websocket("/twilio/media")
async def twilio_media(ws: WebSocket):
    """
    Twilio ידבר איתנו כאן ב-WebSocket עם subprotocol=audio.
    אנחנו מאזינים ל-events של Twilio (start/media/stop),
    מזינים את ה-PCM16 ל-OpenAI, וקולטים חזרה אודיו מהמודל ושולחים לטווליו כ-ulaw frames.
    """
    # קבל את ה-subprotocol שטווליו מציע (בד"כ "audio")
    proto_hdr = ws.headers.get("sec-websocket-protocol")
    chosen_sub = "audio"
    if proto_hdr:
        offers = [p.strip() for p in proto_hdr.split(",")]
        if "audio" in offers:
            chosen_sub = "audio"
        elif "media" in offers:
            chosen_sub = "media"
        else:
            chosen_sub = offers[0]
    await ws.accept(subprotocol=chosen_sub)
    logger.info(f"[WS] Twilio connected with subprotocol={chosen_sub}")

    oa_ws: Optional[websockets.WebSocketClientProtocol] = None
    audio_out_buffer = bytearray()
    inbound_packets = 0

    async def pump_openai_to_twilio():
        """
        קורא הודעות מה-OpenAI Realtime.
        כשמגיעים audio delta-ים (PCM16/16k), ממיר ל-PCM16/8k → μ-law ושולח ברצועות של 20ms (160B μ-law).
        """
        nonlocal audio_out_buffer
        try:
            async for message in oa_ws:
                # אם זה bytes - לרוב זה לא פריימי אודיו בפורמט שאנחנו מצפים, נדלג בבטחה
                if isinstance(message, (bytes, bytearray)):
                    continue

                evt = json.loads(message)
                etype = evt.get("type")

                # נתמוך בכמה שמות אפשריים לדלתא של אודיו (שינויים בגרסאות API)
                is_audio_delta = etype in (
                    "response.audio.delta",
                    "output_audio.delta",
                    "audio.delta",
                )

                if is_audio_delta:
                    # חלק מהסקימות משתמשות "audio", אחרות "delta" (נכסה את שני המקרים)
                    b64 = evt.get("audio") or evt.get("delta") or ""
                    if not b64:
                        continue

                    # PCM16 @ 16kHz מהמודל
                    pcm16_16k = base64.b64decode(b64)
                    audio_out_buffer.extend(pcm16_16k)

                    # נרוקן למקטעים: נבצע downsample ל-8k ונמיר ל-μ-law
                    pcm16_8k = resample_linear16(bytes(audio_out_buffer), 16000, 8000)
                    ulaw = linear16_to_ulaw(pcm16_8k)

                    # Twilio מצפה frames של 20ms: 160 בתים μ-law בכל שליחה
                    for i in range(0, len(ulaw), 160):
                        payload_b64 = base64.b64encode(ulaw[i : i + 160]).decode("ascii")
                        await ws.send_text(json.dumps({"event": "media", "media": {"payload": payload_b64}}))

                    # אפס את המאגר לאחר שליחה
                    audio_out_buffer = bytearray()

                elif etype in ("response.completed", "response.refusal.delta", "response.audio.completed"):
                    # סימון קצה יחידת דיבור
                    await ws.send_text(json.dumps({"event": "mark", "mark": {"name": "done"}}))

        except Exception as e:
            logger.exception("pump_openai_to_twilio error: %s", e)

    try:
        # חבר ל-OpenAI
        oa_ws = await openai_realtime_connect()

        # מאזין Asynchronous מהמודל לכיוון טווליו
        pump_task = asyncio.create_task(pump_openai_to_twilio())

        # קבלה מהטלפון → המודל
        while True:
            try:
                msg = await ws.receive_text()
            except WebSocketDisconnect:
                logger.info("[WS] Twilio disconnected")
                break
            except Exception as e:
                logger.exception("WS receive error: %s", e)
                break

            try:
                t_evt = json.loads(msg)
            except Exception:
                continue

            t_type = t_evt.get("event")
            if t_type == "start":
                stream_sid = t_evt.get("start", {}).get("streamSid")
                logger.info(f"[Twilio] stream started sid={stream_sid}")

            elif t_type == "media":
                # מדבייס 20ms μ-law → PCM16/8k → PCM16/16k → append ל־Realtime
                payload_b64 = t_evt.get("media", {}).get("payload")
                if not payload_b64:
                    continue
                try:
                    ulaw_8k = base64.b64decode(payload_b64)
                    pcm16_8k = ulaw_to_linear16(ulaw_8k)          # PCM16 @ 8kHz
                    pcm16_16k = resample_linear16(pcm16_8k, 8000, 16000)

                    await oa_ws.send(json.dumps({
                        "type": "input_audio_buffer.append",
                        "audio": base64.b64encode(pcm16_16k).decode("ascii")
                    }))

                    inbound_packets += 1
                    # אחת ל~1 שניה (בערך 50 פריימים של 20ms) נבקש מהמודל להגיב
                    if inbound_packets % 50 == 0:
                        await oa_ws.send(json.dumps({"type": "input_audio_buffer.commit"}))
                        await oa_ws.send(json.dumps({"type": "response.create", "response": {}}))

                except Exception as e:
                    logger.exception("media frame handling error: %s", e)

            elif t_type == "stop":
                logger.info("[Twilio] stream stopped by Twilio")
                # נסגור יפה: commit אחרון ובקשת תגובה (אם צריך)
                try:
                    await oa_ws.send(json.dumps({"type": "input_audio_buffer.commit"}))
                    await oa_ws.send(json.dumps({"type": "response.create", "response": {}}))
                except Exception:
                    pass
                break

        # סגירה מסודרת של ה-task
        pump_task.cancel()
        try:
            await pump_task
        except Exception:
            pass

    finally:
        try:
            await ws.close()
        except Exception:
            pass
        try:
            if oa_ws and not oa_ws.closed:
                await oa_ws.close()
        except Exception:
            pass
