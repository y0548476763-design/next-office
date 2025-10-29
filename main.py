import os, json, base64, asyncio, logging
from typing import Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Response, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import ssl, urllib.parse
from websockets.exceptions import InvalidStatusCode
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
app = FastAPI(title="Twilio ׳’ג€ ג€ OpenAI Realtime Bridge")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/", response_class=HTMLResponse)
def index():
    return "<h1>Twilio ׳’ג€ ג€ OpenAI Realtime</h1><p>POST /twilio/voice ׳’ג‚¬ג€ WebSocket /twilio/media ׳’ג‚¬ג€ GET /health</p>"


@app.get("/health")
def health():
    return {"status": "ok"}


def _compute_media_ws(request: Request) -> str:
    """
    ׳³ג€˜׳³ג€¢׳³ֲ ׳³ג€ ׳³ֲ׳³ֳ— ׳³ג€÷׳³ֳ—׳³ג€¢׳³ג€˜׳³ֳ— ׳³ג€׳²ֲ¾wss ׳³ֲ׳²ֲ¾/twilio/media.
    ׳³ֲ׳³ֲ ׳³ג€׳³ג€¢׳³ג€™׳³ג€׳³ֲ¨ PUBLIC_BASE_URL ׳’ג‚¬ג€ ׳³ֲ ׳³ֲ©׳³ֳ—׳³ֲ׳³ֲ© ׳³ג€˜׳³ג€¢; ׳³ֲ׳³ג€”׳³ֲ¨׳³ֳ— ׳³ֲ ׳³ג€˜׳³ֲ ׳³ג€ ׳³ֲ׳³ג€׳²ֲ¾headers ׳³ֲ©׳³ֲ Cloud Run.
    """
    if PUBLIC_BASE_URL:
        base = PUBLIC_BASE_URL
    else:
        # Cloud Run ׳³ֲ׳³ֲ¢׳³ג€˜׳³ג„¢׳³ֲ¨ x-forwarded-host + x-forwarded-proto
        host = request.headers.get("x-forwarded-host") or request.url.netloc or request.url.hostname
        scheme = request.headers.get("x-forwarded-proto", "https")
        base = f"{scheme}://{host}"

    # ׳³ג€׳³ֲ׳³ֲ¨׳³ג€ ׳³ֲ׳²ֲ¾wss
    return base.replace("http://", "wss://").replace("https://", "wss://") + "/twilio/media"


@app.post("/twilio/voice")
async def twilio_voice(
    request: Request,
    CallSid: str = Form(...),
    From: str = Form(...),
    To: str = Form(...),
):
    """
    ׳³ֲ ׳³ֲ§׳³ג€¢׳³ג€׳³ֳ— Twilio Voice Webhook ׳’ג‚¬ג€ ׳³ֲ׳³ג€”׳³ג€“׳³ג„¢׳³ֲ¨׳³ג€ TwiML ׳³ֲ©׳³ֲ׳³ג€”׳³ג€˜׳³ֲ¨ ׳³ֲ׳³ֳ— ׳³ג€׳³ֲ©׳³ג„¢׳³ג€”׳³ג€ ׳³ֲ׳²ֲ¾WebSocket ׳³ֲ©׳³ֲ׳³ֲ ׳³ג€¢.
    ׳³ֲ׳³ֲ©׳³ֳ—׳³ֲ׳³ֲ©׳³ג„¢׳³ֲ ׳³ג€˜-<Connect><Stream> (׳³ֳ—׳³ֲ§׳³ֲ ׳³ג„¢ ׳³ֲ-Media Streams).
    """
    media_ws_url = _compute_media_ws(request)
    logger.info(f"[TwiML] Incoming call From={From} To={To} CallSid={CallSid} ׳’ג€ ג€™ Stream={media_ws_url}")

    # ׳³ֲ©׳³ג„¢׳³ֲ׳³ג€¢׳³ֲ© ׳³ג€˜-voice="alice" ׳³ג€÷׳³ג€׳³ג„¢ ׳³ֲ׳³ג€׳³ג„¢׳³ֲ׳³ֲ ׳³ֲ¢ ׳³ֲ׳³ֲ©׳³ג€™׳³ג„¢׳³ֲ׳³ג€¢׳³ֳ— ׳³ֲ׳³ג„¢׳³ֲ׳³ג€¢׳³ֳ— ׳³ֲ§׳³ג€¢׳³ֲ. (Polly.* ׳³ֲ׳³ֲ ׳³ֳ—׳³ֲ׳³ג„¢׳³ג€ ׳³ג€“׳³ֲ׳³ג„¢׳³ֲ ׳³ג€÷׳³ג€˜׳³ֲ¨׳³ג„¢׳³ֲ¨׳³ֳ— ׳³ֲ׳³ג€”׳³ג€׳³ֲ)
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

    # ׳³ֲ׳³ֲ׳³ג‚×׳³ֲ©׳³ֲ¨ override ׳³ֲ-base URL (׳³ֲ׳³ֲ׳³ֲ©׳³ֲ ׳³ג€׳³ֲ¨׳³ֲ ׳³ג‚×׳³ֲ¨׳³ג€¢׳³ֲ§׳³ֲ¡׳³ג„¢/׳³ֲ׳³ג€“׳³ג€¢׳³ֲ¨)
    base_url = os.getenv("OPENAI_BASE_URL", "wss://api.openai.com")
    uri = f"{base_url}/v1/realtime?model={urllib.parse.quote_plus(REALTIME_MODEL)}"
    headers = [
        ("Authorization", f"Bearer {OPENAI_API_KEY}"),
        ("OpenAI-Beta", "realtime=v1"),
    ]

    ws = None
    # ׳³ֲ¨׳³ג„¢׳³ֻ׳³ֲ¨׳³ג„¢׳³ג„¢ ׳³ֲ§׳³ֲ¦׳³ֲ¨ ׳³ג€÷׳³ג€׳³ג„¢ ׳³ֲ׳³ֲ ׳³ֲ׳³ג€׳³ג‚×׳³ג„¢׳³ֲ ׳³ֲ׳³ֳ— ׳³ֲ©׳³ג„¢׳³ג€”׳³ֳ— ׳³ֻ׳³ג€¢׳³ג€¢׳³ג„¢׳³ֲ׳³ג„¢׳³ג€¢ ׳³ֲ¢׳³ֲ ׳³ג€÷׳³ֲ©׳³ֲ ׳³ֲ¨׳³ג€™׳³ֲ¢׳³ג„¢
    for _attempt in range(3):
        try:
            ws = await websockets.connect(
                uri,
                extra_headers=headers,
                ssl=ssl.create_default_context(),
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                max_queue=None,
                max_size=None,
            )
            break
        except InvalidStatusCode as _e:
            logger.error("OpenAI WS handshake failed (status=%s)", getattr(_e, "status", getattr(_e, "status_code", "?")))
        except Exception as _e:
            logger.exception("OpenAI WS connect failed: %s", _e)
        await asyncio.sleep(0.6)

    if ws is None:
        raise RuntimeError("Upstream connect failed")

    # ׳³ֳ—׳³ֲ¦׳³ג€¢׳³ֲ¨׳³ֳ— ׳³ֲ¡׳³ֲ©׳³ֲ: ׳³ֲ§׳³ג€¢׳³ֲ, ׳³ֲ׳³ג€¢׳³ג€׳³ֲ, VAD ׳³ג€˜׳³ֲ¦׳³ג€ ׳³ג€׳³ֲ©׳³ֲ¨׳³ֳ—, ׳³ג‚×׳³ג€¢׳³ֲ¨׳³ֲ׳³ֻ׳³ג„¢׳³ֲ ׳³ֲ©׳³ֲ ׳³ֲ׳³ג€¢׳³ג€׳³ג„¢׳³ג€¢
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

    # ׳³ג€˜׳³ֲ¨׳³ג€÷׳³ֳ— ׳³ג‚×׳³ֳ—׳³ג„¢׳³ג€”׳³ג€ ׳³ג€÷׳³ג€׳³ג„¢ ׳³ֲ©׳³ֲ׳³ֲ ׳³ג„¢׳³ג€׳³ג„¢׳³ג€ ׳³ֲ©׳³ֲ§׳³ֻ ׳³ג€˜׳³ג€׳³ֳ—׳³ג€”׳³ֲ׳³ג€
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
    Twilio ׳³ג„¢׳³ג€׳³ג€˜׳³ֲ¨ ׳³ֲ׳³ג„¢׳³ֳ—׳³ֲ ׳³ג€¢ ׳³ג€÷׳³ֲ׳³ֲ ׳³ג€˜-WebSocket ׳³ֲ¢׳³ֲ subprotocol=audio.
    ׳³ֲ׳³ֲ ׳³ג€”׳³ֲ ׳³ג€¢ ׳³ֲ׳³ֲ׳³ג€“׳³ג„¢׳³ֲ ׳³ג„¢׳³ֲ ׳³ֲ-events ׳³ֲ©׳³ֲ Twilio (start/media/stop),
    ׳³ֲ׳³ג€“׳³ג„¢׳³ֲ ׳³ג„¢׳³ֲ ׳³ֲ׳³ֳ— ׳³ג€-PCM16 ׳³ֲ-OpenAI, ׳³ג€¢׳³ֲ§׳³ג€¢׳³ֲ׳³ֻ׳³ג„¢׳³ֲ ׳³ג€”׳³ג€“׳³ֲ¨׳³ג€ ׳³ֲ׳³ג€¢׳³ג€׳³ג„¢׳³ג€¢ ׳³ֲ׳³ג€׳³ֲ׳³ג€¢׳³ג€׳³ֲ ׳³ג€¢׳³ֲ©׳³ג€¢׳³ֲ׳³ג€”׳³ג„¢׳³ֲ ׳³ֲ׳³ֻ׳³ג€¢׳³ג€¢׳³ֲ׳³ג„¢׳³ג€¢ ׳³ג€÷-ulaw frames.
    """
    # ׳³ֲ§׳³ג€˜׳³ֲ ׳³ֲ׳³ֳ— ׳³ג€-subprotocol ׳³ֲ©׳³ֻ׳³ג€¢׳³ג€¢׳³ֲ׳³ג„¢׳³ג€¢ ׳³ֲ׳³ֲ¦׳³ג„¢׳³ֲ¢ (׳³ג€˜׳³ג€"׳³ג€÷ "audio")
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
    pump_task: Optional[asyncio.Task] = None
    audio_out_buffer = bytearray()
    inbound_packets = 0
    stream_sid: Optional[str] = None

    async def pump_openai_to_twilio():
        """
        ׳³ֲ§׳³ג€¢׳³ֲ¨׳³ֲ ׳³ג€׳³ג€¢׳³ג€׳³ֲ¢׳³ג€¢׳³ֳ— ׳³ֲ׳³ג€-OpenAI Realtime.
        ׳³ג€÷׳³ֲ©׳³ֲ׳³ג€™׳³ג„¢׳³ֲ¢׳³ג„¢׳³ֲ audio delta-׳³ג„¢׳³ֲ (PCM16/16k), ׳³ֲ׳³ֲ׳³ג„¢׳³ֲ¨ ׳³ֲ-PCM16/8k ׳’ג€ ג€™ ײ¾ֲ¼-law ׳³ג€¢׳³ֲ©׳³ג€¢׳³ֲ׳³ג€” ׳³ג€˜׳³ֲ¨׳³ֲ¦׳³ג€¢׳³ֲ¢׳³ג€¢׳³ֳ— ׳³ֲ©׳³ֲ 20ms (160B ײ¾ֲ¼-law).
        """
        nonlocal audio_out_buffer, stream_sid, stream_sid
        try:
            async for message in oa_ws:
                # ׳³ֲ׳³ֲ ׳³ג€“׳³ג€ bytes - ׳³ֲ׳³ֲ¨׳³ג€¢׳³ג€˜ ׳³ג€“׳³ג€ ׳³ֲ׳³ֲ ׳³ג‚×׳³ֲ¨׳³ג„¢׳³ג„¢׳³ֲ׳³ג„¢ ׳³ֲ׳³ג€¢׳³ג€׳³ג„¢׳³ג€¢ ׳³ג€˜׳³ג‚×׳³ג€¢׳³ֲ¨׳³ֲ׳³ֻ ׳³ֲ©׳³ֲ׳³ֲ ׳³ג€”׳³ֲ ׳³ג€¢ ׳³ֲ׳³ֲ¦׳³ג‚×׳³ג„¢׳³ֲ, ׳³ֲ ׳³ג€׳³ֲ׳³ג€™ ׳³ג€˜׳³ג€˜׳³ֻ׳³ג€”׳³ג€
                if isinstance(message, (bytes, bytearray)):
                    continue

                evt = json.loads(message)
                etype = evt.get("type")

                # ׳³ֲ ׳³ֳ—׳³ֲ׳³ג€¢׳³ֲ ׳³ג€˜׳³ג€÷׳³ֲ׳³ג€ ׳³ֲ©׳³ֲ׳³ג€¢׳³ֳ— ׳³ֲ׳³ג‚×׳³ֲ©׳³ֲ¨׳³ג„¢׳³ג„¢׳³ֲ ׳³ֲ׳³ג€׳³ֲ׳³ֳ—׳³ֲ ׳³ֲ©׳³ֲ ׳³ֲ׳³ג€¢׳³ג€׳³ג„¢׳³ג€¢ (׳³ֲ©׳³ג„¢׳³ֲ ׳³ג€¢׳³ג„¢׳³ג„¢׳³ֲ ׳³ג€˜׳³ג€™׳³ֲ¨׳³ֲ¡׳³ֲ׳³ג€¢׳³ֳ— API)
                is_audio_delta = etype in (
                    "response.audio.delta",
                    "output_audio.delta",
                    "audio.delta",
                )

                if is_audio_delta:
                    # ׳³ג€”׳³ֲ׳³ֲ§ ׳³ֲ׳³ג€׳³ֲ¡׳³ֲ§׳³ג„¢׳³ֲ׳³ג€¢׳³ֳ— ׳³ֲ׳³ֲ©׳³ֳ—׳³ֲ׳³ֲ©׳³ג€¢׳³ֳ— "audio", ׳³ֲ׳³ג€”׳³ֲ¨׳³ג€¢׳³ֳ— "delta" (׳³ֲ ׳³ג€÷׳³ֲ¡׳³ג€ ׳³ֲ׳³ֳ— ׳³ֲ©׳³ֲ ׳³ג„¢ ׳³ג€׳³ֲ׳³ֲ§׳³ֲ¨׳³ג„¢׳³ֲ)
                    b64 = evt.get("audio") or evt.get("delta") or ""
                    if not b64:
                        continue

                    # PCM16 @ 16kHz ׳³ֲ׳³ג€׳³ֲ׳³ג€¢׳³ג€׳³ֲ
                    pcm16_16k = base64.b64decode(b64)
                    audio_out_buffer.extend(pcm16_16k)

                    # ׳³ֲ ׳³ֲ¨׳³ג€¢׳³ֲ§׳³ֲ ׳³ֲ׳³ֲ׳³ֲ§׳³ֻ׳³ֲ¢׳³ג„¢׳³ֲ: ׳³ֲ ׳³ג€˜׳³ֲ¦׳³ֲ¢ downsample ׳³ֲ-8k ׳³ג€¢׳³ֲ ׳³ֲ׳³ג„¢׳³ֲ¨ ׳³ֲ-ײ¾ֲ¼-law
                    pcm16_8k = resample_linear16(bytes(audio_out_buffer), 16000, 8000)
                    ulaw = linear16_to_ulaw(pcm16_8k)

                    # Twilio ׳³ֲ׳³ֲ¦׳³ג‚×׳³ג€ frames ׳³ֲ©׳³ֲ 20ms: 160 ׳³ג€˜׳³ֳ—׳³ג„¢׳³ֲ ײ¾ֲ¼-law ׳³ג€˜׳³ג€÷׳³ֲ ׳³ֲ©׳³ֲ׳³ג„¢׳³ג€”׳³ג€
                    for i in range(0, len(ulaw), 160):
                        payload_b64 = base64.b64encode(ulaw[i : i + 160]).decode("ascii")
                        await ws.send_text(json.dumps({"event": "media", "media": {"payload": payload_b64}}))

                    # ׳³ֲ׳³ג‚×׳³ֲ¡ ׳³ֲ׳³ֳ— ׳³ג€׳³ֲ׳³ֲ׳³ג€™׳³ֲ¨ ׳³ֲ׳³ֲ׳³ג€”׳³ֲ¨ ׳³ֲ©׳³ֲ׳³ג„¢׳³ג€”׳³ג€
                    audio_out_buffer = bytearray()

                elif etype in ("response.completed", "response.refusal.delta", "response.audio.completed"):
                    # ׳³ֲ¡׳³ג„¢׳³ֲ׳³ג€¢׳³ֲ ׳³ֲ§׳³ֲ¦׳³ג€ ׳³ג„¢׳³ג€”׳³ג„¢׳³ג€׳³ֳ— ׳³ג€׳³ג„¢׳³ג€˜׳³ג€¢׳³ֲ¨
                    await ws.send_text(json.dumps({"event": "mark", "mark": {"name": "done"}}))

        except Exception as e:
            logger.exception("pump_openai_to_twilio error: %s", e)

    try:
        # ׳³ג€”׳³ג€˜׳³ֲ¨ ׳³ֲ-OpenAI (׳³ֲ¢׳³ֲ ׳³ֻ׳³ג„¢׳³ג‚×׳³ג€¢׳³ֲ ׳³ג€˜׳³ג€÷׳³ֲ©׳³ֲ ׳³ג€÷׳³ג€׳³ג„¢ ׳³ֲ׳³ֲ ׳³ֲ׳³ג€׳³ג‚×׳³ג„¢׳³ֲ ׳³ֲ׳³ֳ— ׳³ג€-WS ׳³ֲ©׳³ֲ ׳³ֻ׳³ג€¢׳³ג€¢׳³ג„¢׳³ֲ׳³ג„¢׳³ג€¢ ׳³ֲ׳³ג„¢׳³ג€)
        try:
            oa_ws = await openai_realtime_connect()
        except Exception as _e:
            logger.error("OpenAI connect failed; informing Twilio and closing: %s", _e)
            try:
                await ws.send_text(json.dumps({"event":"mark","mark":{"name":"upstream_error"}}))
            except Exception:
                pass
            return

        # ׳³ֲ׳³ֲ׳³ג€“׳³ג„¢׳³ֲ Asynchronous ׳³ֲ׳³ג€׳³ֲ׳³ג€¢׳³ג€׳³ֲ ׳³ֲ׳³ג€÷׳³ג„¢׳³ג€¢׳³ג€¢׳³ֲ ׳³ֻ׳³ג€¢׳³ג€¢׳³ֲ׳³ג„¢׳³ג€¢
        pump_task = asyncio.create_task(pump_openai_to_twilio())

        # ׳³ֲ§׳³ג€˜׳³ֲ׳³ג€ ׳³ֲ׳³ג€׳³ֻ׳³ֲ׳³ג‚×׳³ג€¢׳³ֲ ׳’ג€ ג€™ ׳³ג€׳³ֲ׳³ג€¢׳³ג€׳³ֲ
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
                # ׳³ֲ׳³ג€׳³ג€˜׳³ג„¢׳³ג„¢׳³ֲ¡ 20ms ײ¾ֲ¼-law ׳’ג€ ג€™ PCM16/8k ׳’ג€ ג€™ PCM16/16k ׳’ג€ ג€™ append ׳³ֲ׳²ֲ¾Realtime
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
                    # ׳³ֲ׳³ג€”׳³ֳ— ׳³ֲ~1 ׳³ֲ©׳³ֲ ׳³ג„¢׳³ג€ (׳³ג€˜׳³ֲ¢׳³ֲ¨׳³ֲ 50 ׳³ג‚×׳³ֲ¨׳³ג„¢׳³ג„¢׳³ֲ׳³ג„¢׳³ֲ ׳³ֲ©׳³ֲ 20ms) ׳³ֲ ׳³ג€˜׳³ֲ§׳³ֲ© ׳³ֲ׳³ג€׳³ֲ׳³ג€¢׳³ג€׳³ֲ ׳³ֲ׳³ג€׳³ג€™׳³ג„¢׳³ג€˜
                    if inbound_packets % 50 == 0:
                        await oa_ws.send(json.dumps({"type": "input_audio_buffer.commit"}))
                        await oa_ws.send(json.dumps({"type": "response.create", "response": {}}))

                except Exception as e:
                    logger.exception("media frame handling error: %s", e)

            elif t_type == "stop":
                logger.info("[Twilio] stream stopped by Twilio")
                # ׳³ֲ ׳³ֲ¡׳³ג€™׳³ג€¢׳³ֲ¨ ׳³ג„¢׳³ג‚×׳³ג€: commit ׳³ֲ׳³ג€”׳³ֲ¨׳³ג€¢׳³ֲ ׳³ג€¢׳³ג€˜׳³ֲ§׳³ֲ©׳³ֳ— ׳³ֳ—׳³ג€™׳³ג€¢׳³ג€˜׳³ג€ (׳³ֲ׳³ֲ ׳³ֲ¦׳³ֲ¨׳³ג„¢׳³ֲ)
                try:
                    await oa_ws.send(json.dumps({"type": "input_audio_buffer.commit"}))
                    await oa_ws.send(json.dumps({"type": "response.create", "response": {}}))
                except Exception:
                    pass
                break

        # ׳³ֲ¡׳³ג€™׳³ג„¢׳³ֲ¨׳³ג€ ׳³ֲ׳³ֲ¡׳³ג€¢׳³ג€׳³ֲ¨׳³ֳ— ׳³ֲ©׳³ֲ ׳³ג€-task (׳³ֲ¨׳³ֲ§ ׳³ֲ׳³ֲ ׳³ֲ ׳³ג€¢׳³ֲ¦׳³ֲ¨)
        if pump_task:
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
