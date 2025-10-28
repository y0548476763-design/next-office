import numpy as np

# μ-law constants
BIAS = 0x84
CLIP = 32635

def _linear2ulaw_sample(sample: int) -> int:
    if sample > CLIP: sample = CLIP
    elif sample < -CLIP: sample = -CLIP
    sign = 0x80 if sample < 0 else 0x00
    if sample < 0: sample = -sample
    sample = sample + BIAS
    exponent = 7; mask = 0x4000
    for i in range(7):
        if sample & mask:
            exponent = 7 - i
            break
        mask >>= 1
    mantissa = (sample >> (exponent + 3)) & 0x0F
    return ~(sign | (exponent << 4) | mantissa) & 0xFF

def _ulaw2linear_sample(ulaw: int) -> int:
    ulaw = ~ulaw & 0xFF
    sign = -1 if (ulaw & 0x80) else 1
    exponent = (ulaw >> 4) & 0x07
    mantissa = ulaw & 0x0F
    sample = ((mantissa << 3) + BIAS) << exponent
    return sign * (sample - BIAS)

def ulaw_to_linear16(ulaw_bytes: bytes) -> bytes:
    if not ulaw_bytes:
        return b""
    arr = np.frombuffer(ulaw_bytes, dtype=np.uint8)
    out = np.array([_ulaw2linear_sample(int(x)) for x in arr], dtype=np.int16)
    return out.tobytes()

def linear16_to_ulaw(pcm16_bytes: bytes) -> bytes:
    if not pcm16_bytes:
        return b""
    arr = np.frombuffer(pcm16_bytes, dtype=np.int16)
    out = np.array([_linear2ulaw_sample(int(x)) for x in arr], dtype=np.uint8)
    return out.tobytes()

def resample_linear16(pcm16_bytes: bytes, from_rate: int, to_rate: int) -> bytes:
    if from_rate == to_rate or not pcm16_bytes:
        return pcm16_bytes
    data = np.frombuffer(pcm16_bytes, dtype=np.int16).astype(np.float32)
    n_src = len(data)
    if n_src == 0:
        return b""
    n_dst = int(round(n_src * float(to_rate) / float(from_rate)))
    if n_dst <= 0:
        return b""
    x_old = np.linspace(0.0, 1.0, num=n_src, endpoint=False, dtype=np.float32)
    x_new = np.linspace(0.0, 1.0, num=n_dst, endpoint=False, dtype=np.float32)
    resampled = np.interp(x_new, x_old, data).astype(np.int16)
    return resampled.tobytes()

def chunk_bytes(buf: bytes, chunk_size: int):
    for i in range(0, len(buf), chunk_size):
        yield buf[i:i+chunk_size]
