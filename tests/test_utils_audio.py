import numpy as np
from utils_audio import ulaw_to_linear16, linear16_to_ulaw, resample_linear16

def test_ulaw_roundtrip_len():
    sr = 16000
    t = np.arange(int(0.1 * sr))
    pcm = (np.sin(2 * np.pi * 1000 * t / sr) * 16000).astype(np.int16).tobytes()
    ul = linear16_to_ulaw(pcm)
    rt = ulaw_to_linear16(ul)
    assert len(rt) == len(pcm)

def test_resample_lengths():
    sr_from, sr_to = 16000, 8000
    data = (np.random.randn(16000).astype(np.int16)).tobytes()
    down = resample_linear16(data, sr_from, sr_to)
    up = resample_linear16(down, sr_to, sr_from)
    assert len(down) == len(data)//2
    assert len(up) == len(data)
