"""Audio spike detector with adaptive threshold and deferred post-spike capture.

WebRTC applies noise suppression to the crib's audio stream, which keeps the
ambient RMS near zero when nothing is happening but allows real sounds (a
fussy cry, a word spoken into the crib mic, a door closing nearby) through
at RMS 0.01–0.3. That dynamic range makes cry detection a straightforward
thresholding problem *if* you pick the threshold adaptively — a fixed cutoff
either misses quiet fussing in a quiet room or fires constantly when a fan
is running.

Algorithm:
    1. Maintain an exponential moving average of the 30-second RMS baseline.
    2. Effective threshold = max(absolute floor, 1.8 × baseline).
    3. When current 1s RMS exceeds threshold, start a *deferred* recording:
       wait 5 seconds so the captured WAV includes ~5s before and ~5s after
       the spike (centered around the sound).
    4. After 5s post-spike, write a WAV named with the crib, timestamp, and
       trigger RMS.
    5. Rate-limit: one snippet per crib every 30s.

All buffering is done in a threadsafe deque. Feed it :meth:`add_samples`
from your audio track callback and call :meth:`check_and_save_spike`
periodically (e.g. once a second) from any task.
"""
from __future__ import annotations

import logging
import threading
import time
import wave
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class AudioSpikeDetector:
    """Ring-buffered PCM audio with adaptive spike detection and WAV persistence.

    Args:
        name: Label used in snippet filenames and log lines.
        snippet_dir: Directory where WAVs are written. Created if missing.
        sample_rate: Set via :meth:`configure` once the first audio frame
            arrives (Opus decodes to 48kHz typically).
        max_buffer_seconds: How much audio to keep in memory.
        spike_floor: Absolute RMS floor. Below this, nothing ever triggers —
            prevents pure-silence jitter from firing.
        adaptive_multiplier: Threshold = max(floor, multiplier × baseline).
        snippet_duration: Seconds of audio written per snippet. The detector
            grabs this much audio *after* the post-spike wait, so the captured
            window is centered on the trigger.
        snippet_cooldown: Minimum seconds between snippets.
        ambient_window: Seconds of audio used to compute the rolling baseline.
        post_spike_record: Seconds to keep recording after a spike before
            writing the snippet. This is what creates the "centered" window.
    """
    name: str = "crib"
    snippet_dir: Path = field(default_factory=lambda: Path("audio_snippets"))
    sample_rate: int = 48000

    max_buffer_seconds: float = 60.0
    spike_floor: float = 0.008
    adaptive_multiplier: float = 1.8
    snippet_duration: float = 10.0
    snippet_cooldown: float = 30.0
    ambient_window: float = 30.0
    post_spike_record: float = 5.0

    _samples: deque = field(default_factory=lambda: deque(maxlen=48000 * 60))
    _lock: threading.Lock = field(default_factory=threading.Lock)

    _ambient_rms: float = 0.0
    _ambient_updates: int = 0
    _last_snippet_time: float = 0.0
    _recording_trigger_time: float = 0.0
    _recording_trigger_rms: float = 0.0

    def __post_init__(self) -> None:
        self.snippet_dir = Path(self.snippet_dir)
        self.snippet_dir.mkdir(parents=True, exist_ok=True)

    # --- Buffer management ---------------------------------------------------

    def configure(self, sample_rate: int) -> None:
        """Call once per connection, with the track's actual sample rate."""
        with self._lock:
            self.sample_rate = sample_rate
            max_samples = int(self.max_buffer_seconds * sample_rate)
            self._samples = deque(self._samples, maxlen=max_samples)

    def add_samples(self, samples: np.ndarray) -> None:
        """Append float32 samples in [-1, 1]. Multi-channel input is mixed to mono."""
        if samples.ndim > 1:
            samples = samples.mean(axis=0)
        with self._lock:
            self._samples.extend(samples.astype(np.float32).tolist())

    def add_audio_frame(self, frame) -> None:
        """Convenience wrapper for an aiortc :class:`av.AudioFrame`."""
        arr = frame.to_ndarray()
        if arr.dtype == np.int16:
            mono = arr.mean(axis=0).astype(np.float32) / 32768.0
        else:
            mono = arr.mean(axis=0).astype(np.float32)
        if self.sample_rate != frame.sample_rate:
            self.configure(frame.sample_rate)
        self.add_samples(mono)

    def clear(self) -> None:
        with self._lock:
            self._samples.clear()

    # --- Analysis ------------------------------------------------------------

    def rms(self, seconds: float = 1.0) -> float:
        audio = self._recent(seconds)
        if audio is None or len(audio) == 0:
            return 0.0
        return float(np.sqrt(np.mean(audio ** 2)))

    @property
    def effective_threshold(self) -> float:
        if self._ambient_rms > 0:
            return max(self.spike_floor, self._ambient_rms * self.adaptive_multiplier)
        return self.spike_floor

    def update_ambient(self) -> None:
        """Refresh the rolling-baseline EMA from the last `ambient_window` seconds."""
        current = self.rms(self.ambient_window)
        if current <= 0:
            return
        if self._ambient_updates == 0:
            self._ambient_rms = current
        else:
            # α = 0.1 — slow adaptation, so a ~10s cry doesn't poison the baseline.
            self._ambient_rms = 0.9 * self._ambient_rms + 0.1 * current
        self._ambient_updates += 1

    def check_and_save_spike(self) -> str | None:
        """Run one detection cycle. Call periodically (e.g. every 1s).

        Returns the path of a WAV that was just written, or None if no
        spike completed this cycle.

        The detector is two-phase:
          1. If a previous spike is recording, check whether the post-spike
             window has elapsed; if so, write the snippet.
          2. Otherwise, compare current RMS to threshold; if over, start a
             new deferred recording (which will complete on a later call).
        """
        now = time.time()
        self.update_ambient()

        # Phase 2: finish a deferred recording
        if self._recording_trigger_time > 0:
            elapsed = now - self._recording_trigger_time
            if elapsed >= self.post_spike_record:
                audio = self._recent(self.snippet_duration)
                if audio is None or len(audio) < self.sample_rate:
                    self._reset_recording()
                    return None
                trigger_rms = self._recording_trigger_rms
                self._reset_recording()
                self._last_snippet_time = now
                logger.info(
                    "%s spike: rms=%.4f > threshold=%.4f (ambient=%.4f)",
                    self.name, trigger_rms, self.effective_threshold, self._ambient_rms,
                )
                return self._write_wav(audio, trigger_rms)
            return None

        # Phase 1: detect a new spike
        if (now - self._last_snippet_time) < self.snippet_cooldown:
            return None
        current = self.rms(1.0)
        if current < self.effective_threshold:
            return None
        self._recording_trigger_time = now
        self._recording_trigger_rms = current
        logger.info(
            "%s spike detected (rms=%.4f) — recording %.1fs post-spike",
            self.name, current, self.post_spike_record,
        )
        return None

    # --- Internals -----------------------------------------------------------

    def _recent(self, seconds: float) -> np.ndarray | None:
        with self._lock:
            if not self._samples:
                return None
            n = int(seconds * self.sample_rate)
            if len(self._samples) >= n:
                return np.fromiter((self._samples[i] for i in range(len(self._samples) - n, len(self._samples))), dtype=np.float32, count=n)
            return np.fromiter(self._samples, dtype=np.float32, count=len(self._samples))

    def _reset_recording(self) -> None:
        self._recording_trigger_time = 0.0
        self._recording_trigger_rms = 0.0

    def _write_wav(self, audio: np.ndarray, rms: float) -> str | None:
        try:
            date_dir = self.snippet_dir / time.strftime("%Y-%m-%d")
            date_dir.mkdir(parents=True, exist_ok=True)
            filename = f"{self.name}-{time.strftime('%H%M%S')}-rms{rms:.3f}.wav"
            path = date_dir / filename
            int16 = (audio * 32767).astype(np.int16)
            with wave.open(str(path), "wb") as wf:
                wf.setnchannels(1)
                wf.setsampwidth(2)
                wf.setframerate(self.sample_rate)
                wf.writeframes(int16.tobytes())
            return str(path)
        except Exception:
            logger.exception("Failed to write audio snippet")
            return None
