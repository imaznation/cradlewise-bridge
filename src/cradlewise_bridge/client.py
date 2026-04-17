"""High-level client that wires auth + REST + Janus + audio + recording together.

This is the batteries-included entry point. Instantiate with your credentials
and a list of cribs, register optional callbacks, and call :meth:`run` in an
asyncio event loop. The client will:

    * Authenticate with Cradlewise (via pycradlewise / Cognito SRP).
    * For each crib:
        - Activate the Janus video room via REST (AWS SigV4).
        - Connect to Janus WebSocket with HMAC-signed headers.
        - Subscribe to the feed and negotiate WebRTC.
        - Fan video frames into a per-crib :class:`SegmentRecorder`.
        - Fan audio frames into a per-crib :class:`AudioSpikeDetector`.
    * Auto-reconnect with exponential backoff on any failure.
    * Re-authenticate from scratch after repeated 403s.

If you want finer control (e.g. you already have auth, or you want to use
Janus without the recorder), use :mod:`cradlewise_bridge.janus` directly.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Awaitable, Callable

from .audio import AudioSpikeDetector
from .janus import JanusVideoRoomClient
from .recorder import SegmentRecorder
from .rest import fetch_video_room

logger = logging.getLogger(__name__)

MAX_BACKOFF = 60.0
INITIAL_BACKOFF = 2.0


@dataclass
class CribConfig:
    """Identifiers for a single crib.

    Look these up once by calling :func:`pycradlewise.CradlewiseClient.discover_cradles`
    after authenticating; the returned cradles expose ``id`` (cradle_id) and
    ``device_id`` fields.
    """
    name: str
    cradle_id: str
    device_id: str


SpikeCallback = Callable[[str, str], Awaitable[None]]  # (crib_name, wav_path)
SegmentCallback = Callable[[str, str], Awaitable[None]]  # (crib_name, mp4_path)


@dataclass
class CradlewiseBridge:
    """Manage connections to one or more cribs.

    Args:
        email, password: Cradlewise account credentials. If both are None,
            the bridge expects an already-authenticated ``auth`` object to be
            provided (see :meth:`with_existing_auth`).
        cribs: List of cribs to connect to.
        output_root: Parent directory for snapshots/, recordings/, audio_snippets/.
        target_fps: Recorded video FPS (source feed is ~15fps).
        retention_hours: How long to keep recorded segments.
        on_audio_spike: Async callback fired when a WAV is saved.
        on_segment_flushed: Async callback fired when an MP4 is written.
    """
    email: str
    password: str
    cribs: list[CribConfig]
    output_root: Path = field(default_factory=lambda: Path("./cradlewise_data"))
    target_fps: float = 2.0
    retention_hours: float = 48.0

    on_audio_spike: SpikeCallback | None = None
    on_segment_flushed: SegmentCallback | None = None

    _auth: object | None = field(default=None, init=False, repr=False)
    _app_config: object | None = field(default=None, init=False, repr=False)
    _recorders: dict[str, SegmentRecorder] = field(default_factory=dict, init=False, repr=False)
    _detectors: dict[str, AudioSpikeDetector] = field(default_factory=dict, init=False, repr=False)
    _running: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        self.output_root = Path(self.output_root)
        self.output_root.mkdir(parents=True, exist_ok=True)

    # --- Lifecycle -----------------------------------------------------------

    async def authenticate(self) -> None:
        """Authenticate with Cradlewise (Cognito SRP via pycradlewise)."""
        from pycradlewise import CradlewiseAuth, get_app_config

        self._app_config = await get_app_config()
        self._auth = CradlewiseAuth(
            email=self.email, password=self.password, app_config=self._app_config,
        )
        await self._auth.authenticate()
        logger.info("Cradlewise authentication OK (region=%s)", self._app_config.cognito_region)

    async def run(self) -> None:
        """Connect to every configured crib and run until :meth:`stop`.

        Blocks the calling task. Each crib runs in its own task with its own
        reconnect loop, so a single crib's flakiness doesn't affect others.
        """
        if self._auth is None:
            await self.authenticate()

        self._running = True

        # Build per-crib recorders + detectors
        for crib in self.cribs:
            rec_dir = self.output_root / "recordings" / crib.name
            snip_dir = self.output_root / "audio_snippets" / crib.name
            db_path = self.output_root / "segments.db"
            self._recorders[crib.name] = SegmentRecorder(
                name=crib.name,
                recording_dir=rec_dir,
                target_fps=self.target_fps,
                retention_hours=self.retention_hours,
                index_db_path=db_path,
            )
            self._detectors[crib.name] = AudioSpikeDetector(
                name=crib.name,
                snippet_dir=snip_dir,
            )

        tasks = [asyncio.create_task(self._crib_loop(c), name=f"cw-{c.name}") for c in self.cribs]
        tick_task = asyncio.create_task(self._tick_loop(), name="cw-tick")

        try:
            await asyncio.gather(*tasks, tick_task, return_exceptions=True)
        finally:
            self._running = False

    def stop(self) -> None:
        """Signal all crib loops to stop on the next iteration."""
        self._running = False

    # --- Per-crib connection loop -------------------------------------------

    async def _crib_loop(self, crib: CribConfig) -> None:
        backoff = INITIAL_BACKOFF
        consecutive_403s = 0

        while self._running:
            try:
                await self._connect_once(crib)
                backoff = INITIAL_BACKOFF
                consecutive_403s = 0
            except Exception as e:
                msg = str(e)
                logger.warning("[%s] connection failed: %s", crib.name, msg)
                if "403" in msg:
                    consecutive_403s += 1
                    if consecutive_403s >= 2:
                        logger.info("[%s] re-authenticating after repeated 403s", crib.name)
                        try:
                            await self.authenticate()
                            consecutive_403s = 0
                        except Exception:
                            logger.exception("[%s] re-auth failed", crib.name)

            if not self._running:
                break

            logger.info("[%s] reconnecting in %.0fs", crib.name, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)

    async def _connect_once(self, crib: CribConfig) -> None:
        creds = await fetch_video_room(
            self._auth, self._app_config, crib.cradle_id, crib.device_id,
        )

        recorder = self._recorders[crib.name]
        detector = self._detectors[crib.name]

        async def on_video(frame):
            img = frame.to_image()
            recorder.add_frame(img, timestamp=_timestamp_from_frame(frame))

        async def on_audio(frame):
            detector.add_audio_frame(frame)

        async def on_connected():
            logger.info("[%s] WebRTC stream established", crib.name)

        client = JanusVideoRoomClient(
            cradle_id=crib.cradle_id,
            device_id=crib.device_id,
            credentials=creds,
            on_video_frame=on_video,
            on_audio_frame=on_audio,
            on_connected=on_connected,
        )
        try:
            await client.run()
        finally:
            client.stop()

    async def _tick_loop(self) -> None:
        """Drive recorder.tick() and detector.check_and_save_spike() periodically."""
        while self._running:
            for name, detector in self._detectors.items():
                try:
                    wav = detector.check_and_save_spike()
                except Exception:
                    logger.exception("[%s] spike detection raised", name)
                    continue
                if wav and self.on_audio_spike:
                    try:
                        await self.on_audio_spike(name, wav)
                    except Exception:
                        logger.exception("on_audio_spike callback raised")

            for name, recorder in self._recorders.items():
                try:
                    mp4 = recorder.tick()
                except Exception:
                    logger.exception("[%s] recorder tick raised", name)
                    continue
                if mp4 and self.on_segment_flushed:
                    try:
                        await self.on_segment_flushed(name, mp4)
                    except Exception:
                        logger.exception("on_segment_flushed callback raised")

            await asyncio.sleep(1.0)

    # --- Introspection -------------------------------------------------------

    def recorder(self, crib_name: str) -> SegmentRecorder | None:
        return self._recorders.get(crib_name)

    def detector(self, crib_name: str) -> AudioSpikeDetector | None:
        return self._detectors.get(crib_name)


def _timestamp_from_frame(frame) -> float:
    """Best-effort unix timestamp for a received video frame."""
    import time
    # aiortc frames don't carry wall-clock capture time; we stamp at receipt,
    # which introduces sub-second jitter but is fine for 2fps recording.
    return time.time()
