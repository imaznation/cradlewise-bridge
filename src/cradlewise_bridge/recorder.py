"""Continuous video recording: ring buffer → 5-minute MP4 segments → 48h retention.

The Janus feed delivers ~15 fps of H.264 video. We decimate to a target FPS
(default 2) to keep storage reasonable — at 2 fps with 640×480 frames, 48
hours is on the order of 1 GB. Frames are held in a rolling in-memory buffer
so you always have the recent past for on-demand snapshots, then flushed
every 5 minutes to a dated MP4 on disk.

A segment is encoded with imageio-ffmpeg (H.264 via the bundled ffmpeg
binary, so no system ffmpeg required). Each segment is an independent file
keyed by crib name + wall-clock timestamp; a tiny SQLite index lets
callers ask "what file contains this moment?" in O(log n).

The retention sweep runs once an hour and deletes segments older than
``retention_hours``.
"""
from __future__ import annotations

import logging
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from PIL import Image

logger = logging.getLogger(__name__)


@dataclass
class BufferedFrame:
    """A decoded frame with its capture timestamp (unix seconds)."""
    image: Image.Image
    timestamp: float

    def to_pil(self) -> Image.Image:
        return self.image


class FrameRingBuffer:
    """Thread-safe rolling buffer of decoded frames, decimated to target FPS.

    The buffer length is bounded by wall-clock duration, not frame count, so
    a hiccup in frame arrival doesn't silently shrink the history window.
    """

    def __init__(self, max_seconds: float = 30.0, target_fps: float = 2.0) -> None:
        self.max_seconds = max_seconds
        self.target_fps = target_fps
        self._min_interval = 1.0 / target_fps
        self._frames: list[BufferedFrame] = []
        self._last_add = 0.0
        self._lock = threading.Lock()

    def add_frame(self, image: Image.Image, timestamp: float | None = None) -> bool:
        """Add a frame. Returns True if accepted, False if decimated out."""
        ts = timestamp if timestamp is not None else time.time()
        if ts - self._last_add < self._min_interval:
            return False
        with self._lock:
            self._frames.append(BufferedFrame(image=image, timestamp=ts))
            cutoff = ts - self.max_seconds
            # Drop frames older than the window. List stays small (~60 items).
            while self._frames and self._frames[0].timestamp < cutoff:
                self._frames.pop(0)
        self._last_add = ts
        return True

    def drain(self) -> list[BufferedFrame]:
        """Remove and return all buffered frames (for flushing to disk)."""
        with self._lock:
            frames = self._frames
            self._frames = []
            return frames

    def recent(self, seconds: float = 5.0) -> list[BufferedFrame]:
        """Return a copy of the most recent `seconds` of frames."""
        cutoff = time.time() - seconds
        with self._lock:
            return [f for f in self._frames if f.timestamp >= cutoff]

    @property
    def duration_seconds(self) -> float:
        with self._lock:
            if len(self._frames) < 2:
                return 0.0
            return self._frames[-1].timestamp - self._frames[0].timestamp

    @property
    def frame_count(self) -> int:
        with self._lock:
            return len(self._frames)


@dataclass
class SegmentRecorder:
    """Accumulate frames, flush as MP4 segments, purge old segments.

    Typical usage is via :class:`cradlewise_bridge.CradlewiseBridge`, but the
    recorder is usable standalone: push frames with :meth:`add_frame` and call
    :meth:`tick` periodically to trigger segment flushes and purges.
    """
    name: str
    recording_dir: Path
    target_fps: float = 2.0
    segment_seconds: float = 300.0  # 5 minutes
    retention_hours: float = 48.0
    ring_buffer_seconds: float = 30.0
    index_db_path: Path | None = None

    _accumulator: list[BufferedFrame] = field(default_factory=list)
    _last_flush: float = field(default_factory=time.time)
    _last_purge: float = 0.0
    _db: sqlite3.Connection | None = field(default=None, init=False)
    _ring: FrameRingBuffer = field(init=False)

    def __post_init__(self) -> None:
        self.recording_dir = Path(self.recording_dir)
        self.recording_dir.mkdir(parents=True, exist_ok=True)
        self._ring = FrameRingBuffer(
            max_seconds=self.ring_buffer_seconds,
            target_fps=self.target_fps,
        )
        if self.index_db_path is not None:
            self._db = sqlite3.connect(str(self.index_db_path), check_same_thread=False)
            self._db.execute("""
                CREATE TABLE IF NOT EXISTS segments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    crib_name TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT NOT NULL,
                    duration_sec REAL NOT NULL,
                    file_path TEXT NOT NULL UNIQUE,
                    file_size INTEGER NOT NULL,
                    frame_count INTEGER NOT NULL
                )
            """)
            self._db.execute(
                "CREATE INDEX IF NOT EXISTS idx_segments_lookup ON segments(crib_name, start_time, end_time)"
            )
            self._db.commit()

    # --- Public API ----------------------------------------------------------

    def add_frame(self, image: Image.Image, timestamp: float | None = None) -> None:
        """Push a new frame into the ring buffer (decimated to target FPS)."""
        if self._ring.add_frame(image, timestamp):
            # Accept into the accumulation buffer for flushing
            ts = timestamp if timestamp is not None else time.time()
            self._accumulator.append(BufferedFrame(image=image, timestamp=ts))

    def recent_snapshot(self) -> Image.Image | None:
        """Return the most recent buffered frame, or None if the buffer is empty."""
        frames = self._ring.recent(seconds=self.ring_buffer_seconds)
        return frames[-1].image if frames else None

    def tick(self) -> str | None:
        """Run periodic work: flush a segment if due, purge old segments hourly.

        Returns the path of a segment that was just flushed, or None.
        Call roughly once every 5–30 seconds.
        """
        now = time.time()
        flushed: str | None = None

        if self._accumulator and (now - self._last_flush) >= self.segment_seconds:
            flushed = self._flush_segment()
            self._last_flush = now

        if (now - self._last_purge) >= 3600:
            self._purge_old()
            self._last_purge = now

        return flushed

    def flush_now(self) -> str | None:
        """Force a segment flush regardless of the 5-minute cadence."""
        if not self._accumulator:
            return None
        path = self._flush_segment()
        self._last_flush = time.time()
        return path

    def segment_at(self, timestamp: float) -> str | None:
        """Find the segment file that covers the given unix timestamp."""
        if not self._db:
            return None
        ts_iso = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
        row = self._db.execute(
            "SELECT file_path FROM segments "
            "WHERE crib_name = ? AND start_time <= ? AND end_time >= ? LIMIT 1",
            (self.name, ts_iso, ts_iso),
        ).fetchone()
        if row and Path(row[0]).exists():
            return row[0]
        return None

    def segments_in_range(self, start_ts: float, end_ts: float) -> list[dict[str, Any]]:
        """List all segments overlapping the given unix time range."""
        if not self._db:
            return []
        start_iso = datetime.fromtimestamp(start_ts, tz=timezone.utc).isoformat()
        end_iso = datetime.fromtimestamp(end_ts, tz=timezone.utc).isoformat()
        rows = self._db.execute(
            "SELECT id, start_time, end_time, duration_sec, file_path, file_size "
            "FROM segments WHERE crib_name = ? AND start_time <= ? AND end_time >= ? "
            "ORDER BY start_time",
            (self.name, end_iso, start_iso),
        ).fetchall()
        return [
            {
                "id": r[0], "start_time": r[1], "end_time": r[2],
                "duration_sec": r[3], "file_path": r[4], "file_size": r[5],
            }
            for r in rows if Path(r[4]).exists()
        ]

    def stats(self) -> dict[str, Any]:
        total_bytes = 0
        segment_count = 0
        oldest: float | None = None
        for mp4 in self.recording_dir.rglob("*.mp4"):
            try:
                st = mp4.stat()
            except OSError:
                continue
            total_bytes += st.st_size
            segment_count += 1
            if oldest is None or st.st_mtime < oldest:
                oldest = st.st_mtime
        hours = (time.time() - oldest) / 3600 if oldest else 0.0
        return {
            "segments": segment_count,
            "total_mb": round(total_bytes / (1024 ** 2), 1),
            "hours_stored": round(hours, 1),
            "retention_hours": self.retention_hours,
        }

    # --- Internals -----------------------------------------------------------

    def _flush_segment(self) -> str | None:
        if len(self._accumulator) < 2:
            return None
        try:
            import imageio.v2 as imageio
        except ImportError:
            logger.error("imageio-ffmpeg is required to encode video segments")
            return None

        frames = self._accumulator
        self._accumulator = []

        start_dt = datetime.fromtimestamp(frames[0].timestamp, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(frames[-1].timestamp, tz=timezone.utc)
        date_dir = self.recording_dir / start_dt.strftime("%Y-%m-%d")
        date_dir.mkdir(parents=True, exist_ok=True)
        path = date_dir / f"{self.name}-{start_dt.strftime('%H%M%S')}.mp4"

        try:
            writer = imageio.get_writer(
                str(path), fps=self.target_fps, codec="libx264",
                macro_block_size=1, quality=7,
            )
            try:
                for f in frames:
                    writer.append_data(_pil_to_ndarray(f.image))
            finally:
                writer.close()
        except Exception:
            logger.exception("Failed to encode segment %s", path)
            return None

        size = path.stat().st_size if path.exists() else 0
        duration = max(0.0, frames[-1].timestamp - frames[0].timestamp)
        logger.info(
            "%s segment flushed: %s (%.1fs, %d frames, %dKB)",
            self.name, path.name, duration, len(frames), size // 1024,
        )

        if self._db:
            try:
                self._db.execute(
                    "INSERT OR IGNORE INTO segments "
                    "(crib_name, start_time, end_time, duration_sec, file_path, file_size, frame_count) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (self.name, start_dt.isoformat(), end_dt.isoformat(),
                     duration, str(path), size, len(frames)),
                )
                self._db.commit()
            except sqlite3.Error:
                logger.exception("Failed to index segment")

        return str(path)

    def _purge_old(self) -> None:
        cutoff = time.time() - self.retention_hours * 3600
        try:
            for date_dir in sorted(self.recording_dir.iterdir()):
                if not date_dir.is_dir():
                    continue
                any_kept = False
                for seg in list(date_dir.iterdir()):
                    try:
                        if seg.stat().st_mtime < cutoff:
                            seg.unlink(missing_ok=True)
                        else:
                            any_kept = True
                    except OSError:
                        pass
                if not any_kept:
                    try:
                        date_dir.rmdir()
                    except OSError:
                        pass
            if self._db:
                cutoff_iso = datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat()
                self._db.execute("DELETE FROM segments WHERE end_time < ?", (cutoff_iso,))
                self._db.commit()
        except Exception:
            logger.exception("Purge failed")


def _pil_to_ndarray(img: Image.Image):
    import numpy as np
    if img.mode != "RGB":
        img = img.convert("RGB")
    return np.array(img)
