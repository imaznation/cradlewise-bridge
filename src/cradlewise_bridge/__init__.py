"""cradlewise-bridge: unofficial interoperability library for Cradlewise smart cribs.

This library authenticates as you (using your own Cradlewise credentials) and
subscribes to the video/audio stream your crib publishes to Cradlewise's
Janus WebRTC gateway. It is read-only — it never sends control commands.

See README.md for the legal and ethical framing, and PROTOCOL.md for the
reverse-engineered wire-level details.
"""
from .client import CradlewiseBridge, CribConfig
from .janus import JanusVideoRoomClient
from .local import LocalVideoRoomClient
from .rest import VideoRoomCredentials, fetch_video_room
from .audio import AudioSpikeDetector
from .recorder import SegmentRecorder

__version__ = "0.2.0"
__all__ = [
    "CradlewiseBridge",
    "CribConfig",
    "JanusVideoRoomClient",
    "LocalVideoRoomClient",
    "VideoRoomCredentials",
    "fetch_video_room",
    "AudioSpikeDetector",
    "SegmentRecorder",
]
