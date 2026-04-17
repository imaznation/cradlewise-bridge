"""Janus videoroom WebRTC client.

This is the core reverse-engineered piece. The Cradlewise cloud publishes each
crib's live feed into a Janus gateway; mobile clients connect over WebSocket
(subprotocol ``janus-protocol``) and subscribe to the crib as a WebRTC peer.

Connection flow (all over one WebSocket):
    1. ``create``  — open a Janus session
    2. ``attach`` (videoroom plugin, publisher) — get a handle
    3. ``message`` {request: join, ptype: publisher}  — discover the crib's feed
    4. ``attach`` (videoroom plugin, subscriber) — second handle for receive
    5. ``message`` {request: join, ptype: subscriber, streams: [{feed}]}
    6. Receive SDP offer + trickle ICE candidates
    7. Inject the candidates into the SDP (a=candidate lines after a=ice-pwd)
    8. Create local answer, send back as ``{request: start}`` with jsep
    9. WebRTC negotiates, tracks fire; keepalives every ~10s

The WebSocket itself is authenticated with HMAC-signed headers derived from
per-session parameters returned by :mod:`cradlewise_bridge.rest`. The full
signing scheme is documented in PROTOCOL.md.

This module is deliberately free of dependencies on any larger framework —
it takes a :class:`VideoRoomCredentials`, a cradle id, and a device id, and
exposes async iterators over received video frames and audio frames.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import hmac
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from .rest import VideoRoomCredentials

logger = logging.getLogger(__name__)

# Janus keepalives must be sent at least every 60s, typically every 30s.
# We send every 10s so that a one-off drop doesn't immediately kill the session.
KEEPALIVE_INTERVAL = 10.0

# If no media has arrived in this long, assume the connection is wedged and reconnect.
FRAME_TIMEOUT = 60.0


@dataclass
class JanusSignedHeaders:
    """HMAC-signed headers for the Janus WebSocket handshake.

    The Cradlewise gateway expects these headers on the WebSocket upgrade
    request. The signature is HMAC-SHA256 over the SHA-256 hex digest of the
    canonical "key:value" newline-joined representation of the signed headers,
    using the per-session ``auth_secret`` returned by the REST video room
    endpoint as the HMAC key.

    See PROTOCOL.md §"Janus WebSocket signing" for the canonicalization rules.
    """
    origin: str
    cradle_id: str
    device_id: str
    timestamp: str
    session_id: str
    signature: str

    def as_dict(self) -> dict[str, str]:
        signed_keys = "X-Origin,X-CId,X-DId,X-Timestamp,X-SId"
        return {
            "X-Origin": self.origin,
            "X-CId": self.cradle_id,
            "X-DId": self.device_id,
            "X-Timestamp": self.timestamp,
            "X-SId": self.session_id,
            "X-Signed-Keys": signed_keys,
            "Authorization": f"HMAC {self.signature}",
        }


def sign_ws_headers(
    cradle_id: str,
    device_id: str,
    secret: str,
    origin: str = "20000",
) -> JanusSignedHeaders:
    """Build HMAC-signed WebSocket headers for the Janus gateway handshake.

    Signing algorithm:
        1. Build the header set (X-Origin, X-CId, X-DId, X-Timestamp, X-SId).
        2. Timestamp format is ``YYYYMMDDHHMMSS{microseconds:06d}Z`` (UTC).
        3. Canonical string: ``\\n``.join(``f"{k.lower()}:{v}"`` for each signed key).
        4. Inner hash: ``sha256(canonical_string).hexdigest()``.
        5. Signature: ``hmac_sha256(secret, inner_hex).hexdigest()``.
        6. ``Authorization: HMAC <signature>``.

    Args:
        cradle_id: Cradle UUID.
        device_id: Device identifier.
        secret: Per-session HMAC secret (``VideoRoomCredentials.auth_secret``).
        origin: Client origin marker; ``"20000"`` matches the current mobile client.
    """
    now = dt.datetime.now(dt.timezone.utc)
    timestamp = f"{now.strftime('%Y%m%d%H%M%S')}{now.microsecond:06d}Z"
    session_id = str(uuid.uuid4())

    headers_for_sig = {
        "X-Origin": origin,
        "X-CId": cradle_id,
        "X-DId": device_id,
        "X-Timestamp": timestamp,
        "X-SId": session_id,
    }
    signed_keys = ["X-Origin", "X-CId", "X-DId", "X-Timestamp", "X-SId"]
    canonical = "\n".join(f"{k.lower()}:{headers_for_sig[k]}" for k in signed_keys)
    inner_hex = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    signature = hmac.new(
        secret.encode("utf-8"),
        inner_hex.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    return JanusSignedHeaders(
        origin=origin,
        cradle_id=cradle_id,
        device_id=device_id,
        timestamp=timestamp,
        session_id=session_id,
        signature=signature,
    )


def inject_trickle_candidates_into_sdp(sdp: str, candidates: list[dict]) -> str:
    """Merge Janus trickle ICE candidates into an SDP offer.

    Janus delivers the SDP offer and ICE candidates in separate messages:
    the offer first, then a series of ``{janus: "trickle", candidate: {...}}``
    messages (ending with ``{completed: true}``). Rather than feed them to
    the peer connection out-of-band, we splice them back into the SDP so the
    aiortc peer connection sees a complete offer.

    Each candidate's ``sdpMid`` indicates which m-section to attach it to.
    We insert the ``a=candidate:...`` line after the ``a=ice-pwd:`` line of
    that section — a neutral location that every Janus-issued SDP contains.

    Args:
        sdp: The offer SDP received from Janus.
        candidates: List of trickle candidate payloads (without ``completed``).

    Returns:
        A new SDP string with candidates inlined.
    """
    for cand in candidates:
        candidate_line = cand.get("candidate", "")
        if not candidate_line:
            continue
        mid = int(cand.get("sdpMid", "0"))

        new_lines: list[str] = []
        m_index = -1
        inserted = False
        for line in sdp.split("\r\n"):
            new_lines.append(line)
            if line.startswith("m="):
                m_index += 1
            if not inserted and m_index == mid and line.startswith("a=ice-pwd:"):
                new_lines.append(f"a={candidate_line}")
                inserted = True
        sdp = "\r\n".join(new_lines)

    return sdp


FrameCallback = Callable[[Any], Awaitable[None]]


@dataclass
class ConnectionStats:
    connected: bool = False
    frames_received: int = 0
    audio_samples_received: int = 0
    last_frame_time: float = 0.0
    errors: int = 0
    last_error: str = ""


@dataclass
class JanusVideoRoomClient:
    """Subscribe to a single crib's video+audio feed via Janus.

    This is a one-shot connector: call :meth:`run` and it will connect, stream
    media to your callbacks, and return when the connection ends. Wrap it in
    a reconnect loop (see :class:`CradlewiseBridge`) for production use.

    Callbacks are async and must not block. They receive raw ``av.VideoFrame``
    and ``av.AudioFrame`` objects from aiortc; convert to PIL/numpy there.
    """
    cradle_id: str
    device_id: str
    credentials: VideoRoomCredentials

    on_video_frame: FrameCallback | None = None
    on_audio_frame: FrameCallback | None = None
    on_connected: Callable[[], Awaitable[None]] | None = None

    stats: ConnectionStats = field(default_factory=ConnectionStats)

    # Internal state
    _stop: bool = field(default=False, init=False)

    def stop(self) -> None:
        """Signal the run loop to shut down. Safe to call from any task."""
        self._stop = True

    async def run(self) -> None:
        """Connect, stream media, and return when the session ends.

        Any exception (auth, network, SDP parse) propagates to the caller —
        catch them and reconnect yourself. On graceful shutdown (peer hangup,
        :meth:`stop` called) returns without raising.
        """
        import aiohttp
        from aiortc import RTCPeerConnection, RTCSessionDescription, RTCConfiguration, RTCIceServer
        from aiortc.contrib.media import MediaRelay

        headers = sign_ws_headers(
            self.cradle_id, self.device_id, self.credentials.auth_secret,
        ).as_dict()

        async with aiohttp.ClientSession() as session:
            ws = await session.ws_connect(
                self.credentials.ws_url,
                headers=headers,
                protocols=["janus-protocol"],
            )
            try:
                await self._drive_connection(ws)
            finally:
                if not ws.closed:
                    try:
                        await ws.close()
                    except Exception:
                        pass

    async def _drive_connection(self, ws) -> None:
        """Run the full Janus connection dance, then maintain until disconnect."""
        from aiortc import RTCPeerConnection, RTCSessionDescription, RTCConfiguration, RTCIceServer
        from aiortc.contrib.media import MediaRelay

        # Step 1: create Janus session
        await ws.send_json({"janus": "create", "transaction": "c1"})
        r = await ws.receive_json(timeout=10)
        janus_session = r["data"]["id"]

        # Step 2: attach videoroom plugin (publisher handle used to discover feeds)
        await ws.send_json({
            "janus": "attach",
            "plugin": "janus.plugin.videoroom",
            "opaque_id": self.credentials.opaque_id,
            "transaction": "c2",
            "session_id": janus_session,
        })
        r = await ws.receive_json(timeout=10)
        pub_handle = r["data"]["id"]

        # Step 3: join the room as a publisher to see what feeds are present
        await ws.send_json({
            "janus": "message",
            "body": {
                "request": "join",
                "ptype": "publisher",
                "display": f"{self.device_id}_bridge",
                "pin": self.credentials.pin,
                "room": self.credentials.room_id,
            },
            "transaction": "c3",
            "session_id": janus_session,
            "handle_id": pub_handle,
        })

        feed_id, private_id = await self._await_publisher_list(ws, janus_session)
        if feed_id is None:
            raise ConnectionError("Crib did not publish a video feed within the wait window")

        logger.info("Crib feed discovered: %s", feed_id)

        # Step 4: attach a second handle to use as subscriber
        await ws.send_json({
            "janus": "attach",
            "plugin": "janus.plugin.videoroom",
            "opaque_id": self.credentials.opaque_id,
            "transaction": "c4",
            "session_id": janus_session,
        })
        r = await ws.receive_json(timeout=10)
        sub_handle = r["data"]["id"]

        # Step 5: subscribe to the crib's feed
        await ws.send_json({
            "janus": "message",
            "body": {
                "request": "join",
                "ptype": "subscriber",
                "room": self.credentials.room_id,
                "pin": self.credentials.pin,
                "streams": [{"feed": feed_id}],
                "private_id": private_id,
            },
            "transaction": "c5",
            "session_id": janus_session,
            "handle_id": sub_handle,
        })

        # Step 6: collect SDP offer and trickle candidates
        sdp, trickle = await self._await_offer_and_trickle(ws)
        sdp = inject_trickle_candidates_into_sdp(sdp, trickle)

        # Step 7: WebRTC negotiation
        pc = RTCPeerConnection(
            configuration=RTCConfiguration(iceServers=[RTCIceServer(urls="stun:stun.l.google.com:19302")])
        )
        relay = MediaRelay()
        tracks: dict[str, Any] = {"video": None, "audio": None}

        @pc.on("track")
        def _on_track(track):
            # MediaRelay.subscribe() gives us a stable, buffered proxy track.
            # Without it, aiortc sometimes drops frames under GC pressure.
            logger.info("Received track: %s (state=%s)", track.kind, track.readyState)
            tracks[track.kind] = relay.subscribe(track)

        await pc.setRemoteDescription(RTCSessionDescription(sdp=sdp, type="offer"))
        answer = await pc.createAnswer()
        await pc.setLocalDescription(answer)

        # Step 8: send SDP answer back via Janus
        await ws.send_json({
            "janus": "message",
            "body": {"request": "start"},
            "jsep": {
                "type": "answer",
                "sdp": pc.localDescription.sdp,
                "trickle": False,
            },
            "transaction": "c6",
            "session_id": janus_session,
            "handle_id": sub_handle,
        })

        self.stats.connected = True
        self.stats.last_frame_time = _now()

        # Kick off receive tasks once tracks have appeared
        recv_tasks: list[asyncio.Task] = []
        if tracks["video"] and self.on_video_frame:
            recv_tasks.append(asyncio.create_task(self._video_loop(tracks["video"])))
        if tracks["audio"] and self.on_audio_frame:
            recv_tasks.append(asyncio.create_task(self._audio_loop(tracks["audio"])))

        if self.on_connected:
            await self.on_connected()

        try:
            await self._maintain_session(ws, janus_session)
        finally:
            for t in recv_tasks:
                t.cancel()
            for t in recv_tasks:
                try:
                    await asyncio.wait_for(asyncio.shield(t), timeout=1.0)
                except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                    pass
            await pc.close()
            if not ws.closed:
                try:
                    await ws.send_json({
                        "janus": "destroy",
                        "session_id": janus_session,
                        "transaction": "bye",
                    })
                except Exception:
                    pass

    async def _await_publisher_list(self, ws, janus_session: int) -> tuple[int | None, int | None]:
        """Wait for the ``joined`` event and any initial publishers list."""
        feed_id: int | None = None
        private_id: int | None = None

        for _ in range(15):
            r = await ws.receive_json(timeout=5)
            if r.get("janus") == "event":
                pd = r.get("plugindata", {}).get("data", {})
                if pd.get("videoroom") == "joined":
                    private_id = pd.get("private_id")
                    pubs = pd.get("publishers", [])
                    if pubs:
                        feed_id = pubs[0]["id"]
                    break

        if feed_id is not None:
            return feed_id, private_id

        # Crib wasn't already publishing — wait for it to appear.
        wait_secs = self.credentials.wait_for_cradle_secs
        for _ in range(wait_secs):
            try:
                r = await asyncio.wait_for(ws.receive_json(), timeout=2)
            except asyncio.TimeoutError:
                if ws.closed:
                    break
                try:
                    await ws.send_json({
                        "janus": "keepalive",
                        "session_id": janus_session,
                        "transaction": uuid.uuid4().hex[:8],
                    })
                except Exception:
                    break
                continue
            if r.get("janus") == "event":
                pd = r.get("plugindata", {}).get("data", {})
                pubs = pd.get("publishers", [])
                if pubs:
                    feed_id = pubs[0]["id"]
                    break

        return feed_id, private_id

    async def _await_offer_and_trickle(self, ws) -> tuple[str, list[dict]]:
        """Collect the SDP offer and all trickle candidates until ``completed``."""
        sdp: str | None = None
        trickle: list[dict] = []
        for _ in range(20):
            r = await ws.receive_json(timeout=5)
            jt = r.get("janus")
            if jt == "event":
                jsep = r.get("jsep")
                if jsep and jsep.get("type") == "offer":
                    sdp = jsep["sdp"]
            elif jt == "trickle":
                cand = r.get("candidate", {})
                if cand.get("completed"):
                    break
                trickle.append(cand)
            elif jt == "ack":
                continue
        if sdp is None:
            raise ConnectionError("Janus did not send an SDP offer")
        return sdp, trickle

    async def _maintain_session(self, ws, janus_session: int) -> None:
        """Pump the WebSocket, send keepalives, watch for publisher-leaving."""
        while not self._stop and self.stats.connected:
            try:
                r = await asyncio.wait_for(ws.receive_json(), timeout=KEEPALIVE_INTERVAL)
            except asyncio.TimeoutError:
                if ws.closed:
                    self.stats.connected = False
                    break
                try:
                    await ws.send_json({
                        "janus": "keepalive",
                        "session_id": janus_session,
                        "transaction": uuid.uuid4().hex[:8],
                    })
                except Exception:
                    self.stats.connected = False
                    break
                if _now() - self.stats.last_frame_time > FRAME_TIMEOUT:
                    logger.warning("No frames in %ds — assuming connection is wedged", FRAME_TIMEOUT)
                    self.stats.connected = False
                    break
                continue
            except Exception as e:
                logger.warning("WebSocket recv failed: %s", e)
                self.stats.connected = False
                break

            jt = r.get("janus")
            if jt == "webrtcup":
                logger.info("WebRTC established")
            elif jt == "event":
                pd = r.get("plugindata", {}).get("data", {})
                if pd.get("leaving"):
                    logger.info("Publisher left the room")
                    self.stats.connected = False
                    break

    async def _video_loop(self, track) -> None:
        while not self._stop:
            try:
                frame = await asyncio.wait_for(track.recv(), timeout=15)
            except asyncio.TimeoutError:
                if track.readyState == "ended":
                    return
                continue
            except Exception:
                return
            self.stats.frames_received += 1
            self.stats.last_frame_time = _now()
            if self.on_video_frame:
                try:
                    await self.on_video_frame(frame)
                except Exception:
                    logger.exception("on_video_frame callback raised")

    async def _audio_loop(self, track) -> None:
        while not self._stop:
            try:
                frame = await asyncio.wait_for(track.recv(), timeout=15)
            except asyncio.TimeoutError:
                continue
            except Exception:
                return
            self.stats.audio_samples_received += frame.samples
            if self.on_audio_frame:
                try:
                    await self.on_audio_frame(frame)
                except Exception:
                    logger.exception("on_audio_frame callback raised")


def _now() -> float:
    import time
    return time.monotonic()
