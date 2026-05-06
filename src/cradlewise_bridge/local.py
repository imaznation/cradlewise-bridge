"""Local-mode (LAN) WebRTC client for Cradlewise cribs.

When the subscriber is on the same LAN as the crib, the official mobile app
bypasses the cloud Janus gateway entirely and exchanges WebRTC offers/answers
directly with the crib over its local Greengrass MQTT broker. This module
implements that path.

**Status — 2026-05-05:** Signaling layer is fully working — the cradle
accepts our handshake, mirrors us into ``monitor.localPeers``, and trickles
its ICE candidates back. ICE negotiation completes (UDP host pair). The
remaining blocker is the DTLS-SRTP handshake failing silently between
aiortc and the cradle's libwebrtc-derived stack — likely an aiortc
interop quirk. iOS app source filters for TCP-only candidates, but
aiortc has incomplete TCP-ICE support, so that workaround is out of
reach without switching WebRTC libraries (gstreamer webrtcbin or
node-wrtc bridge).

This module is the signaling reference; producing decoded video frames
requires solving the DTLS layer, which is the next session's work.

Why prefer it over the cloud (Janus) path:
    * Lower latency (no internet round-trip).
    * No dependency on the ``/cradles/{id}/videoRoom`` Lambda, which can gate
      requests on a server-side device-state field that's expensive to poke.
    * Lower exposure: only your own crib's mTLS broker is touched; no
      shared cloud surface area.

Reuse the same auth artifacts as the rest of this library: the per-crib mTLS
cert, key, and group-CA that ``pycradlewise`` (or the ``pairedUsers/v3`` REST
flow) downloads. ``pycradlewise.client.CradlewiseClient.refresh_device_certs``
returns these paths.

This is a one-shot connector with the same shape as
:class:`cradlewise_bridge.janus.JanusVideoRoomClient`: callbacks for video
and audio frames, a :meth:`run` method that returns when the session ends,
and a :meth:`stop` method that any task can call.

See ``PROTOCOL.md §10 — Local mode (LAN signaling)`` for the wire format.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import re
import ssl
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)

# Match the shape of the Janus client for swap-in compatibility.
KEEPALIVE_INTERVAL = 10.0
FRAME_TIMEOUT = 60.0

# Local broker constants — Greengrass on the cradle uses fixed MQTT settings.
MQTT_PORT = 8883
APPLICATION_NAME = "live"


FrameCallback = Callable[[Any], Awaitable[None]]


@dataclass
class LocalConnectionStats:
    connected: bool = False
    frames_received: int = 0
    audio_samples_received: int = 0
    last_frame_time: float = 0.0
    errors: int = 0
    last_error: str = ""


@dataclass
class LocalVideoRoomClient:
    """Subscribe to a crib's video+audio over its local LAN MQTT signaling.

    Args:
        cradle_id: Cradle UUID (from the per-crib mTLS cert / pairedUsers).
        device_id: Our paired-device UUID for *this* crib. The local broker's
            IoT policy gates ``iot:Connect`` on ``client_id == device_id`` —
            using anything else returns ``Not authorized`` at CONNACK.
        cradle_ip: Cradle's LAN IP (e.g. ``192.168.68.69``). Returned by the
            cradle in its shadow under ``state.reported.bluetooth.wifiStats``,
            or discoverable via the REST ``/cradles/{id}/onlineStatus/v2``
            response under ``state_message.info.connectivity.localIP``.
        cert_path, key_path, ca_path: Paths to the per-crib mTLS material.
        on_video_frame, on_audio_frame: Async callbacks invoked with each
            received ``av.VideoFrame`` / ``av.AudioFrame``. Same shape as the
            Janus client's callbacks.
        on_connected: Optional async callback invoked once after the WebRTC
            session is established.

    Lifecycle:
        Construct → ``await client.run()``. ``run()`` returns when the peer
        hangs up, when :meth:`stop` is called, or when an exception occurs.
        Always wrap in a reconnect loop for production use.
    """
    cradle_id: str
    device_id: str
    cradle_ip: str
    cert_path: str
    key_path: str
    ca_path: str

    on_video_frame: FrameCallback | None = None
    on_audio_frame: FrameCallback | None = None
    on_connected: Callable[[], Awaitable[None]] | None = None

    stats: LocalConnectionStats = field(default_factory=LocalConnectionStats)

    _stop: bool = field(default=False, init=False)
    _mqtt: Any = field(default=None, init=False)
    _loop: asyncio.AbstractEventLoop | None = field(default=None, init=False)
    _inbox: asyncio.Queue | None = field(default=None, init=False)
    _session_id: str = field(default="", init=False)
    _topic: str = field(default="", init=False)
    _pc: Any = field(default=None, init=False)

    def stop(self) -> None:
        """Signal the run loop to shut down. Safe to call from any task."""
        self._stop = True

    async def run(self) -> None:
        """Connect, stream media, and return when the session ends."""
        self._loop = asyncio.get_running_loop()
        self._inbox = asyncio.Queue()
        self._session_id = _now_ms_str()
        self._topic = f"/{self.cradle_id}/room"

        try:
            await self._connect_mqtt()
            await self._negotiate_and_stream()
        finally:
            await self._cleanup()

    # --- MQTT plumbing -------------------------------------------------------

    async def _connect_mqtt(self) -> None:
        import paho.mqtt.client as mqtt

        ready = self._loop.create_future()

        def on_connect(c, ud, flags, rc, props=None):
            rc_val = rc.value if hasattr(rc, "value") else rc
            if rc_val != 0:
                msg = f"local broker CONNACK rejected: {rc}"
                logger.error("[%s] %s", self.cradle_id, msg)
                if not ready.done():
                    ready.set_exception(ConnectionError(msg))
                return
            c.subscribe(self._topic, qos=0)

        def on_subscribe(c, ud, mid, rcs, props=None):
            if not ready.done():
                ready.set_result(True)

        def on_disconnect(c, ud, flags, rc, props=None):
            self.stats.connected = False

        def on_message(c, ud, msg):
            try:
                payload = json.loads(msg.payload)
            except Exception:
                return
            if self._loop is None or self._inbox is None:
                return
            self._loop.call_soon_threadsafe(self._inbox.put_nowait, payload)

        client = mqtt.Client(
            client_id=self.device_id,  # MUST equal device_id; see PROTOCOL.md §10
            protocol=mqtt.MQTTv311,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            clean_session=True,
        )
        client.tls_set(
            ca_certs=self.ca_path,
            certfile=self.cert_path,
            keyfile=self.key_path,
            cert_reqs=ssl.CERT_REQUIRED,
            tls_version=ssl.PROTOCOL_TLSv1_2,
        )
        # Local broker uses a self-signed cert; CN won't match the LAN IP.
        client.tls_insecure_set(True)
        client.on_connect = on_connect
        client.on_subscribe = on_subscribe
        client.on_disconnect = on_disconnect
        client.on_message = on_message
        client.connect(self.cradle_ip, MQTT_PORT, keepalive=30)
        client.loop_start()
        self._mqtt = client

        try:
            await asyncio.wait_for(ready, timeout=10.0)
        except asyncio.TimeoutError as e:
            raise ConnectionError(f"local broker {self.cradle_ip}:{MQTT_PORT} subscribe timed out") from e

    def _publish(self, body: dict) -> None:
        if self._mqtt is None:
            raise RuntimeError("MQTT not connected")
        cmd = body.get("command", "?")
        logger.info("[%s] publish %s (%d bytes)", self.cradle_id, cmd, len(json.dumps(body)))
        if cmd == "sendResponse":
            sdp = body.get("sdp", {}).get("sdp", "")
            logger.debug("[%s] outbound answer SDP:\n%s", self.cradle_id, sdp)
        self._mqtt.publish(self._topic, json.dumps(body), qos=0)

    def _stream_info(self) -> dict:
        return {
            "applicationName": APPLICATION_NAME,
            "sessionId": self._session_id,
            "streamName": self.device_id,
        }

    # --- WebRTC negotiation --------------------------------------------------

    async def _negotiate_and_stream(self) -> None:
        from aiortc import RTCPeerConnection, RTCSessionDescription, RTCConfiguration
        from aiortc.contrib.media import MediaRelay

        # iceServers=[] is critical for timing. With default STUN servers,
        # aiortc's setLocalDescription blocks ~5 seconds waiting for STUN
        # gathering. The cradle's session timer is ~3 seconds — miss it and
        # the cradle drops us before our sendResponse arrives. With no STUN
        # servers, gathering is host-only and finishes in ~6 ms (we only
        # need a LAN-direct path anyway).
        pc = RTCPeerConnection(configuration=RTCConfiguration(iceServers=[]))
        self._pc = pc
        relay = MediaRelay()
        tracks: dict[str, Any] = {"video": None, "audio": None}

        @pc.on("track")
        def _on_track(track):
            logger.info("[%s] received track: %s", self.cradle_id, track.kind)
            tracks[track.kind] = relay.subscribe(track)

        @pc.on("connectionstatechange")
        async def _on_state():
            logger.info("[%s] pc state: %s", self.cradle_id, pc.connectionState)

        @pc.on("iceconnectionstatechange")
        async def _on_ice_state():
            logger.info("[%s] ice state: %s", self.cradle_id, pc.iceConnectionState)

        # 1. Trigger the cradle to send us an offer.
        self._publish({
            "command": "getOffer",
            "direction": "play",
            "streamInfo": self._stream_info(),
            "userData": {"param1": "value1"},
        })

        offer_sdp = await self._await_offer(timeout=10.0)

        # 2. Apply the offer + create answer. With iceServers=[] this is fast
        #    enough to beat the cradle's session timeout.
        await pc.setRemoteDescription(RTCSessionDescription(sdp=offer_sdp, type="offer"))
        aiortc_answer = await pc.createAnswer()
        await pc.setLocalDescription(aiortc_answer)

        # 3. Build a cradle-compatible wire answer by mirroring the offer
        #    with aiortc's DTLS material spliced in. aiortc's raw createAnswer
        #    output is silently rejected by the cradle's strict SDP parser
        #    (m=audio non-zero port instead of 0+bundle-only, setup:active vs
        #    expected passive, multi-fingerprint, lowercase 'opus', extra
        #    msid/rtcp/msid-semantic lines). See PROTOCOL.md §10.
        wire_answer = _build_mirrored_answer(offer_sdp, pc.localDescription.sdp)

        self._publish({
            "command": "sendResponse",
            "direction": "play",
            "streamInfo": self._stream_info(),
            "userData": {"param1": "value1"},
            "sdp": {"type": "answer", "sdp": wire_answer},
        })

        # 3. Pump remaining inbox messages: cradle's trickled ICE candidates,
        #    plus background shadow updates (which we ignore for signaling).
        ice_task = asyncio.create_task(self._handle_incoming(pc), name=f"cw-local-ice-{self.cradle_id[:8]}")

        self.stats.connected = True
        self.stats.last_frame_time = _mono()

        if self.on_connected:
            try:
                await self.on_connected()
            except Exception:
                logger.exception("[%s] on_connected raised", self.cradle_id)

        # 4. Wait for tracks to appear, then start receive loops.
        recv_tasks: list[asyncio.Task] = []
        # Tracks fire via @pc.on("track") which can run before or after we get
        # here; poll briefly.
        for _ in range(50):
            if tracks["video"] or tracks["audio"]:
                break
            await asyncio.sleep(0.1)
        if tracks["video"] and self.on_video_frame:
            recv_tasks.append(asyncio.create_task(self._video_loop(tracks["video"]), name=f"cw-local-video-{self.cradle_id[:8]}"))
        if tracks["audio"] and self.on_audio_frame:
            recv_tasks.append(asyncio.create_task(self._audio_loop(tracks["audio"]), name=f"cw-local-audio-{self.cradle_id[:8]}"))

        # 5. Stay alive until stop / disconnect / frame timeout.
        try:
            await self._maintain()
        finally:
            for t in [ice_task, *recv_tasks]:
                t.cancel()
            for t in [ice_task, *recv_tasks]:
                try:
                    await asyncio.wait_for(asyncio.shield(t), timeout=1.0)
                except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                    pass

    async def _await_offer(self, timeout: float) -> str:
        """Pull from the inbox until we see a sendOffer addressed to our session."""
        end = _mono() + timeout
        while _mono() < end:
            remaining = max(0.05, end - _mono())
            try:
                msg = await asyncio.wait_for(self._inbox.get(), timeout=remaining)
            except asyncio.TimeoutError:
                break
            if msg.get("command") != "sendOffer":
                continue
            si = msg.get("streamInfo") or {}
            if si.get("sessionId") != self._session_id:
                continue
            sdp = (msg.get("sdp") or {}).get("sdp")
            if not sdp:
                continue
            return sdp
        raise ConnectionError(f"no sendOffer from cradle {self.cradle_id} within {timeout}s")

    async def _handle_incoming(self, pc) -> None:
        """Forward cradle's trickled ICE candidates into the peer connection."""
        seen = 0
        while not self._stop:
            try:
                msg = await self._inbox.get()
            except asyncio.CancelledError:
                return
            ice = msg.get("ice")
            if not ice:
                continue
            candidate_str = ice.get("candidate", "")
            if not candidate_str:
                continue
            seen += 1
            logger.info("[%s] cradle ICE #%d: %s", self.cradle_id, seen, candidate_str)
            try:
                candidate = _parse_candidate_line(candidate_str)
                if candidate is None:
                    continue
                candidate.sdpMid = ice.get("sdpMid") or "0"
                candidate.sdpMLineIndex = ice.get("sdpMLineIndex", 0)
                await pc.addIceCandidate(candidate)
            except Exception:
                logger.debug("[%s] addIceCandidate failed", self.cradle_id, exc_info=True)

    async def _maintain(self) -> None:
        """Wait until stop / frame timeout / explicit disconnect."""
        while not self._stop and self.stats.connected:
            await asyncio.sleep(1.0)
            if _mono() - self.stats.last_frame_time > FRAME_TIMEOUT:
                logger.warning("[%s] no frames in %ds — assuming wedged", self.cradle_id, FRAME_TIMEOUT)
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
            self.stats.last_frame_time = _mono()
            if self.on_video_frame:
                try:
                    await self.on_video_frame(frame)
                except Exception:
                    logger.exception("[%s] on_video_frame raised", self.cradle_id)

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
                    logger.exception("[%s] on_audio_frame raised", self.cradle_id)

    async def _cleanup(self) -> None:
        if self._pc is not None:
            try:
                await self._pc.close()
            except Exception:
                pass
            self._pc = None
        if self._mqtt is not None:
            try:
                self._mqtt.loop_stop()
                self._mqtt.disconnect()
            except Exception:
                pass
            self._mqtt = None
        self.stats.connected = False


# --- helpers ---------------------------------------------------------------


def _now_ms_str() -> str:
    return str(int(time.time() * 1000))


def _mono() -> float:
    return time.monotonic()


def _build_mirrored_answer(offer_sdp: str, aiortc_answer_sdp: str) -> str:
    """Build a cradle-compatible answer by mirroring the offer + flipping it.

    Why: aiortc's createAnswer produces a syntactically valid WebRTC answer
    that the cradle's SDP parser silently rejects. The differences that matter:

    * ``m=audio`` port: offer uses ``0`` + ``a=bundle-only``; aiortc uses a real
      transport port. Cradle expects port 0 to confirm the bundle.
    * ``a=setup``: offer is ``actpass``; cradle accepts only ``passive``.
    * Multiple DTLS fingerprints: aiortc emits sha-256/384/512; cradle picks
      the *first* and may compare against a sha-256 hash, breaking validation
      when sha-512 is sent first.
    * ``a=msid``, ``a=rtcp``, ``a=msid-semantic``, ``a=ssrc`` — the cradle's
      WebRTC stack chokes on these even though they're valid per spec.
    * ``a=rtpmap:96 opus/...`` (lowercase) vs ``OPUS`` (uppercase) — payloads
      flagged as different codecs in some Wowza-derived stacks.

    Strategy: take the offer, surgically mutate it to be a valid answer, and
    splice in aiortc's actual ufrag/pwd/fingerprint so the DTLS handshake
    that follows actually completes. The result mirrors the offer almost
    line-for-line, which is exactly what the cradle's parser expects.
    """
    ufrag = _re_first(aiortc_answer_sdp, r"a=ice-ufrag:(\S+)")
    pwd = _re_first(aiortc_answer_sdp, r"a=ice-pwd:(\S+)")
    fp = _re_first(aiortc_answer_sdp, r"a=fingerprint:sha-256 (\S+)")
    if not (ufrag and pwd and fp):
        raise RuntimeError("aiortc answer missing ufrag/pwd/sha-256 fingerprint")

    answer = offer_sdp
    answer = re.sub(r"o=- \d+ \d+ IN IP4 0\.0\.0\.0", "o=- 1 1 IN IP4 0.0.0.0", answer)
    answer = re.sub(r"a=ice-ufrag:\S+", f"a=ice-ufrag:{ufrag}", answer)
    answer = re.sub(r"a=ice-pwd:\S+", f"a=ice-pwd:{pwd}", answer)
    answer = re.sub(r"a=fingerprint:sha-256 \S+", f"a=fingerprint:sha-256 {fp}", answer)
    # setup:active means *we* are the DTLS client (we send ClientHello).
    # aiortc's createAnswer also picks setup:active by default, so the wire
    # answer's role and aiortc's internal role agree.
    answer = answer.replace("a=setup:actpass", "a=setup:active")
    answer = answer.replace("a=sendonly", "a=recvonly")
    # Strip the offer's SSRC declarations — those describe the publisher.
    answer = re.sub(r"a=ssrc:\S+.*\r?\n", "", answer)
    return answer


def _re_first(s: str, pattern: str) -> str | None:
    m = re.search(pattern, s)
    return m.group(1) if m else None


_CAND_RE = re.compile(
    r"^(?:a=)?candidate:(?P<foundation>\S+)\s+"
    r"(?P<component>\d+)\s+"
    r"(?P<protocol>\S+)\s+"
    r"(?P<priority>\d+)\s+"
    r"(?P<ip>\S+)\s+"
    r"(?P<port>\d+)\s+"
    r"typ\s+(?P<type>\S+)"
    r"(?:\s+raddr\s+(?P<raddr>\S+)\s+rport\s+(?P<rport>\d+))?"
    r"(?:\s+tcptype\s+(?P<tcptype>\S+))?"
)


def _parse_candidate_line(line: str):
    """Parse an SDP candidate line into an aiortc RTCIceCandidate.

    aiortc's RTCIceCandidate constructor takes parsed components, not the raw
    SDP line — and the version on this machine doesn't expose a public
    fromSdp helper.
    """
    from aiortc import RTCIceCandidate

    m = _CAND_RE.match(line.strip())
    if not m:
        return None
    g = m.groupdict()
    return RTCIceCandidate(
        component=int(g["component"]),
        foundation=g["foundation"],
        ip=g["ip"],
        port=int(g["port"]),
        priority=int(g["priority"]),
        protocol=g["protocol"],
        type=g["type"],
        relatedAddress=g.get("raddr"),
        relatedPort=int(g["rport"]) if g.get("rport") else None,
        tcpType=g.get("tcptype"),
    )
