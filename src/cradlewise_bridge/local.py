"""Local-mode (LAN) WebRTC client for Cradlewise cribs.

When the subscriber is on the same WiFi as the crib, the official mobile app
bypasses the cloud Janus gateway entirely and exchanges WebRTC offers/answers
directly with the crib over its local Greengrass MQTT broker. This module
implements that path end-to-end: MQTT signaling, TCP-passive ICE, custom
RFC 4571-framed datagram transport, DTLS 1.2 handshake (forced, since cradle
rejects DTLS 1.0), SRTP key derivation, RFC 6184 H.264 RTP depacketization.

Why prefer the local path over cloud (Janus):
    * Lower latency (no internet round-trip; ICE selects a LAN-direct pair).
    * No dependency on ``/cradles/{id}/videoRoom`` Lambda, which gates on a
      server-side device-state field that's expensive to poke.
    * Lower attack surface: only your own crib's mTLS broker is touched; no
      shared cloud surface area.

Reuse the same auth artifacts as the rest of this library: the per-crib mTLS
cert, key, and group-CA that ``pycradlewise`` (or ``/cradles/pairedUsers/v3``)
downloads. ``pycradlewise.client.CradlewiseClient.refresh_device_certs``
returns these paths.

See ``PROTOCOL.md §10 — Local mode (LAN signaling)`` for the wire format.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import secrets
import ssl
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable

import paho.mqtt.client as mqtt
from aioice import stun
from aiortc.rtcdtlstransport import (
    RTCCertificate, RTCDtlsTransport, RTCDtlsParameters, RTCDtlsFingerprint,
)
from OpenSSL import SSL

logger = logging.getLogger(__name__)

MQTT_PORT = 8883
APPLICATION_NAME = "live"
DTLS1_2_VERSION = 0xFEFD  # OpenSSL DTLS 1.2 wire version
H264_START_CODE = b"\x00\x00\x00\x01"


# ---------------------------------------------------------------------------
# Custom RFC 4571 framed TCP transport — satisfies aiortc's RTCIceTransport
# duck-typed interface (._recv async returns bytes, ._send async sends bytes).
# Also handles bidirectional ICE STUN: cradle pings us with binding requests
# during and after the DTLS handshake, and silently drops the connection if
# we don't respond. aiortc has no STUN handling, so we intercept STUN packets
# here and only forward DTLS/SRTP up to aiortc.
# ---------------------------------------------------------------------------


class TCPDatagramTransport:
    """A datagram transport over TCP with RFC 4571 framing."""

    def __init__(self, host: str, port: int, *, our_pwd: str = ""):
        self.host = host
        self.port = port
        self.our_pwd = our_pwd
        self.role = "controlled"  # aiortc reads this only when DTLS role is auto
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.peer_addr: tuple[str, int] | None = None
        self._send_lock = asyncio.Lock()

    async def connect(self) -> None:
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        sock = self.writer.get_extra_info("socket")
        self.peer_addr = sock.getpeername()

    async def _recv(self) -> bytes:
        """Return the next DTLS/SRTP datagram. Auto-answers inbound STUN requests."""
        assert self.reader is not None
        while True:
            len_hdr = await self.reader.readexactly(2)
            length = int.from_bytes(len_hdr, "big")
            data = await self.reader.readexactly(length)
            first = data[0] if data else 0
            if first <= 3:
                # STUN: distinguish request vs response
                try:
                    msg = stun.parse_message(data)
                except Exception:
                    continue
                if msg.message_class == stun.Class.REQUEST:
                    await self._answer_stun_request(msg)
                    continue
                # Response/error — propagate; ICE check is waiting for it.
                return data
            return data

    async def _answer_stun_request(self, req: "stun.Message") -> None:
        """Send a STUN binding success response with XOR-MAPPED-ADDRESS."""
        resp = stun.Message(
            message_method=stun.Method.BINDING,
            message_class=stun.Class.RESPONSE,
            transaction_id=req.transaction_id,
        )
        resp.attributes["XOR-MAPPED-ADDRESS"] = self.peer_addr
        resp.add_message_integrity(self.our_pwd.encode("utf-8"))
        payload = bytes(resp)
        framed = len(payload).to_bytes(2, "big") + payload
        async with self._send_lock:
            self.writer.write(framed)
            await self.writer.drain()

    async def _send(self, data: bytes) -> None:
        assert self.writer is not None
        framed = len(data).to_bytes(2, "big") + data
        async with self._send_lock:
            self.writer.write(framed)
            await self.writer.drain()

    async def close(self) -> None:
        if self.writer is not None:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# H.264 RFC 6184 depacketization. Cradle sends single-NAL, FU-A, and STAP-A
# packets. Output is Annex-B (NAL units prefixed with 00 00 00 01 start code).
# ---------------------------------------------------------------------------


class H264Depacketizer:
    """Stateful RTP → Annex-B converter."""

    def __init__(self):
        self._fua_buffer: list[bytes] = []
        self._fua_nal_header: int | None = None

    def feed(self, rtp_packet: bytes) -> bytes:
        """Take an RTP packet (with header), return Annex-B bytes (may be empty)."""
        if not rtp_packet or (rtp_packet[0] & 0xC0) != 0x80:
            return b""
        cc = rtp_packet[0] & 0x0F
        offset = 12 + (cc * 4)
        if rtp_packet[0] & 0x10:  # extension
            if len(rtp_packet) >= offset + 4:
                ext_len = int.from_bytes(rtp_packet[offset+2:offset+4], "big")
                offset += 4 + ext_len * 4
        if offset >= len(rtp_packet):
            return b""
        payload = rtp_packet[offset:]
        if not payload:
            return b""
        nal_unit_type = payload[0] & 0x1F

        if 1 <= nal_unit_type <= 23:
            # Single NAL unit
            return H264_START_CODE + payload
        if nal_unit_type == 28:
            # FU-A fragment
            if len(payload) < 2:
                return b""
            fu_header = payload[1]
            start_bit = bool(fu_header & 0x80)
            end_bit = bool(fu_header & 0x40)
            original_nal_type = fu_header & 0x1F
            if start_bit:
                self._fua_buffer = []
                self._fua_nal_header = (payload[0] & 0xE0) | original_nal_type
                self._fua_buffer.append(bytes([self._fua_nal_header]))
            self._fua_buffer.append(payload[2:])
            if end_bit and self._fua_nal_header is not None:
                out = H264_START_CODE + b"".join(self._fua_buffer)
                self._fua_buffer = []
                self._fua_nal_header = None
                return out
            return b""
        if nal_unit_type == 24:
            # STAP-A: aggregated NAL units
            out = bytearray()
            i = 1
            while i + 2 <= len(payload):
                nal_size = int.from_bytes(payload[i:i+2], "big")
                i += 2
                if i + nal_size > len(payload):
                    break
                out += H264_START_CODE + payload[i:i+nal_size]
                i += nal_size
            return bytes(out)
        # 25-27 STAP-B/MTAP, 29 FU-B — rare; ignore
        return b""


# ---------------------------------------------------------------------------
# Local signaling helper (MQTT to /{cradleId}/room).
# ---------------------------------------------------------------------------


def _mirror_offer_as_answer(offer_sdp: str, ufrag: str, pwd: str, fp: str) -> str:
    """Build the cradle-compatible answer SDP by mirroring the offer.

    Cradle's SDP parser silently rejects aiortc's createAnswer output; mirror
    the offer line-for-line, flipping direction (sendonly→recvonly) and DTLS
    role (actpass→active), and splicing in our DTLS material. See PROTOCOL.md
    §10.4.
    """
    answer = offer_sdp
    answer = re.sub(r"o=- \d+ \d+ IN IP4 0\.0\.0\.0", "o=- 1 1 IN IP4 0.0.0.0", answer)
    answer = re.sub(r"a=ice-ufrag:\S+", f"a=ice-ufrag:{ufrag}", answer)
    answer = re.sub(r"a=ice-pwd:\S+", f"a=ice-pwd:{pwd}", answer)
    answer = re.sub(r"a=fingerprint:sha-256 \S+", f"a=fingerprint:sha-256 {fp}", answer)
    answer = answer.replace("a=setup:actpass", "a=setup:active")
    answer = answer.replace("a=sendonly", "a=recvonly")
    # Strip the offer's SSRC declarations — those describe the publisher.
    answer = re.sub(r"a=ssrc:\S+.*\r?\n", "", answer)
    return answer


def _re_first(s: str, pattern: str) -> str | None:
    m = re.search(pattern, s)
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


H264Callback = Callable[[bytes], Awaitable[None]]
ConnectedCallback = Callable[[], Awaitable[None]]


@dataclass
class LocalConnectionStats:
    connected: bool = False
    rtp_packets: int = 0
    annexb_bytes: int = 0
    last_packet_time: float = 0.0


@dataclass
class LocalVideoRoomClient:
    """Subscribe to a crib's H.264 stream over its local LAN MQTT signaling.

    Args:
        cradle_id: Cradle UUID (from the per-crib mTLS cert / pairedUsers).
        device_id: Our paired-device UUID for *this* crib. The local broker's
            IoT policy gates ``iot:Connect`` on ``client_id == device_id``.
        cradle_ip: Cradle's LAN IP. Discoverable from cradle shadow at
            ``state.reported.bluetooth.wifiStats.localIP`` or REST
            ``/cradles/{id}/onlineStatus/v2``.
        cert_path, key_path, ca_path: per-crib mTLS material.
        on_h264_data: Async callback; receives Annex-B H.264 byte chunks
            (one or more NAL units per call, each prefixed with start code).
            Buffer them and feed to PyAV / ffmpeg for decoding.
        on_connected: Optional async callback once DTLS+SRTP is up.

    Lifecycle:
        ``await client.run()`` blocks until the cradle closes the connection
        (currently happens after ~5–10 s of media — cradle's keepalive
        expectations are still under investigation; see PROTOCOL.md §10.9).
        Wrap in a reconnect loop for continuous capture.
    """
    cradle_id: str
    device_id: str
    cradle_ip: str
    cert_path: str
    key_path: str
    ca_path: str

    on_h264_data: H264Callback | None = None
    on_connected: ConnectedCallback | None = None

    stats: LocalConnectionStats = field(default_factory=LocalConnectionStats)

    _stop: bool = field(default=False, init=False)
    _mqtt_client: Any = field(default=None, init=False)
    _mqtt_inbox: asyncio.Queue | None = field(default=None, init=False)
    _tcp_transport: TCPDatagramTransport | None = field(default=None, init=False)
    _dtls: Any = field(default=None, init=False)

    def stop(self) -> None:
        self._stop = True

    async def run(self) -> None:
        """Connect, negotiate, stream H.264 until the cradle closes the session."""
        loop = asyncio.get_running_loop()
        self._mqtt_inbox = asyncio.Queue()
        try:
            await self._connect_mqtt(loop)
            offer_sdp, session_id = await self._do_signaling()
            await self._setup_media(offer_sdp, session_id)
        finally:
            await self._teardown()

    # --- MQTT signaling -----------------------------------------------------

    async def _connect_mqtt(self, loop: asyncio.AbstractEventLoop) -> None:
        ready = loop.create_future()

        c = mqtt.Client(
            client_id=self.device_id,  # MUST equal device_id (PROTOCOL.md §10.1)
            protocol=mqtt.MQTTv311,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            clean_session=True,
        )
        c.tls_set(
            ca_certs=self.ca_path, certfile=self.cert_path, keyfile=self.key_path,
            cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2,
        )
        c.tls_insecure_set(True)  # local broker uses self-signed; CN won't match LAN IP

        topic = f"/{self.cradle_id}/room"

        def on_connect(*_a, **_k):
            c.subscribe(topic, qos=0)

        def on_subscribe(*_a, **_k):
            if not ready.done():
                ready.set_result(True)

        def on_message(_c, _ud, msg):
            try:
                payload = json.loads(msg.payload)
            except Exception:
                return
            loop.call_soon_threadsafe(self._mqtt_inbox.put_nowait, payload)

        c.on_connect = on_connect
        c.on_subscribe = on_subscribe
        c.on_message = on_message
        c.connect(self.cradle_ip, MQTT_PORT, keepalive=30)
        c.loop_start()
        self._mqtt_client = c
        await asyncio.wait_for(ready, timeout=10)

    def _publish(self, body: dict) -> None:
        topic = f"/{self.cradle_id}/room"
        self._mqtt_client.publish(topic, json.dumps(body), qos=0)

    async def _do_signaling(self) -> tuple[str, str]:
        """Send getOffer; return (offer_sdp, session_id)."""
        sid = str(int(time.time() * 1000))
        stream_info = {
            "applicationName": APPLICATION_NAME,
            "sessionId": sid,
            "streamName": self.device_id,
        }
        self._publish({
            "command": "getOffer", "direction": "play",
            "streamInfo": stream_info, "userData": {"param1": "value1"},
        })

        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            timeout = deadline - time.monotonic()
            msg = await asyncio.wait_for(self._mqtt_inbox.get(), timeout=timeout)
            if (msg.get("command") == "sendOffer"
                    and msg.get("streamInfo", {}).get("sessionId") == sid):
                return msg["sdp"]["sdp"], sid
        raise ConnectionError(f"no sendOffer from cradle {self.cradle_id}")

    # --- Media setup --------------------------------------------------------

    async def _setup_media(self, offer_sdp: str, session_id: str) -> None:
        # Mint our own DTLS material via aiortc's RTCCertificate
        cert = RTCCertificate.generateCertificate()
        our_fp = next(
            f.value for f in cert.getFingerprints() if f.algorithm.lower() == "sha-256"
        )
        our_ufrag = secrets.token_urlsafe(3)[:4]
        our_pwd = secrets.token_urlsafe(18)[:22]

        # Build the wire answer and publish sendResponse
        answer_sdp = _mirror_offer_as_answer(offer_sdp, our_ufrag, our_pwd, our_fp)
        stream_info = {
            "applicationName": APPLICATION_NAME,
            "sessionId": session_id,
            "streamName": self.device_id,
        }
        self._publish({
            "command": "sendResponse", "direction": "play",
            "streamInfo": stream_info, "userData": {"param1": "value1"},
            "sdp": {"type": "answer", "sdp": answer_sdp},
        })

        # Wait for cradle's TCP-passive ICE candidate
        tcp_passive_port = await self._wait_tcp_passive(timeout=5)
        if tcp_passive_port is None:
            raise ConnectionError("cradle did not advertise a TCP-passive candidate")

        # Pull remote ICE creds + DTLS fingerprint from the offer
        remote_ufrag = _re_first(offer_sdp, r"a=ice-ufrag:(\S+)")
        remote_pwd = _re_first(offer_sdp, r"a=ice-pwd:(\S+)")
        remote_fp = _re_first(offer_sdp, r"a=fingerprint:sha-256 (\S+)")
        if not (remote_ufrag and remote_pwd and remote_fp):
            raise ConnectionError("offer SDP missing ICE/DTLS credentials")

        # Open TCP, do ICE STUN check (controlled side)
        tx = TCPDatagramTransport(self.cradle_ip, tcp_passive_port, our_pwd=our_pwd)
        self._tcp_transport = tx
        await tx.connect()
        await self._ice_check(tx, remote_ufrag, our_ufrag, remote_pwd)

        # Force DTLS 1.2 + broad cipher list — cradle rejects DTLS 1.0 with
        # handshake_failure(40) and aiortc's tight cipher list misses libwebrtc.
        orig_create_ctx = cert._create_ssl_context

        def patched_ctx(*args, **kwargs):
            ctx = orig_create_ctx(*args, **kwargs)
            ctx.set_min_proto_version(SSL.TLS1_2_VERSION)
            ctx.set_max_proto_version(SSL.TLS1_2_VERSION)
            ctx.set_cipher_list(b"DEFAULT:HIGH:!aNULL:!eNULL:!MD5:!RC4")
            return ctx

        cert._create_ssl_context = patched_ctx

        dtls = RTCDtlsTransport(transport=tx, certificates=[cert])
        dtls._role = "client"  # bypass auto-role determination
        self._dtls = dtls

        # Hook RTP capture: depacketize H.264 → Annex-B → user callback
        depack = H264Depacketizer()

        async def my_handle_rtp(data: bytes, arrival_time_ms: int) -> None:
            self.stats.rtp_packets += 1
            self.stats.last_packet_time = time.monotonic()
            annexb = depack.feed(data)
            if annexb:
                self.stats.annexb_bytes += len(annexb)
                if self.on_h264_data:
                    try:
                        await self.on_h264_data(annexb)
                    except Exception:
                        logger.exception("[%s] on_h264_data raised", self.cradle_id)

        dtls._handle_rtp_data = my_handle_rtp

        # Start DTLS handshake
        remote_params = RTCDtlsParameters(fingerprints=[
            RTCDtlsFingerprint(algorithm="sha-256", value=remote_fp)
        ])
        await dtls.start(remote_params)
        if dtls.state != "connected":
            raise ConnectionError(f"DTLS handshake failed (state={dtls.state})")

        self.stats.connected = True
        if self.on_connected:
            try:
                await self.on_connected()
            except Exception:
                logger.exception("[%s] on_connected raised", self.cradle_id)

        # aiortc's __run task is reading from transport; just block until
        # something signals stop or DTLS closes.
        while not self._stop and dtls.state == "connected":
            await asyncio.sleep(0.5)

    async def _wait_tcp_passive(self, *, timeout: float) -> int | None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                msg = await asyncio.wait_for(
                    self._mqtt_inbox.get(), timeout=deadline - time.monotonic()
                )
            except asyncio.TimeoutError:
                return None
            ice = msg.get("ice")
            if not ice:
                continue
            cand = ice.get("candidate", "")
            m = re.search(r"TCP \d+ \S+ (\d+) typ host tcptype passive", cand)
            if m:
                return int(m.group(1))
        return None

    async def _ice_check(self, transport: TCPDatagramTransport,
                         remote_ufrag: str, our_ufrag: str,
                         remote_pwd: str) -> None:
        msg = stun.Message(
            message_method=stun.Method.BINDING,
            message_class=stun.Class.REQUEST,
            transaction_id=secrets.token_bytes(12),
        )
        msg.attributes["USERNAME"] = f"{remote_ufrag}:{our_ufrag}"
        msg.attributes["PRIORITY"] = 1853824767
        msg.attributes["ICE-CONTROLLED"] = int.from_bytes(secrets.token_bytes(8), "big")
        msg.add_message_integrity(remote_pwd.encode("utf-8"))

        await transport._send(bytes(msg))

        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            data = await asyncio.wait_for(
                transport._recv(), timeout=deadline - time.monotonic()
            )
            if not data or data[0] > 3:
                # Non-STUN — bail (DTLS shouldn't arrive yet)
                raise ConnectionError("non-STUN data received during ICE check")
            try:
                resp = stun.parse_message(data)
            except Exception:
                continue
            if resp.message_class == stun.Class.ERROR:
                raise ConnectionError(f"ICE STUN error: {resp.attributes}")
            if resp.message_class == stun.Class.RESPONSE:
                return
        raise asyncio.TimeoutError("ICE STUN binding response not received")

    # --- Teardown -----------------------------------------------------------

    async def _teardown(self) -> None:
        self.stats.connected = False
        if self._dtls is not None:
            try:
                await self._dtls.stop()
            except Exception:
                pass
            self._dtls = None
        if self._tcp_transport is not None:
            await self._tcp_transport.close()
            self._tcp_transport = None
        if self._mqtt_client is not None:
            try:
                self._mqtt_client.loop_stop()
                self._mqtt_client.disconnect()
            except Exception:
                pass
            self._mqtt_client = None


# ---------------------------------------------------------------------------
# One-shot snapshot convenience function
# ---------------------------------------------------------------------------


async def capture_snapshot(
    *, cradle_id: str, device_id: str, cradle_ip: str,
    cert_path: str, key_path: str, ca_path: str,
    timeout: float = 15.0,
) -> bytes:
    """Connect, capture H.264 frames until ffmpeg can decode a JPEG, return JPEG bytes.

    The cradle drops the connection after ~5–10 s; in that window we
    typically capture 5+ keyframes. We collect Annex-B data, then once we
    have at least an SPS+PPS+IDR set, decode the first frame to JPEG via
    a PyAV pipeline.

    Returns:
        JPEG bytes ready to write to disk or attach to a notification.

    Raises:
        ConnectionError on signaling/DTLS failure.
        TimeoutError if no decodable frame within ``timeout``.
    """
    import av
    import io
    from PIL import Image

    annexb_chunks: list[bytes] = []

    async def on_h264(data: bytes) -> None:
        annexb_chunks.append(data)

    client = LocalVideoRoomClient(
        cradle_id=cradle_id, device_id=device_id, cradle_ip=cradle_ip,
        cert_path=cert_path, key_path=key_path, ca_path=ca_path,
        on_h264_data=on_h264,
    )

    # Run the client until the cradle drops us (~5–10s) or timeout. The
    # cradle interleaves keyframes ~every 1–2s, so by the natural drop we
    # have multiple decodable IDR sets. Stopping early on a partial GOP
    # leaves us with "non-existent PPS" decode errors.
    run_task = asyncio.create_task(client.run())
    try:
        await asyncio.wait_for(run_task, timeout=timeout)
    except asyncio.TimeoutError:
        client.stop()
        try:
            await asyncio.wait_for(run_task, timeout=2)
        except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
            pass
    except Exception:
        # natural disconnect — that's fine, fall through to decode
        pass

    if not annexb_chunks:
        raise TimeoutError(f"no H.264 data from cradle {cradle_id} in {timeout}s")

    annexb = b"".join(annexb_chunks)
    return _decode_first_jpeg(annexb)


def _decode_first_jpeg(annexb: bytes) -> bytes:
    """Decode an Annex-B H.264 stream and return the first frame as JPEG bytes.

    Uses an ``ffmpeg`` subprocess. PyAV is more brittle on partial streams
    captured mid-flight (insufficient lookahead, missing trailing NAL).
    """
    import subprocess

    proc = subprocess.run(
        [
            "ffmpeg", "-loglevel", "error",
            "-f", "h264", "-i", "pipe:0",
            "-frames:v", "1", "-update", "1",
            "-f", "image2", "-vcodec", "mjpeg", "-q:v", "3",
            "pipe:1",
        ],
        input=annexb, capture_output=True, timeout=20,
    )
    if proc.returncode != 0 or not proc.stdout:
        err = proc.stderr.decode("utf-8", errors="replace")[:500]
        raise ValueError(f"ffmpeg failed to decode H.264: {err}")
    return proc.stdout
