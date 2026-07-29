"""Tests for the publisher-wake hook in capture_stream.

A cradle that has gone idle tears down its local video room and refuses LAN
connections until the cloud asks it to republish. capture_stream detects that
specific failure and gives the caller a chance to trigger the wake.
"""
import asyncio
import errno

import pytest

from cradlewise_bridge import local
from cradlewise_bridge.local import _is_publisher_absent, capture_stream


# --- failure classification -------------------------------------------------

def test_direct_connection_refused():
    assert _is_publisher_absent(ConnectionRefusedError(errno.ECONNREFUSED, "refused"))


def test_oserror_with_econnrefused_errno():
    exc = OSError()
    exc.errno = errno.ECONNREFUSED
    assert _is_publisher_absent(exc)


def test_wrapped_in_cause():
    try:
        try:
            raise ConnectionRefusedError(errno.ECONNREFUSED, "refused")
        except ConnectionRefusedError as inner:
            raise RuntimeError("signaling failed") from inner
    except RuntimeError as outer:
        assert _is_publisher_absent(outer)


def test_wrapped_in_context():
    try:
        try:
            raise ConnectionRefusedError(errno.ECONNREFUSED, "refused")
        except ConnectionRefusedError:
            raise RuntimeError("signaling failed")
    except RuntimeError as outer:
        assert _is_publisher_absent(outer)


@pytest.mark.parametrize("exc", [
    TimeoutError("timed out"),
    ConnectionResetError(errno.ECONNRESET, "reset"),
    OSError(errno.EHOSTUNREACH, "no route"),
    ValueError("nonsense"),
])
def test_other_failures_are_not_publisher_absence(exc):
    """Transient faults must not burn the throttled cloud wake."""
    assert not _is_publisher_absent(exc)


def test_cyclic_chain_terminates():
    a = RuntimeError("a")
    b = RuntimeError("b")
    a.__cause__ = b
    b.__cause__ = a
    assert _is_publisher_absent(a) is False


# --- capture_stream integration --------------------------------------------

class _FakeClient:
    """Stands in for LocalVideoRoomClient; run() fails the way we ask."""

    def __init__(self, exc, **kwargs):
        self._exc = exc

    async def run(self):
        raise self._exc

    def stop(self):
        pass


async def _drive(monkeypatch, exc, hook_calls, max_calls=1):
    """Run capture_stream against a client that always fails with `exc`,
    cancelling once the hook has fired `max_calls` times."""
    done = asyncio.Event()

    monkeypatch.setattr(
        local, "LocalVideoRoomClient",
        lambda **kwargs: _FakeClient(exc, **kwargs),
    )

    async def hook():
        hook_calls.append(1)
        if len(hook_calls) >= max_calls:
            done.set()

    task = asyncio.create_task(capture_stream(
        cradle_id="test-cradle", device_id="dev", cradle_ip="127.0.0.1",
        cert_path="/nonexistent", key_path="/nonexistent", ca_path="/nonexistent",
        on_frame=lambda img, ts: asyncio.sleep(0),
        initial_backoff=0.01, max_backoff=0.01,
        on_publisher_absent=hook,
    ))
    try:
        await asyncio.wait_for(done.wait(), timeout=5)
    except asyncio.TimeoutError:
        pass  # hook never fired; the assertion in the caller reports it
    finally:
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass


@pytest.mark.asyncio
async def test_hook_fires_on_connection_refused(monkeypatch):
    calls = []
    await _drive(monkeypatch, ConnectionRefusedError(errno.ECONNREFUSED, "refused"), calls)
    assert calls, "publisher-wake hook was not invoked on ECONNREFUSED"


@pytest.mark.asyncio
async def test_hook_not_fired_on_transient_failure(monkeypatch):
    calls = []
    await _drive(monkeypatch, TimeoutError("timed out"), calls)
    assert not calls, "wake hook fired for a transient fault"


@pytest.mark.asyncio
async def test_hook_failure_does_not_kill_stream_loop(monkeypatch):
    """A raising hook must be swallowed — the reconnect loop keeps running."""
    attempts = []

    monkeypatch.setattr(
        local, "LocalVideoRoomClient",
        lambda **kwargs: _FakeClient(ConnectionRefusedError(errno.ECONNREFUSED, "refused")),
    )

    async def bad_hook():
        attempts.append(1)
        raise RuntimeError("cloud is down")

    task = asyncio.create_task(capture_stream(
        cradle_id="test-cradle", device_id="dev", cradle_ip="127.0.0.1",
        cert_path="/nonexistent", key_path="/nonexistent", ca_path="/nonexistent",
        on_frame=lambda img, ts: asyncio.sleep(0),
        initial_backoff=0.01, max_backoff=0.01,
        on_publisher_absent=bad_hook,
    ))
    await asyncio.sleep(0.5)
    still_running = not task.done()
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass

    assert len(attempts) >= 2, "loop stopped retrying after the hook raised"
    assert still_running, "a raising wake hook killed the stream loop"


# --- audio (Opus) delivery --------------------------------------------------

class _RtpCapturingClient:
    """Captures the on_rtp_packet callback capture_stream installs, so tests
    can feed packets without a live cradle."""

    captured = {}

    def __init__(self, **kwargs):
        _RtpCapturingClient.captured["on_rtp"] = kwargs.get("on_rtp_packet")

    async def run(self):
        await asyncio.sleep(3600)

    def stop(self):
        pass


async def _with_capture_stream(monkeypatch, on_audio):
    monkeypatch.setattr(local, "LocalVideoRoomClient", _RtpCapturingClient)
    task = asyncio.create_task(local.capture_stream(
        cradle_id="c", device_id="d", cradle_ip="127.0.0.1",
        cert_path="/x", key_path="/x", ca_path="/x",
        on_frame=lambda img, ts: asyncio.sleep(0),
        on_audio=on_audio,
    ))
    for _ in range(50):
        await asyncio.sleep(0.01)
        if _RtpCapturingClient.captured.get("on_rtp"):
            break
    return task, _RtpCapturingClient.captured.get("on_rtp")


def _rtp(payload_type, payload=b"\x00" * 40, seq=1):
    """Minimal 12-byte RTP header + payload."""
    return (bytes([0x80, payload_type & 0x7F])
            + seq.to_bytes(2, "big")
            + (0).to_bytes(4, "big")      # timestamp
            + (12345).to_bytes(4, "big")  # ssrc
            + payload)


@pytest.mark.asyncio
async def test_video_payload_does_not_reach_audio_callback(monkeypatch):
    got = []
    task, on_rtp = await _with_capture_stream(monkeypatch, lambda f, ts: got.append(f))
    assert on_rtp is not None, "capture_stream did not install on_rtp_packet"
    await on_rtp(local.VIDEO_PAYLOAD_TYPE, _rtp(local.VIDEO_PAYLOAD_TYPE))
    assert not got, "video RTP was routed to the audio decoder"
    task.cancel()
    with pytest.raises((asyncio.CancelledError, Exception)):
        await task


@pytest.mark.asyncio
async def test_malformed_audio_packet_is_swallowed(monkeypatch):
    """A corrupt packet must not kill the stream."""
    got = []
    task, on_rtp = await _with_capture_stream(monkeypatch, lambda f, ts: got.append(f))
    await on_rtp(local.AUDIO_PAYLOAD_TYPE, b"\x01\x02")        # too short to parse
    await on_rtp(local.AUDIO_PAYLOAD_TYPE, _rtp(local.AUDIO_PAYLOAD_TYPE, b""))  # empty
    await on_rtp(local.AUDIO_PAYLOAD_TYPE, _rtp(local.AUDIO_PAYLOAD_TYPE, b"\xff" * 8))  # junk
    assert not got
    task.cancel()
    with pytest.raises((asyncio.CancelledError, Exception)):
        await task


@pytest.mark.asyncio
async def test_no_rtp_hook_installed_without_on_audio(monkeypatch):
    """Callers that don't want audio pay no per-packet cost."""
    _RtpCapturingClient.captured.clear()
    monkeypatch.setattr(local, "LocalVideoRoomClient", _RtpCapturingClient)
    task = asyncio.create_task(local.capture_stream(
        cradle_id="c", device_id="d", cradle_ip="127.0.0.1",
        cert_path="/x", key_path="/x", ca_path="/x",
        on_frame=lambda img, ts: asyncio.sleep(0),
    ))
    await asyncio.sleep(0.2)
    assert _RtpCapturingClient.captured.get("on_rtp") is None
    task.cancel()
    with pytest.raises((asyncio.CancelledError, Exception)):
        await task


@pytest.mark.asyncio
async def test_absent_hook_is_optional(monkeypatch):
    """Omitting the hook must behave exactly as before."""
    monkeypatch.setattr(
        local, "LocalVideoRoomClient",
        lambda **kwargs: _FakeClient(ConnectionRefusedError(errno.ECONNREFUSED, "refused")),
    )
    task = asyncio.create_task(capture_stream(
        cradle_id="test-cradle", device_id="dev", cradle_ip="127.0.0.1",
        cert_path="/nonexistent", key_path="/nonexistent", ca_path="/nonexistent",
        on_frame=lambda img, ts: asyncio.sleep(0),
        initial_backoff=0.01, max_backoff=0.01,
    ))
    await asyncio.sleep(0.3)
    still_running = not task.done()
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass
    assert still_running
