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
