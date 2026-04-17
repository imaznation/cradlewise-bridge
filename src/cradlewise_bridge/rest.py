"""REST client: activate a video room for a crib.

The Cradlewise cloud exposes an authenticated REST endpoint that, when called
for a specific cradle, spins up (or returns the existing) Janus video room
credentials needed to subscribe to the crib's feed:

    GET {api_base}/cradles/{cradle_id}/videoRoom?deviceId={device_id}

The response carries everything the Janus client needs to connect:
    {
      "lb_endpoint": "wss://video-room.cradlewise.com/...",
      "video_room_auth_secret": "...",   # HMAC secret for WS headers
      "room_id": 12345,
      "pin": "...",
      "opaque_id": "...",
      "wait_for_cradle_secs": 10
    }

The call is signed with AWS SigV4 using temporary credentials obtained via
pycradlewise (Cognito identity-pool exchange). We reuse pycradlewise's auth
object; this module only adds the SigV4 wrapper and the GET.
"""
from __future__ import annotations

import asyncio
import json
import logging
import urllib.request
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class VideoRoomCredentials:
    """All Janus parameters returned by the video room activation endpoint."""
    ws_url: str
    auth_secret: str
    room_id: int
    pin: str
    opaque_id: str
    wait_for_cradle_secs: int = 10

    @classmethod
    def from_response(cls, data: dict[str, Any]) -> "VideoRoomCredentials":
        return cls(
            ws_url=data["lb_endpoint"],
            auth_secret=data["video_room_auth_secret"],
            room_id=data["room_id"],
            pin=data["pin"],
            opaque_id=data["opaque_id"],
            wait_for_cradle_secs=int(data.get("wait_for_cradle_secs", 10)),
        )


async def fetch_video_room(
    auth: Any,
    app_config: Any,
    cradle_id: str,
    device_id: str,
    timeout: float = 15.0,
) -> VideoRoomCredentials:
    """Call the video room activation endpoint and return Janus credentials.

    Args:
        auth: A ``pycradlewise.CradlewiseAuth`` instance that has already run
            ``.authenticate()``. Must expose ``.credentials.aws`` (boto3-style
            credentials) and ``.ensure_valid()``.
        app_config: A ``pycradlewise.AppConfig`` with ``cognito_region`` and
            ``api_base_url``.
        cradle_id: The cradle's UUID (look up via ``CradlewiseClient.discover_cradles()``).
        device_id: The device identifier paired with this cradle.
        timeout: HTTP timeout in seconds.

    Returns:
        Parsed :class:`VideoRoomCredentials` ready to hand to the Janus client.

    Raises:
        ConnectionError: if the endpoint is unreachable or returns non-2xx.
        KeyError: if the response is missing required fields.
    """
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest

    await auth.ensure_valid()

    url = (
        f"{app_config.api_base_url}/cradles/{cradle_id}/videoRoom"
        f"?deviceId={device_id}"
    )
    aws_request = AWSRequest(method="GET", url=url)
    SigV4Auth(auth.credentials.aws, "execute-api", app_config.cognito_region).add_auth(aws_request)

    req = urllib.request.Request(url, method="GET")
    for k, v in dict(aws_request.headers).items():
        req.add_header(k, v)

    def _do_request() -> dict[str, Any]:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())

    try:
        data = await asyncio.to_thread(_do_request)
    except Exception as e:
        raise ConnectionError(f"Failed to activate video room for cradle {cradle_id}: {e}") from e

    return VideoRoomCredentials.from_response(data)
