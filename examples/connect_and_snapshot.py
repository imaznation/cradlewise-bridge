"""Connect to a crib, grab a few frames, save one as JPG, disconnect.

This is the smallest end-to-end test for the Janus WebRTC flow. If this
works, you're authenticated and the signing scheme is valid.

Run:
    python examples/connect_and_snapshot.py
    (set CRADLEWISE_EMAIL and CRADLEWISE_PASSWORD)
"""
from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

from cradlewise_bridge.janus import JanusVideoRoomClient
from cradlewise_bridge.rest import fetch_video_room

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


async def main() -> None:
    from pycradlewise import CradlewiseAuth, CradlewiseClient, get_app_config

    email = os.environ["CRADLEWISE_EMAIL"]
    password = os.environ["CRADLEWISE_PASSWORD"]

    app_config = await get_app_config()
    auth = CradlewiseAuth(email=email, password=password, app_config=app_config)
    await auth.authenticate()

    cc = CradlewiseClient(auth)
    cradles = await cc.discover_cradles()
    if not cradles:
        raise SystemExit("No cradles on this account")

    # Pick the first active cradle.
    cradle_id, cradle = next(iter(cradles.items()))
    print(f"Connecting to cradle {cradle.baby_name or cradle_id[:8]}")

    creds = await fetch_video_room(auth, app_config, cradle_id, cradle.device_id)

    frame_count = 0
    snapshot_path = Path("snapshot.jpg")

    async def on_video(frame):
        nonlocal frame_count
        frame_count += 1
        if frame_count == 10:
            img = frame.to_image()
            img.save(snapshot_path, quality=85)
            print(f"Saved {snapshot_path} ({img.size[0]}x{img.size[1]})")
            client.stop()

    client = JanusVideoRoomClient(
        cradle_id=cradle_id,
        device_id=cradle.device_id,
        credentials=creds,
        on_video_frame=on_video,
    )
    await client.run()
    print(f"Received {frame_count} frames total.")


if __name__ == "__main__":
    asyncio.run(main())
