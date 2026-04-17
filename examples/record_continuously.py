"""Record every crib on the account continuously, with auto-reconnect.

This is the full bridge in action. Frames are decimated to 2 fps, flushed to
5-minute MP4 segments under ./cradlewise_data/recordings/<crib>/<date>/,
and purged after 48 hours. Audio spikes write WAVs to a parallel directory.

Run:
    python examples/record_continuously.py
    (set CRADLEWISE_EMAIL and CRADLEWISE_PASSWORD)
    Ctrl+C to stop.
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
from pathlib import Path

from cradlewise_bridge import CradlewiseBridge, CribConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")


async def main() -> None:
    from pycradlewise import CradlewiseAuth, CradlewiseClient, get_app_config

    email = os.environ["CRADLEWISE_EMAIL"]
    password = os.environ["CRADLEWISE_PASSWORD"]

    # One-time: discover cradles. The bridge itself re-authenticates as needed,
    # but it needs the (cradle_id, device_id) pairs up front.
    app_config = await get_app_config()
    auth = CradlewiseAuth(email=email, password=password, app_config=app_config)
    await auth.authenticate()
    cradles = await CradlewiseClient(auth).discover_cradles()

    cribs = [
        CribConfig(
            name=(c.baby_name or cid[:8]).replace(" ", "_").lower(),
            cradle_id=cid,
            device_id=c.device_id,
        )
        for cid, c in cradles.items()
    ]
    if not cribs:
        raise SystemExit("No cradles found on this account")

    print(f"Starting bridge for {len(cribs)} crib(s): {[c.name for c in cribs]}")

    async def on_spike(crib_name, wav_path):
        print(f"[{crib_name}] audio spike: {wav_path}")

    async def on_segment(crib_name, mp4_path):
        print(f"[{crib_name}] segment flushed: {mp4_path}")

    bridge = CradlewiseBridge(
        email=email,
        password=password,
        cribs=cribs,
        output_root=Path("./cradlewise_data"),
        on_audio_spike=on_spike,
        on_segment_flushed=on_segment,
    )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, bridge.stop)

    await bridge.run()


if __name__ == "__main__":
    asyncio.run(main())
