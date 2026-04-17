"""Listen to a crib's audio and save WAV snippets on each detected spike.

Useful as a "fussing detector". The adaptive threshold calibrates itself to
the room's ambient noise over ~30 seconds, so leave it running for at least
a minute before expecting meaningful spikes.

Run:
    python examples/detect_audio_spikes.py
    (set CRADLEWISE_EMAIL and CRADLEWISE_PASSWORD)

    WAVs are written to ./audio_snippets/<date>/<crib>-<time>-rms<value>.wav
"""
from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

from cradlewise_bridge.audio import AudioSpikeDetector
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
    cradle_id, cradle = next(iter(cradles.items()))
    crib_name = cradle.baby_name or cradle_id[:8]

    creds = await fetch_video_room(auth, app_config, cradle_id, cradle.device_id)

    detector = AudioSpikeDetector(
        name=crib_name,
        snippet_dir=Path("./audio_snippets") / crib_name,
    )

    async def on_audio(frame):
        detector.add_audio_frame(frame)

    client = JanusVideoRoomClient(
        cradle_id=cradle_id,
        device_id=cradle.device_id,
        credentials=creds,
        on_audio_frame=on_audio,
    )

    async def tick_loop():
        while True:
            wav = detector.check_and_save_spike()
            if wav:
                print(f"Spike! Saved: {wav}")
            await asyncio.sleep(1.0)

    tick_task = asyncio.create_task(tick_loop())
    try:
        await client.run()
    finally:
        tick_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
