"""Poll Cradlewise REST state without touching video.

This is the simplest possible example: it demonstrates pycradlewise auth and
the device-shadow fields we exposed in src/cradlewise_bridge/__init__.py's
docstring. No Janus, no WebRTC — just the plain REST state.

Run:
    python examples/state_polling.py
    (set CRADLEWISE_EMAIL and CRADLEWISE_PASSWORD env vars, or edit below)
"""
from __future__ import annotations

import asyncio
import os


async def main() -> None:
    from pycradlewise import CradlewiseAuth, CradlewiseClient, get_app_config

    email = os.environ["CRADLEWISE_EMAIL"]
    password = os.environ["CRADLEWISE_PASSWORD"]

    app_config = await get_app_config()
    auth = CradlewiseAuth(email=email, password=password, app_config=app_config)
    await auth.authenticate()

    client = CradlewiseClient(auth)
    cradles = await client.discover_cradles()

    for cradle_id, cradle in cradles.items():
        state = await client.get_cradle_state(cradle_id)
        if not state:
            print(f"{cradle_id[:8]}: no state")
            continue

        temp_c = state.get("ambientTempInCelsius")
        temp_f = round(temp_c * 9 / 5 + 32, 1) if temp_c and temp_c > 0 else None
        actuator = state.get("actuator", {})

        print(f"\n=== {cradle.baby_name or cradle_id[:8]} ({cradle_id[:8]}) ===")
        print(f"  uptime:            {int(state.get('uptimeTotal', 0))}s")
        print(f"  baby_present:      {state.get('baby_present')}")
        print(f"  sleep_state:       {state.get('baby_sleep_state')}")
        print(f"  temp:              {temp_f}F")
        print(f"  bouncing:          {actuator.get('on')}")
        print(f"  bounce_amplitude:  {actuator.get('amplitude')}")
        print(f"  music_playing:     {state.get('music', {}).get('play')}")


if __name__ == "__main__":
    asyncio.run(main())
