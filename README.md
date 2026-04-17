# cradlewise-bridge

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Buy Me a Coffee](https://img.shields.io/badge/Buy%20Me%20a%20Coffee-support-yellow?logo=buy-me-a-coffee)](https://buymeacoffee.com/imaznation)

An unofficial interoperability library for [Cradlewise](https://cradlewise.com/)
smart cribs. Uses your own Cradlewise credentials to subscribe to the live
video and audio feed your crib already publishes to Cradlewise's cloud, so
you can integrate it into your own monitoring, home automation, or analytics
pipeline.

**Status:** works, alpha, single maintainer. Not affiliated with Cradlewise Inc.

## What this does

* **REST state polling.** Read the full device shadow (temperature, baby
  presence, sleep state, bouncing, music, light, etc.) for each crib on
  your account. Builds on [`pycradlewise`](https://github.com/jlamendo/pycradlewise)
  for authentication.
* **Live WebRTC video + audio.** Connect to Cradlewise's Janus gateway,
  authenticate with HMAC-signed WebSocket headers, and subscribe to your
  crib's H.264 video + Opus audio streams via [`aiortc`](https://github.com/aiortc/aiortc).
* **Continuous recording.** Decimate video to a configurable FPS (default 2),
  write 5-minute MP4 segments, index them in SQLite, and purge after 48h
  (configurable).
* **Adaptive audio spike detection.** Exponential-moving-average ambient
  baseline + multiplier threshold + deferred post-spike capture — saves
  10-second WAV snippets centered on crying/fussing/loud events.
* **Multi-crib.** Connect to every crib on your account concurrently with
  independent reconnect loops.
* **Read-only by design.** No control commands ever leave this library.
  Bouncing, music, lighting, 2-way audio — all out of scope.

## Install

```bash
pip install -e .
# or
pip install -r requirements.txt
```

Requires Python 3.10+. Depends on `pycradlewise`, `aiortc`, `aiohttp`,
`boto3`, `numpy`, `Pillow`, `imageio[ffmpeg]`.

## Quickstart

```python
import asyncio, os
from cradlewise_bridge import CradlewiseBridge, CribConfig
from pycradlewise import CradlewiseAuth, CradlewiseClient, get_app_config

async def main():
    email, password = os.environ["CRADLEWISE_EMAIL"], os.environ["CRADLEWISE_PASSWORD"]

    # One-time discovery to map cradle_id -> device_id
    app = await get_app_config()
    auth = CradlewiseAuth(email, password, app_config=app)
    await auth.authenticate()
    cradles = await CradlewiseClient(auth).discover_cradles()

    bridge = CradlewiseBridge(
        email=email,
        password=password,
        cribs=[
            CribConfig(name=(c.baby_name or cid[:8]).lower(),
                       cradle_id=cid, device_id=c.device_id)
            for cid, c in cradles.items()
        ],
    )

    async def on_spike(crib, wav):  print(f"[{crib}] cry: {wav}")
    async def on_segment(crib, mp4): print(f"[{crib}] saved: {mp4}")
    bridge.on_audio_spike = on_spike
    bridge.on_segment_flushed = on_segment

    await bridge.run()

asyncio.run(main())
```

See `examples/` for four runnable scripts:

| Script                             | What it does                                   |
|------------------------------------|------------------------------------------------|
| `state_polling.py`                 | Print the device-shadow state (no video)       |
| `connect_and_snapshot.py`          | Connect, grab 10 frames, save one as JPG       |
| `detect_audio_spikes.py`           | Save WAVs whenever crying / loud sounds occur  |
| `record_continuously.py`           | Full 48h rolling recording with auto-reconnect |

## Design notes

* The only piece that required real reverse engineering is the Janus
  gateway layer — WebSocket HMAC signing, videoroom handshake, trickle
  ICE injection into the SDP. It's documented in full in
  [`PROTOCOL.md`](./PROTOCOL.md).
* Everything above that layer (segment recording, adaptive cry detection,
  multi-crib supervision) is standard engineering with no Cradlewise-specific
  surprises. You can freely ignore those modules and just use
  `janus.py` + `rest.py` if you have your own media pipeline.
* `pycradlewise` does all the Cognito / SigV4 auth work; we don't reimplement
  it. Credit where due.

## How is this different from pycradlewise?

`pycradlewise` is a REST-and-MQTT client — it reads the device shadow and
exposes typed properties. It does **not** give you access to the video or
audio stream. `cradlewise-bridge` adds:

* The Janus video-room activation call (AWS SigV4 REST endpoint).
* The Janus WebSocket client with HMAC-signed upgrade headers.
* The videoroom subscribe flow and trickle ICE handling.
* WebRTC peer connection via aiortc to actually receive the media.
* The recording and audio-analysis pipeline on top.

It depends on `pycradlewise` for authentication and shadow access.

## Legal & ethical framing

This project exists so owners of Cradlewise cribs can monitor their own
devices using their own credentials. It is not affiliated with, endorsed by,
or supported by Cradlewise Inc.

**What this code does and doesn't do**

* **Uses your own account.** The library authenticates as you, using
  credentials you provide. It does not bypass authentication, impersonate
  other users, or access anyone else's cribs.
* **Read-only by design.** It subscribes to the video/audio stream and
  reads REST state. It never sends control commands (bouncing, music,
  lights, 2-way audio). The documented API exists — this project just
  consumes it from a non-mobile client.
* **No credentials, tokens, or device identifiers are included.** The
  signing scheme and protocol framing are documented as algorithms; the
  runtime config (API base, region, room secrets) is fetched from
  Cradlewise's own endpoints using your credentials, exactly as the
  official mobile app does.

**Legal basis (U.S., not legal advice)**

* Reverse engineering a product you own for *interoperability* is
  expressly permitted under 17 U.S.C. §1201(f) (DMCA interoperability
  exception) and has been broadly upheld for personal and research use
  (*Sega v. Accolade*, *Sony v. Connectix*).
* The Computer Fraud and Abuse Act (CFAA) targets *unauthorized* access.
  Authenticating to an account you own and accessing your own devices is
  authorized access by definition.
* Publishing protocol documentation derived from clean-room observation
  of your own authenticated sessions is protected speech; no trade
  secrets are misappropriated (public network traffic from a product
  you own is not a trade secret).

**Terms of Service caveat**

Cradlewise's Terms of Service may prohibit use of unofficial clients.
Using this library could result in account termination at Cradlewise's
discretion. That's a risk you accept by using it. If you don't want that
risk, don't use this library.

**Trademarks**

"Cradlewise" is a trademark of Cradlewise Inc. It is used here
nominatively — only to identify the product this library interoperates
with — and implies no endorsement.

**No warranty, not for safety-critical use**

This is an independent personal-use tool provided as-is under the MIT
license. **Do not rely on it for safety-critical monitoring of an infant.**
Use the official Cradlewise app and follow their safety guidance. The
authors assume no responsibility for any harm, financial loss, or account
termination arising from use of this software.

## Contributing

Issues and PRs welcome. Scope I'll accept:

* Bug fixes in the Janus / WebRTC flow.
* Better reconnect logic and error taxonomy.
* Additional examples (HomeAssistant bridge, Frigate sidecar, etc.).
* Protocol documentation improvements.

Scope I'll decline:

* Anything that sends control commands to a crib.
* Anything that ships credentials, tokens, or per-account identifiers.
* Anything that makes the library usable as a mass-scraping tool.

## Credits

* [`pycradlewise`](https://github.com/jlamendo/pycradlewise) by Jon Lamendola —
  AWS Cognito SRP auth, AWS IoT MQTT, device shadow access.
* [`aiortc`](https://github.com/aiortc/aiortc) by Jeremy Lainé — the WebRTC
  stack that makes Python WebRTC actually work.
* [Janus Gateway](https://janus.conf.meetecho.com/) — the server side of the
  media pipeline (not affiliated with this project, just used by Cradlewise).

## Support

If this project helped you get more out of your Cradlewise, consider buying me a coffee!

[![Buy Me a Coffee](https://img.shields.io/badge/Buy%20Me%20a%20Coffee-support-yellow?logo=buy-me-a-coffee&style=for-the-badge)](https://buymeacoffee.com/imaznation)

## License

MIT. See [`LICENSE`](./LICENSE).
