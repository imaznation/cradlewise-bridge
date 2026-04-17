# Cradlewise video-feed protocol

This document describes the wire-level protocol cradlewise-bridge uses to
subscribe to a crib's live video and audio stream. It's derived from observing
authenticated sessions from a user's own account — there are no credentials,
tokens, or device identifiers here, only algorithms and framing.

The goal is twofold: to make the client implementation in this repo easy to
understand, and to make it easier for other interoperability projects to
reach the same feed without repeating the discovery work.

> **Scope.** Only the pieces needed to get a subscriber-side video stream
> going are documented here. Crib *control* (bouncing, music, 2-way audio,
> light) is deliberately out of scope — this library is read-only by design.

## 1. Layered architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ Layer 4:  WebRTC peer connection (H.264 video + Opus audio)     │
├─────────────────────────────────────────────────────────────────┤
│ Layer 3:  Janus videoroom plugin messages over JSON             │
├─────────────────────────────────────────────────────────────────┤
│ Layer 2:  Janus-protocol WebSocket, HMAC-signed upgrade headers │
├─────────────────────────────────────────────────────────────────┤
│ Layer 1:  REST video-room activation (AWS SigV4)                │
├─────────────────────────────────────────────────────────────────┤
│ Layer 0:  AWS Cognito SRP auth (handled by pycradlewise)        │
└─────────────────────────────────────────────────────────────────┘
```

Each layer produces the inputs the next layer needs. In code, these are
`auth.py` (layer 0 — we just use `pycradlewise`), `rest.py` (layer 1),
`janus.py` (layers 2–4).

## 2. Layer 0 — Cognito auth

`pycradlewise` handles this: Cognito SRP login with your account email
and password produces a set of AWS temporary credentials you can use to
sign SigV4 requests to the Cradlewise API. No work is needed on top of
what that library already does.

After `await auth.authenticate()`, you have:

* `auth.credentials.aws` — `botocore`-style `AccessKey`, `SecretKey`,
  `SessionToken` — usable directly with `SigV4Auth`.
* `app_config.cognito_region` — the AWS region (e.g. `us-west-2`).
* `app_config.api_base_url` — the REST endpoint base (an API Gateway host).

Tokens need refreshing every ~1 hour. `auth.ensure_valid()` does this.

## 3. Layer 1 — REST video-room activation

Before you can subscribe to a crib's feed, you need Janus connection
parameters for that specific crib. The Cradlewise API spins up (or returns
the existing) Janus room when you hit:

```
GET {api_base}/cradles/{cradle_id}/videoRoom?deviceId={device_id}
```

Signed with AWS SigV4 (service `execute-api`). Response:

```json
{
  "lb_endpoint":            "wss://video-room.cradlewise.com/...",
  "video_room_auth_secret": "<per-session HMAC key>",
  "room_id":                123456,
  "pin":                    "<room pin>",
  "opaque_id":              "<client-chosen logical identifier>",
  "wait_for_cradle_secs":   10
}
```

The `video_room_auth_secret` is the HMAC key used to sign the WebSocket
upgrade headers at layer 2. It is scoped to this video room — don't reuse
it across cradles or across activation calls.

`wait_for_cradle_secs` hints at how long the server will hold the room open
waiting for the cradle itself to join as publisher, if it isn't already.

## 4. Layer 2 — Janus WebSocket with HMAC-signed headers

Connect to `lb_endpoint` as a WebSocket with subprotocol
`janus-protocol` and these headers on the HTTP upgrade:

| Header          | Value                                                   |
|-----------------|---------------------------------------------------------|
| `X-Origin`      | `20000` (client version identifier)                     |
| `X-CId`         | the cradle id                                           |
| `X-DId`         | the device id                                           |
| `X-Timestamp`   | UTC timestamp, format `YYYYMMDDHHMMSS{microseconds:06d}Z` |
| `X-SId`         | a fresh UUIDv4 per connection                           |
| `X-Signed-Keys` | the literal string `X-Origin,X-CId,X-DId,X-Timestamp,X-SId` |
| `Authorization` | `HMAC <hex signature>` (see below)                      |

### Signing algorithm

Given the five signed headers and the `video_room_auth_secret` from layer 1:

1. Build the canonical string: join, with `\n`, lines of the form
   `"{header_name.lower()}:{header_value}"` in the order listed in
   `X-Signed-Keys`. All five headers are always signed.

   ```
   x-origin:20000
   x-cid:<cradle_id>
   x-did:<device_id>
   x-timestamp:<timestamp>
   x-sid:<uuid>
   ```

2. Inner hash: `SHA-256` over the UTF-8 bytes of the canonical string, as
   a **lowercase hex string** (not raw bytes).

3. Signature: `HMAC-SHA256(secret, inner_hex)`, again rendered as a
   **lowercase hex string**.

4. `Authorization: HMAC <signature>`.

See `sign_ws_headers()` in `src/cradlewise_bridge/janus.py` for the
reference implementation — it's about 25 lines.

### Why this is the tricky part

Two details are easy to get wrong and fail with an opaque 403:

* The HMAC input is the **hex digest** of the inner SHA-256, not the raw
  32-byte digest. Both forms are common in signing schemes; pick the
  wrong one and the signature is silently off.
* The canonical string uses **lowercase header names** but **preserves
  case** in the values. `X-Origin` on the wire, `x-origin:20000` in the
  signing input.

## 5. Layer 3 — Janus videoroom plugin handshake

All Janus messages are JSON over the WebSocket. Each request carries a
`"transaction"` string that Janus echoes on replies; make it unique per
message (we use short counters plus a UUID suffix).

The subscriber flow is:

```
client                                                     Janus
  │   {"janus":"create","transaction":"c1"}                  │
  │ ──────────────────────────────────────────────────────► │
  │                          {"data":{"id":<janus_session>}} │
  │ ◄────────────────────────────────────────────────────── │
  │                                                          │
  │   {"janus":"attach","plugin":"janus.plugin.videoroom",   │
  │    "opaque_id":<opaque>, "session_id":<janus_session>,   │
  │    "transaction":"c2"}                                   │
  │ ──────────────────────────────────────────────────────► │
  │                          {"data":{"id":<pub_handle>}}    │
  │ ◄────────────────────────────────────────────────────── │
  │                                                          │
  │   {"janus":"message","handle_id":<pub_handle>,           │
  │    "session_id":<janus_session>,"transaction":"c3",      │
  │    "body":{"request":"join","ptype":"publisher",         │
  │            "room":<room_id>,"pin":<pin>,                 │
  │            "display":"<any label>"}}                     │
  │ ──────────────────────────────────────────────────────► │
  │                                                          │
  │                  {"janus":"event", ...                   │
  │                    "plugindata":{"data":{                │
  │                      "videoroom":"joined",               │
  │                      "private_id":<private_id>,          │
  │                      "publishers":[{"id":<feed_id>,..}]} │
  │                    }}                                    │
  │ ◄────────────────────────────────────────────────────── │
```

You're now in the room as a publisher, but you never actually publish
anything — you use this handle purely to discover what feeds exist.

If `publishers` is empty, the crib hasn't joined yet. Keepalive the session
(see §7) and wait up to `wait_for_cradle_secs` for a later event carrying
a non-empty `publishers` array.

### Subscriber attach + join

Once you know `feed_id` (the crib's publisher id) and `private_id`
(your own room membership token), attach a **second** handle and join
as subscriber:

```json
{"janus":"attach", "plugin":"janus.plugin.videoroom",
 "opaque_id":"<opaque>", "session_id":<janus_session>, "transaction":"c4"}
```

```json
{"janus":"message", "handle_id":<sub_handle>,
 "session_id":<janus_session>, "transaction":"c5",
 "body":{"request":"join", "ptype":"subscriber",
         "room":<room_id>, "pin":"<pin>",
         "streams":[{"feed":<feed_id>}],
         "private_id":<private_id>}}
```

Janus will then send an SDP offer plus trickle ICE candidates.

## 6. Layer 4 — WebRTC negotiation

Janus delivers the offer and ICE candidates in separate messages:

```json
{"janus":"event", "jsep":{"type":"offer", "sdp":"..."}}
{"janus":"trickle", "candidate":{"sdpMid":"0", "candidate":"candidate:1 1 udp ..."}}
{"janus":"trickle", "candidate":{"sdpMid":"1", "candidate":"candidate:1 1 udp ..."}}
{"janus":"trickle", "candidate":{"completed":true}}
```

Rather than feed the candidates into `pc.addIceCandidate()` one by one
(which some WebRTC stacks handle fine and others don't), this library
inlines them into the SDP before the peer connection ever sees it. The
rule is simple: for each candidate, find the m-section indicated by
`sdpMid`, and insert `a=<candidate line>` immediately after that
m-section's `a=ice-pwd:` line. See `inject_trickle_candidates_into_sdp()`
for the implementation.

Then the standard aiortc dance:

```python
pc = RTCPeerConnection(RTCConfiguration(iceServers=[
    RTCIceServer("stun:stun.l.google.com:19302")
]))
await pc.setRemoteDescription(RTCSessionDescription(sdp=sdp, type="offer"))
answer = await pc.createAnswer()
await pc.setLocalDescription(answer)
```

Send the answer back via Janus:

```json
{"janus":"message", "handle_id":<sub_handle>,
 "session_id":<janus_session>, "transaction":"c6",
 "body":{"request":"start"},
 "jsep":{"type":"answer", "sdp":<pc.localDescription.sdp>, "trickle":false}}
```

Shortly after, Janus sends `{"janus":"webrtcup"}` and media starts
flowing into your `pc.on("track")` handlers. Video is H.264, audio is
Opus (48kHz, mono), both decoded by aiortc into `av.VideoFrame` and
`av.AudioFrame` objects respectively.

### MediaRelay is load-bearing

aiortc's `MediaRelay` is not strictly required, but in practice tracks
received directly (without `relay.subscribe(track)`) sometimes drop
frames under GC pressure or in long-running sessions. Always subscribe.

## 7. Keepalives and teardown

* Send `{"janus":"keepalive", "session_id":<id>}` every 10–30 seconds;
  Janus drops idle sessions after 60s. This library sends every 10s.
* `{"janus":"webrtcup"}`: informational — WebRTC is established.
* `{"janus":"media", ...}`: informational — media flowing/not-flowing.
* `{"janus":"event", "plugindata":{"data":{"leaving":...}}}`: the
  publisher (the crib) has left. Time to reconnect.
* Graceful close: `{"janus":"destroy", "session_id":<id>}` and close
  the WebSocket. Don't worry if it fails — the server will reap the
  session anyway.

## 8. Failure modes worth knowing about

* **403 on WebSocket upgrade.** The HMAC signature is wrong. Check: did
  you hex the inner SHA-256? Did you lowercase header *names* but keep
  value case? Is the timestamp within the server's accepted skew
  (roughly a minute)?
* **403 on the REST video-room call.** Your SigV4 signature is wrong, or
  your Cognito tokens expired. Call `auth.ensure_valid()`.
* **No `publishers` in the `joined` event.** The crib isn't online right
  now (powered off, WiFi dropped, or just not paired to the account).
  Wait `wait_for_cradle_secs` and keepalive; don't spin.
* **`webrtcup` fires but no frames arrive.** Network path is blocking
  the WebRTC media path. Check ICE candidates — if only host and relay
  candidates are present and the crib is on a restrictive NAT, a TURN
  server would be needed, but in practice every crib we've observed
  yields at least one srflx candidate via Google STUN.
* **Frames stop mid-session.** Usually a TCP reset on the WS side. The
  per-crib reconnect loop with exponential backoff handles this; after
  two consecutive 403s, fall all the way back to re-authenticating.

## 9. What we chose not to reverse-engineer

* **Local MQTT control plane.** The crib exposes an mTLS MQTT interface
  that the mobile app uses for low-latency control (bouncing, music, 2-way
  audio). Reading from it would be straightforward; *writing* to it is
  explicitly out of scope because this library is read-only by design.
* **Firmware OTA channel.** Not relevant to monitoring.
* **In-app analytics endpoints.** Not relevant, and likely carry PII we
  have no business touching.

The read-only stance is both ethical (don't mess with a device that a
real infant depends on) and practical (it keeps the blast radius of any
bug in this library small).
