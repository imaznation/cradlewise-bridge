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

* **Local MQTT *control* plane.** The crib exposes an mTLS MQTT interface
  that the mobile app uses for low-latency control (bouncing, music, 2-way
  audio). Sending control commands is explicitly out of scope because this
  library is read-only by design.

  However, the same broker also carries the *subscriber-side video
  signaling* — see §10. That path is a read-only "subscribe to a stream
  the crib already publishes," consistent with this library's ethos, and
  it bypasses the cloud entirely.
* **Firmware OTA channel.** Not relevant to monitoring.
* **In-app analytics endpoints.** Not relevant, and likely carry PII we
  have no business touching.

The read-only stance is both ethical (don't mess with a device that a
real infant depends on) and practical (it keeps the blast radius of any
bug in this library small).

## 10. Local-mode (LAN) WebRTC signaling

When the subscriber is on the same WiFi as the crib, the official mobile
app skips the cloud Janus path entirely and exchanges WebRTC offers/
answers with the crib directly over its local Greengrass MQTT broker. The
``connection_mode: "local"`` field in the app's analytics POSTs is the
giveaway; you'll never see a ``GET /cradles/{id}/videoRoom`` REST call
fire when the app is on the same LAN as the crib.

This is a substantially simpler path: there's no cloud Lambda gating,
no per-session HMAC, no shared video-room infrastructure. Just MQTT to
the crib's local broker, a Wowza-flavored signaling message exchange,
and a direct LAN WebRTC peer connection.

Reference implementation lives in ``cradlewise_bridge.local``
(``LocalVideoRoomClient``).

### 10.1 Transport

Per-crib mTLS to the crib's Greengrass broker on port 8883:

```
host:    <crib LAN IP>            # e.g. 192.168.68.69
port:    8883
TLS:     mTLS, the same per-crib cert/key/CA you use for cloud IoT
         (downloaded via /cradles/pairedUsers/v3 — see pycradlewise)
client_id: <device_id>            # MUST equal the device_id on this cert.
                                  # Anything else returns CONNACK rc=5
                                  # "Not authorized" — the per-cert IoT
                                  # policy gates iot:Connect on
                                  # ${iot:Certificate.Subject.CommonName}.
hostname check: skip              # cert CN doesn't match the LAN IP
```

The crib's local IP can be read from the cradle shadow at
``state.reported.bluetooth.wifiStats.localIP``, or from the REST
``GET /cradles/{cradle_id}/onlineStatus/v2`` response under
``state_message.info.connectivity.localIP``.

### 10.2 Topic + message envelope

All signaling rides on a single topic:

```
/{cradleId}/room                        # both client → crib and crib → client
```

Every message is a JSON object roughly shaped like ``LocalWebRtcMessage`` in
the Android source:

```json
{
  "command":    "<getOffer|sendOffer|sendResponse>",
  "direction":  "<play|publish>",
  "streamInfo": {"applicationName": "live",
                 "sessionId": "<unix-ms timestamp>",
                 "streamName": "<requesting peer's deviceId>"},
  "userData":   {"param1": "value1"},     // free-form; treat as opaque
  "sdp":        {"type": "offer|answer", "sdp": "..."},   // when applicable
  "ice":        {"candidate": "...", "sdpMid": "0", "sdpMLineIndex": 0}
                                                  // ICE-only messages
}
```

The signaling protocol is Wowza-style. Direction values mean:
* ``"play"`` — subscriber side (client). We use this for both ``getOffer``
  and ``sendResponse``.
* ``"publish"`` — publisher side (the crib). The crib uses this when it
  emits ``sendOffer``.

### 10.3 Subscribe-side flow

```
client                                           crib
  │   {command:"getOffer", direction:"play",      │
  │    streamInfo:{...sessionId:S}, userData}     │
  │ ────────────────────────────────────────────► │
  │                                                │
  │   {direction:"publish", command:"sendOffer",   │
  │    sdp:{type:"offer", sdp:"<H264 sendonly>"},  │
  │    streamInfo:{...sessionId:S}, userData}      │
  │ ◄──────────────────────────────────────────── │
  │                                                │
  │   {command:"sendResponse", direction:"play",   │
  │    sdp:{type:"answer", sdp:"<answer>"},        │
  │    streamInfo:{...sessionId:S}, userData}      │
  │ ────────────────────────────────────────────► │
  │                                                │
  │   {ice:{candidate:"...host..."}, streamInfo}   │  × 3 (1 UDP + 2 TCP)
  │ ◄──────────────────────────────────────────── │
  │                                                │
  │   ... ICE checks → DTLS-SRTP → media ...       │
```

The crib's ``sendOffer`` arrives within ~150 ms of ``getOffer``. The
``sessionId`` is echoed verbatim — use that to multiplex if you have
multiple sessions in flight.

### 10.4 The answer SDP must mirror the offer

This is the trickiest part: the crib's libwebrtc-derived SDP parser is
strict and silently drops aiortc's stock ``createAnswer`` output. The
following differences matter, and we've narrowed each one down by
diffing what the iOS app sends vs. what aiortc generates:

| Attribute             | Cradle expects                  | aiortc generates    |
|-----------------------|---------------------------------|---------------------|
| Audio ``m=`` port     | ``0`` + ``a=bundle-only``       | non-zero, no bundle |
| ``a=setup``           | ``passive`` or ``active``       | ``active`` (OK)     |
| Fingerprint count     | exactly one (``sha-256``)       | three (256/384/512) |
| OPUS rtpmap case      | ``OPUS``                        | ``opus``            |
| Extra ``a=msid``, ``a=msid-semantic``, ``a=rtcp:9 IN IP4 ...`` | absent | present |

The cleanest workaround is to build the answer by *mirroring* the offer
verbatim, then surgically flipping the direction-related attributes
(``sendonly`` → ``recvonly``, ``actpass`` → ``active``) and splicing in
aiortc's actual DTLS material (ufrag, pwd, sha-256 fingerprint). The
result mirrors the offer line-for-line, which is exactly what the
crib's parser is happy with. ``_build_mirrored_answer`` in ``local.py``
implements this.

### 10.5 Answer must be trickle-shaped

The cradle expects answers in trickle-ICE form: no ``a=candidate:`` lines
inline, no ``a=end-of-candidates``. ICE candidates come over MQTT as
separate ``ice`` messages.

Send the answer first, then start trickling your own candidates as you
gather them; the cradle starts trickling its candidates immediately after
it accepts the answer (typically 3 candidates: 1 UDP host + 1 TCP active
+ 1 TCP passive, all on the cradle's LAN IP).

### 10.6 Timing budget

The cradle gives you about **3 seconds** between ``sendOffer`` and a
valid ``sendResponse``. Miss it and the cradle removes you from
``monitor.localPeers`` and you have to start over with a fresh
``getOffer``. With aiortc this means setting
``RTCConfiguration(iceServers=[])`` — STUN gathering with the default
Google STUN server adds ~5 seconds and blows the budget.

### 10.7 What ``monitor.localPeers`` tells you

Independently useful: the crib's device shadow at
``state.reported.monitor.localPeers`` is a real-time list of deviceIds
currently subscribed to its WebRTC stream (locally OR via the cloud
path). It updates within ~100 ms of any peer joining or leaving. This
is the cleanest "is anyone watching this crib right now?" signal short
of polling.

### 10.8 Media transport: TCP-passive only (UDP DTLS is silently dropped)

The cradle advertises three ICE candidates on ``/{cradleId}/room``:

```
candidate:1 1 UDP    2015363327 <crib-ip> <port>      typ host
candidate:2 1 TCP    1015021823 <crib-ip> 9           typ host tcptype active
candidate:3 1 TCP    1010827519 <crib-ip> <port>      typ host tcptype passive
```

The UDP candidate is a red herring. The cradle's media path is **TCP only**;
DTLS attempts on the UDP host candidate succeed at ICE level but the
cradle drops them at DTLS handshake (pyOpenSSL surfaces this as
``SSL.ZeroReturnError``). The iOS app source confirms this — its inbound
ICE handler filters cradle candidates down to TCP-only:

```kotlin
if (StringsKt.contains$default(candidate, "TCP", ...)) {
    gotCradlesIceCandidate(localWebRtcMessage)
}
```

Use the **TCP-passive** candidate (we initiate the TCP connection to the
crib's advertised port). RFC 4571 frames every "datagram" on the TCP
connection: ``<2-byte BE length><payload>``. The same TCP connection
carries ICE STUN, DTLS, and SRTP packets, demuxed by the first byte of
each framed payload (per RFC 7983 — STUN: 0–3, DTLS: 20–63, SRTP/SRTCP:
128–191).

### 10.9 ICE on TCP — bidirectional STUN

Both sides do connectivity checks. After we send our STUN binding request
to the crib and get a response, the crib **also** sends us its own STUN
binding requests, both during the DTLS handshake and periodically after.
You must respond to those, or the crib drops the TCP connection within
a few seconds. ``aiortc.RTCDtlsTransport`` has no STUN handling at all
(its ``_recv_next`` ignores STUN packets) — the responder has to live
in the transport layer, intercepting STUN packets before they reach
aiortc.

A binding response needs:
* same ``transaction_id`` as the request
* ``XOR-MAPPED-ADDRESS`` pointing at the cradle's address as we see it
  (i.e. the TCP connection's remote endpoint)
* ``MESSAGE-INTEGRITY`` HMAC-signed with **our** ICE password (the one
  we put in the answer SDP — the cradle verifies the request against it)

### 10.10 DTLS — must be 1.2

The cradle responds to DTLS 1.0 ClientHellos with a fatal
``handshake_failure(40)`` alert. Force pyOpenSSL to negotiate DTLS 1.2
only:

```python
ctx = SSL.Context(SSL.DTLS_METHOD)
ctx.set_min_proto_version(SSL.TLS1_2_VERSION)   # maps to DTLS 1.2 in DTLS context
ctx.set_max_proto_version(SSL.TLS1_2_VERSION)
ctx.set_cipher_list(b"DEFAULT:HIGH:!aNULL:!eNULL:!MD5:!RC4")
```

aiortc's stock cipher list (``ECDHE-ECDSA-*`` only) is also too narrow
for the cradle's libwebrtc cipher set. Widening to ``DEFAULT:HIGH``
covers the negotiated suite without security loss.

### 10.11 H.264 RTP depacketization (RFC 6184)

The cradle's video stream is H.264 Baseline @ Level 4.0, OPUS audio. RTP
payload type 97 for video, 96 for audio. Standard RFC 6184 packetization:

* NAL types 1–23: single NAL packet — emit ``00 00 00 01`` start code +
  payload as Annex-B.
* NAL type 28 (FU-A): fragmented unit. Reassemble by tracking start/end
  bits, then emit start code + reconstructed NAL header + concatenated
  fragments.
* NAL type 24 (STAP-A): aggregation. Walk the embedded length-prefixed
  NAL units and emit each with its own start code.

Output an Annex-B stream and ffmpeg / PyAV decode it directly.

### 10.12 Session lifetime — application-layer keepAlive required

The crib drops the TCP media connection at exactly 15 s without an
application-layer keepalive. **Neither STUN consent-freshness nor RTCP
receiver reports satisfy this** — both transport-layer keepalives can
be sent at high frequency and the crib still drops at 15 s.

The right keepalive is a Wowza-style ``keepAlive`` command published on
the same MQTT signaling topic as the original handshake:

```json
{
  "direction": "play",
  "command":   "keepAlive",
  "streamInfo": {"applicationName": "live",
                 "sessionId": "<same as getOffer>",
                 "streamName": "<our deviceId>"},
  "userData":   {"param1": "value1"}
}
```

Source: ``LocalWebRtc.setLocalStreamKeepAlive`` in the APK. Send every
5 s (the iOS app uses a similar interval). Verified: with this in
place, sessions sustain for the full test window (60+ s with no drop).

### 10.13 Status — fully working

``LocalVideoRoomClient.run()`` and ``capture_snapshot()`` produce
decoded JPEG frames from the crib's live LAN stream, end-to-end. The
``capture_snapshot()`` convenience function is the recommended entry
point for "grab one frame on demand" use cases (e.g. attaching a frame
to a security alert).
