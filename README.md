# mod_audio_stream fork

FreeSWITCH module for bidirectional audio streaming via WebSocket.

Fork of [amigniter/mod_audio_stream](https://github.com/amigniter/mod_audio_stream) with added **WRITE_REPLACE** support for low-latency audio injection.

## Changes from upstream

- **SMBF_WRITE_REPLACE**: Binary WebSocket frames are received and injected directly into the channel's write stream via a ring buffer, bypassing the JSON/base64/temp-file overhead of the original `streamAudio` mechanism.
- **Barge-in support**: `clearAudio` JSON command flushes the write buffer immediately.
- **Speex resampling on write path**: Incoming audio (e.g. 16kHz) is resampled to channel rate (e.g. 8kHz) in the WRITE_REPLACE callback.

## Build

```bash
git submodule init && git submodule update
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make
sudo make install  # or: sudo cp mod_audio_stream.so /usr/lib/freeswitch/mod/
```

Requires: `libfreeswitch-dev`, `libspeexdsp-dev`, `libevent-dev`, `libssl-dev`, `cmake`, `pkg-config`
