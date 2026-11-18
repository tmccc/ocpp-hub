# Raedian app protocol (DataTransfer)

The Raedian app talks to the charger through OCPP `DataTransfer` (`vendorId` READIAN, `messageId` RemoteConfiguration).
Inside, `data` is JSON with `instructType` and `originalData`, the hex of a binary frame. This is not documented by the
vendor. Everything here was read from captured traffic; what is not proven says so.

## Frame

```
head, command, 00 00, length (2 bytes, little endian), 08, check, session (8 bytes), payload
```

`head` is `FE` from the cloud and `AA` from the charger. `check` is the XOR of every other byte of the frame. The
session is the same in a request and its answer. `core/raedian.py` parses and builds frames.

## Commands seen

Seen on the wire while the Raedian app was open (the app sent this burst when it opened). "Decoded" means the hub
understands it and acts on it.

| Command | Request payload | Answer payload | What it is |
|---------|-----------------|----------------|------------|
| `a7` | `00` read, or `01 count windows` write | `00 count windows` (read), `00` (write) | The charging plan. **Decoded**, see below. |
| `b3` | one byte: `02` planning on, `01` planning off | `00` | The switch of the planning in the app. **Decoded** by the hub: off clears its profile, on writes it again. Meaning of the two values inferred from four switches the owner made; the vendor does not document it. |
| `b0` | 4 bytes little-endian Unix time (UTC), `0d` | the same 4 bytes | Clock sync: the cloud tells the time and the charger echoes it. Proven: the value equals the log time. |
| `b1` | `00` | charger id (16 bytes, padded), flag bytes, firmware string, a time stamp, flags | Device information. Layout not decoded. |
| `fb` | `ff` | `05`, then four 60-byte records: each has the charger id, a firmware string and a few flag bytes | A list of four entries (probably the connectors or the device parts). Not decoded. |
| `cb` | none | 4 bytes, `37 00 00 00` = 55 | A counter or setting. Meaning unknown. |
| `e0` | none | `00 10 10` | Unknown, three bytes. |
| `e9` | none | `20 01 80 25 00 00 00 01` | Unknown. `80 25` reads as 0x2580 = 9600, which looks like a serial baud rate. |
| `ac` | none | `00` | Unknown, a single status byte. |
| `fe` | 130 bytes that look encrypted or signed | `00`, firmware string, 8 bytes, the session | Probably a firmware or configuration push. The hub passes it on and logs it. Treat as dangerous; do not replay. |

All of these are passed on to the charger when vendor commands are forwarded, and logged in full in both directions
(`[forward] DataTransfer request for ...` / `answer of ...`).

## The charging plan (`a7`)

```
payload = 01, count, then count windows of 12 bytes:
window  = enabled (01, or 00 for a window that is switched off),
          overnight flag,
          weekday mask (bit 0 = Monday ... bit 6 = Sunday; Wednesday is 04, not 03),
          start (4 bytes, seconds after midnight), end (same),
          20
```

An answer to a read has the same layout. An empty plan answers `00 00`. The app switches a window off instead of
removing it, so the hub keeps disabled windows in the stored plan and excludes them from the active profile.
The charger answers an empty plan to a read. The hub returns its stored plan to the app.

## Reading an unknown command

Run the helper on the Home Assistant log (or a copy of it):

```bash
python tools/raedian_frames.py home-assistant.log --unknown-only
```

It prints one block per command byte with the request and the answer in hex, the readable text inside, and marks the
commands the hub does not decode with NEW. Messages the hub refused (`[plan] ... it said`) are included.

## Implementing a new command

1. Collect at least two exchanges of the command with different values, ideally one before and one after a change you
   made in the app, and put the frames in a test next to `tests/test_raedian.py`.
2. Add the layout to `core/raedian.py` (parse and build functions with a comment per function) and a row to the table
   above. Decode only what you saw; refuse what you did not (see `decode_plan`).
3. Add the command to `KNOWN_COMMANDS` in `tools/raedian_frames.py`.
4. A command that writes to the charger needs the owner's approval and a ceiling, like any other write (see
   `AGENTS.md`).
