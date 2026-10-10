#!/usr/bin/env python3
"""
TL-1 receiver for the Raspberry Pi.
===================================

What this is for
----------------
The phone is a dashboard you look at. This is the thing that sits in the
bag and does not miss anything: it connects to the logger, writes every
line to disk as it arrives, splits the stream into sessions, and turns
each finished session into a CSV and a TCX ready for Strava.

It is also where Strava credentials are allowed to live. The web page
deliberately has no upload button, because Strava's OAuth has no PKCE
flow and the token exchange needs the client secret; a static page
served from GitHub Pages cannot hold a secret, since anyone can read it
from view-source and then act as the application against every account
that ever authorised it. A Pi in your house can hold one. So the page
exports a file and this uploads it.

    pip install bleak
    python3 tl1_recv.py                      # log everything, no upload
    python3 tl1_recv.py --strava             # and upload each session
    python3 tl1_recv.py --strava-setup       # one-time authorisation
    python3 tl1_recv.py --replay capture.txt # no radio, parse a file

Two hosts at once
-----------------
This needs logger rev 16 or later to coexist with the phone. Before
rev 16 the logger had a single peripheral connection slot, so whichever
host connected first locked the other out: the Pi in the bag would take
the slot and the phone would simply fail to connect, with nothing at
either end saying why. rev 16 has two slots. On older firmware this
still works, but it is the Pi or the phone, not both.

Not losing data
---------------
Three rules, each of which exists because the alternative loses a ride:

  1. Every received line is appended to a raw log and flushed
     immediately. Not buffered, not written at the end. If this process
     is killed, whatever arrived is already on disk.
  2. A session CSV is written as the session runs, not assembled when
     it stops. A session that never gets its STOP marker - flat
     battery, crash, walking out of range and staying there - still
     leaves a complete CSV.
  3. Reconnection is unconditional and never gives up, because at a
     velodrome the bike rides out of range on purpose.

The logger's own flash is still the authoritative record. This is a
second copy that is already in a useful format.
"""

import argparse
import asyncio
import datetime as dt
import json
import os
import re
import signal
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

# Nordic UART Service, as advertised by the logger's Bluefruit BLEUart.
NUS_SVC = "6e400001-b5a3-f393-e0a9-e50e24dcca9e"
NUS_RX  = "6e400002-b5a3-f393-e0a9-e50e24dcca9e"   # we write commands here
NUS_TX  = "6e400003-b5a3-f393-e0a9-e50e24dcca9e"   # the logger notifies here

DEFAULT_DIR  = Path.home() / "tl1"
CRED_PATH    = Path.home() / ".config" / "tl1" / "strava.json"
STRAVA_TOKEN = "https://www.strava.com/oauth/token"
STRAVA_UPLOAD = "https://www.strava.com/api/v3/uploads"

LOG_TS = "%Y-%m-%d %H:%M:%S"


def say(msg):
    print(f"{dt.datetime.now().strftime(LOG_TS)}  {msg}", flush=True)


# ════════════════════════════════════════════════════════════════════
#  Sessions
# ════════════════════════════════════════════════════════════════════
class Session:
    """One ride, from a SESSION START marker to its STOP or to the end.

    Rows are written to the CSV as they arrive rather than collected and
    dumped at the end, so an interrupted session still leaves a usable
    file. The parsed copy kept in memory is only what the TCX needs.
    """

    # The column layouts the logger has used. The header line is the
    # contract when it is present; width is the fallback for a capture
    # that starts mid-session.
    LAYOUTS = {
        9:  "t_s section speed_kmh cadence_rpm power_w wheel_rev wheel_evt crank_rev crank_evt",
        10: "t_s section gear speed_kmh cadence_rpm power_w wheel_rev wheel_evt crank_rev crank_evt",
        11: "t_s section gear speed_kmh cadence_rpm power_w wheel_rev wheel_evt crank_rev crank_evt ratio",
    }

    def __init__(self, outdir, started_at, header_line):
        self.started_at = started_at
        self.cols = None
        self.circ_mm = 2100
        self.rows = []          # (t, speed_kmh, cadence, power, wheel_rev)
        self.laps = []          # (n, dur_s, dist_m)
        self.closed = False

        m = re.search(r"circ=(\d+)mm", header_line or "")
        if m:
            self.circ_mm = int(m.group(1))

        stamp = started_at.strftime("%Y-%m-%d_%H%M%S")
        outdir.mkdir(parents=True, exist_ok=True)
        self.csv_path = outdir / f"tl1-{stamp}.csv"
        self.tcx_path = outdir / f"tl1-{stamp}.tcx"
        self._csv = open(self.csv_path, "w", buffering=1)   # line buffered
        if header_line:
            self._csv.write(header_line + "\n")

    # ---- ingest ----
    def feed(self, line):
        self._csv.write(line + "\n")

        if line.startswith("t_s,"):
            names = [n.strip() for n in line.split(",")]
            if "t_s" in names and "speed_kmh" in names:
                self.cols = {n: i for i, n in enumerate(names)}
            return

        m = re.match(r"^# LAP (\d+)\s+t=([\d.]+)s\s+dur=([\d.]+)s\s+d=([\d.]+)m", line)
        if m:
            self.laps.append((int(m.group(1)), float(m.group(3)), float(m.group(4))))
            return

        if not line or not line[0].isdigit():
            return

        f = line.split(",")
        cols = self.cols
        if cols is None:
            names = self.LAYOUTS.get(len(f))
            if not names:
                return
            cols = {n: i for i, n in enumerate(names.split())}

        def num(key):
            i = cols.get(key)
            if i is None or i >= len(f):
                return None
            try:
                v = float(f[i])
            except ValueError:
                return None
            # An empty field means "the sensor has not reported", which
            # is not the same as zero; the logger is careful about that
            # distinction and discarding it here would undo the work.
            return v

        t = num("t_s")
        if t is None:
            return
        self.rows.append((t, num("speed_kmh"), num("cadence_rpm"),
                          num("power_w"), num("wheel_rev")))

    def close(self):
        if self.closed:
            return
        self.closed = True
        try:
            self._csv.close()
        except Exception:
            pass

    # ---- summary ----
    def distance_m(self):
        revs = [r[4] for r in self.rows if r[4] is not None]
        if len(revs) < 2:
            return 0.0
        return max(0.0, (revs[-1] - revs[0]) * (self.circ_mm / 1000.0))

    def duration_s(self):
        return (self.rows[-1][0] - self.rows[0][0]) if len(self.rows) > 1 else 0.0

    def summary(self):
        sp = [r[1] for r in self.rows if r[1] is not None]
        best = min((d for _, d, _ in self.laps if d > 0.5), default=None)
        return (f"{len(self.rows)} samples, {self.duration_s():.0f} s, "
                f"{self.distance_m():.0f} m, top {max(sp, default=0):.1f} km/h, "
                f"{len(self.laps)} laps"
                + (f", best {best:.1f} s" if best else ""))

    # ---- TCX ----
    def write_tcx(self):
        """Garmin TCX, which Strava accepts without GPS.

        The schema fixes the order of children, so these are emitted in
        the order the XSD declares rather than a convenient one. A Lap
        is TotalTimeSeconds, DistanceMeters, MaximumSpeed, Calories,
        Intensity, Cadence, TriggerMethod, Track. Calories, Intensity
        and TriggerMethod are required even though they mean nothing
        for a track session.

        One trackpoint per second. The log runs at 4 Hz, but TCX
        timestamps are whole seconds, so four points would share one
        timestamp and no reader can make sense of that. Strava
        downsamples to 1 Hz on arrival in any case, and the full
        resolution is in the CSV and on the logger's flash.
        """
        if len(self.rows) < 2:
            return None

        circ = self.circ_mm / 1000.0
        t0 = self.rows[0][0]
        base = next((r[4] for r in self.rows if r[4] is not None), None)

        def iso(ts):
            # Timezone-aware on purpose: utcfromtimestamp() is deprecated
            # and, worse, returns a naive datetime that anything
            # downstream is free to reinterpret as local time.
            return dt.datetime.fromtimestamp(
                ts, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        start_epoch = self.started_at.timestamp()
        dist = 0.0
        max_mps = 0.0
        cad_sum = cad_n = 0
        pts = []
        last_sec = -1

        for i, (t, sp, cd, pw, wr) in enumerate(self.rows):
            if wr is not None:
                if base is None:
                    base = wr
                if wr >= base:
                    dist = (wr - base) * circ
            mps = sp / 3.6 if sp is not None else None
            if mps is not None and mps > max_mps:
                max_mps = mps
            cadence = None
            if cd is not None:
                cadence = max(0, min(254, int(round(cd))))
                cad_sum += cadence
                cad_n += 1

            sec = int(t - t0)
            if sec == last_sec and i != len(self.rows) - 1:
                continue
            last_sec = sec

            x = ["   <Trackpoint>",
                 f"    <Time>{iso(start_epoch + (t - t0))}</Time>",
                 f"    <DistanceMeters>{dist:.2f}</DistanceMeters>"]
            if cadence is not None:
                x.append(f"    <Cadence>{cadence}</Cadence>")
            ext = ""
            if mps is not None:
                ext += f"<ns3:Speed>{mps:.3f}</ns3:Speed>"
            if pw is not None:
                ext += f"<ns3:Watts>{int(round(pw))}</ns3:Watts>"
            if ext:
                x.append(f"    <Extensions><ns3:TPX>{ext}</ns3:TPX></Extensions>")
            x.append("   </Trackpoint>")
            pts.append("\n".join(x))

        ident = iso(start_epoch)
        avg_cad = f"    <Cadence>{round(cad_sum / cad_n)}</Cadence>\n" if cad_n else ""
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<TrainingCenterDatabase
 xmlns="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"
 xmlns:ns3="http://www.garmin.com/xmlschemas/ActivityExtension/v2"
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 xsi:schemaLocation="http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2 \
http://www.garmin.com/xmlschemas/TrainingCenterDatabasev2.xsd">
 <Activities>
  <Activity Sport="Biking">
   <Id>{ident}</Id>
   <Lap StartTime="{ident}">
    <TotalTimeSeconds>{self.duration_s():.2f}</TotalTimeSeconds>
    <DistanceMeters>{dist:.2f}</DistanceMeters>
    <MaximumSpeed>{max_mps:.3f}</MaximumSpeed>
    <Calories>0</Calories>
    <Intensity>Active</Intensity>
{avg_cad}    <TriggerMethod>Manual</TriggerMethod>
    <Track>
{chr(10).join(pts)}
    </Track>
   </Lap>
   <Creator xsi:type="Device_t">
    <Name>TL-1 track logger</Name>
    <UnitId>0</UnitId>
    <ProductID>1</ProductID>
    <Version><VersionMajor>1</VersionMajor><VersionMinor>0</VersionMinor>\
<BuildMajor>0</BuildMajor><BuildMinor>0</BuildMinor></Version>
   </Creator>
  </Activity>
 </Activities>
</TrainingCenterDatabase>
"""
        self.tcx_path.write_text(xml)
        return self.tcx_path


# ════════════════════════════════════════════════════════════════════
#  The line handler, shared by the radio and by --replay
# ════════════════════════════════════════════════════════════════════
class Receiver:
    def __init__(self, outdir, on_session_done=None, echo=True):
        self.outdir = Path(outdir)
        self.outdir.mkdir(parents=True, exist_ok=True)
        self.echo = echo
        self.on_session_done = on_session_done
        self.session = None
        self.pending_header = None
        self.fw_rev = None
        self.raw = open(self.outdir / "tl1-raw.log", "a", buffering=1)

    def feed(self, line):
        line = line.rstrip("\r")
        # Rule 1: on disk before anything else can go wrong with it.
        self.raw.write(f"{dt.datetime.now().strftime(LOG_TS)}\t{line}\n")
        if self.echo and (line.startswith("#") or line.startswith("[")):
            say(f"  {line}")

        m = re.match(r"^# id rev=(\S+)", line)
        if m:
            self.fw_rev = m.group(1)
            say(f"logger is rev {self.fw_rev}")
            if self._rev_num() is not None and self._rev_num() < 16:
                say("  note: before rev 16 the logger has one host slot, "
                    "so the phone cannot connect while this is attached")

        if line.startswith("# SESSION START"):
            self._finish("a new session started before the last one stopped")
            self.session = Session(self.outdir, dt.datetime.now(), None)
            self.session.feed(line)
            say(f"session started -> {self.session.csv_path.name}")
            return

        if self.session is None:
            # Lines outside a session are kept in the raw log only.
            return

        self.session.feed(line)

        if line.startswith("# SESSION STOP"):
            self._finish("stopped")

    def _rev_num(self):
        try:
            return int(self.fw_rev)
        except (TypeError, ValueError):
            return None

    def _finish(self, why):
        s, self.session = self.session, None
        if s is None:
            return
        s.close()
        say(f"session {why}: {s.summary()}")
        try:
            tcx = s.write_tcx()
            if tcx:
                say(f"wrote {tcx.name}")
                if self.on_session_done:
                    self.on_session_done(s, tcx)
        except Exception as e:
            say(f"could not write the TCX: {e}")

    def close(self):
        # An unfinished session is still a session. This is rule 2: the
        # STOP marker is nice to have, not a precondition for a file.
        self._finish("ended without a STOP marker")
        try:
            self.raw.close()
        except Exception:
            pass


# ════════════════════════════════════════════════════════════════════
#  Strava
# ════════════════════════════════════════════════════════════════════
def _post(url, data):
    body = urllib.parse.urlencode(data).encode()
    req = urllib.request.Request(url, data=body, method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def strava_setup():
    """One-time authorisation. Needs a browser once, then never again.

    The secret stays on this machine, in a file only you can read. That
    is the whole reason uploading lives here and not in the web page.
    """
    print(__doc__.split("Two hosts at once")[0])
    print("Create an API application at https://www.strava.com/settings/api")
    print("Set the Authorization Callback Domain to: localhost\n")
    cid = input("Client ID: ").strip()
    secret = input("Client Secret: ").strip()
    url = ("https://www.strava.com/oauth/authorize?"
           + urllib.parse.urlencode({
               "client_id": cid, "response_type": "code",
               "redirect_uri": "http://localhost/exchange_token",
               "approval_prompt": "force", "scope": "activity:write,activity:read",
           }))
    print("\nOpen this in a browser and approve it:\n")
    print("  " + url)
    print("\nYou will land on a localhost page that fails to load. That is")
    print("expected. Copy the `code=` value out of its address bar.\n")
    code = input("code: ").strip()
    tok = _post(STRAVA_TOKEN, {
        "client_id": cid, "client_secret": secret,
        "code": code, "grant_type": "authorization_code",
    })
    CRED_PATH.parent.mkdir(parents=True, exist_ok=True)
    CRED_PATH.write_text(json.dumps({
        "client_id": cid, "client_secret": secret,
        "refresh_token": tok["refresh_token"],
    }, indent=1))
    os.chmod(CRED_PATH, 0o600)
    print(f"\nSaved to {CRED_PATH} with mode 600. Run with --strava from now on.")


def strava_upload(tcx_path, name=None):
    if not CRED_PATH.exists():
        say(f"no Strava credentials at {CRED_PATH}; run --strava-setup")
        return False
    cred = json.loads(CRED_PATH.read_text())
    tok = _post(STRAVA_TOKEN, {
        "client_id": cred["client_id"], "client_secret": cred["client_secret"],
        "refresh_token": cred["refresh_token"], "grant_type": "refresh_token",
    })
    # Strava rotates the refresh token on every exchange, so losing this
    # write means re-authorising by hand.
    if tok.get("refresh_token") and tok["refresh_token"] != cred["refresh_token"]:
        cred["refresh_token"] = tok["refresh_token"]
        CRED_PATH.write_text(json.dumps(cred, indent=1))
        os.chmod(CRED_PATH, 0o600)

    boundary = "----tl1" + str(int(time.time()))
    parts = []
    for k, v in (("data_type", "tcx"), ("name", name or tcx_path.stem),
                 ("sport_type", "Ride")):
        parts.append(f"--{boundary}\r\nContent-Disposition: form-data; "
                     f'name="{k}"\r\n\r\n{v}\r\n'.encode())
    parts.append(f"--{boundary}\r\nContent-Disposition: form-data; "
                 f'name="file"; filename="{tcx_path.name}"\r\n'
                 f"Content-Type: application/xml\r\n\r\n".encode())
    parts.append(tcx_path.read_bytes())
    parts.append(f"\r\n--{boundary}--\r\n".encode())

    req = urllib.request.Request(
        STRAVA_UPLOAD, data=b"".join(parts), method="POST",
        headers={"Authorization": "Bearer " + tok["access_token"],
                 "Content-Type": f"multipart/form-data; boundary={boundary}"})
    with urllib.request.urlopen(req, timeout=60) as r:
        out = json.load(r)
    say(f"uploaded to Strava, id {out.get('id')}: {out.get('status')}")
    return True


# ════════════════════════════════════════════════════════════════════
#  The radio
# ════════════════════════════════════════════════════════════════════
async def run_radio(args, rx):
    from bleak import BleakClient, BleakScanner

    buf = ""
    disconnected = asyncio.Event()

    def on_notify(_, data: bytearray):
        # A notification splits lines anywhere, so only complete lines
        # are handled and the remainder waits for the next packet. This
        # is the same framing bug that cost the phone four revisions.
        nonlocal buf
        buf += data.decode("utf-8", "replace")
        if len(buf) > 65536:                 # a peer that never sends \n
            buf = buf[-8192:]
        while "\n" in buf:
            line, buf = buf.split("\n", 1)
            try:
                rx.feed(line)
            except Exception as e:
                say(f"error handling a line: {e}")

    attempt = 0
    while not STOP.is_set():
        try:
            say(f"looking for {args.name}…")
            dev = await BleakScanner.find_device_by_filter(
                lambda d, ad: (d.name or "").startswith(args.name),
                timeout=args.scan_timeout)
            if dev is None:
                raise RuntimeError("not found")

            disconnected.clear()
            say(f"found {dev.name} at {dev.address}, connecting")
            async with BleakClient(
                    dev, disconnected_callback=lambda _: disconnected.set()) as cli:
                attempt = 0
                say("connected")
                await cli.start_notify(NUS_TX, on_notify)
                # Ask what we are talking to. rev 15 and earlier answer
                # "unknown command", which is its own answer.
                await cli.write_gatt_char(NUS_RX, b"id\n", response=False)
                await asyncio.sleep(0.3)
                await cli.write_gatt_char(NUS_RX, b"status\n", response=False)

                # Commands typed on stdin go straight to the logger, so
                # this doubles as the serial console when the logger is
                # on the bike and the laptop is not.
                stdin_task = asyncio.create_task(pump_stdin(cli))
                stop_task = asyncio.create_task(STOP.wait())
                drop_task = asyncio.create_task(disconnected.wait())
                done, pending = await asyncio.wait(
                    [stdin_task, stop_task, drop_task],
                    return_when=asyncio.FIRST_COMPLETED)
                for t in pending:
                    t.cancel()
                if drop_task in done:
                    say("the logger disconnected")
        except Exception as e:
            if STOP.is_set():
                break
            # Rule 3. Riding out of range is normal here, so the wait is
            # capped rather than escalating into giving up.
            wait = min(2 * (1.5 ** min(attempt, 8)), 20)
            attempt += 1
            say(f"{e}; trying again in {wait:.0f} s")
            try:
                await asyncio.wait_for(STOP.wait(), timeout=wait)
            except asyncio.TimeoutError:
                pass


async def pump_stdin(cli):
    """Forward typed commands to the logger. Quiet if there is no tty,
    which is how this runs under systemd."""
    if not sys.stdin or not sys.stdin.isatty():
        await STOP.wait()
        return
    loop = asyncio.get_running_loop()
    while not STOP.is_set():
        line = await loop.run_in_executor(None, sys.stdin.readline)
        if not line:
            return
        cmd = line.strip()
        if cmd:
            await cli.write_gatt_char(NUS_RX, (cmd + "\n").encode(), response=False)


STOP = None   # an asyncio.Event, created inside the loop


# ════════════════════════════════════════════════════════════════════
def main():
    ap = argparse.ArgumentParser(
        description="TL-1 receiver: log, convert and optionally upload.")
    ap.add_argument("--dir", default=str(DEFAULT_DIR),
                    help=f"where to write logs (default {DEFAULT_DIR})")
    ap.add_argument("--name", default="TL-1", help="BLE name prefix to look for")
    ap.add_argument("--scan-timeout", type=float, default=10.0)
    ap.add_argument("--strava", action="store_true",
                    help="upload each finished session")
    ap.add_argument("--strava-setup", action="store_true",
                    help="one-time Strava authorisation, then exit")
    ap.add_argument("--replay", metavar="FILE",
                    help="parse a captured text file instead of using the radio")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    if args.strava_setup:
        strava_setup()
        return 0

    def done(_session, tcx):
        if not args.strava:
            say("not uploading (pass --strava), the file is ready at "
                f"{tcx}")
            return
        try:
            strava_upload(tcx)
        except Exception as e:
            say(f"Strava upload failed, the file is still at {tcx}: {e}")

    rx = Receiver(args.dir, on_session_done=done, echo=not args.quiet)

    if args.replay:
        n = 0
        with open(args.replay, "r", errors="replace") as f:
            for line in f:
                rx.feed(line.rstrip("\n"))
                n += 1
        rx.close()
        say(f"replayed {n} lines from {args.replay}")
        return 0

    async def go():
        global STOP
        STOP = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, STOP.set)
            except NotImplementedError:
                pass
        await run_radio(args, rx)

    try:
        asyncio.run(go())
    finally:
        # Whatever happened, the session on disk gets finished properly.
        rx.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
