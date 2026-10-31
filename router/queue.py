import hashlib
import json
import math
import sqlite3
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from zoneinfo import ZoneInfo


class Conflict(ValueError):
    pass


def quiet(at, zone, start, end):
    if start == end:
        return False
    local = datetime.fromtimestamp(at, ZoneInfo(zone))
    minute = local.hour * 60 + local.minute
    return start <= minute < end if start < end else minute >= start or minute < end


def next_allowed(at, zone, start, end):
    if not quiet(at, zone, start, end):
        return at
    candidate = math.floor(at / 60) * 60 + 60
    # Walk actual UTC instants so skipped and repeated local times are handled.
    for _ in range(26 * 60):
        if not quiet(candidate, zone, start, end):
            return candidate
        candidate += 60
    raise ValueError("Quiet interval has no exit within 26 hours")


class Router:
    def __init__(self, path, clock=time.time):
        self.path, self.clock = str(path), clock
        with self.db() as db:
            db.executescript("""CREATE TABLE IF NOT EXISTS preferences(
                recipient TEXT NOT NULL,channel TEXT NOT NULL,enabled INTEGER NOT NULL,
                zone TEXT NOT NULL,start INTEGER NOT NULL,end INTEGER NOT NULL,PRIMARY KEY(recipient,channel));
                CREATE TABLE IF NOT EXISTS deliveries(
                id TEXT PRIMARY KEY,recipient TEXT NOT NULL,channel TEXT NOT NULL,event TEXT NOT NULL,
                payload TEXT NOT NULL,fingerprint TEXT NOT NULL,state TEXT NOT NULL,due REAL NOT NULL,
                attempts INTEGER NOT NULL,token TEXT,lease_until REAL,last_error TEXT,
                UNIQUE(recipient,channel,event));
                CREATE INDEX IF NOT EXISTS ready ON deliveries(state,due);""")

    @contextmanager
    def db(self, write=False):
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            if write:
                db.execute("BEGIN IMMEDIATE")
            yield db
            if write:
                db.commit()
        except BaseException:
            if write:
                db.rollback()
            raise
        finally:
            db.close()

    def preference(self, recipient, channel, *, enabled=True, zone="UTC", start=0, end=0):
        if not isinstance(recipient, str) or not recipient.strip() or len(recipient) > 200 or channel not in ("email", "sms", "push"):
            raise ValueError("Provide recipient and email, sms or push channel")
        if type(enabled) is not bool or any(type(v) is not int or not 0 <= v < 1440 for v in (start, end)):
            raise ValueError("Use boolean enabled and minute-of-day quiet boundaries")
        ZoneInfo(zone)
        with self.db(True) as db:
            db.execute("INSERT INTO preferences VALUES(?,?,?,?,?,?) ON CONFLICT(recipient,channel) DO UPDATE SET enabled=excluded.enabled,zone=excluded.zone,start=excluded.start,end=excluded.end", (recipient, channel, enabled, zone, start, end))

    def enqueue(self, recipient, channel, event, payload, *, scheduled=None):
        if not isinstance(event, str) or not event.strip() or len(event) > 200 or not isinstance(payload, dict):
            raise ValueError("Event key and JSON object payload required")
        body = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
        if len(body.encode()) > 16384:
            raise ValueError("Payload exceeds 16 KiB")
        due = self.clock() if scheduled is None else scheduled
        if type(due) not in (int, float) or not math.isfinite(due) or not 0 <= due <= 253402214400:
            raise ValueError("Use a finite UTC Unix timestamp before year 9999")
        fingerprint = hashlib.sha256(body.encode()).hexdigest()
        with self.db(True) as db:
            preference = db.execute("SELECT * FROM preferences WHERE recipient=? AND channel=?", (recipient, channel)).fetchone()
            if not preference:
                raise ValueError("Explicit preference required before enqueue")
            existing = db.execute("SELECT * FROM deliveries WHERE recipient=? AND channel=? AND event=?", (recipient, channel, event)).fetchone()
            if existing:
                if existing["fingerprint"] != fingerprint:
                    raise Conflict("Event key was already used with different content")
                return existing["id"]
            ident = uuid.uuid4().hex
            state = "pending" if preference["enabled"] else "suppressed"
            due = next_allowed(due, preference["zone"], preference["start"], preference["end"])
            db.execute("INSERT INTO deliveries VALUES(?,?,?,?,?,?,?,?,0,NULL,NULL,NULL)", (ident, recipient, channel, event, body, fingerprint, state, due))
            return ident

    def claim(self, *, lease=60, max_attempts=3):
        if type(lease) is not int or not 1 <= lease <= 3600 or type(max_attempts) is not int or not 1 <= max_attempts <= 20:
            raise ValueError("Invalid lease or attempt limit")
        now = self.clock()
        with self.db(True) as db:
            db.execute("UPDATE deliveries SET state='pending',token=NULL,lease_until=NULL WHERE state='inflight' AND lease_until<=?", (now,))
            for row in db.execute("SELECT * FROM deliveries WHERE state='pending' AND due<=? ORDER BY due,id", (now,)).fetchall():
                pref = db.execute("SELECT * FROM preferences WHERE recipient=? AND channel=?", (row["recipient"], row["channel"])).fetchone()
                if not pref or not pref["enabled"]:
                    db.execute("UPDATE deliveries SET state='suppressed' WHERE id=?", (row["id"],))
                    continue
                allowed = next_allowed(now, pref["zone"], pref["start"], pref["end"])
                if allowed > now:
                    db.execute("UPDATE deliveries SET due=? WHERE id=?", (allowed, row["id"]))
                    continue
                if row["attempts"] >= max_attempts:
                    db.execute("UPDATE deliveries SET state='dead' WHERE id=?", (row["id"],))
                    continue
                token = uuid.uuid4().hex
                db.execute("UPDATE deliveries SET state='inflight',attempts=attempts+1,token=?,lease_until=? WHERE id=?", (token, now + lease, row["id"]))
                result = dict(db.execute("SELECT * FROM deliveries WHERE id=?", (row["id"],)).fetchone())
                result["payload"] = json.loads(result["payload"])
                return result
        return None

    def settle(self, ident, token, *, error=None, max_attempts=3):
        if type(max_attempts) is not int or not 1 <= max_attempts <= 20 or (error is not None and not isinstance(error, str)):
            raise ValueError("Invalid settlement")
        now = self.clock()
        with self.db(True) as db:
            row = db.execute("SELECT * FROM deliveries WHERE id=?", (ident,)).fetchone()
            if not row or row["state"] != "inflight" or row["token"] != token or row["lease_until"] <= now:
                raise Conflict("Worker lease no longer owns this delivery")
            state = "delivered" if error is None else "dead" if row["attempts"] >= max_attempts else "pending"
            due = now + min(3600, 2 ** row["attempts"])
            db.execute("UPDATE deliveries SET state=?,due=?,token=NULL,lease_until=NULL,last_error=? WHERE id=?", (state, due, error[:500] if error else error, ident))

    def dispatch_one(self, adapters, *, lease=60, max_attempts=3):
        row = self.claim(lease=lease, max_attempts=max_attempts)
        if row is None:
            return None
        try:
            adapter = adapters[row["channel"]]
            adapter(row["recipient"], row["payload"], idempotency_key=row["id"])
        except Exception as exc:
            # Do not persist exception text: provider errors may contain secrets.
            self.settle(row["id"], row["token"], error=type(exc).__name__, max_attempts=max_attempts)
        else:
            self.settle(row["id"], row["token"], max_attempts=max_attempts)
        return self.status(row["id"])

    def status(self, ident):
        with self.db() as db:
            row = db.execute("SELECT id,recipient,channel,event,state,due,attempts,last_error FROM deliveries WHERE id=?", (ident,)).fetchone()
            if row is None:
                raise KeyError(ident)
            return dict(row)
