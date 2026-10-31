import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from router import Router, Conflict
from router.queue import next_allowed, quiet


def utc(value):
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp()


class RouterTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "router.db"
        self.now = utc("2026-01-01T12:00:00")
        self.router = Router(self.path, lambda: self.now)
        self.router.preference("alice", "email")

    def enqueue(self, event="event", payload=None, **kwargs):
        return self.router.enqueue("alice", "email", event, payload or {"message": "sample"}, **kwargs)

    def test_success_and_deduplication(self):
        ident = self.enqueue()
        self.assertEqual(self.enqueue(), ident)
        with self.assertRaises(Conflict):
            self.enqueue(payload={"message": "changed"})
        calls = []
        self.router.dispatch_one({"email": lambda *a, **kw: calls.append((a, kw))})
        self.assertEqual(calls[0][1]["idempotency_key"], ident)
        self.assertEqual(self.router.status(ident)["state"], "delivered")
        self.assertIsNone(self.router.dispatch_one({}))

    def test_explicit_preferences_and_opt_out(self):
        with self.assertRaises(ValueError):
            self.router.enqueue("unknown", "email", "e", {})
        self.router.preference("alice", "email", enabled=False)
        ident = self.enqueue()
        self.assertEqual(self.router.status(ident)["state"], "suppressed")
        self.assertIsNone(self.router.claim())

    def test_preference_change_before_dispatch(self):
        ident = self.enqueue()
        self.router.preference("alice", "email", enabled=False)
        self.assertIsNone(self.router.claim())
        self.assertEqual(self.router.status(ident)["state"], "suppressed")

    def test_schedule_and_restart(self):
        ident = self.enqueue(scheduled=self.now + 300)
        self.assertIsNone(self.router.claim())
        self.now += 300
        self.router = Router(self.path, lambda: self.now)
        self.assertEqual(self.router.claim()["id"], ident)

    def test_quiet_overnight_boundaries(self):
        start = utc("2026-01-01T23:30:12")
        self.assertEqual(next_allowed(start, "UTC", 22*60, 8*60), utc("2026-01-02T08:00:00"))
        self.assertTrue(quiet(utc("2026-01-01T22:00:00"), "UTC", 1320, 480))
        self.assertFalse(quiet(utc("2026-01-01T08:00:00"), "UTC", 1320, 480))
        self.assertEqual(next_allowed(start, "UTC", 0, 0), start)

    def test_daytime_and_timezone(self):
        self.assertEqual(next_allowed(self.now, "UTC", 11*60, 13*60), self.now + 3600)
        self.router.preference("alice", "email", zone="Africa/Lagos", start=13*60, end=14*60)
        ident = self.enqueue()
        self.assertEqual(self.router.status(ident)["due"], self.now + 3600)
        self.assertIsNone(self.router.claim())
        self.now += 3600
        self.assertEqual(self.router.claim()["id"], ident)

    def test_dst_skipped_and_repeated_hours(self):
        # New York jumps from 01:59 to 03:00: a missing 02:30 exit ends at 03:00.
        self.assertEqual(next_allowed(utc("2026-03-08T06:45:00"), "America/New_York", 60, 150), utc("2026-03-08T07:00:00"))
        # Both occurrences of 01:30 stay quiet until local 02:00.
        for stamp in ["2026-11-01T05:30:00", "2026-11-01T06:30:00"]:
            self.assertEqual(next_allowed(utc(stamp), "America/New_York", 60, 120), utc("2026-11-01T07:00:00"))

    def test_preference_quiet_change_rechecks_due_work(self):
        ident = self.enqueue()
        self.router.preference("alice", "email", start=11*60, end=13*60)
        self.assertIsNone(self.router.claim())
        self.assertEqual(self.router.status(ident)["due"], self.now + 3600)

    def test_failure_backoff_and_dead_letter(self):
        ident = self.enqueue()
        calls = []
        def failing(*args, **kwargs):
            calls.append(kwargs["idempotency_key"])
            raise RuntimeError("secret provider response must not be persisted")
        for attempt in range(1, 4):
            result = self.router.dispatch_one({"email": failing})
            self.assertEqual(result["attempts"], attempt)
            self.assertEqual(result["last_error"], "RuntimeError")
            self.assertEqual(result["state"], "dead" if attempt == 3 else "pending")
            if attempt < 3:
                self.assertIsNone(self.router.claim())
                self.now = result["due"]
        self.assertEqual(calls, [ident]*3)

    def test_lease_expiry_and_stale_ack(self):
        ident = self.enqueue()
        first = self.router.claim(lease=1)
        self.now += 1
        second = self.router.claim()
        self.assertNotEqual(first["token"], second["token"])
        with self.assertRaises(Conflict):
            self.router.settle(ident, first["token"])
        self.router.settle(ident, second["token"])
        self.assertEqual(self.router.status(ident)["state"], "delivered")

    def test_concurrent_claim_is_exclusive(self):
        self.enqueue()
        with ThreadPoolExecutor(max_workers=8) as pool:
            claims = list(pool.map(lambda _: self.router.claim(), range(16)))
        self.assertEqual(sum(row is not None for row in claims), 1)

    def test_crash_attempt_exhaustion(self):
        ident = self.enqueue()
        for _ in range(3):
            self.assertIsNotNone(self.router.claim(lease=1))
            self.now += 1
        self.assertIsNone(self.router.claim())
        self.assertEqual(self.router.status(ident)["state"], "dead")

    def test_channels_and_recipients_separate_event_keys(self):
        first = self.enqueue()
        self.router.preference("alice", "push")
        second = self.router.enqueue("alice", "push", "event", {"message": "sample"})
        self.router.preference("bob", "email")
        third = self.router.enqueue("bob", "email", "event", {"message": "sample"})
        self.assertEqual(len({first, second, third}), 3)

    def test_invalid_inputs_and_adapter_failure(self):
        for scheduled in [True, float("nan"), -1]:
            with self.assertRaises(ValueError):
                self.enqueue(scheduled=scheduled)
        with self.assertRaises(ValueError):
            self.enqueue(payload={"large": "x"*17000})
        for start in [-1, True, 1440]:
            with self.assertRaises(ValueError):
                self.router.preference("alice", "email", start=start)
        self.enqueue()
        self.assertEqual(self.router.dispatch_one({})["last_error"], "KeyError")


if __name__ == "__main__":
    unittest.main()
