# Notification Router

SQLite-backed notification outbox with explicit channel preferences, UTC schedules, local-time quiet windows, deduplicated events, worker leases and bounded retries. Python 3.10+ with system IANA timezone data; no runtime Python dependencies.

```sh
python -m unittest discover -s tests -v
python demo.py
```

```python
from router import Router
queue = Router('notifications.db')
queue.preference('demo-user', 'email', zone='Africa/Lagos', start=22*60, end=8*60)
queue.enqueue('demo-user', 'email', 'synthetic-event', {'message': 'Local preview'})
def preview(recipient, payload, *, idempotency_key):
    print(recipient, payload, idempotency_key)
queue.dispatch_one({'email': preview})
```

The included adapter only prints synthetic messages. No email/SMS/push service is configured and nothing is sent externally. An adapter is an injected callable receiving recipient, payload and a stable delivery idempotency key. It must raise on failure. Configure real providers separately only when authorised.

Preferences are required before enqueue; disabled channels are suppressed. Quiet boundaries use local minutes of day, inclusive start/exclusive end; equal boundaries disable quiet hours. Scheduled instants are UTC timestamps. Quiet exits walk actual UTC minutes to handle skipped and repeated daylight-saving times. Dispatch rechecks current opt-outs and quiet hours; relaxing an earlier quiet window does not move an already deferred delivery earlier. Opting in later does not revive suppressed messages.

An event key is unique per recipient/channel. An identical payload returns the original delivery regardless of changed scheduling arguments; changed payload conflicts. Worker claims are atomic; expired leases recover pending work. Provider calls run outside transactions. Retries use 2^attempt seconds capped at one hour, with three attempts by default. Use the same attempt limit across workers. Errors store exception type only. Adapter timeout must be shorter than lease; a stale settlement raises Conflict.

Delivery is at least once: a provider may succeed before a worker crashes. Adapters must enforce the supplied idempotency key to prevent external duplicates. Opt-out changes during an in-flight call cannot retract it. This is a trusted local orchestration library, without a hosted scheduler, authentication, encryption, real provider guarantees or automatic data retention. Payloads and recipient identifiers are stored locally in plaintext; use synthetic data for the demo. Limits: 16 KiB JSON payload, 200-character event key, leases up to one hour, attempt limit up to20.

14 tests cover opt-outs, schedules, restart, timezone boundaries, daylight-saving gaps/folds, content conflicts, channel separation, concurrent claims, expired leases, retries and dead letters. GitHub Actions runs the tests and local preview on Python 3.10/3.12/3.13. Actions → Tests and demo → Run workflow repeats them.
