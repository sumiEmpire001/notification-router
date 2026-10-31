import tempfile
from pathlib import Path
from router import Router

with tempfile.TemporaryDirectory() as folder:
    router = Router(Path(folder) / "router.db")
    router.preference("synthetic-recipient", "email")
    ident = router.enqueue("synthetic-recipient", "email", "demo-event", {"message": "Synthetic local preview"})
    def preview(recipient, payload, *, idempotency_key):
        print("LOCAL PREVIEW ONLY", recipient, payload, idempotency_key)
    print(router.dispatch_one({"email": preview}))
    assert router.status(ident)["state"] == "delivered"
