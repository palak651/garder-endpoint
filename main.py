```python
import base64
import json
import time
from collections import defaultdict, deque
from threading import Lock

from fastapi import FastAPI, Header, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

app = FastAPI()

TOTAL_ORDERS = 42
RATE_LIMIT = 20
WINDOW_SECONDS = 10

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory storage
idempotency_store = {}
created_orders = []
next_order_id = 43

rate_buckets = defaultdict(deque)
lock = Lock()


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    # CORS preflight requests must be allowed through
    if request.method == "OPTIONS":
        return await call_next(request)

    client_id = request.headers.get("X-Client-Id", "anonymous")
    now = time.monotonic()

    with lock:
        bucket = rate_buckets[client_id]

        while bucket and now - bucket[0] >= WINDOW_SECONDS:
            bucket.popleft()

        if len(bucket) >= RATE_LIMIT:
            retry_after = max(
                1, int(WINDOW_SECONDS - (now - bucket[0]) + 0.999)
            )
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded"},
                headers={"Retry-After": str(retry_after)},
            )

        bucket.append(now)

    return await call_next(request)


@app.post("/orders")
async def create_order(
    idempotency_key: str | None = Header(
        default=None, alias="Idempotency-Key"
    ),
):
    global next_order_id

    if not idempotency_key:
        return JSONResponse(
            status_code=400,
            content={"detail": "Idempotency-Key header is required"},
        )

    with lock:
        if idempotency_key in idempotency_store:
            return JSONResponse(
                status_code=200,
                content=idempotency_store[idempotency_key],
            )

        order = {"id": next_order_id}
        next_order_id += 1

        idempotency_store[idempotency_key] = order
        created_orders.append(order)

    return JSONResponse(status_code=201, content=order)


@app.get("/orders")
async def list_orders(
    limit: int = Query(default=10, ge=1, le=42),
    cursor: str | None = None,
):
    # The catalog always contains IDs 1 through 42.
    catalog = [{"id": i} for i in range(1, TOTAL_ORDERS + 1)]

    start = 0

    if cursor:
        try:
            decoded = base64.urlsafe_b64decode(
                cursor + "=" * (-len(cursor) % 4)
            ).decode()
            start = int(decoded)
        except Exception:
            return JSONResponse(
                status_code=400,
                content={"detail": "Invalid cursor"},
            )

    if start < 0 or start > TOTAL_ORDERS:
        return JSONResponse(
            status_code=400,
            content={"detail": "Invalid cursor"},
        )

    items = catalog[start : start + limit]
    next_position = start + len(items)

    next_cursor = None
    if next_position < TOTAL_ORDERS:
        next_cursor = base64.urlsafe_b64encode(
            str(next_position).encode()
        ).decode().rstrip("=")

    return {
        "items": items,
        "next_cursor": next_cursor,
    }


@app.get("/")
async def health():
    return {"status": "ok"}
```
