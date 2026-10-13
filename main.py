```python
import base64
import time
from collections import defaultdict, deque
from threading import Lock

from fastapi import FastAPI, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

app = FastAPI()

# CORS: allow the grader's browser to access the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

TOTAL_ORDERS = 42
RATE_LIMIT = 20
WINDOW_SECONDS = 10

lock = Lock()

# Idempotency storage
idempotency_store = {}
next_order_id = 43

# Independent rate-limit bucket for each client
rate_buckets = defaultdict(deque)


@app.middleware("http")
async def rate_limit_middleware(request, call_next):
    # Browser CORS preflight requests should not consume the limit
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
                1,
                int(WINDOW_SECONDS - (now - bucket[0]) + 0.999)
            )
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded"},
                headers={"Retry-After": str(retry_after)},
            )

        bucket.append(now)

    return await call_next(request)


@app.get("/")
async def home():
    return {"status": "ok"}


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

    return JSONResponse(status_code=201, content=order)


@app.get("/orders")
async def list_orders(
    limit: int = Query(default=10, ge=1, le=42),
    cursor: str | None = None,
):
    catalog = [{"id": i} for i in range(1, TOTAL_ORDERS + 1)]

    start = 0

    if cursor:
        try:
            decoded = base64.urlsafe_b64decode(
                cursor + "=" * (-len(cursor) % 4)
            ).decode("utf-8")
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

    items = catalog[start:start + limit]
    next_position = start + len(items)

    next_cursor = None
    if next_position < TOTAL_ORDERS:
        next_cursor = base64.urlsafe_b64encode(
            str(next_position).encode("utf-8")
        ).decode("utf-8").rstrip("=")

    return {
        "items": items,
        "next_cursor": next_cursor,
    }
```
