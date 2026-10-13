
import os
import time
import math
import base64
import threading
import uuid

from collections import defaultdict, deque
from typing import Optional

from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import JSONResponse
from starlette.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


# ============================================================
# CONFIGURATION
# Configure these variables in Render to match your assignment.
# ============================================================

RATE_LIMIT = int(os.getenv("RATE_LIMIT", "10"))
TOTAL_ORDERS = int(os.getenv("TOTAL_ORDERS", "100"))
WINDOW_SECONDS = 10

if RATE_LIMIT < 1:
    raise ValueError("RATE_LIMIT must be at least 1")

if TOTAL_ORDERS < 1:
    raise ValueError("TOTAL_ORDERS must be at least 1")


# ============================================================
# APPLICATION
# ============================================================

api = FastAPI(title="Orders Middleware API", version="1.0.0")


# ============================================================
# SHARED IN-MEMORY STATE
# Suitable for this single-instance assignment implementation.
# ============================================================

rate_buckets = defaultdict(deque)
orders = {}
idempotency_records = {}

next_order_id = 1
state_lock = threading.Lock()


# ============================================================
# REQUEST CONTEXT + RATE LIMITING
# ============================================================

@api.middleware("http")
async def request_context_and_rate_limit(request: Request, call_next):
    request_id = request.headers.get("X-Request-Id") or str(uuid.uuid4())
    client_id = request.headers.get("X-Client-Id")

    request.state.request_id = request_id
    request.state.client_id = client_id

    # CORS middleware wraps this middleware below, so it can add
    # CORS headers even when this middleware returns an error.

    # Preflight requests should not consume the rate limit.
    if request.method == "OPTIONS":
        response = await call_next(request)
        response.headers["X-Request-Id"] = request_id
        return response

    if not client_id or not client_id.strip():
        response = JSONResponse(
            status_code=400,
            content={"detail": "X-Client-Id header is required"},
        )
        response.headers["X-Request-Id"] = request_id
        return response

    now = time.monotonic()

    with state_lock:
        bucket = rate_buckets[client_id]

        # Rolling 10-second window.
        while bucket and now - bucket[0] >= WINDOW_SECONDS:
            bucket.popleft()

        if len(bucket) >= RATE_LIMIT:
            retry_after = max(
                1,
                math.ceil(WINDOW_SECONDS - (now - bucket[0])),
            )

            response = JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded"},
                headers={"Retry-After": str(retry_after)},
            )
            response.headers["X-Request-Id"] = request_id
            return response

        bucket.append(now)

    response = await call_next(request)
    response.headers["X-Request-Id"] = request_id
    return response


# ============================================================
# REQUEST MODEL
# ============================================================

class OrderRequest(BaseModel):
    product: Optional[str] = "default"
    quantity: int = Field(default=1, ge=1)


# ============================================================
# CURSOR HELPERS
# Cursor is Base64 URL-safe encoding of the last returned ID.
# Example: "ODA" decodes to "80".
# ============================================================

def encode_cursor(order_id: int) -> str:
    encoded = base64.urlsafe_b64encode(
        str(order_id).encode("utf-8")
    ).decode("ascii")

    return encoded.rstrip("=")


def decode_cursor(cursor: str) -> int:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        decoded = base64.b64decode(
            padded,
            altchars=b"-_",
            validate=True,
        ).decode("utf-8")

        value = int(decoded)

        if value < 0 or str(value) != decoded:
            raise ValueError("Invalid cursor value")

        return value

    except (ValueError, UnicodeDecodeError, base64.binascii.Error):
        raise HTTPException(
            status_code=400,
            detail="Invalid pagination cursor",
        )


# ============================================================
# HEALTH CHECK
# ============================================================

@api.get("/")
def health():
    return {
        "status": "ok",
        "service": "orders-api",
    }


# ============================================================
# CREATE ORDER WITH IDEMPOTENCY
# ============================================================

@api.post("/orders")
def create_order(
    payload: OrderRequest,
    idempotency_key: Optional[str] = Header(
        default=None,
        alias="Idempotency-Key",
    ),
):
    global next_order_id

    if not idempotency_key or not idempotency_key.strip():
        raise HTTPException(
            status_code=400,
            detail="Idempotency-Key header is required",
        )

    key = idempotency_key.strip()

    with state_lock:
        # A repeated key returns the original order.
        if key in idempotency_records:
            original_id = idempotency_records[key]
            return orders[original_id]

        order = {
            "id": next_order_id,
            "product": payload.product,
            "quantity": payload.quantity,
        }

        orders[next_order_id] = order
        idempotency_records[key] = next_order_id
        next_order_id += 1

        return order


# ============================================================
# PAGINATED ORDER LIST
#
# GET /orders?limit=8
# GET /orders?limit=8&cursor=ODA
#
# Cursor represents the last ID already received.
# ============================================================

@api.get("/orders")
def list_orders(
    limit: int = 10,
    cursor: Optional[str] = None,
):
    if limit < 1 or limit > 100:
        raise HTTPException(
            status_code=400,
            detail="limit must be between 1 and 100",
        )

    last_id = decode_cursor(cursor) if cursor else 0

    if last_id > TOTAL_ORDERS:
        raise HTTPException(
            status_code=400,
            detail="Pagination cursor exceeds the order range",
        )

    if last_id == TOTAL_ORDERS:
        return {
            "items": [],
            "next_cursor": None,
        }

    first_id = last_id + 1
    last_returned_id = min(last_id + limit, TOTAL_ORDERS)

    items = [
        {"id": order_id}
        for order_id in range(first_id, last_returned_id + 1)
    ]

    next_cursor = (
        encode_cursor(last_returned_id)
        if last_returned_id < TOTAL_ORDERS
        else None
    )

    return {
        "items": items,
        "next_cursor": next_cursor,
    }


# ============================================================
# CORS
# Wrap the entire ASGI application so CORS headers are also
# applied to errors generated by the inner middleware.
# ============================================================

app = CORSMiddleware(
    app=api,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["Retry-After", "X-Request-Id"],
)
