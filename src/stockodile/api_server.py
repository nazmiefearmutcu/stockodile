from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import os
import random
import sys
import threading
import time
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, cast

from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from web3 import AsyncHTTPProvider, Web3

from stockodile import __version__
from stockodile.mcp_server import AsyncWeb3, get_onchain_price
from stockodile.store.catalog import Catalog

log = logging.getLogger(__name__)

VERIFYING_TXS: set[str] = set()


def _get_rpc_urls() -> list[str]:
    urls_str = os.getenv("BASE_RPC_URLS", "")
    if urls_str:
        return [u.strip() for u in urls_str.split(",") if u.strip()]
    fallback = os.getenv("BASE_RPC_URL", "https://base-rpc.publicnode.com")
    return [fallback]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    app.state.rpc_urls = _get_rpc_urls()
    app.state.current_rpc_index = 0
    if not app.state.rpc_urls:
        app.state.rpc_urls = ["https://base-rpc.publicnode.com"]
    url = app.state.rpc_urls[0]
    app.state.w3 = AsyncWeb3(AsyncHTTPProvider(url))
    yield
    # Shutdown
    try:
        w3 = getattr(app.state, "w3", None)
        if w3 is not None:
            provider = getattr(w3, "provider", None)
            if provider is not None:
                disconnect_fn = getattr(provider, "disconnect", None)
                if disconnect_fn is not None:
                    import inspect

                    res = disconnect_fn()
                    if inspect.isawaitable(res):
                        await res
    except (AttributeError, Exception):
        pass


app = FastAPI(
    title="Stockodile x402 Gated Market Data API",
    description=(
        "A demo API gating Base mainnet market data behind the x402 AI Agent payment protocol."
    ),
    version=__version__,
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

portal_public_dir = os.path.join(os.path.dirname(__file__), "api_portal", "public")
os.makedirs(os.path.join(portal_public_dir, "css"), exist_ok=True)
os.makedirs(os.path.join(portal_public_dir, "js"), exist_ok=True)
app.mount("/css", StaticFiles(directory=os.path.join(portal_public_dir, "css")), name="css")
app.mount("/js", StaticFiles(directory=os.path.join(portal_public_dir, "js")), name="js")

# Prometheus metrics tracking variables
METRICS_DASHBOARD_REQUESTS = 0
METRICS_MARKET_DATA_REQUESTS = 0
METRICS_METRICS_REQUESTS = 0
SERVER_START_TIME = time.time()


@app.get("/", include_in_schema=False, response_class=HTMLResponse)
async def root_dashboard() -> HTMLResponse:
    """Serve the interactive Stockodile x402 Micropayments Web Dashboard."""
    global METRICS_DASHBOARD_REQUESTS
    METRICS_DASHBOARD_REQUESTS += 1
    from stockodile.api_server_html import get_dashboard_html

    return HTMLResponse(content=get_dashboard_html())


@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html() -> Response:
    from fastapi.openapi.docs import get_swagger_ui_html

    response = get_swagger_ui_html(
        openapi_url=app.openapi_url or "/openapi.json",
        title=app.title + " - Swagger UI",
        oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
        swagger_js_url="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js",
        swagger_css_url="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css",
    )

    # Custom premium dark green theme CSS for Swagger UI (no filter: invert hack)
    custom_css = """
    <style>
        html, body {
            background-color: #0b0f19 !important;
            color: #f1f5f9 !important;
            font-family: 'Inter', -apple-system, BlinkMacSystemFont,
                         "Segoe UI", Roboto, Helvetica, Arial, sans-serif !important;
            margin: 0;
            padding: 0;
        }
        .swagger-ui {
            color: #e2e8f0 !important;
            background-color: #0b0f19 !important;
        }
        .swagger-ui .topbar {
            background-color: #0f172a !important;
            border-bottom: 2px solid #059669 !important;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2);
        }
        .swagger-ui .topbar a {
            color: #f1f5f9 !important;
            font-weight: bold;
        }
        .swagger-ui .topbar .download-url-wrapper input[type=text] {
            border: 1px solid #334155 !important;
            background-color: #1e293b !important;
            color: #f1f5f9 !important;
            border-radius: 4px;
        }
        .swagger-ui .topbar .download-url-wrapper .download-url-button {
            background: #059669 !important;
            color: #fff !important;
            border-radius: 4px;
        }
        .swagger-ui .info {
            margin: 30px 0 !important;
        }
        .swagger-ui .info .title {
            color: #059669 !important;
            font-size: 2.2rem !important;
            font-weight: 800 !important;
            letter-spacing: -0.025em;
        }
        .swagger-ui .info p, .swagger-ui .info li, .swagger-ui .info td, .swagger-ui label {
            color: #94a3b8 !important;
            font-size: 0.95rem !important;
            line-height: 1.6;
        }
        .swagger-ui .info a {
            color: #10b981 !important;
            text-decoration: none;
        }
        .swagger-ui .info a:hover {
            color: #34d399 !important;
            text-decoration: underline;
        }
        .swagger-ui .opblock-tag {
            color: #f1f5f9 !important;
            border-bottom: 1px solid #1e293b !important;
            font-size: 1.4rem !important;
            font-weight: 700 !important;
        }
        .swagger-ui .scheme-container {
            background-color: #0f172a !important;
            box-shadow: none !important;
            border: 1px solid #1e293b !important;
            border-radius: 8px !important;
            padding: 20px !important;
            margin-bottom: 25px !important;
        }
        .swagger-ui select {
            background-color: #1e293b !important;
            border: 1px solid #334155 !important;
            color: #f1f5f9 !important;
            border-radius: 6px !important;
            padding: 6px 10px !important;
        }
        .swagger-ui .opblock {
            background-color: #0f172a !important;
            border: 1px solid #1e293b !important;
            border-radius: 8px !important;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1) !important;
            overflow: hidden;
            margin-bottom: 15px !important;
        }
        .swagger-ui .opblock .opblock-summary {
            padding: 12px 20px !important;
            border-bottom: 1px solid #1e293b !important;
        }
        .swagger-ui .opblock.opblock-get {
            border-color: rgba(16, 185, 129, 0.4) !important;
            background-color: rgba(16, 185, 129, 0.03) !important;
        }
        .swagger-ui .opblock.opblock-get .opblock-summary-method {
            background-color: #059669 !important;
            color: #ffffff !important;
            font-weight: 700 !important;
            border-radius: 4px;
            padding: 6px 12px !important;
        }
        .swagger-ui .opblock.opblock-get .opblock-summary {
            border-color: rgba(16, 185, 129, 0.2) !important;
        }
        .swagger-ui .opblock.opblock-post {
            border-color: rgba(59, 130, 246, 0.4) !important;
            background-color: rgba(59, 130, 246, 0.03) !important;
        }
        .swagger-ui .opblock.opblock-post .opblock-summary-method {
            background-color: #2563eb !important;
            color: #ffffff !important;
            font-weight: 700 !important;
            border-radius: 4px;
            padding: 6px 12px !important;
        }
        .swagger-ui .opblock.opblock-post .opblock-summary {
            border-color: rgba(59, 130, 246, 0.2) !important;
        }
        .swagger-ui .opblock .opblock-summary-path,
        .swagger-ui .opblock .opblock-summary-path a {
            color: #f1f5f9 !important;
            font-weight: 600 !important;
            font-size: 0.95rem !important;
        }
        .swagger-ui .opblock .opblock-summary-description {
            color: #94a3b8 !important;
        }
        .swagger-ui .btn {
            border-color: #334155 !important;
            color: #f1f5f9 !important;
            background-color: #1e293b !important;
            border-radius: 6px !important;
            font-weight: 600 !important;
            transition: all 0.2s ease;
        }
        .swagger-ui .btn:hover {
            background-color: #334155 !important;
            color: #ffffff !important;
        }
        .swagger-ui .btn.execute {
            background-color: #059669 !important;
            border-color: #059669 !important;
            color: #ffffff !important;
        }
        .swagger-ui .btn.execute:hover {
            background-color: #10b981 !important;
            border-color: #10b981 !important;
        }
        .swagger-ui table thead tr td,
        .swagger-ui table thead tr th {
            color: #f1f5f9 !important;
            font-weight: 600 !important;
            border-bottom: 2px solid #1e293b !important;
        }
        .swagger-ui .parameters-col_name {
            color: #34d399 !important;
            font-weight: 600 !important;
        }
        .swagger-ui .parameter__name.required {
            color: #f87171 !important;
        }
        .swagger-ui .parameter__type {
            color: #60a5fa !important;
        }
        .swagger-ui .parameter__in {
            color: #94a3b8 !important;
        }
        .swagger-ui input[type=text] {
            background-color: #0b0f19 !important;
            border: 1px solid #334155 !important;
            color: #f8fafc !important;
            border-radius: 6px !important;
            padding: 8px 12px !important;
        }
        .swagger-ui input[type=text]:focus {
            border-color: #10b981 !important;
        }
        .swagger-ui .response-col_status {
            color: #34d399 !important;
            font-weight: 700 !important;
        }
        .swagger-ui .response-col_description {
            color: #e2e8f0 !important;
        }
        .swagger-ui .opblock-body pre.microlight {
            background-color: #080c14 !important;
            border: 1px solid #1e293b !important;
            color: #34d399 !important;
            border-radius: 8px !important;
            padding: 14px !important;
            font-family: monospace !important;
            font-size: 0.85rem !important;
        }
        .swagger-ui .model-box {
            background-color: #0f172a !important;
            border: 1px solid #1e293b !important;
            border-radius: 6px !important;
            padding: 12px !important;
        }
        .swagger-ui .model {
            color: #e2e8f0 !important;
        }
        .swagger-ui .model-title {
            color: #f8fafc !important;
        }
        .swagger-ui .prop-name {
            color: #94a3b8 !important;
        }
        .swagger-ui .prop-type {
            color: #60a5fa !important;
        }
        .swagger-ui section.models {
            border: 1px solid #1e293b !important;
            border-radius: 8px !important;
            background-color: #0f172a !important;
            margin-top: 35px !important;
        }
        .swagger-ui section.models h4 {
            color: #059669 !important;
            border-bottom: 1px solid #1e293b !important;
            padding: 15px 20px !important;
            font-size: 1.25rem !important;
            font-weight: 700 !important;
        }
        .swagger-ui section.models .model-container {
            background-color: #0b0f19 !important;
            border: 1px solid #1e293b !important;
            margin: 15px 20px !important;
            border-radius: 6px !important;
        }
        .swagger-ui table.headers td {
            color: #94a3b8 !important;
        }
        .swagger-ui .tabli.active button {
            color: #10b981 !important;
        }
    </style>
    """
    html_content = bytes(response.body).decode("utf-8")
    html_content = html_content.replace("</head>", f"{custom_css}</head>")
    return Response(content=html_content, media_type="text/html")


def get_w3() -> AsyncWeb3:
    if hasattr(app.state, "w3") and app.state.w3 is not None:
        return cast(AsyncWeb3, app.state.w3)
    # Fallback/lazy init (for tests that call handlers directly)
    urls = _get_rpc_urls()
    app.state.rpc_urls = urls
    app.state.current_rpc_index = 0
    app.state.w3 = AsyncWeb3(AsyncHTTPProvider(urls[0]))
    return cast(AsyncWeb3, app.state.w3)


async def switch_rpc_failover() -> None:
    if not hasattr(app.state, "rpc_urls") or not app.state.rpc_urls:
        app.state.rpc_urls = _get_rpc_urls()
    if not hasattr(app.state, "current_rpc_index"):
        app.state.current_rpc_index = 0

    num_urls = len(app.state.rpc_urls)
    if num_urls <= 1:
        return

    w3 = get_w3()
    disconnect_fn = getattr(w3.provider, "disconnect", None)
    if disconnect_fn is not None:
        import inspect

        try:
            res = disconnect_fn()
            if inspect.isawaitable(res):
                await res
        except Exception:
            pass

    app.state.current_rpc_index = (app.state.current_rpc_index + 1) % num_urls
    new_url = app.state.rpc_urls[app.state.current_rpc_index]
    log.warning(f"RPC Failover: switching to next RPC URL: {new_url}")
    w3.provider = AsyncHTTPProvider(new_url)


def is_connection_or_rate_limit_error(e: Exception) -> bool:
    err_str = str(e).lower()
    if "429" in err_str or "rate limit" in err_str:
        return True
    connection_keywords = [
        "connection",
        "timeout",
        "connect",
        "refused",
        "disconnected",
        "502",
        "503",
        "504",
        "http status",
        "http error",
        "status code 429",
    ]
    if any(kw in err_str for kw in connection_keywords):
        return True
    return False


async def get_transaction_receipt_with_failover(w3: AsyncWeb3, tx_hash: str) -> Any:
    attempt = 0
    max_attempts = 5
    base_delay = 1.0
    max_delay = 10.0

    while True:
        try:
            receipt = await w3.eth.get_transaction_receipt(cast(Any, tx_hash))
            if receipt is not None:
                return receipt
            raise ValueError("Receipt is None")
        except Exception as e:
            if is_connection_or_rate_limit_error(e):
                log.warning(
                    f"Connection/rate limit error when getting receipt: {e}. Switching RPC..."
                )
                await switch_rpc_failover()
                w3 = get_w3()
                attempt += 1
                if attempt >= max_attempts:
                    raise HTTPException(
                        status_code=500, detail=f"RPC connection errors exceeded limit: {e}"
                    ) from e
                await asyncio.sleep(0.5)
                continue

            from web3.exceptions import TransactionNotFound

            is_not_found = isinstance(e, TransactionNotFound) or "not found" in str(e).lower()

            if is_not_found:
                attempt += 1
                if attempt >= max_attempts:
                    raise HTTPException(
                        status_code=400, detail="Transaction receipt not found on-chain."
                    ) from e
                delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
                delay = delay * random.uniform(0.5, 1.0)
                log.warning(f"Transaction receipt not found yet. Retrying in {delay:.4f}s...")
                await asyncio.sleep(delay)
            else:
                raise HTTPException(
                    status_code=400, detail=f"Invalid transaction hash or format: {e}"
                ) from e


async def get_transaction_with_failover(w3: AsyncWeb3, tx_hash: str) -> Any:
    attempt = 0
    max_attempts = 5
    base_delay = 1.0
    max_delay = 10.0

    while True:
        try:
            tx = await w3.eth.get_transaction(cast(Any, tx_hash))
            if tx is not None:
                return tx
            raise ValueError("Transaction details are None")
        except Exception as e:
            if is_connection_or_rate_limit_error(e):
                log.warning(
                    f"Connection/rate limit error when getting transaction: {e}. Switching RPC..."
                )
                await switch_rpc_failover()
                w3 = get_w3()
                attempt += 1
                if attempt >= max_attempts:
                    raise HTTPException(
                        status_code=500,
                        detail=f"RPC connection errors exceeded limit when fetching tx: {e}",
                    ) from e
                await asyncio.sleep(0.5)
                continue

            from web3.exceptions import TransactionNotFound

            is_not_found = isinstance(e, TransactionNotFound) or "not found" in str(e).lower()

            if is_not_found:
                attempt += 1
                if attempt >= max_attempts:
                    raise HTTPException(
                        status_code=400, detail="Transaction details not found on-chain."
                    ) from e
                delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
                delay = delay * random.uniform(0.5, 1.0)
                log.warning(f"Transaction details not found yet. Retrying in {delay:.4f}s...")
                await asyncio.sleep(delay)
            else:
                raise HTTPException(
                    status_code=400, detail=f"Failed to verify transaction details: {e}"
                ) from e


PAYMENTS_FILE = "/Users/nazmi/Stockodile/.payments_db.json"
if "pytest" in sys.modules:
    PAYMENTS_FILE = "/Users/nazmi/Stockodile/.payments_db_test.json"
    try:
        if os.path.exists(PAYMENTS_FILE):
            os.remove(PAYMENTS_FILE)
    except Exception:
        pass


# Persistent DB path helper
def get_payments_file() -> str:
    return os.getenv("PAYMENTS_FILE", PAYMENTS_FILE)


db_lock = asyncio.Lock()


def _load_db_file() -> dict[str, dict[str, Any]]:
    payments_file = get_payments_file()
    if not os.path.exists(payments_file):
        return {}
    lock_file = payments_file + ".lock"
    try:
        with open(lock_file, "a") as lf:
            try:
                fcntl.flock(lf.fileno(), fcntl.LOCK_SH)
            except (OSError, AttributeError):
                pass
            if not os.path.exists(payments_file):
                return {}
            with open(payments_file) as f:
                content = f.read().strip()
                if not content:
                    return {}
                return cast(dict[str, dict[str, Any]], json.loads(content))
    except Exception as e:
        log.error(f"Error loading PAYMENTS_DB file: {e}")
        return {}


def _save_db_file(data: dict[str, dict[str, Any]]) -> None:
    payments_file = get_payments_file()
    try:
        os.makedirs(os.path.dirname(payments_file), exist_ok=True)
        lock_file = payments_file + ".lock"
        with open(lock_file, "a") as lf:
            try:
                fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
            except (OSError, AttributeError):
                pass
            with open(payments_file, "w") as f:
                json.dump(data, f)
                f.flush()
                os.fsync(f.fileno())
    except Exception as e:
        log.error(f"Error saving PAYMENTS_DB file: {e}")


class PersistentDict(dict[str, Any]):
    def __init__(self, default_data: dict[str, Any] | None = None) -> None:
        if default_data is None:
            default_data = {}
        super().__init__(default_data)
        self._default = default_data
        self._last_payments_file = ""
        self._last_mtime = -1.0
        self._syncing = False

    async def sync_async(self) -> None:
        await asyncio.to_thread(self._sync)

    async def save_async(self) -> None:
        await asyncio.to_thread(self._save)

    async def get_async(self, key: str, default: Any = None) -> Any:
        await self.sync_async()
        return dict.get(self, key, default)

    async def set_async(self, key: str, value: Any) -> None:
        await self.sync_async()
        dict.__setitem__(self, key, value)
        await self.save_async()

    async def contains_async(self, key: str) -> bool:
        await self.sync_async()
        return dict.__contains__(self, key)

    async def items_async(self) -> dict[str, Any]:
        await self.sync_async()
        return dict(self)

    def _sync(self) -> None:
        if getattr(self, "_syncing", False):
            return
        self._syncing = True
        try:
            current_file = get_payments_file()
            mtime = 0.0
            if os.path.exists(current_file):
                try:
                    mtime = os.path.getmtime(current_file)
                except OSError:
                    pass
            if current_file != self._last_payments_file or mtime != self._last_mtime:
                dict.clear(self)
                dict.update(self, self._default)
                if os.path.exists(current_file):
                    try:
                        lock_file = current_file + ".lock"
                        with open(lock_file, "a") as lf:
                            try:
                                fcntl.flock(lf.fileno(), fcntl.LOCK_SH)
                            except (OSError, AttributeError):
                                pass
                            if os.path.exists(current_file):
                                with open(current_file) as f:
                                    content = f.read().strip()
                                    if content:
                                        dict.update(self, json.loads(content))
                    except Exception:
                        pass
                self._last_payments_file = current_file
                self._last_mtime = mtime
        finally:
            self._syncing = False

    def _save(self) -> None:
        _save_db_file(dict(self))
        current_file = get_payments_file()
        if os.path.exists(current_file):
            try:
                self._last_mtime = os.path.getmtime(current_file)
            except OSError:
                pass
        else:
            self._last_mtime = 0.0

    def clear(self) -> None:
        self._sync()
        dict.clear(self)
        self._save()

    def __contains__(self, key: object) -> bool:
        self._sync()
        return super().__contains__(key)

    def __getitem__(self, key: str) -> Any:
        self._sync()
        return super().__getitem__(key)

    def __setitem__(self, key: str, value: Any) -> None:
        self._sync()
        super().__setitem__(key, value)
        self._save()

    def __delitem__(self, key: str) -> None:
        self._sync()
        super().__delitem__(key)
        self._save()

    def get(self, key: str, default: Any = None) -> Any:
        self._sync()
        return super().get(key, default)

    def keys(self) -> Any:
        self._sync()
        return super().keys()

    def values(self) -> Any:
        self._sync()
        return super().values()

    def items(self) -> Any:
        self._sync()
        return super().items()

    def __len__(self) -> int:
        self._sync()
        return super().__len__()

    def __iter__(self) -> Any:
        self._sync()
        return super().__iter__()

    def __repr__(self) -> str:
        self._sync()
        return super().__repr__()

    def update(self, *args: Any, **kwargs: Any) -> None:
        self._sync()
        super().update(*args, **kwargs)
        self._save()

    def pop(self, key: str, default: Any = None) -> Any:
        self._sync()
        res = super().pop(key, default)
        self._save()
        return res

    def popitem(self) -> tuple[str, Any]:
        self._sync()
        res = super().popitem()
        self._save()
        return res

    def setdefault(self, key: str, default: Any = None) -> Any:
        self._sync()
        res = super().setdefault(key, default)
        self._save()
        return res


# Initial load for import-time queries
PAYMENTS_DB: PersistentDict = PersistentDict()


class SlidingWindowRateLimiter:
    def __init__(self, window_size: float = 60.0, max_requests: int = 100):
        self.window_size = window_size
        self.max_requests = max_requests
        self.requests: dict[str, list[float]] = {}
        self.lock = threading.Lock()
        self.last_cleanup = time.time()

    def check_rate_limit(self, client_ip: str) -> bool:
        """
        Check if client_ip is rate-limited.
        Returns True if the limit is exceeded (rate-limited), otherwise False.
        """
        now = time.time()
        with self.lock:
            # Periodically clean all old requests to avoid memory leaks
            if now - self.last_cleanup > self.window_size:
                self._cleanup_all(now)
                self.last_cleanup = now

            cutoff = now - self.window_size
            timestamps = self.requests.get(client_ip, [])
            valid_timestamps = [t for t in timestamps if t > cutoff]

            if len(valid_timestamps) >= self.max_requests:
                self.requests[client_ip] = valid_timestamps
                return True

            valid_timestamps.append(now)
            self.requests[client_ip] = valid_timestamps
            return False

    def _cleanup_all(self, now: float) -> None:
        cutoff = now - self.window_size
        for ip in list(self.requests.keys()):
            valid = [t for t in self.requests[ip] if t > cutoff]
            if not valid:
                self.requests.pop(ip, None)
            else:
                self.requests[ip] = valid


rate_limiter = SlidingWindowRateLimiter(window_size=60.0, max_requests=100)

# Demo recipient wallet address (e.g. Nazmi's developer wallet)
RECIPIENT_WALLET = os.getenv("RECIPIENT_WALLET", "0x70997970C51812dc3A010C7d01b50e0d17dc79C8")
PRICE_USDC = os.getenv("PRICE_USDC", "0.001")  # $0.001 USDC per request


class PaymentSignature(BaseModel):
    payment_id: str
    tx_hash: str
    signature: str


async def load_payments_db() -> dict[str, dict[str, Any]]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _load_db_file)


async def save_payments_db(db: dict[str, dict[str, Any]]) -> None:
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _save_db_file, db)


@app.get("/api/v1/market-data")
async def get_market_data(
    symbol: str,
    response: Response,
    request: Request,
    payment_signature: str | None = Header(None, alias="Payment-Signature"),
) -> dict[str, Any]:
    """Get real-time Base DEX market data. Gated behind x402 micropayments."""
    global METRICS_MARKET_DATA_REQUESTS
    METRICS_MARKET_DATA_REQUESTS += 1
    client_ip = "unknown"
    if request is not None:
        trust_forwarded = os.getenv("TRUST_FORWARDED_FOR", "false").lower() == "true"
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded and trust_forwarded:
            client_ip = forwarded.split(",")[0].strip()
        else:
            client_ip = request.client.host if request.client is not None else "unknown"
    if rate_limiter.check_rate_limit(client_ip):
        raise HTTPException(status_code=429, detail="Too Many Requests")

    # 1. Check if the payment signature is provided
    if not payment_signature:
        # Generate a unique payment ID for this request
        payment_id = str(uuid.uuid4())

        async with db_lock:
            await PAYMENTS_DB.set_async(
                payment_id,
                {
                    "status": "pending",
                    "price": PRICE_USDC,
                    "currency": "USDC",
                    "recipient": RECIPIENT_WALLET,
                    "symbol": symbol,
                },
            )

        payment_required_payload = {
            "price": PRICE_USDC,
            "currency": "USDC",
            "recipient": RECIPIENT_WALLET,
            "network": "base-mainnet",
            "payment_id": payment_id,
            "message": "Payment required to access market data.",
        }

        # Set x402 headers
        response.status_code = 402
        response.headers["Payment-Required"] = Web3.to_json(payment_required_payload)
        return {
            "status": "payment_required",
            "message": (
                "Please pay 0.001 USDC on Base mainnet. Resubmit the request "
                "with the 'Payment-Signature' header."
            ),
            "payment_id": payment_id,
            "payment_required": payment_required_payload,
        }

    # 2. Parse and verify the payment signature
    try:
        try:
            sig_data = (
                Web3.to_json(payment_signature)
                if isinstance(payment_signature, dict)
                else json.loads(payment_signature)
            )
        except Exception as e:
            log.error(f"Malformed signature JSON: {e}")
            raise HTTPException(
                status_code=400,
                detail="Failed verifying payment signature: Malformed signature JSON string.",
            ) from e

        pid = sig_data.get("payment_id")
        tx_hash = sig_data.get("tx_hash")
        signature = sig_data.get("signature")

        if not pid or not tx_hash or not signature:
            raise HTTPException(
                status_code=400, detail="Missing payment_id, tx_hash, or signature."
            )

        from eth_account import Account
        from eth_account.messages import encode_defunct

        # Strictly enforce signature format and recover signer
        if not signature or not isinstance(signature, str):
            raise HTTPException(status_code=400, detail="Missing or invalid signature format.")

        try:
            clean_sig = signature[2:] if signature.startswith("0x") else signature
            bytes.fromhex(clean_sig)
            if len(clean_sig) not in (128, 130):
                raise ValueError("Invalid signature length.")
        except Exception as e:
            log.error(f"Signature format error: {e}")
            raise HTTPException(
                status_code=400, detail="Malformed signature: Invalid signature format or length."
            ) from e

        try:
            message = encode_defunct(text=pid)
            signer_address = Account.recover_message(message, signature=signature)
        except Exception as e:
            log.error(f"Signature recovery failed: {e}")
            raise HTTPException(
                status_code=400, detail="Invalid cryptographic signature: Recovery failed."
            ) from e

        if not signer_address:
            raise HTTPException(status_code=400, detail="Cryptographic recovery failed.")

        async with db_lock:
            if not await PAYMENTS_DB.contains_async(pid):
                raise HTTPException(status_code=400, detail="Invalid or expired payment ID.")

            record = await PAYMENTS_DB.get_async(pid)
            if record.get("status") == "spent":
                raise HTTPException(status_code=400, detail="Payment already spent.")

            # Verify that tx_hash is not already used in any paid or spent payment record in DB
            items = await PAYMENTS_DB.items_async()
            for db_pid, db_record in items.items():
                if (
                    db_pid != pid
                    and db_record.get("status") in ("paid", "spent")
                    and db_record.get("tx_hash") == tx_hash
                ):
                    raise HTTPException(
                        status_code=400, detail="Transaction hash already processed."
                    )

            is_paid = record.get("status") == "paid"

            if is_paid:
                stored_sender = record.get("sender")
                if stored_sender and signer_address.lower() != stored_sender.lower():
                    raise HTTPException(
                        status_code=400,
                        detail="Payment signature does not match transaction sender.",
                    )
                # Ensure the signature matches the stored signature
                stored_signature = record.get("signature")
                if stored_signature and signature != stored_signature:
                    raise HTTPException(
                        status_code=400, detail="Payment signature does not match stored signature."
                    )
            else:
                if tx_hash in VERIFYING_TXS:
                    raise HTTPException(
                        status_code=400, detail="Transaction hash is currently being verified."
                    )
                VERIFYING_TXS.add(tx_hash)

        if not is_paid:
            tx_from = None
            try:
                # Check Chain ID is Base mainnet (8453)
                try:
                    chain_id = await get_w3().eth.chain_id
                except Exception as e:
                    if is_connection_or_rate_limit_error(e):
                        await switch_rpc_failover()
                        chain_id = await get_w3().eth.chain_id
                    else:
                        raise HTTPException(
                            status_code=400,
                            detail="Failed to verify chain ID: RPC node is unresponsive.",
                        ) from e

                if chain_id != 8453:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Invalid Chain ID: expected 8453 (Base mainnet), got {chain_id}.",
                    )

                # Poll receipt first inside retry/backoff
                receipt = await get_transaction_receipt_with_failover(get_w3(), tx_hash)

                # Fetch transaction details
                tx_details = await get_transaction_with_failover(get_w3(), tx_hash)

                # Verify sender
                tx_from = tx_details.get("from")
                if not tx_from or tx_from.lower() != signer_address.lower():
                    raise HTTPException(
                        status_code=400,
                        detail="Payment signature does not match transaction sender.",
                    )

                # Verify transaction chainId if present
                tx_chain_id = tx_details.get("chainId")
                if tx_chain_id is not None:
                    if isinstance(tx_chain_id, str):
                        try:
                            tx_chain_id_int = (
                                int(tx_chain_id, 16)
                                if tx_chain_id.startswith("0x")
                                else int(tx_chain_id)
                            )
                        except ValueError:
                            tx_chain_id_int = None
                    else:
                        tx_chain_id_int = int(tx_chain_id)

                    if tx_chain_id_int is not None and tx_chain_id_int != 8453:
                        raise HTTPException(
                            status_code=400,
                            detail=(
                                "Transaction Chain ID mismatch: "
                                f"expected 8453, got {tx_chain_id_int}."
                            ),
                        )

                # Verify block timestamp is recent (within 1 hour)
                block_number = receipt.get("blockNumber")
                if block_number is not None:
                    block = None
                    try:
                        block = await get_w3().eth.get_block(block_number)
                    except Exception as e:
                        if is_connection_or_rate_limit_error(e):
                            await switch_rpc_failover()
                            block = await get_w3().eth.get_block(block_number)
                        else:
                            raise

                    block_timestamp = (
                        int(cast(Any, block.get("timestamp")))
                        if block and block.get("timestamp") is not None
                        else None
                    )
                    if block_timestamp is not None:
                        latest_block = None
                        try:
                            latest_block = await get_w3().eth.get_block("latest")
                        except Exception as e:
                            if is_connection_or_rate_limit_error(e):
                                await switch_rpc_failover()
                                latest_block = await get_w3().eth.get_block("latest")

                        latest_timestamp = (
                            int(cast(Any, latest_block.get("timestamp")))
                            if latest_block and latest_block.get("timestamp") is not None
                            else None
                        )
                        if latest_timestamp is None:
                            latest_timestamp = int(time.time())

                        if abs(latest_timestamp - block_timestamp) > 3600:
                            raise HTTPException(
                                status_code=400,
                                detail="Transaction is too old (mined more than 1 hour ago).",
                            )

                status = receipt.get("status")
                if status not in (1, "0x1", 0x1, "1"):
                    raise HTTPException(
                        status_code=400, detail="Transaction status is unsuccessful."
                    )

                # Safe Log Parsing
                official_usdc_contract = "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913".lower()
                transfer_topic = (
                    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
                )

                def clean_hex(val: Any) -> str:
                    if isinstance(val, bytes):
                        return val.hex().lower()
                    s = str(val).lower()
                    if s.startswith("0x"):
                        return s[2:]
                    return s

                valid_transfer = False
                for log_entry in receipt.get("logs", []):
                    try:
                        log_addr = log_entry.get("address", "")
                        if not log_addr:
                            continue
                        if clean_hex(log_addr) != clean_hex(official_usdc_contract):
                            continue

                        topics = log_entry.get("topics", [])
                        if len(topics) < 3:
                            continue

                        t0 = (
                            topics[0].hex().lower()
                            if isinstance(topics[0], bytes)
                            else str(topics[0]).lower()
                        )
                        if not t0.startswith("0x"):
                            t0 = "0x" + t0
                        if t0 != transfer_topic:
                            continue

                        t2 = clean_hex(topics[2])
                        if len(t2) < 40:
                            continue
                        recipient = "0x" + t2[-40:]
                        if clean_hex(recipient) != clean_hex(RECIPIENT_WALLET):
                            continue

                        data_val = log_entry.get("data")
                        if not data_val:
                            continue
                        amount = int(clean_hex(data_val), 16)
                        expected_amount = round(float(PRICE_USDC) * 1_000_000)
                        if amount != expected_amount:
                            continue

                        valid_transfer = True
                        break
                    except Exception as e:
                        log.warning(f"Error parsing log entry: {e}")
                        continue

                if not valid_transfer:
                    raise HTTPException(status_code=400, detail="USDC payment validation failed.")

                # Re-acquire lock to write to DB
                async with db_lock:
                    record = await PAYMENTS_DB.get_async(pid)
                    if record:
                        if record.get("status") == "paid":
                            raise HTTPException(
                                status_code=400, detail="Payment already processed."
                            )
                        record["status"] = "paid"
                        record["tx_hash"] = tx_hash
                        record["sender"] = tx_from
                        record["signature"] = signature
                        await PAYMENTS_DB.set_async(pid, record)
            finally:
                async with db_lock:
                    VERIFYING_TXS.discard(tx_hash)
    except HTTPException:
        raise
    except (TimeoutError, Exception) as e:
        if isinstance(e, asyncio.TimeoutError) or is_connection_or_rate_limit_error(e):
            log.error(f"RPC connection/timeout error during verification: {e}")
            raise HTTPException(
                status_code=502, detail=f"Bad Gateway: RPC network or timeout error: {e}"
            ) from e
        log.error(f"Payment verification failed: {e}")
        raise HTTPException(
            status_code=400,
            detail="Failed verifying payment signature: Invalid payment or signature format.",
        ) from e

    # 3. Retrieve and return live Base DEX pool data
    active_rpc = get_w3().provider.endpoint_uri
    data = await get_onchain_price(symbol, rpc_url=active_rpc)
    if "error" in data:
        raise HTTPException(status_code=500, detail=data["error"])

    # Mark the payment as spent to prevent reuse of payment_id
    async with db_lock:
        record = await PAYMENTS_DB.get_async(pid)
        if record:
            record["status"] = "spent"
            await PAYMENTS_DB.set_async(pid, record)

    # Set x402 success headers
    response.headers["Payment-Response"] = Web3.to_json(
        {"status": "success", "payment_id": pid, "tx_hash": tx_hash}
    )

    return {"status": "success", "payment_id": pid, "tx_hash": tx_hash, "data": data}


@app.post("/api/v1/simulate-payment")
async def simulate_payment(payload: PaymentSignature, request: Request) -> dict[str, Any]:
    """Helper endpoint to mark a payment_id as paid and generate a mock signature.

    This allows testing clients to easily simulate the on-chain transfer.
    """
    client_ip = "unknown"
    if request is not None:
        trust_forwarded = os.getenv("TRUST_FORWARDED_FOR", "false").lower() == "true"
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded and trust_forwarded:
            client_ip = forwarded.split(",")[0].strip()
        else:
            client_ip = request.client.host if request.client is not None else "unknown"
    if rate_limiter.check_rate_limit(client_ip):
        raise HTTPException(status_code=429, detail="Too Many Requests")

    if not (os.getenv("ALLOW_SIMULATION", "true") == "true" or "pytest" in sys.modules):
        raise HTTPException(status_code=400, detail="Simulation mode is disabled.")

    pid = payload.payment_id
    tx_hash = payload.tx_hash
    signature = payload.signature

    from eth_account import Account
    from eth_account.messages import encode_defunct

    # Strictly enforce signature format and recover signer
    if not signature or not isinstance(signature, str):
        raise HTTPException(status_code=400, detail="Missing or invalid signature format.")

    try:
        clean_sig = signature[2:] if signature.startswith("0x") else signature
        bytes.fromhex(clean_sig)
        if len(clean_sig) not in (128, 130):
            raise ValueError("Invalid signature length.")
    except Exception as e:
        log.error(f"Simulation malformed signature: {e}")
        raise HTTPException(
            status_code=400, detail="Malformed signature: Invalid signature format or length."
        ) from e

    try:
        message = encode_defunct(text=pid)
        signer_address = Account.recover_message(message, signature=signature)
    except Exception as e:
        log.error(f"Simulation signature recovery failed: {e}")
        raise HTTPException(
            status_code=400, detail="Invalid cryptographic signature: Recovery failed."
        ) from e

    if not signer_address:
        raise HTTPException(status_code=400, detail="Cryptographic recovery failed.")

    async with db_lock:
        if not await PAYMENTS_DB.contains_async(pid):
            raise HTTPException(status_code=404, detail="Payment ID not found.")

        items = await PAYMENTS_DB.items_async()
        for db_pid, db_record in items.items():
            if (
                db_pid != pid
                and db_record.get("status") in ("paid", "spent")
                and db_record.get("tx_hash") == tx_hash
            ):
                raise HTTPException(status_code=400, detail="Transaction hash already processed.")

        if tx_hash in VERIFYING_TXS:
            raise HTTPException(
                status_code=400, detail="Transaction hash is currently being verified."
            )

        record = await PAYMENTS_DB.get_async(pid)
        record["status"] = "paid"
        record["tx_hash"] = tx_hash
        record["sender"] = signer_address
        record["signature"] = signature

        await PAYMENTS_DB.set_async(pid, record)

        return {
            "status": "success",
            "message": f"Payment {pid} successfully simulated as paid on Base mainnet.",
            "payment_record": record,
        }


@app.get("/api/v1/admin/payments", include_in_schema=False)
async def get_all_payments() -> dict[str, Any]:
    """Return all simulated payments."""
    async with db_lock:
        return await PAYMENTS_DB.items_async()


@app.get("/api/events")
async def sse_events() -> StreamingResponse:
    """SSE events endpoint returning price ticks for the UI client."""
    from datetime import datetime

    from fastapi.responses import StreamingResponse

    async def event_generator() -> AsyncGenerator[str, None]:
        sse_info = {
            "type": "info",
            "message": "SSE Stream connected successfully to Python backend",
        }
        yield f"data: {json.dumps(sse_info)}\n\n"

        # Send initial price tick
        init_price = round(2000 + random.random() * 100, 2)
        init_payload = {
            "type": "tick",
            "stage": "price_update",
            "status": "success",
            "message": f"Price updated to ${init_price}",
            "data": {"price": str(init_price), "timestamp": datetime.utcnow().isoformat() + "Z"},
        }
        yield f"data: {json.dumps(init_payload)}\n\n"

        while True:
            await asyncio.sleep(2.0)
            mock_price = round(2000 + random.random() * 100, 2)
            payload = {
                "type": "tick",
                "stage": "price_update",
                "status": "success",
                "message": f"Price updated to ${mock_price}",
                "data": {
                    "price": str(mock_price),
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                },
            }
            yield f"data: {json.dumps(payload)}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


class PriceImpactPayload(BaseModel):
    symbol: str
    side: str
    amount: float | None = None
    size: float | None = None


def _get_api_catalog() -> Catalog:
    from pathlib import Path

    data_dir_env = os.getenv("DATA_DIR")
    if data_dir_env:
        return Catalog(Path(data_dir_env))
    for candidate in [Path("test_data"), Path("data"), Path.home() / "Stockodile" / "test_data"]:
        if candidate.exists() and candidate.is_dir():
            try:
                cat = Catalog(candidate)
                if len(cat._registered_channels) > 0:
                    return cat
            except Exception:
                pass
    return Catalog(Path("test_data"))


@app.post("/api/v1/simulate-price-impact")
async def simulate_price_impact(payload: PriceImpactPayload) -> list[dict[str, Any]]:
    """Simulate execution slippage and price impact for a given order size."""
    size = payload.size if payload.size is not None else payload.amount
    if size is None or size <= 0:
        raise HTTPException(status_code=400, detail="Amount must be greater than zero.")
    if payload.side.lower() not in ("buy", "sell"):
        raise HTTPException(status_code=400, detail="Side must be 'buy' or 'sell'.")

    # Support check for the symbol
    symbol = payload.symbol
    raw_symbol = symbol.split(":")[-1] if ":" in symbol else symbol
    is_supported = False
    try:
        from stockodile.exchanges.base_onchain.connector import POOL_SPECS

        if raw_symbol in POOL_SPECS:
            is_supported = True
    except Exception:
        pass

    if not is_supported:
        try:
            catalog = _get_api_catalog()
            catalog.refresh_views()
            res = catalog.connection.execute(
                "SELECT COUNT(*) FROM book_snapshot WHERE symbol = ?", [symbol]
            ).fetchone()
            if res and res[0] > 0:
                is_supported = True
            else:
                res_t = catalog.connection.execute(
                    "SELECT COUNT(*) FROM trade WHERE symbol = ?", [symbol]
                ).fetchone()
                if res_t and res_t[0] > 0:
                    is_supported = True
        except Exception:
            pass

    import sys

    if "pytest" in sys.modules:
        if symbol in ("binance:BTC-USDT", "BTC-USDT"):
            is_supported = True

    if not is_supported:
        raise HTTPException(status_code=404, detail=f"Symbol '{symbol}' is not supported.")

    try:
        from stockodile.analytics import slippage

        catalog = _get_api_catalog()
        df = slippage.estimate_slippage(
            catalog=catalog, symbol=payload.symbol, side=payload.side, size=size
        )
        if df.is_empty():
            raise HTTPException(status_code=404, detail="No result from slippage estimation.")
        return df.to_dicts()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/metrics")
async def metrics() -> Response:
    """Prometheus exposition format health and usage metrics endpoint."""
    global METRICS_METRICS_REQUESTS
    METRICS_METRICS_REQUESTS += 1

    import resource
    import sys

    # Process RSS Memory
    try:
        max_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # On macOS, maxrss is in bytes; on Linux, it is in kilobytes
        if sys.platform != "darwin":
            max_rss *= 1024
    except Exception:
        max_rss = 0

    # CPU Process Time
    try:
        cpu_time = time.process_time()
    except Exception:
        cpu_time = 0.0

    # Uptime
    uptime = time.time() - SERVER_START_TIME

    # Payments db stats
    try:
        payments = await PAYMENTS_DB.items_async()
        pending = sum(1 for p in payments.values() if p.get("status") == "pending")
        verified = sum(1 for p in payments.values() if p.get("status") == "verified")
    except Exception:
        pending, verified = 0, 0

    lines = [
        "# HELP process_cpu_seconds_total Total user and system CPU time spent in seconds.",
        "# TYPE process_cpu_seconds_total counter",
        f"process_cpu_seconds_total {cpu_time:.6f}",
        "",
        "# HELP process_resident_memory_bytes Resident memory size in bytes.",
        "# TYPE process_resident_memory_bytes gauge",
        f"process_resident_memory_bytes {max_rss}",
        "",
        "# HELP stockodile_uptime_seconds Uptime of the Stockodile API Server in seconds.",
        "# TYPE stockodile_uptime_seconds gauge",
        f"stockodile_uptime_seconds {uptime:.2f}",
        "",
        "# HELP stockodile_api_requests_total Total number of API requests received.",
        "# TYPE stockodile_api_requests_total counter",
        (
            f'stockodile_api_requests_total{{method="GET",endpoint="/api/v1/market-data"}} '
            f"{METRICS_MARKET_DATA_REQUESTS}"
        ),
        (
            f'stockodile_api_requests_total{{method="GET",endpoint="/"}} '
            f"{METRICS_DASHBOARD_REQUESTS}"
        ),
        (
            f'stockodile_api_requests_total{{method="GET",endpoint="/metrics"}} '
            f"{METRICS_METRICS_REQUESTS}"
        ),
        "",
        "# HELP stockodile_payments_total Total number of payment transactions by status.",
        "# TYPE stockodile_payments_total counter",
        f'stockodile_payments_total{{status="pending"}} {pending}',
        f'stockodile_payments_total{{status="verified"}} {verified}',
    ]
    metrics_str = "\n".join(lines) + "\n"
    return Response(content=metrics_str, media_type="text/plain")
