from __future__ import annotations

import asyncio
import hashlib
import io
import logging
import os
import re
import sys
import zipfile
from collections.abc import AsyncIterator, Iterable
from datetime import UTC, datetime
from typing import Any

import aiohttp
import polars as pl
from yarl import URL

from stockodile.providers.base import Provider
from stockodile.reference.registry import Instrument, InstrumentRegistry
from stockodile.schema.enums import SecurityType
from stockodile.schema.records import OHLCV, IndexValue, Record
from stockodile.sink.base import Sink
from stockodile.util.time import now_ns

log = logging.getLogger(__name__)


class _BufferedResponse:
    """aiohttp ClientResponse wrapper that re-serves an already-read body.

    Used when we peek at text/html for PoW markers but still need callers to
    read the full body (e.g. CAPTCHA submit returning ``"1"``).
    """

    def __init__(self, resp: aiohttp.ClientResponse, body: str) -> None:
        self._resp = resp
        self._body = body
        self.status = resp.status
        self.headers = resp.headers
        self.url = resp.url
        self.reason = resp.reason
        self.ok = resp.ok

    async def text(self, *args: Any, **kwargs: Any) -> str:
        return self._body

    async def read(self) -> bytes:
        return self._body.encode("utf-8")

    async def json(self, *args: Any, **kwargs: Any) -> Any:
        import json

        return json.loads(self._body)

    def close(self) -> None:
        self._resp.close()

    def release(self) -> None:
        if hasattr(self._resp, "release"):
            self._resp.release()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._resp, name)


def _mine_pow(c: str, target_prefix: str) -> int:
    n = 0
    while True:
        data = (c + str(n)).encode("utf-8")
        h = hashlib.sha256(data).hexdigest()
        if h.startswith(target_prefix):
            return n
        n += 1


class StooqProvider(Provider):
    name = "stooq"
    ws_url = ""
    rest_url = "https://stooq.com"

    def __init__(
        self,
        symbols: list[str],
        channels: list[str],
        out: Sink,
        registry: InstrumentRegistry,
        zip_path: str | None = None,
        captcha_api_key: str | None = None,
        captcha_service: str = "2captcha",
        domain: str = "stooq.com",
    ) -> None:
        super().__init__(symbols, channels, out, registry)
        self.zip_path = zip_path or os.environ.get("STOOQ_ZIP_PATH")
        self.captcha_api_key = captcha_api_key or os.environ.get("STOOQ_CAPTCHA_API_KEY")
        self.captcha_service = captcha_service
        self.domain = domain
        self.session: aiohttp.ClientSession | None = None
        self._zip_index: dict[str, dict[str, str]] = {}

    async def list_instruments(self) -> list[Instrument]:
        insts = []
        for sym in self.symbols:
            # Check if symbol is an index
            sec_type = SecurityType.UNKNOWN if sym.startswith("^") else SecurityType.CS
            insts.append(
                Instrument(
                    symbol=sym,
                    provider=self.name,
                    symbol_raw=sym,
                    security_type=sec_type,
                )
            )
        return insts

    async def _subscribe(self, transport: Any) -> None:
        # Pull-only source has no subscription over WS transport
        pass

    def normalize(self, msg: object, local_ts: int) -> Iterable[Record]:
        # Pull-only source does not use websocket message normalization
        return ()

    async def backfill(
        self,
        channel: str,
        symbol: str,
        start_ns: int,
        end_ns: int,
    ) -> AsyncIterator[Record]:
        zip_file_path = self._find_zip_file(symbol)
        if zip_file_path:
            log.info("Backfilling symbol %s from local ZIP file: %s", symbol, zip_file_path)
            records = await self._read_from_zip(zip_file_path, symbol, start_ns, end_ns)
            for rec in records:
                yield rec
            return

        log.info("Backfilling symbol %s from Stooq REST API", symbol)
        csv_data = await self._download_csv(symbol)
        if csv_data:
            records = self._parse_csv(csv_data, symbol, start_ns, end_ns)
            for rec in records:
                yield rec

    async def close(self) -> None:
        if self.session is not None:
            await self.session.close()
            self.session = None

    def _find_zip_file(self, symbol: str) -> str | None:
        if not self.zip_path:
            return None
        if os.path.isfile(self.zip_path):
            return self.zip_path
        if os.path.isdir(self.zip_path):
            # Try specific zip names based on symbol type
            if symbol.startswith("^") or any(suffix in symbol.lower() for suffix in [".b", ".fx"]):
                world_path = os.path.join(self.zip_path, "d_world_txt.zip")
                if os.path.isfile(world_path):
                    return world_path
            us_path = os.path.join(self.zip_path, "d_us_txt.zip")
            if os.path.isfile(us_path):
                return us_path
            # Fall back to checking any ZIP in the directory
            for f in os.listdir(self.zip_path):
                if f.endswith(".zip"):
                    return os.path.join(self.zip_path, f)
        return None

    async def _get_zip_index(self, zip_file_path: str) -> dict[str, str]:
        if zip_file_path not in self._zip_index:

            def _build_index() -> dict[str, str]:
                idx = {}
                with zipfile.ZipFile(zip_file_path) as z:
                    for name in z.namelist():
                        base = name.split("/")[-1].lower()
                        idx[base] = name
                return idx

            self._zip_index[zip_file_path] = await asyncio.to_thread(_build_index)
        return self._zip_index[zip_file_path]

    async def _read_from_zip(
        self,
        zip_file_path: str,
        symbol: str,
        start_ns: int,
        end_ns: int,
    ) -> list[Record]:
        try:
            index = await self._get_zip_index(zip_file_path)
            clean_sym = symbol.lower().replace("^", "")
            target_name = f"{clean_sym}.txt"

            matching_name = index.get(target_name) or index.get(f"^{target_name}")
            if not matching_name:
                log.warning("Symbol %s not found in ZIP file: %s", symbol, zip_file_path)
                return []

            def _read_data() -> bytes:
                with zipfile.ZipFile(zip_file_path) as z:
                    with z.open(matching_name) as f:
                        return f.read()

            csv_data = await asyncio.to_thread(_read_data)
            return self._parse_csv(csv_data, symbol, start_ns, end_ns)
        except Exception as e:
            log.error("Failed to read symbol %s from ZIP file %s: %s", symbol, zip_file_path, e)
            return []

    async def _download_csv(self, symbol: str) -> bytes | None:
        try:
            await self.ensure_authenticated(symbol)
        except Exception as e:
            log.warning(
                "Authentication failed during CSV download setup: %s. Attempting anyway.",
                e,
            )

        token = ""
        if self.session is not None:
            from urllib.parse import unquote

            cookies = self.session.cookie_jar.filter_cookies(URL(f"https://{self.domain}"))
            cookie_user = cookies.get("cookie_user")
            if cookie_user:
                val = unquote(cookie_user.value)
                m = re.search(r"\?([^|]+)\|", val)
                if m:
                    token = m.group(1)

        if token:
            csv_url = f"https://{self.domain}/q/d/l/?s={symbol.lower()}&i=d&apikey={token}"
        else:
            csv_url = f"https://{self.domain}/q/d/l/?s={symbol.lower()}&i=d"

        referer = f"https://{self.domain}/q/d/?s={symbol.lower()}&get_apikey"
        headers = {"Referer": referer}

        resp = await self._request("GET", csv_url, headers=headers)
        if resp.status != 200:
            log.error("Failed to download Stooq CSV for %s (HTTP %d)", symbol, resp.status)
            return None

        data = await resp.read()
        if b"Access denied" in data or b"Odmowa" in data or b"verify your browser" in data:
            log.error(
                "Access denied downloading Stooq CSV for %s. Response starts with: %r",
                symbol,
                data[:100],
            )
            return None

        return data

    async def ensure_authenticated(self, symbol: str | None = None) -> None:
        if symbol:
            url = f"https://{self.domain}/q/d/?s={symbol.lower()}&get_apikey"
        else:
            url = f"https://{self.domain}/db/h/"

        # Fetch page to handle PoW/initialize session
        (await self._request("GET", url)).close()

        # Download CAPTCHA image
        img_url = f"https://{self.domain}/q/l/s/i/"
        img_resp = await self._request("GET", img_url)
        if img_resp.status != 200:
            raise RuntimeError(f"Failed to fetch CAPTCHA image (HTTP {img_resp.status})")
        img_bytes = await img_resp.read()

        code = ""
        if self.captcha_api_key:
            try:
                if self.captcha_service == "2captcha":
                    code = await self.solve_captcha_2captcha(self.captcha_api_key, img_bytes)
                elif self.captcha_service == "anticaptcha":
                    code = await self.solve_captcha_anticaptcha(self.captcha_api_key, img_bytes)
                else:
                    raise ValueError(f"Unsupported CAPTCHA service: {self.captcha_service}")
            except Exception as e:
                log.warning(
                    "CAPTCHA API solving failed: %s. Falling back to manual console entry.",
                    e,
                )

        if not code:
            code = await self._solve_captcha_manual(img_bytes)

        # Submit code
        submit_url = f"https://{self.domain}/q/l/s/?t={code.strip().lower()}"
        submit_resp = await self._request("GET", submit_url)
        submit_res = await submit_resp.text()
        if submit_res != "1":
            raise RuntimeError(
                f"CAPTCHA verification failed. Entered code: {code}, response: {submit_res}"
            )

        # Propagate cookies by requesting the target page again
        (await self._request("GET", url)).close()

    async def _solve_captcha_manual(self, img_bytes: bytes) -> str:
        import tempfile

        if not sys.stdin.isatty():
            raise RuntimeError(
                "Non-interactive terminal detected. Cannot prompt for manual CAPTCHA entry."
            )

        tmp_file = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
        captcha_path = tmp_file.name
        try:

            def _write_file() -> None:
                tmp_file.write(img_bytes)
                tmp_file.close()

            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _write_file)

            print(f"\n[Stooq] Visual CAPTCHA saved to {captcha_path}")
            print("Please view the CAPTCHA image and enter the 4-character code below.")
            sys.stdout.write("Enter CAPTCHA code: ")
            sys.stdout.flush()

            code = await loop.run_in_executor(None, sys.stdin.readline)
            return str(code).strip()
        except Exception as e:
            log.error("Failed manual CAPTCHA prompt: %s", e)
            raise
        finally:
            if os.path.exists(captcha_path):  # noqa: ASYNC240
                try:
                    os.unlink(captcha_path)
                except Exception:
                    pass

    async def solve_captcha_2captcha(self, api_key: str, img_bytes: bytes) -> str:
        import base64

        img_b64 = base64.b64encode(img_bytes).decode("utf-8")
        # Form-encoded body (not JSON); https to avoid leaking API key
        url_in = "https://2captcha.com/in.php"
        data = {
            "key": api_key,
            "method": "base64",
            "body": img_b64,
            "json": 1,
        }
        session = self._get_session()
        timeout = aiohttp.ClientTimeout(total=15.0)
        try:
            async with session.post(url_in, data=data, timeout=timeout) as resp:
                if resp.status != 200:
                    body_text = await resp.text()
                    raise RuntimeError(
                        f"2captcha error: HTTP status {resp.status}, response: {body_text[:100]}"
                    )
                try:
                    res_json = await resp.json()
                except aiohttp.ContentTypeError as err:
                    body_text = await resp.text()
                    raise RuntimeError(
                        f"2captcha returned non-JSON content: {body_text[:100]}"
                    ) from err
                if res_json.get("status") != 1:
                    raise RuntimeError(f"2captcha error: {res_json.get('request')}")
                task_id = res_json["request"]
        except Exception as e:
            if not isinstance(e, RuntimeError):
                raise RuntimeError(f"2captcha communication failed: {e}") from e
            raise

        url_res = "https://2captcha.com/res.php"
        for _ in range(30):
            await asyncio.sleep(5)
            params = {"key": api_key, "action": "get", "id": task_id, "json": 1}
            try:
                async with session.get(url_res, params=params, timeout=timeout) as res_resp:
                    if res_resp.status != 200:
                        continue
                    try:
                        res_json = await res_resp.json()
                    except aiohttp.ContentTypeError:
                        continue
                    if res_json.get("status") == 1:
                        return str(res_json["request"])
                    if res_json.get("request") != "CAPCHA_NOT_READY":
                        raise RuntimeError(f"2captcha error: {res_json.get('request')}")
            except Exception as e:
                log.warning("2captcha polling request failed: %s", e)
                continue
        raise RuntimeError("2captcha solution timeout")

    async def solve_captcha_anticaptcha(self, api_key: str, img_bytes: bytes) -> str:
        import base64

        img_b64 = base64.b64encode(img_bytes).decode("utf-8")
        url_in = "https://api.anti-captcha.com/createTask"
        data = {
            "clientKey": api_key,
            "task": {
                "type": "ImageToTextTask",
                "body": img_b64,
            },
        }
        session = self._get_session()
        timeout = aiohttp.ClientTimeout(total=15.0)
        try:
            async with session.post(url_in, json=data, timeout=timeout) as resp:
                if resp.status != 200:
                    body_text = await resp.text()
                    raise RuntimeError(
                        f"anticaptcha error: HTTP status {resp.status}, response: {body_text[:100]}"
                    )
                try:
                    res_json = await resp.json()
                except aiohttp.ContentTypeError as err:
                    body_text = await resp.text()
                    raise RuntimeError(
                        f"anticaptcha returned non-JSON content: {body_text[:100]}"
                    ) from err
                if res_json.get("errorId", 0) != 0:
                    raise RuntimeError(f"anticaptcha error: {res_json.get('errorDescription')}")
                task_id = res_json["taskId"]
        except Exception as e:
            if not isinstance(e, RuntimeError):
                raise RuntimeError(f"anticaptcha communication failed: {e}") from e
            raise

        url_res = "https://api.anti-captcha.com/getTaskResult"
        for _ in range(30):
            await asyncio.sleep(5)
            payload = {"clientKey": api_key, "taskId": task_id}
            try:
                async with session.post(url_res, json=payload, timeout=timeout) as res_resp:
                    if res_resp.status != 200:
                        continue
                    try:
                        res_json = await res_resp.json()
                    except aiohttp.ContentTypeError:
                        continue
                    if res_json.get("errorId", 0) != 0:
                        raise RuntimeError(f"anticaptcha error: {res_json.get('errorDescription')}")
                    if res_json.get("status") == "ready":
                        return str(res_json["solution"]["text"])
            except Exception as e:
                log.warning("anticaptcha polling request failed: %s", e)
                continue
        raise RuntimeError("anticaptcha solution timeout")

    async def _solve_pow(self, url: str, body: str, session: aiohttp.ClientSession) -> None:
        match = re.search(r'const c="([^"]+)"', body)
        if not match:
            return
        c = match.group(1)
        target_prefix = "0" * 4

        n = await asyncio.to_thread(_mine_pow, c, target_prefix)

        verify_url = str(URL(url).with_path("/__verify"))
        headers = {"Referer": url, "Content-Type": "application/x-www-form-urlencoded"}
        async with session.post(verify_url, data={"c": c, "n": str(n)}, headers=headers) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Proof-of-Work verification failed with status {resp.status}")
            await resp.text()

    def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "en-US,en;q=0.9",
            }
            timeout = aiohttp.ClientTimeout(total=60.0, connect=10.0, sock_read=30.0)
            self.session = aiohttp.ClientSession(headers=headers, timeout=timeout)
        return self.session

    async def _request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> aiohttp.ClientResponse | _BufferedResponse:
        session = self._get_session()
        for attempt in range(5):
            try:
                resp = await session.request(method, url, **kwargs)
                content_type = resp.headers.get("Content-Type", "").lower()
                if resp.status == 200 and "text/html" in content_type:
                    body = await resp.text()
                    if "verify your browser" in body or "const c=" in body:
                        resp.close()
                        await self._solve_pow(url, body, session)
                        continue
                    # Body already consumed — wrap so callers can re-read text/bytes
                    return _BufferedResponse(resp, body)
                return resp
            except aiohttp.ClientError as e:
                log.warning("Stooq request failed (attempt %d/5): %s", attempt + 1, e)
                if attempt == 4:
                    raise
                await asyncio.sleep(1.0 * (2**attempt))
        raise RuntimeError("Max Proof-of-Work solve attempts exceeded")

    def _parse_csv(
        self,
        csv_data: bytes,
        symbol: str,
        start_ns: int,
        end_ns: int,
    ) -> list[Record]:
        try:
            df = pl.read_csv(io.BytesIO(csv_data))
        except Exception as e:
            log.error("Failed to parse Stooq CSV data with Polars: %s", e)
            return []

        # Normalize bulk-ZIP headers (<DATE>) and REST headers (Date, Volume)
        rename_map: dict[str, str] = {}
        for col in df.columns:
            stripped = col.strip("<>").strip()
            key = stripped.upper()
            if key in ("DATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOL", "VOLUME", "TICKER", "PER"):
                if key == "VOLUME":
                    key = "VOL"
                rename_map[col] = key
            else:
                rename_map[col] = stripped
        df = df.rename(rename_map)

        required = ["DATE", "OPEN", "HIGH", "LOW", "CLOSE"]
        for col in required:
            if col not in df.columns:
                log.error("Stooq CSV is missing required column %r", col)
                return []

        records: list[Record] = []
        is_index = symbol.startswith("^")
        provider = self.name

        EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

        dates = df["DATE"].to_list()
        opens = df["OPEN"].to_list()
        highs = df["HIGH"].to_list()
        lows = df["LOW"].to_list()
        closes = df["CLOSE"].to_list()
        volumes = df["VOL"].to_list() if "VOL" in df.columns else [0.0] * len(df)

        local_ts = now_ns()

        for i in range(len(df)):
            date_val = str(dates[i]).strip()
            try:
                if "-" in date_val:
                    # REST CSV: yyyy-MM-dd
                    dt = datetime.strptime(date_val[:10], "%Y-%m-%d").replace(tzinfo=UTC)
                else:
                    # Bulk ZIP: yyyymmdd
                    dt = datetime.strptime(date_val[:8], "%Y%m%d").replace(tzinfo=UTC)
                source_ts = int((dt - EPOCH).total_seconds()) * 1_000_000_000
            except Exception:
                continue

            if not (start_ns <= source_ts <= end_ns):
                continue

            close_px = float(closes[i])

            if is_index:
                records.append(
                    IndexValue(
                        provider=provider,
                        symbol=symbol,
                        symbol_raw=symbol,
                        source_ts=source_ts,
                        local_ts=local_ts,
                        value=close_px,
                    )
                )
            else:
                records.append(
                    OHLCV(
                        provider=provider,
                        symbol=symbol,
                        symbol_raw=symbol,
                        source_ts=source_ts,
                        local_ts=local_ts,
                        interval="1d",
                        open=float(opens[i]),
                        high=float(highs[i]),
                        low=float(lows[i]),
                        close=close_px,
                        volume=float(volumes[i]),
                        vwap=None,
                        trade_count=None,
                    )
                )
        return records
