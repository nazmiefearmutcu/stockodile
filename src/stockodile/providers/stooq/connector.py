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
            records = self._read_from_zip(zip_file_path, symbol, start_ns, end_ns)
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

    def _read_from_zip(
        self,
        zip_file_path: str,
        symbol: str,
        start_ns: int,
        end_ns: int,
    ) -> list[Record]:
        try:
            with zipfile.ZipFile(zip_file_path) as z:
                target_name = f"{symbol.lower()}.txt"
                names = z.namelist()
                matching = []
                for name in names:
                    base = name.split("/")[-1].lower()
                    if base == target_name or base == f"^{target_name}":
                        matching.append(name)

                if not matching:
                    log.warning("Symbol %s not found in ZIP file: %s", symbol, zip_file_path)
                    return []

                with z.open(matching[0]) as f:
                    csv_data = f.read()

                return self._parse_csv(csv_data, symbol, start_ns, end_ns)
        except Exception as e:
            log.error("Failed to read symbol %s from ZIP file %s: %s", symbol, zip_file_path, e)
            return []

    async def _download_csv(self, symbol: str) -> bytes | None:
        try:
            await self.ensure_authenticated(symbol)
        except Exception as e:
            log.warning(
                "Authentication failed during CSV download setup: %s. "
                "Attempting anyway.",
                e,
            )

        token = ""
        if self.session is not None:
            from urllib.parse import unquote
            cookies = self.session.cookie_jar.filter_cookies(URL(f"https://{self.domain}"))
            cookie_user = cookies.get("cookie_user")
            if cookie_user:
                val = unquote(cookie_user.value)
                m = re.search(r'\?([^|]+)\|', val)
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
                "Access denied downloading Stooq CSV for %s. "
                "Response starts with: %r",
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
        await self._request("GET", url)

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
                f"CAPTCHA verification failed. Entered code: {code}, "
                f"response: {submit_res}"
            )

        # Propagate cookies by requesting the target page again
        await self._request("GET", url)

    async def _solve_captcha_manual(self, img_bytes: bytes) -> str:
        captcha_path = os.path.join(os.getcwd(), "captcha.png")
        try:
            def _write_file() -> None:
                with open(captcha_path, "wb") as f:
                    f.write(img_bytes)

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

    async def solve_captcha_2captcha(self, api_key: str, img_bytes: bytes) -> str:
        import base64
        img_b64 = base64.b64encode(img_bytes).decode("utf-8")
        url_in = "http://2captcha.com/in.php"
        data = {
            "key": api_key,
            "method": "base64",
            "body": img_b64,
            "json": 1,
        }
        async with aiohttp.ClientSession() as solver_session:
            async with solver_session.post(url_in, json=data) as resp:
                res_json = await resp.json()
                if res_json.get("status") != 1:
                    raise RuntimeError(f"2captcha error: {res_json.get('request')}")
                task_id = res_json["request"]

            url_res = "http://2captcha.com/res.php"
            for _ in range(30):
                await asyncio.sleep(5)
                params = {"key": api_key, "action": "get", "id": task_id, "json": 1}
                async with solver_session.get(url_res, params=params) as res_resp:
                    res_json = await res_resp.json()
                    if res_json.get("status") == 1:
                        return str(res_json["request"])
                    if res_json.get("request") != "CAPCHA_NOT_READY":
                        raise RuntimeError(f"2captcha error: {res_json.get('request')}")
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
            }
        }
        async with aiohttp.ClientSession() as solver_session:
            async with solver_session.post(url_in, json=data) as resp:
                res_json = await resp.json()
                if res_json.get("errorId", 0) != 0:
                    raise RuntimeError(f"anticaptcha error: {res_json.get('errorDescription')}")
                task_id = res_json["taskId"]

            url_res = "https://api.anti-captcha.com/getTaskResult"
            for _ in range(30):
                await asyncio.sleep(5)
                payload = {"clientKey": api_key, "taskId": task_id}
                async with solver_session.post(url_res, json=payload) as res_resp:
                    res_json = await res_resp.json()
                    if res_json.get("errorId", 0) != 0:
                        raise RuntimeError(f"anticaptcha error: {res_json.get('errorDescription')}")
                    if res_json.get("status") == "ready":
                        return str(res_json["solution"]["text"])
        raise RuntimeError("anticaptcha solution timeout")

    async def _solve_pow(self, url: str, body: str, session: aiohttp.ClientSession) -> None:
        match = re.search(r'const c="([^"]+)"', body)
        if not match:
            return
        c = match.group(1)
        target_prefix = "0" * 4
        n = 0
        while True:
            data = (c + str(n)).encode("utf-8")
            h = hashlib.sha256(data).hexdigest()
            if h.startswith(target_prefix):
                break
            n += 1

        verify_url = str(URL(url).with_path("/__verify"))
        headers = {
            "Referer": url,
            "Content-Type": "application/x-www-form-urlencoded"
        }
        async with session.post(verify_url, data={"c": c, "n": str(n)}, headers=headers) as resp:
            await resp.text()

    async def _request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> aiohttp.ClientResponse:
        if self.session is None:
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "en-US,en;q=0.9",
            }
            self.session = aiohttp.ClientSession(headers=headers)

        session = self.session
        for _ in range(5):
            resp = await session.request(method, url, **kwargs)
            if resp.status == 200 and "text/html" in resp.headers.get("Content-Type", "").lower():
                body = await resp.text()
                if "verify your browser" in body or "const c=" in body:
                    await self._solve_pow(url, body, session)
                    continue
            return resp
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

        df = df.rename({col: col.strip("<>") for col in df.columns})

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
            date_val = str(dates[i])
            try:
                dt = datetime.strptime(date_val, "%Y%m%d").replace(tzinfo=UTC)
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
