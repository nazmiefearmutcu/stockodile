"""SEC EDGAR Provider Client."""

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Generator, Iterable
from pathlib import Path
from typing import Any

import aiohttp
import msgspec

from stockodile.ratelimit import TokenBucketLimiter
from stockodile.schema.records import Filing, Fundamental


class SecEdgarClient:
    """Client for interacting with the SEC EDGAR API and parsing XBRL facts."""

    def __init__(
        self,
        user_agent: str = "Stockodile/0.0.1 (contact@stockodile.org)",
        session: aiohttp.ClientSession | None = None,
        rate_limit: float = 10.0,
    ) -> None:
        """Initialize the SEC EDGAR client.

        Args:
            user_agent: Mandatory User-Agent header (must contain AppName contact@domain).
            session: Optional pre-existing aiohttp ClientSession.
            rate_limit: Rate limit in requests per second (default 10.0).
        """
        self.user_agent = user_agent
        self.session = session
        self._limiter = TokenBucketLimiter(rate_limit, rate_limit)

        self._ticker_to_cik: dict[str, int] = {}
        self._cik_to_tickers: dict[int, list[str]] = {}
        self._cik_to_primary_ticker: dict[int, str] = {}

    def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def close(self) -> None:
        """Close the underlying HTTP session."""
        if self.session is not None and not self.session.closed:
            await self.session.close()

    async def _request(self, url: str) -> aiohttp.ClientResponse:
        attempts = 0
        while True:
            await self._limiter.acquire()
            headers = {"User-Agent": self.user_agent}
            try:
                session = self._get_session()
                resp = await session.get(url, headers=headers)
                if resp.status in (403, 429):
                    attempts += 1
                    if attempts > 5:
                        resp.raise_for_status()
                    # Exponential backoff
                    delay = min(30.0, 1.0 * (2**attempts))
                    await asyncio.sleep(delay)
                    continue
                return resp
            except Exception:
                attempts += 1
                if attempts > 5:
                    raise
                delay = min(30.0, 1.0 * (2**attempts))
                await asyncio.sleep(delay)

    async def _request_json(self, url: str) -> Any:
        resp = await self._request(url)
        try:
            resp.raise_for_status()
            content = await resp.read()
            return msgspec.json.decode(content)
        finally:
            resp.close()

    async def fetch_ticker_map(self) -> None:
        """Fetch the ticker-to-CIK mapping from the SEC website."""
        url = "https://www.sec.gov/files/company_tickers.json"
        data = await self._request_json(url)

        ticker_to_cik: dict[str, int] = {}
        cik_to_tickers: dict[int, list[str]] = {}
        cik_to_primary_ticker: dict[int, str] = {}

        if isinstance(data, dict):
            for item in data.values():
                cik = int(item["cik_str"])
                ticker = str(item["ticker"]).upper()

                ticker_to_cik[ticker] = cik
                if cik not in cik_to_tickers:
                    cik_to_tickers[cik] = []
                cik_to_tickers[cik].append(ticker)

        for cik, tickers in cik_to_tickers.items():
            cik_to_primary_ticker[cik] = tickers[0]

        self._ticker_to_cik = ticker_to_cik
        self._cik_to_tickers = cik_to_tickers
        self._cik_to_primary_ticker = cik_to_primary_ticker

    async def ensure_ticker_map(self) -> None:
        """Ensure the ticker-to-CIK mapping is populated."""
        if not self._ticker_to_cik:
            await self.fetch_ticker_map()

    async def fetch_submissions(self, cik: str | int) -> dict[str, Any]:
        """Fetch the submissions metadata JSON for a company by CIK."""
        cik_str = self.normalize_cik(cik)
        url = f"https://data.sec.gov/submissions/CIK{cik_str}.json"
        res = await self._request_json(url)
        if not isinstance(res, dict):
            raise TypeError("Expected dict from SEC submissions endpoint")
        return res

    async def fetch_company_facts(self, cik: str | int) -> dict[str, Any]:
        """Fetch the XBRL facts JSON for a company by CIK."""
        cik_str = self.normalize_cik(cik)
        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_str}.json"
        res = await self._request_json(url)
        if not isinstance(res, dict):
            raise TypeError("Expected dict from SEC company facts endpoint")
        return res

    @staticmethod
    def normalize_cik(cik: str | int) -> str:
        """Normalize a CIK to a 10-digit zero-padded string."""
        if isinstance(cik, int):
            return f"{cik:010d}"
        clean = "".join(filter(str.isdigit, cik))
        if not clean:
            raise ValueError(f"Invalid CIK: {cik}")
        return f"{int(clean):010d}"

    def _parse_filing_dict(
        self, filings_data: dict[str, Any], symbol: str, cik: int, local_ts: int
    ) -> list[Filing]:
        accession_numbers = filings_data.get("accessionNumber", [])
        forms = filings_data.get("form", [])
        filing_dates = filings_data.get("filingDate", [])
        report_dates = filings_data.get("reportDate", [])
        primary_documents = filings_data.get("primaryDocument", [])
        is_xbrl_list = filings_data.get("isXBRL", [])

        filings = []
        for i in range(len(accession_numbers)):
            accn = accession_numbers[i]
            accn_no_dashes = accn.replace("-", "")
            doc = primary_documents[i]
            doc_url = (
                f"https://www.sec.gov/Archives/edgar/data/{cik}/{accn_no_dashes}/{doc}"
            )

            filings.append(
                Filing(
                    provider="sec_edgar",
                    symbol=symbol,
                    symbol_raw=symbol,
                    source_ts=None,
                    local_ts=local_ts,
                    accession_number=accn,
                    form=forms[i],
                    filing_date=filing_dates[i],
                    report_date=report_dates[i] if i < len(report_dates) else None,
                    primary_document=doc,
                    document_url=doc_url,
                    is_xbrl=bool(is_xbrl_list[i]) if i < len(is_xbrl_list) else None,
                )
            )
        return filings

    async def get_filings(self, symbol: str, include_historical: bool = False) -> list[Filing]:
        """Get the filings for a company by symbol or CIK.

        Args:
            symbol: Ticker symbol (e.g. 'AAPL') or CIK.
            include_historical: If True, fetch older submission files listed in history.
        """
        await self.ensure_ticker_map()
        symbol_upper = symbol.upper()

        cik = self._ticker_to_cik.get(symbol_upper)
        if cik is None:
            try:
                cik = int(symbol_upper.replace("CIK", ""))
            except ValueError as err:
                raise ValueError(f"Unknown symbol or CIK: {symbol}") from err

        data = await self.fetch_submissions(cik)
        local_ts = time.time_ns()

        filings = self._parse_filing_dict(
            data.get("filings", {}).get("recent", {}), symbol_upper, cik, local_ts
        )

        if include_historical:
            files = data.get("filings", {}).get("files", [])
            for file_info in files:
                filename = file_info.get("name")
                if filename:
                    url_hist = f"https://data.sec.gov/submissions/{filename}"
                    hist_data = await self._request_json(url_hist)
                    filings.extend(
                        self._parse_filing_dict(hist_data, symbol_upper, cik, local_ts)
                    )

        return filings

    def _normalize_facts(
        self, cik: int, facts_data: dict[str, Any], local_ts: int
    ) -> Generator[Fundamental, None, None]:
        symbol = self._cik_to_primary_ticker.get(cik, f"CIK{cik:010d}")
        facts = facts_data.get("facts", {})
        for taxonomy, tags in facts.items():
            for tag, tag_data in tags.items():
                units = tag_data.get("units", {})
                for unit, values in units.items():
                    for val_obj in values:
                        yield Fundamental(
                            provider="sec_edgar",
                            symbol=symbol,
                            symbol_raw=symbol,
                            source_ts=None,
                            local_ts=local_ts,
                            taxonomy=taxonomy,
                            tag=tag,
                            unit=unit,
                            val=float(val_obj.get("val", 0.0)),
                            end=val_obj.get("end", ""),
                            start=val_obj.get("start"),
                            fy=val_obj.get("fy"),
                            fp=val_obj.get("fp"),
                            form=val_obj.get("form"),
                            filed=val_obj.get("filed"),
                            accn=val_obj.get("accn"),
                            frame=val_obj.get("frame"),
                        )

    def _deduplicate_facts(self, facts: Iterable[Fundamental]) -> list[Fundamental]:
        deduped: dict[tuple[str, str, str, int | None, str | None], Fundamental] = {}
        for fact in facts:
            key = (fact.taxonomy, fact.tag, fact.end, fact.fy, fact.fp)
            existing = deduped.get(key)
            if existing is None:
                deduped[key] = fact
            else:
                existing_filed = existing.filed or ""
                new_filed = fact.filed or ""
                if new_filed > existing_filed:
                    deduped[key] = fact
                elif new_filed == existing_filed:
                    if fact.frame and not existing.frame:
                        deduped[key] = fact
        return list(deduped.values())

    async def get_fundamentals(
        self, symbol: str, deduplicate: bool = True
    ) -> list[Fundamental]:
        """Get fundamental facts for a company by symbol or CIK.

        Args:
            symbol: Ticker symbol or CIK.
            deduplicate: If True, keep only the latest restatement of each fact.
        """
        await self.ensure_ticker_map()
        symbol_upper = symbol.upper()

        cik = self._ticker_to_cik.get(symbol_upper)
        if cik is None:
            try:
                cik = int(symbol_upper.replace("CIK", ""))
            except ValueError as err:
                raise ValueError(f"Unknown symbol or CIK: {symbol}") from err

        data = await self.fetch_company_facts(cik)
        local_ts = time.time_ns()

        raw_facts = self._normalize_facts(cik, data, local_ts)
        if deduplicate:
            return self._deduplicate_facts(raw_facts)
        return list(raw_facts)

    async def download_company_facts_zip(self, dest_path: str | Path) -> None:
        """Download the bulk company facts ZIP file."""
        url = "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip"
        resp = await self._request(url)
        try:
            resp.raise_for_status()
            with open(dest_path, "wb") as f:  # noqa: ASYNC230
                async for chunk in resp.content.iter_chunked(8192):
                    f.write(chunk)
        finally:
            resp.close()

    def parse_company_facts_zip(
        self, zip_path: str | Path, deduplicate: bool = True
    ) -> Generator[Fundamental, None, None]:
        """Parse a bulk company facts ZIP file and yield Fundamental records.

        Args:
            zip_path: Path to the local companyfacts.zip file.
            deduplicate: If True, keep only the latest restatement of each company's facts.
        """
        import zipfile

        local_ts = time.time_ns()
        with zipfile.ZipFile(zip_path, "r") as z:
            for info in z.infolist():
                if not info.filename.endswith(".json"):
                    continue

                basename = os.path.basename(info.filename)
                cik_str = basename.replace(".json", "").replace("CIK", "")
                try:
                    cik = int(cik_str)
                except ValueError:
                    continue

                with z.open(info) as f:
                    try:
                        content = f.read()
                        data = msgspec.json.decode(content)
                    except Exception:
                        continue

                    raw_facts = self._normalize_facts(cik, data, local_ts)
                    if deduplicate:
                        for fact in self._deduplicate_facts(raw_facts):
                            yield fact
                    else:
                        for fact in raw_facts:
                            yield fact
