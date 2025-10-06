"""
Reusable client for CAISO OASIS SingleZip endpoints.

Provides helpers to fetch zipped CSV/XML payloads and parse into rows.
"""

from __future__ import annotations

import csv
import io
import zipfile
from dataclasses import dataclass
from typing import Dict, List, Optional

import httpx


@dataclass
class OASISClientConfig:
    base_url: str = "https://oasis.caiso.com/oasisapi"
    timeout: int = 30
    user_agent: str = "254Carbon-Ingestion/1.0"


class OASISClient:
    """Thin HTTP client for OASIS SingleZip endpoints (CSV/XML in ZIP)."""

    def __init__(self, config: Optional[OASISClientConfig] = None) -> None:
        self.config = config or OASISClientConfig()
        self._client = httpx.AsyncClient(
            base_url=self.config.base_url,
            timeout=self.config.timeout,
            headers={"User-Agent": self.config.user_agent},
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def fetch_singlezip(self, queryname: str, params: Dict[str, str]) -> bytes:
        """Fetch raw ZIP bytes for a SingleZip request.

        Raises httpx.HTTPStatusError on non-2xx.
        """
        all_params = {"queryname": queryname, **params}
        resp = await self._client.get("/SingleZip", params=all_params)
        resp.raise_for_status()
        return resp.content

    @staticmethod
    def _unzip_single_file(zip_bytes: bytes) -> bytes:
        with io.BytesIO(zip_bytes) as bio:
            with zipfile.ZipFile(bio) as zf:
                # Take the first file in the archive
                names = zf.namelist()
                if not names:
                    raise ValueError("Empty ZIP archive from OASIS")
                with zf.open(names[0]) as f:
                    return f.read()

    @staticmethod
    def _parse_csv(data: bytes) -> List[Dict[str, str]]:
        text = data.decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        return [dict(row) for row in reader]

    async def fetch_csv_rows(
        self,
        queryname: str,
        params: Dict[str, str],
        *,
        resultformat: str = "6",
    ) -> List[Dict[str, str]]:
        """Fetch rows for a SingleZip query as list of dicts (CSV)."""
        zip_bytes = await self.fetch_singlezip(queryname, {**params, "resultformat": resultformat})
        file_bytes = self._unzip_single_file(zip_bytes)
        # Sometimes OASIS returns XML with an <ERROR> even if resultformat=6 requested.
        # Detect by leading token and short-circuit if needed.
        if file_bytes.startswith(b"<?xml"):
            # Let caller decide how to handle; keep the XML to allow better error surfacing.
            # Return empty rows to signal no CSV payload.
            return []
        return self._parse_csv(file_bytes)

    async def fetch_prc_lmp_csv(
        self,
        *,
        startdatetime: str,
        enddatetime: str,
        market_run_id: str,
        node: str,
        version: str = "12",
        resultformat: str = "6",
    ) -> List[Dict[str, str]]:
        """Convenience for PRC_LMP rows in CSV."""
        params = {
            "version": version,
            "startdatetime": startdatetime,
            "enddatetime": enddatetime,
            "market_run_id": market_run_id,
            # OASIS supports `node` or `nodename` depending on report; `PRC_LMP` accepts `node`.
            "node": node,
        }
        return await self.fetch_csv_rows("PRC_LMP", params, resultformat=resultformat)

