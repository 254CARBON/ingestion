"""
CAISO extractor built on OASIS SingleZip endpoints.

Fetches PRC_LMP data and pivots component rows (MCE/MCC/MCL/LMP/MGHG)
into a single record per interval/node to match the raw_caiso_trade schema.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from ..base import ExtractionResult
from ..base.exceptions import ExtractionError
from ..base.utils import setup_logging
from .config import CAISOConnectorConfig
from .oasis_client import OASISClient, OASISClientConfig


class CAISOExtractor:
    """CAISO data extractor using OASIS SingleZip CSV."""

    def __init__(self, config: CAISOConnectorConfig):
        self.config = config
        self.logger = setup_logging(self.__class__.__name__)
        self._client = OASISClient(
            OASISClientConfig(
                base_url=getattr(config, "caiso_base_url", "https://oasis.caiso.com/oasisapi"),
                timeout=getattr(config, "caiso_timeout", 30),
                user_agent=getattr(config, "caiso_user_agent", "254Carbon/1.0"),
            )
        )

    @staticmethod
    def _format_oasis_dt(dt_like: Any) -> str:
        """Format input to OASIS datetime string YYYYMMDDThh:mm-0000 (UTC)."""
        if isinstance(dt_like, str):
            # Assume caller passed correct string; minimal validation here
            return dt_like
        if isinstance(dt_like, datetime):
            dt_utc = dt_like.astimezone(timezone.utc)
            return dt_utc.strftime("%Y%m%dT%H:%M-0000")
        raise ValueError("start/end must be datetime or OASIS-formatted string")

    @staticmethod
    def _parse_float(row: Dict[str, str], keys: List[str]) -> Optional[float]:
        for k in keys:
            if k in row and row[k] not in (None, ""):
                try:
                    return float(str(row[k]).strip())
                except ValueError:
                    continue
        return None

    @staticmethod
    def _parse_int(row: Dict[str, str], key: str) -> Optional[int]:
        v = row.get(key)
        try:
            return int(v) if v not in (None, "") else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_epoch_us(ts_gmt: str) -> int:
        # ts like 2025-01-01T00:00:00-00:00
        dt = datetime.strptime(ts_gmt.replace("-00:00", "+00:00"), "%Y-%m-%dT%H:%M:%S%z")
        return int(dt.timestamp() * 1_000_000)

    async def extract_prc_lmp(
        self,
        *,
        start: Any,
        end: Any,
        market_run_id: str,
        node: str,
        version: str = "12",
    ) -> ExtractionResult:
        """Extract PRC_LMP for a time window and node, pivoted per interval."""
        start_str = self._format_oasis_dt(start)
        end_str = self._format_oasis_dt(end)

        try:
            rows = await self._client.fetch_prc_lmp_csv(
                startdatetime=start_str,
                enddatetime=end_str,
                market_run_id=market_run_id,
                node=node,
                version=version,
            )
        except Exception as e:
            raise ExtractionError(f"OASIS fetch failed: {e}") from e
        finally:
            await self._client.aclose()

        if not rows:
            return ExtractionResult(
                data=[],
                metadata={
                    "query": {
                        "start": start_str,
                        "end": end_str,
                        "market_run_id": market_run_id,
                        "node": node,
                        "version": version,
                    },
                    "source": "oasis-singlezip",
                },
                record_count=0,
            )

        # Group rows by interval/node/run
        groups: Dict[tuple[str, str, str, str], List[Dict[str, str]]] = {}
        for r in rows:
            key = (
                r.get("INTERVALSTARTTIME_GMT") or r.get("INTERVAL_START_GMT") or "",
                r.get("INTERVALENDTIME_GMT") or r.get("INTERVAL_END_GMT") or "",
                r.get("NODE") or r.get("RESOURCE_NAME") or "",
                r.get("MARKET_RUN_ID") or "",
            )
            groups.setdefault(key, []).append(r)

        processed: List[Dict[str, Any]] = []
        for (start_gmt, end_gmt, node_name, run_id), group_rows in groups.items():
            # Seed values
            lmp = None
            mce = None
            mcc = None
            mcl = None
            ghg = None

            for r in group_rows:
                lmp_type = (r.get("LMP_TYPE") or r.get("DATA_ITEM") or "").upper()
                value = self._parse_float(r, ["MW", "VALUE"])  # header varies; VALUE is typical
                if lmp_type in ("LMP", "LMP_PRC"):
                    lmp = value
                elif lmp_type in ("MCE", "LMP_ENE_PRC"):
                    mce = value
                elif lmp_type in ("MCC", "LMP_CONG_PRC"):
                    mcc = value
                elif lmp_type in ("MCL", "LMP_LOSS_PRC"):
                    mcl = value
                elif lmp_type in ("MGHG", "LMP_GHG_PRC"):
                    ghg = value

            opr_dt = None
            opr_hr = None
            # Pick any row to source OPR_DT/OPR_HR
            sample = group_rows[0]
            if sample:
                opr_dt = sample.get("OPR_DT")
                opr_hr = self._parse_int(sample, "OPR_HR")

            record = {
                "event_id": str(uuid4()),
                "trace_id": "",
                "occurred_at": self._to_epoch_us(end_gmt) if end_gmt else int(datetime.now(timezone.utc).timestamp() * 1_000_000),
                "tenant_id": "default",
                "schema_version": "1.0.0",
                "producer": "caiso-connector",
                "market": "CAISO",
                "market_id": "CAISO",
                "timezone": "UTC",
                "currency": "USD",
                "unit": "MWh",
                "price_unit": "$/MWh",
                "data_type": "market_price",
                "source": "caiso-oasis",
                # Trade-like fields aligned to schema
                "trade_id": None,
                "delivery_location": node_name or None,
                "delivery_date": opr_dt,
                "delivery_hour": opr_hr,
                # Price fields (pivoted)
                "price": lmp,
                "quantity": None,
                "bid_price": None,
                "offer_price": None,
                "clearing_price": mce,
                "congestion_price": mcc,
                "loss_price": mcl,
                "curve_type": None,
                "price_type": None,
                "status_type": None,
                "status_value": None,
                "timestamp": end_gmt or start_gmt,
                "raw_data": json.dumps(group_rows),
                # transformation_timestamp will be added in transform step
            }
            processed.append(record)

        return ExtractionResult(
            data=processed,
            metadata={
                "query": {
                    "start": start_str,
                    "end": end_str,
                    "market_run_id": market_run_id,
                    "node": node,
                    "version": version,
                },
                "source": "oasis-singlezip",
                "records": len(processed),
            },
            record_count=len(processed),
        )

