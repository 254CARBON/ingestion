from pathlib import Path

import pytest

from connectors.base import ExtractionResult, TransformationResult, LoadResult
from connectors.caiso.connector import CAISOConnector
from examples.pipeline.raw_pipeline import RawKafkaIngestionPipeline


@pytest.mark.asyncio
async def test_raw_pipeline_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pipeline publishes records successfully with patched connector."""
    config_path = Path("examples/pipeline/raw_pipeline_config.yaml")
    pipeline = RawKafkaIngestionPipeline.from_path(config_path)

    async def fake_extract(self, **kwargs) -> ExtractionResult:
        return ExtractionResult(data=[{"event_id": "1"}], metadata={}, record_count=1)

    async def fake_transform(self, extraction_result: ExtractionResult) -> TransformationResult:
        return TransformationResult(
            data=extraction_result.data,
            metadata={},
            record_count=extraction_result.record_count,
            validation_errors=[],
        )

    async def fake_load(self, transformation_result: TransformationResult) -> LoadResult:
        return LoadResult(
            records_attempted=transformation_result.record_count,
            records_published=transformation_result.record_count,
            records_failed=0,
            metadata={"load_method": "test"},
            errors=[],
        )

    async def fake_cleanup(self) -> None:
        return None

    monkeypatch.setattr(CAISOConnector, "extract", fake_extract)
    monkeypatch.setattr(CAISOConnector, "transform", fake_transform)
    monkeypatch.setattr(CAISOConnector, "load", fake_load)
    monkeypatch.setattr(CAISOConnector, "cleanup", fake_cleanup)

    summaries = await pipeline.run()
    assert len(summaries) == 1

    summary = summaries[0]
    assert summary.status == "success"
    assert summary.load_success is True
    assert summary.load_published == 1
    assert summary.output_topic == "ingestion.caiso.raw.exemplar.v1"


@pytest.mark.asyncio
async def test_raw_pipeline_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """Dry-run skips publishing but still reports success."""
    config_path = Path("examples/pipeline/raw_pipeline_config.yaml")
    pipeline = RawKafkaIngestionPipeline.from_path(config_path)

    async def fake_extract(self, **kwargs) -> ExtractionResult:
        return ExtractionResult(data=[{"event_id": "1"}], metadata={}, record_count=1)

    async def fake_transform(self, extraction_result: ExtractionResult) -> TransformationResult:
        return TransformationResult(
            data=extraction_result.data,
            metadata={},
            record_count=extraction_result.record_count,
            validation_errors=[],
        )

    async def fake_cleanup(self) -> None:
        return None

    monkeypatch.setattr(CAISOConnector, "extract", fake_extract)
    monkeypatch.setattr(CAISOConnector, "transform", fake_transform)
    monkeypatch.setattr(CAISOConnector, "cleanup", fake_cleanup)

    summaries = await pipeline.run(dry_run=True)
    summary = summaries[0]
    assert summary.status == "success"
    assert summary.load_success is True
    assert summary.load_published == 0
    assert summary.load_failed == 0
