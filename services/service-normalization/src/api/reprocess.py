"""
Reprocessing endpoints for the Normalization Service.
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field

from ..core.normalizer import NormalizationService

router = APIRouter()
logger = logging.getLogger(__name__)


class ReprocessRequest(BaseModel):
    """Reprocessing request model."""
    
    start_time: datetime = Field(..., description="Start time for reprocessing")
    end_time: datetime = Field(..., description="End time for reprocessing")
    market: Optional[str] = Field(None, description="Market filter")
    connector: Optional[str] = Field(None, description="Connector filter")
    dry_run: bool = Field(False, description="Dry run mode")
    force: bool = Field(False, description="Force reprocessing")


class ReprocessResponse(BaseModel):
    """Reprocessing response model."""
    
    job_id: str
    status: str
    message: str
    timestamp: str
    estimated_records: Optional[int] = None
    estimated_duration: Optional[str] = None


class ReprocessStatus(BaseModel):
    """Reprocessing status model."""
    
    job_id: str
    status: str
    progress: float
    records_processed: int
    records_total: Optional[int] = None
    start_time: str
    end_time: Optional[str] = None
    error_message: Optional[str] = None


# In-memory job tracking (would use Redis/database in production)
reprocess_jobs: Dict[str, Dict[str, Any]] = {}


@router.post("/", response_model=ReprocessResponse)
async def start_reprocess(
    request: ReprocessRequest,
    background_tasks: BackgroundTasks
):
    """Start a reprocessing job."""
    try:
        # Generate job ID
        job_id = f"reprocess_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{id(request) % 10000:04d}"
        
        # Validate request
        if request.end_time <= request.start_time:
            raise HTTPException(
                status_code=400,
                detail="End time must be after start time"
            )
        
        # Estimate records and duration
        time_diff = request.end_time - request.start_time
        estimated_records = int(time_diff.total_seconds() / 3600) * 1000  # Rough estimate
        estimated_duration = f"{time_diff.total_seconds() / 60:.1f} minutes"
        
        # Initialize job status
        reprocess_jobs[job_id] = {
            "status": "queued",
            "progress": 0.0,
            "records_processed": 0,
            "records_total": estimated_records,
            "start_time": datetime.now(timezone.utc).isoformat(),
            "end_time": None,
            "error_message": None,
            "request": request.dict()
        }
        
        # Start background task
        background_tasks.add_task(
            _run_reprocess_job,
            job_id,
            request
        )
        
        return ReprocessResponse(
            job_id=job_id,
            status="queued",
            message="Reprocessing job queued successfully",
            timestamp=datetime.now(timezone.utc).isoformat(),
            estimated_records=estimated_records,
            estimated_duration=estimated_duration
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to start reprocess job: {e}")
        raise HTTPException(status_code=500, detail="Failed to start reprocess job")


@router.get("/{job_id}", response_model=ReprocessStatus)
async def get_reprocess_status(job_id: str):
    """Get status of a reprocessing job."""
    try:
        if job_id not in reprocess_jobs:
            raise HTTPException(status_code=404, detail="Job not found")
        
        job = reprocess_jobs[job_id]
        
        return ReprocessStatus(
            job_id=job_id,
            status=job["status"],
            progress=job["progress"],
            records_processed=job["records_processed"],
            records_total=job["records_total"],
            start_time=job["start_time"],
            end_time=job["end_time"],
            error_message=job["error_message"]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get reprocess status for {job_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to get reprocess status")


@router.get("/")
async def list_reprocess_jobs():
    """List all reprocessing jobs."""
    try:
        jobs = []
        for job_id, job in reprocess_jobs.items():
            jobs.append({
                "job_id": job_id,
                "status": job["status"],
                "progress": job["progress"],
                "start_time": job["start_time"],
                "end_time": job["end_time"]
            })
        
        return {
            "jobs": jobs,
            "total": len(jobs)
        }
        
    except Exception as e:
        logger.error(f"Failed to list reprocess jobs: {e}")
        raise HTTPException(status_code=500, detail="Failed to list reprocess jobs")


@router.delete("/{job_id}")
async def cancel_reprocess_job(job_id: str):
    """Cancel a reprocessing job."""
    try:
        if job_id not in reprocess_jobs:
            raise HTTPException(status_code=404, detail="Job not found")
        
        job = reprocess_jobs[job_id]
        
        if job["status"] in ["completed", "failed", "cancelled"]:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot cancel job in {job['status']} status"
            )
        
        # Update job status
        job["status"] = "cancelled"
        job["end_time"] = datetime.now(timezone.utc).isoformat()
        
        return {
            "job_id": job_id,
            "status": "cancelled",
            "message": "Job cancelled successfully",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to cancel reprocess job {job_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to cancel reprocess job")


async def _run_reprocess_job(job_id: str, request: ReprocessRequest):
    """Run a reprocessing job in the background."""
    try:
        # Update job status
        reprocess_jobs[job_id]["status"] = "running"
        
        # Initialize normalization service
        normalization_service = NormalizationService()
        
        # Simulate reprocessing
        total_records = reprocess_jobs[job_id]["records_total"]
        processed_records = 0
        
        # Process in chunks
        chunk_size = 1000
        while processed_records < total_records:
            # Simulate processing time
            await asyncio.sleep(0.1)
            
            # Update progress
            processed_records += chunk_size
            progress = min(processed_records / total_records, 1.0)
            
            reprocess_jobs[job_id]["progress"] = progress
            reprocess_jobs[job_id]["records_processed"] = processed_records
            
            # Check for cancellation
            if reprocess_jobs[job_id]["status"] == "cancelled":
                return
        
        # Mark as completed
        reprocess_jobs[job_id]["status"] = "completed"
        reprocess_jobs[job_id]["end_time"] = datetime.now(timezone.utc).isoformat()
        
        logger.info(f"Reprocess job {job_id} completed successfully")
        
    except Exception as e:
        logger.error(f"Reprocess job {job_id} failed: {e}")
        reprocess_jobs[job_id]["status"] = "failed"
        reprocess_jobs[job_id]["error_message"] = str(e)
        reprocess_jobs[job_id]["end_time"] = datetime.now(timezone.utc).isoformat()
