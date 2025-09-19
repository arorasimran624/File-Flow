from fastapi import APIRouter, UploadFile, File, HTTPException,Form,Query
from services.db_services import insert_file,fetch_files,get_file,get_processed,fetch_file_counts
from services.rabbit_service import publish_to_queue
from services.stats import get_filename_date_stats
from services.utlities import compute_file_stats
import os
from datetime import date
from services.db_services import get_file_stats_by_date
from dotenv import load_dotenv
import traceback
files = APIRouter(prefix="/files")

QUEUE_FIRST=os.getenv("QUEUE_FIRST")

@files.post("/upload")
async def upload_file( file_id: str = Form(...), userid: str = Form(...),username: str = Form(...),role: str = Form(...),file: UploadFile = File(...)):
    """Upload a file, save it in DB, and push it to the processing queue."""
    try:
        file_bytes = await file.read()
        await insert_file(
            file_id=file_id,
            filename=file.filename,
            userid=userid,
            username=username,
            role=role
        )
        message = {
            "file_id": file_id,
            "filename": file.filename,
            "file_content": file_bytes.decode("utf-8"),
        }
        await publish_to_queue(message, QUEUE_FIRST)
        return {"status": "queued", "file_id": file_id, "filename": file.filename}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@files.get("/")
async def get_files():
    """Fetch all files from the database."""
    try:
        rows = await fetch_files()
        return {"files": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@files.get("/{file_id}")
async def fetch_file(file_id: str):
    """Fetch details of a single file by its file_id."""
    result = await get_file(file_id)
    if not result:
        raise HTTPException(status_code=404, detail="File not found")
    return {
        "file_id": result.file_id,
        "filename": result.filename,
        "processed": result.processed,
        
    }

@files.get("/processed/{status}")
async def fetch_processed(status: bool):
    """Fetch files filtered by their processed status (success or failure)."""
    results = await get_processed(status)
    if not results:
        raise HTTPException(status_code=404, detail="File not found")
    return [dict(row) for row in results]

@files.get("/files/stats")
async def get_file_stats():
    """Fetch aggregated counts of total, passed, and failed files."""
    total, passed, failed = await fetch_file_counts()
    return compute_file_stats(total, passed, failed)


@files.post("/files/date-stats")
async def date_stats(
    filename: str = Form(...),
    date: str = Form(None, description="Optional date in YYYY-MM-DD format")
):
    """Fetch validation statistics (passed/failed rows) for a specific filename and date."""
    stats = await get_filename_date_stats(filename, date)
    if stats["total_rows"] == 0:
        raise HTTPException(status_code=404, detail="No records found for given filename/date")
    return stats

@files.get("/files/file-stats")
async def file_stats(date: date = Query(..., description="Date in YYYY-MM-DD format")):
    """Fetch aggregated counts of successfully processed and failed files for a specific date."""
    result = await get_file_stats_by_date(date)
    return {
        "date": str(date),
        "success_count": result["success_count"] if result else 0,
        "failure_count": result["failure_count"] if result else 0
    }