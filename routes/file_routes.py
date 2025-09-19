from fastapi import APIRouter, UploadFile, File, HTTPException,Form
from services.db_services import insert_file,fetch_files,get_file,get_processed
from services.rabbit_service import publish_to_queue
import os
from dotenv import load_dotenv
import traceback
files = APIRouter() 

QUEUE_FIRST=os.getenv("QUEUE_FIRST")

@files.post("/upload")
async def upload_file( file_id: str = Form(...), userid: str = Form(...),username: str = Form(...),role: str = Form(...),file: UploadFile = File(...)):
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
    try:
        rows = await fetch_files()
        return {"files": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@files.get("/{file_id}")
async def fetch_file(file_id: str):
    result = await get_file(file_id)
    if not result:
        raise HTTPException(status_code=404, detail="File not found")
    return {
        "file_id": result.file_id,
        "filename": result.filename,
        "processed": result.processed,
        "error": result.error
    }

@files.get("/processed/{status}")
async def fetch_processed(status: bool):
    results = await get_processed(status)
    if not results:
        raise HTTPException(status_code=404, detail="File not found")
    return [dict(row) for row in results]