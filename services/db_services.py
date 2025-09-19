import os
from databases import Database
from dotenv import load_dotenv
import psycopg2
import json
from services.utlities import get_row_hash
from datetime import datetime, date
load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

DATABASE_URL = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
database = Database(DATABASE_URL)

def test_database():
    """Test database connectivity by connecting and fetching version."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        cursor.close()
        conn.close()
        return f"Database connected, version: {version[0]}"
    except Exception as e:
        return f"Database test failed: {e}"

async def insert_file(file_id: str, filename: str, userid: str, username: str, role: str):
    """Insert a new file record into the 'files' table."""
    query = """INSERT INTO files (file_id, filename, userid, username, role) VALUES (:file_id, :filename, :userid, :username, :role)
    """
    values = {
        "file_id": file_id,
        "filename": filename,
        "userid": userid,
        "username": username,
        "role": role
    }
    await database.execute(query=query, values=values)


async def fetch_files():
    """Fetch all files ordered by file_id """
    query = """ SELECT *FROM files ORDER BY File_id """
    return await database.fetch_all(query=query)

async def get_file(file_id: str):
    """Fetch a single file by its file_id."""
    query = "SELECT * FROM files WHERE file_id = :file_id"
    result = await database.fetch_one(query=query, values={"file_id": file_id})
    return result

async def get_processed(processed: bool):
    """Fetch files filtered by their processed status (True/False)."""
    query = "SELECT * FROM files WHERE processed = :processed"
    results = await database.fetch_all(query=query, values={"processed": processed})
    return results

async def update_file_status(file_id: str, status: str):
    """Update a file's processed status and processed_at timestamp."""
    processed = True if status == "success" else False
    query = """UPDATE files SET processed = :processed,processed_at = :processed_at WHERE file_id = :file_id"""
    values = { "processed": processed,"processed_at": datetime.utcnow(),"file_id": file_id } 
    await database.execute(query=query, values=values)

async def insert_failure(file_id: str, error_type: str, error_detail: str):
    """Insert a failure record into 'file_failure' table for a file row."""
    query = """ INSERT INTO file_failure (file_id, error_type, errors, processed_at) VALUES (:file_id, :error_type, :errors, :processed_at)"""
    values = {
        "file_id": file_id,
        "error_type": error_type,
        "errors": json.dumps(error_detail),
        "processed_at": datetime.utcnow()
    }
    await database.execute(query=query, values=values)
async def row_exists(row_hash: str) -> bool:
    """Check if a row with this hash already exists in 'file_success' to avoid duplicates."""
    query_check = """
        SELECT 1 FROM file_success WHERE row_hash = :row_hash
    """
    existing = await database.fetch_one(
        query=query_check, values={"row_hash": row_hash}
    )
    return bool(existing)


async def insert_success(file_id: str, row_data: dict):
    """Insert a successful row into 'file_success' table."""
    row_hash = get_row_hash(row_data)
    if await row_exists(row_hash):
        return
    query_insert = """
        INSERT INTO file_success (file_id, processed_at, row_data, row_hash)
        VALUES (:file_id, :processed_at, :row_data, :row_hash)
    """
    values = {
        "file_id": file_id,
        "processed_at": datetime.utcnow(),
        "row_data": json.dumps(row_data),
        "row_hash": row_hash
    }
    await database.execute(query=query_insert, values=values)

async def fetch_file_counts():
    """Fetch total, passed, and failed file counts for summary statistics."""
    total_query = "SELECT COUNT(*) FROM files"
    total = await database.fetch_val(total_query)
    passed_query = "SELECT COUNT(*) FROM files WHERE processed = true"
    passed = await database.fetch_val(passed_query)
    failed = total - passed if total else 0
    return total, passed, failed

async def get_file_ids_by_filename_and_date(filename: str, date_str: str = None):
    """Fetch file_ids for a given filename and processed date."""
    values = {"filename": filename}
    if date_str:
        parsed_date = date.fromisoformat(date_str.strip())

        query = """
            SELECT file_id FROM files
            WHERE filename = :filename
            AND processed_at IS NOT NULL
            AND DATE(processed_at) = :date
        """
        values["date"] = parsed_date
    else:
        query = """
            SELECT file_id FROM files
            WHERE filename = :filename
        """

    rows = await database.fetch_all(query=query, values=values)
    return [row["file_id"] for row in rows]

async def count_success_rows_by_fileids(file_ids: list):
    """Count the number of successful rows for a list of file_ids."""
    if not file_ids:
        return 0
    query = "SELECT COUNT(*) FROM file_success WHERE file_id = ANY(:file_ids)"
    return await database.fetch_val(query, values={"file_ids": file_ids})


async def count_failure_rows_by_fileids(file_ids: list):
    """Count the number of failed rows for a list of file_ids."""
    if not file_ids:
        return 0
    query = "SELECT COUNT(*) FROM file_failure WHERE file_id = ANY(:file_ids)"
    return await database.fetch_val(query, values={"file_ids": file_ids})

async def get_file_stats_by_date(date):
    """Fetch the count of success and failure files for a given date."""
    query = """
        SELECT 
            COUNT(*) FILTER (WHERE processed = true) AS success_count,
            COUNT(*) FILTER (WHERE processed = false) AS failure_count
        FROM files
        WHERE DATE(processed_at) = :date
    """
    return await database.fetch_one(query=query, values={"date": date})
