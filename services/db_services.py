import os
from databases import Database
from dotenv import load_dotenv
import psycopg2
from datetime import datetime
import json
from services.utlities import get_row_hash
load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

DATABASE_URL = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
database = Database(DATABASE_URL)

print(DATABASE_URL)
def test_database():
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
    query = """ SELECT *FROM files ORDER BY File_id DESC """
    return await database.fetch_all(query=query)

async def get_file(file_id: str):
    query = "SELECT * FROM files WHERE file_id = :file_id"
    result = await database.fetch_one(query=query, values={"file_id": file_id})
    return result

async def get_processed(processed: bool):
    query = "SELECT * FROM files WHERE processed = :processed"
    results = await database.fetch_all(query=query, values={"processed": processed})
    return results

from datetime import datetime

async def update_file_status(file_id: str, status: str):
    processed = True if status == "success" else False
    query = """UPDATE files SET processed = :processed,processed_at = :processed_at WHERE file_id = :file_id"""
    values = { "processed": processed,"processed_at": datetime.utcnow(),"file_id": file_id } 
    await database.execute(query=query, values=values)

# async def insert_success(file_id: str, passed_rows_data: list):
#     """
#     passed_rows_data: list of dicts, each dict is a full row that passed validation
#     """
#     for row_data in passed_rows_data:
#         query = """ INSERT INTO file_success (file_id, processed_at, ) VALUES (:file_id, :processed_at, :passed_row)"""

#         values = {
#             "file_id": file_id,
#             "processed_at": datetime.utcnow(),
#             "passed_row": json.dumps(row_data)  # store full row JSON
#         }
#         await database.execute(query=query, values=values)

# Insert into failure table
async def insert_failure(file_id: str, error_type: str, error_detail: str):
    query = """ INSERT INTO file_failure (file_id, error_type, errors, processed_at) VALUES (:file_id, :error_type, :errors, :processed_at)"""
    values = {
        "file_id": file_id,
        "error_type": error_type,
        "errors": json.dumps(error_detail),
        "processed_at": datetime.utcnow()
    }
    await database.execute(query=query, values=values)
async def row_exists(row_hash: str) -> bool:
    """
    Check if a row with this hash already exists in file_success.
    """
    query_check = """
        SELECT 1 FROM file_success WHERE row_hash = :row_hash
    """
    existing = await database.fetch_one(
        query=query_check, values={"row_hash": row_hash}
    )
    return bool(existing)


async def insert_success(file_id: str, row_data: dict):
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
