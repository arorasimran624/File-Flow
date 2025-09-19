from services.db_services import get_file_ids_by_filename_and_date,count_failure_rows_by_fileids,count_success_rows_by_fileids
from services.utlities import compute_pass_fail_stats
import datetime
async def get_filename_date_stats(filename: str, date: str):
    """Retrieve pass/fail statistics for all files matching a given filename and optional date."""

    file_ids = await get_file_ids_by_filename_and_date(filename, date)
    if not file_ids:
        return {
            "filename": filename,
            "date": date,
            "total_rows": 0,
            "passed": 0,
            "failed": 0,
            "passed_percent": 0,
            "failed_percent": 0
        }

    passed = await count_success_rows_by_fileids(file_ids)
    failed = await count_failure_rows_by_fileids(file_ids)

    stats = compute_pass_fail_stats(passed, failed)
    stats["filename"] = filename
    stats["date"] = date
    return stats