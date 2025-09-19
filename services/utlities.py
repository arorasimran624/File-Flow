import hashlib
import json

def get_row_hash(row_data: dict) -> str:
    """Generate a hash for a row dictionary."""
    row_str = json.dumps(row_data, sort_keys=True)  # deterministic string
    return hashlib.md5(row_str.encode("utf-8")).hexdigest()

def compute_file_stats(total: int, passed: int, failed: int):
    """Compute file-level statistics: total, passed, failed counts and percentages."""
    passed_percent = round((passed / total) * 100, 2) if total else 0
    failed_percent = round((failed / total) * 100, 2) if total else 0
    return {"total_files": total, "passed": passed, "failed": failed, "passed_percent": passed_percent, "failed_percent": failed_percent}

def compute_pass_fail_stats(passed: int, failed: int):
    """Compute row-level pass/fail statistics and their percentages."""
    total = passed + failed
    passed_percent = round((passed / total) * 100, 2) if total else 0
    failed_percent = round((failed / total) * 100, 2) if total else 0

    return {
        "total_rows": total,
        "passed": passed,
        "failed": failed,
        "passed_percent": passed_percent,
        "failed_percent": failed_percent
    }
