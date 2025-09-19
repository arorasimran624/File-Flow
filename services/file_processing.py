import io
import pandas as pd
from services.file_validators import validate_template, validate_nulls, validate_data_types
from services.rabbit_service import publish_to_queue
from services.db_services import update_file_status,insert_failure,insert_success
import os,json
from dotenv import load_dotenv

QUEUE_SECOND = os.getenv("QUEUE_SECOND")

async def process_file(file_id: str, file_content: str) -> dict:
    """Process a single uploaded CSV file."""
    result = {"file_id": file_id, "status": "success", "errors": {}}
    try:
        df = pd.read_csv(io.StringIO(file_content),dtype={"contact_no": str},keep_default_na=False)
        df.columns = df.columns.str.strip() 

        template_result = validate_template(df)
        if template_result["status"] != "success":
            result["errors"]["template"] = template_result.get("details", {})
            result["status"] = "failed" 

        null_result = validate_nulls(df)
        if null_result["status"] != "success":
            result["errors"]["null_check"] = null_result.get("details", {}).get("null_error", [])
            result["status"] = "failed"

        type_result = validate_data_types(df)
        if type_result["status"] != "success":
            result["errors"]["data_type_check"] = type_result.get("details", {})
            result["status"] = "failed"

        failed_rows = set()

        if "template" in result["errors"]:
            details = result["errors"]["template"]
            for missing in details.get("missing_columns", []):
                await insert_failure(file_id, "template", f"missing column: {missing}")
            for extra in details.get("extra_columns", []):
                await insert_failure(file_id, "template", f"extra column: {extra}")
            if details.get("order_mismatch"):
                await insert_failure(file_id, "template", "order mismatch")

        for null_row in result["errors"].get("null_check", []):
            row_num = null_row["row"]
            failed_rows.add(row_num)
            null_cols = ", ".join(null_row["null_columns"])
            row_data = null_row["data"]
            await insert_failure(
                file_id,
                "null_check",
                f"Null value in column(s): {null_cols} at row {row_num}: {json.dumps(row_data)}"
            )

        for dtype_error, rows in result["errors"].get("data_type_check", {}).items():
            for row_info in rows:
                if isinstance(row_info, dict):
                    row_num = row_info["row"]
                    row_data = row_info["data"]
                else:
                    row_num = row_info
                    row_data = df.iloc[row_num - 2].to_dict() 
                failed_rows.add(row_num)
                await insert_failure(
                    file_id,
                    dtype_error,
                    f"{dtype_error} at row {row_num}: {json.dumps(row_data)}"
                )

        all_rows = list(range(2, len(df) + 2))  
        passed_rows = [r for r in all_rows if r not in failed_rows]
        passed_rows_data = [
         df.iloc[r - 2].where(pd.notna(df.iloc[r - 2]), None).to_dict()  
         for r in passed_rows
        ]
        for row_data in passed_rows_data:
            await insert_success(file_id, row_data)

        if result["status"] == "failed":
            result["message"] = f"{len(failed_rows)} row(s) failed, {len(passed_rows)} row(s) passed"
        else:
            result["status"] = "success"
            result["message"] = "All validations passed"


        await update_file_status(file_id, result["status"])
        await publish_to_queue(result, QUEUE_SECOND)
        return result

    except Exception as e:
        return {"file_id": file_id, "status": "error", "message": str(e)}


def compute_file_stats(total: int, passed: int, failed: int):
    """ Compute overall file validation statistics."""
    passed_percent = round((passed / total) * 100, 2) if total else 0
    failed_percent = round((failed / total) * 100, 2) if total else 0
    
    return {
        "total_files": total,
        "passed": passed,
        "failed": failed,
        "passed_percent": passed_percent,
        "failed_percent": failed_percent
    }
