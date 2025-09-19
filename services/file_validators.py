from typing import List
import pandas as pd
from datetime import datetime
from services.constants import EXPECTED_COLUMNS
import math
import re
from dateutil.parser import parse

EMAIL_REGEX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$") 

def validate_template(df: pd.DataFrame) -> dict:
    found_columns = list(df.columns)
    missing = [col for col in EXPECTED_COLUMNS if col not in found_columns]
    extra = [col for col in found_columns if col not in EXPECTED_COLUMNS]
    order_mismatch = EXPECTED_COLUMNS != found_columns

    if missing or extra or order_mismatch:
        details = {}
        if missing:
            details["missing_columns"] = missing
        if extra:
            details["extra_columns"] = extra
        if order_mismatch:
            details["order_mismatch"] = True
        return {"status": "failed", "details": details}

    return {"status": "success"}

def validate_nulls(df: pd.DataFrame) -> dict:
    null_rows = []
    for idx, row in df.iterrows():
        row_null_cols = [col for col in df.columns if pd.isna(row[col]) or str(row[col]).strip() == ""]
        if row_null_cols:
            null_rows.append({
                "row": idx + 2, 
                "null_columns": row_null_cols,
                "data": row.to_dict() 
            })
    if null_rows:
        return {"status": "failed", "details": {"null_error": null_rows}}
    return {"status": "success", "details": {"null_error": []}}

def validate_data_types(df: pd.DataFrame) -> dict:
    errors = {"email_error": [], "phone_error": [], "dob_error": []}
    for idx, row in df.iterrows():
        if row.isna().all():
            continue
        row_dict = row.to_dict()
        phone_val = row["contact_no"] if "contact_no" in row else ""
        if pd.notna(phone_val) and phone_val != "":
            if isinstance(phone_val, float):
                phone_str = str(int(phone_val))
            else:
                phone_str = str(phone_val).strip()
            phone_str = ''.join(filter(str.isdigit, phone_str))
            if len(phone_str) != 10:
                errors["phone_error"].append({"row": idx + 2, "data": row_dict})

        dob_val = row["datetime"] if "datetime" in row else ""
        if pd.notna(dob_val) and dob_val != "":
            dob_str = str(dob_val).strip().replace("\u200b", "")
            try:
                datetime.strptime(dob_str, "%m-%d-%Y")
            except Exception:
                errors["dob_error"].append({"row": idx + 2, "data": row_dict})

        email_str = str(row["email"]).strip().replace("\u200b", "") if "email" in row else ""
        if email_str and not EMAIL_REGEX.match(email_str):
            errors["email_error"].append({"row": idx + 2, "data": row_dict})

    errors = {k: v for k, v in errors.items() if v}
    if errors:
        return {"status": "failed", "details": errors}
    return {"status": "success"}
