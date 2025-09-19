import json
import requests
import os

TEAMS_WEBHOOK_URL = os.getenv("TEAMS_WEBHOOK_URL")

def send_teams_message(file_id: str, status: str, errors: dict = None) -> str:
    if not TEAMS_WEBHOOK_URL:
        return "TEAMS_WEBHOOK_URL not configured "

    message = f"**File ID:** {file_id}\n**Status:** {status}"

    if errors:
        message += "\n**Errors:**"
        for key, value in errors.items():
            if isinstance(value, dict):
                message += f"\n- {key}:"
                for sub_key, sub_val in value.items():
                    message += f"\n    - {sub_key} → rows {sub_val}"
            elif isinstance(value, list):
                message += f"\n- {key} → {value}"
            else:
                message += f"\n- {key} → {value}"

    payload = {"text": message}

    try:
        response = requests.post(
            TEAMS_WEBHOOK_URL,
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload)
        )
        if response.status_code == 200:
            return f"Message sent to Teams for file_id: {file_id} "
        else:
            return f"Failed to send message: {response.status_code} {response.text}"
    except Exception as e:
        return f"Error sending message: {e}"
