import re
from googleapiclient.errors import HttpError

def normalize_sheet_id(value: str) -> str:
    v = (value or "").strip()
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", v)
    if m:
        return m.group(1)
    return v

def list_sheet_titles(service, spreadsheet_id: str):
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)

    try:
        meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    except HttpError as e:
        status = getattr(e.resp, "status", None)
        reason = getattr(e.resp, "reason", "")
        try:
            content = e.content.decode("utf-8", errors="ignore")
        except Exception:
            content = str(e)

        # isso vai aparecer no log do Streamlit Cloud
        raise RuntimeError(
            f"Sheets API HttpError status={status} reason={reason} content={content}"
        ) from e

    sheets = meta.get("sheets", [])
    return [s.get("properties", {}).get("title", "") for s in sheets if s.get("properties")]
