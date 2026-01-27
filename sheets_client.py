import re
from typing import List, Optional

import pandas as pd
import streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def normalize_sheet_id(value: str) -> str:
    v = (value or "").strip()
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", v)
    if m:
        return m.group(1)
    return v


@st.cache_resource
def get_service_cached():
    if "gcp_service_account" not in st.secrets:
        raise RuntimeError("Secrets ausente: [gcp_service_account].")

    info = dict(st.secrets["gcp_service_account"])
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _http_error_details(e: HttpError) -> dict:
    status = getattr(e.resp, "status", None)
    reason = getattr(e.resp, "reason", "")
    try:
        content = e.content.decode("utf-8", errors="ignore")
    except Exception:
        content = str(e)
    return {"status": status, "reason": reason, "content": content}


def list_sheet_titles(service, spreadsheet_id: str) -> List[str]:
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)
    try:
        meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    except HttpError as e:
        d = _http_error_details(e)
        raise RuntimeError(
            f"Sheets API falhou ao listar abas. spreadsheet_id={spreadsheet_id} details={d}"
        ) from e

    sheets = meta.get("sheets", [])
    return [s.get("properties", {}).get("title", "") for s in sheets if s.get("properties")]


def pick_existing_tab(service, spreadsheet_id: str, candidates: List[str]) -> str:
    titles = list_sheet_titles(service, spreadsheet_id)
    title_set = set(titles)

    for c in candidates:
        if c in title_set:
            return c

    lower_map = {t.lower(): t for t in titles}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]

    raise RuntimeError(
        f"Não encontrei aba válida. spreadsheet_id={spreadsheet_id} candidatas={candidates} existentes={sorted(titles)}"
    )


def read_df(service, spreadsheet_id: str, tab_name: str) -> pd.DataFrame:
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)
    rng = f"{tab_name}!A:ZZ"
    try:
        resp = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    except HttpError as e:
        d = _http_error_details(e)
        raise RuntimeError(f"Sheets API falhou ao ler dados. tab={tab_name} details={d}") from e

    values = resp.get("values", [])
    if not values:
        return pd.DataFrame()

    header = values[0]
    rows = values[1:] if len(values) > 1 else []
    df = pd.DataFrame(rows, columns=header)

    df = df.replace("", pd.NA)
    return df


def _sheet_has_any_value(service, spreadsheet_id: str, tab_name: str) -> bool:
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)
    rng = f"{tab_name}!A1:A1"
    try:
        resp = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    except HttpError:
        return False
    values = resp.get("values", [])
    return bool(values and values[0] and str(values[0][0]).strip())


def _write_header_if_empty(service, spreadsheet_id: str, tab_name: str, header: List[str]) -> None:
    if _sheet_has_any_value(service, spreadsheet_id, tab_name):
        return

    spreadsheet_id = normalize_sheet_id(spreadsheet_id)
    body = {"values": [header]}
    try:
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!A1",
            valueInputOption="RAW",
            body=body,
        ).execute()
    except HttpError as e:
        d = _http_error_details(e)
        raise RuntimeError(f"Sheets API falhou ao escrever header. tab={tab_name} details={d}") from e


def append_row(
    service,
    spreadsheet_id: str,
    tab_name: str,
    row: List,
    header_if_empty: Optional[List[str]] = None,
) -> None:
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)

    if header_if_empty:
        _write_header_if_empty(service, spreadsheet_id, tab_name, header_if_empty)

    body = {"values": [row]}
    try:
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!A:ZZ",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body=body,
        ).execute()
    except HttpError as e:
        d = _http_error_details(e)
        raise RuntimeError(f"Sheets API falhou ao append. tab={tab_name} details={d}") from e
