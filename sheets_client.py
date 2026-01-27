import os
import re
from typing import List, Optional, Any

import pandas as pd
import streamlit as st

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


def normalize_sheet_id(value: str) -> str:
    v = (value or "").strip()
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", v)
    return m.group(1) if m else v


@st.cache_resource
def get_service_cached():
    """
    Cria cliente do Google Sheets usando st.secrets["gcp_service_account"].
    """
    if "gcp_service_account" not in st.secrets:
        raise RuntimeError("Faltou configurar [gcp_service_account] no secrets.toml")

    info = dict(st.secrets["gcp_service_account"])
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def list_sheet_titles(service, spreadsheet_id: str) -> List[str]:
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)
    try:
        meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    except HttpError as e:
        raise RuntimeError(_format_http_error("list_sheet_titles", e)) from e

    sheets = meta.get("sheets", [])
    return [
        s.get("properties", {}).get("title", "")
        for s in sheets
        if s.get("properties")
    ]


def ensure_tab_exists(service, spreadsheet_id: str, tab_name: str):
    """
    Garante que exista a aba tab_name. Se não existir, cria.
    """
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)
    titles = set(list_sheet_titles(service, spreadsheet_id))
    if tab_name in titles:
        return

    body = {
        "requests": [
            {"addSheet": {"properties": {"title": tab_name}}}
        ]
    }
    try:
        service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
    except HttpError as e:
        raise RuntimeError(_format_http_error("ensure_tab_exists(addSheet)", e)) from e


def read_values(service, spreadsheet_id: str, tab: str, a1_range: str = "A:ZZ") -> List[List[Any]]:
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)
    rng = f"{tab}!{a1_range}"
    try:
        resp = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=rng,
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute()
    except HttpError as e:
        raise RuntimeError(_format_http_error(f"read_values({rng})", e)) from e

    return resp.get("values", [])


def read_df(service, spreadsheet_id: str, tab: str) -> pd.DataFrame:
    """
    Lê uma aba inteira e retorna DataFrame (linha 1 = header).
    """
    vals = read_values(service, spreadsheet_id, tab, "A:ZZ")
    if not vals:
        return pd.DataFrame()

    header = [str(x).strip() for x in vals[0]]
    rows = vals[1:] if len(vals) > 1 else []

    df = pd.DataFrame(rows, columns=header)
    return df


def append_row(service, spreadsheet_id: str, tab: str, row: List[Any], header_if_empty: Optional[List[str]] = None):
    """
    Append de 1 linha.
    Se a aba estiver vazia e header_if_empty for fornecido, escreve header antes.
    """
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)

    # Garantir aba existe
    ensure_tab_exists(service, spreadsheet_id, tab)

    # Se vazio, cria header
    vals = read_values(service, spreadsheet_id, tab, "A1:ZZ1")
    is_empty = (not vals) or (len(vals) == 0) or (len(vals[0]) == 0)

    if is_empty and header_if_empty:
        try:
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{tab}!A1",
                valueInputOption="RAW",
                body={"values": [header_if_empty]},
            ).execute()
        except HttpError as e:
            raise RuntimeError(_format_http_error("append_row(write_header)", e)) from e

    try:
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{tab}!A:ZZ",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [row]},
        ).execute()
    except HttpError as e:
        raise RuntimeError(_format_http_error("append_row(append)", e)) from e


def _format_http_error(ctx: str, e: HttpError) -> str:
    status = getattr(e.resp, "status", None)
    reason = getattr(e.resp, "reason", "")
    try:
        content = e.content.decode("utf-8", errors="ignore")
    except Exception:
        content = str(e)
    return f"Sheets API HttpError em {ctx} | status={status} reason={reason} content={content}"
