import re
from typing import List, Optional, Any

import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials


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
    Retorna um client gspread autenticado via st.secrets["gcp_service_account"].
    """
    if "gcp_service_account" not in st.secrets:
        raise RuntimeError("Faltou configurar [gcp_service_account] no secrets.toml")

    info = dict(st.secrets["gcp_service_account"])
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)


def list_sheet_titles(service, spreadsheet_id: str) -> List[str]:
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)
    sh = service.open_by_key(spreadsheet_id)
    return [ws.title for ws in sh.worksheets()]


def ensure_tab_exists(service, spreadsheet_id: str, tab_name: str, rows: int = 1000, cols: int = 26):
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)
    sh = service.open_by_key(spreadsheet_id)
    titles = [ws.title for ws in sh.worksheets()]
    if tab_name in titles:
        return
    sh.add_worksheet(title=tab_name, rows=rows, cols=cols)


def _read_all_values(service, spreadsheet_id: str, tab: str) -> List[List[Any]]:
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)
    sh = service.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab)
    vals = ws.get_all_values()
    return vals or []


def read_df(service, spreadsheet_id: str, tab: str) -> pd.DataFrame:
    """
    Lê a aba inteira como DataFrame.
    Linha 1 vira header.
    """
    vals = _read_all_values(service, spreadsheet_id, tab)
    if not vals:
        return pd.DataFrame()
    header = [str(x).strip() for x in vals[0]]
    rows = vals[1:] if len(vals) > 1 else []
    return pd.DataFrame(rows, columns=header)


def append_row(service, spreadsheet_id: str, tab: str, row: List[Any], header_if_empty: Optional[List[str]] = None):
    """
    Append de 1 linha na aba.
    Se estiver vazia e header_if_empty for informado, cria header e depois faz append.
    """
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)

    ensure_tab_exists(service, spreadsheet_id, tab)

    sh = service.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab)

    existing = ws.get_all_values()
    is_empty = (not existing) or (len(existing) == 0) or (len(existing[0]) == 0)

    if is_empty and header_if_empty:
        ws.update("A1", [header_if_empty])

    ws.append_row(row, value_input_option="RAW")
