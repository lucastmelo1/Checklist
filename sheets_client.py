from __future__ import annotations

import re
from typing import List, Any, Optional

import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def normalize_sheet_id(value: str) -> str:
    v = (value or "").strip()
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", v)
    if m:
        return m.group(1)
    return v


def _service_account_info_from_secrets() -> dict:
    """
    Espera st.secrets["gcp_service_account"] como um TOML table.
    Converte para dict "normal" (evita AttrDict não serializável).
    """
    if "gcp_service_account" not in st.secrets:
        raise RuntimeError(
            "Secrets não configurado: faltou [gcp_service_account] no Streamlit Secrets."
        )

    sa = st.secrets["gcp_service_account"]
    info = dict(sa)  # <- isso resolve o erro AttrDict não serializable

    # Em alguns casos a chave vem com "\\n". Garantimos que vira \n real.
    pk = info.get("private_key", "")
    if isinstance(pk, str) and "\\n" in pk:
        info["private_key"] = pk.replace("\\n", "\n")

    return info


@st.cache_resource(show_spinner=False)
def get_service_cached():
    info = _service_account_info_from_secrets()
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def list_sheet_titles(service, spreadsheet_id: str) -> List[str]:
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
        raise RuntimeError(
            f"Sheets API HttpError em list_sheet_titles | status={status} reason={reason} content={content}"
        ) from e

    sheets = meta.get("sheets", [])
    return [s.get("properties", {}).get("title", "") for s in sheets if s.get("properties")]


def _get_values(service, spreadsheet_id: str, range_a1: str) -> list:
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)
    try:
        resp = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=range_a1
        ).execute()
        return resp.get("values", [])
    except HttpError as e:
        status = getattr(e.resp, "status", None)
        reason = getattr(e.resp, "reason", "")
        try:
            content = e.content.decode("utf-8", errors="ignore")
        except Exception:
            content = str(e)
        raise RuntimeError(
            f"Sheets API HttpError em GET values | range={range_a1} | status={status} reason={reason} content={content}"
        ) from e


def read_df(service, spreadsheet_id: str, tab_name: str, a1_range: str = "A:Z") -> pd.DataFrame:
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)
    tab_name = (tab_name or "").strip()
    if not tab_name:
        return pd.DataFrame()

    values = _get_values(service, spreadsheet_id, f"{tab_name}!{a1_range}")
    if not values:
        return pd.DataFrame()

    headers = [str(h).strip() for h in values[0]]
    rows = values[1:]
    df = pd.DataFrame(rows, columns=headers)
    return df


def ensure_header(service, spreadsheet_id: str, tab_name: str, header: List[str]):
    """
    Se a aba estiver vazia, escreve o cabeçalho na linha 1.
    """
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)
    existing = _get_values(service, spreadsheet_id, f"{tab_name}!1:1")
    if existing and len(existing) > 0 and any(str(x).strip() for x in existing[0]):
        return

    try:
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!A1",
            valueInputOption="RAW",
            body={"values": [header]},
        ).execute()
    except HttpError as e:
        status = getattr(e.resp, "status", None)
        reason = getattr(e.resp, "reason", "")
        try:
            content = e.content.decode("utf-8", errors="ignore")
        except Exception:
            content = str(e)
        raise RuntimeError(
            f"Sheets API HttpError em ensure_header | status={status} reason={reason} content={content}"
        ) from e


def append_row(
    service,
    spreadsheet_id: str,
    tab_name: str,
    row: List[Any],
    header_if_empty: Optional[List[str]] = None,
):
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)

    if header_if_empty:
        ensure_header(service, spreadsheet_id, tab_name, header_if_empty)

    try:
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!A:Z",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [row]},
        ).execute()
    except HttpError as e:
        status = getattr(e.resp, "status", None)
        reason = getattr(e.resp, "reason", "")
        try:
            content = e.content.decode("utf-8", errors="ignore")
        except Exception:
            content = str(e)
        raise RuntimeError(
            f"Sheets API HttpError em append_row | status={status} reason={reason} content={content}"
        ) from e
