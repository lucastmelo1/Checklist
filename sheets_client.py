from __future__ import annotations

import re
import json
import logging
from typing import Any, Optional, List

import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def normalize_sheet_id(value: str) -> str:
    v = (value or "").strip()
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", v)
    if m:
        return m.group(1)
    return v


def _http_error_details(e: HttpError) -> dict:
    status = getattr(e.resp, "status", None)
    reason = getattr(e.resp, "reason", "")
    try:
        content = e.content.decode("utf-8", errors="ignore")
    except Exception:
        content = str(e)
    return {"status": status, "reason": reason, "content": content}


def _service_account_info_from_secrets() -> dict:
    if "gcp_service_account" not in st.secrets:
        raise RuntimeError("Faltou [gcp_service_account] nos Secrets do Streamlit Cloud.")

    # Converte AttrDict para dict puro
    info = dict(st.secrets["gcp_service_account"])

    # Corrige private_key caso venha com \\n
    pk = info.get("private_key", "")
    if isinstance(pk, str) and "\\n" in pk:
        info["private_key"] = pk.replace("\\n", "\n")

    # Validação mínima
    must = ["type", "project_id", "private_key_id", "private_key", "client_email"]
    missing = [k for k in must if not str(info.get(k, "")).strip()]
    if missing:
        raise RuntimeError(f"Secrets incompleto em [gcp_service_account]. Faltando: {missing}")

    return info


@st.cache_resource(show_spinner=False)
def get_service_cached():
    info = _service_account_info_from_secrets()
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def try_read_header(service, spreadsheet_id: str, tab_name: str, max_cols: int = 26) -> Optional[List[str]]:
    """
    Retorna:
      - None se a aba não existe
      - [] se existe mas está vazia
      - [headers...] se existe e tem header
    """
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)
    tab_name = (tab_name or "").strip()
    if not tab_name:
        return None

    end_col = chr(ord("A") + min(max_cols, 26) - 1)
    rng = f"{tab_name}!A1:{end_col}1"

    try:
        resp = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=rng
        ).execute()
        values = resp.get("values", [])
        if not values:
            return []
        return [str(x).strip() for x in values[0]]

    except HttpError as e:
        det = _http_error_details(e)
        # Aba inexistente costuma virar 400 "Unable to parse range"
        txt = (det.get("content") or "") + " " + str(e)
        if "Unable to parse range" in txt:
            return None

        logger.error("Sheets HttpError try_read_header %s", json.dumps(det, ensure_ascii=False))
        raise RuntimeError(f"Sheets API falhou ao ler header. tab={tab_name} details={det}") from e


def pick_existing_tab(service, spreadsheet_id: str, candidates: list[str]) -> str:
    last_err = None
    for c in candidates:
        try:
            hdr = try_read_header(service, spreadsheet_id, c)
            if hdr is None:
                continue
            return c
        except Exception as e:
            last_err = e

    if last_err:
        raise last_err
    raise RuntimeError(f"Nenhuma aba encontrada nas candidatas: {candidates}")


def read_df(service, spreadsheet_id: str, tab_name: str, a1_range: str = "A:Z") -> pd.DataFrame:
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)
    tab_name = (tab_name or "").strip()
    if not tab_name:
        return pd.DataFrame()

    rng = f"{tab_name}!{a1_range}"

    try:
        resp = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=rng
        ).execute()
        values = resp.get("values", [])
    except HttpError as e:
        det = _http_error_details(e)
        logger.error("Sheets HttpError read_df %s", json.dumps(det, ensure_ascii=False))
        raise RuntimeError(f"Sheets API falhou em read_df. range={rng} details={det}") from e

    if not values:
        return pd.DataFrame()

    headers = [str(h).strip() for h in values[0]]
    rows = values[1:]
    return pd.DataFrame(rows, columns=headers)


def ensure_header(service, spreadsheet_id: str, tab_name: str, header: list[str]):
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)

    hdr = try_read_header(service, spreadsheet_id, tab_name)
    if hdr is None:
        raise RuntimeError(f"Aba '{tab_name}' não existe na planilha {spreadsheet_id}.")

    if hdr and any(str(x).strip() for x in hdr):
        return

    try:
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!A1",
            valueInputOption="RAW",
            body={"values": [header]},
        ).execute()
    except HttpError as e:
        det = _http_error_details(e)
        logger.error("Sheets HttpError ensure_header %s", json.dumps(det, ensure_ascii=False))
        raise RuntimeError(f"Sheets API falhou em ensure_header. details={det}") from e


def append_row(
    service,
    spreadsheet_id: str,
    tab_name: str,
    row: list[Any],
    header_if_empty: Optional[list[str]] = None,
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
        det = _http_error_details(e)
        logger.error("Sheets HttpError append_row %s", json.dumps(det, ensure_ascii=False))
        raise RuntimeError(f"Sheets API falhou em append_row. details={det}") from e
