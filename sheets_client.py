import json
from pathlib import Path

import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


def _secrets_sa_dict():
    # st.secrets["gcp_service_account"] vira AttrDict, convertemos para dict puro
    sa = st.secrets["gcp_service_account"]
    try:
        return dict(sa)
    except Exception:
        return {k: sa[k] for k in sa.keys()}


@st.cache_resource
def get_service_cached():
    # Streamlit Cloud: secrets
    if hasattr(st, "secrets") and "gcp_service_account" in st.secrets:
        sa = _secrets_sa_dict()
        creds = Credentials.from_service_account_info(sa, scopes=SCOPES)
        return build("sheets", "v4", credentials=creds, cache_discovery=False)

    # Local: arquivo credentials.json na pasta do app
    cred_path = Path(__file__).resolve().parent / "credentials.json"
    if not cred_path.exists():
        raise FileNotFoundError("credentials.json não encontrado. No Streamlit Cloud, configure st.secrets[gcp_service_account].")
    creds = Credentials.from_service_account_file(str(cred_path), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def list_sheet_titles(service, spreadsheet_id: str) -> list[str]:
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = meta.get("sheets", [])
    return [s["properties"]["title"] for s in sheets]


def read_values(service, spreadsheet_id: str, tab_name: str, a1_range: str = "A:ZZ") -> list[list[str]]:
    rng = f"{tab_name}!{a1_range}"
    resp = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    return resp.get("values", [])


def read_df(service, spreadsheet_id: str, tab_name: str) -> pd.DataFrame:
    if not tab_name:
        return pd.DataFrame()
    rows = read_values(service, spreadsheet_id, tab_name, "A:ZZ")
    if not rows:
        return pd.DataFrame()
    header = [str(c).strip() for c in rows[0]]
    data = rows[1:] if len(rows) > 1 else []
    df = pd.DataFrame(data, columns=header)
    return df


def append_row(service, spreadsheet_id: str, tab_name: str, row: list, header_if_empty: list[str] | None = None):
    # Se vazio, tenta criar header na linha 1
    try:
        rows = read_values(service, spreadsheet_id, tab_name, "A1:ZZ1")
        if (not rows or not rows[0]) and header_if_empty:
            _update_values(service, spreadsheet_id, tab_name, "A1", [header_if_empty])
    except HttpError:
        # se aba não existe, falha claramente
        raise RuntimeError(f"Aba '{tab_name}' não encontrada na planilha {spreadsheet_id}. Crie a aba antes.")

    body = {"values": [row]}
    rng = f"{tab_name}!A:ZZ"
    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=rng,
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body,
    ).execute()


def _update_values(service, spreadsheet_id: str, tab_name: str, start_a1: str, values: list[list]):
    rng = f"{tab_name}!{start_a1}"
    body = {"values": values}
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=rng,
        valueInputOption="USER_ENTERED",
        body=body,
    ).execute()
