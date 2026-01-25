from __future__ import annotations

from pathlib import Path
from typing import List, Any, Optional, Dict
import json
import pandas as pd

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def _load_sa_info_from_file(credentials_path: str) -> Optional[Dict[str, Any]]:
    p = Path(credentials_path)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def get_client_email(credentials_path: str = "credentials.json") -> str:
    data = _load_sa_info_from_file(credentials_path)
    if not data:
        return ""
    return str(data.get("client_email", "")).strip()


def get_service(credentials_path: str = "credentials.json"):
    """
    Local:
      - usa credentials.json no disco

    Streamlit Cloud:
      - usa st.secrets["gcp_service_account"] (cole o JSON inteiro lá)
    """
    # Tenta secrets primeiro (Cloud)
    try:
        import streamlit as st  # import local
        if "gcp_service_account" in st.secrets:
            sa = dict(st.secrets["gcp_service_account"])
            creds = Credentials.from_service_account_info(sa, scopes=SCOPES)
            return build("sheets", "v4", credentials=creds, cache_discovery=False)
    except Exception:
        pass

    # Fallback: arquivo local
    p = Path(credentials_path)
    if not p.exists():
        raise FileNotFoundError(f"credentials.json nao encontrado em: {p.resolve()}")
    creds = Credentials.from_service_account_file(str(p), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def list_sheets(service, spreadsheet_id: str) -> List[str]:
    try:
        meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = meta.get("sheets", [])
        return [s.get("properties", {}).get("title", "") for s in sheets]
    except HttpError as e:
        _raise_sheet_http_error(e, spreadsheet_id)
        raise


def _a1(tab_name: str, a1_range: str) -> str:
    # sempre protege com aspas e faz escape de aspas simples duplicando (padrão do Sheets)
    safe = tab_name.replace("'", "''")
    return f"'{safe}'!{a1_range}"


def _raise_sheet_http_error(e: HttpError, spreadsheet_id: str):
    status = getattr(e, "status_code", None) or getattr(getattr(e, "resp", None), "status", None)
    if status in (403, 404):
        msg = (
            f"Sheets API retornou {status} ao acessar a planilha.\n\n"
            f"Spreadsheet ID: {spreadsheet_id}\n\n"
            "Causas mais comuns:\n"
            "1) ID errado\n"
            "2) Service Account sem permissao (Share da planilha nao incluiu o client_email)\n"
            "3) Planilha existe mas esta em outro Google Workspace com restricao\n\n"
            "Acao:\n"
            "- Abra a planilha, clique Share, adicione o client_email da Service Account como Editor.\n"
            "- Se estiver no Streamlit Cloud, garanta que o JSON da Service Account esta em Secrets.\n"
        )
        raise RuntimeError(msg) from e


def read_values(service, spreadsheet_id: str, tab_name: str, a1_range: str = "A:ZZ") -> List[List[Any]]:
    try:
        sheets = list_sheets(service, spreadsheet_id)
        if tab_name not in sheets:
            raise RuntimeError(
                f"Aba '{tab_name}' nao existe na planilha {spreadsheet_id}. Abas encontradas: {sheets}"
            )
        resp = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=_a1(tab_name, a1_range),
        ).execute()
        return resp.get("values", [])
    except HttpError as e:
        _raise_sheet_http_error(e, spreadsheet_id)
        raise


def read_df(service, spreadsheet_id: str, tab_name: str, a1_range: str = "A:ZZ") -> pd.DataFrame:
    rows = read_values(service, spreadsheet_id, tab_name, a1_range)
    if not rows:
        return pd.DataFrame()
    header = rows[0]
    data = rows[1:] if len(rows) > 1 else []
    df = pd.DataFrame(data, columns=header)
    df = df.loc[:, [c for c in df.columns if str(c).strip() != ""]]
    return df


def ensure_header(service, spreadsheet_id: str, tab_name: str, header: List[str]):
    """
    Nao cria aba.
    Se aba existir e estiver vazia, escreve header na linha 1.
    """
    rows = read_values(service, spreadsheet_id, tab_name, "A:ZZ")
    if not rows:
        body = {"values": [header]}
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=_a1(tab_name, "A1"),
            valueInputOption="RAW",
            body=body,
        ).execute()


def append_row(service, spreadsheet_id: str, tab_name: str, row: List[Any]):
    body = {"values": [row]}
    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=_a1(tab_name, "A:ZZ"),
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body=body,
    ).execute()
