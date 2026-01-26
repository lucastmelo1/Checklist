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
    if not v:
        return ""
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", v)
    return m.group(1) if m else v


def _secrets_get(path1: str, path2: str, default=None):
    try:
        if path1 in st.secrets and path2 in st.secrets[path1]:
            return st.secrets[path1][path2]
    except Exception:
        pass
    return default


@st.cache_resource
def get_service_cached():
    """
    Cria o client do Google Sheets diretamente a partir do st.secrets,
    sem escrever arquivo local e sem json.dump (evita AttrDict not JSON serializable).
    """
    sa = _secrets_get("gcp_service_account", "type", None)
    if sa is None:
        raise RuntimeError(
            "Secrets não encontrado: crie [gcp_service_account] no Streamlit Secrets "
            "com o JSON da Service Account (em formato TOML)."
        )

    # st.secrets pode retornar um tipo interno (AttrDict). Convertemos para dict puro.
    sa_dict = dict(st.secrets["gcp_service_account"])

    creds = service_account.Credentials.from_service_account_info(sa_dict, scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return service


def _raise_http_error(prefix: str, spreadsheet_id: str, err: HttpError) -> None:
    status = getattr(err.resp, "status", None)
    reason = getattr(err.resp, "reason", "")
    try:
        content = err.content.decode("utf-8", errors="ignore")
    except Exception:
        content = str(err)

    raise RuntimeError(
        f"{prefix} | spreadsheet_id={spreadsheet_id} | status={status} reason={reason} content={content}"
    ) from err


def list_sheet_titles(service, spreadsheet_id: str) -> List[str]:
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)
    if not spreadsheet_id:
        raise RuntimeError("spreadsheet_id vazio. Verifique RULES_SHEET_ID/CONFIG_SHEET_ID/LOGS_SHEET_ID nos Secrets.")

    try:
        meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    except HttpError as e:
        _raise_http_error("Sheets API falhou ao listar abas (metadata)", spreadsheet_id, e)

    sheets = meta.get("sheets", [])
    return [s.get("properties", {}).get("title", "") for s in sheets if s.get("properties")]


def pick_existing_tab(service, spreadsheet_id: str, candidates: List[str]) -> str:
    titles = list_sheet_titles(service, spreadsheet_id)
    titles_set = set(titles)

    # match exato
    for c in candidates:
        if c in titles_set:
            return c

    # match case-insensitive
    lower_map = {t.lower(): t for t in titles}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]

    raise RuntimeError(
        f"Nenhuma aba candidata encontrada. "
        f"Planilha={normalize_sheet_id(spreadsheet_id)} | "
        f"Candidatas={candidates} | Existentes={sorted(titles)}"
    )


def read_df(service, spreadsheet_id: str, tab_name: str) -> pd.DataFrame:
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)
    if not spreadsheet_id:
        raise RuntimeError("spreadsheet_id vazio ao tentar ler dados. Verifique Secrets.")

    rng = f"{tab_name}"
    try:
        resp = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=rng,
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute()
    except HttpError as e:
        _raise_http_error(f"Sheets API falhou ao ler aba '{tab_name}'", spreadsheet_id, e)

    values = resp.get("values", [])
    if not values or len(values) < 1:
        return pd.DataFrame()

    header = [str(x).strip() for x in values[0]]
    rows = values[1:] if len(values) > 1 else []
    df = pd.DataFrame(rows, columns=header)
    return df


def append_row(service, spreadsheet_id: str, tab_name: str, row: list, header_if_empty: Optional[list] = None) -> None:
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)
    if not spreadsheet_id:
        raise RuntimeError("spreadsheet_id vazio ao tentar gravar log. Verifique Secrets.")

    # Se quiser garantir header quando vazio
    if header_if_empty:
        df = read_df(service, spreadsheet_id, tab_name)
        if df.empty:
            try:
                service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=f"{tab_name}!A1",
                    valueInputOption="RAW",
                    body={"values": [header_if_empty]},
                ).execute()
            except HttpError as e:
                _raise_http_error(f"Falhou ao criar header em '{tab_name}'", spreadsheet_id, e)

    try:
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [row]},
        ).execute()
    except HttpError as e:
        _raise_http_error(f"Falhou ao append em '{tab_name}'", spreadsheet_id, e)
