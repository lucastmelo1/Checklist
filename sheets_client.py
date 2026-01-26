import os
import re
import json
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


def _http_error_details(e: HttpError) -> dict:
    status = getattr(e.resp, "status", None)
    reason = getattr(e.resp, "reason", "")
    try:
        content = e.content.decode("utf-8", errors="ignore")
    except Exception:
        content = str(e)
    return {"status": status, "reason": reason, "content": content}


@st.cache_resource
def get_service_cached():
    """
    Cria o client do Google Sheets usando st.secrets["gcp_service_account"].
    Não escreve arquivo JSON local (evita erro AttrDict não serializável).
    """
    if not hasattr(st, "secrets") or "gcp_service_account" not in st.secrets:
        raise RuntimeError(
            "Secrets não configurado. Crie em Streamlit -> Settings -> Secrets:\n"
            "[gcp_service_account] com a service account JSON.\n"
            "E em [app] defina CONFIG_SHEET_ID, RULES_SHEET_ID, LOGS_SHEET_ID."
        )

    # streamlit secrets é um AttrDict, converte para dict puro
    info = dict(st.secrets["gcp_service_account"])

    # Garante que private_key tenha quebras corretas (caso venha com \\n)
    pk = info.get("private_key", "")
    if isinstance(pk, str) and "\\n" in pk:
        info["private_key"] = pk.replace("\\n", "\n")

    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return service


def list_sheet_titles(service, spreadsheet_id: str) -> List[str]:
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)
    try:
        meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    except HttpError as e:
        det = _http_error_details(e)
        raise RuntimeError(
            f"Sheets API HttpError em list_sheet_titles | spreadsheet_id={spreadsheet_id} | details={json.dumps(det)}"
        ) from e

    sheets = meta.get("sheets", [])
    return [s.get("properties", {}).get("title", "") for s in sheets if s.get("properties")]


def read_df(service, spreadsheet_id: str, tab_name: str) -> pd.DataFrame:
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)

    if not tab_name:
        return pd.DataFrame()

    # Range: aba inteira
    rng = f"'{tab_name}'"

    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=rng,
            valueRenderOption="UNFORMATTED_VALUE",
            dateTimeRenderOption="FORMATTED_STRING",
        ).execute()
    except HttpError as e:
        det = _http_error_details(e)
        raise RuntimeError(
            f"Sheets API HttpError em read_df | spreadsheet_id={spreadsheet_id} | tab={tab_name} | range={rng} | details={json.dumps(det)}"
        ) from e

    values = result.get("values", [])
    if not values:
        return pd.DataFrame()

    headers = [str(h).strip() for h in values[0]]
    rows = values[1:] if len(values) > 1 else []
    df = pd.DataFrame(rows, columns=headers)
    return df


def try_read_header(service, spreadsheet_id: str, tab_name: str) -> Optional[List[str]]:
    """
    Tenta ler A1:Z1 da aba. Se a aba não existir, retorna None.
    Se existir mas der erro de permissão/id, levanta RuntimeError com detalhes.
    """
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)
    rng = f"'{tab_name}'!A1:Z1"

    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=rng,
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute()
        vals = result.get("values", [])
        if not vals:
            return []
        return [str(x).strip() for x in vals[0]]
    except HttpError as e:
        det = _http_error_details(e)
        # Se for erro típico de aba/range inválido, tratamos como "aba não existe"
        content = (det.get("content") or "").lower()
        if "unable to parse range" in content or "badrequest" in content:
            return None

        raise RuntimeError(
            f"Sheets API falhou ao ler header | spreadsheet_id={spreadsheet_id} | tab={tab_name} | range={rng} | details={json.dumps(det)}"
        ) from e


def pick_existing_tab(service, spreadsheet_id: str, candidates: List[str]) -> str:
    """
    Escolhe a primeira aba existente.
    Estratégia:
    - tenta ler header A1:Z1 para cada candidata (mais confiável do que meta em alguns casos)
    - se nenhuma, cai para list_sheet_titles
    """
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)

    last_err = None
    for c in candidates:
        try:
            hdr = try_read_header(service, spreadsheet_id, c)
            if hdr is not None:
                return c
        except Exception as e:
            last_err = e

    # fallback: títulos do metadata
    try:
        titles = set(list_sheet_titles(service, spreadsheet_id))
        for c in candidates:
            if c in titles:
                return c
        lower_map = {t.lower(): t for t in titles}
        for c in candidates:
            if c.lower() in lower_map:
                return lower_map[c.lower()]
    except Exception as e:
        last_err = e

    if last_err:
        raise last_err

    raise RuntimeError(
        f"Não encontrei aba válida | spreadsheet_id={spreadsheet_id} | candidates={candidates}"
    )


def _get_next_row_index(service, spreadsheet_id: str, tab_name: str) -> int:
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)
    rng = f"'{tab_name}'!A:A"
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=rng
        ).execute()
    except HttpError as e:
        det = _http_error_details(e)
        raise RuntimeError(
            f"Sheets API HttpError ao calcular next_row | spreadsheet_id={spreadsheet_id} | tab={tab_name} | details={json.dumps(det)}"
        ) from e

    values = result.get("values", [])
    # values inclui header e linhas preenchidas, próximo índice é len(values)+1 (1-based)
    return len(values) + 1


def append_row(service, spreadsheet_id: str, tab_name: str, row: list, header_if_empty: Optional[list] = None):
    """
    Faz append no final.
    Se sheet estiver vazio e header_if_empty for fornecido, escreve o header primeiro.
    """
    spreadsheet_id = normalize_sheet_id(spreadsheet_id)

    # Se precisar criar header, checa A1
    if header_if_empty:
        try:
            hdr = try_read_header(service, spreadsheet_id, tab_name)
            if hdr == []:
                # sheet existe mas está vazia, grava header em A1
                rng_header = f"'{tab_name}'!A1"
                service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=rng_header,
                    valueInputOption="RAW",
                    body={"values": [header_if_empty]},
                ).execute()
        except HttpError as e:
            det = _http_error_details(e)
            raise RuntimeError(
                f"Sheets API HttpError ao escrever header | spreadsheet_id={spreadsheet_id} | tab={tab_name} | details={json.dumps(det)}"
            ) from e

    try:
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"'{tab_name}'!A:Z",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [row]},
        ).execute()
    except HttpError as e:
        det = _http_error_details(e)
        raise RuntimeError(
            f"Sheets API HttpError em append_row | spreadsheet_id={spreadsheet_id} | tab={tab_name} | details={json.dumps(det)}"
        ) from e
