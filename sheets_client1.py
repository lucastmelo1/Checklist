import time
import re
from typing import Optional

import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def normalize_sheet_id(value: str) -> str:
    v = (value or "").strip()
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", v)
    if m:
        return m.group(1)
    return v


def _retryable(fn, *, tries=6, base_sleep=0.8, max_sleep=8.0):
    """
    Retry com backoff exponencial para APIError, principalmente 429.
    """
    last = None
    for i in range(tries):
        try:
            return fn()
        except APIError as e:
            last = e
            msg = str(e)
            # gspread coloca status no texto, mas nem sempre padronizado
            is_quota = ("429" in msg) or ("Quota exceeded" in msg) or ("RESOURCE_EXHAUSTED" in msg)
            if not is_quota and i >= 1:
                # para outros erros, não fica insistindo muito
                raise

            sleep_s = min(max_sleep, base_sleep * (2 ** i))
            time.sleep(sleep_s)
    raise last


@st.cache_resource
def get_gspread_client():
    if "gcp_service_account" not in st.secrets:
        raise RuntimeError("Secrets precisa ter [gcp_service_account].")

    info = dict(st.secrets["gcp_service_account"])
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client


@st.cache_data(ttl=900)  # 15 minutos
def list_sheet_titles_cached(client, spreadsheet_id: str):
    sid = normalize_sheet_id(spreadsheet_id)

    def _do():
        sh = client.open_by_key(sid)
        return [ws.title for ws in sh.worksheets()]

    return _retryable(_do)


def get_or_create_worksheet(sh, title: str, rows: int = 2000, cols: int = 20):
    try:
        return sh.worksheet(title)
    except Exception:
        return sh.add_worksheet(title=title, rows=str(rows), cols=str(cols))


def read_df(client, spreadsheet_id: str, worksheet_title: str, last_n: Optional[int] = None) -> pd.DataFrame:
    sid = normalize_sheet_id(spreadsheet_id)

    def _do():
        sh = client.open_by_key(sid)
        ws = sh.worksheet(worksheet_title)

        if last_n and last_n > 0:
            # pega tudo e recorta no client, mas reduz chamadas (1 chamada)
            values = ws.get_all_values()
            if not values:
                return pd.DataFrame()
            header = values[0]
            body = values[1:]
            if last_n < len(body):
                body = body[-last_n:]
            return pd.DataFrame(body, columns=header)

        # leitura normal
        values = ws.get_all_values()
        if not values:
            return pd.DataFrame()
        return pd.DataFrame(values[1:], columns=values[0])

    return _retryable(_do)


def append_row(client, spreadsheet_id: str, worksheet_title: str, row: list, header_if_empty: Optional[list] = None):
    sid = normalize_sheet_id(spreadsheet_id)

    def _do():
        sh = client.open_by_key(sid)
        ws = sh.worksheet(worksheet_title)

        # se vazio, cria header
        if header_if_empty:
            first = ws.row_values(1)
            if not first or all(str(x).strip() == "" for x in first):
                ws.append_row(header_if_empty, value_input_option="RAW")

        ws.append_row(row, value_input_option="RAW")

    return _retryable(_do)
