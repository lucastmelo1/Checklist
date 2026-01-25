import os
import time
import re
from datetime import datetime, date
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st

from sheets_client import get_service_cached, read_df, append_row, list_sheet_titles, normalize_sheet_id
from auth import authenticate_user

TZ = ZoneInfo("America/Sao_Paulo")

def _get_cfg(name: str, default: str = "") -> str:
    if hasattr(st, "secrets") and "app" in st.secrets and name in st.secrets["app"]:
        return str(st.secrets["app"][name]).strip()
    return os.getenv(name, default).strip()

CONFIG_SHEET_ID = _get_cfg("CONFIG_SHEET_ID", "")
RULES_SHEET_ID = _get_cfg("RULES_SHEET_ID", "")
LOGS_SHEET_ID = _get_cfg("LOGS_SHEET_ID", "")

WS_AREAS_CANDIDATES = ["Areas", "AREAS", "Checklist_Areas", "CHECKLIST_AREAS"]
WS_ITENS_CANDIDATES = ["Itens", "ITENS", "Checklist_Itens", "CHECKLIST_ITENS"]
WS_USERS_CANDIDATES = ["Users", "USERS", "Usuarios", "USUARIOS", "Login", "LOGIN", "USUARIOS"]
WS_PARAMS_CANDIDATES = ["Params", "PARAMS", "Regras", "REGRAS", "Rules", "RULES", "PARAMETROS", "PARAMETROS"]
WS_LOGS_CANDIDATES = ["Checklist_Logs", "CHECKLIST_LOGS", "Logs", "LOGS"]

COL_AREA = "area"
COL_TURNO = "turno"
COL_ITEM_ID = "item_id"
COL_TEXTO = "texto"
COL_ATIVO = "ativo"
COL_ORDEM = "ordem"
COL_DEADLINE = "deadline"
COL_TOL_MIN = "tolerancia_min"
COL_DIA_SEMANA = "dia_semana"

LOG_COLS = [
    "ts", "data", "dia_semana", "area", "turno", "item_id", "texto",
    "status", "user", "deadline", "tolerancia_min",
]

TURNOS_DEFAULT = ["Almoço", "Jantar"]

st.set_page_config(page_title="Checklist Operacional", layout="wide", initial_sidebar_state="expanded")

MOBILE_CSS = """
<style>
.block-container { padding-top: 1.2rem; padding-left: 0.9rem; padding-right: 0.9rem; }
header[data-testid="stHeader"] { height: 0px !important; }
div[data-testid="stToolbar"] { visibility: hidden; height: 0px; }
footer { visibility: hidden; height: 0px; }
h1,h2,h3 { margin-top: 0.6rem; }
div[data-testid="stSidebar"] { width: 280px; }
@media (max-width: 700px){
  .block-container { padding-top: 1.6rem; }
  h1 { font-size: 1.35rem !important; }
  h2 { font-size: 1.10rem !important; }
  h3 { font-size: 1.00rem !important; }
  .stButton button { width: 100%; }
}
</style>
"""
st.markdown(MOBILE_CSS, unsafe_allow_html=True)

def _now_ts() -> datetime:
    return datetime.now(TZ)

def _weekday_pt(d: date) -> str:
    names = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
    return names[d.weekday()]

def _coerce_bool(x):
    if pd.isna(x):
        return False
    s = str(x).strip().lower()
    return s in ["1", "true", "sim", "yes", "y", "ativo"]

def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df

def _pick_first_existing_tab(service, spreadsheet_id: str, candidates: list[str]) -> str:
    titles = set(list_sheet_titles(service, spreadsheet_id))
    for c in candidates:
        if c in titles:
            return c
    lower_map = {t.lower(): t for t in titles}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    raise RuntimeError(
        f"Não encontrei nenhuma aba válida. Planilha={spreadsheet_id} "
        f"Candidatas={candidates} Existentes={sorted(titles)}"
    )

@st.cache_resource
def service_client():
    return get_service_cached()

@st.cache_data(ttl=5)
def load_tables(cache_buster: str, config_sheet_id: str, rules_sheet_id: str, logs_sheet_id: str):
    svc = service_client()

    ws_areas = _pick_first_existing_tab(svc, config_sheet_id, WS_AREAS_CANDIDATES)
    ws_itens = _pick_first_existing_tab(svc, config_sheet_id, WS_ITENS_CANDIDATES)
    ws_users = _pick_first_existing_tab(svc, rules_sheet_id, WS_USERS_CANDIDATES)

    ws_params = None
    try:
        ws_params = _pick_first_existing_tab(svc, rules_sheet_id, WS_PARAMS_CANDIDATES)
    except Exception:
        ws_params = None

    ws_logs = _pick_first_existing_tab(svc, logs_sheet_id, WS_LOGS_CANDIDATES)

    areas_df = _normalize_cols(read_df(svc, config_sheet_id, ws_areas))
    itens_df = _normalize_cols(read_df(svc, config_sheet_id, ws_itens))
    users_df = _normalize_cols(read_df(svc, rules_sheet_id, ws_users))
    params_df = _normalize_cols(read_df(svc, rules_sheet_id, ws_params)) if ws_params else pd.DataFrame()
    logs_df = _normalize_cols(read_df(svc, logs_sheet_id, ws_logs))

    return {
        "ws_areas": ws_areas,
        "ws_itens": ws_itens,
        "ws_users": ws_users,
        "ws_params": ws_params,
        "ws_logs": ws_logs,
        "areas_df": areas_df,
        "itens_df": itens_df,
        "users_df": users_df,
        "params_df": params_df,
        "logs_df": logs_df,
    }

def _validate_itens_columns(itens_df: pd.DataFrame):
    needed = {COL_AREA, COL_TURNO, COL_ITEM_ID, COL_TEXTO}
    missing = [c for c in needed if c not in itens_df.columns]
    if missing:
        raise RuntimeError(
            f"A aba ITENS precisa ter colunas {sorted(list(needed))}. "
            f"Faltando {missing}. Colunas atuais {list(itens_df.columns)}"
        )

def _available_areas(areas_df: pd.DataFrame, itens_df: pd.DataFrame) -> list[str]:
    if "area" in areas_df.columns and areas_df["area"].notna().any():
        vals = sorted({str(x).strip() for x in areas_df["area"].dropna().tolist() if str(x).strip()})
        if vals:
            return vals
    return sorted({str(x).strip() for x in itens_df[COL_AREA].dropna().tolist() if str(x).strip()})

def _available_turnos(itens_df: pd.DataFrame) -> list[str]:
    vals = sorted({str(x).strip() for x in itens_df[COL_TURNO].dropna().tolist() if str(x).strip()})
    return vals if vals else TURNOS_DEFAULT

def _filter_items_for(itens_df: pd.DataFrame, area: str, turno: str, dia_semana: str) -> pd.DataFrame:
    df = itens_df.copy()
    df[COL_AREA] = df[COL_AREA].astype(str).str.strip()
    df[COL_TURNO] = df[COL_TURNO].astype(str).str.strip()
    df = df[(df[COL_AREA] == area) & (df[COL_TURNO] == turno)]

    if COL_ATIVO in df.columns:
        df = df[df[COL_ATIVO].apply(_coerce_bool) == True]

    if COL_DIA_SEMANA in df.columns and df[COL_DIA_SEMANA].notna().any():
        dcol = df[COL_DIA_SEMANA].astype(str).str.strip()
        df = df[(dcol == "") | (dcol.str.lower() == "nan") | (dcol == dia_semana)]

    if COL_ORDEM in df.columns:
        df[COL_ORDEM] = pd.to_numeric(df[COL_ORDEM], errors="coerce")
        df = df.sort_values([COL_ORDEM, COL_ITEM_ID], na_position="last")
    else:
        df = df.sort_values([COL_ITEM_ID])

    if COL_TOL_MIN not in df.columns:
        df[COL_TOL_MIN] = 0
    else:
        df[COL_TOL_MIN] = pd.to_numeric(df[COL_TOL_MIN], errors="coerce").fillna(0).astype(int)

    return df.reset_index(drop=True)

def _invalidate_cache():
    st.cache_data.clear()

def page_diagnostico():
    st.subheader("Diagnóstico de acesso (Google Sheets)")

    st.write("IDs lidos do Secrets (normalizados):")
    st.code(
        "\n".join([
            f"CONFIG_SHEET_ID = {normalize_sheet_id(CONFIG_SHEET_ID)}",
            f"RULES_SHEET_ID  = {normalize_sheet_id(RULES_SHEET_ID)}",
            f"LOGS_SHEET_ID   = {normalize_sheet_id(LOGS_SHEET_ID)}",
        ])
    )

    svc = service_client()

    def test_one(label: str, sid: str):
        st.write(f"Teste: {label}")
        try:
            titles = list_sheet_titles(svc, sid)
            st.success(f"OK. Abas encontradas: {titles}")
        except Exception as e:
            st.error("Falhou. Isso é o que precisa corrigir (403 ou 404).")
            st.code(str(e))

    test_one("CONFIG", CONFIG_SHEET_ID)
    test_one("RULES", RULES_SHEET_ID)
    test_one("LOGS", LOGS_SHEET_ID)

def page_dashboard(tables, dia_str: str):
    st.subheader("Dashboard operacional")
    areas_df = tables["areas_df"]
    itens_df = tables["itens_df"]
    _validate_itens_columns(itens_df)

    areas = _available_areas(areas_df, itens_df)
    turnos = _available_turnos(itens_df)

    st.write("OK. Dados carregados.")
    st.write({"areas": areas, "turnos": turnos})

def main():
    with st.sidebar:
        st.markdown("## Checklist Operacional")

        st.session_state.setdefault("nav", "Diagnóstico")
        nav = st.radio("Ir para", ["Diagnóstico", "Dashboard", "Checklist", "LOGS"], key="nav", label_visibility="collapsed")

        st.markdown("### Status")
        try:
            _ = service_client()
            st.success("Google client OK")
        except Exception:
            st.error("Falha ao criar client Google")

    if not CONFIG_SHEET_ID or not RULES_SHEET_ID or not LOGS_SHEET_ID:
        st.error("Faltam IDs das planilhas no Secrets. Preencha CONFIG_SHEET_ID, RULES_SHEET_ID e LOGS_SHEET_ID em [app].")
        st.stop()

    if nav == "Diagnóstico":
        page_diagnostico()
        st.stop()

    st.session_state.setdefault("cache_buster", "v1")

    try:
        user = authenticate_user(
            rules_sheet_id=RULES_SHEET_ID,
            users_tab_candidates=WS_USERS_CANDIDATES,
            service_client=service_client,
        )
    except Exception as e:
        st.error("Falha ao acessar RULES_SHEET_ID durante login.")
        st.code(str(e))
        st.stop()

    if not user:
        return

    today = _now_ts().date()
    dia_str = _weekday_pt(today)

    try:
        tables = load_tables(
            cache_buster=st.session_state["cache_buster"],
            config_sheet_id=CONFIG_SHEET_ID,
            rules_sheet_id=RULES_SHEET_ID,
            logs_sheet_id=LOGS_SHEET_ID,
        )
    except Exception as e:
        st.error("Falha ao carregar tabelas (abas ou permissões).")
        st.code(str(e))
        st.stop()

    page_dashboard(tables, dia_str)

if __name__ == "__main__":
    main()
