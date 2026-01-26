import os
import time
import re
from datetime import datetime, date
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st

from sheets_client import get_service_cached, read_df, append_row, pick_existing_tab
from auth import authenticate_user


TZ = ZoneInfo("America/Sao_Paulo")


def _get_cfg(name: str, default: str = "") -> str:
    if hasattr(st, "secrets") and "app" in st.secrets and name in st.secrets["app"]:
        return str(st.secrets["app"][name]).strip()
    return os.getenv(name, default).strip()


CONFIG_SHEET_ID = _get_cfg("CONFIG_SHEET_ID", "")
RULES_SHEET_ID = _get_cfg("RULES_SHEET_ID", "")
LOGS_SHEET_ID = _get_cfg("LOGS_SHEET_ID", "")

WS_AREAS_CANDIDATES = ["AREAS", "Areas", "Checklist_Areas", "CHECKLIST_AREAS"]
WS_ITENS_CANDIDATES = ["ITENS", "Itens", "Checklist_Itens", "CHECKLIST_ITENS"]
WS_USERS_CANDIDATES = ["USUARIOS", "Usuarios", "USERS", "Users", "LOGIN", "Login"]
WS_LOGS_CANDIDATES = ["LOGS", "Logs", "Checklist_Logs", "CHECKLIST_LOGS"]

COL_AREA = "area"
COL_TURNO = "turno"
COL_ITEM_ID = "item_id"
COL_TEXTO = "texto"
COL_ATIVO = "ativo"
COL_ORDEM = "ordem"
COL_DEADLINE = "deadline"
COL_TOL_MIN = "tolerancia_min"

LOG_COLS = [
    "ts",
    "data",
    "dia_semana",
    "area",
    "turno",
    "item_id",
    "texto",
    "status",
    "user",
    "deadline",
    "tolerancia_min",
]

TURNOS_DEFAULT = ["Almoço", "Jantar"]


st.set_page_config(page_title="Checklist Operacional", layout="wide", initial_sidebar_state="expanded")

MOBILE_CSS = """
<style>
.block-container { padding-top: 1.2rem; padding-left: 0.9rem; padding-right: 0.9rem; }
header[data-testid="stHeader"] { height: 0px !important; }
div[data-testid="stToolbar"] { visibility: hidden; height: 0px; }
footer { visibility: hidden; height: 0px; }
div[data-testid="stSidebar"] { width: 280px; }
@media (max-width: 700px){
  .block-container { padding-top: 1.6rem; }
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


@st.cache_resource
def service_client():
    return get_service_cached()


@st.cache_data(ttl=10)
def load_tables(cache_buster: str):
    svc = service_client()

    if not CONFIG_SHEET_ID or not RULES_SHEET_ID or not LOGS_SHEET_ID:
        raise RuntimeError(
            "IDs das planilhas não configurados. Defina CONFIG_SHEET_ID, RULES_SHEET_ID, LOGS_SHEET_ID "
            "no Streamlit Secrets em [app]."
        )

    ws_areas = pick_existing_tab(svc, CONFIG_SHEET_ID, WS_AREAS_CANDIDATES)
    ws_itens = pick_existing_tab(svc, CONFIG_SHEET_ID, WS_ITENS_CANDIDATES)
    ws_logs = pick_existing_tab(svc, LOGS_SHEET_ID, WS_LOGS_CANDIDATES)

    areas_df = _normalize_cols(read_df(svc, CONFIG_SHEET_ID, ws_areas))
    itens_df = _normalize_cols(read_df(svc, CONFIG_SHEET_ID, ws_itens))
    logs_df = _normalize_cols(read_df(svc, LOGS_SHEET_ID, ws_logs))

    return {
        "ws_areas": ws_areas,
        "ws_itens": ws_itens,
        "ws_logs": ws_logs,
        "areas_df": areas_df,
        "itens_df": itens_df,
        "logs_df": logs_df,
    }


def _validate_itens_columns(itens_df: pd.DataFrame):
    needed = {COL_AREA, COL_TURNO, COL_ITEM_ID, COL_TEXTO}
    missing = [c for c in needed if c not in itens_df.columns]
    if missing:
        raise RuntimeError(
            f"A aba ITENS precisa ter as colunas: {sorted(list(needed))}. "
            f"Faltando: {missing}. Colunas atuais: {list(itens_df.columns)}"
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


def _filter_items_for(itens_df: pd.DataFrame, area: str, turno: str) -> pd.DataFrame:
    df = itens_df.copy()
    df[COL_AREA] = df[COL_AREA].astype(str).str.strip()
    df[COL_TURNO] = df[COL_TURNO].astype(str).str.strip()
    df = df[(df[COL_AREA] == area) & (df[COL_TURNO] == turno)]

    if COL_ATIVO in df.columns:
        df = df[df[COL_ATIVO].apply(_coerce_bool) == True]

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


def _latest_status_map(logs_df: pd.DataFrame):
    if logs_df.empty:
        return {}
    needed = ["data", "area", "turno", "item_id", "status", "ts"]
    for c in needed:
        if c not in logs_df.columns:
            return {}

    df = logs_df.copy()
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    df = df.dropna(subset=["ts"])
    df["data"] = df["data"].astype(str).str.strip()
    df["area"] = df["area"].astype(str).str.strip()
    df["turno"] = df["turno"].astype(str).str.strip()
    df["item_id"] = df["item_id"].astype(str).str.strip()

    df = df.sort_values("ts")
    latest = df.groupby(["data", "area", "turno", "item_id"], as_index=False).tail(1)

    mp = {}
    for _, r in latest.iterrows():
        mp[(r["data"], r["area"], r["turno"], r["item_id"])] = str(r["status"]).strip()
    return mp


def _invalidate_cache():
    st.cache_data.clear()


def _write_log(area: str, turno: str, item_id: str, texto: str, status: str, user: str, deadline: str, tol: int, ws_logs: str):
    svc = service_client()
    now = _now_ts()
    row = [
        now.isoformat(),
        now.date().isoformat(),
        _weekday_pt(now.date()),
        area,
        turno,
        item_id,
        texto,
        status,
        user,
        deadline,
        int(tol or 0),
    ]
    append_row(svc, LOGS_SHEET_ID, ws_logs, row, header_if_empty=LOG_COLS)
    _invalidate_cache()


def page_dashboard(tables):
    st.subheader("Dashboard operacional")

    areas_df = tables["areas_df"]
    itens_df = tables["itens_df"]
    logs_df = tables["logs_df"]

    _validate_itens_columns(itens_df)

    areas = _available_areas(areas_df, itens_df)
    turnos = _available_turnos(itens_df)
    status_map = _latest_status_map(logs_df)

    now = _now_ts()
    today = now.date().isoformat()

    for area in areas:
        st.markdown(f"### {area}")
        cols = st.columns(2) if len(turnos) >= 2 else st.columns(1)

        for idx, turno in enumerate(turnos):
            items = _filter_items_for(itens_df, area, turno)
            total = len(items)
            done = 0
            for _, it in items.iterrows():
                item_id = str(it[COL_ITEM_ID]).strip()
                stt = status_map.get((today, area, turno, item_id), "")
                if stt.upper() == "OK":
                    done += 1

            pct = (done / total) if total else 0.0
            with cols[idx % len(cols)]:
                st.metric(f"{turno}", f"{done}/{total}", f"{int(pct*100)}%")


def page_checklist(tables, user: str):
    st.subheader("Checklist")

    itens_df = tables["itens_df"]
    logs_df = tables["logs_df"]
    ws_logs = tables["ws_logs"]

    _validate_itens_columns(itens_df)

    areas = _available_areas(tables["areas_df"], itens_df)
    turnos = _available_turnos(itens_df)

    c1, c2 = st.columns([1, 1])
    with c1:
        area = st.selectbox("Área", areas)
    with c2:
        turno = st.selectbox("Turno", turnos)

    items = _filter_items_for(itens_df, area, turno)
    if items.empty:
        st.warning("Sem itens para esta combinação.")
        return

    now = _now_ts()
    today = now.date().isoformat()
    status_map = _latest_status_map(logs_df)

    for _, it in items.iterrows():
        item_id = str(it[COL_ITEM_ID]).strip()
        texto = str(it[COL_TEXTO]).strip()
        deadline = str(it.get(COL_DEADLINE, "") or "").strip()
        tol = int(it.get(COL_TOL_MIN, 0) or 0)

        current = status_map.get((today, area, turno, item_id), "").upper().strip()
        st.write(f"**{texto}**  | status: `{current or 'PENDENTE'}`")

        b1, b2 = st.columns([1, 1])
        with b1:
            if st.button("OK", key=f"ok_{area}_{turno}_{item_id}"):
                _write_log(area, turno, item_id, texto, "OK", user, deadline, tol, ws_logs)
                st.rerun()
        with b2:
            if st.button("NÃO OK", key=f"nok_{area}_{turno}_{item_id}"):
                _write_log(area, turno, item_id, texto, "NÃO OK", user, deadline, tol, ws_logs)
                st.rerun()


def page_logs(tables):
    st.subheader("LOGS")
    df = tables["logs_df"].copy()
    if df.empty:
        st.info("Sem registros ainda.")
        return
    st.dataframe(df, use_container_width=True, height=520)


def main():
    with st.sidebar:
        st.markdown("## Checklist Operacional")

        if st.button("Logout"):
            for k in list(st.session_state.keys()):
                if k != "cache_buster":
                    st.session_state.pop(k, None)
            st.rerun()

        st.markdown("### Status")
        try:
            _ = service_client()
            st.success("Google Sheets conectado")
        except Exception:
            st.error("Falha ao conectar no Google Sheets")

        st.markdown("### Navegação")
        st.session_state.setdefault("nav", "Dashboard")
        st.radio("Ir para", ["Dashboard", "Checklist", "LOGS"], key="nav", label_visibility="collapsed")

    # LOGIN
    user = authenticate_user(
        rules_sheet_id=RULES_SHEET_ID,
        users_tab_candidates=WS_USERS_CANDIDATES,
        service_client=service_client,
    )
    if not user:
        return

    st.session_state.setdefault("cache_buster", "v1")

    # CARREGA TABELAS
    try:
        tables = load_tables(st.session_state["cache_buster"])
    except Exception as e:
        st.error("Falha ao carregar dados das planilhas. Verifique IDs e permissões.")
        st.code(str(e))
        return

    if st.session_state.get("nav") == "Checklist":
        page_checklist(tables, user)
    elif st.session_state.get("nav") == "LOGS":
        page_logs(tables)
    else:
        page_dashboard(tables)


if __name__ == "__main__":
    main()
