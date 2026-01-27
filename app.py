import os
import time
import re
from datetime import datetime, date
from zoneinfo import ZoneInfo
from typing import Optional

import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

TZ = ZoneInfo("America/Sao_Paulo")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

EVENT_COLS = [
    "ts_iso",
    "data",
    "hora",
    "dia_semana",
    "user_login",
    "user_nome",
    "area_id",
    "turno",
    "item_id",
    "texto",
    "status",
]

WS_AREAS_CANDIDATES = ["AREAS", "Areas", "Checklist_Areas", "CHECKLIST_AREAS"]
WS_ITENS_CANDIDATES = ["ITENS", "Itens", "Checklist_Itens", "CHECKLIST_ITENS"]
WS_USERS_CANDIDATES = ["USUARIOS", "Usuarios", "Users", "USERS", "Login", "LOGIN", "USUARIOS "]
WS_EVENTS_CANDIDATES = ["EVENTS", "Checklist_Events", "CHECKLIST_EVENTS"]

COL_AREA_ID = "area_id"
COL_AREA_NOME = "area_nome"
COL_ATIVO = "ativo"
COL_ORDEM = "ordem"

COL_ITEM_ID = "item_id"
COL_TEXTO = "texto"
COL_TURNO = "turno"
COL_AREA_REF = "area_id"


def _get_cfg(name: str, default: str = "") -> str:
    if hasattr(st, "secrets") and "app" in st.secrets and name in st.secrets["app"]:
        return str(st.secrets["app"][name]).strip()
    if hasattr(st, "secrets") and name in st.secrets:
        return str(st.secrets[name]).strip()
    return os.getenv(name, default).strip()


CONFIG_SHEET_ID = _get_cfg("CONFIG_SHEET_ID", "")
RULES_SHEET_ID = _get_cfg("RULES_SHEET_ID", "")
LOGS_SHEET_ID = _get_cfg("LOGS_SHEET_ID", "")


def normalize_sheet_id(value: str) -> str:
    v = (value or "").strip()
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", v)
    if m:
        return m.group(1)
    return v


def _require_ids():
    if not CONFIG_SHEET_ID or not RULES_SHEET_ID or not LOGS_SHEET_ID:
        raise RuntimeError(
            "IDs das planilhas nao configurados. Coloque em Secrets: "
            "CONFIG_SHEET_ID, RULES_SHEET_ID, LOGS_SHEET_ID."
        )


def _retryable(fn, tries=6, base_sleep=0.8, max_sleep=10.0):
    last = None
    for i in range(tries):
        try:
            return fn()
        except APIError as e:
            last = e
            msg = str(e)
            is_quota = ("429" in msg) or ("Quota exceeded" in msg) or ("RESOURCE_EXHAUSTED" in msg)
            if not is_quota and i >= 1:
                raise
            time.sleep(min(max_sleep, base_sleep * (2 ** i)))
    raise last


@st.cache_resource
def gs_client():
    if "gcp_service_account" not in st.secrets:
        raise RuntimeError("Secrets precisa ter [gcp_service_account].")

    info = dict(st.secrets["gcp_service_account"])
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)


@st.cache_data(ttl=900)
def list_sheet_titles_cached(spreadsheet_id: str):
    client = gs_client()
    sid = normalize_sheet_id(spreadsheet_id)

    def _do():
        sh = client.open_by_key(sid)
        return [ws.title for ws in sh.worksheets()]

    return _retryable(_do)


def get_or_create_worksheet(spreadsheet_id: str, title: str, rows: int = 5000, cols: int = 20):
    client = gs_client()
    sid = normalize_sheet_id(spreadsheet_id)

    def _do():
        sh = client.open_by_key(sid)
        try:
            return sh.worksheet(title)
        except Exception:
            return sh.add_worksheet(title=title, rows=str(rows), cols=str(cols))

    return _retryable(_do)


def read_df(spreadsheet_id: str, worksheet_title: str, last_n: Optional[int] = None) -> pd.DataFrame:
    client = gs_client()
    sid = normalize_sheet_id(spreadsheet_id)

    def _do():
        sh = client.open_by_key(sid)
        ws = sh.worksheet(worksheet_title)
        values = ws.get_all_values()
        if not values:
            return pd.DataFrame()
        header = values[0]
        body = values[1:]
        if last_n and last_n > 0 and last_n < len(body):
            body = body[-last_n:]
        return pd.DataFrame(body, columns=header)

    return _retryable(_do)


def append_row(spreadsheet_id: str, worksheet_title: str, row: list, header_if_empty: Optional[list] = None):
    client = gs_client()
    sid = normalize_sheet_id(spreadsheet_id)

    def _do():
        sh = client.open_by_key(sid)
        ws = sh.worksheet(worksheet_title)
        if header_if_empty:
            first = ws.row_values(1)
            if not first or all(str(x).strip() == "" for x in first):
                ws.append_row(header_if_empty, value_input_option="RAW")
        ws.append_row(row, value_input_option="RAW")

    return _retryable(_do)


def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def _coerce_bool(x) -> bool:
    if pd.isna(x):
        return False
    s = str(x).strip().lower()
    return s in ["1", "true", "sim", "yes", "y", "ativo"]


def _weekday_pt(d: date) -> str:
    names = ["Segunda", "Terca", "Quarta", "Quinta", "Sexta", "Sabado", "Domingo"]
    return names[d.weekday()]


def _pick_first_existing_tab(spreadsheet_id: str, candidates: list[str]) -> str:
    titles = set(list_sheet_titles_cached(spreadsheet_id))
    for c in candidates:
        if c in titles:
            return c
    lower_map = {t.lower(): t for t in titles}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    raise RuntimeError(f"Nenhuma aba encontrada. Candidatas: {candidates}. Existentes: {sorted(titles)}")


def _validate_config(areas_df: pd.DataFrame, itens_df: pd.DataFrame):
    for c in [COL_AREA_ID, COL_AREA_NOME]:
        if c not in areas_df.columns:
            raise RuntimeError(f"A aba AREAS precisa ter a coluna '{c}'. Colunas atuais: {list(areas_df.columns)}")

    needed = {COL_AREA_REF, COL_TURNO, COL_ITEM_ID, COL_TEXTO}
    missing = [c for c in needed if c not in itens_df.columns]
    if missing:
        raise RuntimeError(
            f"A aba ITENS precisa ter as colunas {sorted(list(needed))}. "
            f"Faltando: {missing}. Colunas atuais: {list(itens_df.columns)}"
        )


def _areas_list(areas_df: pd.DataFrame) -> list[dict]:
    df = areas_df.copy()
    if COL_ATIVO in df.columns:
        df = df[df[COL_ATIVO].apply(_coerce_bool) == True]
    if COL_ORDEM in df.columns:
        df[COL_ORDEM] = pd.to_numeric(df[COL_ORDEM], errors="coerce")
        df = df.sort_values([COL_ORDEM, COL_AREA_NOME], na_position="last")
    else:
        df = df.sort_values([COL_AREA_NOME])
    out = []
    for _, r in df.iterrows():
        out.append({"area_id": str(r[COL_AREA_ID]).strip(), "area_nome": str(r[COL_AREA_NOME]).strip()})
    return out


def _turnos_list(itens_df: pd.DataFrame) -> list[str]:
    vals = sorted({str(x).strip() for x in itens_df[COL_TURNO].dropna().tolist() if str(x).strip()})
    return vals if vals else ["Almoco", "Jantar"]


def _items_for(itens_df: pd.DataFrame, area_id: str, turno: str) -> pd.DataFrame:
    df = itens_df.copy()
    df[COL_AREA_REF] = df[COL_AREA_REF].astype(str).str.strip()
    df[COL_TURNO] = df[COL_TURNO].astype(str).str.strip()
    df = df[(df[COL_AREA_REF] == area_id) & (df[COL_TURNO] == turno)]
    if COL_ATIVO in df.columns:
        df = df[df[COL_ATIVO].apply(_coerce_bool) == True]
    if COL_ORDEM in df.columns:
        df[COL_ORDEM] = pd.to_numeric(df[COL_ORDEM], errors="coerce")
        df = df.sort_values([COL_ORDEM, COL_ITEM_ID], na_position="last")
    else:
        df = df.sort_values([COL_ITEM_ID])
    df[COL_ITEM_ID] = df[COL_ITEM_ID].astype(str).str.strip()
    df[COL_TEXTO] = df[COL_TEXTO].astype(str).str.strip()
    return df.reset_index(drop=True)


def _latest_status_today(events_df: pd.DataFrame, today_iso: str) -> dict:
    if events_df.empty:
        return {}
    needed = {"data", "area_id", "turno", "item_id", "status", "ts_iso"}
    if any(c not in events_df.columns for c in needed):
        return {}

    df = events_df.copy()
    df["data"] = df["data"].astype(str).str.strip()
    df = df[df["data"] == today_iso].copy()
    if df.empty:
        return {}

    df["ts_dt"] = pd.to_datetime(df["ts_iso"], errors="coerce")
    df = df.dropna(subset=["ts_dt"])
    df = df.sort_values("ts_dt")
    latest = df.groupby(["data", "area_id", "turno", "item_id"], as_index=False).tail(1)

    mp = {}
    for _, r in latest.iterrows():
        key = (str(r["area_id"]).strip(), str(r["turno"]).strip(), str(r["item_id"]).strip())
        mp[key] = str(r["status"]).strip().upper()
    return mp


def _card_style(status: str) -> tuple[str, str]:
    s = (status or "").strip().upper()
    if s == "OK":
        return "#d1fae5", "Concluido"
    if s in ["NAO_OK", "NÃO OK", "NAO OK"]:
        return "#fee2e2", "Nao OK"
    return "#f3f4f6", "Pendente"


def _invalidate_data_cache():
    st.cache_data.clear()


@st.cache_data(ttl=60)
def load_tables():
    _require_ids()

    ws_areas = _pick_first_existing_tab(CONFIG_SHEET_ID, WS_AREAS_CANDIDATES)
    ws_itens = _pick_first_existing_tab(CONFIG_SHEET_ID, WS_ITENS_CANDIDATES)
    ws_users = _pick_first_existing_tab(RULES_SHEET_ID, WS_USERS_CANDIDATES)

    try:
        ws_events = _pick_first_existing_tab(LOGS_SHEET_ID, WS_EVENTS_CANDIDATES)
    except Exception:
        ws = get_or_create_worksheet(LOGS_SHEET_ID, "EVENTS", rows=5000, cols=20)
        ws_events = ws.title
        # header
        existing = ws.row_values(1)
        if not existing or all(str(x).strip() == "" for x in existing):
            ws.append_row(EVENT_COLS, value_input_option="RAW")

    areas_df = _normalize_cols(read_df(CONFIG_SHEET_ID, ws_areas))
    itens_df = _normalize_cols(read_df(CONFIG_SHEET_ID, ws_itens))
    users_df = _normalize_cols(read_df(RULES_SHEET_ID, ws_users))
    events_df = _normalize_cols(read_df(LOGS_SHEET_ID, ws_events, last_n=1500))

    return {
        "ws_areas": ws_areas,
        "ws_itens": ws_itens,
        "ws_users": ws_users,
        "ws_events": ws_events,
        "areas_df": areas_df,
        "itens_df": itens_df,
        "users_df": users_df,
        "events_df": events_df,
    }


def authenticate_user(users_df: pd.DataFrame):
    st.session_state.setdefault("logged_in", False)
    st.session_state.setdefault("user_login", "")
    st.session_state.setdefault("user_nome", "")

    if st.session_state["logged_in"]:
        return {"login": st.session_state["user_login"], "nome": st.session_state["user_nome"]}

    st.title("Login")
    st.caption("Acesso protegido por usuario e senha.")

    u = st.text_input("Usuario", key="login_user")
    p = st.text_input("Senha", type="password", key="login_pass")

    if st.button("Entrar", type="primary"):
        if users_df.empty:
            st.error("A aba de usuarios esta vazia.")
            return None

        df = users_df.copy()
        df.columns = [str(c).strip().lower() for c in df.columns]

        col_login = None
        for c in ["login", "user", "usuario"]:
            if c in df.columns:
                col_login = c
                break

        col_pass = None
        for c in ["senha", "password"]:
            if c in df.columns:
                col_pass = c
                break

        col_nome = "nome" if "nome" in df.columns else None
        col_ativo = "ativo" if "ativo" in df.columns else None

        if not col_login or not col_pass:
            st.error("A aba de usuarios precisa ter colunas login/senha (ou user/password).")
            st.info(f"Colunas encontradas: {list(df.columns)}")
            return None

        if col_ativo:
            tmp = df[col_ativo].astype(str).str.strip().str.lower()
            df = df[tmp.isin(["true", "1", "sim", "yes", "ativo"])]

        u2 = str(u).strip()
        p2 = str(p).strip()

        df[col_login] = df[col_login].astype(str).str.strip()
        df[col_pass] = df[col_pass].astype(str).str.strip()

        hit = df[(df[col_login] == u2) & (df[col_pass] == p2)]
        if hit.empty:
            st.error("Usuario ou senha invalidos.")
            return None

        nome = u2
        if col_nome:
            nome = str(hit.iloc[0][col_nome]).strip() or u2

        st.session_state["logged_in"] = True
        st.session_state["user_login"] = u2
        st.session_state["user_nome"] = nome
        st.rerun()

    return None


def write_event(area_id: str, turno: str, item_id: str, texto: str, status: str, user_login: str, user_nome: str, ws_events: str):
    now = datetime.now(TZ)
    row = [
        now.isoformat(),
        now.date().isoformat(),
        now.strftime("%H:%M:%S"),
        _weekday_pt(now.date()),
        user_login,
        user_nome,
        area_id,
        turno,
        item_id,
        texto,
        status,
    ]
    append_row(LOGS_SHEET_ID, ws_events, row, header_if_empty=EVENT_COLS)
    _invalidate_data_cache()


def page_dashboard(tables):
    st.subheader("Dashboard operacional")

    areas_df = tables["areas_df"]
    itens_df = tables["itens_df"]
    events_df = tables["events_df"]

    _validate_config(areas_df, itens_df)

    today = datetime.now(TZ).date().isoformat()
    status_map = _latest_status_today(events_df, today)

    areas = _areas_list(areas_df)
    turnos = _turnos_list(itens_df)

    c1, c2 = st.columns([1, 1])
    with c1:
        if st.button("Atualizar dados agora"):
            _invalidate_data_cache()
            st.rerun()
    with c2:
        st.caption("Sem auto refresh para evitar erro 429. Use o botao quando precisar.")

    for a in areas:
        st.markdown(f"### {a['area_nome']}")
        cols = st.columns(2 if len(turnos) >= 2 else 1)

        for idx, turno in enumerate(turnos):
            items = _items_for(itens_df, a["area_id"], turno)
            total = len(items)

            done = 0
            nok = 0
            for _, it in items.iterrows():
                stt = status_map.get((a["area_id"], turno, str(it[COL_ITEM_ID]).strip()), "PENDENTE")
                if stt == "OK":
                    done += 1
                elif stt in ["NAO_OK", "NÃO OK", "NAO OK"]:
                    nok += 1

            pct = (done / total) if total else 0.0
            subtitle = f"{done}/{total} OK | {nok} Nao OK"

            color = "#2b2b2b"
            if done == total and total > 0:
                color = "#0b6a5a"
            elif done > 0:
                color = "#8b6b12"
            if nok > 0:
                color = "#7a1f2b"

            with cols[idx % len(cols)]:
                st.markdown(
                    f"""
                    <div style="border-radius:16px;padding:14px;margin:8px 0;background:{color};color:white;">
                      <div style="font-size:16px;font-weight:800;">{turno}</div>
                      <div style="font-size:13px;opacity:0.9;margin-top:4px;">{subtitle}</div>
                      <div style="font-size:22px;font-weight:900;margin-top:10px;">{int(round(pct*100,0))}%</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )


def page_checklist(tables, user_login: str, user_nome: str):
    st.subheader("Checklist")

    areas_df = tables["areas_df"]
    itens_df = tables["itens_df"]
    events_df = tables["events_df"]
    ws_events = tables["ws_events"]

    _validate_config(areas_df, itens_df)

    areas = _areas_list(areas_df)
    turnos = _turnos_list(itens_df)

    area_labels = [f"{a['area_nome']} ({a['area_id']})" for a in areas]

    c1, c2 = st.columns([1, 1])
    with c1:
        area_sel = st.selectbox("Area", area_labels, index=0)
    with c2:
        turno_sel = st.selectbox("Turno", turnos, index=0)

    area_id = area_sel.split("(")[-1].replace(")", "").strip()
    items = _items_for(itens_df, area_id, turno_sel)
    if items.empty:
        st.warning("Sem itens para esta Area e Turno.")
        return

    today = datetime.now(TZ).date().isoformat()
    status_map = _latest_status_today(events_df, today)

    st.markdown(f"### {area_sel} | {turno_sel}")
    st.caption("Tudo comeca PENDENTE. Clique OK ou Nao OK. Para desfazer, clique Desmarcar.")

    for _, it in items.iterrows():
        item_id = str(it[COL_ITEM_ID]).strip()
        texto = str(it[COL_TEXTO]).strip()

        current = status_map.get((area_id, turno_sel, item_id), "PENDENTE").upper()
        bg, label = _card_style(current)

        st.markdown(
            f"""
            <div style="border-radius:14px;padding:12px;background:{bg};margin:10px 0;">
              <div style="font-size:15px;font-weight:900;">{texto}</div>
              <div style="font-size:12px;margin-top:6px;"><b>Status:</b> {label}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        b1, b2, b3 = st.columns([1, 1, 1])

        with b1:
            if st.button("OK", key=f"ok_{area_id}_{turno_sel}_{item_id}", type="primary"):
                write_event(area_id, turno_sel, item_id, texto, "OK", user_login, user_nome, ws_events)
                st.rerun()

        with b2:
            if st.button("Nao OK", key=f"nok_{area_id}_{turno_sel}_{item_id}"):
                write_event(area_id, turno_sel, item_id, texto, "NAO_OK", user_login, user_nome, ws_events)
                st.rerun()

        with b3:
            if st.button("Desmarcar", key=f"rst_{area_id}_{turno_sel}_{item_id}"):
                write_event(area_id, turno_sel, item_id, texto, "PENDENTE", user_login, user_nome, ws_events)
                st.rerun()


def page_events(tables):
    st.subheader("EVENTS")
    if st.button("Atualizar EVENTS"):
        _invalidate_data_cache()
        st.rerun()

    df = tables["events_df"].copy()
    if df.empty:
        st.info("Sem eventos ainda.")
        return

    if "ts_iso" in df.columns:
        df["ts_dt"] = pd.to_datetime(df["ts_iso"], errors="coerce")
        df = df.sort_values("ts_dt", ascending=False).drop(columns=["ts_dt"], errors="ignore")

    st.dataframe(df, use_container_width=True, height=520)


def main():
    st.set_page_config(page_title="Checklist Operacional", layout="wide", initial_sidebar_state="expanded")

    css = """
    <style>
    .block-container { padding-top: 1.2rem; padding-left: 0.9rem; padding-right: 0.9rem; }
    header[data-testid="stHeader"] { height: 0px !important; }
    div[data-testid="stToolbar"] { visibility: hidden; height: 0px; }
    footer { visibility: hidden; height: 0px; }
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)

    _require_ids()

    with st.sidebar:
        st.markdown("## Checklist Operacional")

        if st.button("Logout"):
            for k in list(st.session_state.keys()):
                st.session_state.pop(k, None)
            st.rerun()

        st.markdown("### Status")
        try:
            _ = gs_client()
            st.success("Google Sheets conectado")
        except Exception as e:
            st.error(f"Falha ao conectar: {e}")

        st.markdown("### Navegacao")
        st.session_state.setdefault("nav", "Dashboard")
        st.radio("Ir para", ["Dashboard", "Checklist", "EVENTS"], key="nav", label_visibility="collapsed")

    tables = load_tables()
    user = authenticate_user(tables["users_df"])
    if not user:
        return

    nav = st.session_state.get("nav", "Dashboard")
    if nav == "Checklist":
        page_checklist(tables, user_login=user["login"], user_nome=user["nome"])
    elif nav == "EVENTS":
        page_events(tables)
    else:
        page_dashboard(tables)


if __name__ == "__main__":
    main()
