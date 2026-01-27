import os
import time
import re
from datetime import datetime, date
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st

from sheets_client import (
    get_service_cached,
    read_df,
    append_row,
    list_sheet_titles,
    ensure_tab_exists,
)
from auth import authenticate_user

TZ = ZoneInfo("America/Sao_Paulo")


# =========================
# CONFIG
# =========================

def _get_cfg(name: str, default: str = "") -> str:
    if hasattr(st, "secrets") and "app" in st.secrets and name in st.secrets["app"]:
        return str(st.secrets["app"][name]).strip()
    return os.getenv(name, default).strip()


CONFIG_SHEET_ID = _get_cfg("CONFIG_SHEET_ID", "")
RULES_SHEET_ID = _get_cfg("RULES_SHEET_ID", "")
LOGS_SHEET_ID = _get_cfg("LOGS_SHEET_ID", "")

# Abas candidatas
WS_AREAS_CANDIDATES = ["AREAS", "Areas", "Checklist_Areas", "CHECKLIST_AREAS"]
WS_ITENS_CANDIDATES = ["ITENS", "Itens", "Checklist_Itens", "CHECKLIST_ITENS"]
WS_USERS_CANDIDATES = ["USUARIOS", "Usuarios", "USERS", "Users", "LOGIN", "Login"]

# Aba de eventos nova dentro da planilha de logs
WS_EVENTS = "EVENTS"

# Colunas do EVENT LOG (por item)
EVENT_COLS = [
    "ts",
    "data",
    "dia_semana",
    "hora",
    "user_login",
    "user_nome",
    "area_id",
    "turno",
    "item_id",
    "texto",
    "status",        # "OK", "NAO_OK", "PENDENTE"
]

STATUS_OK = "OK"
STATUS_NOK = "NAO_OK"
STATUS_PEND = "PENDENTE"


# =========================
# UI
# =========================

st.set_page_config(page_title="Checklist Operacional", layout="wide", initial_sidebar_state="expanded")

CSS = """
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
st.markdown(CSS, unsafe_allow_html=True)


# =========================
# HELPERS
# =========================

@st.cache_resource
def service_client():
    return get_service_cached()


def _now_ts() -> datetime:
    return datetime.now(TZ)


def _weekday_pt(d: date) -> str:
    names = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
    return names[d.weekday()]


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
    raise RuntimeError(f"Não encontrei nenhuma aba válida em {spreadsheet_id}. Candidatas: {candidates}. Existentes: {sorted(titles)}")


def _coerce_bool(x) -> bool:
    if pd.isna(x):
        return False
    s = str(x).strip().lower()
    return s in ["1", "true", "sim", "yes", "y", "ativo"]


def _invalidate():
    st.cache_data.clear()


def _map_itens_columns(itens_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adapta sua aba ITENS, aceitando colunas como:
    - area_id (ou area)
    - item_id
    - turno
    - texto (ou pergunta/descricao)
    - ativo (opcional)
    - ordem (opcional)
    """
    df = itens_df.copy()
    cols = set(df.columns)

    # area
    if "area_id" not in cols:
        if "area" in cols:
            df["area_id"] = df["area"]
        else:
            raise RuntimeError(f"A aba ITENS precisa ter coluna area_id (ou area). Colunas atuais: {list(df.columns)}")

    # texto
    if "texto" not in cols:
        fallback = None
        for c in ["pergunta", "descricao", "item", "nome", "texto_item"]:
            if c in cols:
                fallback = c
                break
        if fallback:
            df["texto"] = df[fallback]
        else:
            raise RuntimeError(f"A aba ITENS precisa ter coluna texto (ou pergunta/descricao). Colunas atuais: {list(df.columns)}")

    # item_id
    if "item_id" not in cols:
        raise RuntimeError(f"A aba ITENS precisa ter coluna item_id. Colunas atuais: {list(df.columns)}")

    # turno
    if "turno" not in cols:
        raise RuntimeError(f"A aba ITENS precisa ter coluna turno. Colunas atuais: {list(df.columns)}")

    # padroniza tipos
    df["area_id"] = df["area_id"].astype(str).str.strip()
    df["turno"] = df["turno"].astype(str).str.strip()
    df["item_id"] = df["item_id"].astype(str).str.strip()
    df["texto"] = df["texto"].astype(str).str.strip()

    # ativo
    if "ativo" in df.columns:
        df = df[df["ativo"].apply(_coerce_bool)]

    # ordem
    if "ordem" in df.columns:
        df["ordem"] = pd.to_numeric(df["ordem"], errors="coerce")
        df = df.sort_values(["area_id", "turno", "ordem", "item_id"], na_position="last")
    else:
        df = df.sort_values(["area_id", "turno", "item_id"])

    return df.reset_index(drop=True)


def _load_tables(config_sheet_id: str, rules_sheet_id: str, logs_sheet_id: str):
    svc = service_client()

    if not config_sheet_id or not rules_sheet_id or not logs_sheet_id:
        raise RuntimeError(
            "IDs das planilhas não configurados. Defina CONFIG_SHEET_ID, RULES_SHEET_ID, LOGS_SHEET_ID "
            "no Streamlit Secrets [app] ou em variáveis de ambiente."
        )

    ws_areas = _pick_first_existing_tab(svc, config_sheet_id, WS_AREAS_CANDIDATES)
    ws_itens = _pick_first_existing_tab(svc, config_sheet_id, WS_ITENS_CANDIDATES)

    # garante aba EVENTS
    ensure_tab_exists(svc, logs_sheet_id, WS_EVENTS)

    areas_df = _normalize_cols(read_df(svc, config_sheet_id, ws_areas))
    itens_df = _normalize_cols(read_df(svc, config_sheet_id, ws_itens))
    events_df = _normalize_cols(read_df(svc, logs_sheet_id, WS_EVENTS))

    itens_df = _map_itens_columns(itens_df)

    return {
        "ws_areas": ws_areas,
        "ws_itens": ws_itens,
        "ws_events": WS_EVENTS,
        "areas_df": areas_df,
        "itens_df": itens_df,
        "events_df": events_df,
    }


def _available_areas(areas_df: pd.DataFrame, itens_df: pd.DataFrame) -> list[str]:
    # sua AREAS tem: area_id, area_nome, ativo, ordem
    if "area_id" in areas_df.columns and areas_df["area_id"].notna().any():
        tmp = areas_df.copy()
        if "ativo" in tmp.columns:
            tmp = tmp[tmp["ativo"].apply(_coerce_bool)]
        if "ordem" in tmp.columns:
            tmp["ordem"] = pd.to_numeric(tmp["ordem"], errors="coerce")
            tmp = tmp.sort_values(["ordem", "area_id"], na_position="last")
        vals = [str(x).strip() for x in tmp["area_id"].dropna().tolist() if str(x).strip()]
        if vals:
            return vals
    return sorted({str(x).strip() for x in itens_df["area_id"].dropna().tolist() if str(x).strip()})


def _available_turnos(itens_df: pd.DataFrame) -> list[str]:
    vals = sorted({str(x).strip() for x in itens_df["turno"].dropna().tolist() if str(x).strip()})
    return vals


def _latest_status_map(events_df: pd.DataFrame) -> dict:
    """
    Retorna o último status por (data, area_id, turno, item_id).
    """
    if events_df.empty:
        return {}

    needed = {"data", "area_id", "turno", "item_id", "status", "ts"}
    if not needed.issubset(set(events_df.columns)):
        return {}

    df = events_df.copy()
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    df = df.dropna(subset=["ts"])

    df["data"] = df["data"].astype(str).str.strip()
    df["area_id"] = df["area_id"].astype(str).str.strip()
    df["turno"] = df["turno"].astype(str).str.strip()
    df["item_id"] = df["item_id"].astype(str).str.strip()
    df["status"] = df["status"].astype(str).str.strip().str.upper()

    df = df.sort_values("ts")
    last = df.groupby(["data", "area_id", "turno", "item_id"], as_index=False).tail(1)

    mp = {}
    for _, r in last.iterrows():
        mp[(r["data"], r["area_id"], r["turno"], r["item_id"])] = r["status"]
    return mp


def _status_to_ui(status: str):
    s = (status or "").strip().upper()
    if s == STATUS_OK:
        return ("Concluído", "#d1fae5")    # verde claro
    if s == STATUS_NOK:
        return ("Não OK", "#fee2e2")       # vermelho claro
    return ("Pendente", "#f3f4f6")         # cinza claro


def _write_event(area_id: str, turno: str, item_id: str, texto: str, status: str, user: dict):
    svc = service_client()
    now = _now_ts()
    row = [
        now.isoformat(),
        now.date().isoformat(),
        _weekday_pt(now.date()),
        now.strftime("%H:%M:%S"),
        user["login"],
        user.get("nome", user["login"]),
        area_id,
        turno,
        item_id,
        texto,
        status,
    ]
    append_row(
        svc,
        LOGS_SHEET_ID,
        WS_EVENTS,
        row,
        header_if_empty=EVENT_COLS,
    )
    _invalidate()
    # força rerun e leitura nova imediatamente
    st.rerun()


# =========================
# PAGES
# =========================

def page_dashboard(tables):
    st.subheader("Dashboard operacional")

    if st.button("Atualizar agora"):
        _invalidate()
        st.rerun()

    itens_df = tables["itens_df"]
    areas_df = tables["areas_df"]
    events_df = tables["events_df"]

    areas = _available_areas(areas_df, itens_df)
    turnos = _available_turnos(itens_df)
    status_map = _latest_status_map(events_df)

    today = _now_ts().date().isoformat()

    for area_id in areas:
        st.markdown(f"### {area_id}")
        cols = st.columns(2) if len(turnos) >= 2 else st.columns(1)

        for idx, turno in enumerate(turnos):
            df = itens_df[(itens_df["area_id"] == area_id) & (itens_df["turno"] == turno)]
            total = len(df)
            done = 0
            nok = 0

            for _, it in df.iterrows():
                key = (today, area_id, turno, it["item_id"])
                stt = status_map.get(key, STATUS_PEND)
                if stt == STATUS_OK:
                    done += 1
                elif stt == STATUS_NOK:
                    nok += 1

            pct = (done / total) if total else 0.0

            # cor do card KPI
            if total == 0:
                color = "#2b2b2b"
            elif done == total:
                color = "#0b6a5a"
            elif done > 0 or nok > 0:
                color = "#8b6b12"
            else:
                color = "#2b2b2b"

            subtitle = f"{done}/{total} OK"
            if nok > 0:
                subtitle += f" | {nok} Não OK"

            pct_txt = f"{int(round(pct*100, 0))}%"

            with cols[idx % len(cols)]:
                st.markdown(
                    f"""
                    <div style="border-radius:16px;padding:14px 14px 12px 14px;margin:8px 0;background:{color};color:white;">
                      <div style="font-size:16px;font-weight:700;line-height:1.1;">{turno}</div>
                      <div style="font-size:13px;opacity:0.9;margin-top:4px;">{subtitle}</div>
                      <div style="font-size:22px;font-weight:800;margin-top:10px;">{pct_txt}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    st.caption("O dashboard é calculado pelo último status registrado por item na aba EVENTS.")


def page_checklist(tables, user: dict):
    st.subheader("Checklist")

    itens_df = tables["itens_df"]
    areas_df = tables["areas_df"]
    events_df = tables["events_df"]

    areas = _available_areas(areas_df, itens_df)
    turnos = _available_turnos(itens_df)

    with st.container():
        st.write("Selecione Área e Turno e toque em OK para abrir os itens.")
        c1, c2 = st.columns([1, 1])
        with c1:
            area_id = st.selectbox("Área", areas, key="sel_area")
        with c2:
            turno = st.selectbox("Turno", turnos, key="sel_turno")

        if st.button("OK", type="primary"):
            st.session_state["ctx_area_id"] = area_id
            st.session_state["ctx_turno"] = turno
            st.session_state["open_items"] = True
            st.rerun()

    if not st.session_state.get("open_items", False):
        st.info("Escolha Área e Turno e aperte OK.")
        return

    area_id = st.session_state.get("ctx_area_id", area_id)
    turno = st.session_state.get("ctx_turno", turno)

    df = itens_df[(itens_df["area_id"] == area_id) & (itens_df["turno"] == turno)].copy()
    if df.empty:
        st.warning("Sem itens para esta combinação de Área e Turno.")
        return

    today = _now_ts().date().isoformat()
    status_map = _latest_status_map(events_df)

    st.markdown(f"### {area_id} | {turno}")
    st.caption("Tudo começa pendente. Clique em OK ou NÃO OK para mudar a cor. Use Desmarcar para voltar a pendente.")

    for _, it in df.iterrows():
        item_id = str(it["item_id"]).strip()
        texto = str(it["texto"]).strip()

        key = (today, area_id, turno, item_id)
        current = status_map.get(key, STATUS_PEND)
        label, box_color = _status_to_ui(current)

        st.markdown(
            f"""
            <div style="border-radius:14px;padding:12px 12px;background:{box_color};margin:10px 0;">
              <div style="font-size:14px;font-weight:800;">{texto}</div>
              <div style="font-size:12px;margin-top:6px;"><b>Status:</b> {label}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        b1, b2, b3 = st.columns([1, 1, 1])
        with b1:
            if st.button("Marcar OK", key=f"ok_{area_id}_{turno}_{item_id}", type="primary"):
                _write_event(area_id, turno, item_id, texto, STATUS_OK, user)
        with b2:
            if st.button("Marcar NÃO OK", key=f"nok_{area_id}_{turno}_{item_id}"):
                _write_event(area_id, turno, item_id, texto, STATUS_NOK, user)
        with b3:
            if st.button("Desmarcar", key=f"clr_{area_id}_{turno}_{item_id}"):
                _write_event(area_id, turno, item_id, texto, STATUS_PEND, user)


def page_events(tables):
    st.subheader("EVENTS (log por item)")
    st.caption("Aqui ficam os registros por item. Este log alimenta o checklist e o dashboard.")

    if st.button("Atualizar"):
        _invalidate()
        st.rerun()

    df = tables["events_df"].copy()
    if df.empty:
        st.info("Sem registros ainda.")
        return

    if "ts" in df.columns:
        df["ts_dt"] = pd.to_datetime(df["ts"], errors="coerce")
        df = df.sort_values("ts_dt", ascending=False).drop(columns=["ts_dt"], errors="ignore")

    st.dataframe(df, use_container_width=True, height=520)


# =========================
# MAIN
# =========================

def main():
    with st.sidebar:
        st.markdown("## Checklist Operacional")

        if st.button("Logout"):
            for k in list(st.session_state.keys()):
                if k not in []:
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
        st.radio(
            "Ir para",
            ["Dashboard", "Checklist", "EVENTS"],
            key="nav",
            label_visibility="collapsed",
        )

    user = authenticate_user(
        rules_sheet_id=RULES_SHEET_ID,
        users_tab_candidates=WS_USERS_CANDIDATES,
        service_client=service_client,
    )
    if not user:
        return

    tables = _load_tables(
        config_sheet_id=CONFIG_SHEET_ID,
        rules_sheet_id=RULES_SHEET_ID,
        logs_sheet_id=LOGS_SHEET_ID,
    )

    nav = st.session_state.get("nav", "Dashboard")
    if nav == "Checklist":
        page_checklist(tables, user)
    elif nav == "EVENTS":
        page_events(tables)
    else:
        page_dashboard(tables)


if __name__ == "__main__":
    main()
