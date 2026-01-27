import os
import time
import re
from datetime import datetime, date
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st

from sheets_client import (
    get_gspread_client,
    read_df,
    append_row,
    get_or_create_worksheet,
    list_sheet_titles_cached,
)
from auth import authenticate_user

TZ = ZoneInfo("America/Sao_Paulo")


# =========================
# CONFIG
# =========================

def _get_cfg(name: str, default: str = "") -> str:
    # 1) [app] no secrets
    if hasattr(st, "secrets") and "app" in st.secrets and name in st.secrets["app"]:
        return str(st.secrets["app"][name]).strip()

    # 2) raiz do secrets
    if hasattr(st, "secrets") and name in st.secrets:
        return str(st.secrets[name]).strip()

    # 3) env var
    return os.getenv(name, default).strip()


CONFIG_SHEET_ID = _get_cfg("CONFIG_SHEET_ID", "")
RULES_SHEET_ID = _get_cfg("RULES_SHEET_ID", "")
LOGS_SHEET_ID = _get_cfg("LOGS_SHEET_ID", "")

# Abas candidates
WS_AREAS_CANDIDATES = ["AREAS", "Areas", "Checklist_Areas", "CHECKLIST_AREAS"]
WS_ITENS_CANDIDATES = ["ITENS", "Itens", "Checklist_Itens", "CHECKLIST_ITENS"]
WS_USERS_CANDIDATES = ["USUARIOS", "Usuarios", "Users", "USERS", "Login", "LOGIN"]

# Vamos gravar e ler o estado do checklist desta aba (nova, controlada pelo app)
WS_EVENTS_CANDIDATES = ["EVENTS", "Checklist_Events", "CHECKLIST_EVENTS"]

# Colunas esperadas nas planilhas CONFIG
COL_AREA_ID = "area_id"
COL_AREA_NOME = "area_nome"
COL_ATIVO = "ativo"
COL_ORDEM = "ordem"

COL_ITEM_ID = "item_id"
COL_TEXTO = "texto"
COL_TURNO = "turno"
COL_AREA_REF = "area_id"  # em ITENS, referencia para AREAS

# Colunas do EVENTS (aba dedicada)
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
    "status",        # OK | NAO_OK | PENDENTE
]


TURNOS_DEFAULT = ["Almoco", "Jantar"]


# =========================
# UI
# =========================

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


# =========================
# HELPERS
# =========================

def _now() -> datetime:
    return datetime.now(TZ)


def _weekday_pt(d: date) -> str:
    names = ["Segunda", "Terca", "Quarta", "Quinta", "Sexta", "Sabado", "Domingo"]
    return names[d.weekday()]


def _coerce_bool(x) -> bool:
    if pd.isna(x):
        return False
    s = str(x).strip().lower()
    return s in ["1", "true", "sim", "yes", "y", "ativo"]


def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def _require_ids():
    if not CONFIG_SHEET_ID or not RULES_SHEET_ID or not LOGS_SHEET_ID:
        raise RuntimeError(
            "IDs das planilhas nao configurados. Confirme no Secrets: "
            "[app] CONFIG_SHEET_ID, RULES_SHEET_ID, LOGS_SHEET_ID."
        )


def _pick_first_existing_tab(client, spreadsheet_id: str, candidates: list[str]) -> str:
    # MUITO importante: cache pesado para não estourar quota
    titles = set(list_sheet_titles_cached(client, spreadsheet_id))
    for c in candidates:
        if c in titles:
            return c
    lower_map = {t.lower(): t for t in titles}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    raise RuntimeError(f"Nenhuma aba encontrada em {spreadsheet_id}. Candidatas: {candidates}. Existentes: {sorted(titles)}")


def _card_style(status: str) -> tuple[str, str]:
    # retorna (bg, label)
    s = (status or "").strip().upper()
    if s == "OK":
        return "#d1fae5", "Concluido"
    if s in ["NAO_OK", "NÃO OK", "NAO OK"]:
        return "#fee2e2", "Nao OK"
    return "#f3f4f6", "Pendente"


def _invalidate_data_cache():
    # Só limpa cache de dados, não de credenciais
    st.cache_data.clear()


# =========================
# LOAD TABLES
# =========================

@st.cache_resource
def gs_client():
    return get_gspread_client()


@st.cache_data(ttl=60)  # 60s para reduzir leituras e evitar 429
def load_tables(config_sheet_id: str, rules_sheet_id: str, logs_sheet_id: str):
    _require_ids()
    client = gs_client()

    ws_areas = _pick_first_existing_tab(client, config_sheet_id, WS_AREAS_CANDIDATES)
    ws_itens = _pick_first_existing_tab(client, config_sheet_id, WS_ITENS_CANDIDATES)
    ws_users = _pick_first_existing_tab(client, rules_sheet_id, WS_USERS_CANDIDATES)

    # EVENTS: cria se não existir, mas SEM ficar lendo metadata toda hora
    try:
        ws_events = _pick_first_existing_tab(client, logs_sheet_id, WS_EVENTS_CANDIDATES)
    except Exception:
        sh = client.open_by_key(logs_sheet_id)
        ws = get_or_create_worksheet(sh, "EVENTS", rows=5000, cols=20)
        ws_events = ws.title
        # garante header
        existing = ws.row_values(1)
        if not existing or all(str(x).strip() == "" for x in existing):
            ws.append_row(EVENT_COLS, value_input_option="RAW")

    areas_df = _normalize_cols(read_df(client, config_sheet_id, ws_areas))
    itens_df = _normalize_cols(read_df(client, config_sheet_id, ws_itens))
    users_df = _normalize_cols(read_df(client, rules_sheet_id, ws_users))

    # eventos: ler somente últimas linhas reduzindo custo
    events_df = _normalize_cols(read_df(client, logs_sheet_id, ws_events, last_n=1500))

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


def _validate_config(areas_df: pd.DataFrame, itens_df: pd.DataFrame):
    # AREAS mínimo
    for c in [COL_AREA_ID, COL_AREA_NOME]:
        if c not in areas_df.columns:
            raise RuntimeError(f"A aba AREAS precisa ter a coluna '{c}'. Colunas atuais: {list(areas_df.columns)}")

    # ITENS mínimo
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
    return vals if vals else TURNOS_DEFAULT


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
    # mapa: (area_id, turno, item_id) -> status
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


def _write_event(area_id: str, turno: str, item_id: str, texto: str, status: str, user_login: str, user_nome: str, tables):
    client = gs_client()
    ws_events = tables["ws_events"]
    now = _now()
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
    append_row(client, LOGS_SHEET_ID, ws_events, row, header_if_empty=EVENT_COLS)
    _invalidate_data_cache()


# =========================
# PAGES
# =========================

def page_dashboard(tables):
    st.subheader("Dashboard operacional")

    areas_df = tables["areas_df"]
    itens_df = tables["itens_df"]
    events_df = tables["events_df"]

    _validate_config(areas_df, itens_df)

    today = _now().date().isoformat()
    status_map = _latest_status_today(events_df, today)

    areas = _areas_list(areas_df)
    turnos = _turnos_list(itens_df)

    c1, c2 = st.columns([1, 1])
    with c1:
        if st.button("Atualizar dados agora"):
            _invalidate_data_cache()
            st.rerun()
    with c2:
        st.caption("Sem auto refresh para evitar erro 429. Use o botao acima quando precisar.")

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

    today = _now().date().isoformat()
    status_map = _latest_status_today(events_df, today)

    st.markdown(f"### {area_sel} | {turno_sel}")
    st.caption("Tudo começa PENDENTE. Clique OK ou Nao OK. Para desfazer, clique Desmarcar.")

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
                _write_event(area_id, turno_sel, item_id, texto, "OK", user_login, user_nome, tables)
                st.rerun()

        with b2:
            if st.button("Nao OK", key=f"nok_{area_id}_{turno_sel}_{item_id}"):
                _write_event(area_id, turno_sel, item_id, texto, "NAO_OK", user_login, user_nome, tables)
                st.rerun()

        with b3:
            # Desmarcar volta ao estado pendente (gravando evento PENDENTE)
            if st.button("Desmarcar", key=f"rst_{area_id}_{turno_sel}_{item_id}"):
                _write_event(area_id, turno_sel, item_id, texto, "PENDENTE", user_login, user_nome, tables)
                st.rerun()


def page_events(tables):
    st.subheader("EVENTS (leitura)")
    st.caption("Esta aba e o historico do checklist. Atualize quando precisar.")

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


# =========================
# MAIN
# =========================

def main():
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
        st.radio(
            "Ir para",
            ["Dashboard", "Checklist", "EVENTS"],
            key="nav",
            label_visibility="collapsed",
        )

    # Login (usa a planilha de regras)
    user = authenticate_user(
        rules_sheet_id=RULES_SHEET_ID,
        users_tab_candidates=WS_USERS_CANDIDATES,
        gs_client=gs_client,
    )
    if not user:
        return

    # carregar dados (cacheado)
    tables = load_tables(
        config_sheet_id=CONFIG_SHEET_ID,
        rules_sheet_id=RULES_SHEET_ID,
        logs_sheet_id=LOGS_SHEET_ID,
    )

    nav = st.session_state.get("nav", "Dashboard")
    if nav == "Checklist":
        page_checklist(tables, user_login=user["login"], user_nome=user["nome"])
    elif nav == "EVENTS":
        page_events(tables)
    else:
        page_dashboard(tables)


if __name__ == "__main__":
    main()
