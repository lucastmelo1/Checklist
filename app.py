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

WS_AREAS_CANDIDATES = ["Areas", "AREAS", "Checklist_Areas", "CHECKLIST_AREAS"]
WS_ITENS_CANDIDATES = ["Itens", "ITENS", "Checklist_Itens", "CHECKLIST_ITENS"]
WS_USERS_CANDIDATES = ["Users", "USERS", "Usuarios", "USUARIOS", "Login", "LOGIN"]
WS_LOGS_CANDIDATES = ["Checklist_Logs", "CHECKLIST_LOGS", "Logs", "LOGS"]

COL_AREA = "area"
COL_TURNO = "turno"
COL_ITEM_ID = "item_id"
COL_TEXTO = "texto"
COL_ATIVO = "ativo"
COL_ORDEM = "ordem"
COL_DEADLINE = "deadline"          # HH:MM
COL_TOL_MIN = "tolerancia_min"     # minutos
COL_DIA_SEMANA = "dia_semana"

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


@st.cache_resource
def service_client():
    return get_service_cached()


@st.cache_data(ttl=5)
def load_tables(cache_buster: str, config_sheet_id: str, rules_sheet_id: str, logs_sheet_id: str):
    svc = service_client()

    if not config_sheet_id or not rules_sheet_id or not logs_sheet_id:
        raise RuntimeError(
            "IDs das planilhas não configurados. Defina CONFIG_SHEET_ID, RULES_SHEET_ID, LOGS_SHEET_ID "
            "em Streamlit Secrets [app]."
        )

    ws_areas = pick_existing_tab(svc, config_sheet_id, WS_AREAS_CANDIDATES)
    ws_itens = pick_existing_tab(svc, config_sheet_id, WS_ITENS_CANDIDATES)
    ws_users = pick_existing_tab(svc, rules_sheet_id, WS_USERS_CANDIDATES)
    ws_logs = pick_existing_tab(svc, logs_sheet_id, WS_LOGS_CANDIDATES)

    areas_df = _normalize_cols(read_df(svc, config_sheet_id, ws_areas))
    itens_df = _normalize_cols(read_df(svc, config_sheet_id, ws_itens))
    users_df = _normalize_cols(read_df(svc, rules_sheet_id, ws_users))
    logs_df = _normalize_cols(read_df(svc, logs_sheet_id, ws_logs))

    return {
        "ws_areas": ws_areas,
        "ws_itens": ws_itens,
        "ws_users": ws_users,
        "ws_logs": ws_logs,
        "areas_df": areas_df,
        "itens_df": itens_df,
        "users_df": users_df,
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
    if COL_DEADLINE not in itens_df.columns:
        st.warning("A aba ITENS não tem a coluna 'deadline' (HH:MM). Sem alerta de atraso.")
    if COL_TOL_MIN not in itens_df.columns:
        st.warning("A aba ITENS não tem a coluna 'tolerancia_min'. Tolerância padrão será 0.")


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


def _parse_deadline_today(deadline_str: str) -> datetime | None:
    if not deadline_str or str(deadline_str).strip() == "" or str(deadline_str).lower() == "nan":
        return None
    s = str(deadline_str).strip()
    m = re.match(r"^(\d{1,2}):(\d{2})$", s)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    now = _now_ts()
    return datetime(now.year, now.month, now.day, hh, mm, tzinfo=TZ)


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


def _write_log(area: str, turno: str, item_id: str, texto: str, status: str, user: str, deadline: str, tol: int, tables):
    svc = service_client()
    ws_logs = tables["ws_logs"]

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


def page_dashboard(tables, dia_str: str):
    st.subheader("Dashboard operacional")
    st.caption("Atualização automática a cada 6 segundos. Você pode desligar.")

    st.session_state.setdefault("auto_refresh_sec", 6)
    sec = st.number_input("Auto refresh (segundos)", 0, 60, int(st.session_state["auto_refresh_sec"]), 1)
    st.session_state["auto_refresh_sec"] = int(sec)

    if st.button("Atualizar agora"):
        _invalidate_cache()
        st.rerun()

    if st.session_state["auto_refresh_sec"] > 0:
        st.session_state.setdefault("_last_refresh", time.time())
        if time.time() - st.session_state["_last_refresh"] >= st.session_state["auto_refresh_sec"]:
            st.session_state["_last_refresh"] = time.time()
            _invalidate_cache()
            st.rerun()

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
            items = _filter_items_for(itens_df, area, turno, dia_str)
            total = len(items)
            done = 0
            has_overdue = False

            for _, it in items.iterrows():
                item_id = str(it[COL_ITEM_ID]).strip()
                stt = status_map.get((today, area, turno, item_id), "").upper().strip()
                if stt == "OK":
                    done += 1

                dl_dt = _parse_deadline_today(str(it.get(COL_DEADLINE, "") or ""))
                tol = int(it.get(COL_TOL_MIN, 0) or 0)
                if dl_dt is not None and stt != "OK":
                    if now > (dl_dt + pd.Timedelta(minutes=tol)):
                        has_overdue = True

            pct = (done / total) if total else 0.0
            subtitle = f"{done}/{total} concluídos" if total else "Sem itens"

            color = "#0b6a5a" if pct >= 0.999 else ("#7a1f2b" if has_overdue else ("#8b6b12" if pct > 0 else "#2b2b2b"))
            pct_txt = f"{int(round(pct * 100, 0))}%"

            with cols[idx % len(cols)]:
                st.markdown(
                    f"""
                    <div style="border-radius:16px;padding:14px;margin:8px 0;background:{color};color:white;">
                      <div style="font-size:16px;font-weight:700;">{turno}</div>
                      <div style="font-size:13px;opacity:0.9;margin-top:4px;">{subtitle}</div>
                      <div style="font-size:22px;font-weight:800;margin-top:10px;">{pct_txt}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )


def page_checklist(tables, user: str, dia_str: str):
    st.subheader("Checklist")

    areas_df = tables["areas_df"]
    itens_df = tables["itens_df"]
    logs_df = tables["logs_df"]

    _validate_itens_columns(itens_df)

    areas = _available_areas(areas_df, itens_df)
    turnos = _available_turnos(itens_df)

    c1, c2 = st.columns([1, 1])
    with c1:
        area = st.selectbox("Área", areas, key="sel_area")
    with c2:
        turno = st.selectbox("Turno", turnos, key="sel_turno")

    items = _filter_items_for(itens_df, area, turno, dia_str)
    if items.empty:
        st.warning("Sem itens para esta combinação de Área, Turno e dia.")
        return

    now = _now_ts()
    today = now.date().isoformat()
    status_map = _latest_status_map(logs_df)

    st.markdown(f"### {area} | {turno}")

    for _, it in items.iterrows():
        item_id = str(it[COL_ITEM_ID]).strip()
        texto = str(it[COL_TEXTO]).strip()

        dl_raw = str(it.get(COL_DEADLINE, "") or "").strip()
        tol = int(it.get(COL_TOL_MIN, 0) or 0)

        dl_dt = _parse_deadline_today(dl_raw)
        current = status_map.get((today, area, turno, item_id), "")
        current_up = current.upper().strip()

        overdue = False
        if dl_dt is not None and current_up != "OK":
            overdue = now > (dl_dt + pd.Timedelta(minutes=tol))

        box_color = "#f3f4f6"
        label = "Pendente"
        if current_up == "OK":
            box_color = "#d1fae5"
            label = "Concluído"
        elif current_up in ["NÃO OK", "NAO OK"]:
            box_color = "#fee2e2"
            label = "Não OK"
        if overdue:
            box_color = "#ffe4e6"

        deadline_txt = ""
        if dl_dt is not None:
            deadline_txt = f"Deadline {dl_raw}"
            if tol > 0:
                deadline_txt += f" (+{tol} min)"
            if overdue:
                deadline_txt += " | ATRASADO"

        st.markdown(
            f"""
            <div style="border-radius:14px;padding:12px;background:{box_color};margin:10px 0;">
              <div style="font-size:14px;font-weight:800;">{texto}</div>
              <div style="font-size:12px;opacity:0.85;margin-top:4px;">{deadline_txt}</div>
              <div style="font-size:12px;margin-top:6px;"><b>Status:</b> {label}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        b1, b2, b3 = st.columns([1, 1, 1])
        with b1:
            if st.button("OK", key=f"ok_{area}_{turno}_{item_id}", type="primary"):
                _write_log(area, turno, item_id, texto, "OK", user, dl_raw, tol, tables)
                st.rerun()
        with b2:
            if st.button("NÃO OK", key=f"nok_{area}_{turno}_{item_id}"):
                _write_log(area, turno, item_id, texto, "NÃO OK", user, dl_raw, tol, tables)
                st.rerun()
        with b3:
            if st.button("Atualizar", key=f"rf_{area}_{turno}_{item_id}"):
                _invalidate_cache()
                st.rerun()


def page_logs(tables):
    st.subheader("LOGS")
    if st.button("Atualizar LOGS"):
        _invalidate_cache()
        st.rerun()

    df = tables["logs_df"].copy()
    if df.empty:
        st.info("Sem registros ainda.")
        return

    if "ts" in df.columns:
        df["ts_dt"] = pd.to_datetime(df["ts"], errors="coerce")
        df = df.sort_values("ts_dt", ascending=False).drop(columns=["ts_dt"], errors="ignore")

    st.dataframe(df, use_container_width=True, height=520)


def main():
    with st.sidebar:
        st.markdown("## Checklist Operacional")

        if st.button("Logout"):
            for k in list(st.session_state.keys()):
                if k not in ["cache_buster"]:
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

    st.session_state.setdefault("cache_buster", "v1")

    # Diagnóstico objetivo no login: se RULES_SHEET_ID estiver errado ou sem permissão,
    # o erro vai aparecer na tela com status 403/404/400 e details.
    try:
        user = authenticate_user(
            rules_sheet_id=RULES_SHEET_ID,
            users_tab_candidates=WS_USERS_CANDIDATES,
            service_client=service_client,
        )
    except Exception as e:
        st.error("Falha ao validar login lendo a planilha RULES_SHEET_ID.")
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
        st.error("Falha ao carregar tabelas do Google Sheets.")
        st.code(str(e))
        st.stop()

    nav = st.session_state.get("nav", "Dashboard")
    if nav == "Checklist":
        page_checklist(tables, user, dia_str)
    elif nav == "LOGS":
        page_logs(tables)
    else:
        page_dashboard(tables, dia_str)


if __name__ == "__main__":
    main()
