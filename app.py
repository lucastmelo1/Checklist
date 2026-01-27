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
    pick_existing_tab,
)
from auth import authenticate_user

TZ = ZoneInfo("America/Sao_Paulo")

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
# CONFIG
# =========================

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
WS_LOGS_CANDIDATES = ["LOGS", "Logs", "CHECKLIST_LOGS", "Checklist_Logs", "Checklist_Logs"]

COL_AREA = "area"
COL_TURNO = "turno"
COL_ITEM_ID = "item_id"
COL_TEXTO = "texto"
COL_ATIVO = "ativo"
COL_ORDEM = "ordem"
COL_DEADLINE = "deadline"          # HH:MM (opcional)
COL_TOL_MIN = "tolerancia_min"     # minutos (opcional)
COL_DIA_SEMANA = "dia_semana"      # opcional

# Status padronizados
STATUS_OK = "OK"
STATUS_NOK = "NÃO OK"
STATUS_PEND = "PENDENTE"

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


# =========================
# HELPERS
# =========================

def _now_ts() -> datetime:
    return datetime.now(TZ)


def _weekday_pt(d: date) -> str:
    names = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
    return names[d.weekday()]


def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def _rename_cols_with_aliases(df: pd.DataFrame, mapping: dict[str, list[str]]) -> pd.DataFrame:
    df = df.copy()
    cols = [c.strip().lower() for c in df.columns]
    colset = set(cols)

    rename = {}
    for final_col, aliases in mapping.items():
        final_col_l = final_col.lower()
        if final_col_l in colset:
            continue
        for a in aliases:
            a_l = a.lower()
            if a_l in colset:
                idx = cols.index(a_l)
                rename[df.columns[idx]] = final_col_l
                colset.add(final_col_l)
                break

    if rename:
        df = df.rename(columns=rename)
    return df


def _normalize_itens_df(itens_df: pd.DataFrame) -> pd.DataFrame:
    itens_df = _normalize_cols(itens_df)
    itens_df = _rename_cols_with_aliases(
        itens_df,
        {
            "area": ["area_id", "areaid", "setor", "departamento"],
            "texto": ["descricao", "desc", "item", "tarefa", "pergunta", "texto_item"],
            "ativo": ["is_active", "active"],
            "ordem": ["order", "sequencia", "seq", "posicao"],
            "deadline": ["prazo", "hora_limite", "horario", "limite", "cutoff"],
            "tolerancia_min": ["tolerancia", "tolerancia_minutos", "tolerancia_em_min", "tolerancia (min)"],
            "dia_semana": ["dia", "weekday"],
        },
    )
    return itens_df


def _normalize_areas_df(areas_df: pd.DataFrame) -> pd.DataFrame:
    areas_df = _normalize_cols(areas_df)
    areas_df = _rename_cols_with_aliases(areas_df, {"area": ["area_id", "setor", "departamento", "nome"]})
    return areas_df


def _coerce_bool(x) -> bool:
    if pd.isna(x):
        return False
    s = str(x).strip().lower()
    return s in ["1", "true", "sim", "yes", "y", "ativo"]


def _validate_itens_columns(itens_df: pd.DataFrame):
    needed = {COL_AREA, COL_TURNO, COL_ITEM_ID, COL_TEXTO}
    missing = [c for c in needed if c not in itens_df.columns]
    if missing:
        raise RuntimeError(
            f"A aba ITENS precisa ter as colunas (ou aliases): {sorted(list(needed))}. "
            f"Faltando: {missing}. Colunas atuais: {list(itens_df.columns)}"
        )
    if COL_DEADLINE not in itens_df.columns:
        st.warning("A aba ITENS não tem a coluna 'deadline' (HH:MM). Sem isso, não haverá alerta de atraso.")
    if COL_TOL_MIN not in itens_df.columns:
        st.warning("A aba ITENS não tem a coluna 'tolerancia_min' (minutos). Sem isso, tolerância padrão será 0.")


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


def _invalidate_cache():
    st.cache_data.clear()


@st.cache_resource
def service_client():
    return get_service_cached()


@st.cache_data(ttl=10)
def load_tables(cache_buster: str, config_sheet_id: str, rules_sheet_id: str, logs_sheet_id: str):
    svc = service_client()

    if not config_sheet_id or not rules_sheet_id or not logs_sheet_id:
        raise RuntimeError(
            "IDs não configurados. Adicione no Streamlit Secrets uma seção [app] com "
            "CONFIG_SHEET_ID, RULES_SHEET_ID e LOGS_SHEET_ID."
        )

    ws_areas = pick_existing_tab(svc, config_sheet_id, WS_AREAS_CANDIDATES)
    ws_itens = pick_existing_tab(svc, config_sheet_id, WS_ITENS_CANDIDATES)
    ws_users = pick_existing_tab(svc, rules_sheet_id, WS_USERS_CANDIDATES)
    ws_logs = pick_existing_tab(svc, logs_sheet_id, WS_LOGS_CANDIDATES)

    areas_df = _normalize_areas_df(read_df(svc, config_sheet_id, ws_areas))
    itens_df = _normalize_itens_df(read_df(svc, config_sheet_id, ws_itens))
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


def _latest_status_map(logs_df: pd.DataFrame):
    """
    Retorna o último status por (data, area, turno, item_id).
    Regras:
      - Se não existe log, status é PENDENTE (implícito).
      - Se último log for PENDENTE, volta ao estado desmarcado.
    """
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
    df["status"] = df["status"].astype(str).str.strip()

    df = df.sort_values("ts")
    latest = df.groupby(["data", "area", "turno", "item_id"], as_index=False).tail(1)

    mp = {}
    for _, r in latest.iterrows():
        stt = str(r["status"]).strip().upper()
        if stt in ["", "NAN", "NONE"]:
            stt = STATUS_PEND
        # normaliza NAO OK sem acento
        if stt == "NAO OK":
            stt = STATUS_NOK
        if stt not in [STATUS_OK, STATUS_NOK, STATUS_PEND]:
            stt = STATUS_PEND
        mp[(r["data"], r["area"], r["turno"], r["item_id"])] = stt
    return mp


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


def _status_style(status: str, overdue: bool) -> tuple[str, str]:
    """
    Retorna (cor do card, label do status).
    """
    status = (status or STATUS_PEND).strip().upper()
    if status == "NAO OK":
        status = STATUS_NOK

    if status == STATUS_OK:
        return ("#d1fae5", "Concluído (OK)")
    if status == STATUS_NOK:
        return ("#fee2e2", "Marcado como NÃO OK")

    # pendente
    if overdue:
        return ("#ffe4e6", "Pendente (Atrasado)")
    return ("#f3f4f6", "Pendente")


def _card_color_kpi(done_pct: float, has_overdue: bool) -> str:
    if has_overdue:
        return "#7a1f2b"
    if done_pct >= 0.999:
        return "#0b6a5a"
    if done_pct > 0:
        return "#8b6b12"
    return "#2b2b2b"


def _render_kpi_card(title: str, subtitle: str, pct: float, has_overdue: bool):
    color = _card_color_kpi(pct, has_overdue)
    pct_txt = f"{int(round(pct * 100, 0))}%"
    html = f"""
    <div style="border-radius:16px;padding:14px 14px 12px 14px;margin:8px 0;background:{color};color:white;">
      <div style="font-size:16px;font-weight:700;line-height:1.1;">{title}</div>
      <div style="font-size:13px;opacity:0.9;margin-top:4px;">{subtitle}</div>
      <div style="font-size:22px;font-weight:800;margin-top:10px;">{pct_txt}</div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)


# =========================
# PAGES
# =========================

def page_dashboard(tables, dia_str: str):
    st.subheader("Dashboard operacional")
    st.caption("Atualização automática. Se estiver lento, use Atualizar agora.")
    st.session_state.setdefault("auto_refresh_sec", 6)

    colA, colB = st.columns([1, 1])
    with colA:
        st.session_state["auto_refresh_sec"] = st.number_input(
            "Auto refresh (segundos)",
            min_value=0,
            max_value=60,
            value=int(st.session_state["auto_refresh_sec"]),
            step=1,
        )
    with colB:
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
    today = _now_ts().date().isoformat()
    now = _now_ts()

    for area in areas:
        st.markdown(f"### {area}")
        cols = st.columns(2) if len(turnos) >= 2 else st.columns(1)

        for idx, turno in enumerate(turnos):
            items = _filter_items_for(itens_df, area, turno, dia_str)
            total = len(items)

            if total == 0:
                with cols[idx % len(cols)]:
                    _render_kpi_card(turno, "Sem itens para este dia", 0.0, False)
                continue

            done = 0
            has_overdue = False

            for _, it in items.iterrows():
                item_id = str(it[COL_ITEM_ID]).strip()
                key = (today, area, turno, item_id)
                stt = status_map.get(key, STATUS_PEND)

                if stt == STATUS_OK:
                    done += 1

                dl = _parse_deadline_today(str(it.get(COL_DEADLINE, "") or "").strip())
                tol = int(it.get(COL_TOL_MIN, 0) or 0)
                if dl is not None and stt != STATUS_OK:
                    if now > (dl + pd.Timedelta(minutes=tol)):
                        has_overdue = True

            pct = done / total if total else 0.0
            subtitle = f"{done}/{total} OK"

            with cols[idx % len(cols)]:
                _render_kpi_card(turno, subtitle, pct, has_overdue)


def page_checklist(tables, user: str, dia_str: str):
    st.subheader("Checklist")

    areas_df = tables["areas_df"]
    itens_df = tables["itens_df"]
    logs_df = tables["logs_df"]

    _validate_itens_columns(itens_df)

    areas = _available_areas(areas_df, itens_df)
    turnos = _available_turnos(itens_df)

    st.write("Selecione Área e Turno e toque em OK para abrir os itens.")
    c1, c2 = st.columns([1, 1])
    with c1:
        area = st.selectbox("Área", areas, key="sel_area")
    with c2:
        turno = st.selectbox("Turno", turnos, key="sel_turno")

    if st.button("OK", type="primary"):
        st.session_state["ctx_area"] = area
        st.session_state["ctx_turno"] = turno
        st.session_state["open_items"] = True
        st.rerun()

    if not st.session_state.get("open_items", False):
        st.info("Escolha Área e Turno e aperte OK.")
        return

    area = st.session_state.get("ctx_area", area)
    turno = st.session_state.get("ctx_turno", turno)

    items = _filter_items_for(itens_df, area, turno, dia_str)
    if items.empty:
        st.warning("Sem itens para esta combinação de Área, Turno e dia.")
        return

    now = _now_ts()
    today = now.date().isoformat()
    status_map = _latest_status_map(logs_df)

    st.markdown(f"### {area} | {turno}")
    st.caption("Tudo começa como Pendente. Clique em OK, NÃO OK ou DESMARCAR. O card muda de cor conforme o último clique.")

    # progresso
    total = len(items)
    ok_count = 0
    for _, it in items.iterrows():
        item_id = str(it[COL_ITEM_ID]).strip()
        stt = status_map.get((today, area, turno, item_id), STATUS_PEND)
        if stt == STATUS_OK:
            ok_count += 1
    st.progress(ok_count / total if total else 0.0)
    st.caption(f"{ok_count}/{total} concluídos (OK)")

    # render itens
    for _, it in items.iterrows():
        item_id = str(it[COL_ITEM_ID]).strip()
        texto = str(it[COL_TEXTO]).strip()

        dl_raw = str(it.get(COL_DEADLINE, "") or "").strip()
        tol = int(it.get(COL_TOL_MIN, 0) or 0)
        dl_dt = _parse_deadline_today(dl_raw)

        current = status_map.get((today, area, turno, item_id), STATUS_PEND)
        overdue = False
        if dl_dt is not None and current != STATUS_OK:
            overdue = now > (dl_dt + pd.Timedelta(minutes=tol))

        box_color, label = _status_style(current, overdue)

        deadline_txt = ""
        if dl_dt is not None:
            deadline_txt = f"Deadline {dl_raw}"
            if tol > 0:
                deadline_txt += f" (+{tol} min)"
            if overdue and current != STATUS_OK:
                deadline_txt += " | ATRASADO"

        st.markdown(
            f"""
            <div style="border-radius:14px;padding:12px 12px;background:{box_color};margin:10px 0;">
              <div style="font-size:14px;font-weight:800;">{texto}</div>
              <div style="font-size:12px;opacity:0.85;margin-top:4px;">{deadline_txt}</div>
              <div style="font-size:12px;margin-top:6px;"><b>Status:</b> {label}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        b1, b2, b3, b4 = st.columns([1, 1, 1, 1])
        with b1:
            if st.button("OK", key=f"ok_{area}_{turno}_{item_id}", type="primary"):
                _write_log(area, turno, item_id, texto, STATUS_OK, user, dl_raw, tol, tables)
                st.rerun()
        with b2:
            if st.button("NÃO OK", key=f"nok_{area}_{turno}_{item_id}"):
                _write_log(area, turno, item_id, texto, STATUS_NOK, user, dl_raw, tol, tables)
                st.rerun()
        with b3:
            # desmarcar volta ao estado original
            if st.button("DESMARCAR", key=f"undo_{area}_{turno}_{item_id}"):
                _write_log(area, turno, item_id, texto, STATUS_PEND, user, dl_raw, tol, tables)
                st.rerun()
        with b4:
            if st.button("Atualizar", key=f"rf_{area}_{turno}_{item_id}"):
                _invalidate_cache()
                st.rerun()


def page_logs(tables):
    st.subheader("LOGS")
    st.caption("Últimos registros. Atualize para ver alterações recentes.")
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


# =========================
# MAIN
# =========================

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
        except Exception as e:
            st.error("Falha ao conectar no Google Sheets")
            st.caption(str(e))

        st.markdown("### Navegação")
        st.session_state.setdefault("nav", "Checklist")
        st.radio(
            "Ir para",
            ["Dashboard", "Checklist", "LOGS"],
            key="nav",
            label_visibility="collapsed",
        )

    st.session_state.setdefault("cache_buster", "v1")

    user = authenticate_user(
        rules_sheet_id=RULES_SHEET_ID,
        users_tab_candidates=WS_USERS_CANDIDATES,
        service_client=service_client,
    )
    if not user:
        return

    today = _now_ts().date()
    dia_str = _weekday_pt(today)

    tables = load_tables(
        cache_buster=st.session_state["cache_buster"],
        config_sheet_id=CONFIG_SHEET_ID,
        rules_sheet_id=RULES_SHEET_ID,
        logs_sheet_id=LOGS_SHEET_ID,
    )

    nav = st.session_state.get("nav")
    if nav == "Dashboard":
        page_dashboard(tables, dia_str)
    elif nav == "LOGS":
        page_logs(tables)
    else:
        page_checklist(tables, user, dia_str)


if __name__ == "__main__":
    main()
