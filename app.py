import os
import time
import re
from datetime import datetime, date
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st

from sheets_client import get_service_cached, read_df, append_row, list_sheet_titles
from auth import authenticate_user

TZ = ZoneInfo("America/Sao_Paulo")


# =========================
# CONFIGURACAO (NOMES DE ABAS)
# =========================

# Planilhas (IDs): recomendo colocar no Streamlit Secrets
# [app]
# CONFIG_SHEET_ID="..."
# RULES_SHEET_ID="..."
# LOGS_SHEET_ID="..."
#
# ou usar variaveis de ambiente local
def _get_cfg(name: str, default: str = "") -> str:
    if hasattr(st, "secrets") and "app" in st.secrets and name in st.secrets["app"]:
        return str(st.secrets["app"][name]).strip()
    return os.getenv(name, default).strip()


CONFIG_SHEET_ID = _get_cfg("CONFIG_SHEET_ID", "")
RULES_SHEET_ID = _get_cfg("RULES_SHEET_ID", "")
LOGS_SHEET_ID = _get_cfg("LOGS_SHEET_ID", "")

# Tabs esperadas (tenta fallback automaticamente)
WS_AREAS_CANDIDATES = ["Areas", "AREAS", "Checklist_Areas", "CHECKLIST_AREAS"]
WS_ITENS_CANDIDATES = ["Itens", "ITENS", "Checklist_Itens", "CHECKLIST_ITENS"]
WS_USERS_CANDIDATES = ["Users", "USERS", "Usuarios", "USUARIOS", "Login", "LOGIN"]
WS_PARAMS_CANDIDATES = ["Params", "PARAMS", "Regras", "REGRAS", "Rules", "RULES"]
WS_LOGS_CANDIDATES = ["Checklist_Logs", "CHECKLIST_LOGS", "Logs", "LOGS"]

# Colunas minimas
COL_AREA = "area"
COL_TURNO = "turno"
COL_ITEM_ID = "item_id"
COL_TEXTO = "texto"
COL_ATIVO = "ativo"
COL_ORDEM = "ordem"
COL_DEADLINE = "deadline"  # HH:MM
COL_TOL_MIN = "tolerancia_min"  # minutos
COL_DIA_SEMANA = "dia_semana"  # opcional
COL_DESC = "descricao"  # opcional

# Logs
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
# UI
# =========================

st.set_page_config(page_title="Checklist Operacional", layout="wide", initial_sidebar_state="expanded")

MOBILE_CSS = """
<style>
/* mobile friendly spacing */
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

def _now_ts() -> datetime:
    return datetime.now(TZ)


def _weekday_pt(d: date) -> str:
    # 0=Mon
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
    # tenta case-insensitive
    lower_map = {t.lower(): t for t in titles}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    raise RuntimeError(f"Não encontrei nenhuma aba válida em {spreadsheet_id}. Candidatas: {candidates}. Existentes: {sorted(titles)}")


@st.cache_resource
def service_client():
    return get_service_cached()


@st.cache_data(ttl=5)
def load_tables(cache_buster: str, config_sheet_id: str, rules_sheet_id: str, logs_sheet_id: str):
    svc = service_client()

    if not config_sheet_id or not rules_sheet_id or not logs_sheet_id:
        raise RuntimeError(
            "IDs das planilhas não configurados. Defina CONFIG_SHEET_ID, RULES_SHEET_ID, LOGS_SHEET_ID "
            "no Streamlit Secrets [app] ou em variáveis de ambiente."
        )

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
            f"A aba ITENS precisa ter as colunas: {sorted(list(needed))}. "
            f"Faltando: {missing}. Colunas atuais: {list(itens_df.columns)}"
        )
    # deadline e tolerancia sao opcionais, mas recomendados
    if COL_DEADLINE not in itens_df.columns:
        st.warning("A aba ITENS não tem a coluna 'deadline' (HH:MM). Sem isso, não haverá alerta de atraso.")
    if COL_TOL_MIN not in itens_df.columns:
        st.warning("A aba ITENS não tem a coluna 'tolerancia_min' (minutos). Sem isso, tolerância padrão será 0.")


def _available_areas(areas_df: pd.DataFrame, itens_df: pd.DataFrame) -> list[str]:
    # Se areas_df tiver coluna "area" usa ela, senao deriva do itens_df
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

    # ativo
    if COL_ATIVO in df.columns:
        df = df[df[COL_ATIVO].apply(_coerce_bool) == True]

    # dia da semana opcional (se existir filtra, se nao, ignora)
    if COL_DIA_SEMANA in df.columns and df[COL_DIA_SEMANA].notna().any():
        dcol = df[COL_DIA_SEMANA].astype(str).str.strip()
        # aceita vazio como "vale para todos"
        df = df[(dcol == "") | (dcol.str.lower() == "nan") | (dcol == dia_semana)]

    # ordem
    if COL_ORDEM in df.columns:
        df[COL_ORDEM] = pd.to_numeric(df[COL_ORDEM], errors="coerce")
        df = df.sort_values([COL_ORDEM, COL_ITEM_ID], na_position="last")
    else:
        df = df.sort_values([COL_ITEM_ID])

    # tolerancia default
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
    # devolve status mais recente por (data, area, turno, item_id)
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


def _card_color(done_pct: float, has_overdue: bool) -> str:
    if has_overdue:
        return "#7a1f2b"  # vermelho escuro
    if done_pct >= 0.999:
        return "#0b6a5a"  # verde
    if done_pct > 0:
        return "#8b6b12"  # dourado escuro
    return "#2b2b2b"      # neutro


def _render_kpi_card(title: str, subtitle: str, pct: float, has_overdue: bool):
    color = _card_color(pct, has_overdue)
    pct_txt = f"{int(round(pct*100, 0))}%"
    html = f"""
    <div style="border-radius:16px;padding:14px 14px 12px 14px;margin:8px 0;background:{color};color:white;">
      <div style="font-size:16px;font-weight:700;line-height:1.1;">{title}</div>
      <div style="font-size:13px;opacity:0.9;margin-top:4px;">{subtitle}</div>
      <div style="font-size:22px;font-weight:800;margin-top:10px;">{pct_txt}</div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)


def _invalidate_cache():
    st.cache_data.clear()
    # cache_resource do service não precisa limpar


# =========================
# PAGES
# =========================

def page_dashboard(tables, dia_str: str):
    st.subheader("Dashboard operacional")

    # auto refresh
    st.caption("Atualização automática a cada 6 segundos. Se estiver lento, reduza a frequência ou use o botão Atualizar.")
    st.session_state.setdefault("auto_refresh_sec", 6)

    colA, colB = st.columns([1, 1])
    with colA:
        st.session_state["auto_refresh_sec"] = st.number_input(
            "Auto refresh (segundos)",
            min_value=0,
            max_value=60,
            value=int(st.session_state["auto_refresh_sec"]),
            step=1
        )
    with colB:
        if st.button("Atualizar agora"):
            _invalidate_cache()
            st.rerun()

    if st.session_state["auto_refresh_sec"] > 0:
        # auto rerun sem depender de lib externa
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

    # Construir cards por area/turno
    for area in areas:
        st.markdown(f"### {area}")
        cols = st.columns(2) if len(turnos) >= 2 else st.columns(1)

        for idx, turno in enumerate(turnos):
            items = _filter_items_for(itens_df, area, turno, dia_str)
            total = len(items)
            if total == 0:
                with cols[idx % len(cols)]:
                    _render_kpi_card(f"{turno}", "Sem itens para este dia", 0.0, False)
                continue

            done = 0
            has_overdue = False
            now = _now_ts()
            for _, it in items.iterrows():
                item_id = str(it[COL_ITEM_ID]).strip()
                key = (now.date().isoformat(), area, turno, item_id)
                stt = status_map.get(key, "")
                if stt.upper() == "OK":
                    done += 1

                dl = _parse_deadline_today(it.get(COL_DEADLINE, ""))
                tol = int(it.get(COL_TOL_MIN, 0) or 0)
                if dl is not None:
                    limit = dl.replace()  # tz ok
                    limit = limit if tol <= 0 else (limit.replace() + pd.Timedelta(minutes=tol))
                    if stt.upper() != "OK" and now > (dl + pd.Timedelta(minutes=tol)):
                        has_overdue = True

            pct = done / total if total else 0.0
            subtitle = f"{done}/{total} concluídos"

            with cols[idx % len(cols)]:
                _render_kpi_card(turno, subtitle, pct, has_overdue)

    st.caption("Dica: o dashboard mostra progresso ao vivo baseado no LOGS. Se algo não aparece, verifique se a aba de LOGS existe e se a Service Account tem acesso.")


def page_checklist(tables, user: str, dia_str: str):
    st.subheader("Checklist")

    areas_df = tables["areas_df"]
    itens_df = tables["itens_df"]
    logs_df = tables["logs_df"]

    _validate_itens_columns(itens_df)

    areas = _available_areas(areas_df, itens_df)
    turnos = _available_turnos(itens_df)

    # Step 1: escolher area e turno
    with st.container():
        st.write("Selecione Área e Turno e toque em OK para abrir os itens.")
        c1, c2 = st.columns([1, 1])
        with c1:
            area = st.selectbox("Área", areas, key="sel_area")
        with c2:
            turno = st.selectbox("Turno", turnos, key="sel_turno")

        if st.button("OK", type="primary"):
            st.session_state["ctx_area"] = area
            st.session_state["ctx_turno"] = turno
            st.session_state["ctx_day"] = dia_str
            st.session_state["open_items"] = True
            # Ao iniciar operação, manda para dashboard também
            st.session_state["nav"] = "Dashboard"
            st.rerun()

    if not st.session_state.get("open_items", False):
        st.info("Escolha Área e Turno e aperte OK.")
        return

    area = st.session_state.get("ctx_area", area)
    turno = st.session_state.get("ctx_turno", turno)

    # Itens filtrados
    items = _filter_items_for(itens_df, area, turno, dia_str)
    if items.empty:
        st.warning("Sem itens para esta combinação de Área, Turno e dia.")
        return

    # status mais recente por item para hoje
    now = _now_ts()
    today = now.date().isoformat()
    status_map = _latest_status_map(logs_df)

    st.markdown(f"### {area} | {turno}")
    st.caption("Os itens começam sem marcação. Use OK ou NÃO OK por item. Isso registra no LOGS e atualiza o dashboard.")

    for i, it in items.iterrows():
        item_id = str(it[COL_ITEM_ID]).strip()
        texto = str(it[COL_TEXTO]).strip()

        dl_raw = str(it.get(COL_DEADLINE, "") or "").strip()
        tol = int(it.get(COL_TOL_MIN, 0) or 0)

        dl_dt = _parse_deadline_today(dl_raw)
        overdue = False
        if dl_dt is not None:
            overdue = (status_map.get((today, area, turno, item_id), "").upper() != "OK") and (now > (dl_dt + pd.Timedelta(minutes=tol)))

        current = status_map.get((today, area, turno, item_id), "")
        current_up = current.upper().strip()

        # UI
        box_color = "#f3f4f6"
        label = "Pendente"
        if current_up == "OK":
            box_color = "#d1fae5"
            label = "Concluído"
        elif current_up == "NÃO OK" or current_up == "NAO OK":
            box_color = "#fee2e2"
            label = "Não OK"
        elif current_up:
            box_color = "#e5e7eb"
            label = current_up

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
            <div style="border-radius:14px;padding:12px 12px;background:{box_color};margin:10px 0;">
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
            # refresh status local
            if st.button("Atualizar", key=f"rf_{area}_{turno}_{item_id}"):
                _invalidate_cache()
                st.rerun()


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


def page_logs(tables):
    st.subheader("LOGS")
    st.caption("Visualização em leitura. Atualize para ver registros recentes.")
    if st.button("Atualizar LOGS"):
        _invalidate_cache()
        st.rerun()

    df = tables["logs_df"].copy()
    if df.empty:
        st.info("Sem registros ainda.")
        return

    # ordenar
    if "ts" in df.columns:
        df["ts_dt"] = pd.to_datetime(df["ts"], errors="coerce")
        df = df.sort_values("ts_dt", ascending=False).drop(columns=["ts_dt"], errors="ignore")

    st.dataframe(df, use_container_width=True, height=520)


# =========================
# MAIN
# =========================

def main():
    # Sidebar
    with st.sidebar:
        st.markdown("## Checklist Operacional")

        if st.button("Logout"):
            for k in list(st.session_state.keys()):
                if k not in ["cache_buster"]:
                    st.session_state.pop(k, None)
            st.rerun()

        # IDs (somente leitura, não expor credenciais)
        st.markdown("### Status")
        try:
            _ = service_client()
            st.success("Google Sheets conectado")
        except Exception:
            st.error("Falha ao conectar no Google Sheets")

        st.markdown("### Navegação")
        st.session_state.setdefault("nav", "Dashboard")
        nav = st.radio(
            "Ir para",
            ["Dashboard", "Checklist", "LOGS"],
            key="nav",
            label_visibility="collapsed",
        )

    # Login
    st.session_state.setdefault("cache_buster", "v1")
    user = authenticate_user(
        rules_sheet_id=RULES_SHEET_ID,
        users_tab_candidates=WS_USERS_CANDIDATES,
        service_client=service_client,
    )
    if not user:
        return

    # Dia da semana
    today = _now_ts().date()
    dia_str = _weekday_pt(today)

    # Carregar dados
    tables = load_tables(
        cache_buster=st.session_state["cache_buster"],
        config_sheet_id=CONFIG_SHEET_ID,
        rules_sheet_id=RULES_SHEET_ID,
        logs_sheet_id=LOGS_SHEET_ID,
    )

    # Roteamento
    if st.session_state.get("nav") == "Checklist":
        page_checklist(tables, user, dia_str)
    elif st.session_state.get("nav") == "LOGS":
        page_logs(tables)
    else:
        page_dashboard(tables, dia_str)


if __name__ == "__main__":
    main()
