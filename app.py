import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date, time as dtime
from dateutil import tz
import re

# =========================
# Config
# =========================
st.set_page_config(page_title="Checklist Operacional", layout="wide")

LOCAL_TZ = tz.gettz("America/Sao_Paulo")

STATUS_OK = "OK"
STATUS_NAO_OK = "NAO_OK"
STATUS_PENDENTE = "PENDENTE"
STATUS_ATRASADO = "ATRASADO"

AREAS_CANDIDATES = ["AREAS", "ÁREAS", "AREAS_"]
ITENS_CANDIDATES = ["ITENS", "ITEMS", "CHECKLIST", "CHECKLIST_ITENS"]
DEADLINES_CANDIDATES = ["DEADLINES", "PRAZOS", "HORARIOS", "HORÁRIOS"]
USERS_CANDIDATES = ["USUARIOS", "USERS", "LOGIN", "USUÁRIOS"]
LOGS_CANDIDATES = ["LOGS", "EVENTS", "REGISTROS", "HISTORICO", "HISTÓRICO"]

# =========================
# CSS (botões neutros + cards)
# =========================
st.markdown(
    """
<style>
/* deixar botões neutros para não parecer "marcado" */
div.stButton > button {
  background: white !important;
  color: #111 !important;
  border: 1px solid #d0d0d0 !important;
  border-radius: 10px !important;
  padding: 0.55rem 1.0rem !important;
  font-weight: 600 !important;
}
div.stButton > button:hover {
  border-color: #999 !important;
}

/* cards */
.card {
  padding: 16px 16px;
  border-radius: 14px;
  border: 1px solid rgba(0,0,0,0.08);
  margin-bottom: 10px;
}
.card-title { font-size: 18px; font-weight: 700; margin: 0 0 6px 0; }
.card-sub { font-size: 14px; opacity: 0.90; margin: 0; }

.badge {
  display: inline-block;
  padding: 4px 10px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 800;
  border: 1px solid rgba(0,0,0,0.10);
  margin-right: 8px;
}

.badge-ok { background: rgba(0, 200, 120, 0.14); }
.badge-no { background: rgba(255, 80, 80, 0.16); }
.badge-pend { background: rgba(120, 120, 120, 0.12); }
.badge-atr { background: rgba(255, 180, 0, 0.18); }

.muted { opacity: 0.75; }
</style>
""",
    unsafe_allow_html=True,
)

# =========================
# Helpers
# =========================
def _now() -> datetime:
    return datetime.now(tz=LOCAL_TZ)

def _today() -> date:
    return _now().date()

def _safe_str(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()

def _norm_col(c: str) -> str:
    c = c.strip().lower()
    c = re.sub(r"\s+", "_", c)
    return c

def _parse_hhmm(s: str) -> dtime | None:
    s = _safe_str(s)
    if not s:
        return None
    # aceita HH:MM
    m = re.match(r"^(\d{1,2}):(\d{2})$", s)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    if hh < 0 or hh > 23 or mm < 0 or mm > 59:
        return None
    return dtime(hour=hh, minute=mm)

def _weekday_pt(dt: date) -> str:
    # simples e suficiente
    nomes = ["Segunda", "Terca", "Quarta", "Quinta", "Sexta", "Sabado", "Domingo"]
    return nomes[dt.weekday()]

def _get_secrets_app() -> dict:
    return dict(st.secrets.get("app", {}))

def _get_sheet_ids() -> tuple[str, str, str]:
    app = _get_secrets_app()
    config_id = app.get("config_sheet_id", "").strip()
    rules_id = app.get("rules_sheet_id", "").strip()
    logs_id = app.get("logs_sheet_id", "").strip()
    if not config_id or not rules_id or not logs_id:
        st.error("Faltam IDs no secrets.toml em [app]: config_sheet_id, rules_sheet_id, logs_sheet_id.")
        st.stop()
    return config_id, rules_id, logs_id

def _get_tab_names() -> dict:
    app = _get_secrets_app()
    return {
        "areas": app.get("config_areas_tab", "").strip(),
        "itens": app.get("config_items_tab", "").strip(),
        "deadlines": app.get("config_deadlines_tab", "").strip(),
        "users": app.get("rules_users_tab", "").strip(),
        "logs": app.get("logs_tab", "").strip(),
    }

@st.cache_resource
def _get_gspread_client():
    if "gcp_service_account" not in st.secrets:
        st.error("Faltou [gcp_service_account] no secrets.toml.")
        st.stop()

    sa_info = dict(st.secrets["gcp_service_account"])
    # scopes necessários
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
    return gspread.authorize(creds)

def _pick_worksheet(spreadsheet, explicit_name: str, candidates: list[str]):
    if explicit_name:
        try:
            return spreadsheet.worksheet(explicit_name)
        except Exception:
            pass

    titles = [ws.title for ws in spreadsheet.worksheets()]
    titles_set = {t.strip(): t for t in titles}
    for c in candidates:
        for t in titles:
            if t.strip().upper() == c.strip().upper():
                return spreadsheet.worksheet(t)
    # tenta parcial
    for c in candidates:
        for t in titles:
            if c.strip().upper() in t.strip().upper():
                return spreadsheet.worksheet(t)

    raise RuntimeError(f"Não achei aba. Candidatas: {candidates}. Encontradas: {titles}")

def _df_from_ws(ws) -> pd.DataFrame:
    values = ws.get_all_values()
    if not values or len(values) < 1:
        return pd.DataFrame()
    header = [_norm_col(h) for h in values[0]]
    rows = values[1:]
    df = pd.DataFrame(rows, columns=header)
    # limpar colunas vazias
    df = df.loc[:, [c for c in df.columns if c and c != ""]]
    return df

@st.cache_data(ttl=45)
def load_tables(_cache_buster: int, config_sheet_id: str, rules_sheet_id: str, logs_sheet_id: str, tabs: dict):
    gc = _get_gspread_client()

    sh_config = gc.open_by_key(config_sheet_id)
    sh_rules = gc.open_by_key(rules_sheet_id)
    sh_logs = gc.open_by_key(logs_sheet_id)

    ws_areas = _pick_worksheet(sh_config, tabs["areas"], AREAS_CANDIDATES)
    ws_itens = _pick_worksheet(sh_config, tabs["itens"], ITENS_CANDIDATES)

    # deadlines é opcional
    ws_dead = None
    try:
        ws_dead = _pick_worksheet(sh_config, tabs["deadlines"], DEADLINES_CANDIDATES)
    except Exception:
        ws_dead = None

    ws_users = _pick_worksheet(sh_rules, tabs["users"], USERS_CANDIDATES)
    ws_logs = _pick_worksheet(sh_logs, tabs["logs"], LOGS_CANDIDATES)

    df_areas = _df_from_ws(ws_areas)
    df_itens = _df_from_ws(ws_itens)
    df_dead = _df_from_ws(ws_dead) if ws_dead is not None else pd.DataFrame()
    df_users = _df_from_ws(ws_users)

    # Logs pode ser grande, então não carrega tudo aqui
    return {
        "sh_config": sh_config,
        "sh_rules": sh_rules,
        "sh_logs": sh_logs,
        "ws_logs": ws_logs,
        "areas": df_areas,
        "itens": df_itens,
        "deadlines": df_dead,
        "users": df_users,
        "tabs_titles": {
            "areas": ws_areas.title,
            "itens": ws_itens.title,
            "deadlines": ws_dead.title if ws_dead else "",
            "users": ws_users.title,
            "logs": ws_logs.title,
        },
    }

@st.cache_data(ttl=45)
def load_logs_for_date(_cache_buster: int, logs_sheet_id: str, logs_tab_title: str, target_date: date) -> pd.DataFrame:
    gc = _get_gspread_client()
    sh = gc.open_by_key(logs_sheet_id)
    ws = sh.worksheet(logs_tab_title)

    df = _df_from_ws(ws)
    if df.empty:
        return df

    # normalizar possíveis nomes de data
    # tenta achar coluna data
    date_cols = [c for c in df.columns if c in ["data", "date", "dia"]]
    ts_cols = [c for c in df.columns if c in ["timestamp", "ts", "datetime"]]

    if date_cols:
        col = date_cols[0]
        # espera yyyy-mm-dd
        df[col] = df[col].astype(str).str.strip()
        df = df[df[col] == target_date.isoformat()].copy()
        df.rename(columns={col: "data"}, inplace=True)
    elif ts_cols:
        col = ts_cols[0]
        # tenta parse
        parsed = pd.to_datetime(df[col], errors="coerce")
        df["data"] = parsed.dt.date.astype(str)
        df = df[df["data"] == target_date.isoformat()].copy()
    else:
        # se não tiver data, não filtra (mas vai errar dashboard)
        df["data"] = ""

    # padronizar colunas principais
    for need in ["hora", "user_login", "user_nome", "area_id", "turno", "item_id", "texto", "status", "obs"]:
        if need not in df.columns:
            df[need] = ""

    # normalizar status para OK/NAO_OK/PENDENTE
    df["status"] = df["status"].astype(str).str.strip().str.upper()
    df["status"] = df["status"].replace({"NÃO_OK": "NAO_OK", "NAO OK": "NAO_OK"})
    df["hora"] = df["hora"].astype(str).str.strip()

    return df

def compute_item_status(itens_df: pd.DataFrame, logs_df: pd.DataFrame, target_date: date, area_id: str, turno: str):
    """
    Retorna df_itens_filtrado com colunas:
    - status_atual (OK/NAO_OK/PENDENTE)
    - obs_atual
    - deadline_hhmm (string)
    - atrasado (bool)
    """
    df = itens_df.copy()

    # colunas esperadas no ITENS
    # mínimos: area_id, turno, item_id, item_texto, ordem
    rename_map = {}
    for c in df.columns:
        if c == "area":
            rename_map[c] = "area_id"
        if c == "turno_nome":
            rename_map[c] = "turno"
        if c == "texto":
            rename_map[c] = "item_texto"
    if rename_map:
        df.rename(columns=rename_map, inplace=True)

    if "area_id" not in df.columns or "turno" not in df.columns or "item_id" not in df.columns:
        raise RuntimeError("A aba ITENS precisa ter colunas: area_id, turno, item_id (além de item_texto).")

    if "item_texto" not in df.columns:
        # tenta achar
        cand = [c for c in df.columns if c in ["texto_item", "item", "descricao", "descrição", "item_texto"]]
        if cand:
            df.rename(columns={cand[0]: "item_texto"}, inplace=True)
        else:
            df["item_texto"] = df["item_id"]

    if "ordem" not in df.columns:
        df["ordem"] = "9999"

    # tipo_resposta e min são opcionais
    if "tipo_resposta" not in df.columns:
        df["tipo_resposta"] = "OK, NAO OK"
    if "min" not in df.columns:
        df["min"] = ""

    # deadline pode vir em ITENS
    if "deadline_hhmm" not in df.columns:
        df["deadline_hhmm"] = ""

    # filtrar por area e turno
    df = df[(df["area_id"].astype(str).str.strip() == area_id) & (df["turno"].astype(str).str.strip() == turno)].copy()

    # ordenar
    df["ordem_num"] = pd.to_numeric(df["ordem"], errors="coerce").fillna(9999).astype(int)
    df.sort_values(["ordem_num", "item_id"], inplace=True)

    # status atual pela última ocorrência no LOGS (para o dia/area/turno/item)
    df["status_atual"] = STATUS_PENDENTE
    df["obs_atual"] = ""

    if not logs_df.empty:
        subset = logs_df[
            (logs_df["area_id"].astype(str).str.strip() == area_id)
            & (logs_df["turno"].astype(str).str.strip() == turno)
        ].copy()

        # garantir ordem cronológica
        # hora como HH:MM:SS ou HH:MM
        def _to_time(x):
            x = str(x).strip()
            try:
                return datetime.strptime(x, "%H:%M:%S").time()
            except Exception:
                try:
                    return datetime.strptime(x, "%H:%M").time()
                except Exception:
                    return dtime(0, 0)

        subset["_t"] = subset["hora"].apply(_to_time)
        subset.sort_values(["_t"], inplace=True)

        last_by_item = subset.groupby("item_id", as_index=False).tail(1)
        last_map_status = dict(zip(last_by_item["item_id"], last_by_item["status"]))
        last_map_obs = dict(zip(last_by_item["item_id"], last_by_item["obs"]))

        df["status_atual"] = df["item_id"].map(lambda k: last_map_status.get(k, STATUS_PENDENTE))
        df["obs_atual"] = df["item_id"].map(lambda k: _safe_str(last_map_obs.get(k, "")))

    # atrasado: pendente e passou do deadline
    now_dt = _now()
    now_date = now_dt.date()
    now_time = now_dt.time()

    def _is_overdue(row):
        if row["status_atual"] != STATUS_PENDENTE:
            return False
        hhmm = _safe_str(row.get("deadline_hhmm", ""))
        t = _parse_hhmm(hhmm)
        if t is None:
            return False
        if target_date < now_date:
            return True
        if target_date > now_date:
            return False
        return now_time >= t

    df["atrasado"] = df.apply(_is_overdue, axis=1)
    return df

def apply_deadlines_from_tab(itens_df: pd.DataFrame, dead_df: pd.DataFrame) -> pd.DataFrame:
    """
    Se existir uma aba DEADLINES com colunas (area_id, turno, item_id, deadline_hhmm),
    mescla no ITENS quando ITENS estiver vazio.
    """
    if dead_df is None or dead_df.empty:
        return itens_df
    df = itens_df.copy()

    if "deadline_hhmm" in df.columns and df["deadline_hhmm"].astype(str).str.strip().ne("").any():
        return df  # ITENS já tem deadlines preenchidos

    needed = {"area_id", "turno", "item_id"}
    if not needed.issubset(set(dead_df.columns)):
        return df

    if "deadline_hhmm" not in dead_df.columns:
        # tenta achar
        cand = [c for c in dead_df.columns if "deadline" in c or "hora" in c]
        if not cand:
            return df
        dead_df = dead_df.rename(columns={cand[0]: "deadline_hhmm"})

    key_cols = ["area_id", "turno", "item_id"]
    dead2 = dead_df[key_cols + ["deadline_hhmm"]].copy()
    dead2["deadline_hhmm"] = dead2["deadline_hhmm"].astype(str).str.strip()

    df = df.merge(dead2, on=key_cols, how="left", suffixes=("", "_dl"))
    if "deadline_hhmm_dl" in df.columns:
        df["deadline_hhmm"] = df["deadline_hhmm"].astype(str).str.strip()
        df["deadline_hhmm"] = df["deadline_hhmm"].mask(df["deadline_hhmm"].eq(""), df["deadline_hhmm_dl"].fillna(""))
        df.drop(columns=["deadline_hhmm_dl"], inplace=True, errors="ignore")
    return df

def ensure_columns(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = ""
    return out

def append_log_row(ws_logs, row_dict: dict):
    # respeita cabeçalho existente na planilha
    header = ws_logs.row_values(1)
    header_norm = [_norm_col(h) for h in header]

    # cria linha alinhada ao header
    line = []
    for hn in header_norm:
        line.append(row_dict.get(hn, ""))

    ws_logs.append_row(line, value_input_option="USER_ENTERED")

# =========================
# Auth
# =========================
def authenticate(df_users: pd.DataFrame, login: str, senha: str):
    df = df_users.copy()
    if df.empty:
        return None

    # normaliza colunas
    df.columns = [_norm_col(c) for c in df.columns]
    if "login" not in df.columns or "senha" not in df.columns:
        return None

    df["login"] = df["login"].astype(str).str.strip()
    df["senha"] = df["senha"].astype(str).str.strip()
    df["ativo"] = df.get("ativo", "TRUE").astype(str).str.strip().str.upper()

    user = df[(df["login"] == login.strip()) & (df["senha"] == senha.strip()) & (df["ativo"].isin(["TRUE", "1", "SIM", "YES"]))]

    if user.empty:
        return None

    r = user.iloc[0].to_dict()
    return {
        "login": _safe_str(r.get("login")),
        "nome": _safe_str(r.get("nome", r.get("login"))),
        "perfil": _safe_str(r.get("perfil", "")),
        "user_id": _safe_str(r.get("user_id", "")),
    }

# =========================
# Dashboard computations
# =========================
def build_dashboard(areas_df, itens_df, logs_df, dead_df, target_date: date):
    # itens + deadlines
    itens2 = apply_deadlines_from_tab(itens_df, dead_df)

    # listas de areas e turnos disponíveis
    areas_df = ensure_columns(areas_df, ["area_id", "area_nome", "ativo", "ordem"])
    areas_df["ativo"] = areas_df["ativo"].astype(str).str.upper()
    areas_active = areas_df[areas_df["ativo"].isin(["TRUE", "1", "SIM", "YES"])].copy()
    if areas_active.empty:
        areas_active = areas_df.copy()

    # turnos vêm de ITENS
    itens2 = ensure_columns(itens2, ["area_id", "turno", "item_id", "item_texto", "ordem", "deadline_hhmm", "tipo_resposta", "min"])
    itens2["area_id"] = itens2["area_id"].astype(str).str.strip()
    itens2["turno"] = itens2["turno"].astype(str).str.strip()

    rows = []
    totals = {"OK": 0, "NAO_OK": 0, "PENDENTE": 0, "ATRASADO": 0, "TOTAL": 0}

    for area_id in sorted(itens2["area_id"].dropna().unique()):
        for turno in sorted(itens2[itens2["area_id"] == area_id]["turno"].dropna().unique()):
            df_status = compute_item_status(itens2, logs_df, target_date, area_id, turno)

            # conta atrasados
            ok = int((df_status["status_atual"] == STATUS_OK).sum())
            no = int((df_status["status_atual"] == STATUS_NAO_OK).sum())
            pend = int((df_status["status_atual"] == STATUS_PENDENTE).sum())
            atr = int((df_status["atrasado"] == True).sum())

            total = len(df_status)

            totals["OK"] += ok
            totals["NAO_OK"] += no
            totals["PENDENTE"] += pend
            totals["ATRASADO"] += atr
            totals["TOTAL"] += total

            rows.append(
                {
                    "area_id": area_id,
                    "turno": turno,
                    "total": total,
                    "ok": ok,
                    "nao_ok": no,
                    "pendente": pend,
                    "atrasado": atr,
                }
            )

    dash = pd.DataFrame(rows)
    if not dash.empty:
        dash.sort_values(["area_id", "turno"], inplace=True)
    return dash, totals

# =========================
# Pages
# =========================
def page_login(tables):
    st.title("Login")

    c1, c2 = st.columns([2, 1])
    with c1:
        login = st.text_input("Usuário", value=st.session_state.get("login_input", ""))
        senha = st.text_input("Senha", type="password", value=st.session_state.get("senha_input", ""))

        st.session_state["login_input"] = login
        st.session_state["senha_input"] = senha

        if st.button("Entrar", key="btn_login"):
            user = authenticate(tables["users"], login, senha)
            if not user:
                st.error("Usuário/senha inválidos ou usuário inativo.")
                return
            st.session_state["user"] = user
            st.success("Login OK.")
            st.rerun()

    with c2:
        st.info("Acesso protegido por usuário e senha.")

def page_dashboard(tables, target_date: date):
    st.header("Dashboard operacional")

    # filtra data
    st.caption("Mostrando status do dia selecionado. Ao abrir, o padrão é hoje.")

    logs_df = load_logs_for_date(st.session_state["cache_buster"], st.session_state["logs_sheet_id"], tables["tabs_titles"]["logs"], target_date)
    dash_df, totals = build_dashboard(tables["areas"], tables["itens"], logs_df, tables["deadlines"], target_date)

    # cards de totais
    t1, t2, t3, t4, t5 = st.columns(5)
    t1.metric("Total", totals["TOTAL"])
    t2.metric("OK", totals["OK"])
    t3.metric("Não OK", totals["NAO_OK"])
    t4.metric("Pendente", totals["PENDENTE"])
    t5.metric("Atrasado", totals["ATRASADO"])

    st.divider()

    if dash_df.empty:
        st.warning("Sem dados para montar o dashboard (verifique ITENS e LOGS).")
        return

    st.dataframe(dash_df, use_container_width=True, hide_index=True)

def page_checklist(tables, target_date: date):
    st.header("Checklist")

    itens_df = apply_deadlines_from_tab(tables["itens"], tables["deadlines"])

    # listas para seleção
    itens_df = ensure_columns(itens_df, ["area_id", "turno", "item_id", "item_texto", "ordem", "deadline_hhmm", "tipo_resposta", "min"])
    itens_df["area_id"] = itens_df["area_id"].astype(str).str.strip()
    itens_df["turno"] = itens_df["turno"].astype(str).str.strip()

    areas = sorted([a for a in itens_df["area_id"].dropna().unique() if a])
    turnos = sorted([t for t in itens_df["turno"].dropna().unique() if t])

    if not areas or not turnos:
        st.error("A aba ITENS precisa ter registros com area_id e turno.")
        return

    c1, c2 = st.columns([1, 1])
    with c1:
        area_sel = st.selectbox("Área", areas, index=0, key="area_sel")
    with c2:
        turno_sel = st.selectbox("Turno", turnos, index=0, key="turno_sel")

    st.caption("Tudo começa PENDENTE. Clique OK, Não OK ou Desmarcar. Para itens NUMERO/TEXTO, preencha o campo e clique OK/Não OK.")

    # carregar logs do dia
    logs_df = load_logs_for_date(st.session_state["cache_buster"], st.session_state["logs_sheet_id"], tables["tabs_titles"]["logs"], target_date)

    # status por item
    df_status = compute_item_status(itens_df, logs_df, target_date, area_sel, turno_sel)

    if df_status.empty:
        st.warning("Nenhum item encontrado para esta Área e Turno.")
        return

    # render itens
    ws_logs = tables["ws_logs"]
    user = st.session_state.get("user", {})
    user_login = user.get("login", "")
    user_nome = user.get("nome", "")

    for _, row in df_status.iterrows():
        item_id = _safe_str(row["item_id"])
        texto = _safe_str(row["item_texto"])
        status_atual = _safe_str(row["status_atual"]).upper() or STATUS_PENDENTE
        obs_atual = _safe_str(row.get("obs_atual", ""))
        deadline_hhmm = _safe_str(row.get("deadline_hhmm", ""))
        tipo_resp = _safe_str(row.get("tipo_resposta", "OK, NAO OK")).upper()
        minimo = _safe_str(row.get("min", ""))

        atrasado = bool(row.get("atrasado", False))

        # status visual
        show_status = status_atual
        if atrasado and status_atual == STATUS_PENDENTE:
            show_status = STATUS_ATRASADO

        if show_status == STATUS_OK:
            badge_cls = "badge-ok"
            bg = "rgba(0, 200, 120, 0.14)"
            label = "OK"
        elif show_status == STATUS_NAO_OK:
            badge_cls = "badge-no"
            bg = "rgba(255, 80, 80, 0.14)"
            label = "Não OK"
        elif show_status == STATUS_ATRASADO:
            badge_cls = "badge-atr"
            bg = "rgba(255, 180, 0, 0.18)"
            label = "Atrasado"
        else:
            badge_cls = "badge-pend"
            bg = "rgba(120, 120, 120, 0.10)"
            label = "Pendente"

        deadline_txt = f"Horário: {deadline_hhmm}" if deadline_hhmm else "Horário: (sem deadline)"
        st.markdown(
            f"""
<div class="card" style="background:{bg}">
  <div class="card-title">{texto}</div>
  <div class="card-sub">
    <span class="badge {badge_cls}">{label}</span>
    <span class="muted">{deadline_txt}</span>
  </div>
</div>
""",
            unsafe_allow_html=True,
        )

        # campo adicional para NUMERO/TEXTO
        obs_value = ""
        needs_input = ("NUMERO" in tipo_resp) or ("NÚMERO" in tipo_resp) or ("TEXTO" in tipo_resp)
        if needs_input:
            k = f"obs_{item_id}"
            help_txt = "Digite um valor e clique OK ou Não OK para registrar no log (coluna OBS)."
            if ("NUMERO" in tipo_resp) or ("NÚMERO" in tipo_resp):
                obs_value = st.text_input(f"Valor (número) para {item_id}", value=st.session_state.get(k, ""), key=k, help=help_txt)
                if minimo.strip() != "":
                    st.caption(f"Min (referência): {minimo}")
            else:
                obs_value = st.text_input(f"Observação (texto) para {item_id}", value=st.session_state.get(k, ""), key=k, help=help_txt)

        b1, b2, b3 = st.columns([1, 1, 1])

        def _make_log_row(new_status: str, obs: str):
            now_dt = _now()
            row_dict = {
                "timestamp": now_dt.isoformat(),
                "data": target_date.isoformat(),
                "hora": now_dt.strftime("%H:%M:%S"),
                "dia_semana": _weekday_pt(target_date),
                "user_login": user_login,
                "user_nome": user_nome,
                "area_id": area_sel,
                "turno": turno_sel,
                "item_id": item_id,
                "texto": texto,
                "status": new_status,
                "obs": obs,
            }
            return row_dict

        with b1:
            if st.button("OK", key=f"ok_{item_id}"):
                # valida NUMERO se aplicável (sem travar, só alerta)
                obs_to_save = obs_value if needs_input else ""
                if needs_input and (("NUMERO" in tipo_resp) or ("NÚMERO" in tipo_resp)):
                    try:
                        float(obs_to_save.replace(",", "."))
                    except Exception:
                        st.warning("Valor numérico inválido. Mesmo assim o OK será registrado sem OBS.")
                        obs_to_save = ""
                append_log_row(ws_logs, _make_log_row(STATUS_OK, obs_to_save))
                st.session_state["cache_buster"] += 1
                st.cache_data.clear()
                st.rerun()

        with b2:
            if st.button("Não OK", key=f"no_{item_id}"):
                obs_to_save = obs_value if needs_input else ""
                if needs_input and (("NUMERO" in tipo_resp) or ("NÚMERO" in tipo_resp)):
                    try:
                        float(obs_to_save.replace(",", "."))
                    except Exception:
                        st.warning("Valor numérico inválido. Mesmo assim o Não OK será registrado sem OBS.")
                        obs_to_save = ""
                append_log_row(ws_logs, _make_log_row(STATUS_NAO_OK, obs_to_save))
                st.session_state["cache_buster"] += 1
                st.cache_data.clear()
                st.rerun()

        with b3:
            if st.button("Desmarcar", key=f"un_{item_id}"):
                append_log_row(ws_logs, _make_log_row(STATUS_PENDENTE, ""))  # desmarca
                st.session_state["cache_buster"] += 1
                st.cache_data.clear()
                st.rerun()

        # mostrar obs atual se existir
        if obs_atual:
            st.caption(f"OBS atual: {obs_atual}")

        st.write("")  # espaçamento

def page_events_placeholder():
    st.header("EVENTS")
    st.info("Opcional. Se quiser, depois a gente liga esta aba com relatórios de auditoria e export.")

# =========================
# Main
# =========================
def main():
    if "cache_buster" not in st.session_state:
        st.session_state["cache_buster"] = 1

    config_id, rules_id, logs_id = _get_sheet_ids()
    st.session_state["logs_sheet_id"] = logs_id
    tabs = _get_tab_names()

    with st.sidebar:
        st.title("Checklist Operacional")
        if st.session_state.get("user"):
            if st.button("Logout"):
                st.session_state.pop("user", None)
                st.rerun()

        st.subheader("Status")
        st.success("Google Sheets conectado")

        st.subheader("Navegação")
        page = st.radio(" ", ["Dashboard", "Checklist", "EVENTS"], index=0)

        st.subheader("Data")
        target_date = st.date_input("Mostrar dia", value=_today(), key="dash_date")

    # carrega config e rules
    try:
        tables = load_tables(
            st.session_state["cache_buster"],
            config_id,
            rules_id,
            logs_id,
            tabs,
        )
    except Exception as e:
        st.error(f"Falha ao carregar planilhas. Detalhe: {e}")
        st.stop()

    # login obrigatório
    if not st.session_state.get("user"):
        page_login(tables)
        return

    if page == "Dashboard":
        page_dashboard(tables, target_date)
    elif page == "Checklist":
        page_checklist(tables, target_date)
    else:
        page_events_placeholder()

if __name__ == "__main__":
    main()
