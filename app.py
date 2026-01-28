# app.py
# Checklist Operacional (Streamlit + Google Sheets)
# Funciona com secrets antigos (CONFIG_SHEET_ID/RULES_SHEET_ID/LOGS_SHEET_ID) e também com [app] (opcional).
# Usa service account em [gcp_service_account] (como você já tem).
#
# Estrutura esperada (flexível, por nomes de colunas):
# 1) Checklist_Config (CONFIG_SHEET_ID)
#    - Aba AREAS: area_id, area_nome, ativo, ordem
#    - Aba ITENS: (colunas mínimas) item_id, item_texto (ou texto), area_id, turno, ordem
#      + opcionais: critico, tipo_resposta (OK, Nao OK | NUMERO | TEXTO), min, deadline_hhmm
# 2) App_Regras (RULES_SHEET_ID)
#    - Aba USUARIOS: user_id, nome, login, senha, ativo, perfil
# 3) Checklist_Logs (LOGS_SHEET_ID)
#    - Aba LOGS: data, hora, dia_semana, user_login, user_nome, area_id, turno, item_id, texto, status, obs
#
# Status válidos: PENDENTE, OK, NAO_OK
# Atraso é calculado (não gravado): se status PENDENTE e deadline_hhmm já passou para a data selecionada.

import re
import time
from dataclasses import dataclass
from datetime import datetime, date
import pandas as pd
import streamlit as st

import gspread
from google.oauth2.service_account import Credentials


# =========================
# Config e helpers
# =========================

APP_TITLE = "Checklist Operacional"

WS_AREAS_CANDIDATES = ["AREAS", "ÁREAS", "AREAS ", "areas"]
WS_ITENS_CANDIDATES = ["ITENS", "ITEMS", "itens"]
WS_USUARIOS_CANDIDATES = ["USUARIOS", "USUÁRIOS", "USERS", "LOGIN", "usuarios"]
WS_LOGS_CANDIDATES = ["LOGS", "LOG", "REGISTROS", "records"]

STATUS_OK = "OK"
STATUS_NAO_OK = "NAO_OK"
STATUS_PENDENTE = "PENDENTE"

TZ_NOTE = "America/Sao_Paulo"


def _norm(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    s = s.replace("\u00a0", " ")
    return s


def _norm_key(s: str) -> str:
    s = _norm(s).lower()
    s = re.sub(r"\s+", "_", s)
    s = s.replace("ç", "c").replace("ã", "a").replace("á", "a").replace("à", "a").replace("â", "a")
    s = s.replace("é", "e").replace("ê", "e")
    s = s.replace("í", "i")
    s = s.replace("ó", "o").replace("ô", "o").replace("õ", "o")
    s = s.replace("ú", "u")
    return s


def _col(df: pd.DataFrame, *cands: str) -> str | None:
    # retorna o nome real da coluna que bate com algum candidato (case/acentos/underscore)
    if df is None or df.empty:
        return None
    mapping = { _norm_key(c): c for c in df.columns }
    for c in cands:
        k = _norm_key(c)
        if k in mapping:
            return mapping[k]
    return None


def _parse_bool(x) -> bool:
    if isinstance(x, bool):
        return x
    s = _norm(x).lower()
    if s in ["true", "1", "sim", "yes", "y"]:
        return True
    if s in ["false", "0", "nao", "não", "no", "n", ""]:
        return False
    return False


def parse_hhmm(hhmm: str):
    hhmm = _norm(hhmm)
    if not hhmm:
        return None
    m = re.match(r"^(\d{1,2}):(\d{2})$", hhmm)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    if hh < 0 or hh > 23 or mm < 0 or mm > 59:
        return None
    return hh, mm


def now_dt() -> datetime:
    # Streamlit Cloud geralmente roda em UTC. Para reduzir surpresa, usamos hora local via offset simples.
    # Se você quiser precisão absoluta por timezone, dá para usar zoneinfo.
    # Aqui: usa datetime.now() do ambiente e mantém coerente com o que aparece nos logs atuais.
    return datetime.now()


def weekday_pt_br(dt: date) -> str:
    dias = ["Segunda", "Terca", "Quarta", "Quinta", "Sexta", "Sabado", "Domingo"]
    return dias[dt.weekday()]


def is_overdue(selected_date: date, deadline_hhmm: str, status_atual: str) -> bool:
    if status_atual != STATUS_PENDENTE:
        return False
    parsed = parse_hhmm(deadline_hhmm)
    if not parsed:
        return False

    hh, mm = parsed
    n = now_dt()

    # Se a data selecionada for hoje, compara hora atual.
    # Se a data selecionada for no passado, qualquer pendente com deadline é atrasado.
    # Se a data selecionada for no futuro, não marca atraso.
    if selected_date < n.date():
        return True
    if selected_date > n.date():
        return False

    return (n.hour, n.minute) >= (hh, mm)


# =========================
# Secrets compatível
# =========================

def _get_sheet_ids():
    """
    Compatível com secrets antigos e novos.
    Não exige [app].
    """
    app_cfg = st.secrets.get("app", {})
    if app_cfg:
        config_id = str(app_cfg.get("config_sheet_id", "")).strip()
        rules_id = str(app_cfg.get("rules_sheet_id", "")).strip()
        logs_id = str(app_cfg.get("logs_sheet_id", "")).strip()
        if config_id and rules_id and logs_id:
            return config_id, rules_id, logs_id

    candidates = [
        ("CONFIG_SHEET_ID", "RULES_SHEET_ID", "LOGS_SHEET_ID"),
        ("config_sheet_id", "rules_sheet_id", "logs_sheet_id"),
        ("CHECKLIST_CONFIG_SHEET_ID", "APP_REGRAS_SHEET_ID", "CHECKLIST_LOGS_SHEET_ID"),
    ]
    for a, b, c in candidates:
        config_id = str(st.secrets.get(a, "")).strip()
        rules_id = str(st.secrets.get(b, "")).strip()
        logs_id = str(st.secrets.get(c, "")).strip()
        if config_id and rules_id and logs_id:
            return config_id, rules_id, logs_id

    st.error(
        "Não encontrei os IDs das planilhas no secrets. "
        "Procurei por [app].config_sheet_id/rules_sheet_id/logs_sheet_id e também por "
        "CONFIG_SHEET_ID/RULES_SHEET_ID/LOGS_SHEET_ID (e variações)."
    )
    st.stop()


def _get_sa_info():
    if "gcp_service_account" not in st.secrets:
        st.error("Falta [gcp_service_account] no secrets.toml.")
        st.stop()
    return dict(st.secrets["gcp_service_account"])


# =========================
# Google Sheets client
# =========================

@st.cache_resource
def get_gspread_client():
    info = _get_sa_info()
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def _open_sheet(spreadsheet_id: str):
    gc = get_gspread_client()
    return gc.open_by_key(spreadsheet_id)


def _pick_worksheet(sh, candidates: list[str]):
    titles = [w.title for w in sh.worksheets()]
    title_set = set(titles)
    for c in candidates:
        if c in title_set:
            return sh.worksheet(c)
    # tenta match case-insensitive
    low_map = {t.lower(): t for t in titles}
    for c in candidates:
        if c.lower() in low_map:
            return sh.worksheet(low_map[c.lower()])
    return None


@st.cache_data(ttl=25, show_spinner=False)
def read_worksheet_df(spreadsheet_id: str, worksheet_title: str, cache_buster: int) -> pd.DataFrame:
    sh = _open_sheet(spreadsheet_id)
    ws = sh.worksheet(worksheet_title)
    values = ws.get_all_values()
    if not values or len(values) < 1:
        return pd.DataFrame()
    header = values[0]
    rows = values[1:]
    df = pd.DataFrame(rows, columns=header)
    # normaliza colunas vazias
    df.columns = [ _norm(c) for c in df.columns ]
    return df


def append_log_row(logs_sheet_id: str, ws_title: str, row: list):
    sh = _open_sheet(logs_sheet_id)
    ws = sh.worksheet(ws_title)
    ws.append_row(row, value_input_option="USER_ENTERED")


# =========================
# Modelos e carregamento
# =========================

@dataclass
class Tables:
    areas: pd.DataFrame
    itens: pd.DataFrame
    usuarios: pd.DataFrame
    logs: pd.DataFrame
    ws_logs_title: str
    ws_areas_title: str
    ws_itens_title: str
    ws_usuarios_title: str


def load_tables(config_sheet_id: str, rules_sheet_id: str, logs_sheet_id: str, cache_buster: int) -> Tables:
    sh_cfg = _open_sheet(config_sheet_id)
    ws_areas = _pick_worksheet(sh_cfg, WS_AREAS_CANDIDATES)
    ws_itens = _pick_worksheet(sh_cfg, WS_ITENS_CANDIDATES)
    if not ws_areas or not ws_itens:
        st.error("Checklist_Config precisa ter as abas AREAS e ITENS (ou nomes compatíveis).")
        st.stop()

    sh_rules = _open_sheet(rules_sheet_id)
    ws_users = _pick_worksheet(sh_rules, WS_USUARIOS_CANDIDATES)
    if not ws_users:
        st.error("App_Regras precisa ter a aba USUARIOS (ou nome compatível).")
        st.stop()

    sh_logs = _open_sheet(logs_sheet_id)
    ws_logs = _pick_worksheet(sh_logs, WS_LOGS_CANDIDATES)
    if not ws_logs:
        st.error("Checklist_Logs precisa ter a aba LOGS (ou nome compatível).")
        st.stop()

    df_areas = read_worksheet_df(config_sheet_id, ws_areas.title, cache_buster)
    df_itens = read_worksheet_df(config_sheet_id, ws_itens.title, cache_buster)
    df_users = read_worksheet_df(rules_sheet_id, ws_users.title, cache_buster)
    df_logs = read_worksheet_df(logs_sheet_id, ws_logs.title, cache_buster)

    return Tables(
        areas=df_areas,
        itens=df_itens,
        usuarios=df_users,
        logs=df_logs,
        ws_logs_title=ws_logs.title,
        ws_areas_title=ws_areas.title,
        ws_itens_title=ws_itens.title,
        ws_usuarios_title=ws_users.title,
    )


# =========================
# Validações mínimas
# =========================

def validate_users(df: pd.DataFrame):
    c_login = _col(df, "login", "user", "usuario", "usuário")
    c_senha = _col(df, "senha", "password")
    c_ativo = _col(df, "ativo", "active")
    c_nome = _col(df, "nome", "name")
    if not c_login or not c_senha:
        st.error("A aba USUARIOS precisa ter colunas login e senha.")
        st.stop()
    return c_login, c_senha, c_ativo, c_nome


def validate_areas(df: pd.DataFrame):
    c_id = _col(df, "area_id", "id_area")
    c_nome = _col(df, "area_nome", "nome_area", "area")
    c_ativo = _col(df, "ativo", "active")
    c_ordem = _col(df, "ordem", "order")
    if not c_id or not c_nome:
        st.error("A aba AREAS precisa ter colunas area_id e area_nome.")
        st.stop()
    return c_id, c_nome, c_ativo, c_ordem


def validate_itens(df: pd.DataFrame):
    c_item_id = _col(df, "item_id", "id_item")
    c_texto = _col(df, "item_texto", "texto", "descricao", "descrição")
    c_area = _col(df, "area_id", "id_area")
    c_turno = _col(df, "turno", "shift")
    c_ordem = _col(df, "ordem", "order")
    if not c_item_id or not c_texto or not c_area or not c_turno:
        st.error("A aba ITENS precisa ter: item_id, item_texto (ou texto), area_id e turno.")
        st.stop()

    c_tipo = _col(df, "tipo_resposta", "tipo", "resposta")
    c_deadline = _col(df, "deadline_hhmm", "deadline", "prazo", "hora_deadline")
    c_critico = _col(df, "critico", "crítico", "critical")
    c_min = _col(df, "min", "minimo", "mínimo")
    return c_item_id, c_texto, c_area, c_turno, c_ordem, c_tipo, c_deadline, c_critico, c_min


# =========================
# Auth
# =========================

def authenticate(tables: Tables):
    df = tables.usuarios.copy()
    c_login, c_senha, c_ativo, c_nome = validate_users(df)

    st.subheader("Login")
    st.caption("Acesso protegido por usuário e senha.")

    user = st.text_input("Usuário", key="login_user")
    pwd = st.text_input("Senha", type="password", key="login_pwd")
    ok = st.button("Entrar", type="primary")

    if not ok:
        return None

    df[c_login] = df[c_login].astype(str).str.strip()
    df[c_senha] = df[c_senha].astype(str).str.strip()

    row = df[df[c_login].str.lower() == _norm(user).lower()]
    if row.empty:
        st.error("Usuário não encontrado.")
        return None

    row = row.iloc[0]
    if c_ativo and not _parse_bool(row[c_ativo]):
        st.error("Usuário inativo.")
        return None

    if _norm(pwd) != _norm(row[c_senha]):
        st.error("Senha incorreta.")
        return None

    return {
        "login": _norm(row[c_login]),
        "nome": _norm(row[c_nome]) if c_nome else _norm(row[c_login]),
    }


# =========================
# Status por item no dia selecionado
# =========================

def build_status_map(itens_df: pd.DataFrame, logs_df: pd.DataFrame, selected_date: date):
    # colunas logs
    if logs_df is None or logs_df.empty:
        return {}

    c_data = _col(logs_df, "data", "date")
    c_item = _col(logs_df, "item_id", "id_item")
    c_status = _col(logs_df, "status")
    c_hora = _col(logs_df, "hora", "time")
    if not c_data or not c_item or not c_status:
        return {}

    # filtra dia
    day_str = selected_date.isoformat()
    d = logs_df.copy()
    d[c_data] = d[c_data].astype(str).str.strip()
    d = d[d[c_data] == day_str]
    if d.empty:
        return {}

    # ordena por hora, se existir
    if c_hora and c_hora in d.columns:
        d["_hora_sort"] = d[c_hora].astype(str)
        d = d.sort_values("_hora_sort")
    else:
        d = d.reset_index()

    status_map = {}
    for _, r in d.iterrows():
        item_id = _norm(r[c_item])
        status = _norm(r[c_status]).upper()
        if status in ["NÃO_OK", "NAO OK", "NAO_OK"]:
            status = STATUS_NAO_OK
        if status in ["OK"]:
            status = STATUS_OK
        if status in ["PENDENTE", ""]:
            status = STATUS_PENDENTE
        status_map[item_id] = status

    return status_map


# =========================
# UI: estilos
# =========================

def inject_css():
    st.markdown("""
    <style>
    /* botões neutros (OK e Não OK iguais visualmente) */
    div.stButton > button {
      background: white !important;
      color: #111 !important;
      border: 1px solid #d0d0d0 !important;
      border-radius: 10px !important;
      font-weight: 700 !important;
      padding: 0.5rem 1.1rem !important;
    }
    div.stButton > button:hover { border-color: #9a9a9a !important; }
    div.stButton > button:disabled {
      opacity: 0.55 !important;
      border-color: #333 !important;
      box-shadow: inset 0 0 0 2px #333 !important;
    }

    /* cards */
    .card {
      border-radius: 14px;
      padding: 14px 16px;
      margin: 10px 0 10px 0;
      border: 1px solid rgba(0,0,0,0.06);
    }
    .card h4 { margin: 0 0 6px 0; font-size: 16px; }
    .meta { opacity: 0.85; font-size: 13px; }
    </style>
    """, unsafe_allow_html=True)


def card_html(title: str, status_label: str, deadline: str, bg: str):
    dl = _norm(deadline)
    dl_txt = f" | Deadline: {dl}" if dl else ""
    return f"""
    <div class="card" style="background:{bg};">
      <h4>{title}</h4>
      <div class="meta">Status: <b>{status_label}</b>{dl_txt}</div>
    </div>
    """


# =========================
# Gravação de log
# =========================

def write_log(tables: Tables, selected_date: date, user: dict, area_id: str, turno: str, item_id: str, texto: str, status: str, obs: str):
    # prepara colunas conforme padrão atual
    day_str = selected_date.isoformat()
    hr = now_dt().strftime("%H:%M:%S")
    dia_semana = weekday_pt_br(selected_date)

    row = [
        day_str,               # data
        hr,                    # hora
        dia_semana,            # dia_semana
        user.get("login",""),  # user_login
        user.get("nome",""),   # user_nome
        area_id,               # area_id
        turno,                 # turno
        item_id,               # item_id
        texto,                 # texto
        status,                # status
        obs or "",             # obs
    ]
    append_log_row(tables.logs_sheet_id, tables.ws_logs_title, row)  # type: ignore


# =========================
# Páginas
# =========================

def page_dashboard(tables: Tables, selected_date: date):
    st.header("Dashboard operacional")

    areas = tables.areas.copy()
    itens = tables.itens.copy()
    logs = tables.logs.copy()

    c_area_id, c_area_nome, c_area_ativo, c_area_ordem = validate_areas(areas)
    c_item_id, c_texto, c_area, c_turno, c_ordem, c_tipo, c_deadline, c_critico, c_min = validate_itens(itens)

    # filtra áreas ativas
    if c_area_ativo:
        areas["_ativo"] = areas[c_area_ativo].apply(_parse_bool)
        areas = areas[areas["_ativo"] == True].copy()

    # status map por item no dia
    status_map = build_status_map(itens, logs, selected_date)

    # monta base itens
    base = itens.copy()
    base["item_id"] = base[c_item_id].astype(str).map(_norm)
    base["texto"] = base[c_texto].astype(str).map(_norm)
    base["area_id"] = base[c_area].astype(str).map(_norm)
    base["turno"] = base[c_turno].astype(str).map(_norm)

    if c_deadline:
        base["deadline_hhmm"] = base[c_deadline].astype(str).map(_norm)
    else:
        base["deadline_hhmm"] = ""

    if c_ordem:
        base["_ordem"] = pd.to_numeric(base[c_ordem], errors="coerce").fillna(9999).astype(int)
    else:
        base["_ordem"] = 9999

    base["status"] = base["item_id"].map(lambda x: status_map.get(x, STATUS_PENDENTE))

    base["atrasado"] = base.apply(lambda r: is_overdue(selected_date, r["deadline_hhmm"], r["status"]), axis=1)

    # agregações
    total = len(base)
    n_ok = int((base["status"] == STATUS_OK).sum())
    n_no = int((base["status"] == STATUS_NAO_OK).sum())
    n_pendente = int((base["status"] == STATUS_PENDENTE).sum())
    n_atrasado = int((base["atrasado"] == True).sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("OK", n_ok)
    c2.metric("Não OK", n_no)
    c3.metric("Pendentes", n_pendente)
    c4.metric("Atrasados", n_atrasado)

    st.divider()

    # detalhe por área e turno
    st.subheader("Resumo por área e turno")
    grp = base.groupby(["area_id", "turno"], as_index=False).agg(
        total=("item_id", "count"),
        ok=("status", lambda s: int((s == STATUS_OK).sum())),
        nao_ok=("status", lambda s: int((s == STATUS_NAO_OK).sum())),
        pendente=("status", lambda s: int((s == STATUS_PENDENTE).sum())),
        atrasado=("atrasado", lambda s: int((s == True).sum())),
    )
    if grp.empty:
        st.info("Sem itens configurados.")
        return

    st.dataframe(grp, use_container_width=True)


def page_checklist(tables: Tables, user: dict, selected_date: date):
    st.header("Checklist")

    areas = tables.areas.copy()
    itens = tables.itens.copy()
    logs = tables.logs.copy()

    c_area_id, c_area_nome, c_area_ativo, c_area_ordem = validate_areas(areas)
    c_item_id, c_texto, c_area, c_turno, c_ordem, c_tipo, c_deadline, c_critico, c_min = validate_itens(itens)

    # areas ativas e ordenadas
    if c_area_ativo:
        areas["_ativo"] = areas[c_area_ativo].apply(_parse_bool)
        areas = areas[areas["_ativo"] == True].copy()

    if c_area_ordem:
        areas["_ord"] = pd.to_numeric(areas[c_area_ordem], errors="coerce").fillna(9999).astype(int)
        areas = areas.sort_values("_ord")
    else:
        areas["_ord"] = 9999

    # turnos disponíveis a partir dos itens
    itens["turno_norm"] = itens[c_turno].astype(str).map(_norm)
    turnos = sorted([t for t in itens["turno_norm"].unique().tolist() if t])

    # ui seleção
    st.caption("Tudo começa PENDENTE. Clique OK, Não OK ou Desmarcar. Para itens NUMERO/TEXTO, preencha o campo antes de registrar.")
    colA, colB = st.columns(2)
    with colA:
        area_opts = [(r[c_area_id], r[c_area_nome]) for _, r in areas.iterrows()]
        area_labels = [f"{aid} - {an}" for aid, an in area_opts]
        area_sel = st.selectbox("Área", options=list(range(len(area_opts))), format_func=lambda i: area_labels[i])
        area_id = area_opts[area_sel][0]
    with colB:
        turno = st.selectbox("Turno", options=turnos if turnos else ["(sem turnos)"])

    if st.button("Atualizar lista"):
        st.session_state["cache_buster"] += 1
        st.rerun()

    # filtra itens por area e turno
    base = itens.copy()
    base["_area_id"] = base[c_area].astype(str).map(_norm)
    base["_turno"] = base[c_turno].astype(str).map(_norm)

    base = base[(base["_area_id"] == area_id) & (base["_turno"] == turno)].copy()
    if base.empty:
        st.info("Sem itens para esta área e turno.")
        return

    # ordenação
    if c_ordem:
        base["_ordem"] = pd.to_numeric(base[c_ordem], errors="coerce").fillna(9999).astype(int)
    else:
        base["_ordem"] = 9999
    base = base.sort_values("_ordem")

    # status do dia
    status_map = build_status_map(itens, logs, selected_date)

    # render itens
    for _, r in base.iterrows():
        item_id = _norm(r[c_item_id])
        texto = _norm(r[c_texto])
        tipo = _norm(r[c_tipo]).upper() if c_tipo else "OK, NAO OK"
        deadline = _norm(r[c_deadline]) if c_deadline else ""

        status_atual = status_map.get(item_id, STATUS_PENDENTE)

        # atraso
        overdue = is_overdue(selected_date, deadline, status_atual)

        # cores do card
        if overdue:
            bg = "rgba(255, 214, 102, 0.30)"  # amarelo suave
            status_label = "ATRASADO"
        elif status_atual == STATUS_OK:
            bg = "rgba(46, 204, 113, 0.18)"   # verde suave
            status_label = "OK"
        elif status_atual == STATUS_NAO_OK:
            bg = "rgba(231, 76, 60, 0.16)"    # vermelho suave
            status_label = "Não OK"
        else:
            bg = "rgba(0, 0, 0, 0.04)"        # cinza suave
            status_label = "PENDENTE"

        st.markdown(card_html(texto, status_label, deadline, bg), unsafe_allow_html=True)

        # campo de obs para NUMERO/TEXTO
        obs_val = ""
        needs_obs = (tipo in ["NUMERO", "TEXTO"])

        if needs_obs:
            if tipo == "NUMERO":
                obs_val = st.text_input("Valor (número)", key=f"obs_{item_id}", placeholder="Ex: 3,5 ou 10")
            else:
                obs_val = st.text_input("Observação", key=f"obs_{item_id}", placeholder="Digite o texto")

        # botões com realce via disabled
        b1, b2, b3 = st.columns([1, 1, 1])

        is_ok = (status_atual == STATUS_OK)
        is_no = (status_atual == STATUS_NAO_OK)
        is_pendente = (status_atual == STATUS_PENDENTE)

        with b1:
            if st.button("OK", key=f"ok_{item_id}", disabled=is_ok):
                if needs_obs and not _norm(obs_val):
                    st.warning("Preencha o valor antes de registrar.")
                else:
                    _append_log(tables, selected_date, user, area_id, turno, item_id, texto, STATUS_OK, obs_val if needs_obs else "")
                    st.session_state["cache_buster"] += 1
                    st.rerun()

        with b2:
            if st.button("Não OK", key=f"no_{item_id}", disabled=is_no):
                if needs_obs and not _norm(obs_val):
                    st.warning("Preencha o valor antes de registrar.")
                else:
                    _append_log(tables, selected_date, user, area_id, turno, item_id, texto, STATUS_NAO_OK, obs_val if needs_obs else "")
                    st.session_state["cache_buster"] += 1
                    st.rerun()

        with b3:
            if st.button("Desmarcar", key=f"un_{item_id}", disabled=is_pendente):
                _append_log(tables, selected_date, user, area_id, turno, item_id, texto, STATUS_PENDENTE, "")
                st.session_state["cache_buster"] += 1
                st.rerun()


def _append_log(tables: Tables, selected_date: date, user: dict, area_id: str, turno: str, item_id: str, texto: str, status: str, obs: str):
    # Escreve linha no LOGS com o layout padrão.
    logs_sheet_id = st.session_state["LOGS_SHEET_ID"]
    ws_title = tables.ws_logs_title

    day_str = selected_date.isoformat()
    hr = now_dt().strftime("%H:%M:%S")
    dia_semana = weekday_pt_br(selected_date)

    row = [
        day_str,                 # data
        hr,                      # hora
        dia_semana,              # dia_semana
        user.get("login", ""),   # user_login
        user.get("nome", ""),    # user_nome
        area_id,                 # area_id
        turno,                   # turno
        item_id,                 # item_id
        texto,                   # texto
        status,                  # status
        obs or "",               # obs
    ]
    append_log_row(logs_sheet_id, ws_title, row)


# =========================
# Main
# =========================

def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    inject_css()

    if "cache_buster" not in st.session_state:
        st.session_state["cache_buster"] = 1

    config_sheet_id, rules_sheet_id, logs_sheet_id = _get_sheet_ids()
    st.session_state["CONFIG_SHEET_ID"] = config_sheet_id
    st.session_state["RULES_SHEET_ID"] = rules_sheet_id
    st.session_state["LOGS_SHEET_ID"] = logs_sheet_id

    # Sidebar
    st.sidebar.title(APP_TITLE)
    st.sidebar.caption("Status")
    st.sidebar.success("Google Sheets conectado")

    # Data (default hoje)
    st.sidebar.caption("Filtro de data (Dashboard e Checklist)")
    selected_date = st.sidebar.date_input("Data", value=date.today())

    # Carrega tabelas (cacheado)
    try:
        tables = load_tables(config_sheet_id, rules_sheet_id, logs_sheet_id, st.session_state["cache_buster"])
        # injeta ids no objeto para usar no writer sem alterar estrutura
        tables.config_sheet_id = config_sheet_id  # type: ignore
        tables.rules_sheet_id = rules_sheet_id    # type: ignore
        tables.logs_sheet_id = logs_sheet_id      # type: ignore
    except gspread.exceptions.APIError as e:
        # evita ciclo de erro
        st.error("Erro ao acessar Google Sheets. Verifique permissões e IDs das planilhas.")
        st.code(str(e))
        return

    # Login / sessão
    if "user" not in st.session_state:
        st.session_state["user"] = None

    # Navegação
    page = st.sidebar.radio("Navegação", ["Dashboard", "Checklist"])

    if st.session_state["user"] is None:
        user = authenticate(tables)
        if user:
            st.session_state["user"] = user
            st.rerun()
        return

    st.sidebar.button("Logout", on_click=lambda: st.session_state.update({"user": None}))

    if page == "Dashboard":
        page_dashboard(tables, selected_date)
    else:
        page_checklist(tables, st.session_state["user"], selected_date)


if __name__ == "__main__":
    main()
