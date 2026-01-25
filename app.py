from __future__ import annotations

from datetime import datetime, date
import pandas as pd
import streamlit as st

from auth import validate_user
from sheets_client import (
    get_service,
    get_client_email,
    read_df,
    append_row,
    ensure_header,
)

# =========================
# CONFIG FIXA (SEUS LINKS)
# =========================
CREDENTIALS_PATH = "credentials.json"

LOGS_SHEET_ID   = "1h17A7dmYb6l3CXKRqOminzlH9rB9zTyBacqJ-DObHGQ"
REGRAS_SHEET_ID = "1CjeDfGgKZBGkEseDc_JxbI5DxEsPvSMGXIv4SV0Qg_0"
CONFIG_SHEET_ID = "1aasGiUbhVUc86yVt9z-5YzYCG6DBDTl6PEuG4wV_jcU"

# Abas (MAIUSCULAS)
WS_AREAS = "AREAS"
WS_ITENS = "ITENS"
WS_DEADLINES = "DEADLINES"
WS_USERS = "USUARIOS"
WS_PARAMS = "PARAMETROS"
WS_LOGS = "LOGS"

LOG_HEADER = [
    "timestamp",
    "data",
    "hora",
    "weekday",
    "user_login",
    "user_nome",
    "area_id",
    "turno",
    "item_id",
    "item_texto",
    "status",
    "deadline_hhmm",
    "tolerancia_min",
]

st.set_page_config(page_title="Checklist Operacao", layout="wide")

MOBILE_CSS = """
<style>
.block-container { padding-top: 2.4rem; padding-bottom: 2rem; }
h1 { margin-top: 0.3rem; }
[data-testid="stSidebar"] { background: #f5f7fb; }
div.stButton > button { border-radius: 12px; padding: 0.55rem 0.95rem; }
.card {
  background: #ffffff;
  border: 1px solid rgba(0,0,0,0.07);
  border-radius: 16px;
  padding: 16px;
  box-shadow: 0 6px 18px rgba(0,0,0,0.04);
}
.pill {
  display:inline-block;
  padding: 6px 10px;
  border-radius: 999px;
  background: rgba(15, 40, 70, 0.07);
  border: 1px solid rgba(15, 40, 70, 0.10);
  font-size: 12px;
  margin-right: 6px;
}
.ok { color: #0a7; font-weight: 700; }
.nok { color: #c22; font-weight: 700; }
.na { color: #666; font-weight: 700; }
</style>
"""
st.markdown(MOBILE_CSS, unsafe_allow_html=True)


def now_dt():
    return datetime.now()


def weekday_str(d: date) -> str:
    names = ["SEG", "TER", "QUA", "QUI", "SEX", "SAB", "DOM"]
    return names[d.weekday()]


def safe_str(x) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x)


def to_int(x, default=0):
    try:
        s = safe_str(x).strip()
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default


def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def require_cols(df: pd.DataFrame, cols: list[str], where: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"Faltam colunas em {where}: {missing}. Colunas atuais: {list(df.columns)}")


def ensure_logs(service):
    ensure_header(service, LOGS_SHEET_ID, WS_LOGS, LOG_HEADER)


@st.cache_resource(show_spinner=False)
def get_service_cached():
    """Google API service nao deve ser retornado pelo cache_data (nao e serializavel).
    Use cache_resource para manter um singleton seguro."""
    return get_service(CREDENTIALS_PATH)


@st.cache_data(show_spinner=False, ttl=60)
def load_all(cache_buster: int):
    """Carrega apenas DataFrames (cache_data)."""
    svc = get_service_cached()

    areas_df = norm_cols(read_df(svc, CONFIG_SHEET_ID, WS_AREAS))
    itens_df = norm_cols(read_df(svc, CONFIG_SHEET_ID, WS_ITENS))
    users_df = norm_cols(read_df(svc, REGRAS_SHEET_ID, WS_USERS))
    params_df = norm_cols(read_df(svc, REGRAS_SHEET_ID, WS_PARAMS))
    logs_df = norm_cols(read_df(svc, LOGS_SHEET_ID, WS_LOGS))

    try:
        deadlines_df = norm_cols(read_df(svc, CONFIG_SHEET_ID, WS_DEADLINES))
    except Exception:
        deadlines_df = pd.DataFrame()

    return areas_df, itens_df, deadlines_df, users_df, params_df, logs_df


def logs_with_local_overlay(logs_df: pd.DataFrame) -> pd.DataFrame:
    """Combina os logs lidos do Sheets com registros locais desta sessao.

    Motivo: evitar novas leituras do Google Sheets apos cada "Confirmar".
    O Sheets tem limite baixo de leituras/minuto por usuario/projeto.
    Então, quando registramos um item, gravamos no Sheets e tambem guardamos
    a mesma linha em memoria (st.session_state) e a UI passa a enxergar isso
    imediatamente, sem precisar recarregar todas as abas.
    """
    extra = st.session_state.get("_local_logs_rows", [])
    if not extra:
        return logs_df
    try:
        extra_df = pd.DataFrame(extra)
        if extra_df.empty:
            return logs_df
        extra_df = norm_cols(extra_df)
        # Alinha colunas (uniao), preservando o que ja existe no logs_df
        cols = list(dict.fromkeys(list(logs_df.columns) + list(extra_df.columns)))
        for c in cols:
            if c not in logs_df.columns:
                logs_df[c] = ""
            if c not in extra_df.columns:
                extra_df[c] = ""
        out = pd.concat([logs_df[cols], extra_df[cols]], ignore_index=True)
        return norm_cols(out)
    except Exception:
        return logs_df


def ensure_logs_once(service):
    if st.session_state.get("_logs_header_ok", False):
        return
    ensure_logs(service)
    st.session_state["_logs_header_ok"] = True


def prep_areas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "area_id" not in df.columns:
        if "area" in df.columns:
            df = df.rename(columns={"area": "area_id"})
        else:
            raise RuntimeError("Aba AREAS precisa ter coluna area_id (ou area).")
    if "nome" not in df.columns:
        df["nome"] = df["area_id"]

    df["area_id"] = df["area_id"].map(lambda x: safe_str(x).strip())
    df["nome"] = df["nome"].map(lambda x: safe_str(x).strip())
    df = df[df["area_id"] != ""]
    return df[["area_id", "nome"]].drop_duplicates()


def prep_itens(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # tolera coluna com typo antigo "item_textoo"
    if "item_texto" not in df.columns and "item_textoo" in df.columns:
        df = df.rename(columns={"item_textoo": "item_texto"})

    require_cols(df, ["item_id", "area_id", "turno", "ordem", "item_texto"], "ITENS")

    if "deadline_hhmm" not in df.columns:
        df["deadline_hhmm"] = ""
    if "tolerancia_min" not in df.columns:
        df["tolerancia_min"] = ""
    if "tipo_resposta" not in df.columns:
        df["tipo_resposta"] = "OK_NA_NA"
    if "ativo" not in df.columns:
        df["ativo"] = "TRUE"

    df["ativo"] = df["ativo"].map(lambda x: safe_str(x).strip().upper())
    df = df[df["ativo"].isin(["TRUE", "1", "SIM", "YES", "Y"])]

    df["item_id"] = df["item_id"].map(lambda x: safe_str(x).strip())
    df["area_id"] = df["area_id"].map(lambda x: safe_str(x).strip())
    df["turno"] = df["turno"].map(lambda x: safe_str(x).strip())
    df["ordem"] = pd.to_numeric(df["ordem"], errors="coerce").fillna(0).astype(int)
    df["item_texto"] = df["item_texto"].map(lambda x: safe_str(x).strip())

    df["deadline_hhmm"] = df["deadline_hhmm"].map(lambda x: safe_str(x).strip())
    df["tolerancia_min"] = df["tolerancia_min"].map(lambda x: safe_str(x).strip())
    df["tipo_resposta"] = df["tipo_resposta"].map(lambda x: safe_str(x).strip().upper())

    df = df[(df["item_id"] != "") & (df["area_id"] != "") & (df["turno"] != "")]
    df = df.sort_values(["area_id", "turno", "ordem", "item_id"]).reset_index(drop=True)
    return df


def prep_logs(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=LOG_HEADER)
    df = df.copy()
    for c in LOG_HEADER:
        if c not in df.columns:
            df[c] = ""
    return df


def status_from_logs(logs_df: pd.DataFrame, target_date: str) -> pd.DataFrame:
    df = logs_df.copy()
    if df.empty:
        return df

    df = df[df["data"].map(lambda x: safe_str(x)) == target_date].copy()
    if df.empty:
        return df

    df["timestamp"] = df["timestamp"].map(lambda x: safe_str(x))
    df = df.sort_values("timestamp")
    df = df.drop_duplicates(subset=["data", "area_id", "turno", "item_id"], keep="last")
    return df


def compute_dashboard(areas_df, itens_df, logs_df, target_date: str) -> pd.DataFrame:
    base = itens_df[["area_id", "turno", "ordem", "item_id", "item_texto", "deadline_hhmm", "tolerancia_min"]].copy()
    last = status_from_logs(logs_df, target_date)

    if not last.empty:
        last2 = last[["area_id", "turno", "item_id", "status", "hora", "user_login"]].copy()
        base = base.merge(last2, on=["area_id", "turno", "item_id"], how="left")
    else:
        base["status"] = ""
        base["hora"] = ""
        base["user_login"] = ""

    areas_map = dict(zip(areas_df["area_id"], areas_df["nome"]))
    base["area_nome"] = base["area_id"].map(lambda x: areas_map.get(x, x))

    def pretty(s):
        s = safe_str(s).strip().upper()
        if s == "OK":
            return "OK"
        if s == "NAO_OK":
            return "NAO OK"
        if s == "NA":
            return "NA"
        return "PENDENTE"

    base["status_view"] = base["status"].map(pretty)
    return base.sort_values(["area_nome", "turno", "ordem", "item_id"]).reset_index(drop=True)


def write_item_log(service, user, target_date: str, area_id: str, turno: str, item_id: str, item_texto: str,
                   status: str, deadline_hhmm: str = "", tolerancia_min: str = ""):
    ts = now_dt()
    row = [
        ts.isoformat(timespec="seconds"),
        target_date,
        ts.strftime("%H:%M"),
        weekday_str(date.fromisoformat(target_date)),
        user["login"],
        user["nome"],
        area_id,
        turno,
        item_id,
        item_texto,
        status,
        deadline_hhmm or "",
        tolerancia_min or "",
    ]
    append_row(service, LOGS_SHEET_ID, WS_LOGS, row)


def deadline_alert(deadline_hhmm: str, toler_min: int, target_date: str) -> tuple[str, str]:
    d = safe_str(deadline_hhmm).strip()
    if not d:
        return ("", "")
    try:
        hh, mm = d.split(":")
        hh = int(hh)
        mm = int(mm)
        dl = datetime.fromisoformat(target_date + "T00:00:00").replace(hour=hh, minute=mm)
        nowv = now_dt()

        if target_date != nowv.strftime("%Y-%m-%d"):
            return ("", f"Deadline {d} (tol {toler_min} min)")

        if nowv > dl:
            mins = int((nowv - dl).total_seconds() // 60)
            if mins > toler_min:
                return ("nok", f"Atrasado {mins} min (dl {d}, tol {toler_min})")
            return ("na", f"Passou do deadline {mins} min (dl {d}, tol {toler_min})")

        return ("", f"Deadline {d} (tol {toler_min} min)")
    except Exception:
        return ("", f"Deadline {d}")


def page_login(users_df):
    st.markdown("<div class='card'>", unsafe_allow_html=True)
    st.title("Checklist Operacao")
    st.caption("Acesso por usuario e senha")

    login = st.text_input("Usuario", placeholder="ex: lucas")
    senha = st.text_input("Senha", type="password", placeholder="senha")

    if st.button("Entrar"):
        user = validate_user(users_df, login, senha)
        if not user:
            st.error("Usuario ou senha invalidos, ou usuario inativo.")
        else:
            st.session_state.user = user
            st.session_state.auth = True
            st.rerun()

    st.markdown("</div>", unsafe_allow_html=True)


def page_checklist(service, areas_df, itens_df, logs_df, target_date: str):
    st.title("Checklist")

    areas_df = prep_areas(areas_df)
    itens_df = prep_itens(itens_df)
    logs_df = prep_logs(logs_df)

    area_opts = areas_df.sort_values("nome").to_dict("records")
    labels = [f'{a["nome"]} ({a["area_id"]})' for a in area_opts]
    label_to_id = {f'{a["nome"]} ({a["area_id"]})': a["area_id"] for a in area_opts}

    def _reset_opened():
        st.session_state.opened = False

    c1, c2 = st.columns([1.2, 1])
    with c1:
        area_label = st.selectbox("Area", labels, key="sel_area", on_change=_reset_opened)
        area_id = label_to_id[area_label]
    with c2:
        turnos = sorted(itens_df[itens_df["area_id"] == area_id]["turno"].unique().tolist())
        turno = st.selectbox("Turno", turnos, key="sel_turno", on_change=_reset_opened)

    if "opened" not in st.session_state:
        st.session_state.opened = False

    if st.button("OK, abrir checklist", use_container_width=True):
        st.session_state.opened = True
        st.rerun()

    if not st.session_state.opened:
        st.info("Selecione Area e Turno e toque em OK para abrir.")
        return

    df_turno = itens_df[(itens_df["area_id"] == area_id) & (itens_df["turno"] == turno)].copy()
    if df_turno.empty:
        st.warning("Nao existem itens para este filtro.")
        return

    last = status_from_logs(logs_df, target_date)
    last = last[(last["area_id"] == area_id) & (last["turno"] == turno)].copy()
    last_map = {}
    if not last.empty:
        for _, r in last.iterrows():
            last_map[safe_str(r["item_id"])] = safe_str(r["status"]).strip().upper()

    st.markdown("<div class='card'>", unsafe_allow_html=True)
    st.subheader(f"{area_label} | {turno} | {target_date}")

    for _, r in df_turno.iterrows():
        item_id = safe_str(r["item_id"])
        texto = safe_str(r["item_texto"])
        dl = safe_str(r.get("deadline_hhmm", "")).strip()
        tol = to_int(r.get("tolerancia_min", ""), 0)

        current = last_map.get(item_id, "")
        cls = "na"
        tag = "PENDENTE"
        if current == "OK":
            cls = "ok"
            tag = "OK"
        elif current == "NAO_OK":
            cls = "nok"
            tag = "NAO OK"
        elif current == "NA":
            cls = "na"
            tag = "NA"

        alert_cls, alert_txt = deadline_alert(dl, tol, target_date)

        st.markdown(f"**{int(r['ordem'])}. {texto}**  <span class='{cls}'>[{tag}]</span>", unsafe_allow_html=True)
        if alert_txt:
            st.markdown(f"<span class='pill'>{alert_txt}</span>", unsafe_allow_html=True)

        # Chave do widget e flag para limpar no proximo rerun (Streamlit nao permite
        # alterar st.session_state[widget_key] depois do widget ser instanciado).
        ans_key = f"ans_{target_date}_{area_id}_{turno}_{item_id}"
        clear_flag = f"__clear_{ans_key}"

        # Aplica limpeza solicitada no run anterior, antes do widget existir.
        if st.session_state.get(clear_flag, False):
            st.session_state.pop(ans_key, None)
            st.session_state.pop(clear_flag, None)

        # Preenche com o ultimo status conhecido (persistencia visual).
        current_choice = current if current in ["OK", "NAO_OK", "NA"] else ""
        if ans_key not in st.session_state:
            st.session_state[ans_key] = current_choice

        opts = ["", "OK", "NAO_OK", "NA"]
        cur_val = st.session_state.get(ans_key, "")
        try:
            cur_idx = opts.index(cur_val)
        except ValueError:
            cur_idx = 0

        choice = st.radio(
            "Resposta",
            options=opts,
            index=cur_idx,
            horizontal=True,
            key=ans_key,
            label_visibility="collapsed",
        )

        cA, cB = st.columns([1, 1])
        with cA:
            if st.button("Confirmar", key=f"btn_ok_{ans_key}", use_container_width=True):
                if not choice:
                    st.warning("Selecione OK, NAO_OK ou NA antes de confirmar.")
                else:
                    ensure_logs_once(service)
                    write_item_log(
                        service=service,
                        user=st.session_state.user,
                        target_date=target_date,
                        area_id=area_id,
                        turno=turno,
                        item_id=item_id,
                        item_texto=texto,
                        status=choice,
                        deadline_hhmm=dl,
                        tolerancia_min=str(tol),
                    )
                    # Guarda em memoria para refletir imediatamente no app, sem reler o Sheets.
                    ts = now_dt()
                    local_row = {
                        "timestamp": ts.isoformat(timespec="seconds"),
                        "data": target_date,
                        "hora": ts.strftime("%H:%M"),
                        "weekday": weekday_str(date.fromisoformat(target_date)),
                        "user_login": st.session_state.user["login"],
                        "user_nome": st.session_state.user["nome"],
                        "area_id": area_id,
                        "turno": turno,
                        "item_id": item_id,
                        "item_texto": texto,
                        "status": choice,
                        "deadline_hhmm": dl or "",
                        "tolerancia_min": str(tol),
                    }
                    st.session_state.setdefault("_local_logs_rows", []).append(local_row)
                    st.success("Registrado.")
                    st.rerun()

        with cB:
            if st.button("Limpar selecao", key=f"btn_clear_{ans_key}", use_container_width=True):
                st.session_state[clear_flag] = True
                st.rerun()

        st.divider()

    st.markdown("</div>", unsafe_allow_html=True)


def page_dashboard(areas_df, itens_df, logs_df, target_date: str):
    st.title("Dashboard")

    areas_df = prep_areas(areas_df)
    itens_df = prep_itens(itens_df)
    logs_df = prep_logs(logs_df)

    dash = compute_dashboard(areas_df, itens_df, logs_df, target_date)

    def kpi_row(g):
        total = len(g)
        ok = (g["status_view"] == "OK").sum()
        nok = (g["status_view"] == "NAO OK").sum()
        na = (g["status_view"] == "NA").sum()
        pend = (g["status_view"] == "PENDENTE").sum()
        return pd.Series({
            "Total": total,
            "OK": ok,
            "NAO OK": nok,
            "NA": na,
            "Pendente": pend,
            "% OK": round(ok / total * 100, 1) if total else 0.0,
        })

    kpi = dash.groupby(["area_nome", "turno"]).apply(kpi_row).reset_index()

    st.markdown("<div class='card'>", unsafe_allow_html=True)
    st.subheader(f"Resumo por Area e Turno | {target_date}")
    st.dataframe(kpi, use_container_width=True, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div class='card'>", unsafe_allow_html=True)
    st.subheader("Detalhe por item (inclui pendentes)")
    st.dataframe(
        dash[["area_nome", "turno", "ordem", "item_texto", "deadline_hhmm", "tolerancia_min", "status_view", "hora", "user_login"]],
        use_container_width=True,
        hide_index=True,
    )
    st.caption("PENDENTE significa que nao existe registro ainda para o item nessa data.")
    st.markdown("</div>", unsafe_allow_html=True)


def page_logs(logs_df, target_date: str):
    st.title("Logs")
    logs_df = prep_logs(logs_df)

    df = logs_df.copy()
    if not df.empty:
        df = df[df["data"].map(lambda x: safe_str(x)) == target_date].copy()
        df = df.sort_values("timestamp", ascending=False)

    st.markdown("<div class='card'>", unsafe_allow_html=True)
    st.subheader(f"Eventos do dia | {target_date}")
    st.dataframe(df, use_container_width=True, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)


def main():
    if "auth" not in st.session_state:
        st.session_state.auth = False
    if "user" not in st.session_state:
        st.session_state.user = None
    if "cache_buster" not in st.session_state:
        st.session_state.cache_buster = 0

    try:
        service = get_service_cached()
        areas_df, itens_df, deadlines_df, users_df, params_df, logs_df = load_all(st.session_state.cache_buster)
        logs_df = logs_with_local_overlay(logs_df)
    except Exception as e:
        st.error("Erro ao carregar dados do Google Sheets.")
        st.exception(e)
        st.stop()

    with st.sidebar:
        st.header("Menu")
        st.caption("Mobile first, use no celular em tela cheia via Adicionar a tela inicial")

        today = now_dt().strftime("%Y-%m-%d")
        target_date = st.date_input("Data", value=date.fromisoformat(today)).strftime("%Y-%m-%d")

        nav = st.radio("Navegacao", ["Checklist", "Dashboard", "Logs"], key="nav_radio")

        st.divider()
        st.caption("Credenciais e acesso")
        st.code(get_client_email(CREDENTIALS_PATH) or "(client_email indisponivel)")

        if st.session_state.auth:
            st.caption(f"Logado: {st.session_state.user['nome']} ({st.session_state.user['login']})")
            if st.button("Logout"):
                st.session_state.auth = False
                st.session_state.user = None
                st.session_state.opened = False
                st.rerun()

        st.divider()
        if st.button("Atualizar dados"):
            st.session_state.cache_buster += 1
            st.cache_data.clear()
            st.rerun()

    if not st.session_state.auth:
        page_login(users_df)
        return

    if nav == "Checklist":
        page_checklist(service, areas_df, itens_df, logs_df, target_date)
    elif nav == "Dashboard":
        page_dashboard(areas_df, itens_df, logs_df, target_date)
    else:
        page_logs(logs_df, target_date)


if __name__ == "__main__":
    main()
