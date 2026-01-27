import streamlit as st
import pandas as pd

from sheets_client import list_sheet_titles, read_df


def _pick_users_tab(service, spreadsheet_id: str, candidates: list[str]) -> str:
    titles = set(list_sheet_titles(service, spreadsheet_id))
    for c in candidates:
        if c in titles:
            return c
    lower_map = {t.lower(): t for t in titles}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    raise RuntimeError(
        f"Não encontrei aba de usuários. Candidatas: {candidates}. Existentes: {sorted(titles)}"
    )


def _find_first_col(df_cols: list[str], options: list[str]) -> str | None:
    cols = [c.strip().lower() for c in df_cols]
    for opt in options:
        if opt in cols:
            return opt
    return None


def authenticate_user(rules_sheet_id: str, users_tab_candidates: list[str], service_client):
    """
    Login simples (texto puro).
    Aceita as seguintes colunas na aba de usuários:

    Usuário:
      - user, usuario, login, username

    Senha:
      - password, senha, pass

    Opcional:
      - ativo (1/0, true/false, sim/nao) -> se existir, só deixa logar se ativo == 1/true/sim
    """
    st.session_state.setdefault("logged_in", False)
    st.session_state.setdefault("user_name", "")

    if st.session_state["logged_in"]:
        return st.session_state["user_name"]

    st.title("Login")
    st.caption("Acesso protegido por usuário e senha.")

    u = st.text_input("Usuário", key="login_user")
    p = st.text_input("Senha", type="password", key="login_pass")

    if st.button("Entrar", type="primary"):
        svc = service_client()
        users_tab = _pick_users_tab(svc, rules_sheet_id, users_tab_candidates)

        users_df = read_df(svc, rules_sheet_id, users_tab)
        if users_df is None or users_df.empty:
            st.error("A aba de usuários está vazia.")
            return None

        users_df.columns = [str(c).strip().lower() for c in users_df.columns]
        cols = list(users_df.columns)

        # aceita user/usuario/login/username
        col_user = _find_first_col(cols, ["user", "usuario", "login", "username"])
        # aceita password/senha/pass
        col_pass = _find_first_col(cols, ["password", "senha", "pass"])

        if not col_user or not col_pass:
            st.error("A aba de usuários precisa ter colunas user/password (ou usuario/senha, ou login/senha).")
            st.info(f"Colunas encontradas: {cols}")
            return None

        users_df[col_user] = users_df[col_user].astype(str).str.strip()
        users_df[col_pass] = users_df[col_pass].astype(str).str.strip()

        # se existir coluna ativo, filtra só ativos
        if "ativo" in users_df.columns:
            def _is_active(x):
                s = str(x).strip().lower()
                return s in ["1", "true", "sim", "yes", "y", "ativo"]
            users_df = users_df[users_df["ativo"].apply(_is_active)]

        ok = ((users_df[col_user] == str(u).strip()) & (users_df[col_pass] == str(p).strip())).any()

        if not ok:
            st.error("Usuário ou senha inválidos (ou usuário inativo).")
            return None

        st.session_state["logged_in"] = True
        st.session_state["user_name"] = str(u).strip()
        st.rerun()

    return None
