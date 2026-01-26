import streamlit as st

from sheets_client import pick_existing_tab, read_df


def authenticate_user(rules_sheet_id: str, users_tab_candidates: list[str], service_client):
    """
    Espera uma aba com colunas:
    - user ou usuario ou login
    - password ou senha

    Login simples em texto puro.
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

        # Aqui é onde estava quebrando: se RULES_SHEET_ID estiver errado ou sem permissão,
        # pick_existing_tab vai levantar RuntimeError com status 403/404 na mensagem.
        users_tab = pick_existing_tab(svc, rules_sheet_id, users_tab_candidates)

        users_df = read_df(svc, rules_sheet_id, users_tab)
        if users_df.empty:
            st.error("A aba de usuários está vazia.")
            return None

        users_df.columns = [str(c).strip().lower() for c in users_df.columns]

        col_user = None
        for c in ["user", "usuario", "login"]:
            if c in users_df.columns:
                col_user = c
                break

        col_pass = None
        for c in ["password", "senha"]:
            if c in users_df.columns:
                col_pass = c
                break

        if not col_user or not col_pass:
            st.error("A aba de usuários precisa ter colunas user/password (ou usuario/senha, ou login/senha).")
            return None

        users_df[col_user] = users_df[col_user].astype(str).str.strip()
        users_df[col_pass] = users_df[col_pass].astype(str).str.strip()

        ok = ((users_df[col_user] == str(u).strip()) & (users_df[col_pass] == str(p).strip())).any()
        if not ok:
            st.error("Usuário ou senha inválidos.")
            return None

        st.session_state["logged_in"] = True
        st.session_state["user_name"] = str(u).strip()
        st.rerun()

    return None
