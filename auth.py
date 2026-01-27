import streamlit as st
import pandas as pd

from sheets_client import pick_existing_tab, read_df


def authenticate_user(rules_sheet_id: str, users_tab_candidates: list[str], service_client):
    st.session_state.setdefault("logged_in", False)
    st.session_state.setdefault("user_name", "")

    if st.session_state["logged_in"]:
        return st.session_state["user_name"]

    st.title("Login")
    st.caption("Acesso protegido por usuário e senha.")

    u = st.text_input("Usuário", key="login_user")
    p = st.text_input("Senha", type="password", key="login_pass")

    if st.button("Entrar", type="primary"):
        try:
            svc = service_client()
            tab = pick_existing_tab(svc, rules_sheet_id, users_tab_candidates)
            df = read_df(svc, rules_sheet_id, tab)

            if df.empty:
                st.error("A aba de usuários está vazia.")
                return None

            df.columns = [str(c).strip().lower() for c in df.columns]

            # Aceita vários nomes, incluindo o seu: login + senha
            col_login = None
            for c in ["login", "user", "usuario"]:
                if c in df.columns:
                    col_login = c
                    break

            col_pass = None
            for c in ["senha", "password"]:
                if c in df.columns:
                    col_pass = c
                    break

            if not col_login or not col_pass:
                st.error("A aba de usuários precisa ter colunas login/senha (ou user/password, usuario/senha).")
                st.info(f"Colunas encontradas: {list(df.columns)}")
                return None

            df[col_login] = df[col_login].astype(str).str.strip()
            df[col_pass] = df[col_pass].astype(str).str.strip()

            u_in = str(u or "").strip()
            p_in = str(p or "").strip()

            ok = ((df[col_login] == u_in) & (df[col_pass] == p_in)).any()
            if not ok:
                st.error("Usuário ou senha inválidos.")
                return None

            st.session_state["logged_in"] = True
            st.session_state["user_name"] = u_in
            st.rerun()

        except Exception as e:
            st.error("Falha ao validar login lendo a planilha RULES_SHEET_ID.")
            st.info(str(e))
            return None

    return None
