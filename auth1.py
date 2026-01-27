import streamlit as st
import pandas as pd

from sheets_client import read_df, list_sheet_titles_cached


def _pick_users_tab(client, spreadsheet_id: str, candidates: list[str]) -> str:
    titles = set(list_sheet_titles_cached(client, spreadsheet_id))
    for c in candidates:
        if c in titles:
            return c
    lower_map = {t.lower(): t for t in titles}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    raise RuntimeError(f"Nao encontrei aba de usuarios. Candidatas: {candidates}. Existentes: {sorted(titles)}")


def authenticate_user(rules_sheet_id: str, users_tab_candidates: list[str], gs_client):
    st.session_state.setdefault("logged_in", False)
    st.session_state.setdefault("user_login", "")
    st.session_state.setdefault("user_nome", "")

    if st.session_state["logged_in"]:
        return {"login": st.session_state["user_login"], "nome": st.session_state["user_nome"]}

    st.title("Login")
    st.caption("Acesso protegido por usuario e senha.")

    u = st.text_input("Usuario", key="login_user")
    p = st.text_input("Senha", type="password", key="login_pass")

    if st.button("Entrar", type="primary"):
        client = gs_client()
        users_tab = _pick_users_tab(client, rules_sheet_id, users_tab_candidates)
        users_df = read_df(client, rules_sheet_id, users_tab)

        if users_df.empty:
            st.error("A aba de usuarios esta vazia.")
            return None

        users_df.columns = [str(c).strip().lower() for c in users_df.columns]

        # aceita varios nomes
        col_login = None
        for c in ["login", "user", "usuario"]:
            if c in users_df.columns:
                col_login = c
                break

        col_pass = None
        for c in ["senha", "password"]:
            if c in users_df.columns:
                col_pass = c
                break

        col_nome = "nome" if "nome" in users_df.columns else None
        col_ativo = "ativo" if "ativo" in users_df.columns else None

        if not col_login or not col_pass:
            st.error("A aba de usuarios precisa ter colunas login/senha (ou user/password).")
            st.info(f"Colunas encontradas: {list(users_df.columns)}")
            return None

        users_df[col_login] = users_df[col_login].astype(str).str.strip()
        users_df[col_pass] = users_df[col_pass].astype(str).str.strip()
        if col_nome:
            users_df[col_nome] = users_df[col_nome].astype(str).str.strip()

        # filtra ativo se existir
        if col_ativo:
            tmp = users_df[col_ativo].astype(str).str.strip().str.lower()
            users_df = users_df[tmp.isin(["true", "1", "sim", "yes", "ativo"])]

        u2 = str(u).strip()
        p2 = str(p).strip()

        hit = users_df[(users_df[col_login] == u2) & (users_df[col_pass] == p2)]
        if hit.empty:
            st.error("Usuario ou senha invalidos.")
            return None

        nome = u2
        if col_nome:
            nome = str(hit.iloc[0][col_nome]).strip() or u2

        st.session_state["logged_in"] = True
        st.session_state["user_login"] = u2
        st.session_state["user_nome"] = nome
        st.rerun()

    return None
