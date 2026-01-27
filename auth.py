import streamlit as st
import pandas as pd

from sheets_client import read_df, list_sheet_titles


def _pick_users_tab(service, spreadsheet_id: str, candidates: list[str]) -> str:
    titles = set(list_sheet_titles(service, spreadsheet_id))
    for c in candidates:
        if c in titles:
            return c
    lower_map = {t.lower(): t for t in titles}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    raise RuntimeError(f"Não encontrei aba de usuários. Candidatas: {candidates}. Existentes: {sorted(titles)}")


def authenticate_user(rules_sheet_id: str, users_tab_candidates: list[str], service_client):
    """
    Espera uma aba de usuários com colunas:
    - login
    - senha
    - ativo (opcional, TRUE/FALSE)
    """
    st.session_state.setdefault("logged_in", False)
    st.session_state.setdefault("user_login", "")
    st.session_state.setdefault("user_nome", "")

    if st.session_state["logged_in"]:
        return {
            "login": st.session_state["user_login"],
            "nome": st.session_state.get("user_nome", st.session_state["user_login"]),
        }

    st.title("Login")
    st.caption("Acesso protegido por usuário e senha.")

    u = st.text_input("Usuário", key="login_user")
    p = st.text_input("Senha", type="password", key="login_pass")

    if st.button("Entrar", type="primary"):
        svc = service_client()
        users_tab = _pick_users_tab(svc, rules_sheet_id, users_tab_candidates)
        df = read_df(svc, rules_sheet_id, users_tab)

        if df.empty:
            st.error("A aba de usuários está vazia.")
            return None

        df.columns = [str(c).strip().lower() for c in df.columns]

        # Compatível com sua planilha (login/senha)
        if "login" not in df.columns or "senha" not in df.columns:
            st.error("A aba de usuários precisa ter colunas login e senha.")
            st.info(f"Colunas encontradas: {list(df.columns)}")
            return None

        df["login"] = df["login"].astype(str).str.strip()
        df["senha"] = df["senha"].astype(str).str.strip()

        # ativo opcional
        if "ativo" in df.columns:
            ativo = df["ativo"].astype(str).str.strip().str.lower()
            df = df[ativo.isin(["true", "1", "sim", "yes", "y"])]

        hit = df[df["login"] == str(u).strip()]
        if hit.empty:
            st.error("Usuário ou senha inválidos.")
            return None

        ok = (hit["senha"] == str(p).strip()).any()
        if not ok:
            st.error("Usuário ou senha inválidos.")
            return None

        nome = str(hit.iloc[0]["nome"]).strip() if "nome" in hit.columns else str(u).strip()

        st.session_state["logged_in"] = True
        st.session_state["user_login"] = str(u).strip()
        st.session_state["user_nome"] = nome
        st.rerun()

    return None
