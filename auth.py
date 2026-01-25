from __future__ import annotations
import pandas as pd


def validate_user(users_df: pd.DataFrame, login: str, senha: str):
    """
    users_df esperado (aba USUARIOS):
    - login
    - senha
    - ativo  (TRUE/FALSE)
    - nome   (opcional)
    - perfil (opcional)
    """
    if users_df is None or users_df.empty:
        return None

    df = users_df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    for col in ["login", "senha"]:
        if col not in df.columns:
            return None

    # ativo opcional (default TRUE)
    if "ativo" in df.columns:
        df["ativo"] = df["ativo"].astype(str).str.strip().str.upper()
        df = df[df["ativo"].isin(["TRUE", "1", "SIM", "YES", "Y"])]

    login = (login or "").strip()
    senha = (senha or "").strip()
    if not login or not senha:
        return None

    df["login"] = df["login"].astype(str).str.strip()
    df["senha"] = df["senha"].astype(str).str.strip()

    m = df[(df["login"] == login) & (df["senha"] == senha)]
    if m.empty:
        return None

    row = m.iloc[0].to_dict()
    return {
        "login": row.get("login", login),
        "nome": row.get("nome", row.get("login", login)),
        "perfil": row.get("perfil", "operador"),
    }
