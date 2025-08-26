# auth_service.py
import os, re
import pandas as pd
from sqlalchemy import text
from db import ENGINE  # reutiliza el mismo ENGINE configurado en db.py
try:
    from passlib.hash import bcrypt
except ModuleNotFoundError:  # dependencias opcionales
    bcrypt = None
from auth_map import *


def _q(sql: str, params=None) -> pd.DataFrame:
    """Ejecuta query y devuelve DataFrame."""
    with ENGINE.connect() as c:
        return pd.read_sql(text(sql), c, params=params or {})


def _norm(s: str) -> str:
    """Normaliza un string para comparaciones de login."""
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s.casefold()


def _verify_pwd(candidate: str, stored: str) -> bool:
    """Verifica contraseña: bcrypt o texto plano (legacy)."""
    if stored is None:
        return False
    s = str(stored)
    if s.startswith("$2"):   # bcrypt
        if bcrypt is None:
            return False
        try:
            return bcrypt.verify(candidate, s)
        except Exception:
            return False
    # texto plano (temporal / legacy)
    return candidate == s


def login_usuario(nombre: str, clave: str):
    """
    Login de nivel 1 (usuario/clave).
    Retorna dict con datos o None si falla.
    """
    sql = f"""
        SELECT {USERS_COL_NOM}, {USERS_COL_PWD}, {USERS_COL_MAIL}
        FROM {USERS_TABLE}
        WHERE {USERS_COL_NOM} = :n
    """
    df = _q(sql, {"n": nombre})
    if df.empty:
        return None

    r = df.iloc[0]
    if not _verify_pwd(clave, r[USERS_COL_PWD]):
        return None

    return {
        "nombre": r[USERS_COL_NOM],
        "mail": r.get(USERS_COL_MAIL),
    }


def login_nivel2_operario(codigo: str, clave_nombre: str):
    """
    Autentica operario de nivel 2 usando código y el nombre como clave.
    Retorna un dict con los datos del operario o None si no coincide.
    """
    cols = [PERSO_COL_COD, PERSO_COL_NOM]
    for op in [PERSO_COL_APE, PERSO_COL_CARG, PERSO_COL_SUC, PERSO_COL_ACT]:
        if op:
            cols.append(op)

    sql = f"SELECT {', '.join(cols)} FROM {PERSO_TABLE} WHERE {PERSO_COL_COD} = :c"
    df = _q(sql, {"c": codigo})
    if df.empty:
        return None

    r = df.iloc[0]

    # Validar si está activo (si la columna existe)
    val_act = str(r.get(PERSO_COL_ACT, "")).strip().lower()
    if val_act in {"0", "false", "no", "n"}:
        return None

    # Validar clave = nombre normalizado
    if _norm(r[PERSO_COL_NOM]) != _norm(clave_nombre):
        return None

    return {
        "codigo": r[PERSO_COL_COD],
        "nombre": r[PERSO_COL_NOM],
        "apellido": r.get(PERSO_COL_APE),
        "cargo": r.get(PERSO_COL_CARG),
        "sucursal": r.get(PERSO_COL_SUC),
    }
