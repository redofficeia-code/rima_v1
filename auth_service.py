# auth_service.py
import os, re
import pandas as pd
from sqlalchemy import text
from db import ENGINE  # reutiliza el mismo ENGINE configurado en db.py

try:
    from passlib.hash import bcrypt
except ModuleNotFoundError:  # dependencias opcionales
    bcrypt = None

import auth_map as am


# -------------------- utilidades internas --------------------

def _q(sql: str, params=None) -> pd.DataFrame:
    """Ejecuta una consulta y devuelve DataFrame."""
    with ENGINE.connect() as c:
        return pd.read_sql(text(sql), c, params=params or {})

def _norm(s: str) -> str:
    """Normaliza texto para comparaciones de login."""
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s.casefold()

def _verify_pwd(candidate: str, stored: str) -> bool:
    """Verifica contraseña: bcrypt ($2...) o texto plano (legacy)."""
    if stored is None:
        return False
    s = str(stored)
    if s.startswith("$2"):
        if bcrypt is None:
            return False
        try:
            return bcrypt.verify(candidate, s)
        except Exception:
            return False
    return candidate == s


# -------------------- helpers de rol --------------------

def _resolver_rol(nombre: str) -> str | None:
    """
    Mapea el nombre lógico (ej.: 'JEFE BODEGA', 'BODEGA', 'OPERARIO BODEGA')
    a un rol definido en auth_map.ROL_ALIASES.
    """
    nm = _norm(nombre or "")
    # coincidencia exacta con alias
    for k, v in am.ROL_ALIASES.items():
        if _norm(k) == nm:
            return v
    # heurística
    if "jefe" in nm and "bodega" in nm:
        return am.ROL_JEFE
    if "operario" in nm and "bodega" in nm:
        return am.ROL_OPERARIO
    return None


# -------------------- logins --------------------

def login_usuario(nombre: str, clave: str):
    """
    Login de nivel 1 (usuario/clave).
    Retorna dict con {nombre, mail, rol} o None si falla.
    """
    sql = f"""
        SELECT {am.USER_COL_NOM}, {am.USER_COL_PWD}, {am.USER_COL_MAIL}, {am.USER_COL_ACT}
        FROM {am.USER_TABLE}
        WHERE {am.USER_COL_NOM} = :n
    """
    df = _q(sql, {"n": nombre})
    if df.empty:
        return None

    r = df.iloc[0]

    # 'Eliminado' = 0 => activo, 1 => inactivo
    val_user_act = str(r.get(am.USER_COL_ACT, "0")).strip().lower()
    if val_user_act in {"1", "true", "sí", "si", "yes"}:
        return None

    # contraseña
    if not _verify_pwd(clave, r[am.USER_COL_PWD]):
        return None

    rol = _resolver_rol(r[am.USER_COL_NOM]) or am.ROL_OPERARIO

    return {
        "nombre": r[am.USER_COL_NOM],
        "mail": r.get(am.USER_COL_MAIL),
        "rol":  rol,
    }


def login_nivel2_operario(codigo: str, clave_nombre: str):
    """
    Login de nivel 2 (operario): código + nombre como clave.
    Retorna dict con {codigo, nombre, apellido, cargo, sucursal, rol} o None.
    """
    cols = [am.PERSO_COL_COD, am.PERSO_COL_NOM]
    for op in [am.PERSO_COL_APE, am.PERSO_COL_CARG, am.PERSO_COL_SUC, am.PERSO_COL_ACT]:
        if op:
            cols.append(op)

    sql = f"SELECT {', '.join(cols)} FROM {am.PERSO_TABLE} WHERE {am.PERSO_COL_COD} = :c"
    df = _q(sql, {"c": codigo})
    if df.empty:
        return None

    r = df.iloc[0]

    # 'Eliminado' = 0 => activo, 1 => inactivo
    val_act = str(r.get(am.PERSO_COL_ACT, "0")).strip().lower()
    if val_act in {"1", "true", "sí", "si", "yes"}:
        return None

    # clave = nombre normalizado
    if _norm(r[am.PERSO_COL_NOM]) != _norm(clave_nombre):
        return None

    return {
        "codigo":   r[am.PERSO_COL_COD],
        "nombre":   r[am.PERSO_COL_NOM],
        "apellido": r.get(am.PERSO_COL_APE),
        "cargo":    r.get(am.PERSO_COL_CARG),
        "sucursal": r.get(am.PERSO_COL_SUC),
        "rol":      am.ROL_OPERARIO,
    }
