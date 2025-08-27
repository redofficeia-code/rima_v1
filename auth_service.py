# auth_service.py
import re
import pandas as pd
from sqlalchemy import text
from db import ENGINE

# alias del mapa
import auth_map as am

# algoritmo legado (dotcode)
import legacy_passwords as lp

# bcrypt (opcional)
try:
    from passlib.hash import bcrypt
except ModuleNotFoundError:
    bcrypt = None
<<<<<<< ours
=======
from auth_map import (
    USER_TABLE,
    USER_COL_NOM,
    USER_COL_PWD,
    USER_COL_ACT,
    PERSO_TABLE,
    PERSO_COL_COD,
    PERSO_COL_NOM,
    PERSO_COL_APE,
    PERSO_COL_CARG,
    PERSO_COL_SUC,
    PERSO_COL_ACT,
    ROL_ALIASES,
    ROL_JEFE,
)
>>>>>>> theirs


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
    """
    Verifica la contraseña ingresada contra la almacenada en la BD.
    Soporta bcrypt, formato legado con puntos y texto plano.
    """
    if stored is None:
        return False
    s = str(stored)

    # 1) bcrypt ($2...)
    if s.startswith("$2"):
        if bcrypt is None:
            return False
        try:
            return bcrypt.verify(candidate, s)
        except Exception:
            return False

    # 2) formato con puntos (dotcode legado)
    if lp.looks_dotcode(s):
        try:
            candidate_norm = lp.legacy_preprocess(candidate)  # p.ej. capitalize()
            calc = lp.codificar_clave(candidate_norm)
            return calc.strip() == s.strip()
        except Exception:
            return False

    # 3) texto plano (legacy / pruebas)
    return (candidate or "").strip() == s.strip()


# -------------------- helpers de rol --------------------

def _resolver_rol(nombre: str) -> str | None:
    """
    Mapea el nombre lógico (ej.: 'JEFE BODEGA', 'BODEGA', 'OPERARIO BODEGA')
    a un rol definido en auth_map.ROL_ALIASES.
    """
<<<<<<< ours
    nm = _norm(nombre or "")
    for k, v in am.ROL_ALIASES.items():
        if _norm(k) == nm:
            return v
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
=======
    Login 1 contra USER_DB: NOMBRE + PASSWORD.
    Deriva rol desde NOMBRE y lo normaliza al formato canon
    (``Bodega`` o ``Operario``).
>>>>>>> theirs
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

<<<<<<< ours
    rol = _resolver_rol(r[am.USER_COL_NOM]) or am.ROL_OPERARIO

    return {
        "nombre": r[am.USER_COL_NOM],
        "mail": r.get(am.USER_COL_MAIL),
        "rol":  rol,
    }
=======
    # rol lógico desde nombre (normalizado a canon)
    nom_str = (r["nom"] or "").strip().upper()
    rol = ROL_ALIASES.get(nom_str, nom_str)

    return {"nombre": (r["nom"] or "").strip(), "rol": rol}
>>>>>>> theirs


def login_nivel2_operario(codigo: str, clave_nombre: str):
    """
<<<<<<< ours
    Login de nivel 2 (operario): código + nombre como clave.
    Retorna dict con {codigo, nombre, apellido, cargo, sucursal, rol} o None.
=======
    Login 2 SOLO si rol = ``Operario``.
    Valida CODIGO + NOMBRE (como clave) en PERSO_DB.
>>>>>>> theirs
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


# --- Compat: alias para código legacy ---
def login_nivel1(nombre: str, clave: str):
    """Compatibilidad con import legacy desde app.py"""
    return login_usuario(nombre, clave)
