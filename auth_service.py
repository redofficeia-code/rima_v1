# auth_service.py
import os, re, hmac, hashlib
import pandas as pd
from sqlalchemy import create_engine, text
try:
    from passlib.hash import bcrypt
except ModuleNotFoundError:  # pragma: no cover - dependencias opcionales
    bcrypt = None
from auth_map import *

# Un solo engine para ambas tablas (misma BD)
ENGINE = create_engine(os.environ.get("USERS_DB_URL", "sqlite://"), pool_pre_ping=True, future=True)


def _q(sql: str, params=None) -> pd.DataFrame:
    with ENGINE.connect() as c:
        return pd.read_sql(text(sql), c, params=params or {})


def _norm(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s.casefold()


def _verify_pwd(candidate: str, stored: str) -> bool:
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


# Clave secreta para HMAC; usar variable de entorno para producción
_HMAC_KEY = os.environ.get("USERS_PWD_HMAC_KEY", "dev-hmac-key").encode()


def login_nivel1(nombre: str, password: str):
    """
    Login 1 contra USER_DB: NOMBRE + PASSWORD.
    Deriva rol desde NOMBRE (JEFE BODEGA / OPERARIO BODEGA).
    """
    sql = f"""
    SELECT {USER_COL_NOM} AS nom, {USER_COL_PWD} AS pwd, {USER_COL_ACT} AS act
    FROM {USER_TABLE}
    WHERE RTRIM({USER_COL_NOM}) = :n
    """
    df = _q(sql, {"n": nombre})
    if df.empty:
        return None

    r = df.iloc[0]
    # activo?
    if str(r["act"]) in ("1", "True", "true"):
        return None

    # password
    if not _verify_pwd(password, r["pwd"]):
        return None

    # rol lógico desde nombre
    nom_str = (r["nom"] or "").strip().upper()
    rol = ROL_ALIASES.get(nom_str, nom_str)

    return {"nombre": (r["nom"] or "").strip(), "rol": rol, "is_admin": (rol == ROL_JEFE)}



def login_nivel2_operario(codigo: str, clave_nombre: str):
    """
    Login 2 SOLO si rol = OPERARIO BODEGA.
    Valida CODIGO + NOMBRE (como clave) en PERSO_DB.
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
    # activo?
    if str(r.get(PERSO_COL_ACT, 0)) in ("1", "True", "true"):
        return None

    # clave = NOMBRE normalizado
    if _norm(r[PERSO_COL_NOM]) != _norm(clave_nombre):
        return None

    return {
        "codigo": r[PERSO_COL_COD],
        "nombre": r[PERSO_COL_NOM],
        "apellido": r.get(PERSO_COL_APE),
        "cargo": r.get(PERSO_COL_CARG),
        "sucursal": r.get(PERSO_COL_SUC),
    }


def login_usuario(nombre: str, password: str):
    """Login de usuario mediante verificación HMAC del password."""
    sql = f"""
    SELECT {USER_COL_NOM} AS nom, {USER_COL_PWD} AS pwd, {USER_COL_ACT} AS act
    FROM {USER_TABLE}
    WHERE RTRIM({USER_COL_NOM}) = :n
    """
    df = _q(sql, {"n": nombre})
    if df.empty:
        return None

    r = df.iloc[0]
    if str(r["act"]) in ("1", "True", "true"):
        return None

    digest = hmac.new(_HMAC_KEY, password.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(digest, str(r["pwd"])):
        return None

    nom_str = (r["nom"] or "").strip().upper()
    rol = ROL_ALIASES.get(nom_str, nom_str)

    return {"nombre": (r["nom"] or "").strip(), "rol": rol, "is_admin": (rol == ROL_JEFE)}