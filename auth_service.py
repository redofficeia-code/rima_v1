# auth_service.py
# -*- coding: utf-8 -*-
import os, re
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# --- dependencias opcionales ---
try:
    from passlib.hash import bcrypt
except ModuleNotFoundError:
    bcrypt = None
<<<<<<< ours

try:
    import legacy_passwords as lp
except ModuleNotFoundError:
    lp = None

# --- mapa de constantes/tablas/roles (provenientes de tu proyecto) ---
# Debes tener estas constantes en auth_map.py; aquí solo las importamos.
# Si alguna faltara, usamos fallbacks más abajo para no romper la app.
try:
    from auth_map import *
except Exception:
    # Fallbacks súper conservadores para desarrollo si no carga auth_map
    ROL_JEFE = "Bodega"
    ROL_OPERARIO = "Operario"
    # Tablas/columnas por defecto (ajusta en tu auth_map real)
    USERS_TABLE_RIMA = "RIMA.dbo.USERS_DB"
    USERS_TABLE_SCL = "SANTIAGO.dbo.USERS_DB"
    PERSO_TABLE = "PERSO_DB"
    PERSO_COL_COD = "CODIGO"
    PERSO_COL_NOM = "NOMBRE"
    PERSO_COL_APE = "APELLIDO"
    PERSO_COL_CARG = "CARGO"
    PERSO_COL_SUC = "SUCURSAL"
    PERSO_COL_ACT = "AGGVER"  # si tu “activo” es otro campo, cámbialo en auth_map.py


# -------------------- conexión a BD --------------------
def _engine_from_env():
    """
    Crea el engine aceptando:
      - USERS_DB_URL: URL SQLAlchemy (mssql+pyodbc://user:pwd@host:1433/DB?driver=ODBC+Driver+17+for+SQL+Server&...)
      - USERS_ODBC_CONNECT: Cadena ODBC (DRIVER=...;SERVER=host,port;DATABASE=...;UID=...;PWD=...;)
    Si USERS_DB_URL contiene por error una cadena ODBC (DRIVER=/SERVER=), la convertimos automáticamente.
    """
    url = (os.environ.get("USERS_DB_URL") or "").strip()
    if url:
        if "DRIVER=" in url.upper() or "SERVER=" in url.upper():
            print("[auth_service] Detectada cadena ODBC en USERS_DB_URL → usando odbc_connect")
            return create_engine(f"mssql+pyodbc:///?odbc_connect={quote_plus(url)}", pool_pre_ping=True, future=True)
        print("[auth_service] Usando USERS_DB_URL (URL SQLAlchemy)")
        return create_engine(url, pool_pre_ping=True, future=True)

    odbc = (os.environ.get("USERS_ODBC_CONNECT") or "").strip()
    if odbc:
        print("[auth_service] Usando USERS_ODBC_CONNECT (ODBC con coma en SERVER permitido)")
        return create_engine(f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc)}", pool_pre_ping=True, future=True)

    print("[auth_service] Sin variables de conexión → usando sqlite:// (solo pruebas)")
    return create_engine("sqlite://", pool_pre_ping=True, future=True)

=======
try:
    import legacy_passwords as lp
except ModuleNotFoundError:  # pragma: no cover
    lp = None
from auth_map import *

# Usuario permitidos en Login 1 (whitelist)
LOGIN1_ALLOWED = {'BB1', 'SPT'}

# Mapeo COD -> NOMBRE para USER_DB
LOGIN1_COD_TO_NOMBRE = {
    'BB1': 'JEFE BODEGA',
    'SPT': 'OPERARIO BODEGA',
}

# Un solo engine para ambas tablas (misma BD)
ENGINE = create_engine(os.environ.get("USERS_DB_URL", "sqlite://"), pool_pre_ping=True, future=True)
>>>>>>> theirs

ENGINE = _engine_from_env()


# -------------------- utilidades internas --------------------
def _q(sql: str, params=None) -> pd.DataFrame:
    """Ejecuta una consulta y devuelve DataFrame."""
    with ENGINE.connect() as c:
        return pd.read_sql(text(sql), c, params=params or {})


def _norm(s: str) -> str:
    """Normaliza texto para comparaciones robustas (espacios y may/min)."""
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s.casefold()


def _verify_pwd(candidate: str, stored: str) -> bool:
    """
    Verifica la contraseña ingresada contra la almacenada en la BD.
    Soporta:
      1) bcrypt ($2...)
      2) formato legado "números.separados.por.puntos"
      3) texto plano (fallback)
    """
    if stored is None:
        return False

    s = str(stored).strip()

    # 1) bcrypt
    if s.startswith("$2"):
        if bcrypt is None:
            return False
        try:
            return bcrypt.verify(candidate, s)
        except Exception:
            return False
<<<<<<< ours

    # 2) legado con puntos (solo dígitos/puntos/espacio y contiene '.')
    if ("." in s) and all(ch.isdigit() or ch == "." or ch.isspace() for ch in s):
        if lp is None:
            return False
        enc = None
        # Intentamos distintos nombres de función por compatibilidad
        for fn_name in ("codificar_clave", "encode"):
            try:
                fn = getattr(lp, fn_name, None)
                if fn:
                    enc = fn(candidate)
                    break
            except Exception:
                enc = None
        return enc is not None and str(enc).strip() == s

    # 3) texto plano (último recurso)
    return candidate == s


# -------------------- helpers de rol/usuarios --------------------
def _map_rol(grupo_raw):
    """
    Traduce el GRUPO (texto o numérico) a los roles internos.
    - Bodega / 21 -> ROL_JEFE
    - Soporte / Operario / Operario Bodega / 14 -> ROL_OPERARIO
    """
    gtxt = (str(grupo_raw or "")).strip().casefold()
    try:
        gnum = int(str(grupo_raw).strip())
    except Exception:
        gnum = None

    if gtxt in {"bodega", "jefe bodega", "jefebodega"} or gnum == 21:
        return ROL_JEFE
    if gtxt in {"soporte", "operario", "operario bodega"} or gnum == 14:
        return ROL_OPERARIO
    return ROL_OPERARIO


def _find_user_any_db(usuario_query: str):
=======
    if lp is not None:
        try:
            return lp.codificar_clave(candidate) == s
        except Exception:
            pass
    # texto plano (temporal / legacy)
    return candidate == s


def login_nivel1(usuario_query: str, password: str):
    """
    Login 1: restringido a COD 'BB1' (Jefe) y 'SPT' (Operario).
    """
    cod = (usuario_query or '').strip().upper()
    if cod not in LOGIN1_ALLOWED:
        return None
    nombre = LOGIN1_COD_TO_NOMBRE.get(cod)
    if not nombre:
        return None
    sql = f"""
    SELECT {USER_COL_NOM} AS nom, {USER_COL_PWD} AS pwd, {USER_COL_ACT} AS act
    FROM {USER_TABLE}
    WHERE RTRIM({USER_COL_NOM}) = :n
>>>>>>> theirs
    """
    Busca el usuario por COD o NOMBRE en posibles tablas de usuarios.
    Intenta primero en RIMA y luego en SANTIAGO (o defaults si no hay constantes).
    Devuelve un dict estandarizado o None.
    """
    # Construimos lista de tablas a intentar
    candidate_tables = []
    try:
        if USERS_TABLE_RIMA:
            candidate_tables.append(USERS_TABLE_RIMA)
    except NameError:
        candidate_tables.append("USERS_DB")

    try:
        if USERS_TABLE_SCL:
            candidate_tables.append(USERS_TABLE_SCL)
    except NameError:
        candidate_tables.append("SANTIAGO.dbo.USERS_DB")

    # Evita duplicados
    seen = set()
    for tbl in candidate_tables:
        if not tbl or tbl in seen:
            continue
        seen.add(tbl)
        try:
            df = _q(
                f"""
                SELECT TOP 1
                    COD       AS cod,
                    NOMBRE    AS nombre,
                    PASSWORD  AS password,
                    GRUPO     AS grupo
                FROM {tbl}
                WHERE COD = :u OR NOMBRE = :u
                """,
                {"u": usuario_query},
            )
        except Exception:
            # La tabla puede no existir o no estar accesible con este ENGINE
            continue

        if not df.empty:
            row = df.iloc[0].to_dict()
            row["tabla_origen"] = tbl
            return row

    return None


# -------------------- login nivel 1 --------------------
def login_usuario(usuario_query: str, clave: str):
    """
    Autentica un usuario por COD o NOMBRE contra RIMA/SANTIAGO.
    Retorna dict {usuario, nombre, rol, tabla} o None si falla.
    """
    usuario = _find_user_any_db(usuario_query)
    if not usuario:
        return None

    stored = usuario.get("password")
    if stored is None or not _verify_pwd(clave, stored):
        return None

    return {
        "usuario": usuario["cod"],
        "nombre": usuario["nombre"],
        "rol": _map_rol(usuario.get("grupo")),
        "tabla": usuario.get("tabla_origen"),
    }


def login_nivel1(usuario_query: str, clave: str):
    """
    Autentica un usuario contra RIMA/SANTIAGO por COD o NOMBRE.
    Devuelve dict con campos mínimos para sesión, incluyendo 'rol' y 'is_admin'.
    """
    u = login_usuario(usuario_query, clave)
    if u:
        u["is_admin"] = (u.get("rol") == ROL_JEFE)
    return u


# -------------------- login nivel 2 (operario) --------------------
def login_nivel2_operario(codigo: str, clave_nombre: str):
    """
    Login 2 SOLO si rol = OPERARIO.
    Valida CODIGO + NOMBRE (como clave) en PERSO_DB.
    """
    try:
        # Usa las constantes del auth_map.py
        cols = [PERSO_COL_COD, PERSO_COL_NOM]
        for op in [PERSO_COL_APE, PERSO_COL_CARG, PERSO_COL_SUC, PERSO_COL_ACT]:
            if op:
                cols.append(op)
        table = PERSO_TABLE
        col_cod, col_nom, col_ape, col_carg, col_suc, col_act = (
            PERSO_COL_COD, PERSO_COL_NOM, PERSO_COL_APE, PERSO_COL_CARG, PERSO_COL_SUC, PERSO_COL_ACT
        )
    except NameError:
        # Fallback si no hay mapeo (definimos locales)
        table = "PERSO_DB"
        col_cod, col_nom, col_ape, col_carg, col_suc, col_act = (
            "CODIGO", "NOMBRE", "APELLIDO", "CARGO", "SUCURSAL", "AGGVER"
        )
        cols = [col_cod, col_nom, col_ape, col_carg, col_suc, col_act]

    sql = f"SELECT {', '.join(cols)} FROM {table} WHERE {col_cod} = :c"
    df = _q(sql, {"c": codigo})
    if df.empty:
        return None

    r = df.iloc[0]

    # activo?
    if str(r.get(col_act, 0)) in ("1", "True", "true"):
        return None

    # clave = NOMBRE normalizado
    if _norm(r[col_nom]) != _norm(clave_nombre):
        return None

    return {
        "codigo": r[col_cod],
        "nombre": r[col_nom],
        "apellido": r.get(col_ape),
        "cargo": r.get(col_carg),
        "sucursal": r.get(col_suc),
    }

