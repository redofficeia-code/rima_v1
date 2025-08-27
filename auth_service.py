# auth_service.py
# -*- coding: utf-8 -*-
import os
import re
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus  # << necesario para odbc_connect

# --- dependencias opcionales ---
try:
    from passlib.hash import bcrypt
except ModuleNotFoundError:  # pragma: no cover
    bcrypt = None

try:
    import legacy_passwords as lp  # debe exponer codificar_clave()
except ModuleNotFoundError:  # pragma: no cover
    lp = None

# Mapa/constantes del proyecto (tablas, columnas, roles, etc.)
from auth_map import *

# ---------------------------------------------------------------------
# Login 1: SOLO estos usuarios (COD exacto)
# ---------------------------------------------------------------------
LOGIN1_ALLOWED = {'BB1', 'SPT'}  # 'BB1' = JEFE BODEGA, 'SPT' = OPERARIO BODEGA

# ---------------------------------------------------------------------
# Conexión a BD
#   Soporta:
#   1) USERS_DB_ODBC  -> ODBC puro (DRIVER=...;SERVER=...;DATABASE=...;UID=...;PWD=...)
#   2) Variables separadas (MSSQL_HOST, MSSQL_PORT, MSSQL_DB, MSSQL_USER, MSSQL_PASSWORD, MSSQL_DRIVER)
#   3) USERS_DB_URL   -> URL SQLAlchemy. Si detecta "host,port" convierte a odbc_connect automáticamente.
#   Si nada está definido, usa sqlite:// (solo pruebas).
# ---------------------------------------------------------------------
def _engine_from_env():
    # 1) ODBC string directa
    odbc_str = os.getenv("USERS_DB_ODBC")
    if odbc_str:
        odbc = odbc_str
        if "TrustServerCertificate" not in odbc:
            odbc += ";TrustServerCertificate=yes"
        return create_engine(
            "mssql+pyodbc:///?odbc_connect=" + quote_plus(odbc),
            pool_pre_ping=True, future=True, fast_executemany=True
        )

    # 2) Variables separadas
    host = os.getenv("MSSQL_HOST") or os.getenv("SQLSERVER_HOST")
    port = os.getenv("MSSQL_PORT") or os.getenv("SQLSERVER_PORT") or "1433"
    db   = os.getenv("MSSQL_DB") or os.getenv("SQLSERVER_DB") or os.getenv("DB_NAME")
    user = os.getenv("MSSQL_USER") or os.getenv("SQLSERVER_USER") or os.getenv("DB_USER")
    pwd  = os.getenv("MSSQL_PASSWORD") or os.getenv("SQLSERVER_PASSWORD") or os.getenv("DB_PASSWORD")
    drv  = os.getenv("MSSQL_DRIVER") or os.getenv("SQLSERVER_DRIVER") or "ODBC Driver 17 for SQL Server"

    if host and db and user and pwd:
        # SERVER admite "ip,puerto" o "nombre\instancia"
        if "," in host or "\\" in host:
            server_field = host  # ya trae puerto o instancia
        else:
            server_field = f"{host},{port}" if port else host

        odbc = f"DRIVER={{{{ {drv} }}}};SERVER={server_field};DATABASE={db};UID={user};PWD={pwd};TrustServerCertificate=yes"
        return create_engine(
            "mssql+pyodbc:///?odbc_connect=" + quote_plus(odbc),
            pool_pre_ping=True, future=True, fast_executemany=True
        )

    # 3) URL directa
    url = os.getenv("USERS_DB_URL")
    if url:
        # Si es URL mssql y contiene "host,port", conviértela a odbc_connect
        if url.lower().startswith("mssql") and "," in url.split("@")[-1].split("/")[0]:
            # Extraer partes simples: mssql+pyodbc://user:pwd@host,port/db?driver=...
            # Recomendación: usa odbc_connect para soportar la coma segura
            # Intento de parseo mínimo:
            try:
                # Busca ?driver=... o usa driver por defecto
                drv_q = "ODBC Driver 17 for SQL Server"
                if "driver=" in url.lower():
                    drv_q = url.split("driver=")[-1]
                    drv_q = drv_q.split("&")[0].replace("+", " ")

                # user:pwd
                creds = url.split("://", 1)[1].split("@", 1)[0]
                dbhost = url.split("@", 1)[1]
                host_port = dbhost.split("/", 1)[0]           # host,port
                dbname = dbhost.split("/", 1)[1].split("?", 1)[0]
                user, pwd = creds.split(":", 1)

                odbc = f"DRIVER={{{{ {drv_q} }}}};SERVER={host_port};DATABASE={dbname};UID={user};PWD={pwd};TrustServerCertificate=yes"
                return create_engine(
                    "mssql+pyodbc:///?odbc_connect=" + quote_plus(odbc),
                    pool_pre_ping=True, future=True, fast_executemany=True
                )
            except Exception:
                # Si falla, intenta crear la URL tal cual (por si no había coma)
                pass

        return create_engine(url, pool_pre_ping=True, future=True)

    # 4) Fallback
    return create_engine("sqlite://", pool_pre_ping=True, future=True)

# Crea el engine usando la lógica robusta
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
      2) formato legado "números.separados.por.puntos" (lp.codificar_clave)
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

    # 2) formato legado (si existe módulo legacy_passwords)
    if lp is not None:
        try:
            return lp.codificar_clave(candidate) == s
        except Exception:
            pass

    # 3) texto plano (legacy)
    return candidate == s

def _safe_get(name: str, default=None):
    """Obtiene una constante importada desde auth_map si existe; si no, default."""
    try:
        return globals()[name]
    except KeyError:
        return default

def _find_user_any_db(usuario_query: str):
    """
    Busca el usuario por COD o NOMBRE en posibles tablas de usuarios.
    Intenta primero en RIMA y luego en SANTIAGO (o defaults si no hay constantes).
    Devuelve un dict estandarizado o None.
    """
    candidate_tables = []
    tbl_rima = _safe_get("USERS_TABLE_RIMA", None)
    tbl_scl  = _safe_get("USERS_TABLE_SCL", None)

    if tbl_rima:
        candidate_tables.append(tbl_rima)
    if tbl_scl:
        candidate_tables.append(tbl_scl)
    if not candidate_tables:
        candidate_tables = ["USERS_DB"]

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
            continue

        if not df.empty:
            row = df.iloc[0].to_dict()
            row["tabla_origen"] = tbl
            return row
    return None

# -------------------- login nivel 1 y helpers --------------------
def _map_rol_safe(grupo_val):
    """
    Usa _map_rol de auth_map si existe; si no, fallback:
      - 21 -> ROL_JEFE
      - 14 -> ROL_OPERARIO
      - otro -> ROL_OPERARIO
    """
    try:
        return _map_rol(grupo_val)  # type: ignore
    except Exception:
        try:
            g = int(grupo_val) if grupo_val is not None else None
        except Exception:
            g = None
        if g == 21:
            return ROL_JEFE
        if g == 14:
            return ROL_OPERARIO
        return ROL_OPERARIO

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
        "rol": _map_rol_safe(usuario.get("grupo")),
        "tabla": usuario.get("tabla_origen"),
    }

def login_nivel1(usuario_query: str, clave: str):
    """
    Login 1: restringido a COD 'BB1' (Jefe Bodega) y 'SPT' (Operario Bodega).
    """
    cod = (usuario_query or '').strip().upper()
    if cod not in LOGIN1_ALLOWED:
        return None

    u = login_usuario(cod, clave)
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
        cols = [PERSO_COL_COD, PERSO_COL_NOM]
        for op in [PERSO_COL_APE, PERSO_COL_CARG, PERSO_COL_SUC, PERSO_COL_ACT]:
            if op:
                cols.append(op)
        table = PERSO_TABLE
        col_cod, col_nom, col_ape, col_carg, col_suc, col_act = (
            PERSO_COL_COD, PERSO_COL_NOM, PERSO_COL_APE, PERSO_COL_CARG, PERSO_COL_SUC, PERSO_COL_ACT
        )
    except NameError:
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

    # ¿activo? (ajusta si en tu instalación el significado es inverso)
    if str(r.get(col_act, 0)).strip() in ("1", "True", "true"):
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
