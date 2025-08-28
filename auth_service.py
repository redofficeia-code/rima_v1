# auth_service.py
# -*- coding: utf-8 -*-
import os
import re
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus  # para odbc_connect


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
# ---------------------------------------------------------------------
def _engine_from_env():
    # 1) ODBC string directa
    odbc_str = os.getenv("USER_DB_ODBC")
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
    dbn  = os.getenv("MSSQL_DB") or os.getenv("SQLSERVER_DB") or os.getenv("DB_NAME")
    user = os.getenv("MSSQL_USER") or os.getenv("SQLSERVER_USER") or os.getenv("DB_USER")
    pwd  = os.getenv("MSSQL_PASSWORD") or os.getenv("SQLSERVER_PASSWORD") or os.getenv("DB_PASSWORD")
    drv  = os.getenv("MSSQL_DRIVER") or os.getenv("SQLSERVER_DRIVER") or "ODBC Driver 17 for SQL Server"

    if host and dbn and user and pwd:
        # SERVER admite "ip,puerto" o "nombre\\instancia"
        if "," in host or "\\" in host:
            server_field = host  # ya trae puerto o instancia
        else:
            server_field = f"{host},{port}" if port else host

        # braces correctos para DRIVER
        odbc = f"DRIVER={{{drv}}};SERVER={server_field};DATABASE={dbn};UID={user};PWD={pwd};TrustServerCertificate=yes"
        return create_engine(
            "mssql+pyodbc:///?odbc_connect=" + quote_plus(odbc),
            pool_pre_ping=True, future=True, fast_executemany=True
        )

    # 3) URL directa (¡OJO: plural!)
    url = os.getenv("USER_DB_URL")
    if url:
        return create_engine(url, pool_pre_ping=True, future=True)

    # 4) Fallback (solo para tests; en app se sobreescribe con db.ENGINE)
    return create_engine("sqlite://", pool_pre_ping=True, future=True)

# Usa el mismo ENGINE que el resto de la app
try:
    import db
    ENGINE = db.ENGINE
except Exception:
    ENGINE = _engine_from_env()  # fallback si no se pudo importar


# -------------------- utilidades internas --------------------
def _q(sql: str, params=None) -> pd.DataFrame:
    """Ejecuta una consulta y devuelve DataFrame con el ENGINE local (no preferido)."""
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
    Busca el usuario por COD (o nombre) en tablas de usuarios posibles.
    Respeta columnas definidas en auth_map (si existen).
    Devuelve dict estandarizado o None.
    """
    # --- columnas (desde auth_map con fallback seguro) ---
    col_login = globals().get("USER_COL_LOGIN", "COD")
    col_nom   = globals().get("USER_COL_NOM",   "NOMBRE")
    col_pwd   = globals().get("USER_COL_PWD",   "PASSWORD")
    col_grp   = globals().get("USER_COL_GRP",   "GRUPO")

    # --- tablas candidatas ---
    candidates = []
    for name in ["USERS_TABLE_RIMA", "USERS_TABLE_SCL", "USER_TABLE"]:
        v = globals().get(name)
        if v:
            candidates.append(v)

    if not candidates:
        # Fallbacks: probar singular y plural en la BD actual
        candidates = ["USER_DB", "USERS_DB"]

    uparam = (usuario_query or "").strip().upper()
    seen = set()

    for tbl in candidates:
        if not tbl or tbl in seen:
            continue
        seen.add(tbl)
        try:
            sql = f"""
                SELECT TOP 1
                    LTRIM(RTRIM({col_login})) AS cod,
                    LTRIM(RTRIM({col_nom}))   AS nombre,
                    LTRIM(RTRIM({col_pwd}))   AS password,
                    {col_grp}                 AS grupo
                FROM {tbl}
                WHERE UPPER(LTRIM(RTRIM({col_login}))) = :u
                   OR UPPER(LTRIM(RTRIM({col_nom})))   = :u
            """
            df = _q(sql, {"u": uparam})
        except Exception:
            # puede fallar si la BD/tabla no existe o hay permisos
            continue

        if not df.empty:
            row = df.iloc[0].to_dict()
            row["tabla_origen"] = tbl
            return row

    return None

import auth_map as am

# -------------------- login nivel 1 y helpers --------------------
def _map_rol_safe(grupo):
    """Mapea el grupo a rol lógico usando tu auth_map; tolerante a None/cadenas."""
    try:
        return am._map_rol(grupo)
    except Exception:
        return am.ROL_OPERARIO  # fallback

def _verify_pwd_login1(ingresada: str, almacenada: str) -> bool:
    """Comparador para Login1: texto, bcrypt o legacy-puntos."""
    if almacenada is None:
        return False
    s_in = (ingresada or "").strip()
    s_st = str(almacenada).strip()
    if not s_in:
        return False

    # 1) texto plano
    if s_in == s_st:
        return True

    # 2) bcrypt
    if s_st.startswith("$2"):
        if bcrypt is None:
            return False
        try:
            return bcrypt.verify(s_in, s_st)
        except Exception:
            return False

    # 3) legacy puntos
    if lp is not None:
        try:
            return lp.codificar_clave(s_in) == s_st
        except Exception:
            pass

    return False

def _fetch_user_santiago_login1(uq: str):
    """Busca al usuario por COD o NOMBRE directamente en SANTIAGO.dbo.USER_DB (BB1/SPT)."""
    uq = (uq or "").strip()
    if not uq:
        return None

    # Tabla y columnas fijas
    table = "[SANTIAGO].[dbo].[USER_DB]"
    col_id, col_cod, col_nom, col_pwd, col_grp = "ID", "COD", "NOMBRE", "PASSWORD", "GRUPO"

    sql = f"""
        SELECT TOP 1
            {col_id}  AS ID,
            {col_cod} AS COD,
            {col_nom} AS NOMBRE,
            {col_pwd} AS PASSWORD,
            {col_grp} AS GRUPO
        FROM {table}
        WHERE ({col_cod} = :uq OR {col_nom} = :uq)
          AND {col_cod} IN ('BB1','SPT')
        ORDER BY {col_id} DESC;
    """
    try:
        df = db.query_df(sql, {"uq": uq})
    except Exception:
        return None

    if df is None or df.empty:
        return None
    return df.iloc[0].to_dict()

def login_usuario(usuario_query: str, clave: str):
    """
    Login 1: autentica EXCLUSIVAMENTE contra SANTIAGO.dbo.USER_DB
    y SOLO permite los COD 'BB1' y 'SPT'.
    """
    uq = (usuario_query or "").strip()
    if not uq:
        return None

    # Comparación insensible a may/minus y sin espacios alrededor
    sql = """
        SELECT TOP 1
            ID,
            COD,
            NOMBRE,
            PASSWORD,
            GRUPO
        FROM [SANTIAGO].[dbo].[USER_DB]
        WHERE (UPPER(LTRIM(RTRIM(COD)))    = UPPER(:uq)
            OR UPPER(LTRIM(RTRIM(NOMBRE))) = UPPER(:uq))
          AND COD IN ('BB1','SPT')
        ORDER BY ID DESC;
    """
    try:
        df = _q(sql, {"uq": uq})
    except Exception:
        return None

    if df is None or df.empty:
        return None

    row = df.iloc[0].to_dict()

    # Verifica contraseña (texto/bcrypt/“puntos”)
    input_pwd = (clave or "").strip()
    stored = (row.get("PASSWORD") or "").strip()

    if stored:
        try:
            ok = _verify_pwd_login1(input_pwd, stored)  # si la definiste
        except NameError:
            ok = _verify_pwd(input_pwd, stored)
        if not ok:
            return None
    else:
        if input_pwd:   # si en BD está vacía, solo aceptamos input vacío
            return None

    # Rol lógico (usa tu mapper por GRUPO si existe)
    nombre = (row.get("NOMBRE") or "").strip()
    try:
        rol = _map_rol_safe(row.get("GRUPO"))
    except Exception:
        rol = ROL_JEFE if _norm(nombre) == _norm("Bodega") else ROL_OPERARIO

    return {
        "usuario": (row.get("COD") or "").strip(),
        "nombre": nombre,
        "rol": rol,
        "tabla": "SANTIAGO.dbo.USER_DB",
    }


def login_nivel1(usuario_query: str, clave: str):
    """Login 1: restringido a COD 'BB1' y 'SPT'."""
    cod = (usuario_query or '').strip().upper()
    if cod not in LOGIN1_ALLOWED:
        return None

    u = login_usuario(cod, clave)
    if u:
        u["is_admin"] = (u.get("rol") == ROL_JEFE)
    return u

def login_nivel1_debug(usuario_query: str, clave: str):
    """Igual que login_nivel1 pero con motivo para diagnóstico."""
    cod = (usuario_query or "").strip().upper()
    if cod not in LOGIN1_ALLOWED:
        return None, "whitelist"

    row = _fetch_user_santiago_login1(cod)
    if not row:
        return None, "no_user"

    input_pwd = (clave or "").strip()
    stored = (row.get("PASSWORD") or "").strip()

    if not stored and input_pwd:
        return None, "no_pwd"  # sin pwd en BD pero ingresaron algo

    if stored and not _verify_pwd_login1(input_pwd, stored):
        return None, "bad_pwd"

    u = {
        "usuario": row.get("COD"),
        "nombre": row.get("NOMBRE"),
        "rol": _map_rol_safe(row.get("GRUPO")),
        "tabla": "SANTIAGO.dbo.USER_DB",
        "is_admin": _map_rol_safe(row.get("GRUPO")) == ROL_JEFE
    }
    return u, "ok"

def login_nivel2_operario(codigo: str, clave_nombre: str):
    """Login 2 SOLO si rol = OPERARIO.

    Busca por CODIGO en PERSO_DB de Santiago y RIMA (en ese orden),
    y valida que la 'clave' sea el NOMBRE normalizado (case/espacios-insensible)."""

    # --- columnas desde auth_map (con fallbacks seguros) ---
    try:
        base_cols = [PERSO_COL_COD, PERSO_COL_NOM]
        opt_cols  = []
        for op in [PERSO_COL_APE, PERSO_COL_CARG, PERSO_COL_SUC, PERSO_COL_ACT]:
            if op:  # solo si está definida
                opt_cols.append(op)
        col_cod, col_nom = PERSO_COL_COD, PERSO_COL_NOM
        col_ape, col_carg = PERSO_COL_APE, PERSO_COL_CARG
        col_suc, col_act  = PERSO_COL_SUC, PERSO_COL_ACT
    except NameError:
        col_cod, col_nom = "CODIGO", "NOMBRE"
        col_ape, col_carg, col_suc, col_act = "APELLIDO", "CARGO", None, "ACTIVO"
        base_cols = [col_cod, col_nom]
        opt_cols  = [col_ape, col_carg, col_act]

    # --- tablas candidatas: SANTIAGO -> RIMA -> genérica ---
    candidates = []
    for nm in ("PERSO_TABLE_SCL", "PERSO_TABLE_RIMA", "PERSO_TABLE"):
        t = _safe_get(nm, None)
        if t:
            candidates.append(t)
    if not candidates:
        candidates = ["Santiago.dbo.PERSO_DB", "RIMA.dbo.PERSO_DB", "PERSO_DB"]

    codigo_norm = (codigo or "").strip().upper()
    clave_norm  = _norm(clave_nombre)

    def esta_inactivo(row, flag_col: str) -> bool:
        """Devuelve True si el registro debe considerarse inactivo."""
        if not flag_col:
            return False
        name = flag_col.lower()
        val  = str(row.get(flag_col, "")).strip().lower()
        if "activo" in name:
            # ACTIVO: 1/true => activo, 0/false => inactivo
            return val in ("0", "false", "no", "")
        if any(k in name for k in ("elim", "baja", "aggver")):
            # ELIMINADO/BAJA/AGGVER: 1 => inactivo
            return val in ("1", "true", "sí", "si")
        return False  # desconocido: no bloquear

    for tbl in candidates:
        # Intento 1: con todas las columnas definidas
        cols_try = base_cols + [c for c in opt_cols if c]
        try:
            sql = f"""
                SELECT {', '.join(cols_try)}
                FROM {tbl}
                WHERE UPPER(LTRIM(RTRIM({col_cod}))) = :c
            """
            df = _q(sql, {"c": codigo_norm})
        except Exception:
            # Intento 2: mínimo indispensable (CODIGO, NOMBRE) si falló por columnas inexistentes
            try:
                sql = f"""
                    SELECT {col_cod}, {col_nom}
                    FROM {tbl}
                    WHERE UPPER(LTRIM(RTRIM({col_cod}))) = :c
                """
                df = _q(sql, {"c": codigo_norm})
            except Exception:
                continue

        if df.empty:
            continue

        r = df.iloc[0]

        # Chequeo de activo (si tenemos columna de estado)
        if col_act and col_act in r.index and esta_inactivo(r, col_act):
            continue

        # clave = NOMBRE normalizado
        if _norm(r[col_nom]) != clave_norm:
            continue

        return {
            "codigo": r[col_cod],
            "nombre": r[col_nom],
            "apellido": r.get(col_ape) if col_ape else None,
            "cargo": r.get(col_carg) if col_carg else None,
            "sucursal": r.get(col_suc) if col_suc else None,
            "tabla_origen": tbl,
        }

    return None
