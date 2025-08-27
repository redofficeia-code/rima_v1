# auth_service.py
import os, re
import pandas as pd
from sqlalchemy import create_engine, text

# Soporte opcional para bcrypt
try:
    from passlib.hash import bcrypt
except ModuleNotFoundError:
    bcrypt = None

# Soporte opcional para contraseñas legadas con puntos
try:
    import legacy_passwords as lp
except ModuleNotFoundError:
    lp = None

# Mapa de constantes/tablas/roles
from auth_map import *

# Un solo engine para ambas tablas (misma cadena)
ENGINE = create_engine(os.environ.get("USERS_DB_URL", "sqlite://"), pool_pre_ping=True, future=True)


# -------------------- utilidades internas --------------------
def _q(sql: str, params=None) -> pd.DataFrame:
    """Ejecuta una consulta y devuelve DataFrame."""
    with ENGINE.connect() as c:
        return pd.read_sql(text(sql), c, params=params or {})


def _norm(s: str) -> str:
    """Normaliza texto para comparaciones 'casefold'."""
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

    # 2) legado con puntos
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

    # 3) texto plano
    return candidate == s


# -------------------- login nivel 1 --------------------
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
    # Columnas mínimas
    cols = [PERSO_COL_COD, PERSO_COL_NOM]
    for op in [PERSO_COL_APE, PERSO_COL_CARG, PERSO_COL_SUC, PERSO_COL_ACT]:
        if op:
            cols.append(op)

    sql = f"SELECT {', '.join(cols)} FROM {PERSO_TABLE} WHERE {PERSO_COL_COD} = :c"
    df = _q(sql, {"c": codigo})
    if df.empty:
        return None

    r = df.iloc[0]

    # activo? (ajusta según tu convención)
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


# -------------------- helpers de rol/usuarios --------------------
def _map_rol(grupo_raw):
    """
    Traduce el GRUPO (texto o numérico) a los roles internos.
    - Bodega / 21 -> ROL_JEFE
    - Soporte / Operario / 14 -> ROL_OPERARIO
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
    """
    Busca el usuario por COD o NOMBRE en posibles tablas de usuarios.
    Intenta en RIMA y luego en SANTIAGO (o defaults si no existen constantes).
    """
    candidate_tables = []
    try:
        candidate_tables.append(USERS_TABLE_RIMA)
    except NameError:
        candidate_tables.append("USERS_DB")
    try:
        candidate_tables.append(USERS_TABLE_SCL)
    except NameError:
        candidate_tables.append("SANTIAGO.dbo.USERS_DB")

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
            # La tabla puede no existir o no ser accesible con este engine
            continue

        if not df.empty:
            row = df.iloc[0].to_dict()
            row["tabla_origen"] = tbl
            return row

    return None


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
