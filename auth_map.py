# auth_map.py
# -*- coding: utf-8 -*-

# =========================
# USER_DB (Login 1)
# =========================
USER_TABLE       = "USER_DB"     # nombre lógico (sin esquema)
USER_COL_LOGIN   = "COD"         # <- login por código (BB1 / SPT)
USER_COL_NOM     = "NOMBRE"
USER_COL_PWD     = "PASSWORD"
USER_COL_ACT     = "Eliminado"   # 0 = activo, 1 = baja
USER_COL_MAIL    = "mail_usr"
USER_COL_GRP     = "GRUPO"

# Tablas físicas con esquema (ajusta si tu servidor/DB difieren)
USERS_TABLE_RIMA = "RIMA.dbo.USER_DB"
USERS_TABLE_SCL  = "Santiago.dbo.USER_DB"

# =========================
# PERSO_DB (Login 2)
# =========================
PERSO_TABLE      = "PERSO_DB"          # nombre lógico
PERSO_TABLE_SCL  = "Santiago.dbo.PERSO_DB"
PERSO_TABLE_RIMA = "RIMA.dbo.PERSO_DB"

PERSO_COL_COD  = "CODIGO"              # código del operario
PERSO_COL_NOM  = "NOMBRE"
PERSO_COL_APE  = "APELLIDO"
PERSO_COL_CARG = "CARGO"
PERSO_COL_SUC  = "PERSUC"              # sucursal (deja None si no existe)
PERSO_COL_ACT  = "ACTIVO"              # en tu tabla: ACTIVO=1 significa activo

# =========================
# Roles lógicos
# =========================
ROL_JEFE     = "Bodega"
ROL_OPERARIO = "OPERARIO BODEGA"

def _map_rol(grupo):
    """
    Traduce el valor de GRUPO de USER_DB a roles lógicos.
    21 -> Jefe Bodega
    14 -> Operario Bodega
    Otro -> Operario (por defecto)
    """
    try:
        g = int(str(grupo).strip())
    except Exception:
        g = None
    if g == 21:
        return ROL_JEFE
    if g == 14:
        return ROL_OPERARIO
    return ROL_OPERARIO

# =========================
# Back-compat (aliases antiguos)
# =========================
# Muchos módulos viejos referencian USERS_*; los exponemos explícitamente
USERS_TABLE    = USER_TABLE
USERS_COL_NOM  = USER_COL_NOM
USERS_COL_PWD  = USER_COL_PWD
USERS_COL_MAIL = USER_COL_MAIL
USERS_COL_ACT  = USER_COL_ACT

# También algunos proyectos antiguos usan estos nombres alternativos;
# si alguien los importa, quedarán definidos y no romperán.
PERSONAL_TABLE     = PERSO_TABLE
PERSONAL_COL_COD   = PERSO_COL_COD
PERSONAL_COL_NOM   = PERSO_COL_NOM
PERSONAL_COL_APE   = PERSO_COL_APE
PERSONAL_COL_CARGO = PERSO_COL_CARG
PERSONAL_COL_SUC   = PERSO_COL_SUC
PERSONAL_COL_ACT   = PERSO_COL_ACT

# Export explícito opcional (ayuda a linters)
__all__ = [
    # user
    "USER_TABLE", "USER_COL_LOGIN", "USER_COL_NOM", "USER_COL_PWD",
    "USER_COL_ACT", "USER_COL_MAIL", "USER_COL_GRP",
    "USERS_TABLE_RIMA", "USERS_TABLE_SCL",
    # perso
    "PERSO_TABLE", "PERSO_TABLE_SCL", "PERSO_TABLE_RIMA",
    "PERSO_COL_COD", "PERSO_COL_NOM", "PERSO_COL_APE",
    "PERSO_COL_CARG", "PERSO_COL_SUC", "PERSO_COL_ACT",
    # roles
    "ROL_JEFE", "ROL_OPERARIO", "_map_rol",
    # back-compat
    "USERS_TABLE", "USERS_COL_NOM", "USERS_COL_PWD",
    "USERS_COL_MAIL", "USERS_COL_ACT",
    "PERSONAL_TABLE", "PERSONAL_COL_COD", "PERSONAL_COL_NOM",
    "PERSONAL_COL_APE", "PERSONAL_COL_CARGO", "PERSONAL_COL_SUC",
    "PERSONAL_COL_ACT",
]

# Aliases de roles para compatibilidad
ROL_ALIASES = {
    # Jefe
    "BODEGA": ROL_JEFE,
    "ADMIN": ROL_JEFE,
    # Operarios
    "OPERARIO": ROL_OPERARIO,
    "OPER": ROL_OPERARIO,
    "OPERARIO BODEGA": ROL_OPERARIO,
}

# --- Login 1: origen SANTIAGO y whitelist estricta ---
USER_TABLE_LOGIN1_FQN = "[SANTIAGO].[dbo].[USER_DB]"  # tabla física
USER_LOGIN1_WHITELIST = ("BB1", "SPT")                # únicos usuarios válidos

# Columnas de USER_DB usadas por Login 1
USER_COLS_LOGIN1 = dict(ID="ID", COD="COD", NOMBRE="NOMBRE", PASSWORD="PASSWORD")

# Asegura rol admin esperado para ruteo (si ya lo definiste, se respeta)
ROL_JEFE = globals().get("ROL_JEFE", "Bodega")
