# auth_map.py
# -*- coding: utf-8 -*-

# ===== USER_DB (Login 1) =====
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

# ===== PERSO_DB (Login 2) =====
PERSO_TABLE      = "PERSO_DB"
PERSO_TABLE_SCL  = "Santiago.dbo.PERSO_DB"
PERSO_TABLE_RIMA = "RIMA.dbo.PERSO_DB"

PERSO_COL_COD  = "CODIGO"
PERSO_COL_NOM  = "NOMBRE"
PERSO_COL_APE  = "APELLIDO"
PERSO_COL_CARG = "CARGO"
PERSO_COL_SUC  = "PERSUC"
PERSO_COL_ACT  = "ACTIVO"  # ACTIVO=1 significa activo

# ===== Roles lógicos =====
ROL_JEFE     = "Bodega"
ROL_OPERARIO = "OPERARIO BODEGA"

def _map_rol(grupo):
    try:
        g = int(str(grupo).strip())
    except Exception:
        g = None
    if g == 21:
        return ROL_JEFE
    if g == 14:
        return ROL_OPERARIO
    return ROL_OPERARIO

# Aliases (compatibilidad para import antiguos)
USERS_TABLE    = USER_TABLE
USERS_COL_NOM  = USER_COL_NOM
USERS_COL_PWD  = USER_COL_PWD
USERS_COL_MAIL = USER_COL_MAIL
USERS_COL_ACT  = USER_COL_ACT

# Pedías ROL_ALIASES en app.py, lo exponemos:
ROL_ALIASES = {
    "BODEGA": ROL_JEFE,
    "ADMIN": ROL_JEFE,
    "OPERARIO": ROL_OPERARIO,
    "OPER": ROL_OPERARIO,
    "OPERARIO BODEGA": ROL_OPERARIO,
}
