# auth_map.py
# --- USER_DB (Login 1) ---
USER_TABLE   = "USER_DB"
USER_COL_NOM = "NOMBRE"        # login por nombre (rol lógico)
USER_COL_PWD = "PASSWORD"      # texto o hash
USER_COL_ACT = "Eliminado"     # 0=activo, 1=baja

# --- PERSO_DB (Login 2) ---
PERSO_TABLE    = "PERSO_DB"
PERSO_COL_COD  = "CODIGO"      # login operario
PERSO_COL_NOM  = "NOMBRE"      # *** clave del operario (temporal) ***
PERSO_COL_APE  = "APELLIDO"    # opcional
PERSO_COL_CARG = "CARGO"       # opcional
PERSO_COL_SUC  = "PERSUC"      # opcional
PERSO_COL_ACT  = "Eliminado"   # 0=activo

# --- Roles lógicos (derivados desde USER_DB.NOMBRE) ---
ROL_JEFE     = "JEFE BODEGA"
ROL_OPERARIO = "OPERARIO BODEGA"

# Soporta legacy "BODEGA" como JEFE BODEGA
ROL_ALIASES = {
    "BODEGA": ROL_JEFE,
    "JEFE BODEGA": ROL_JEFE,
    "OPERARIO BODEGA": ROL_OPERARIO,
}