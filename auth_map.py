# auth_map.py
# -*- coding: utf-8 -*-

# --- USER_DB (Login 1) ---
USER_TABLE    = "USER_DB"
USER_COL_NOM  = "NOMBRE"       # login por nombre (rol lógico)
USER_COL_PWD  = "PASSWORD"     # texto o hash
USER_COL_ACT  = "Eliminado"    # 0 = activo, 1 = baja
USER_COL_MAIL = "mail_usr"     # correo del usuario

# ---- Tablas físicas para Login 1 (con esquema) ----
# Asegúrate que coincida con el lugar donde está COD='BB1'
USERS_TABLE_RIMA = "RIMA.dbo.USER_DB"
# IMPORTANTE: aquí indicamos Santiago explícitamente (SCL es solo el nombre de la constante)
USERS_TABLE_SCL  = "Santiago.dbo.USER_DB"

# --- PERSO_DB (Login 2) ---
# Tabla genérica (fallback)
PERSO_TABLE    = "PERSO_DB"
# Tablas físicas por BD para que login 2 busque en ambas
PERSO_TABLE_SCL  = "Santiago.dbo.PERSO_DB"   # Santiago
PERSO_TABLE_RIMA = "RIMA.dbo.PERSO_DB"       # RIMA

# Columnas (ajusta el flag y quita sucursal si no existe)
PERSO_COL_ACT  = "ACTIVO"   # <-- en tu tabla ACTIVO=1 significa activo
PERSO_COL_SUC  = None       # <-- no selecciones columna de sucursal si no existe

PERSO_COL_COD  = "CODIGO"      # login operario
PERSO_COL_NOM  = "NOMBRE"      # clave del operario (temporal)
PERSO_COL_APE  = "APELLIDO"    # opcional
PERSO_COL_CARG = "CARGO"       # opcional
PERSO_COL_SUC  = "PERSUC"      # sucursal


# --- Roles lógicos (derivados desde USER_DB) ---
ROL_JEFE     = "Bodega"
ROL_OPERARIO = "OPERARIO BODEGA"

# Mapeo explícito por GRUPO de la BD (recomendado)
def _map_rol(grupo):
    """
    Traduce el valor de GRUPO de la tabla USER_DB a los roles lógicos.
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

# --- Aliases que pueden escribir en el login y deben mapear al canon ---
ROL_ALIASES = {
    # Aliases de jefe
    "BODEGA": ROL_JEFE,
    "ADMIN": ROL_JEFE,

    # Aliases de operario
    "OPERARIO": ROL_OPERARIO,
    "OPER": ROL_OPERARIO,
    "OPERARIO BODEGA": ROL_OPERARIO,
}

# --- Aliases para compatibilidad (evita romper otros módulos) ---

# Usuario (login nivel 1)
try:
    USERS_TABLE
except NameError:
    try:
        USERS_TABLE = USER_TABLE
    except NameError:
        pass

try:
    USERS_COL_NOM
except NameError:
    try:
        USERS_COL_NOM = USER_COL_NOM
    except NameError:
        pass

try:
    USERS_COL_PWD
except NameError:
    try:
        USERS_COL_PWD = USER_COL_PWD
    except NameError:
        pass

try:
    USERS_COL_MAIL
except NameError:
    try:
        USERS_COL_MAIL = USER_COL_MAIL
    except NameError:
        pass

try:
    USERS_COL_ACT
except NameError:
    try:
        USERS_COL_ACT = USER_COL_ACT
    except NameError:
        pass

# Personal (operarios, login nivel 2)
def _alias(name, *alts):
    g = globals()
    if name not in g or g[name] is None:
        for a in alts:
            if a in g and g[a]:
                g[name] = g[a]
                break

_alias('PERSO_TABLE', 'PERSONAL_TABLE')

_alias('PERSO_COL_COD',  'PERSONAL_COL_COD',  'PERSO_COD',  'CODIGO')
_alias('PERSO_COL_NOM',  'PERSONAL_COL_NOM',  'PERSO_NOM',  'NOMBRE')
_alias('PERSO_COL_APE',  'PERSONAL_COL_APE',  'PERSO_APE',  'APELLIDO')
_alias('PERSO_COL_CARG', 'PERSONAL_COL_CARG', 'PERSO_CARGO','CARGO')
_alias('PERSO_COL_SUC',  'PERSONAL_COL_SUC',  'PERSO_SUC',  'SUCUR', 'PERSUC')
_alias('PERSO_COL_ACT',  'PERSONAL_COL_ACT',  'PERSO_ACT',  'ACTIVO','Eliminado')
