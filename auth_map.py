# auth_map.py
# -*- coding: utf-8 -*-
# =======================
#  USER_DB (Login 1)
# =======================

# Tabla física en SANTIAGO que contiene BB1 y SPT
USER_TABLE_FQN = "[SANTIAGO].[dbo].[USER_DB]"   # totalmente calificada

# Columnas
USER_COL_ID   = "ID"
USER_COL_COD  = "COD"        # BB1 / SPT
USER_COL_NOM  = "NOMBRE"     # 'Bodega', 'Ejecutivo(a)', etc.
USER_COL_PWD  = "PASSWORD"   # cifrada con tu esquema de puntos
USER_COL_ACT  = "Eliminado"  # 0 = activo, 1 = baja (si no existe, se ignora)

# Whitelist de usuarios permitidos en Login 1
USER_LOGIN1_WHITELIST = ("BB1", "SPT")

# =======================
#  Roles lógicos (ruteo)
# =======================
# Si el nombre del rol es 'Bodega' => Admin
ROL_JEFE      = "Bodega"
ROL_OPERARIO  = "Operario"   # etiqueta genérica para el resto

# Alias opcionales (no obligatorio)
ROL_ALIASES = {
    "bodega": ROL_JEFE,
    "jefe bodega": ROL_JEFE,
    "ejecutivo(a)": "Ejecutivo(a)",
}
