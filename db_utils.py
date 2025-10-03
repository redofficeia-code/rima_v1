# -*- coding: utf-8 -*-
"""
Deprecated compatibility layer. Use `db` module instead.

Además, incluye utilidades de flujo para Notas de Venta (NV_FLOW, opción B):
- get_nv_flow(num_nota)
- upsert_nv_flow(num_nota, **fields)
- set_nv_estado_aprobada(num_nota, aprobado_por)
- set_nv_estado_pendiente(num_nota)
- set_nv_zona(num_nota, zona_id, asignado_por)
- set_nv_retira(num_nota, retira)
"""

from datetime import datetime
from typing import Any, Dict, Optional, List

from sqlalchemy import text

from db import (
    ENGINE,
    query_df,
    get_oc_detalle,
    get_nota_detalle,
    get_stock_actual,
    get_guia_desde_nv,
    get_factura_desde_nv,
)

# Intentar usar el engine dedicado a RIMA si existe; si no, caer en ENGINE
try:
    from db import ENGINE_RIMA as _ENGINE_RIMA  # type: ignore
except Exception:  # pragma: no cover
    _ENGINE_RIMA = ENGINE


# =========================
#   ZONAS (Paso 2)
# =========================

def get_zonas() -> List[Dict[str, Any]]:
    """
    Devuelve la lista de zonas desde RIMA.dbo.ZONAS_DB
    con llaves: ID_ZONA (int) y NOMBRE (str), ordenadas por NOMBRE.
    """
    sql = text("""
        SELECT ID AS ID_ZONA,
               NOMBRE
        FROM RIMA.dbo.ZONAS_DB
        ORDER BY NOMBRE
    """)
    with _ENGINE_RIMA.begin() as cx:
        rows = cx.execute(sql).mappings().all()
    # Normalizamos a dict por si el caller no maneja MappingResult
    return [dict(r) for r in rows]


# =========================
#   NV_FLOW (Opción B)
# =========================

def _nvflow_defaults() -> Dict[str, Any]:
    """Valores por defecto para NV_FLOW cuando no existe registro."""
    return {
        "ESTADO": "",                # "", "PEND", "APROB"
        "ZONA_ID": None,
        "RETIRA_CLIENTE": 0,
        "APROBADO_POR": None,
        "APROBADO_FECHA": None,
        "ASIGNADO_POR": None,
        "ASIGNADO_FECHA": None,
        "UPDATED_AT": None,
    }


def get_nv_flow(num_nota: int) -> Dict[str, Any]:
    """
    Lee el estado de flujo para la Nota de Venta desde RIMA.dbo.NV_FLOW.
    Devuelve un dict con llaves: ESTADO, ZONA_ID, RETIRA_CLIENTE, APROBADO_POR, APROBADO_FECHA,
    ASIGNADO_POR, ASIGNADO_FECHA, UPDATED_AT.
    Si no existe registro, devuelve valores por defecto ("" / None / 0).
    """
    sql = text("""
        SELECT TOP 1
            ESTADO,
            ZONA_ID,
            ISNULL(RETIRA_CLIENTE, 0) AS RETIRA_CLIENTE,
            APROBADO_POR,
            APROBADO_FECHA,
            ASIGNADO_POR,
            ASIGNADO_FECHA,
            UPDATED_AT
        FROM RIMA.dbo.NV_FLOW
        WHERE NUMNOTA = :n
        ORDER BY UPDATED_AT DESC
    """)
    with _ENGINE_RIMA.begin() as cx:
        row = cx.execute(sql, {"n": int(num_nota)}).mappings().first()

    if not row:
        return _nvflow_defaults()

    data = dict(row)

    # Normalizar/asegurar llaves
    defaults = _nvflow_defaults()
    for k, v in defaults.items():
        data.setdefault(k, v)

    # Si viniera NULL en BD, convertir ESTADO a cadena vacía
    if not data.get("ESTADO"):
        data["ESTADO"] = ""

    # Cast seguro de RETIRA_CLIENTE
    data["RETIRA_CLIENTE"] = int(bool(data.get("RETIRA_CLIENTE", 0)))

    return data


def upsert_nv_flow(
    num_nota: int,
    *,
    ESTADO: Optional[str] = None,            # 'PEND' | 'APROB'
    ZONA_ID: Optional[int] = None,
    RETIRA_CLIENTE: Optional[bool] = None,
    APROBADO_POR: Optional[str] = None,
    ASIGNADO_POR: Optional[str] = None,
) -> None:
    """
    Inserta/actualiza el flujo de NV en RIMA.dbo.NV_FLOW usando MERGE.
    - Si ESTADO == 'APROB', se setean APROBADO_POR y APROBADO_FECHA = now.
    - Si ZONA_ID se envía, se setean ASIGNADO_POR y ASIGNADO_FECHA = now.
    - RETIRA_CLIENTE se guarda como bit (0/1).
    """
    now = datetime.now()

    sets = []
    cols = []
    params: Dict[str, Any] = {"n": int(num_nota), "UPDATED_AT": now}

    if ESTADO is not None:
        sets.append("ESTADO = :ESTADO")
        cols.append("ESTADO")
        params["ESTADO"] = ESTADO
        if ESTADO == "APROB":
            sets += ["APROBADO_POR = :APROBADO_POR", "APROBADO_FECHA = :APROBADO_FECHA"]
            cols += ["APROBADO_POR", "APROBADO_FECHA"]
            params["APROBADO_POR"] = APROBADO_POR
            params["APROBADO_FECHA"] = now

    if ZONA_ID is not None:
        sets.append("ZONA_ID = :ZONA_ID")
        cols.append("ZONA_ID")
        params["ZONA_ID"] = int(ZONA_ID)
        sets += ["ASIGNADO_POR = :ASIGNADO_POR", "ASIGNADO_FECHA = :ASIGNADO_FECHA"]
        cols += ["ASIGNADO_POR", "ASIGNADO_FECHA"]
        params["ASIGNADO_POR"] = ASIGNADO_POR
        params["ASIGNADO_FECHA"] = now

    if RETIRA_CLIENTE is not None:
        sets.append("RETIRA_CLIENTE = :RETIRA_CLIENTE")
        cols.append("RETIRA_CLIENTE")
        params["RETIRA_CLIENTE"] = int(bool(RETIRA_CLIENTE))

    # UPDATED_AT siempre
    sets.append("UPDATED_AT = :UPDATED_AT")
    if "UPDATED_AT" not in cols:
        cols.append("UPDATED_AT")

    merge_sql = text(f"""
        MERGE RIMA.dbo.NV_FLOW AS tgt
        USING (SELECT :n AS NUMNOTA) AS src
        ON (tgt.NUMNOTA = src.NUMNOTA)
        WHEN MATCHED THEN
          UPDATE SET {", ".join(sets)}
        WHEN NOT MATCHED THEN
          INSERT (NUMNOTA, {", ".join(cols)})
          VALUES (:n, {", ".join(":"+c for c in cols)});
    """)
    with _ENGINE_RIMA.begin() as cx:
        cx.execute(merge_sql, params)


# Helpers convenientes (envoltorios claros)

def set_nv_estado_aprobada(num_nota: int, aprobado_por: Optional[str] = None) -> None:
    upsert_nv_flow(num_nota, ESTADO="APROB", APROBADO_POR=aprobado_por)

def set_nv_estado_pendiente(num_nota: int) -> None:
    upsert_nv_flow(num_nota, ESTADO="PEND")

def set_nv_zona(num_nota: int, zona_id: int, asignado_por: Optional[str] = None) -> None:
    upsert_nv_flow(num_nota, ZONA_ID=zona_id, ASIGNADO_POR=asignado_por)

def set_nv_retira(num_nota: int, retira: bool) -> None:
    upsert_nv_flow(num_nota, RETIRA_CLIENTE=retira)


__all__ = [
    # re-exports legacy
    "ENGINE",
    "query_df",
    "get_oc_detalle",
    "get_nota_detalle",
    "get_stock_actual",
    "get_guia_desde_nv",
    "get_factura_desde_nv",
    # zonas
    "get_zonas",
    # nv_flow helpers
    "get_nv_flow",
    "upsert_nv_flow",
    "set_nv_estado_aprobada",
    "set_nv_estado_pendiente",
    "set_nv_zona",
    "set_nv_retira",
]
