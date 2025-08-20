"""Deprecated compatibility layer. Use `db` module instead."""

from db import (
    ENGINE,
    query_df,
    get_oc_detalle,
    get_nota_detalle,
    get_stock_actual,
    get_guia_desde_nv,
    get_factura_desde_nv,
)

__all__ = [
    "ENGINE",
    "query_df",
    "get_oc_detalle",
    "get_nota_detalle",
    "get_stock_actual",
    "get_guia_desde_nv",
    "get_factura_desde_nv",
]
