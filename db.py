# db.py
# -*- coding: utf-8 -*-
import os
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from dotenv import load_dotenv
load_dotenv(override=True)  # fuerza que se usen las credenciales del .env

# =========================================================
# Cargar .env ANTES de usar os.getenv
# =========================================================
load_dotenv()

# -------------------- Utils --------------------
def _env_bool(value: str | None) -> bool:
    if value is None:
        return False
    v = value.strip().lower()
    if v in {"yes", "true", "1"}:
        return True
    if v in {"no", "false", "0"}:
        return False
    return True

def _build_pyodbc_engine(database: str) -> "Engine":
    """
    Construye un engine mssql+pyodbc con los parámetros DB_* del .env
    hacia la base indicada en `database`.
    """
    DRIVER    = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
    SERVER    = os.getenv("DB_SERVER", "localhost")
    AUTH      = (os.getenv("DB_AUTH", "sql") or "sql").strip().lower()
    TRUSTCERT = _env_bool(os.getenv("DB_TRUST_CERT", "yes"))

    if AUTH == "windows":
        odbc = (
            f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={database};"
            f"Trusted_Connection=yes;Encrypt=yes;"
            f"TrustServerCertificate={'yes' if TRUSTCERT else 'no'};"
        )
    else:
        USER = os.getenv("DB_USER", "")
        PWD  = os.getenv("DB_PASSWORD", "")
        odbc = (
            f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={database};"
            f"UID={USER};PWD={PWD};Encrypt=yes;"
            f"TrustServerCertificate={'yes' if TRUSTCERT else 'no'};"
        )

    params = urllib.parse.quote_plus(odbc)
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        pool_pre_ping=True,
        future=True,
        fast_executemany=True,
    )

# =========================================================
# ENGINE principal (Santiago / general)
# =========================================================
_USERS_DB_URL = (os.getenv("USERS_DB_URL") or "").strip()
ENGINE = None

if _USERS_DB_URL:
    # Intentar con URL completa
    try:
        ENGINE = create_engine(
            _USERS_DB_URL,
            pool_pre_ping=True,
            future=True,
            fast_executemany=True,
        )
    except Exception:
        # Fallback: ODBC con DB_* hacia la base indicada en DB_DATABASE
        DB_DATABASE = os.getenv("DB_DATABASE", "")
        ENGINE = _build_pyodbc_engine(DB_DATABASE)
else:
    # No hay USERS_DB_URL => construir ODBC con DB_*
    DB_DATABASE = os.getenv("DB_DATABASE", "")
    ENGINE = _build_pyodbc_engine(DB_DATABASE)

# =========================================================
# ENGINE RIMA (para ZONAS_DB y demás)
# =========================================================
RIMA_ENGINE = None
_RIMA_URL = (os.getenv("RIMA_DB_URL") or "").strip()
if _RIMA_URL:
    try:
        RIMA_ENGINE = create_engine(
            _RIMA_URL,
            pool_pre_ping=True,
            future=True,
            fast_executemany=True,
        )
    except Exception:
        # Fallback robusto si la URL tiene '@tcp:' y comas en el host.
        RIMA_ENGINE = _build_pyodbc_engine(os.getenv("RIMA_DB_DATABASE", "RIMA"))
else:
    # Si no hay RIMA_DB_URL, construir con DB_* apuntando a la base RIMA
    RIMA_ENGINE = _build_pyodbc_engine(os.getenv("RIMA_DB_DATABASE", "RIMA"))

# -------------------- Helpers de ejecución --------------------
def execute(sql: str, params: dict | None = None) -> None:
    """Ejecuta SQL (DML/DDL) en la BD principal."""
    with ENGINE.begin() as conn:
        conn.execute(text(sql), params or {})

def query_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    """Lee datos de la BD principal como DataFrame."""
    with ENGINE.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

def execute_rima(sql: str, params: dict | None = None) -> None:
    """Ejecuta SQL (DML/DDL) en la BD RIMA."""
    with RIMA_ENGINE.begin() as conn:
        conn.execute(text(sql), params or {})

def query_df_rima(sql: str, params: dict | None = None) -> pd.DataFrame:
    """Lee datos de la BD RIMA como DataFrame."""
    with RIMA_ENGINE.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

# ==================== Repositorio para /ingreso ====================
def get_oc_detalle_por_oc(num_oc: str) -> pd.DataFrame:
    """
    Devuelve líneas de la OC desde OCDET_DB.
    Campos clave confirmados: CANTIDAD, CANTRECI, CANTFAC, BODEGA, CENTCC, ITEM.
    """
    sql = """
    SELECT
        d.ITEM,
        d.CANTIDAD,
        d.CANTRECI,
        d.CANTFAC,
        d.BODEGA,
        d.CENTCC
    FROM OCDET_DB d
    WHERE d.NUMORDEN = :num_oc
    ORDER BY d.ITEM
    """
    return query_df(sql, {"num_oc": num_oc})

def get_art_por_codigos2(codigos2: list[str]) -> pd.DataFrame:
    """Trae datos de ART_DB por CODIGO2 (código visible en la UI)."""
    if not codigos2:
        return pd.DataFrame(columns=["CODIGO2","NREGUIST","CODIGO","NOMBRE","NOMBRE2","PRECVTA"])
    binds = ",".join([f":c{i}" for i in range(len(codigos2))])
    sql = f"""
    SELECT a.CODIGO2, a.NREGUIST, a.CODIGO, a.NOMBRE, a.NOMBRE2, a.PRECVTA
    FROM ART_DB a
    WHERE a.CODIGO2 IN ({binds})
    """
    params = {f"c{i}": v for i, v in enumerate(codigos2)}
    return query_df(sql, params)

def get_docu_por_numorden(num_oc: str) -> pd.DataFrame:
    """Cabecera de documentos por NUMORDEN (DOCU_DB)."""
    sql = "SELECT * FROM DOCU_DB WHERE NUMORDEN = :num_oc"
    return query_df(sql, {"num_oc": num_oc})

def get_numguia_por_numorden(num_oc: str) -> str | None:
    df = query_df("SELECT TOP 1 NUMGUIAF FROM DOCU_DB WHERE NUMORDEN = :num_oc", {"num_oc": num_oc})
    return (df["NUMGUIAF"].iloc[0] if not df.empty and "NUMGUIAF" in df.columns else None)

def get_oc_items(num_oc: str) -> tuple[pd.DataFrame, str | None]:
    """
    Obtiene líneas de una OC directamente desde la base de datos.
    Devuelve DataFrame: Código, Nombre, Cantidad, Prec.Unit.; y la guía asociada (si existe).
    """
    sql = """
        SELECT
            a.CODIGO2,
            a.NOMBRE,
            d.CANTIDAD,
            d.RPECUNIT
        FROM DOCDE_DB d
        JOIN ART_DB a ON a.CODIGO = d.CODIGO
        WHERE d.NUMORDEN = :num_oc
        ORDER BY d.ITEM
    """
    df = query_df(sql, {"num_oc": num_oc})
    if not df.empty:
        df = df.rename(columns={
            "CODIGO2": "Código",
            "NOMBRE": "Nombre",
            "CANTIDAD": "Cantidad",
            "RPECUNIT": "Prec.Unit."
        })
    num_guia = get_numguia_por_numorden(num_oc)
    return df, num_guia

# ==================== Migrado desde db_utils.py ====================
def get_oc_detalle(num_oc: str) -> list[dict]:
    """Obtiene el detalle de una OC como lista de diccionarios."""
    sql = """
        SELECT
            h.NUMORDEN       AS num_orden,
            h.NUMGUIAF       AS num_guia,
            a.CODIGO2        AS codigo,
            a.NOMBRE         AS nombre,
            d.CANTIDAD       AS cantidad,
            d.PRECUNIT       AS prec_unit
        FROM DOCU_DB  h
        JOIN DOCDE_DB d  ON d.NUMRECOR = h.PGNUMRECOR
        LEFT JOIN ART_DB a ON a.NREGUIST = d.NCODART
        WHERE h.NUMORDEN = :num_oc
        ORDER BY d.ITEM
    """
    df = query_df(sql, {"num_oc": num_oc})
    if not df.empty:
        df["cantidad"] = pd.to_numeric(df["cantidad"], errors="coerce").fillna(0).astype(int)
        df["prec_unit"] = pd.to_numeric(df["prec_unit"], errors="coerce").fillna(0.0)
    return df.to_dict(orient="records")

def get_nota_detalle(num_nota: str) -> pd.DataFrame:
    """Trae detalle de NV desde la BBDD."""
    sql = """
        SELECT
            nv.NUMNOTA                                             AS num_nota,
            COALESCE(art.CODIGO2, CAST(nd.NCODART AS VARCHAR(50))) AS codigo,
            nd.DESCRIP                                             AS nombre,
            (nd.CANTIDAD - COALESCE(nd.CANTDESP, 0))               AS cantidad,
            nd.PRECUNIT                                            AS prec_unit
        FROM dbo.NOTV_DB  AS nv
        JOIN dbo.NOTDE_DB AS nd
            ON nd.NUMRECOR = nv.NUMREG
        LEFT JOIN dbo.ART_DB AS art
            ON art.NREGUIST = nd.NCODART
        WHERE nv.NUMNOTA = :num_nota
        ORDER BY nd.ITEM
    """
    df = query_df(sql, {"num_nota": num_nota})
    if not df.empty:
        df["cantidad"] = pd.to_numeric(df["cantidad"], errors="coerce").fillna(0).astype(int)
        df["prec_unit"] = pd.to_numeric(df["prec_unit"], errors="coerce").fillna(0.0)
    return df

def get_stock_actual() -> pd.DataFrame:
    """Obtiene el stock físico de los productos desde la BBDD."""
    sql = """
        SELECT
            art.CODIGO2    AS codigo,
            art.NOMBRE     AS nombre,
            stk.STK_FISICO AS cantidad
        FROM dbo.STOCK_DB AS stk
        JOIN dbo.ART_DB   AS art
            ON art.NREGUIST = stk.ARTICULO
    """
    df = query_df(sql)
    if not df.empty:
        df["cantidad"] = pd.to_numeric(df["cantidad"], errors="coerce").fillna(0).astype(int)
    return df

def get_guia_desde_nv(num_nota: str) -> tuple[dict, list[dict]]:
    """Retorna (header, detalles) para prellenar la Guía de Despacho."""
    header_sql = """
        SELECT
            CAST(NULL AS VARCHAR(50))       AS GD_NUM,
            nv.NUMORDC                      AS OC_NUM,
            nv.RUTFACT                      AS FAV_RUT,
            c.RAZSOC                        AS FAV_RAZSOC,
            nv.NRUTCLIE                     AS DESP_A,
            p.CODIGO                        AS VEND_CODIGO,
            CONCAT(p.NOMBRE, ' ', p.APELLIDO) AS VEND_NOMBRE,
            c.DIR                           AS ENTREGAR_EN,
            nv.COMISION                     AS COMISION,
            nv.SUCUR                        AS SUCURSAL,
            nv.GLOSACON                     AS GLOSA_PAG,
            CASE WHEN MIN(COALESCE(nd.DESCTO,0)) = MAX(COALESCE(nd.DESCTO,0))
                 THEN MIN(COALESCE(nd.DESCTO,0))
                 ELSE NULL
            END                             AS DESCUENTO_UNICO_LINEAS
        FROM dbo.NOTV_DB  nv
        LEFT JOIN dbo.CLIEN_DB c  ON c.NREGUIST = nv.NRUTCLIE
        LEFT JOIN dbo.PERSO_DB p  ON p.NUMREG   = nv.CODVEND
        JOIN dbo.NOTDE_DB nd ON nd.NUMRECOR = nv.NUMREG
        WHERE nv.NUMNOTA = :num_nota
        GROUP BY nv.NUMORDC, nv.RUTFACT, c.RAZSOC, nv.NRUTCLIE, p.CODIGO, p.NOMBRE, p.APELLIDO, c.DIR, nv.COMISION, nv.SUCUR, nv.GLOSACON
    """
    detail_sql = """
        SELECT
            COALESCE(art.CODIGO2, CAST(nd.NCODART AS VARCHAR(50))) AS Codigo,
            nd.DESCRIP                                             AS Descripcion,
            (COALESCE(nd.CANTIDAD,0) - COALESCE(nd.CANTDESP,0))    AS Cantidad,
            nd.PRECUNIT                                            AS Precio,
            COALESCE(nd.DESCTO,0)                                  AS [D%],
            nd.ITEM                                                AS Item
        FROM dbo.NOTV_DB  nv
        JOIN dbo.NOTDE_DB nd   ON nd.NUMRECOR = nv.NUMREG
        LEFT JOIN dbo.ART_DB art ON art.NREGUIST = nd.NCODART
        WHERE nv.NUMNOTA = :num_nota
        ORDER BY nd.ITEM
    """
    with ENGINE.begin() as conn:
        df_h = pd.read_sql(text(header_sql), conn, params={"num_nota": num_nota})
        df_d = pd.read_sql(text(detail_sql), conn,  params={"num_nota": num_nota})
    header = df_h.iloc[0].to_dict() if not df_h.empty else {}
    detalles = df_d.to_dict(orient="records")
    return header, detalles

def get_factura_desde_nv(num_nota: str) -> dict:
    """Obtiene datos para prellenar la Factura de Venta desde una Nota de Venta."""
    sql = """
        SELECT
            nv.RUTFAC                       AS FAV_A,
            p.CODIGO                        AS VEND_CODIGO,
            CONCAT(p.NOMBRE, ' ', p.APELLIDO) AS VEND_NOMBRE
        FROM dbo.NOTV_DB nv
        LEFT JOIN dbo.PERSO_DB p ON p.NUMREG = nv.CODVEND
        WHERE nv.NUMNOTA = :num_nota
    """
    df = query_df(sql, {"num_nota": num_nota})
    return df.iloc[0].to_dict() if not df.empty else {}

# Alias retrocompatible por si tenías este nombre en otros módulos
def execute_main(sql: str, params: dict | None = None) -> None:
    execute(sql, params)

print("USERS_DB_URL en uso:", os.getenv("USERS_DB_URL"))
print("RIMA_DB_URL en uso:", os.getenv("RIMA_DB_URL"))
