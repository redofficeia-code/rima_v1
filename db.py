# db.py
# -*- coding: utf-8 -*-
import os
import urllib.parse
import pandas as pd
from datetime import date
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from typing import Dict, Any, List, Optional, Tuple
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine  # ⬅️ nuevo
from contextlib import contextmanager  # ⬅️ nuevo


# =========================================================
# Cargar .env ANTES de usar os.getenv
# =========================================================
load_dotenv(override=True)  # fuerza que se usen las credenciales del .env

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

@contextmanager
def get_conn(engine: Engine | None = None):
    """
    Context manager de conexión transaccional.
    Uso:
        with get_conn() as conn:
            conn.execute(text("..."), {...})
    """
    eng = engine or ENGINE
    with eng.begin() as conn:
        yield conn

def exec_sp(sp_name: str, params: dict | None = None, engine: Engine | None = None):
    """
    Ejecuta un procedimiento almacenado con parámetros con nombre.
    Ej:
        exec_sp("dbo.usp_gd_build_from_nv", {"GD_NUMREG": 3669936, "NV_NUMREG": 3076976})
    """
    eng = engine or ENGINE
    params = params or {}
    placeholders = ", ".join([f"@{k}=:{k}" for k in params.keys()])
    sql = text(f"EXEC {sp_name} {placeholders}")
    with eng.begin() as conn:
        res = conn.execute(sql, params)
        if res.returns_rows:
            return [dict(r) for r in res.mappings().all()]
        return None

_SQL_CREATE_CICLO_INDEXES = text("""
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_DOCDE_SQNVDET' AND object_id = OBJECT_ID('dbo.DOCDE_DB'))
    CREATE NONCLUSTERED INDEX IX_DOCDE_SQNVDET  ON dbo.DOCDE_DB(SQNVDET) INCLUDE(NUMRECOR, ITEM, SQGDDET, SQOCDET, TIME_DET);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_NOTDE_SEQNVDET' AND object_id = OBJECT_ID('dbo.NOTDE_DB'))
    CREATE NONCLUSTERED INDEX IX_NOTDE_SEQNVDET ON dbo.NOTDE_DB(SEQNVDET) INCLUDE(NUMRECOR, ITEM);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_DOCDE_NUMRECOR' AND object_id = OBJECT_ID('dbo.DOCDE_DB'))
    CREATE NONCLUSTERED INDEX IX_DOCDE_NUMRECOR ON dbo.DOCDE_DB(NUMRECOR) INCLUDE(ITEM, SQNVDET, SQGDDET, SQOCDET);
""")

def apply_ciclo_indexes(engine: Engine | None = None):
    """Crea (si no existen) los índices que usa el Ciclo. Idempotente."""
    eng = engine or ENGINE
    with eng.begin() as conn:
        conn.execute(_SQL_CREATE_CICLO_INDEXES)


def _build_pyodbc_engine(database: str) -> "Engine":
    """
    Construye un engine mssql+pyodbc con los parámetros DB_* del .env
    hacia la base indicada en `database`.
    """
    DRIVER = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
    SERVER = os.getenv("DB_SERVER", "localhost")
    AUTH = (os.getenv("DB_AUTH", "sql") or "sql").strip().lower()
    TRUSTCERT = _env_bool(os.getenv("DB_TRUST_CERT", "yes"))

    if AUTH == "windows":
        odbc = (
            f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={database};"
            f"Trusted_Connection=yes;Encrypt=yes;"
            f"TrustServerCertificate={'yes' if TRUSTCERT else 'no'};"
        )
    else:
        USER = os.getenv("DB_USER", "")
        PWD = os.getenv("DB_PASSWORD", "")
        odbc = (
            f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={database};"
            f"UID={USER};PWD={PWD};Encrypt=yes;"
            f"TrustServerCertificate={'yes' if TRUSTCERT else 'no'};"
        )

    params = urllib.parse.quote_plus(odbc)
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        pool_size=20,
        max_overflow=40,
        pool_recycle=1800,
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
    try:
        ENGINE = create_engine(
            _USERS_DB_URL,
            pool_size=20,
            max_overflow=40,
            pool_recycle=1800,
            pool_pre_ping=True,
            future=True,
            fast_executemany=True,
        )
    except Exception:
        DB_DATABASE = os.getenv("DB_DATABASE", "")
        ENGINE = _build_pyodbc_engine(DB_DATABASE)
else:
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
            pool_size=20,
            max_overflow=40,
            pool_recycle=1800,
            pool_pre_ping=True,
            future=True,
            fast_executemany=True,
        )
    except Exception:
        RIMA_ENGINE = _build_pyodbc_engine(os.getenv("RIMA_DB_DATABASE", "RIMA"))
else:
    RIMA_ENGINE = _build_pyodbc_engine(os.getenv("RIMA_DB_DATABASE", "RIMA"))

# -------------------- Helpers de ejecución --------------------
def execute(sql: str, params: dict | None = None) -> None:
    with ENGINE.begin() as conn:
        conn.execute(text(sql), params or {})


def query_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    with ENGINE.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


def execute_rima(sql: str, params: dict | None = None) -> None:
    with RIMA_ENGINE.begin() as conn:
        conn.execute(text(sql), params or {})


def query_df_rima(sql: str, params: dict | None = None) -> pd.DataFrame:
    with RIMA_ENGINE.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

_IDENTITY_COLS_SQL = text(
    """
SELECT name AS COLUMN_NAME
FROM sys.identity_columns
WHERE object_id = OBJECT_ID('dbo.DOCDE_DB')
"""
)


def _docde_identity_cols(conn) -> set[str]:
    rows = conn.execute(_IDENTITY_COLS_SQL).fetchall()
    return {r.COLUMN_NAME for r in rows}


_NOTNULL_COLS_SQL = text(
    """
SELECT c.COLUMN_NAME, c.DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS c
LEFT JOIN sys.columns sc
  ON sc.object_id = OBJECT_ID('dbo.DOCDE_DB')
 AND sc.name = c.COLUMN_NAME
LEFT JOIN sys.default_constraints dc
  ON dc.parent_object_id = sc.object_id
 AND dc.parent_column_id = sc.column_id
WHERE c.TABLE_SCHEMA = 'dbo'
  AND c.TABLE_NAME  = 'DOCDE_DB'
  AND c.IS_NULLABLE = 'NO'
  AND dc.name IS NULL
"""
)


def _safe_default(col: str, dtype: str, idx: int, header_fecha: date | None = None):
    if col in {"LIQDDO"}:
        return 0
    if col in {"TJ_SORT"}:
        return idx + 1
    if col in {"TJ_SUBSORT"}:
        return 0
    if col in {"SQNVDET"}:
        return idx + 1
    if col in {"SQGDDET"}:
        return idx + 1
    if col in {"SQOCDET"}:
        return idx + 1
    if col in {"NRO_ROL"}:
        return 0
    if col in {"LOT_IDE"}:
        return 0
    if col in {"DESCRIP"}:
        return ""
    dtype = (dtype or "").lower()
    if dtype in {
        "int",
        "bigint",
        "smallint",
        "tinyint",
        "decimal",
        "numeric",
        "float",
        "real",
        "money",
        "smallmoney",
        "bit",
    }:
        return 0
    if dtype in {"date", "datetime", "smalldatetime", "datetime2"}:
        return header_fecha or date.today()
    return ""


def _docde_notnull_cols(conn) -> dict[str, str]:
    rows = conn.execute(_NOTNULL_COLS_SQL).fetchall()
    return {r.COLUMN_NAME: r.DATA_TYPE for r in rows}

# Todas las columnas reales existentes en DOCDE_DB
_ALL_DOCDE_COLS_SQL = text(
    """
SELECT c.COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_SCHEMA = 'dbo' AND c.TABLE_NAME = 'DOCDE_DB'
"""
)


def _docde_all_cols(conn) -> set[str]:
    rows = conn.execute(_ALL_DOCDE_COLS_SQL).fetchall()
    return {r.COLUMN_NAME for r in rows}


def _build_docde_insert_sql(
    extra_cols: list[str],
    skip_cols: set[str] | None = None,
) -> Tuple[Any, List[str]]:
    skip_cols = skip_cols or set()
    base_cols = [
        "NUMRECOR",
        "ITEM",
        "NCODART",
        "DESCRIP",
        "CANTIDAD",
        "PRECUNIT",
        "DESCTO",
        "BODEGA",
        "NSERIE",
        "CENCOSTO",
        "CTACTBLE",
        "TAX",
        "TIP_TAX",
        "SQNVDET",
        "SQGDDET",
        "SQOCDET",
        "LIQDDO",
        "TJ_SORT",
        "TJ_SUBSORT",
        "NRO_ROL",
        "LOT_IDE",
    ]
    # quita cualquier columna a saltar (p.ej. IDENTITY)
    base_cols = [c for c in base_cols if c not in skip_cols]
    extras = [c for c in extra_cols if c not in skip_cols and c not in base_cols]
    cols = base_cols + extras
    placeholders = [f":{c.lower()}" for c in cols]
    sql = f"INSERT INTO dbo.DOCDE_DB ({', '.join(cols)}) VALUES ({', '.join(placeholders)})"
    return text(sql), cols


# ==================== Funciones varias ====================

def get_oc_detalle_por_oc(num_oc: str) -> pd.DataFrame:
    sql = """
    SELECT d.ITEM, d.CANTIDAD, d.CANTRECI, d.CANTFAC, d.BODEGA, d.CENTCC
    FROM OCDET_DB d
    WHERE d.NUMORDEN = :num_oc
    ORDER BY d.ITEM
    """
    return query_df(sql, {"num_oc": num_oc})


def get_art_por_codigos2(codigos2: list[str]) -> pd.DataFrame:
    if not codigos2:
        return pd.DataFrame(
            columns=["CODIGO2", "NREGUIST", "CODIGO", "NOMBRE", "NOMBRE2", "PRECVTA"]
        )
    binds = ",".join([f":c{i}" for i in range(len(codigos2))])
    sql = f"""
    SELECT a.CODIGO2, a.NREGUIST, a.CODIGO, a.NOMBRE, a.NOMBRE2, a.PRECVTA
    FROM ART_DB a
    WHERE a.CODIGO2 IN ({binds})
    """
    params = {f"c{i}": v for i, v in enumerate(codigos2)}
    return query_df(sql, params)


def get_docu_por_numorden(num_oc: str) -> pd.DataFrame:
    return query_df("SELECT * FROM DOCU_DB WHERE NUMORDEN = :num_oc", {"num_oc": num_oc})


def get_numguia_por_numorden(num_oc: str) -> str | None:
    # Correlativo visible de Guía = NUMFACT (TIPODOC=2)
    df = query_df(
        "SELECT TOP 1 NUMFACT FROM DOCU_DB WHERE NUMORDEN = :num_oc AND TIPODOC = 2 ORDER BY NUMREG DESC",
        {"num_oc": num_oc},
    )
    return str(df["NUMFACT"].iloc[0]) if not df.empty else None


def get_oc_items(num_oc: str) -> Tuple[pd.DataFrame, Optional[str]]:
    sql = """
        SELECT a.CODIGO2, a.NOMBRE, d.CANTIDAD, d.RPECUNIT
        FROM DOCDE_DB d
        JOIN ART_DB a ON a.CODIGO = d.CODIGO
        WHERE d.NUMORDEN = :num_oc
        ORDER BY d.ITEM
    """
    df = query_df(sql, {"num_oc": num_oc})
    if not df.empty:
        df = df.rename(
            columns={
                "CODIGO2": "Código",
                "NOMBRE": "Nombre",
                "CANTIDAD": "Cantidad",
                "RPECUNIT": "Prec.Unit.",
            }
        )
    num_guia = get_numguia_por_numorden(num_oc)
    return df, num_guia


def get_oc_detalle(num_oc: str) -> List[dict]:
    sql = """
        SELECT h.NUMORDEN AS num_orden, h.NUMFACT AS num_guia,
               a.CODIGO2 AS codigo, a.NOMBRE AS nombre,
               d.CANTIDAD AS cantidad, d.PRECUNIT AS prec_unit
        FROM DOCU_DB  h
        JOIN DOCDE_DB d  ON d.NUMRECOR = h.PGNUMRECOR
        LEFT JOIN ART_DB a ON a.NREGUIST = d.NCODART
        WHERE h.NUMORDEN = :num_oc AND h.TIPODOC = 2
        ORDER BY d.ITEM
    """
    df = query_df(sql, {"num_oc": num_oc})
    if not df.empty:
        df["cantidad"] = pd.to_numeric(df["cantidad"], errors="coerce").fillna(0).astype(int)
        df["prec_unit"] = pd.to_numeric(df["prec_unit"], errors="coerce").fillna(0.0)
    return df.to_dict(orient="records")


def get_nota_detalle(num_nota: str) -> pd.DataFrame:
    sql = """
        SELECT nv.NUMNOTA AS num_nota,
               COALESCE(art.CODIGO2, CAST(nd.NCODART AS VARCHAR(50))) AS codigo,
               nd.DESCRIP AS nombre,
               (nd.CANTIDAD - COALESCE(nd.CANTDESP, 0)) AS cantidad,
               nd.PRECUNIT AS prec_unit
        FROM dbo.NOTV_DB nv
        JOIN dbo.NOTDE_DB nd ON nd.NUMRECOR = nv.NUMREG
        LEFT JOIN dbo.ART_DB art ON art.NREGUIST = nd.NCODART
        WHERE nv.NUMNOTA = :num_nota
        ORDER BY nd.ITEM
    """
    df = query_df(sql, {"num_nota": num_nota})
    if not df.empty:
        df["cantidad"] = pd.to_numeric(df["cantidad"], errors="coerce").fillna(0).astype(int)
        df["prec_unit"] = pd.to_numeric(df["prec_unit"], errors="coerce").fillna(0.0)
    return df


def get_stock_actual() -> pd.DataFrame:
    sql = """
        SELECT art.CODIGO2 AS codigo, art.NOMBRE AS nombre, stk.STK_FISICO AS cantidad
        FROM dbo.STOCK_DB stk
        JOIN dbo.ART_DB art ON art.NREGUIST = stk.ARTICULO
    """
    df = query_df(sql)
    if not df.empty:
        df["cantidad"] = pd.to_numeric(df["cantidad"], errors="coerce").fillna(0).astype(int)
    return df


def get_stock_por_codigos(codigos: List[str]) -> pd.DataFrame:
    codigos = [str(c).strip() for c in (codigos or []) if str(c or "").strip()]
    if not codigos:
        return pd.DataFrame(columns=["codigo", "nombre", "cantidad"])
    ph = ", ".join([f":c{i}" for i in range(len(codigos))])
    params = {f"c{i}": codigos[i] for i in range(len(codigos))}
    sql = f"""
        SELECT art.CODIGO2 AS codigo, art.NOMBRE AS nombre,
               SUM(COALESCE(stk.STK_FISICO, 0)) AS cantidad
        FROM dbo.ART_DB art
        JOIN dbo.STOCK_DB stk ON stk.ARTICULO = art.NREGUIST
        WHERE art.CODIGO2 IN ({ph})
        GROUP BY art.CODIGO2, art.NOMBRE
    """
    df = query_df(sql, params)
    if df is not None and not df.empty:
        df["cantidad"] = pd.to_numeric(df["cantidad"], errors="coerce").fillna(0).astype(int)
        df["codigo"] = (
            df["codigo"].astype(str).str.trim()
            if hasattr(str, "trim")
            else df["codigo"].astype(str).str.strip()
        )
        df["nombre"] = df["nombre"].astype(str).str.strip()
        return df[["codigo", "nombre", "cantidad"]]
    return pd.DataFrame(columns=["codigo", "nombre", "cantidad"])


def get_guia_desde_nv(num_nota: str) -> Tuple[dict, List[dict]]:
    header_sql = """
        SELECT CAST(NULL AS VARCHAR(50)) AS GD_NUM,
               nv.NUMORDC AS OC_NUM, nv.RUTFACT AS FAV_RUT, c.RAZSOC AS FAV_RAZSOC,
               nv.NRUTCLIE AS DESP_A, p.CODIGO AS VEND_CODIGO,
               CONCAT(p.NOMBRE, ' ', p.APELLIDO) AS VEND_NOMBRE,
               c.DIR AS ENTREGAR_EN, nv.COMISION AS COMISION,
               nv.SUCUR AS SUCURSAL, nv.GLOSACON AS GLOSA_PAG,
               CASE WHEN MIN(COALESCE(nd.DESCTO,0)) = MAX(COALESCE(nd.DESCTO,0))
                    THEN MIN(COALESCE(nd.DESCTO,0)) ELSE NULL END AS DESCUENTO_UNICO_LINEAS
        FROM dbo.NOTV_DB nv
        LEFT JOIN dbo.CLIEN_DB c ON c.NREGUIST = nv.NRUTCLIE
        LEFT JOIN dbo.PERSO_DB p ON p.NUMREG = nv.CODVEND
        JOIN dbo.NOTDE_DB nd ON nd.NUMRECOR = nv.NUMREG
        WHERE nv.NUMNOTA = :num_nota
        GROUP BY nv.NUMORDC, nv.RUTFACT, c.RAZSOC, nv.NRUTCLIE, p.CODIGO, p.NOMBRE, p.APELLIDO,
                 c.DIR, nv.COMISION, nv.SUCUR, nv.GLOSACON
    """

    detail_sql = """
        SELECT 
            nd.NCODART AS NCODART,
            COALESCE(art.CODIGO2, CAST(nd.NCODART AS VARCHAR(50)))           AS Codigo,

            -- Descripción: primero la del detalle; si viene vacía usa ART_DB.NOMBRE, luego NOMBRE2
            CAST(
                COALESCE(
                    NULLIF(LTRIM(RTRIM(CAST(nd.DESCRIP AS NVARCHAR(4000)))), N''),
                    CAST(art.NOMBRE  AS NVARCHAR(4000)),
                    CAST(art.NOMBRE2 AS NVARCHAR(4000)),
                    N''
                ) AS NVARCHAR(4000)
            )                                                                AS Descripcion,

            -- Alias extra por compatibilidad con plantillas que buscan 'Nombre'
            CAST(
                COALESCE(
                    NULLIF(LTRIM(RTRIM(CAST(nd.DESCRIP AS NVARCHAR(4000)))), N''),
                    CAST(art.NOMBRE  AS NVARCHAR(4000)),
                    CAST(art.NOMBRE2 AS NVARCHAR(4000)),
                    N''
                ) AS NVARCHAR(4000)
            )                                                                AS Nombre,

            (COALESCE(nd.CANTIDAD,0) - COALESCE(nd.CANTDESP,0))              AS Cantidad,

            -- Precio unitario con alias compatibles
            nd.PRECUNIT                                                      AS Precio,
            nd.PRECUNIT                                                      AS [Prec.Unit],
            nd.PRECUNIT                                                      AS PRECUNIT,

            COALESCE(nd.DESCTO,0)                                            AS [D%],
            nd.ITEM                                                          AS Item
        FROM dbo.NOTV_DB nv
        JOIN dbo.NOTDE_DB nd ON nd.NUMRECOR = nv.NUMREG
        LEFT JOIN dbo.ART_DB art ON art.NREGUIST = nd.NCODART
        WHERE nv.NUMNOTA = :num_nota
        ORDER BY nd.ITEM
    """

    with ENGINE.begin() as conn:
        df_h = pd.read_sql(text(header_sql), conn, params={"num_nota": num_nota})
        df_d = pd.read_sql(text(detail_sql), conn, params={"num_nota": num_nota})

    header = df_h.iloc[0].to_dict() if not df_h.empty else {}
    detalles = df_d.to_dict(orient="records")
    return header, detalles


# ==================== Encabezado para Factura ====================

def get_fav_header(num_nota: int | str) -> pd.DataFrame:
    """
    Campos confirmados para Factura:
    - FAV A: RUTFACT + RAZSOC (si es convertible a int)
    - Cliente (despacho): NRUTCLIE + RAZSOC
    - Glosa Pago
    - Vendedor
    """
    sql = """
        SELECT TOP (1)
            nv.NUMNOTA AS FAV_NRO,
            LTRIM(RTRIM(nv.RUTFACT)) AS FAV_RUT,
            ISNULL(cf.RAZSOC, '') AS FAV_RAZSOC,
            nv.NRUTCLIE AS CLI_RUT,
            ISNULL(cc.RAZSOC, '') AS CLI_RAZSOC,
            ISNULL(nv.GLOSACON, '') AS GLOSA_PAGO,
            nv.CODVEND,
            ISNULL(p.NOMBRE, '') AS VENDEDOR
        FROM dbo.NOTV_DB nv
        LEFT JOIN dbo.CLIEN_DB cf
               ON cf.NREGUIST = CASE
                                   WHEN ISNUMERIC(REPLACE(LTRIM(RTRIM(nv.RUTFACT)),'-','')) = 1
                                        THEN CONVERT(INT, REPLACE(LTRIM(RTRIM(nv.RUTFACT)),'-',''))
                                   ELSE NULL
                                END
        LEFT JOIN dbo.CLIEN_DB cc ON cc.NREGUIST = nv.NRUTCLIE
        LEFT JOIN dbo.PERSO_DB p ON p.NUMREG = nv.CODVEND
        WHERE nv.NUMNOTA = :num_nota;
    """
    try:
        num_nota_int = int(num_nota)
    except (ValueError, TypeError):
        num_nota_int = int(float(str(num_nota)))
    return query_df(sql, {"num_nota": num_nota_int})


def get_nv_observaciones(num_nota: int | str):
    """
    Devuelve las 2 observaciones que se heredan a la Guía:
      - OBSDESP  -> Observaciones para la Guía de Despacho
      - OBSFACT  -> Observaciones para la Factura de Venta
    """
    SQL = text(
        """
    SELECT TOP (1)
        CONVERT(NVARCHAR(4000), nv.OBSDESP) AS OBS_GUIA_DESP,
        CONVERT(NVARCHAR(4000), nv.OBSFACT) AS OBS_FACTURA
    FROM dbo.NOTV_DB nv
    WHERE nv.NUMNOTA = :num_nota
    ORDER BY nv.FECHA DESC
    """
    )
    return query_df(SQL, {"num_nota": int(str(num_nota))})


def get_nv_obs_gral(num_nota: int | str):
    """
    Devuelve la observación general de la Nota de Venta (NOTV_DB.OBSGRAL).
    """
    SQL = text(
        """
        SELECT TOP (1)
            CONVERT(NVARCHAR(4000), ISNULL(OBSGRAL, '')) AS OBSGRAL
        FROM dbo.NOTV_DB WITH (NOLOCK)
        WHERE CAST(NUMNOTA AS INT) = :n
        ORDER BY NUMREG DESC
    """
    )
    return query_df(SQL, {"n": int(str(num_nota))})


def get_factura_desde_nv(num_nota: str | int) -> dict:
    df = get_fav_header(num_nota)
    return df.iloc[0].to_dict() if df is not None and not df.empty else {}


# -------------------- Guía de Despacho: header, detail, insert --------------------

def get_gd_header(num_nota: int | str) -> Optional[Dict[str, Any]]:
    """
    Cabecera para Guía desde una NV. Evita GROUP BY sobre text/ntext y
    evita conversiones implícitas texto→int al resolver cliente.
    """
    SQL_NV_BASE = text(
        """
    SELECT TOP (1)
        nv.NUMREG AS NV_NUMREG,
        CAST(nv.FECHA AS DATE) AS FECHA,

        LTRIM(RTRIM(CONVERT(VARCHAR(100), nv.NRUTCLIE))) AS NRUTCLIE_TXT,
        LTRIM(RTRIM(CONVERT(VARCHAR(100), nv.CODVEND ))) AS CODVEND_TXT,
        LTRIM(RTRIM(CONVERT(VARCHAR(100), nv.NUMORD  ))) AS NUMORDEN_TXT,

        -- Sucursal: usar código NVSUC y también CODIGO/DESCRIP desde CHOI_DB
        CONVERT(VARCHAR(20), nv.NVSUC) AS SUCUR_COD_TXT,
        ch.CODIGO                                     AS SUCUR_CODIGO,
        CONVERT(NVARCHAR(200), ch.DESCRIP)            AS SUCUR_DESCRIP,

        nv.DCTOTIPO, nv.DCTOPJE, nv.DCTOPESO,
        CONVERT(NVARCHAR(4000), nv.GLOSACON) AS GLOSACON,
        CONVERT(NVARCHAR(4000), nv.OBSDESP ) AS OBSDESP,
        CONVERT(NVARCHAR(4000), nv.OBSFACT ) AS OBSFACT,
        CONVERT(NVARCHAR(4000), nv.DIRDESP ) AS ENTREGAR
    FROM dbo.NOTV_DB nv
    LEFT JOIN dbo.CHOI_DB ch ON ch.NUMREG = nv.NVSUC   -- ⬅️ aquí el diccionario
    WHERE nv.NUMNOTA = :num_nota
    ORDER BY nv.FECHA DESC

    """
    )

    SQL_TOTALES = text(
        """
    SELECT
      SUM(d.CANTIDAD * d.PRECUNIT * (1 - ISNULL(NULLIF(d.DESCTO,0),0)/100.0)) AS TOTNETO,
      SUM((d.CANTIDAD * d.PRECUNIT * (1 - ISNULL(NULLIF(d.DESCTO,0),0)/100.0)) *
          CASE WHEN ISNULL(d.TAX,0)=0 THEN 0 ELSE d.TAX/100.0 END) AS TOTIVA,
      SUM((d.CANTIDAD * d.PRECUNIT * (1 - ISNULL(NULLIF(d.DESCTO,0),0)/100.0)) *
          (1 + CASE WHEN ISNULL(d.TAX,0)=0 THEN 0 ELSE d.TAX/100.0 END)) AS TOTAL
    FROM dbo.NOTDE_DB d
    WHERE d.NUMRECOR = :nv_numreg
    """
    )
    SQL_CLIENTE = text(
        """
    SELECT TOP 1
      c.NREGUIST AS NRUTFACT,
      COALESCE(NULLIF(c.RUT,''), CONVERT(VARCHAR(20), c.NREGUIST)) AS RUTFACT,
      c.COMUNA AS ENTR_COMU,
      c.CIUDAD AS ENTR_CIUD
    FROM dbo.CLIEN_DB c
    WHERE
          CONVERT(VARCHAR(100), c.NREGUIST) = :key
       OR (c.RUT    IS NOT NULL AND LTRIM(RTRIM(UPPER(c.RUT)))    = LTRIM(RTRIM(UPPER(:key))))
       OR (c.RAZSOC IS NOT NULL AND LTRIM(RTRIM(UPPER(c.RAZSOC))) = LTRIM(RTRIM(UPPER(:key))))
    """
    )
    with ENGINE.begin() as conn:
        base = conn.execute(SQL_NV_BASE, {"num_nota": int(str(num_nota))}).fetchone()
        if not base:
            return None
        base = dict(base._mapping)
        tots = conn.execute(SQL_TOTALES, {"nv_numreg": base["NV_NUMREG"]}).fetchone()
        tots = dict(tots._mapping) if tots else {"TOTNETO": 0, "TOTIVA": 0, "TOTAL": 0}
        cli = conn.execute(SQL_CLIENTE, {"key": base["NRUTCLIE_TXT"]}).fetchone()
        cli = dict(cli._mapping) if cli else {}

    # armado final
    return {
        "NUMNOTA": int(str(num_nota)),
        "FECHA": base["FECHA"],
        "NRUTCLIE": base["NRUTCLIE_TXT"],
        "NRUTFACT": cli.get("NRUTFACT"),
        "RUTFACT": cli.get("RUTFACT", base["NRUTCLIE_TXT"]),
        "SUCUR": base["SUCUR_COD_TXT"],      # (1222, 160029…)
        "SUCUR_CODIGO": base.get("SUCUR_CODIGO"),      # p.ej. 115
        "SUCUR_DESCRIP": base.get("SUCUR_DESCRIP"),    # p.ej. 'Sucursal Santiago'
        "PROFORMA": 0,
        "CODVEND": base["CODVEND_TXT"],
        "NUMORDEN": base["NUMORDEN_TXT"],
        "DCTOTIPO": base["DCTOTIPO"],
        "DCTOPJE": base["DCTOPJE"],
        "DCTOPESO": base["DCTOPESO"],
        "GLOSACON": base["GLOSACON"],
        "OBSFACT": base["OBSFACT"],
        "OBSDESP": base["OBSDESP"],
        "ENTREGAR": base["ENTREGAR"],
        "ENTR_COMU": cli.get("ENTR_COMU"),
        "ENTR_CIUD": cli.get("ENTR_CIUD"),
        "TOTNETO": tots["TOTNETO"],
        "TOTIVA": tots["TOTIVA"],
        "TOTAL": tots["TOTAL"],
        "NV_NUMREG": base["NV_NUMREG"],
    }


def get_gd_detail(num_nota: int | str) -> List[Dict[str, Any]]:
    """
    Detalle para Guía desde NV. Si falta descripción en la NV,
    usa ART_DB.NOMBRE/NOMBRE2 como respaldo.
    """
    SQL_DETALLE = text(
        """
    WITH nv AS (
      SELECT TOP (1) nv.NUMREG AS NV_NUMREG
      FROM dbo.NOTV_DB AS nv
      WHERE nv.NUMNOTA = :num_nota
      ORDER BY nv.FECHA DESC
    )
    SELECT
      d.ITEM,
      d.NCODART,
      /* Prioriza DESCRIP del detalle; si viene nula/vacía usa ART_DB.NOMBRE y luego NOMBRE2 */
      CAST(
        COALESCE(
          NULLIF(CAST(d.DESCRIP AS NVARCHAR(4000)), N''),
          CAST(a.NOMBRE  AS NVARCHAR(4000)),
          CAST(a.NOMBRE2 AS NVARCHAR(4000)),
          N''
        ) AS NVARCHAR(4000)
      ) AS DESCRIP,
      d.CANTIDAD,
      /* Precio unitario directo del detalle */
      d.PRECUNIT,
      d.DESCTO,
      d.BODEGA,
      CAST(d.NSERIE AS NVARCHAR(200)) AS NSERIE,
      d.CENCOSTO,
      d.CTACTBLE,
      ISNULL(d.TAX, 0)     AS TAX,
      ISNULL(d.TIP_TAX, 0) AS TIP_TAX
    FROM dbo.NOTDE_DB AS d
    JOIN nv               ON d.NUMRECOR = nv.NV_NUMREG
    LEFT JOIN dbo.ART_DB  AS a ON a.NREGUIST = d.NCODART
    ORDER BY d.ITEM
    """
    )
    with ENGINE.begin() as conn:
        rows = conn.execute(
            SQL_DETALLE, {"num_nota": int(str(num_nota))}
        ).mappings().all()
    return list(rows)



def crear_guia_desde_nv(
    num_nota: int | str, tipodoc: int = 2, mueve_stock: int = 1
) -> Dict[str, Any]:
    """
    Crea Guía (TIPODOC=2) en DOCU_DB + DOCDE_DB desde una NV.
    - Genera correlativo visible desde DOCU_DB.NUMFACT con bloqueo (UPDLOCK, HOLDLOCK)
    - Captura NUMREG con OUTPUT INSERTED.NUMREG
    - Evita conversiones implícitas (texto→int) para NRUTCLIE/CODVEND
    - Inserta detalle adaptándose al esquema real (con o sin DESCRIP)
    """
    header = get_gd_header(num_nota)
    if not header:
        raise ValueError(f"No se encontró la NV {num_nota}")

    # NRUTCLIE independiente de NRUTFACT
    nrutclie_txt = str(header.get("NRUTCLIE") or "").strip()
    nrutclie_insert = int(nrutclie_txt) if nrutclie_txt.isdigit() else None

    # CODVEND: si es numérico, lo mandamos como int; si no, NULL
    codvend_txt = str(header.get("CODVEND") or "").strip()
    codvend_insert = int(codvend_txt) if codvend_txt.isdigit() else None

    # SUCUR desde NVSUC y cast a int con default
    sucur_txt = str(header.get("SUCUR") or "").strip()
    sucur_insert = int(sucur_txt) if sucur_txt.isdigit() else 1222

    # 1) Correlativo NUMFACT (bloqueado por TIPODOC y por SUCUR)
    SQL_NEXT_NUMFACT = text(
        """
    DECLARE @sucur INT = :sucur;
    DECLARE @desde DATE = DATEADD(DAY, -365, GETDATE());
    DECLARE @min INT, @max INT, @base INT;

    -- rangos por sucursal (ajústalos si cambia la política)
    IF @sucur IN (1222)   BEGIN SET @min = 300000; SET @max = 399999; END
    ELSE IF @sucur IN (160029) BEGIN SET @min = 400000; SET @max = 499999; END
    ELSE BEGIN SET @min = 1; SET @max = 999999; END

    -- 1) Máximo válido por SUCUR
    SELECT @base = MAX(CAST(NUMFACT AS INT))
    FROM dbo.DOCU_DB WITH (UPDLOCK, HOLDLOCK)
    WHERE TIPODOC = :tipodoc
      AND SUCUR   = @sucur
      AND ISNUMERIC(NUMFACT) = 1
      AND NUMFACT NOT LIKE '%.%' AND NUMFACT NOT LIKE '%e%'
      AND CAST(NUMFACT AS INT) BETWEEN @min AND @max
      AND COALESCE(FECHADESP, FECHA) >= @desde;

    -- 2) Fallback mismo SUCUR sin rango
    IF @base IS NULL
    BEGIN
      SELECT @base = MAX(CAST(NUMFACT AS INT))
      FROM dbo.DOCU_DB WITH (UPDLOCK, HOLDLOCK)
      WHERE TIPODOC = :tipodoc
        AND SUCUR   = @sucur
        AND ISNUMERIC(NUMFACT) = 1
        AND NUMFACT NOT LIKE '%.%' AND NUMFACT NOT LIKE '%e%';
    END

    -- 3) Fallback global (última barrera)
    IF @base IS NULL
    BEGIN
      SELECT @base = MAX(CAST(NUMFACT AS INT))
      FROM dbo.DOCU_DB WITH (UPDLOCK, HOLDLOCK)
      WHERE TIPODOC = :tipodoc
        AND ISNUMERIC(NUMFACT) = 1
        AND NUMFACT NOT LIKE '%.%' AND NUMFACT NOT LIKE '%e%';
    END

    SELECT ISNULL(@base, 0) + 1 AS NEXT_NUMFACT;
    """
    )

    # 2) INSERT de DOCU_DB (✅ ahora en una variable SQL_INSERT_DOCU)
    SQL_INSERT_DOCU = text(
        """
    SET NOCOUNT ON;

    DECLARE @out TABLE (NUMREG INT);

    INSERT INTO dbo.DOCU_DB (
        NUMFACT, TIPODOC, FECHA,
        NRUTFACT, RUTFACT, NRUTCLIE, PROFORMA, CODVEND,
        NUMORDEN, SUCUR, IDNREMP,              -- <— agregado IDNREMP
        GLOSACON, NOTAOBS, ENTREGAR, ENTR_COMU, ENTR_CIUD,
        TOTNETO, TOTIVA, TOTAL, STOCK,
        FECHADESP, HORADESP, FECACTUAL, HORACTUAL,
        ESELECTR, NUMEMPDO                     -- <— agregados ESELECTR y NUMEMPDO
    )
    OUTPUT INSERTED.NUMREG INTO @out(NUMREG)
    VALUES (
        :numfact, :tipodoc, :fecha,
        :nrutfact, :rutfact, :nrutclie, 0, :codvend,
        :numorden, :sucur, 1,              -- IDNREMP = 1 (forzado)
        :glosacon, :notaobs, :entregar, :entr_comu, :entr_ciud,
        :totneto, :totiva, :total, :stock,
        :fecha, CONVERT(VARCHAR(5), GETDATE(), 108), GETDATE(), CONVERT(VARCHAR(5), GETDATE(), 108),
        1, 1                                -- ESELECTR = 1, NUMEMPDO = 1 (forzados)
    );

    SELECT NUMREG FROM @out;
        """
    )

    # 3) INSERT detalle DOCDE_DB dinámico
    detail = get_gd_detail(num_nota)

    with ENGINE.begin() as conn:
        # correlativo por sucursal
        next_numfact = conn.execute(
            SQL_NEXT_NUMFACT, {"tipodoc": tipodoc, "sucur": sucur_insert}
        ).scalar()
        numfact = int(next_numfact or 1)

        params_docu = {
            "numfact": numfact,
            "tipodoc": tipodoc,
            "fecha": header["FECHA"],
            "nrutfact": header.get("NRUTFACT"),
            "rutfact": header.get("RUTFACT"),
            "nrutclie": nrutclie_insert,
            "codvend": codvend_insert,
            "numorden": header.get("NUMORDEN"),
            "sucur": sucur_insert,
            "glosacon": header.get("GLOSACON"),
            "notaobs": header.get("OBSFACT"),
            "entregar": header.get("ENTREGAR"),
            "entr_comu": header.get("ENTR_COMU"),
            "entr_ciud": header.get("ENTR_CIUD"),
            "totneto": header.get("TOTNETO"),
            "totiva": header.get("TOTIVA"),
            "total": header.get("TOTAL"),
            "stock": int(mueve_stock),
        }
        numreg = int(conn.execute(SQL_INSERT_DOCU, params_docu).scalar())

        # Descubrir metadatos reales de DOCDE_DB
        notnull = _docde_notnull_cols(conn)  # {'COL': 'DATA_TYPE', ...}
        identity_cols = _docde_identity_cols(conn)  # {'SEQLINE', ...}
        all_cols = _docde_all_cols(conn)  # {'NUMRECOR', 'ITEM', ...}

        # set base filtrado por columnas existentes
        base_known = {
            "NUMRECOR",
            "ITEM",
            "NCODART",
            "DESCRIP",
            "CANTIDAD",
            "PRECUNIT",
            "DESCTO",
            "BODEGA",
            "NSERIE",
            "CENCOSTO",
            "CTACTBLE",
            "TAX",
            "TIP_TAX",
            "SQNVDET",
            "SQGDDET",
            "SQOCDET",
            "LIQDDO",
            "TJ_SORT",
            "TJ_SUBSORT",
            "NRO_ROL",
            "LOT_IDE",
        }
        base_known = {c for c in base_known if c in all_cols}

        # NOT NULL sin default que existan y no estén en base ni sean IDENTITY
        extras = [
            c
            for c in notnull.keys()
            if c in all_cols and c not in base_known and c not in identity_cols
        ]

        # Construye el INSERT saltando columnas IDENTITY
        SQL_INSERT_DOCDE, col_order = _build_docde_insert_sql(
            extras, skip_cols=identity_cols
        )
        has_descrip = "DESCRIP" in col_order

        rows = []
        for idx, it in enumerate(detail):
            row: Dict[str, Any] = {
                "numrecor": numreg,
                "item": it["ITEM"],
                "ncodart": it["NCODART"],
                **({"descrip": (str(it.get("DESCRIP") or "").strip())} if has_descrip else {}),
                "cantidad": it["CANTIDAD"],
                "precunit": it["PRECUNIT"],
                "descto": it["DESCTO"],
                "bodega": it["BODEGA"],
                "nserie": (str(it.get("NSERIE") or "").strip()),
                "cencosto": it["CENCOSTO"],
                "ctactble": it["CTACTBLE"],
                "tax": it["TAX"],
                "tip_tax": it["TIP_TAX"],
                "sqnvdet": it.get("SQNVDET") or it.get("SEQNVDET") or it["ITEM"],
                "sqgddet": idx + 1,
                "sqocdet": it.get("SQOCDET") or it.get("SEQOCDET") or it["ITEM"],
                "liqddo": 0,
                "tj_sort": idx + 1,
                "tj_subsort": 0,
                "nro_rol": it.get("NRO_ROL") or 0,
                "lot_ide": it.get("LOT_IDE") or 0,
            }
            # Completar extras con defaults seguros
            for c in extras:
                row[c.lower()] = _safe_default(c, notnull[c], idx, header["FECHA"])
            # Eliminar cualquier columna IDENTITY del payload (defensivo)
            for c in identity_cols:
                row.pop(c.lower(), None)
            rows.append(row)

        if rows:
            conn.execute(SQL_INSERT_DOCDE, rows)

    return {"numreg": numreg, "numfact": numfact}


# -------------------- Retrocompatibilidad --------------------

def execute_main(sql: str, params: dict | None = None) -> None:
    execute(sql, params)


from sqlalchemy import text as _text

def get_engine(name: str):
    """
    Devuelve un engine por nombre.
    - 'santiago' -> ENGINE
    - 'rima'     -> RIMA_ENGINE
    """
    name = name.lower()
    if name == "santiago":
        return ENGINE
    if name == "rima":
        return RIMA_ENGINE
    raise ValueError(f"engine desconocido: {name}")

def probe_connections() -> dict:
    """
    Smoke test para verificar conexiones sin ensuciar logs.
    Útil en un healthcheck (/health) o al boot de la app.
    """
    out = {}
    for name, eng in (("santiago", ENGINE), ("rima", RIMA_ENGINE)):
        try:
            with eng.begin() as cx:
                row = cx.execute(_text("SELECT DB_NAME() AS db, @@SERVERNAME AS srv")).mappings().one()
            out[name] = {"ok": True, "db": row["db"], "server": row["srv"]}
        except Exception as e:
            out[name] = {"ok": False, "error": str(e)}
    return out
