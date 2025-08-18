# db.py
import os
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DRIVER   = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
SERVER   = os.getenv("DB_SERVER", "localhost")
DATABASE = os.getenv("DB_DATABASE", "")
AUTH     = os.getenv("DB_AUTH", "sql").lower()
def _env_bool(value: str) -> bool:
    value = value.strip().lower()
    if value in {"yes", "true", "1"}:
        return True
    if value in {"no", "false", "0"}:
        return False
    return True

TRUST_CERT = _env_bool(os.getenv("DB_TRUST_CERT", "yes"))

if AUTH == "windows":
    odbc = (
        f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};"
        f"Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate={'yes' if TRUST_CERT else 'no'};"
    )
else:
    USER = os.getenv("DB_USER", "")
    PWD  = os.getenv("DB_PASSWORD", "")
    odbc = (
        f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};UID={USER};PWD={PWD};"
        f"Encrypt=yes;TrustServerCertificate={'yes' if TRUST_CERT else 'no'};"
    )

params = urllib.parse.quote_plus(odbc)
ENGINE = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", pool_pre_ping=True, fast_executemany=True)

def query_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    with ENGINE.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

# === Repositorio para /ingreso ===

def get_oc_detalle_por_oc(num_oc: str) -> pd.DataFrame:
    """
    Devuelve líneas de la OC desde OCDET_DB.
    Campos clave confirmados en diccionario: CANTIDAD, CANTRECI, CANTFAC, BODEGA, CENTCC, ITEM.
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
    """
    Trae datos de ART_DB por CODIGO2 (código visible en la UI).
    """
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
    """Obtiene líneas de una OC directamente desde la base de datos.

    Devuelve un ``DataFrame`` con las columnas ``Código``, ``Nombre``,
    ``Cantidad`` y ``Prec.Unit.``.  Además retorna el número de guía
    asociado a la OC (si existe).
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


# === Migrado desde db_utils.py ===

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
            art.CODIGO2   AS codigo,
            art.NOMBRE    AS nombre,
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
    """Retorna ``(header, detalles)`` para prellenar la Guía de Despacho."""
    header_sql = """
        SELECT
            CAST(NULL AS VARCHAR(50))       AS GD_NUM,
            nv.NUMORD                       AS OC_NUM,
            nv.RUTFACT                      AS FAV_RUT,
            c.RAZSOC                        AS FAV_RAZSOC,
            nv.NRUTCLIE                     AS DESP_A,
            p.CODIGO                        AS VEND_CODIGO,
            CONCAT(p.NOMBRE, ' ', p.APELLIDO) AS VEND_NOMBRE,
            nv.DIRDESP                      AS ENTREGAR_EN,
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
        GROUP BY nv.NUMORD, nv.RUTFACT, c.RAZSOC, nv.NRUTCLIE, p.CODIGO, p.NOMBRE, p.APELLIDO, nv.DIRDESP, nv.COMISION, nv.SUCUR, nv.GLOSACON
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
