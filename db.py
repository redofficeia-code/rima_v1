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
TRUST    = os.getenv("DB_TRUST_CERT", "yes")

if AUTH == "windows":
    odbc = (
        f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};"
        f"Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate={'yes' if TRUST=='yes' else 'no'};"
    )
else:
    USER = os.getenv("DB_USER", "")
    PWD  = os.getenv("DB_PASSWORD", "")
    odbc = (
        f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};UID={USER};PWD={PWD};"
        f"Encrypt=yes;TrustServerCertificate={'yes' if TRUST=='yes' else 'no'};"
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
