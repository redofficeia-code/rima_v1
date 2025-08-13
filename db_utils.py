import os
import pyodbc
import pandas as pd
from sqlalchemy import create_engine, text

def get_oc_detalle(num_oc):
    conn_str = (
        f"DRIVER={{{os.getenv('DB_DRIVER')}}};"
        f"SERVER={os.getenv('DB_SERVER')};"
        f"DATABASE={os.getenv('DB_DATABASE')};"
        f"UID={os.getenv('DB_USER')};"
        f"PWD={os.getenv('DB_PASSWORD')};"
        f"TrustServerCertificate={os.getenv('DB_TRUST_CERT')};"
    )

    query = f"""
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
        WHERE h.NUMORDEN = ?
        ORDER BY d.ITEM
    """

    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute(query, (num_oc,))
        rows = cursor.fetchall()

    # Convertir a lista de diccionarios
    columnas = [col[0] for col in cursor.description]
    data = [dict(zip(columnas, row)) for row in rows]
    return data


# --- Nuevas utilidades ----------------------------------------------------

# Construcción de engine SQLAlchemy reutilizable para consultas con pandas.
_driver = os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')
_driver_enc = _driver.replace(' ', '+')
ENGINE = create_engine(
    f"mssql+pyodbc://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_SERVER')}/{os.getenv('DB_DATABASE')}?"  # type: ignore
    f"driver={_driver_enc}&TrustServerCertificate={os.getenv('DB_TRUST_CERT')}"
)


def get_nota_detalle(num_nota: str) -> pd.DataFrame:
    """Trae detalle de NV desde la BBDD.

    Mapas:
      - num_nota  -> NOTV_DB.NUMNOTA
      - codigo    -> ART_DB.CODIGO2 (fallback: NOTDE_DB.NCODART)
      - nombre    -> NOTDE_DB.DESCRIP
      - cantidad  -> (NOTDE_DB.CANTIDAD - NOTDE_DB.CANTDESP)
      - prec_unit -> NOTDE_DB.PRECUNIT
    """
    sql = text(
        """
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
    )

    with ENGINE.begin() as conn:
        df = pd.read_sql(sql, conn, params={"num_nota": num_nota})

    if not df.empty:
        df["cantidad"] = pd.to_numeric(df["cantidad"], errors="coerce").fillna(0).astype(int)
        df["prec_unit"] = pd.to_numeric(df["prec_unit"], errors="coerce").fillna(0.0)
        # (Opcional) Evitar negativos:
        # df["cantidad"] = df["cantidad"].clip(lower=0)

    return df


def get_stock_actual() -> pd.DataFrame:
    """Obtiene el stock físico de los productos desde la BBDD.

    Une ``STOCK_DB`` con ``ART_DB`` para entregar código visible,
    nombre y stock disponible.
    """
    sql = text(
        """
        SELECT
            art.CODIGO2   AS codigo,
            art.NOMBRE    AS nombre,
            stk.STK_FISICO AS cantidad
        FROM dbo.STOCK_DB AS stk
        JOIN dbo.ART_DB   AS art
            ON art.NREGUIST = stk.ARTICULO
        """
    )

    with ENGINE.begin() as conn:
        df = pd.read_sql(sql, conn)

    if not df.empty:
        df["cantidad"] = pd.to_numeric(df["cantidad"], errors="coerce").fillna(0).astype(int)

    return df
