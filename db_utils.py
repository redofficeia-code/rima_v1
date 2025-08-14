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


# === Added by Codex: función para prellenar guía desde NV ===
def get_guia_desde_nv(num_nota: str):
    """
    Retorna (header: dict, detalles: list[dict]) para prellenar la Guía de Despacho
    a partir de NOTV_DB/NOTDE_DB/ART_DB.
    """
    header_sql = text(
        """
        SELECT
            CAST(NULL AS VARCHAR(50))       AS GD_NUM,      -- DOCU_DB.NUMGUIAF al crear la guía
            nv.NUMORD                       AS OC_NUM,
            nv.NRUTCLIE                     AS FAV_A,
            nv.NRUTCLIE                     AS DESP_A,
            nv.CODVEND                      AS VENDEDOR,
            nv.DIRDESP                      AS ENTREGAR_EN,
            nv.COMISION                     AS COMISION,
            nv.SUCUR                        AS SUCURSAL,
            nv.GLOSACON                     AS GLOSA_PAG,
            CASE WHEN MIN(COALESCE(nd.DESCTO,0)) = MAX(COALESCE(nd.DESCTO,0))
                 THEN MIN(COALESCE(nd.DESCTO,0))
                 ELSE NULL
            END                             AS DESCUENTO_UNICO_LINEAS
        FROM dbo.NOTV_DB  nv
        JOIN dbo.NOTDE_DB nd ON nd.NUMRECOR = nv.NUMREG
        WHERE nv.NUMNOTA = :num_nota
        GROUP BY nv.NUMORD, nv.NRUTCLIE, nv.CODVEND, nv.DIRDESP, nv.COMISION, nv.SUCUR, nv.GLOSACON
        """
    )

    detail_sql = text(
        """
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
    )

    with ENGINE.begin() as conn:
        df_h = pd.read_sql(header_sql, conn, params={"num_nota": num_nota})
        df_d = pd.read_sql(detail_sql, conn,  params={"num_nota": num_nota})

    header = (df_h.iloc[0].to_dict() if not df_h.empty else {})
    detalles = df_d.to_dict(orient="records")
    return header, detalles

