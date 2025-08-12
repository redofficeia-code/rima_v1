import pyodbc
import os

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
