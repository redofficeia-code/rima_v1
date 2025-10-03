# services/guia_service.py
from typing import Optional, Dict, Any, List
from sqlalchemy import text
from services.folios import next_numfact_unico  # folio único NUMFACT

# ===================== Constantes TIPODOC =====================
TIPODOC_FV   = 1  # Factura Venta (ajusta si tu esquema difiere)
TIPODOC_GD   = 2  # Guía de Despacho

# ============ UTIL: Mapeo de empresa por sucursal ============
def map_empresa_por_sucursal(sucur: int, default_emp: int = 1) -> int:
    """Devuelve el ID de empresa (NUMEMPDO) según la sucursal.
    Ajusta el mapa según tu realidad. Si no hay match, retorna default_emp.
    """
    mapa = {1222: 1}  # <- agrega acá más sucursales si corresponde
    try:
        return int(mapa.get(int(sucur or 0), default_emp))
    except Exception:
        return int(default_emp)

# =================== SELECT Encabezado NOTV_DB =================
SQL_NV_HEADER = """
SELECT TOP (1)
    CAST(nv.FECHA AS DATE)                                       AS FECHA,
    nv.NRUTCLIE                                                  AS NRUTCLIE,
    ISNULL(nv.SUCUR, 0)                                          AS SUCUR,
    NULLIF(LTRIM(RTRIM(CONVERT(VARCHAR(70),  nv.DIRDESP))),  '') AS ENTREGAR,
    NULLIF(LTRIM(RTRIM(CONVERT(VARCHAR(80),  nv.GLOSACON))), '') AS GLOSACON,
    NULLIF(LTRIM(RTRIM(CONVERT(VARCHAR(13),  nv.RUTFACT))),  '') AS RUTFACT,
    nv.CODVEND                                                   AS CODVEND,
    NULLIF(LTRIM(RTRIM(CONVERT(VARCHAR(10),  nv.MONEDA))),   '') AS MONEDA,
    NULLIF(LTRIM(RTRIM(CONVERT(VARCHAR(MAX), nv.OBSFACT))),  '') AS NOTAOBS,
    CAST(nv.NUMNOTA AS INT)                                      AS NUMOTS,
    CAST(ISNULL(nv.EXENTO, 0) AS INT)                            AS EXENTO
FROM dbo.NOTV_DB AS nv WITH (NOLOCK)
WHERE CAST(nv.NUMNOTA AS INT) = ?
ORDER BY nv.NUMREG DESC
"""

def leer_encabezado_nv(engine, num_nota: int) -> Optional[dict]:
    with engine.begin() as conn:
        row = conn.exec_driver_sql(SQL_NV_HEADER, (int(num_nota),)).first()
        if not row:
            return None
        return dict(row._mapping)

# Retrocompat
def leer_encabezado_nv_por_numnota(engine, num_nota: int) -> Optional[dict]:
    return leer_encabezado_nv(engine, num_nota)

# ===================== TOTALES desde NOTV_DB ===================
SQL_TOTALES_DESDE_NV = """
WITH nv AS (
  SELECT TOP (1) nv.NUMREG AS NV_NUMREG
  FROM dbo.NOTV_DB nv
  WHERE nv.NUMNOTA = :num_nota
  ORDER BY nv.FECHA DESC
)
SELECT
  ROUND(SUM((ISNULL(d.CANTIDAD,0) * ISNULL(d.PRECUNIT,0)) * (1 - ISNULL(NULLIF(d.DESCTO,0),0)/100.0)), 2) AS TOTNETO
FROM nv
JOIN dbo.NOTDE_DB d ON d.NUMRECOR = nv.NV_NUMREG
"""

def _calcular_totales(engine, num_nota: int, tasa_iva: float = 19.0, afecta: bool = True) -> Dict[str, float]:
    with engine.begin() as conn:
        row = conn.execute(text(SQL_TOTALES_DESDE_NV), {"num_nota": int(num_nota)}).first()
        tot_neto = float((row[0] if row and row[0] is not None else 0.0))
    if not afecta:
        return {"tot_neto": tot_neto, "tot_iva": 0.0, "total": round(tot_neto, 2), "iva_pct": 0.0}
    tot_iva = round(tot_neto * (tasa_iva / 100.0), 2)
    total = round(tot_neto + tot_iva, 2)
    return {"tot_neto": tot_neto, "tot_iva": tot_iva, "total": total, "iva_pct": tasa_iva}

# =============== INSERT Detalle DOCDE_DB desde NV =============

SQL_INSERT_DETALLE_DESDE_NV = """
WITH nv AS (
  SELECT TOP (1) nv.NUMREG AS NV_NUMREG
  FROM dbo.NOTV_DB nv
  WHERE nv.NUMNOTA = :num_nota
  ORDER BY nv.FECHA DESC, nv.NUMREG DESC
),
maxcorr AS (
  SELECT
    ISNULL(MAX(SQGDDET),0) AS base_gd,
    ISNULL(MAX(SQOCDET),0) AS base_oc
  FROM dbo.DOCDE_DB
  WHERE NUMRECOR = :numreg
),
det AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY d.ITEM) AS rn,
    d.ITEM,
    d.NCODART,
    CAST(CONVERT(NVARCHAR(4000),
         COALESCE(CONVERT(VARCHAR(4000), d.DESCRIP),
                  CONVERT(VARCHAR(4000), a.NOMBRE),
                  CONVERT(VARCHAR(4000), a.NOMBRE2), '')
    ) AS NVARCHAR(4000)) AS DESCRIP,
    ISNULL(d.CANTIDAD,0)  AS CANTIDAD,
    ISNULL(d.PRECUNIT,0)  AS PRECUNIT,
    ISNULL(d.DESCTO,0)    AS DESCTO,
    d.BODEGA,
    CAST(d.NSERIE AS NVARCHAR(200)) AS NSERIE,
    ISNULL(d.CENCOSTO,0)  AS CENCOSTO,
    ISNULL(d.CTACTBLE,0)  AS CTACTBLE,
    ISNULL(d.TAX,0)       AS TAX,
    ISNULL(d.TIP_TAX,0)   AS TIP_TAX,
    d.SEQNVDET            AS SEQNVDET
  FROM nv
  JOIN dbo.NOTDE_DB d      ON d.NUMRECOR = nv.NV_NUMREG
  LEFT JOIN dbo.ART_DB a   ON a.NREGUIST = d.NCODART
)
INSERT INTO dbo.DOCDE_DB (
  NUMRECOR, ITEM, NCODART, DESCRIP, CANTIDAD,
  despcant,               -- marca despacho
  PRECUNIT, DESCTO, BODEGA, NSERIE, CTACTBLE, CENCOSTO,
  TAX, TIP_TAX, FACTSI,
  SQNVDET, SQPKNV,        -- << CLAVES CICLO
  SQGDDET, SQOCDET,       -- << NOT NULL correlativos
  LIQDDO, TJ_SORT, TJ_SUBSORT, NRO_ROL, JDD, PREC_CMBIO, LETSEQ,
  LOT_IDE,                -- << NUEVO: NOT NULL en tu esquema
  TIME_DET, CICLO
)
SELECT
  :numreg,
  d.ITEM,
  d.NCODART,
  d.DESCRIP,
  d.CANTIDAD,
  d.CANTIDAD,                 -- despcant = cantidad
  d.PRECUNIT,
  d.DESCTO,
  d.BODEGA,
  ISNULL(d.NSERIE, N''),
  d.CTACTBLE,
  d.CENCOSTO,
  d.TAX,
  d.TIP_TAX,
  0,                          -- FACTSI
  d.SEQNVDET,                 -- << SQNVDET = SEQNVDET REAL
  (SELECT NV_NUMREG FROM nv), -- << SQPKNV  = NUMREG NV
  (SELECT base_gd FROM maxcorr) + d.rn,   -- SQGDDET
  (SELECT base_oc FROM maxcorr) + d.rn,   -- SQOCDET
  0, 0, 0, 0, 0, 0, 0,        -- LIQDDO/TJ_*/NRO_ROL/JDD/PREC_CMBIO/LETSEQ
  0,                          -- << LOT_IDE = 0 (sin lote)
  GETDATE(),
  0
FROM det d;
"""


# ========= Bloques reutilizables: limpieza / pista / cabecera =========

# Limpia duplicados por ITEM (conserva la línea más reciente)
SQL_LIMPIEZA_DUP_DETALLE = """
;WITH R AS (
  SELECT
      NUMRECOR,
      ITEM,
      ROW_NUMBER() OVER (
        PARTITION BY NUMRECOR, ITEM
        ORDER BY TIME_DET DESC, SQGDDET DESC, SQOCDET DESC
      ) AS rn
  FROM dbo.DOCDE_DB
  WHERE NUMRECOR = :numreg
)
DELETE d
FROM dbo.DOCDE_DB d
JOIN R
  ON R.NUMRECOR = d.NUMRECOR
 AND R.ITEM     = d.ITEM
WHERE R.rn > 1;
"""

# Pista de ciclo en detalle (CICLOGV2 = 'NV=<NUMREG>;SEQ=<SEQNVDET>')
SQL_SET_CICLOGV2_DETALLE = """
UPDATE d
SET d.CICLOGV2 = CASE
                   WHEN d.CICLOGV2 IS NULL OR LTRIM(RTRIM(CONVERT(varchar(200), d.CICLOGV2))) = ''
                     THEN 'NV=' + CONVERT(varchar(20), nv.NUMREG) + ';SEQ=' + CONVERT(varchar(20), nde.SEQNVDET)
                   ELSE d.CICLOGV2
                 END
FROM dbo.DOCDE_DB d
JOIN dbo.NOTDE_DB nde ON nde.SEQNVDET = d.SQNVDET
JOIN dbo.NOTV_DB  nv  ON nv.NUMREG    = nde.NUMRECOR
WHERE d.NUMRECOR = :numreg;
"""

# Normaliza cabecera para el Ciclo (NUMGUIAF/NGUIA/FACTURAD/MONEDA/SUCUR/RUTFACT)
SQL_NORMALIZA_CABECERA_GD = """
DECLARE @sucur_txt VARCHAR(50) = NULL;

SELECT TOP (1) @sucur_txt = CONVERT(varchar(50), nv.SUCUR)
FROM dbo.NOTV_DB nv
WHERE nv.NUMNOTA = :num_nota
ORDER BY nv.NUMREG DESC;

UPDATE d
SET
  d.FACTURAD = 0,
  d.NUMGUIAF = CASE WHEN LTRIM(RTRIM(ISNULL(d.NUMGUIAF,'')))='' THEN CONVERT(varchar(20), d.NUMFACT) ELSE d.NUMGUIAF END,
  d.NGUIA    = CASE WHEN LTRIM(RTRIM(ISNULL(d.NGUIA,   '')))='' THEN CONVERT(varchar(20), d.NUMFACT) ELSE d.NGUIA  END,
  d.MONEDA   = CASE WHEN LTRIM(RTRIM(ISNULL(d.MONEDA,'')))='' THEN 'P' ELSE d.MONEDA END,
  d.RUTFACT  = LTRIM(RTRIM(REPLACE(ISNULL(d.RUTFACT,' '), '_','-'))),
  d.SUCUR    = CASE
                 WHEN @sucur_txt IS NOT NULL AND @sucur_txt NOT LIKE '%[^0-9]%' THEN CAST(@sucur_txt AS INT)
                 ELSE d.SUCUR
               END
FROM dbo.DOCU_DB d
WHERE d.NUMREG = :numreg;
"""

# ================== Helpers de relacionamiento =================
SQL_GET_NUMREG_NV = """
SELECT TOP (1) nv.NUMREG
FROM dbo.NOTV_DB nv WITH (NOLOCK)
WHERE nv.NUMNOTA = :num_nota
ORDER BY nv.FECHA DESC, nv.NUMREG DESC
"""

SQL_INSERT_DOCREL_NV_TO_GD = """
INSERT INTO dbo.DOCREL_DB (NUMREG_ORIG, TIPODOC_ORIG, NUMREG_DEST, TIPODOC_DEST)
SELECT :numreg_nv, :tipodoc_nv, :numreg_gd, :tipodoc_gd
WHERE NOT EXISTS (
    SELECT 1
    FROM dbo.DOCREL_DB r
    WHERE r.NUMREG_ORIG = :numreg_nv
      AND r.TIPODOC_ORIG = :tipodoc_nv
      AND r.NUMREG_DEST = :numreg_gd
      AND r.TIPODOC_DEST = :tipodoc_gd
)
"""

def _vincular_nv_con_guia(conn, num_nota: int, numreg_guia: int) -> None:
    """
    Crea el vínculo NV -> Guía en la tabla de ciclo (DOCREL_DB).
    Usa TIPODOC_ORIG = 14 (Nota de Venta) por defecto para emular a Manager.
    Si el módulo define TIPODOC_NV, lo utilizará en lugar del 14.
    """
    # 0) Si no existe DOCREL_DB, salir silenciosamente
    chk = conn.execute(text("""
        SELECT 1
        WHERE OBJECT_ID('dbo.DOCREL_DB','U') IS NOT NULL
    """)).first()
    if not chk:
        return  # No hay tabla de relaciones en este esquema

    # 1) Buscar NUMREG de la NV
    row_nv = conn.execute(text("""
        SELECT TOP (1) nv.NUMREG
        FROM dbo.NOTV_DB nv WITH (NOLOCK)
        WHERE nv.NUMNOTA = :num_nota
        ORDER BY nv.FECHA DESC, nv.NUMREG DESC
    """), {"num_nota": int(num_nota)}).first()
    if not row_nv:
        return

    numreg_nv = int(row_nv[0])

    # 1.1) Determinar TIPODOC de NV (usar constante si existe; si no, 14 por defecto)
    tipodoc_nv = globals().get("TIPODOC_NV", 14)  # Nota de Venta (ajusta si tu esquema difiere)

    # 2) Insertar relación evitando duplicados (NV -> GD)
    conn.execute(text("""
        INSERT INTO dbo.DOCREL_DB (NUMREG_ORIG, TIPODOC_ORIG, NUMREG_DEST, TIPODOC_DEST)
        SELECT :numreg_nv, :tipodoc_nv, :numreg_gd, :tipodoc_gd
        WHERE NOT EXISTS (
            SELECT 1 FROM dbo.DOCREL_DB r
            WHERE r.NUMREG_ORIG = :numreg_nv
              AND r.TIPODOC_ORIG = :tipodoc_nv
              AND r.NUMREG_DEST  = :numreg_gd
              AND r.TIPODOC_DEST = :tipodoc_gd
        )
    """), {
        "numreg_nv": numreg_nv,
        "tipodoc_nv": int(tipodoc_nv),
        "numreg_gd": int(numreg_guia),
        "tipodoc_gd": TIPODOC_GD,  # Guía de Despacho
    })

def _insertar_detalle_desde_lista(conn, numreg_docu: int, num_nota: int, items: List[Dict[str, Any]]) -> None:
    """
    Inserta en DOCDE_DB usando SOLO los items escaneados.
    items: [{ "NCODART": int, "DESCRIP": str, "CANTIDAD": int, "PRECUNIT": num, "DESCTO": num, "BODEGA": int }, ...]
    """
    # Bases para correlativos y NUMREG de NV (para SQPKNV)
    row_bases = conn.execute(text("""
        SELECT
          ISNULL(MAX(SQGDDET),0) AS base_gd,
          ISNULL(MAX(SQOCDET),0) AS base_oc
        FROM dbo.DOCDE_DB
        WHERE NUMRECOR = :numreg
    """), {"numreg": numreg_docu}).first()
    base_gd = int(row_bases[0] or 0)
    base_oc = int(row_bases[1] or 0)

    nv_row = conn.execute(text(SQL_GET_NUMREG_NV), {"num_nota": int(num_nota)}).first()
    sqpknv = int(nv_row[0]) if nv_row else None

    payload = []
    for idx, it in enumerate(items, start=1):
        payload.append({
            "numrecor": numreg_docu,
            "item": idx,                                      # posición en la guía
            "ncodart": int(it["NCODART"]),
            "descrip": (it.get("DESCRIP") or ""),
            "cantidad": int(it["CANTIDAD"]),
            "precunit": float(it.get("PRECUNIT", 0)),
            "descto": float(it.get("DESCTO", 0)),
            "bodega": int(it.get("BODEGA") or 0),
            "sqgd": base_gd + idx,
            "sqoc": base_oc + idx,
            "sqpknv": sqpknv,
        })

    if not payload:
        return

    conn.execute(text("""
        INSERT INTO dbo.DOCDE_DB
        (NUMRECOR, ITEM, NCODART, DESCRIP, CANTIDAD,
         despcant, PRECUNIT, DESCTO, BODEGA, NSERIE,
         CTACTBLE, CENCOSTO, TAX, TIP_TAX, FACTSI,
         SQNVDET, SQPKNV, SQGDDET, SQOCDET,
         LIQDDO, TJ_SORT, TJ_SUBSORT, NRO_ROL, JDD, PREC_CMBIO, LETSEQ,
         LOT_IDE, TIME_DET, CICLO)
        VALUES
        (:numrecor, :item, :ncodart, :descrip, :cantidad,
         :cantidad, :precunit, :descto, :bodega, N'',
         0, 0, 0, 0, 0,
         NULL, :sqpknv, :sqgd, :sqoc,
         0, 0, 0, 0, 0, 0, 0,
         0, GETDATE(), 0)
    """), payload)


# ============== Upsert DOCU_DB_AUX (compatibilidad) ============

SQL_UPSERT_DOCU_DB_AUX = """
IF OBJECT_ID('dbo.DOCU_DB_AUX','U') IS NULL
BEGIN
  RETURN;
END;

-- 1) Asegura la fila: inserta respetando todas las columnas NOT NULL sin DEFAULT
IF NOT EXISTS (SELECT 1 FROM dbo.DOCU_DB_AUX WHERE numregDocu = :p_numreg)
BEGIN
  DECLARE @cols NVARCHAR(MAX) = N'numregDocu';
  DECLARE @vals NVARCHAR(MAX) = N'@p_numreg';

  ;WITH req AS (
    SELECT c.name AS col, t.name AS typ
    FROM sys.columns c
    JOIN sys.types   t ON t.user_type_id = c.user_type_id
    LEFT JOIN sys.default_constraints d
           ON d.parent_object_id = c.object_id AND d.parent_column_id = c.column_id
    WHERE c.object_id = OBJECT_ID('dbo.DOCU_DB_AUX')
      AND c.is_nullable = 0
      AND c.name <> 'numregDocu'
      AND d.object_id IS NULL  -- sin DEFAULT definido
  )
  SELECT
    @cols = @cols + N',' + QUOTENAME(col),
    @vals = @vals + N',' + CASE
      WHEN typ IN ('bit') THEN N'0'
      WHEN typ IN ('int','bigint','smallint','tinyint','decimal','numeric','money','smallmoney','float','real') THEN N'0'
      WHEN typ IN ('date','datetime','smalldatetime','datetime2') THEN N'CAST(GETDATE() AS DATE)'
      ELSE N''''''  -- cadenas vacías
    END
  FROM req;

  DECLARE @sqlins NVARCHAR(MAX) = N'INSERT INTO dbo.DOCU_DB_AUX (' + @cols + N') VALUES (' + @vals + N');';
  EXEC sp_executesql @sqlins, N'@p_numreg INT', @p_numreg = :p_numreg;
END;

-- 2) Actualizaciones sólo si existen las columnas (cada una en SQL dinámico)

-- NUMNOTA
IF COL_LENGTH('dbo.DOCU_DB_AUX', 'NUMNOTA') IS NOT NULL
BEGIN
  DECLARE @sql1 NVARCHAR(MAX) =
    N'UPDATE dbo.DOCU_DB_AUX
        SET [NUMNOTA] = CASE WHEN ISNUMERIC(CONVERT(varchar(50), @p_nv)) = 1
                              THEN CONVERT(int, @p_nv) ELSE NULL END
      WHERE numregDocu = @p_numreg;';
  EXEC sp_executesql @sql1,
    N'@p_numreg INT, @p_nv NVARCHAR(4000)',
    @p_numreg = :p_numreg, @p_nv = :p_nv;
END;

-- NumNota
IF COL_LENGTH('dbo.DOCU_DB_AUX', 'NumNota') IS NOT NULL
BEGIN
  DECLARE @sql2 NVARCHAR(MAX) =
    N'UPDATE dbo.DOCU_DB_AUX
        SET [NumNota] = CASE WHEN ISNUMERIC(CONVERT(varchar(50), @p_nv)) = 1
                              THEN CONVERT(int, @p_nv) ELSE NULL END
      WHERE numregDocu = @p_numreg;';
  EXEC sp_executesql @sql2,
    N'@p_numreg INT, @p_nv NVARCHAR(4000)',
    @p_numreg = :p_numreg, @p_nv = :p_nv;
END;

-- numNotaVta
IF COL_LENGTH('dbo.DOCU_DB_AUX', 'numNotaVta') IS NOT NULL
BEGIN
  DECLARE @sql3 NVARCHAR(MAX) =
    N'UPDATE dbo.DOCU_DB_AUX
        SET [numNotaVta] = CASE WHEN ISNUMERIC(CONVERT(varchar(50), @p_nv)) = 1
                                 THEN CONVERT(int, @p_nv) ELSE NULL END
      WHERE numregDocu = @p_numreg;';
  EXEC sp_executesql @sql3,
    N'@p_numreg INT, @p_nv NVARCHAR(4000)',
    @p_numreg = :p_numreg, @p_nv = :p_nv;
END;

-- NumPedidoNtVta
IF COL_LENGTH('dbo.DOCU_DB_AUX', 'NumPedidoNtVta') IS NOT NULL
BEGIN
  DECLARE @sql4 NVARCHAR(MAX) =
    N'UPDATE dbo.DOCU_DB_AUX
        SET [NumPedidoNtVta] = CASE WHEN ISNUMERIC(CONVERT(varchar(50), @p_nv)) = 1
                                     THEN CONVERT(int, @p_nv) ELSE NULL END
      WHERE numregDocu = @p_numreg;';
  EXEC sp_executesql @sql4,
    N'@p_numreg INT, @p_nv NVARCHAR(4000)',
    @p_numreg = :p_numreg, @p_nv = :p_nv;
END;

-- NroPedido
IF COL_LENGTH('dbo.DOCU_DB_AUX', 'NroPedido') IS NOT NULL
BEGIN
  DECLARE @sql5 NVARCHAR(MAX) =
    N'UPDATE dbo.DOCU_DB_AUX
        SET [NroPedido] = CASE WHEN ISNUMERIC(CONVERT(varchar(50), @p_nv)) = 1
                                THEN CONVERT(int, @p_nv) ELSE NULL END
      WHERE numregDocu = @p_numreg;';
  EXEC sp_executesql @sql5,
    N'@p_numreg INT, @p_nv NVARCHAR(4000)',
    @p_numreg = :p_numreg, @p_nv = :p_nv;
END;

-- confirmación opcional
SELECT CAST(1 AS INT) AS ok;
"""



# =================== Crear Guía (sin SP, como Manager) ===================

def crear_guia_sin_sp(
    engine,
    datos: Dict[str, Any],
    usuario4: str,
    sucur_defecto: int = 1222,
    insertar_detalle: bool = True,
    tasa_iva: float = 19.0,
    afecta: Optional[bool] = None,
    eselectr: int = 1,
    numempdo: Optional[int] = None,
    items_scaneados: Optional[List[Dict[str, Any]]] = None   # << NUEVO
) -> Dict[str, int]:
    u4 = (usuario4 or "").strip()[:4] or "RIMA"
    nrutclie = int(datos.get("NRUTCLIE") or 0)
    codvend = str(datos.get("CODVEND") if datos.get("CODVEND") is not None else "").strip()
    fecha    = datos.get("FECHA")
    moneda   = (datos.get("MONEDA") or None)
    glosa    = (datos.get("GLOSACON") or None)
    entregar = (datos.get("ENTREGAR") or None)
    notaobs  = (datos.get("NOTAOBS")  or None)
    numots   = int(datos.get("NUMOTS") or 0)
    rutfact  = (datos.get("RUTFACT")  or None)
    sucur_nv = int(datos.get("SUCUR")  or 0)

    if afecta is None:
        exento_flag = str(datos.get("EXENTO") or "0").strip()
        afecta = (exento_flag not in {"1", "true", "True"})

    numfact = next_numfact_unico()

    totales = {"tot_neto": 0.0, "tot_iva": 0.0, "total": 0.0, "iva_pct": (tasa_iva if afecta else 0.0)}
    if numots > 0 and not items_scaneados:
        # si vamos a traer todo desde la NV, podemos precalcular con NV
        totales = _calcular_totales(engine, numots, tasa_iva=tasa_iva, afecta=afecta)

    sucursal_final = sucur_nv if sucur_nv > 0 else int(sucur_defecto)
    afecta_bit = 1 if afecta else 0
    exento_bit = 0 if afecta else 1
    numempdo_final = map_empresa_por_sucursal(sucursal_final, default_emp=1) if numempdo is None else int(numempdo)

    with engine.begin() as conn:
        # 1) Encabezado
        row = conn.execute(text("""
            DECLARE @ahora DATETIME = GETDATE();
            INSERT dbo.DOCU_DB (
                TIPODOC, NUMFACT,
                FECHA, SUCUR, IDNREMP,
                NRUTCLIE, RUTFACT, CODVEND, STOCK,
                FECACTUAL, HORACTUAL, FECHADESP, HORADESP, FECHAMODIF,
                MONEDA, GLOSACON, ENTREGAR, NOTAOBS, NUMOTS,
                IVA, TOTNETO, TOTIVA, TOTAL,
                IMPRESA, VENTA, TASACBIO, VENCIDIA,
                AFECTO, EXENTO, ESELECTR, NUMEMPDO, USERMODI
            ) VALUES (
                :tipodoc, :numfact,
                COALESCE(:fecha, CAST(@ahora AS DATE)), :sucur, 1,
                :nrutclie, COALESCE(:rutfact,''), :codvend, 1,
                CAST(@ahora AS DATE), LEFT(CONVERT(varchar(8), @ahora, 108),5),
                CAST(@ahora AS DATE), LEFT(CONVERT(varchar(8), @ahora, 108),5), CAST(@ahora AS DATE),
                COALESCE(:moneda, ''), COALESCE(:glosa,  ''), COALESCE(:entregar, ''), COALESCE(:notaobs, ''), :numots,
                :iva_pct, :tot_neto, :tot_iva, :total,
                1, 1, 0, 0,
                :afecto_bit, :exento_bit, :eselectr, :numempdo, :usermodi
            );
            SELECT SCOPE_IDENTITY() AS numreg;
        """), {
            "tipodoc": TIPODOC_GD, "numfact": numfact, "fecha": fecha, "sucur": sucursal_final,
            "nrutclie": nrutclie, "rutfact": rutfact, "codvend": codvend,
            "moneda": moneda, "glosa": glosa, "entregar": entregar, "notaobs": notaobs, "numots": numots,
            "iva_pct": totales["iva_pct"], "tot_neto": totales["tot_neto"],
            "tot_iva": totales["tot_iva"], "total": totales["total"],
            "afecto_bit": afecta_bit, "exento_bit": exento_bit,
            "eselectr": int(eselectr), "numempdo": numempdo_final, "usermodi": (u4 or "RIMA")[:4]
        }).mappings().one()

        numreg = int(row["numreg"])

        # 2) Detalle: preferir SOLO lo escaneado; si no hay, traer todo desde la NV
        if insertar_detalle:
            if items_scaneados and len(items_scaneados) > 0:
                _insertar_detalle_desde_lista(conn, numreg_docu=numreg, num_nota=numots, items=items_scaneados)
            elif numots > 0:
                conn.execute(text(SQL_INSERT_DETALLE_DESDE_NV), {"num_nota": numots, "numreg": numreg})

            # limpieza/pistas/totales
            conn.execute(text(SQL_LIMPIEZA_DUP_DETALLE), {"numreg": numreg})
            conn.execute(text(SQL_SET_CICLOGV2_DETALLE), {"numreg": numreg})

            # Recalcular totales desde el propio detalle insertado
            row_tot = conn.execute(text("""
                SELECT ROUND(SUM((ISNULL(d.CANTIDAD,0) * ISNULL(d.PRECUNIT,0)) * (1 - ISNULL(NULLIF(d.DESCTO,0),0)/100.0)), 2)
                FROM dbo.DOCDE_DB d WHERE d.NUMRECOR = :numreg
            """), {"numreg": numreg}).first()
            tot_neto2 = float((row_tot[0] if row_tot and row_tot[0] is not None else 0.0))
            if afecta:
                tot_iva2 = round(tot_neto2 * (tasa_iva / 100.0), 2)
                total2 = round(tot_neto2 + tot_iva2, 2)
                iva_pct2 = tasa_iva
            else:
                tot_iva2, total2, iva_pct2 = 0.0, round(tot_neto2, 2), 0.0

            conn.execute(text("""
                UPDATE dbo.DOCU_DB
                   SET IVA=:iva, TOTNETO=:neto, TOTIVA=:iva_m, TOTAL=:tot
                 WHERE NUMREG=:numreg
            """), {"iva": iva_pct2, "neto": tot_neto2, "iva_m": tot_iva2, "tot": total2, "numreg": numreg})

            conn.execute(text("""
                UPDATE dbo.DOCU_DB
                   SET AFECTO = CASE WHEN TOTIVA > 0 THEN 1 ELSE 0 END,
                       EXENTO = CASE WHEN TOTIVA > 0 THEN 0 ELSE 1 END
                 WHERE NUMREG = :numreg
            """), {"numreg": numreg})

            conn.execute(text(SQL_NORMALIZA_CABECERA_GD), {"num_nota": numots, "numreg": numreg})

        # Enlaces/aux (igual que antes)
        if numots > 0:
            _vincular_nv_con_guia(conn, numots, numreg)

            # ---- UPSERT en DOCU_DB_AUX (sin leer filas; binds :p_numreg / :p_nv)
            conn.execute(
                text(SQL_UPSERT_DOCU_DB_AUX),
                {"p_numreg": numreg, "p_nv": str(numots)}
            )

        return {"numreg": numreg, "numfact": numfact}


# ===================== Wrapper "inyectar" (público) =====================
def inyectar_guia_con_sp(
    engine,
    num_nota: int,
    usuario4: str,
    sucur_defecto: int = 1222,
    afecta: Optional[bool] = None,
    tasa_iva: float = 19.0,
    insertar_detalle: bool = True,
    eselectr: int = 1,
    numempdo: Optional[int] = None,
) -> Dict[str, int]:
    """
    Crea una guía (TIPODOC=2) a partir del folio de NV (NUMNOTA),
    leyendo la cabecera desde NOTV_DB y usando crear_guia_sin_sp.
    """
    header = leer_encabezado_nv(engine, int(num_nota))
    if not header:
        raise ValueError(f"No se encontró la NV {num_nota} en NOTV_DB.")
    datos = dict(header)
    datos["NUMOTS"] = int(num_nota)
    return crear_guia_sin_sp(
        engine=engine,
        datos=datos,
        usuario4=usuario4,
        sucur_defecto=sucur_defecto,
        insertar_detalle=insertar_detalle,
        tasa_iva=tasa_iva,
        afecta=afecta,
        eselectr=eselectr,
        numempdo=numempdo,
    )

__all__ = ["inyectar_guia_con_sp", "crear_guia_sin_sp"]
