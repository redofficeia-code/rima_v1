# services/factura_service.py
from sqlalchemy import text

def inyectar_factura_con_sp(engine, payload: dict, usuario: str = "admin"):
    """
    Inserta Factura (TIPODOC=1) en DOCU_DB/DOCDE_DB.
    - payload: dict con TITULO/HEADER e ITEMS (ya viene armado desde admin).
    Estructura esperada:
      {
        "TIPODOC": 1,
        "NUMNOTA": 2326570,
        "RUT": "76123456-7",
        "RAZON": "Cliente SA",
        "FECHA": "2025-09-15",
        "VENCE": "2025-09-30",
        "GLOSA": "Pago 30 días",
        "MONEDA": 1,
        "VENDEDOR": "BB1"  # o código numérico según tu BD
        "ITEMS": [{"codigo":"6500849011","cantidad":2,"prec_unit":1234.0}, ...]
      }
    Retorna: {"numreg": <int>, "numfact": <str opcional>}
    """
    with engine.begin() as conn:
        # 1) Inserta encabezado en DOCU_DB (campos mínimos compatibles)
        #    TIPODOC: 1=Factura Venta (según tu enumeración guardada)
        ins_header = text("""
            INSERT INTO dbo.DOCU_DB
                (TIPODOC, FECHA, VENCIMIE, NRUTFACT, RUTFACT, GLOSACON,
                 MONEDA, CODVEND, STOCK, FACTURAD, FECACTUAL, HORACTUAL, USERMODI)
            VALUES
                (:tipodoc, :fecha, :vence, :rut, :rut, :glosa,
                 :moneda, :vend, 1, 0, CAST(GETDATE() AS DATE), CONVERT(CHAR(8), GETDATE(), 108), :usuario);

            SELECT SCOPE_IDENTITY() AS NUMREG;
        """)
        r = conn.execute(ins_header, {
            "tipodoc": int(payload.get("TIPODOC", 1)),
            "fecha":   payload.get("FECHA"),
            "vence":   payload.get("VENCE"),
            "rut":     payload.get("RUT"),
            "glosa":   payload.get("GLOSA"),
            "moneda":  payload.get("MONEDA", 1),
            "vend":    payload.get("VENDEDOR"),
            "usuario": usuario or "admin",
        }).mappings().first()
        numreg = int(r["NUMREG"])

        # 2) Inserta detalle en DOCDE_DB enlazado al encabezado
        #    Nota: en tu esquema, DOCDE_DB.NUMRECOR referencia DOCU_DB.NUMREG (o PGNUMRECOR).
        #    Ajusta NCODART si trabajas con NREGUIST (aquí asumimos CODIGO2 -> ART_DB.NREGUIST).
        for item in (payload.get("ITEMS") or []):
            # Resolver NREGUIST por CODIGO2
            q_art = conn.execute(text("""
                SELECT TOP 1 NREGUIST FROM dbo.ART_DB WHERE CODIGO2 = :c
            """), {"c": str(item.get("codigo"))}).mappings().first()
            ncodart = int(q_art["NREGUIST"]) if q_art else None

            conn.execute(text("""
                INSERT INTO dbo.DOCDE_DB
                    (NUMRECOR, ITEM, NCODART, DESCRIP, CANTIDAD, PRECUNIT, FACTSI)
                VALUES
                    (:numrecor, :item, :ncodart, :descrip, :cantidad, :prec, 1)
            """), {
                "numrecor": numreg,
                "item":     1_000 + (payload["ITEMS"].index(item) + 1),  # o el correlativo que uses
                "ncodart":  ncodart,
                "descrip":  str(item.get("desc") or item.get("descripcion") or ""),
                "cantidad": int(item.get("cantidad") or 0),
                "prec":     float(item.get("prec_unit") or 0.0),
            })

        # 3) (Opcional) Generar número visible de factura (NUMFACT) si tu sistema lo maneja aquí
        #    Si lo genera otro proceso/trigger, omite esta actualización.
        #    Aquí solo ejemplificamos autogenerar un correlativo simple.
        conn.execute(text("""
            UPDATE dbo.DOCU_DB
               SET NUMFACT = COALESCE(NUMFACT, CAST(:numreg AS VARCHAR(20)))
             WHERE NUMREG = :numreg
        """), {"numreg": numreg})

        return {"numreg": numreg, "numfact": str(numreg)}
