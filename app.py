import os
import csv
import math
import io
import logging
from datetime import datetime
from sqlalchemy import text
import json
from services.guia_service import inyectar_guia_con_sp


from flask import (
    Flask, render_template, request, redirect,
    url_for, flash, session, send_file, current_app, abort, g
)
from werkzeug.utils import secure_filename
import pandas as pd
import re
import unicodedata
import db
import db_utils
from db_utils import get_oc_detalle
from auth_map import ROL_JEFE, ROL_OPERARIO
try:
    from auth_service import login_nivel2_operario
except ImportError:
    login_nivel2_operario = None

def get_nv_payload_fast(num_nota: int):
    """
    Carga rápida para una NV específica:
    - Header NOTV_DB (lo mínimo)
    - Detalle NOTDE_DB de esa NV
    - Stock agregado SOLO de los artículos de esa NV
    Retorna: (df_header, df_detalle_con_stock)
    """
    if not num_nota:
        return None, None

    # 1) Header (sin arrastrar columnas pesadas)
    sql_header = """
    SELECT /*+ FAST 1 */
           CAST(nv.NUMNOTA AS INT) AS NUMNOTA,
           CAST(nv.FECHA AS DATE)  AS FECHA,
           nv.NRUTCLIE,
           nv.SUCUR,
           ISNULL(nv.OBSFACT, '')  AS OBSFACT
    FROM [Santiago].dbo.NOTV_DB WITH (NOLOCK)
    WHERE CAST(nv.NUMNOTA AS INT) = :n
    """
    df_h = db.query_df(sql_header, {"n": int(num_nota)})

    # 2) Detalle de ESA NV (solo columnas necesarias)
    sql_det = """
    SELECT d.ITEM,
           d.NCODART,           -- id artículo (↔ ART_DB.NREGUIST)
           d.DESCRIP,
           CAST(ISNULL(d.CANTIDAD,0)  AS INT) AS CANTIDAD,
           CAST(ISNULL(d.CANTDESP,0) AS INT) AS CANTDESP
    FROM [Santiago].dbo.NOTDE_DB d WITH (NOLOCK)
    INNER JOIN [Santiago].dbo.NOTV_DB h WITH (NOLOCK)
            ON h.NUMREG = d.NUMRECOR
    WHERE CAST(h.NUMNOTA AS INT) = :n
    ORDER BY d.ITEM
    """
    df_d = db.query_df(sql_det, {"n": int(num_nota)})

    if df_d is None or df_d.empty:
        return df_h, df_d  # sin detalle, nada que sumar

    # 3) Stock SOLO para los artículos del detalle (sin barrer la tabla completa)
    #    Armamos una lista segura de ARTICULO ids (NCODART ↔ ART_DB.NREGUIST)
    ids = sorted(set(int(x) for x in df_d["NCODART"].tolist() if str(x).isdigit()))
    if not ids:
        df_d["STK"] = 0
        df_d["PENDIENTE"] = (df_d["CANTIDAD"] - df_d["CANTDESP"]).clip(lower=0)
        return df_h, df_d

    # Para SQL Server, IN (...) de muchos valores: usamos tabla temporal inline via VALUES
    values_clause = ",".join(f"({i})" for i in ids)

    sql_stock = f"""
    WITH wanted(ARTICULO) AS (
        SELECT v.col FROM (VALUES {",".join(f"({i})" for i in ids)}) AS v(col)
    )
    SELECT s.ARTICULO,
           SUM(COALESCE(s.STK_FISICO,0)) AS STK
    FROM [Santiago].dbo.STOCK_DB s WITH (NOLOCK)
    INNER JOIN wanted w ON w.ARTICULO = s.ARTICULO
    GROUP BY s.ARTICULO
    """
    df_s = db.query_df(sql_stock, {})

    # 4) Merge en Python (rápido: dataset pequeño = solo items de la NV)
    if df_s is not None and not df_s.empty:
        df_d = df_d.merge(df_s.rename(columns={"ARTICULO":"NCODART"}), how="left", on="NCODART")
    else:
        df_d["STK"] = 0

    # 5) Campos derivados
    df_d["STK"] = df_d["STK"].fillna(0).astype(int)
    df_d["PENDIENTE"] = (df_d["CANTIDAD"] - df_d["CANTDESP"]).clip(lower=0)

    return df_h, df_d



# --- Helper: obtener OBSFACT por NUMNOTA desde Santiago ---
def get_obsfact_by_num_nota(num_nota: int) -> str:
    """
    Devuelve OBSFACT (observaciones de facturación) para la Nota de Venta dada.
    """
    if not num_nota:
        return ""
    try:
        n = int(str(num_nota).split('.')[0])
    except Exception:
        return ""
    try:
        df = db.query_df(
            """
            SELECT TOP 1
                   CAST(ISNULL(OBSFACT,'') AS NVARCHAR(4000)) AS ObsFact
            FROM dbo.NOTV_DB
            WHERE NUMNOTA = :n
            ORDER BY NUMREG DESC
            """,
            {"n": n}
        )
        if df is not None and not df.empty:
            return (str(df.iloc[0]["ObsFact"]) or "").strip()
    except Exception:
        pass
    return ""


def _ensure_zonas_table():
    # IMPORTANTE: usar db.execute_rima (no db.execute)
    sql = """
    IF OBJECT_ID('dbo.ZONAS_DB','U') IS NULL
    BEGIN
        CREATE TABLE dbo.ZONAS_DB (
            ID INT IDENTITY(1,1) PRIMARY KEY,
            NOMBRE NVARCHAR(100) NOT NULL
        );
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = 'UX_ZONAS_DB_NOMBRE'
              AND object_id = OBJECT_ID('dbo.ZONAS_DB')
        )
            CREATE UNIQUE INDEX UX_ZONAS_DB_NOMBRE ON dbo.ZONAS_DB (NOMBRE);
    END
    ELSE
    BEGIN
        IF COL_LENGTH('dbo.ZONAS_DB','NOMBRE') IS NULL
        BEGIN
            ALTER TABLE dbo.ZONAS_DB ADD NOMBRE NVARCHAR(100) NULL;
            UPDATE dbo.ZONAS_DB SET NOMBRE = ISNULL(LTRIM(RTRIM(NOMBRE)), '');
            ALTER TABLE dbo.ZONAS_DB ALTER COLUMN NOMBRE NVARCHAR(100) NOT NULL;
        END
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = 'UX_ZONAS_DB_NOMBRE'
              AND object_id = OBJECT_ID('dbo.ZONAS_DB')
        )
            CREATE UNIQUE INDEX UX_ZONAS_DB_NOMBRE ON dbo.ZONAS_DB (NOMBRE);
    END
    """
    import db
    db.execute_rima(sql)

# BD cualificadas (no afectan login)
ZONAS_TBL    = "dbo.ZONAS_DB"                  # en RIMA (escritura via *_rima)
NV_ZONAS_TBL = "[SANTIAGO].[dbo].[NV_ZONAS]"   # lectura (si existe) via ENGINE (Santiago)

def _get_zonas() -> list[dict]:
    """Lee zonas desde RIMA."""
    try:
        df = db.query_df_rima("SELECT ID, NOMBRE FROM dbo.ZONAS_DB ORDER BY NOMBRE;", {})
        return df.to_dict(orient="records")
    except Exception as e:
        app.logger.error(f"Error leyendo ZONAS_DB: {e}")
        return []


def _seed_zonas_if_empty():
    # ¿Ya hay datos en RIMA?
    try:
        df_cnt = db.query_df_rima("SELECT COUNT(*) AS n FROM dbo.ZONAS_DB", {})
        if not df_cnt.empty and int(df_cnt.iloc[0]["n"]) > 0:
            return
    except Exception:
        return

    # 1) Intentar poblar desde NV_ZONAS en SANTIAGO (engine normal)
    try:
        df_src = db.query_df(
            f"SELECT DISTINCT LTRIM(RTRIM(ZONA)) AS Z FROM {NV_ZONAS_TBL} "
            "WHERE ZONA IS NOT NULL AND LTRIM(RTRIM(ZONA)) <> ''", {}
        )
        if not df_src.empty:
            for _, r in df_src.iterrows():
                z = str(r["Z"]).strip()
                db.execute_rima(
                    """
                    IF NOT EXISTS (
                        SELECT 1 FROM dbo.ZONAS_DB
                        WHERE UPPER(LTRIM(RTRIM(NOMBRE))) = UPPER(:n)
                    )
                    INSERT INTO dbo.ZONAS_DB(NOMBRE) VALUES(:n)
                    """,
                    {"n": z}
                )
            return
    except Exception:
        pass

    # 2) Semilla por defecto (en RIMA)
    defaults = ["LA SERENA","LINARES","LOS LAGOS","PUERTO MONTT","RANCAGUA","SANTIAGO"]
    for z in defaults:
        db.execute_rima(
            """
            IF NOT EXISTS (
                SELECT 1 FROM dbo.ZONAS_DB
                WHERE UPPER(LTRIM(RTRIM(NOMBRE))) = UPPER(:n)
            )
            INSERT INTO dbo.ZONAS_DB(NOMBRE) VALUES(:n)
            """,
            {"n": z}
        )


# Usuarios disponibles para Login 1 (value=COD, label visible)
LOGIN1_USUARIOS = [
    ("BB1", "JEFE BODEGA"),
    ("SPT", "OPERARIO BODEGA"),
]

# --- Configuración de logging ---
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'dev-secret-change-me')

@app.route('/docs/enviar', methods=['POST'])
def docs_enviar():
    cu = session.get('current_user')
    op = session.get('operario')
    if not cu:
        return redirect(url_for('login1'))
    if cu.get('rol') == ROL_OPERARIO and not op:
        return redirect(url_for('login2'))

    # 1) Tomar num_nota (desde form o desde sesión de la guía)
    num_nota = (request.form.get('num_nota') or session.get('nv_para_guia') or '').strip()
    if not num_nota:
        flash('Falta el número de Nota de Venta para enviar.', 'warning')
        return redirect(url_for('salida'))
    try:
        num_nota_int = int(float(num_nota))
    except Exception:
        flash('Número de Nota de Venta inválido.', 'danger')
        return redirect(url_for('salida'))

    # 2) Traer header de NOTV_DB + cliente (Santiago)
    try:
        sql_hdr = """
            SELECT TOP (1)
                nv.NUMNOTA,
                nv.NRUTCLIE AS RUT,
                ISNULL(c.RAZSOC,'') AS RAZON_SOCIAL,
                nv.SUCUR      AS CIUDAD,
                CAST(nv.FECHA AS DATE) AS FECHA_DOC,
                ISNULL(nv.OBSFACT, '') AS OBSFACT
            FROM dbo.NOTV_DB nv
            LEFT JOIN dbo.CLIEN_DB c ON c.NREGUIST = nv.NRUTCLIE
            WHERE nv.NUMNOTA = :n
        """
        hdr = db.query_df(sql_hdr, {"n": num_nota_int})
        if hdr.empty:
            flash(f'No se encontró información para la Nota de Venta {num_nota_int}.', 'warning')
            return redirect(url_for('salida'))
        header = hdr.iloc[0].to_dict()
    except Exception as e:
        flash(f'Error consultando encabezado NV: {e}', 'danger')
        return redirect(url_for('salida'))

    # 3) Traer detalle NV desde NOTDE_DB (para snapshot)
    try:
        sql_det = """
            SELECT
                d.ITEM,
                d.NCODART,
                d.DESCRIP,
                CAST(ISNULL(d.CANTIDAD,0) AS INT)  AS CANTIDAD,
                CAST(ISNULL(d.CANTDESP,0) AS INT)  AS CANTDESP,
                CAST(ISNULL(d.PRECUNIT,0) AS DECIMAL(18,2)) AS PRECUNIT
            FROM dbo.NOTDE_DB d
            INNER JOIN dbo.NOTV_DB h ON h.NUMREG = d.NUMRECOR
            WHERE h.NUMNOTA = :n
            ORDER BY d.ITEM
        """
        det = db.query_df(sql_det, {"n": num_nota_int})
        items = det.to_dict(orient='records')
    except Exception as e:
        flash(f'Error consultando detalle NV: {e}', 'danger')
        return redirect(url_for('salida'))

    # 4) Incluir escaneos de la sesión si existen (lo que se usó para armar la guía)
    scans = session.get('items_para_guia') or []

    # 5) Preparar snapshot JSON
    header_json = json.dumps(header, ensure_ascii=False, default=str)
    items_json  = json.dumps({"items": items, "scans": scans}, ensure_ascii=False, default=str)


    # 6) Insertar en RIMA.dbo.DOC_OUTBOX (queda PENDIENTE)
    try:
        db.execute_rima("""
            INSERT INTO dbo.DOC_OUTBOX
                (TIPO, NUMNOTA, NUMGUIA, RUT, RAZON_SOCIAL, CIUDAD, FECHA_DOC,
                 HEADER_JSON, ITEMS_JSON, ESTADO, CREATED_BY)
            VALUES
                ('GUIA', :num_nota, NULL, :rut, :razon, :ciudad, :fdoc,
                 :hjson, :ijson, 'PENDIENTE', :by)
        """, {
            "num_nota": num_nota_int,
            "rut": header.get("RUT"),
            "razon": header.get("RAZON_SOCIAL"),
            "ciudad": header.get("CIUDAD"),
            "fdoc": header.get("FECHA_DOC").isoformat() if hasattr(header.get("FECHA_DOC"), "isoformat") else header.get("FECHA_DOC"),
            "hjson": header_json,
            "ijson": items_json,
            "by": (cu.get('nombre') or cu.get('usuario') or 'operario')
        })
    except Exception as e:
        flash(f'Error guardando en la bandeja de envío: {e}', 'danger')
        return redirect(url_for('guia_despacho'))

    flash("Documento enviado al Jefe de Bodega para inyección.", "success")

    # 7) Volver a la pantalla de fin de salida (tu flujo actual)
    return redirect(url_for('finalizar_salida', num_nota=num_nota_int))

# routes_admin_docs.py (o similar)
from flask import Blueprint, request, render_template, flash, redirect, url_for, session, current_app, abort
from datetime import datetime
from services.guia_service import inyectar_guia_con_sp


from flask import Blueprint, request, redirect, url_for, flash, session, current_app, abort
from datetime import datetime
from services.guia_service import leer_encabezado_nv_por_numnota, inyectar_guia_con_sp





# === Inyección de documentos (Admin) ===
@app.route('/admin/docs/inyeccion')
def admin_docs_inyeccion():
    cu = session.get('current_user') or {}
    if not (session.get('is_admin') and cu.get('rol') == ROL_JEFE):
        return abort(403)

    # Solo pendientes (puedes ampliar a ENVIADO si quieres)
    df = db.query_df_rima("""
        SELECT TOP 500
            ID, TIPO, NUMNOTA, RUT, RAZON_SOCIAL, CIUDAD, FECHA_DOC, ESTADO, CREATED_BY, CREATED_AT
        FROM dbo.DOC_OUTBOX
        WHERE ESTADO IN ('PENDIENTE')  -- cambia si quieres mostrar ENVIADO también
        ORDER BY CREATED_AT DESC
    """)
    docs = df.to_dict(orient='records')
    return render_template('admin/inyeccion.html', docs=docs)

# --- Inyección real en DOCU_DB (TIPODOC = 2) -------------------------------
from flask import jsonify

def _digits_only_int(val, default=0):
    try:
        s = ''.join(ch for ch in str(val) if ch.isdigit())
        return int(s) if s else default
    except Exception:
        return default

def _rut_str_clean(s):
    # quita puntos, mantiene dígito verificador si viene "12345678-9"
    s = (s or "").strip().upper().replace('.', '')
    return s

# --- Detalle de documento (vista) ------------------------------------------
from datetime import datetime
import json

from services.guia_service import leer_encabezado_nv_por_numnota, inyectar_guia_con_sp

@app.post("/admin/docs/inject/<int:doc_id>", endpoint="admin_docs_inject")
def admin_docs_inject(doc_id: int):
    cu = session.get('current_user') or {}
    if not (session.get('is_admin') and cu.get('rol') == ROL_JEFE):
        return abort(403)

    # 0) Engine de Santiago desde el módulo db
    engine = getattr(db, "ENGINE", None) or getattr(db, "engine", None)
    if engine is None:
        flash("No hay engine de Santiago configurado en db.ENGINE.", "error")
        return redirect(url_for("admin_docs_inyeccion"))

    # 1) Leer Outbox (RIMA) por ID para obtener TIPO, NUMNOTA y payloads JSON
    ob = db.query_df_rima("""
        SELECT TOP 1
            ID, TIPO, NUMNOTA, NUMGUIA,
            HEADER_JSON, ITEMS_JSON
        FROM dbo.DOC_OUTBOX
        WHERE ID = :id
    """, {"id": doc_id})
    if ob is None or ob.empty:
        flash(f"No existe el documento {doc_id} en la bandeja.", "warning")
        return redirect(url_for("admin_docs_inyeccion"))

    row = ob.iloc[0]
    tipo = (str(row.get("TIPO") or "")).upper()
    num_nota = int(''.join(ch for ch in str(row.get("NUMNOTA") or "") if ch.isdigit()) or "0")
    if num_nota <= 0:
        flash("NUMNOTA inválido en la bandeja.", "error")
        return redirect(url_for("admin_docs_inyeccion"))

    # 2) USERMODI (varchar(4))
    usuario4 = (cu.get('codigo') or cu.get('user') or cu.get('nombre') or 'WMS ')[:4]

    # 3) Ramas por TIPO
    if tipo == "GUIA":
        # === Inyección de Guía usando nuestro servicio (TIPODOC=2) ==========
        try:

            items_scaneados = (session.get('items_para_guia') or [])

            # crea la guía directamente desde la NV (usa numfact único y llena el ciclo)
            res = inyectar_guia_con_sp(
                engine=engine,
                num_nota=int(num_nota),
                usuario4=usuario4,          # varchar(4), ya armado arriba
                sucur_defecto=1222,         # ajusta si tu sucursal por defecto es otra
                afecta=None,                # autodetecta EXENTO/AFECTO desde la NV
                tasa_iva=19.0,
                insertar_detalle=True,      # copia detalle NOTDE_DB -> DOCDE_DB
                eselectr=1,                 # documento electrónico
                numempdo=None               # mapea por sucursal (guia_service.map_empresa_por_sucursal)
            )
            numreg  = int(res.get("numreg"))
            numfact = str(res.get("numfact"))

            # --- Marcar ENVIADO en outbox, guardando el correlativo visible ---
            try:
                cols_df = db.query_df_rima("""
                    SELECT UPPER(c.name) AS name
                    FROM sys.columns c
                    WHERE c.object_id = OBJECT_ID('dbo.DOC_OUTBOX')
                """)
                available = {str(n).upper() for n in cols_df["name"]} if cols_df is not None and not cols_df.empty else set()

                sql_parts = ["UPDATE dbo.DOC_OUTBOX SET ESTADO='ENVIADO'"]
                params = {"id": doc_id}

                if "NUMGUIA" in available:
                    sql_parts.append(", NUMGUIA = :g")
                    params["g"] = numfact
                if "NUMFACT" in available:
                    sql_parts.append(", NUMFACT = :f")
                    params["f"] = numfact
                if "ENVIADO_AT" in available:
                    sql_parts.append(", ENVIADO_AT = SYSUTCDATETIME()")
                if "ENVIADO_BY" in available:
                    sql_parts.append(", ENVIADO_BY = :by")
                    params["by"] = usuario4

                sql_parts.append(" WHERE ID = :id")
                db.execute_rima("\n".join(sql_parts), params)

            except Exception as e_upd:
                flash(f"⚠️ Guía creada (NUMREG {numreg}), pero no pude marcar ENVIADO en outbox: {e_upd}", "warning")

            flash(f"✅ Guía inyectada: G/D N° {numfact} (NUMREG {numreg}).", "success")

        except Exception as e:
            try:
                current_app.logger.exception("Error al inyectar guía")
            except Exception:
                pass
            flash(f"❌ Error al inyectar NUMNOTA {num_nota}: {e}", "error")

        return redirect(url_for('admin_docs_inyeccion'))


    elif tipo == "FACTURA":
        # === Inyección de Factura (TIPODOC=1) =============================
        import json
        from services.factura_service import inyectar_factura_con_sp

        # 3.2) Parsear payloads desde Outbox (operario)
        try:
            header = json.loads(row.get("HEADER_JSON") or "{}")
            items  = json.loads(row.get("ITEMS_JSON") or "[]")
        except Exception as e:
            flash(f"❌ HEADER_JSON/ITEMS_JSON inválidos para ID {doc_id}: {e}", "error")
            return redirect(url_for("admin_docs_inyeccion"))

        # 3.3) Armar payload para el servicio/SP
        payload = {
            "TIPODOC": 1,  # Factura Venta
            "NUMNOTA": header.get("NUMNOTA") or num_nota,
            "RUT":      header.get("RUT"),
            "RAZON":    header.get("RAZON_SOCIAL"),
            "FECHA":    header.get("FECHA_DOC"),
            "VENCE":    header.get("VENCIMIENTO"),
            "GLOSA":    header.get("GLOSA_PAGO"),
            "MONEDA":   header.get("MONEDA"),
            "VENDEDOR": header.get("CODVEND") or header.get("VENDEDOR"),
            "ITEMS":    items
        }

        try:
            res = inyectar_factura_con_sp(engine, payload, usuario4)
            numreg  = res.get("numreg")
            numfact = str(res.get("numfact") or "").strip()

            # --- BLOQUE ROBUSTO: marcar ENVIADO en outbox (y NUMFACT si existe) ---
            try:
                cols_df = db.query_df_rima("""
                    SELECT UPPER(c.name) AS name
                    FROM sys.columns c
                    WHERE c.object_id = OBJECT_ID('dbo.DOC_OUTBOX')
                """)
                available = {str(n).upper() for n in cols_df["name"]} if cols_df is not None and not cols_df.empty else set()

                sql_parts = ["UPDATE dbo.DOC_OUTBOX SET ESTADO='ENVIADO'"]
                params = {"id": doc_id}

                if "NUMFACT" in available:
                    sql_parts.append(", NUMFACT = :f")
                    params["f"] = numfact

                if "ENVIADO_AT" in available:
                    sql_parts.append(", ENVIADO_AT = SYSUTCDATETIME()")
                if "ENVIADO_BY" in available:
                    sql_parts.append(", ENVIADO_BY = :by")
                    params["by"] = usuario4

                sql_parts.append(" WHERE ID = :id")
                db.execute_rima("\n".join(sql_parts), params)
            except Exception as e_upd:
                flash(f"⚠️ Factura inyectada (NUMREG {numreg}), pero no pude marcar ENVIADO en outbox: {e_upd}", "warning")

            if numfact:
                flash(f"✅ Factura inyectada: N° {numfact} (NUMREG {numreg}).", "success")
            else:
                flash(f"✅ Factura inyectada (NUMREG {numreg}).", "success")

        except Exception as e:
            try:
                current_app.logger.exception("Error al inyectar factura")
            except Exception:
                pass
            flash(f"❌ Error al inyectar FACTURA para NUMNOTA {num_nota}: {e}", "error")

        return redirect(url_for('admin_docs_inyeccion'))

    else:
        # Tipos no soportados (por ahora)
        flash(f"Tipo de documento no soportado: {tipo or '(desconocido)'}", "warning")
        return redirect(url_for("admin_docs_inyeccion"))



# Fallback GET para dispositivos RF (Pocket IE / Windows CE)
@app.get("/admin/docs/inject/<int:doc_id>/go", endpoint="admin_docs_inject_go")
def admin_docs_inject_go(doc_id: int):
    # Reutiliza exactamente la misma lógica de inyección
    return admin_docs_inject(doc_id)


# --- Detalle de documento (mínimo para que no rompa el enlace) -------------
from datetime import datetime
from flask import abort, session, flash, redirect, url_for, render_template

@app.get("/admin/docs/detalle/<int:doc_id>", endpoint="admin_docs_detalle")
def admin_docs_detalle(doc_id: int):
    cu = session.get('current_user') or {}
    if not (session.get('is_admin') and cu.get('rol') == ROL_JEFE):
        return abort(403)

    # 1) Leer la fila de la outbox (RIMA)
    ob = db.query_df_rima("""
        SELECT TOP 1 *
        FROM dbo.DOC_OUTBOX
        WHERE ID = :id
    """, {"id": doc_id})

    if ob is None or ob.empty:
        flash(f"No existe el documento {doc_id} en la bandeja.", "warning")
        return redirect(url_for("admin_docs_inyeccion"))

    row = ob.iloc[0].to_dict()

    # 2) Traer encabezado + detalle desde la NV real (Santiago)
    try:
        header_nv, detalles_nv = db.get_guia_desde_nv(str(row.get("NUMNOTA")))
    except Exception:
        header_nv, detalles_nv = ({}, [])

    # 3) Completar/override con datos de la outbox
    header = {
        **(header_nv or {}),
        "GD_NUM":     row.get("NUMGUIA") or (header_nv.get("GD_NUM") if header_nv else ""),
        "OC_NUM":     (header_nv.get("OC_NUM") if header_nv else ""),
        "FAV_RUT":    row.get("RUT") or (header_nv.get("FAV_RUT") if header_nv else ""),
        "FAV_RAZSOC": row.get("RAZON_SOCIAL") or (header_nv.get("FAV_RAZSOC") if header_nv else ""),
    }
    detalles = detalles_nv or []

    datos = {}
    refs = {
        "oc_ref": "", "fecha_oc_ref": "",
        "hes_ref": "", "fecha_hes_ref": "",
        "numero_contrato": "",
        "numero_pedido": str(row.get("NUMNOTA") or "")
    }
    obsfact_inicial = ""
    scans = []
    retira_cliente = False
    pago_a_dias = 0
    fecha_doc = row.get("FECHA_DOC") or ""
    neto = 0.0
    otros_imp = 0.0

    return render_template(
        "admin/doc_detalle.html",
        num_nota=row.get("NUMNOTA"),
        header=header,
        datos=datos,
        detalles=detalles,
        refs=refs,
        obsfact_inicial=obsfact_inicial,
        scans=scans,
        retira_cliente=retira_cliente,
        pago_a_dias=pago_a_dias,
        fecha_doc=fecha_doc,
        neto=neto,
        otros_imp=otros_imp,
        datetime=datetime
    )


@app.context_processor
def inject_roles():
    """Hace disponibles las constantes de roles en las plantillas."""
    return {"ROL_JEFE": ROL_JEFE, "ROL_OPERARIO": ROL_OPERARIO}

# ---------- Decorador admin_required ----------
from functools import wraps
def admin_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("is_admin"):
            flash("Requiere rol administrador.", "error")
            return redirect(url_for("login1"))
        return f(*args, **kwargs)
    return wrapper
# ---------------------------------------------

# --- Directorios y rutas de archivos ---
BASE_DIR     = os.path.dirname(__file__)
DATA_DIR     = os.path.join(BASE_DIR, 'data')
os.makedirs(DATA_DIR, exist_ok=True)

GUIDE_FOLDER = os.path.join(DATA_DIR, 'guides')
os.makedirs(GUIDE_FOLDER, exist_ok=True)

UPLOADS_DIR  = os.path.join(DATA_DIR, 'uploads')
os.makedirs(UPLOADS_DIR, exist_ok=True)

STOCK_FILE    = os.path.join(DATA_DIR, 'stock.csv')
OC_FILE       = os.path.join(DATA_DIR, 'oc_pendientes.csv')
NV_FILE       = os.path.join(DATA_DIR, 'nv.csv')      # Notas de venta
FACTURA_FILE  = os.path.join(DATA_DIR, 'facturas_compra.csv')
MASTER_FILE   = os.path.join(DATA_DIR, 'productos_maestra.csv')

INV_SESIONES_FILE = os.path.join(DATA_DIR, 'inv_sesiones.csv')

ALLOWED_EXT  = {'csv', 'xls', 'xlsx'}
FIELDNAMES   = ['codigo_producto', 'cantidad', 'ultima_actualizacion']

# --- Integración con la base de datos ---
def fetch_oc_items(num_oc):
    """Obtiene detalle de una OC usando db_utils.get_oc_detalle.

    Retorna un DataFrame con columnas estandarizadas y el número de guía
    si está disponible.
    """
    rows = get_oc_detalle(num_oc)
    guia = rows[0].get('num_guia') if rows else None
    df = pd.DataFrame([
        {
            'codigo': r.get('codigo'),
            'nombre': r.get('nombre'),
            'cantidad': r.get('cantidad'),
            'prec_unit': r.get('prec_unit'),
        }
        for r in rows
    ])
    return df, guia

def norm_code(x):
    # quita espacios y * de Code39; pasa a mayúsculas
    return str(x).strip().strip('*').upper()

def group_by_code(df):
    """Agrupa filas por código de producto sumando sus cantidades.

    Se detectan las columnas típicas de código, nombre, cantidad y precio
    unitario y se combinan las filas duplicadas del mismo producto para que
    se muestren apiladas en lugar de repetidas.
    """
    if df is None or df.empty:
        return df

    cols = {c.lower(): c for c in df.columns}
    code_col = cols.get('codigo') or cols.get('código')
    if not code_col:
        return df

    name_col = cols.get('nombre')
    qty_col = cols.get('cantidad') or cols.get('cant.') or cols.get('cant')
    price_col = (
        cols.get('prec_unit') or cols.get('precio unitario') or cols.get('prec.unit.')
    )

    if qty_col:
        df[qty_col] = pd.to_numeric(df[qty_col], errors='coerce').fillna(0)

    agg = {qty_col: 'sum'} if qty_col else {}
    if name_col:
        agg[name_col] = 'first'
    if price_col:
        agg[price_col] = 'first'

    if agg:
        df = df.groupby(code_col, as_index=False).agg(agg)

    cols_order = [code_col]
    if name_col:
        cols_order.append(name_col)
    if qty_col:
        cols_order.append(qty_col)
    if price_col:
        cols_order.append(price_col)

    return df[cols_order]

# --- Funciones auxiliares ---
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT

def cargar_stock():
    if not os.path.exists(STOCK_FILE):
        return []
    with open(STOCK_FILE, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def guardar_stock(stock_list):
    with open(STOCK_FILE, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        w.writerows(stock_list)

def append_guide_entry(guia, codigo, cantidad, timestamp):
    path = os.path.join(GUIDE_FOLDER, f'guia_{guia}.csv')
    exists = os.path.exists(path)
    with open(path, 'a', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        if not exists:
            w.writerow(['guia','codigo_producto','cantidad','fecha_hora'])
        w.writerow([guia, codigo, cantidad, timestamp])


def inv_create_session():
    """Crea un registro de sesión de inventario y retorna su ID."""
    now = datetime.now().strftime('%Y%m%d%H%M%S')
    record = {'id': now, 'estado': 'EN_PROCESO', 'creado': datetime.now().isoformat()}
    exists = os.path.exists(INV_SESIONES_FILE)
    with open(INV_SESIONES_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['id', 'estado', 'creado'])
        if not exists:
            writer.writeheader()
        writer.writerow(record)
    return record['id']


def inv_get_session(sid):
    """Obtiene la información de una sesión de inventario por ID."""
    if not os.path.exists(INV_SESIONES_FILE):
        return None
    with open(INV_SESIONES_FILE, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            if row.get('id') == sid:
                return row
    return None

# app.py (o admin_routes.py)
from flask import request, jsonify, session
from db_utils import (
    get_nv_flow,
    set_nv_estado_aprobada,
    set_nv_estado_pendiente,
    set_nv_zona,
    set_nv_retira,
)

from auth_map import ROL_JEFE  # Asegúrate que exista este alias

def _es_admin():
    cu = session.get('current_user') or {}
    # Requiere flag de admin y rol de jefe
    return bool(session.get('is_admin') and cu.get('rol') == ROL_JEFE)

def _back_to_gestionar():
    """Vuelve a /admin/nv/gestionar (o a 'next' si viene en el form)."""
    return redirect(request.form.get("next") or request.referrer or url_for("admin_nv_gestionar"))

@app.post("/admin/nv/<int:num_nota>/aprobar")
def admin_nv_aprobar(num_nota):
    if not _es_admin():
        flash("No autorizado.", "error")
        return _back_to_gestionar()

    set_nv_estado_aprobada(
        num_nota,
        aprobado_por=(session.get('current_user') or {}).get('nombre')
    )
    flash(f"NV {num_nota} aprobada.", "success")
    return _back_to_gestionar()

@app.post("/admin/nv/<int:num_nota>/pendiente")
def admin_nv_pendiente(num_nota):
    if not _es_admin():
        flash("No autorizado.", "error")
        return _back_to_gestionar()

    set_nv_estado_pendiente(num_nota)
    flash(f"NV {num_nota} marcada como pendiente.", "info")
    return _back_to_gestionar()

# =========================
# Helpers NV_ZONAS / RIMA
# =========================

def _rima_execute(sql: str, params=None):
    """Ejecuta contra RIMA si está disponible; si no, usa la conexión por defecto."""
    try:
        return db.execute_rima(sql, params or {})
    except Exception:
        return db.execute(sql, params or {})

def _rima_query_df(sql: str, params=None):
    """Query contra RIMA si está disponible; si no, usa la conexión por defecto."""
    try:
        return db.query_df_rima(sql, params or {})
    except Exception:
        return db.query_df(sql, params or {})

def _ensure_nv_zonas_table() -> None:
    """
    Garantiza la existencia de RIMA.dbo.NV_ZONAS con el esquema esperado.
    NUMREG = NOTV_DB.NUMREG, ID_ZONA = ZONAS_DB.ID
    """
    try:
        _rima_execute("""
        IF OBJECT_ID('dbo.NV_ZONAS','U') IS NULL
        BEGIN
            CREATE TABLE dbo.NV_ZONAS(
                ID          INT IDENTITY(1,1) PRIMARY KEY,
                NUMREG      INT          NOT NULL,     -- NOTV_DB.NUMREG
                ID_ZONA     INT          NOT NULL,     -- ZONAS_DB.ID
                USUARIO     NVARCHAR(50) NULL,
                FECHA_ASIG  DATETIME2    NOT NULL DEFAULT SYSUTCDATETIME()
            );
            CREATE INDEX IX_NV_ZONAS_NUMREG ON dbo.NV_ZONAS(NUMREG);
        END;
        """)
    except Exception as e:
        current_app.logger.warning("No se pudo validar/crear RIMA.dbo.NV_ZONAS: %s", e)

def _asignar_zona_impl(num_nota: int, zona_id: int, usuario: str = "") -> None:
    """
    Lógica común de asignación:
    - Obtiene NUMREG desde NOTV_DB por NUMNOTA.
    - UPSERT en RIMA.dbo.NV_ZONAS.
    - Actualiza NV_FLOW (opción B) vía db_utils.set_nv_zona.
    """
    # 1) NUMREG
    df = db.query_df("""
        SELECT TOP 1 NUMREG
        FROM dbo.NOTV_DB
        WHERE NUMNOTA = :n
    """, {"n": int(num_nota)})
    if df is None or df.empty:
        raise RuntimeError(f"No se encontró NUMREG para la NV {num_nota}.")
    numreg = int(df.iloc[0]["NUMREG"])

    # 2) Asegurar tabla y UPSERT
    _ensure_nv_zonas_table()
    _rima_execute("""
        MERGE dbo.NV_ZONAS AS tgt
        USING (SELECT :numreg AS NUMREG) AS src
        ON (tgt.NUMREG = src.NUMREG)
        WHEN MATCHED THEN
            UPDATE SET
                ID_ZONA    = :zona,
                USUARIO    = :usr,
                FECHA_ASIG = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (NUMREG, ID_ZONA, USUARIO)
            VALUES (:numreg, :zona, :usr);
    """, {"numreg": numreg, "zona": int(zona_id), "usr": usuario or ""})

    # 3) Actualizar también NV_FLOW para consistencia
    try:
        db_utils.set_nv_zona(num_nota=int(num_nota), zona_id=int(zona_id), asignado_por=usuario or None)
    except Exception as e:
        # No hacemos fail hard: dejamos registro y seguimos
        current_app.logger.warning("No se pudo actualizar NV_FLOW para NV %s: %s", num_nota, e)


# =========================
# Endpoints de asignación
# =========================

@app.post("/admin/nv/<int:num_nota>/asignar", endpoint="admin_nv_asignar")
def admin_nv_asignar(num_nota: int):
    """
    Variante moderna (AJAX/fetch) usada por el botón 'Asignar' en la grilla.
    Recibe: zona_id (en FormData).
    Devuelve: 204 si OK, 400 si falta zona, 4xx/5xx si error.
    """
    cu = session.get('current_user') or {}
    if not (session.get('is_admin') and cu.get('rol') == ROL_JEFE):
        return abort(403)

    zona_raw = (request.form.get("zona_id") or "").strip()
    if not zona_raw:
        return ("Debes seleccionar una zona.", 400)
    try:
        zona_id = int(zona_raw)
    except Exception:
        return ("Zona inválida.", 400)

    try:
        _asignar_zona_impl(num_nota=num_nota, zona_id=zona_id, usuario=(cu.get("nombre") or cu.get("user") or ""))
    except Exception as e:
        current_app.logger.exception("admin_nv_asignar: error")
        return (f"Error asignando la zona: {e}", 500)

    # Soporta redirección si vino 'next' desde un form tradicional
    nxt = request.form.get("next")
    if nxt:
        return redirect(nxt)

    # Para fetch(): 204 = OK sin contenido, el front recarga.
    return ("", 204)


@app.post("/admin/nv/asignar", endpoint="admin_nv_asignar_form")
def admin_nv_asignar_form():
    """
    Variante LEGADA (form tradicional) para compatibilidad.
    Acepta:
      - nv        (número de nota, int)
      - zona      (nombre de la zona)  [opcional si viene zona_id]
      - zona_id   (ID entero de la zona)
      - retira_cliente (0/1)  ← se guarda junto con la zona
    """
    cu = session.get('current_user') or {}
    if not (session.get('is_admin') and cu.get('rol') == ROL_JEFE):
        return abort(403)

    nv_raw   = (request.form.get("nv") or "").strip()
    zona_txt = (request.form.get("zona") or "").strip()
    zona_raw = (request.form.get("zona_id") or "").strip()

    # —— leer retira_cliente soportando hidden+checkbox o solo checkbox
    rc_list = request.form.getlist("retira_cliente")
    # ej. ["0","1"] o ["1"] o ["0"] o []
    retira_cliente = 1 if any(v in ("1", "true", "on") for v in rc_list[-1:]) else 0

    # Validar NV
    if not nv_raw:
        flash("Falta el N° de Nota de Venta.", "warning")
        return redirect(url_for("admin_nv_gestionar"))
    try:
        num_nota = int(nv_raw)
    except Exception:
        flash("Número de Nota inválido.", "warning")
        return redirect(url_for("admin_nv_gestionar"))

    # Resolver zona_id
    zona_id = None
    if zona_raw:
        try:
            zona_id = int(zona_raw)
        except Exception:
            zona_id = None

    if zona_id is None and zona_txt:
        # Buscar ID de la zona por nombre (case-insensitive)
        df_z = _rima_query_df("""
            SELECT TOP 1 ID
            FROM dbo.ZONAS_DB
            WHERE UPPER(LTRIM(RTRIM(NOMBRE))) = UPPER(LTRIM(RTRIM(:n)))
        """, {"n": zona_txt})
        if df_z is not None and not df_z.empty:
            zona_id = int(df_z.iloc[0]["ID"])

    if zona_id is None:
        flash("Faltan datos para asignar zona (selecciona una zona válida).", "warning")
        return redirect(url_for("admin_nv_gestionar"))

    # Helper local para ejecutar en RIMA con el método disponible en tu proyecto
    def _exec_rima(sql, params):
        fn = globals().get("_rima_execute") or getattr(db, "execute_rima", None) or getattr(db, "exec_rima", None)
        if fn:
            return fn(sql, params)
        # Como último recurso, algunos proyectos aceptan DML con _rima_query_df
        qf = globals().get("_rima_query_df")
        if qf:
            return qf(sql, params)
        raise RuntimeError("No se encontró ejecutor para RIMA (execute_rima/_rima_execute).")

    try:
        # 1) Usa tu implementación existente para la zona (por compatibilidad)
        _asignar_zona_impl(
            num_nota=num_nota,
            zona_id=zona_id,
            usuario=(cu.get("nombre") or cu.get("user") or "")
        )

        # 2) Asegura que RETIRA_CLIENTE quede persistido junto a la zona (upsert)
        _exec_rima("""
            MERGE dbo.NV_ZONAS AS tgt
            USING (SELECT CAST(:nv AS INT) AS NUMNOTA) AS src
               ON tgt.NUMNOTA = src.NUMNOTA
            WHEN MATCHED THEN
                UPDATE SET
                    ZONA_ID = :zona_id,
                    RETIRA_CLIENTE = :retira,
                    FECHAMODIF = GETDATE(),
                    USERMODI = :user
            WHEN NOT MATCHED THEN
                INSERT (NUMNOTA, ZONA_ID, RETIRA_CLIENTE, FECHACREA, USERCREA)
                VALUES (CAST(:nv AS INT), :zona_id, :retira, GETDATE(), :user);
        """, {
            "nv": num_nota,
            "zona_id": zona_id,
            "retira": retira_cliente,
            "user": (cu.get("nombre") or cu.get("user") or "")
        })

        flash(f"NV {num_nota}: zona asignada y 'Retira cliente' guardado.", "success")

    except Exception as e:
        current_app.logger.exception("admin_nv_asignar_form: error")
        flash(f"No se pudo asignar la NV {num_nota}: {e}", "danger")

    return redirect(url_for("admin_nv_gestionar"))



# --- Detector RF: se ejecuta en TODAS las requests ---
@app.before_request
def detect_rf_device():
    ua = (request.headers.get('User-Agent') or '').lower()

    # Heurística para Symbol/Motorola/Zebra + IE/Windows CE
    rf_tokens = [
        'windows ce', 'iemobile', 'msie', 'ppc',       # IE móvil / CE
        'symbol', 'motorola', 'zebra', 'mc32', 'mc32n0'
    ]

    # 1) Permite forzar por query param una vez y lo recuerda en sesión
    if 'rf' in request.args:
        session['is_rf'] = request.args.get('rf') in ('1', 'true', 'yes', 'on')

    # 2) Autodetección si aún no está definido
    if 'is_rf' not in session and any(t in ua for t in rf_tokens):
        session['is_rf'] = True

    # 3) Exponer a templates vía g (y evitar crashear si algo falla)
    try:
        g.is_rf = bool(session.get('is_rf', False))
    except Exception:
        pass


# --- Rutas opcionales para forzar ON/OFF desde cualquier dispositivo ---
@app.route('/rf/<state>')
def rf_switch(state):
    session['is_rf'] = (state.lower() in ('on', '1', 'true', 'yes'))
    # vuelve a la página anterior o al index
    return redirect(request.referrer or url_for('index'))


@app.post("/salida/preparar_guia")
def salida_preparar_guia():
    from flask import request, session, redirect, url_for, flash
    # ---- helpers -----------------------------------------------------------
    def _first_key(d: dict, keys):
        for k in keys:
            if k in d and d[k] is not None:
                return k
        return None

    def _to_int(x, default=0):
        try:
            return int(float(str(x).replace(',', '.')))
        except Exception:
            return default

    go = (request.args.get("go") or request.form.get("go") or "").lower()

    # Nº de NV desde el form (si lo envías) o desde sesión
    nv = (request.form.get("num_nota") or session.get("current_nv") or "").strip()

    # 1) Intentar obtener la lista principal desde sesión
    candidatos_lista = [
        "salida_items", "items_salida", "items_scan", "scan_items",
        "detalle_salida", "salida_detalle", "escaneados", "escaneos_list"
    ]
    salida_items = None
    for k in candidatos_lista:
        if isinstance(session.get(k), list) and session.get(k):
            salida_items = session.get(k)
            break
    if salida_items is None:
        # aunque no exista o esté vacía, definimos lista
        salida_items = session.get("salida_items", []) or []

    # 2) Normalizar a [{codigo, cantidad}, ...]
    code_keys = [
        "codigo", "Código", "Codigo", "cod", "sku", "SKU",
        "item", "Articulo", "articulo", "CodigoBarra", "CodBarra", "cod_barra", "barra"
    ]
    qty_keys = [
        "cantidad", "Cant.Salida", "CantSalida", "cant_salida",
        "salida", "Scan", "Escaneado", "escaneado",
        "Despachado", "CantDesp", "cant_desp", "CantidadEscaneada"
    ]

    items = []
    if isinstance(salida_items, list):
        for row in salida_items:
            if not isinstance(row, dict):
                continue
            kc = _first_key(row, code_keys)
            kq = _first_key(row, qty_keys)
            if not kc or not kq:
                continue
            codigo = str(row.get(kc) or "").strip()
            qty = _to_int(row.get(kq), 0)
            if codigo and qty > 0:
                items.append({"codigo": codigo, "cantidad": qty})

    # 3) Fallback: a veces guardan solo una lista de códigos escaneados
    if not items:
        candidatos_codigos = ["escaneos", "scans", "scan_list", "codigos_scaneados", "barcodes"]
        codigos = None
        for k in candidatos_codigos:
            if isinstance(session.get(k), list) and session.get(k):
                codigos = session.get(k)
                break
        if codigos:
            # agrupa por código (cada aparición = 1)
            cont = {}
            for c in codigos:
                c = str(c).strip()
                if c:
                    cont[c] = cont.get(c, 0) + 1
            items = [{"codigo": c, "cantidad": q} for c, q in cont.items() if q > 0]

    # Logs útiles para depurar (los verás en consola del servidor)
    try:
        app.logger.info("PREP_GUIA: nv=%s, fuente=%s, registros_fuente=%s, items_normalizados=%s",
                        nv,
                        'lista' if salida_items else 'ninguna',
                        len(salida_items) if isinstance(salida_items, list) else 0,
                        len(items))
    except Exception:
        pass

    # 4) Validación: si no hay nada escaneado, volvemos a /salida
    if not items:
        flash("No hay productos preparados para la guía (no se han escaneado ítems).", "warning")
        return redirect(url_for("salida"))

    # 5) Guardar en sesión lo que la guía espera
    session["items_para_guia"] = items
    session["nv_para_guia"] = nv

        # 6) Redirigir según el destino pedido
    if go == "factura":
        # Necesitamos num_nota para factura
        if nv:
            return redirect(url_for("factura_nv", num_nota=nv))
        flash("Falta el número de Nota de Venta para la factura.", "warning")
        return redirect(url_for("finalizar_salida"))

    # default → guía
    if nv:
        return redirect(url_for("guia_despacho", num_nota=nv))
    return redirect(url_for("guia_despacho"))




@app.post("/admin/nv/<int:num_nota>/retira")
def admin_nv_retira(num_nota):
    if not _es_admin():
        flash("No autorizado.", "error")
        return _back_to_gestionar()

    retira = (request.form.get("retira") in ("1", "true", "on", "True"))
    set_nv_retira(num_nota, retira)
    flash(("Marcado como 'retira cliente'." if retira else "Marcado como 'no retira'."), "success")
    return _back_to_gestionar()


# --- Rutas ---
@app.route('/')
def index():
    """Página principal para operarios una vez autenticados."""
    cu = session.get('current_user')
    if not cu:
        # No hay sesión, ir al primer nivel de login
        return redirect(url_for('login1'))

    # Jefe: mostrar su panel directamente en "/"
    if cu.get('rol') == ROL_JEFE:
        return render_template('admin/index.html')  # <-- esto fuerza admin


    # Si no se ha autenticado como operario aún, volver al login de nivel 2
    if not session.get('operario'):
        return redirect(url_for('login2'))

    # Operario autenticado, mostrar menú principal
    return render_template('index.html')


@app.route('/panel_jefe')
def panel_jefe():
    """Página principal para el Jefe de Bodega."""
    cu = session.get('current_user')
    if not cu or cu.get('rol') != ROL_JEFE:
        return redirect(url_for('login1'))
    return redirect(url_for('admin_index'))


@app.route('/login1', methods=['GET', 'POST'])
def login1():
    # Si ya hay alguien en sesión, enrutar según rol
    cu = session.get('current_user')
    if cu:
        if cu.get('rol') == ROL_JEFE:
            session['is_admin'] = True
            return redirect(url_for('index'))
        session['is_admin'] = False
        return redirect(url_for('login2'))

    if request.method == 'POST':
        usuario_input = (request.form.get('usuario') or '').strip()
        clave_input   = (request.form.get('clave')   or '').strip()

        from auth_service import login_usuario
        u = login_usuario(usuario_input, clave_input)

        if not u:
            flash(f"No se encontró el usuario '{usuario_input}' en USERS_DB o la clave es inválida.", "error")
            return render_template('login1.html', usuarios=LOGIN1_USUARIOS)

        # Guarda sesión y rutea según rol
        session['current_user'] = {
            'usuario': u['usuario'],
            'nombre':  u['nombre'],
            'rol':     u['rol'],
        }

        # Admin si rol == ROL_JEFE
        if u['rol'] == ROL_JEFE:
            session['is_admin'] = True
            return redirect(url_for('index'))

        # Caso contrario va a Login 2 (operario)
        session['is_admin'] = False
        return redirect(url_for('login2'))

    # GET → renderizar formulario
    return render_template('login1.html', usuarios=LOGIN1_USUARIOS)


@app.route('/login2', methods=['GET', 'POST'])
def login2():
    cu = session.get('current_user')
    if not cu:
        return redirect(url_for('login1'))
    if cu.get('rol') != ROL_OPERARIO:
        return redirect(url_for('panel_jefe'))

    if request.method == 'POST':
        codigo       = (request.form.get('codigo') or '').strip()
        clave_nombre = (request.form.get('clave_nombre') or '').strip()

        op = login_nivel2_operario(codigo, clave_nombre)
        if not op:
            flash('Código o clave de operario inválidos.', 'error')
            return render_template('login2.html')

        session['operario'] = op
        # Después de un login exitoso, llevar al menú principal
        return redirect(url_for('index'))

    return render_template('login2.html')


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login1'))


@app.route('/admin')
def admin_index():
    """Panel principal de administración.

    Requiere que el usuario esté autenticado como administrador. En modo
    desarrollo es posible acceder pasando ``?key=`` con la clave definida en
    la variable de entorno ``ADMIN_KEY`` (``admin123`` por defecto)."""
    key = request.args.get('key')
    if key and key == os.environ.get('ADMIN_KEY', 'admin123'):
        session['is_admin'] = True

    if not session.get('is_admin'):
        return abort(403)

    return render_template('admin/menu.html')


# --- ZONAS: endpoints mínimos para que url_for no falle ---

@app.get("/admin/zonas")
def admin_zonas():
    cu = session.get('current_user') or {}
    if not (session.get('is_admin') and cu.get('rol') == ROL_JEFE):
        return abort(403)
    _ensure_zonas_table()
    _seed_zonas_if_empty()
    zonas = _get_zonas()
    return render_template("admin/zonas_admin.html", zonas=zonas)

@app.post("/admin/zonas/add")
def zonas_admin_add():
    cu = session.get('current_user') or {}
    if not (session.get('is_admin') and cu.get('rol') == ROL_JEFE):
        return abort(403)

    nombre = (request.form.get("nombre") or "").strip()
    if not nombre:
        flash("Ingrese un nombre de zona.", "error")
        return redirect(url_for("admin_zonas"))

    _ensure_zonas_table()
    try:
        exists = db.query_df_rima(
            "SELECT 1 FROM dbo.ZONAS_DB WHERE UPPER(LTRIM(RTRIM(NOMBRE))) = UPPER(:n)",
            {"n": nombre}
        )
        if exists.empty:
            db.execute_rima("INSERT INTO dbo.ZONAS_DB (NOMBRE) VALUES (:n)", {"n": nombre})
            flash("Zona agregada.", "success")
        else:
            flash("La zona ya existe.", "info")
    except Exception as e:
        flash(f"No se pudo agregar la zona: {e}", "error")

    return redirect(url_for("admin_zonas"))

@app.post("/admin/zonas/<int:zona_id>/update")
def zonas_admin_update(zona_id: int):
    cu = session.get('current_user') or {}
    if not (session.get('is_admin') and cu.get('rol') == ROL_JEFE):
        return abort(403)

    nuevo_nombre = (request.form.get("nombre") or "").strip()
    if not nuevo_nombre:
        flash("Ingrese un nombre válido.", "error")
        return redirect(url_for("admin_zonas"))

    try:
        db.execute_rima("""
            UPDATE dbo.ZONAS_DB
               SET NOMBRE = :n
             WHERE ID = :i
        """, {"n": nuevo_nombre, "i": zona_id})
        flash("Zona actualizada.", "success")
    except Exception as e:
        flash(f"No se pudo actualizar: {e}", "error")

    return redirect(url_for("admin_zonas"))

@app.post("/admin/zonas/delete")
def zonas_admin_delete():
    cu = session.get('current_user') or {}
    if not (session.get('is_admin') and cu.get('rol') == ROL_JEFE):
        return abort(403)

    zid = request.form.get("id", type=int)
    if not zid:
        flash("Falta ID de zona a eliminar.", "warning")
        return redirect(url_for("admin_zonas"))

    try:
        db.execute_rima("DELETE FROM dbo.ZONAS_DB WHERE ID = :i", {"i": zid})
        flash("Zona eliminada.", "success")
    except Exception as e:
        flash(f"No se pudo eliminar la zona: {e}", "danger")

    return redirect(url_for("admin_zonas"))


@app.get("/admin/tipos-despacho")
def admin_tipos_despacho():
    cu = session.get('current_user') or {}
    if not (session.get('is_admin') and cu.get('rol') == ROL_JEFE):
        return abort(403)
    return "<h1>Gestionar tipo despacho</h1><p><a href='{}'>Volver al Menú Admin</a></p>".format(url_for('admin_index'))

@app.get("/admin/notas-venta")
def admin_notas_venta():
    cu = session.get('current_user') or {}
    if not (session.get('is_admin') and cu.get('rol') == ROL_JEFE):
        return abort(403)
    return "<h1>Gestionar notas de venta</h1><p><a href='{}'>Volver al Menú Admin</a></p>".format(url_for('admin_index'))


@app.route('/admin/listados')
@admin_required
def admin_listados():
    return render_template('admin/listados.html')


@app.route('/devoluciones')
def devoluciones():
    return render_template('devoluciones.html')

# --- FACTURA (desde NV) -------------------------------------------------------
from flask import request

# --- FACTURA (desde NV) -------------------------------------------------------
from flask import request

@app.route("/factura", methods=["GET", "POST"], endpoint="factura_nv")
def factura_nv():
    cu = session.get('current_user'); op = session.get('operario')
    if not cu: 
        return redirect(url_for('login1'))
    if cu.get('rol') == ROL_OPERARIO and not op:
        return redirect(url_for('login2'))

    num_nota = (request.args.get("num_nota") or request.form.get("num_nota") or "").strip()
    if not num_nota:
        flash("Falta el número de Nota de Venta (num_nota).", "warning")
        return redirect(url_for("listado_nv"))

    # 1) Cabecera FAV desde NOTV_DB (joins a CLIEN_DB y PERSO_DB)
    try:
        df_fav = db.get_fav_header(num_nota)
        if df_fav is None or df_fav.empty:
            flash(f"No se encontró la NV {num_nota}.", "warning")
            return redirect(url_for("listado_nv"))
        fav = df_fav.iloc[0].to_dict()
        # Normaliza FAV_NRO (por si viene como float)
        if fav.get("FAV_NRO") is not None:
            try:
                fav["FAV_NRO"] = str(int(float(fav["FAV_NRO"])))
            except Exception:
                fav["FAV_NRO"] = str(fav["FAV_NRO"])
    except Exception as e:
        flash(f"Error al consultar la BBDD: {e}", "danger")
        return redirect(url_for("listado_nv"))

    # 2) Ítems SOLO escaneados en esta salida (igual que guía)
    items_scan = session.get('items_para_guia') or session.get('salida_items') or []
    if not items_scan:
        flash("No hay productos preparados (no se han escaneado ítems).", "warning")
        return redirect(url_for("listado_nv"))

    # 3) GET -> mostrar prefill; POST -> enviar a outbox (para inyección del Admin)
    if request.method == "GET":
        return render_template("factura_nv.html", fav=fav, detalles=items_scan, num_nota=num_nota)

    # POST: guardar en outbox para que el Admin inyecte TIPODOC=1
    from datetime import datetime
    import json

    # Armar header mínimo estándar para outbox (solo campos confirmados)
    header = {
        "TIPODOC": 1,  # 1 = Factura Venta
        "NUMNOTA": int(num_nota),
        # FAV A (Facturar a)
        "RUT":          (fav.get("FAV_RUT") or "").strip(),
        "RAZON_SOCIAL": (fav.get("FAV_RAZSOC") or "").strip(),
        # Otros confirmados
        "GLOSA_PAGO":   fav.get("GLOSA_PAGO"),
        "VENDEDOR":     (fav.get("VENDEDOR") or "").strip(),
        "CODVEND":      fav.get("CODVEND"),
        # Fechas: por ahora no las traemos de BBDD; FECHA_DOC = hoy, VENCIMIENTO vacío
        "FECHA_DOC":    str(datetime.now().date()),
        "VENCIMIENTO":  ""
    }

    # Normalizar items a [{codigo, cantidad, prec_unit?}]
    detalles = []
    for it in items_scan:
        cod = str(it.get("codigo") or it.get("Código") or "").strip()
        qty = int(it.get("cantidad") or it.get("Cant.Salida") or 0)
        if not cod or qty <= 0:
            continue
        detalles.append({
            "codigo": cod,
            "cantidad": qty,
            # opcional: si tienes precio de base en sesión/consulta, agrégalo:
            "prec_unit": it.get("prec_unit") or it.get("Prec.Unit.") or 0
        })

    if not detalles:
        flash("La Factura no tiene líneas válidas para enviar.", "warning")
        return redirect(url_for("finalizar_salida"))

    # Insert a DOC_OUTBOX (TIPO='FACTURA')
    try:
        db.execute_rima("""
            INSERT INTO dbo.DOC_OUTBOX
                (TIPO, NUMNOTA, NUMGUIA, RUT, RAZON_SOCIAL, CIUDAD, FECHA_DOC,
                 HEADER_JSON, ITEMS_JSON, ESTADO, CREATED_BY)
            VALUES
                ('FACTURA', :num_nota, NULL, :rut, :razon, :ciudad, :fdoc,
                 :hjson, :ijson, 'PENDIENTE', :by)
        """, {
            "num_nota": int(num_nota),
            "rut": header["RUT"],
            "razon": header["RAZON_SOCIAL"],
            "ciudad": "",  # si luego quieres, puedes traer SUCUR/CIUDAD desde NOTV_DB
            "fdoc": header["FECHA_DOC"],
            "hjson": json.dumps(header, ensure_ascii=False),
            "ijson": json.dumps(detalles, ensure_ascii=False),
            "by": (cu.get('nombre') or cu.get('usuario') or 'operario')
        })
    except Exception as e:
        flash(f"Error guardando Factura en la bandeja de envío: {e}", "danger")
        return redirect(url_for("finalizar_salida"))

    flash("Factura enviada al Jefe de Bodega para inyección.", "success")
    return redirect(url_for("finalizar_salida"))


@app.route('/devoluciones/ingreso', methods=['GET', 'POST'])
def devolucion_ingreso():
    cu = session.get('current_user')
    op = session.get('operario')
    if not cu:
        return redirect(url_for('login1'))
    if cu.get('rol') == ROL_OPERARIO and not op:
        return redirect(url_for('login2'))
    return ingreso_core(
        'devoluciones_ingreso.html',
        'devolucion_ingreso',
        data_file=FACTURA_FILE,
        query_param='factura',
        field_name='No. Factura',
        label='Factura',
        search_action='buscar_factura',
        session_keys={'num': 'dev_current_factura',
                      'guia': 'dev_current_guia',
                      'items': 'dev_factura_items',
                      'scanned': 'dev_scanned'},
        context_keys={'num': 'factura', 'items': 'factura_items'}
    )

@app.route('/devoluciones_salida', methods=['GET', 'POST'])
def devoluciones_salida():
    """Permite cargar una Factura de Compra para gestionar devoluciones.

    La vista replica el comportamiento de ``salida``: se busca una factura,
    se muestra su detalle junto al stock disponible y se pueden escanear
    códigos para registrar la devolución.
    """
    cu = session.get('current_user')
    op = session.get('operario')
    if not cu:
        return redirect(url_for('login1'))
    if cu.get('rol') == ROL_OPERARIO and not op:
        return redirect(url_for('login2'))
    # Recuperar datos desde sesión
    factura      = session.get('dev_current_factura', '')
    guia_actual  = session.get('dev_current_guia', '')
    factura_items = session.get('dev_factura_items', [])
    salida_items = session.get('dev_salida_items', [])

    if request.method == 'POST':
        action = request.form.get('action', 'buscar_factura')

        if action == 'buscar_factura':
            factura = (request.form.get('factura') or '').strip()
            session['dev_current_factura'] = factura
            session.pop('dev_factura_items', None)
            session.pop('dev_salida_items', None)

            if not factura:
                flash('Debes ingresar un número de Factura de Compra.', 'warning')
                return redirect(url_for('devoluciones_salida'))
            if not os.path.exists(FACTURA_FILE):
                flash('No se ha importado ninguna Factura de Compra.', 'warning')
                return redirect(url_for('devoluciones_salida'))

            try:
                df_nv = pd.read_csv(FACTURA_FILE, header=0, dtype=str, keep_default_na=False)
                df_nv.columns = [c.strip() for c in df_nv.columns]
                df_nv = df_nv.loc[:, ~df_nv.columns.str.match(r'^Unnamed', case=False)]

                if 'No. Factura' not in df_nv.columns:
                    flash("La columna 'No. Factura' no está en el archivo de Facturas.", 'error')
                    return redirect(url_for('devoluciones_salida'))

                df_nv = df_nv[df_nv['No. Factura'] == factura]
                if df_nv.empty:
                    flash(f'No se encontró la Factura {factura}.', 'error')
                    return redirect(url_for('devoluciones_salida'))

                df_nv['Cantidad'] = pd.to_numeric(df_nv.get('Cantidad','0'), errors='coerce').fillna(0).astype(int)
                df_nv['Precio Unitario'] = pd.to_numeric(df_nv.get('Precio Unitario','0'), errors='coerce').fillna(0).astype(int)

                df_show = df_nv[['Código','Descriptor','Cantidad','Precio Unitario']].copy()
                df_show.columns = ['Código','Nombre','Cant.','Prec.Unit.']
                df_show['Faltan'] = df_show['Cant.']

                factura_items = df_show.to_dict(orient='records')
                session['dev_factura_items'] = factura_items
                session['dev_salida_items'] = []
                flash(f'Factura {factura} cargada con {len(factura_items)} líneas.', 'success')
            except Exception as e:
                app.logger.error(f"Error al leer Factura en devoluciones: {e}")
                flash(f"Error al leer Factura: {e}", 'error')
                return redirect(url_for('devoluciones_salida'))

        elif action == 'scan':
            codigo = (request.form.get('codigo') or '').strip()
            try:
                cantidad = int(request.form.get('cantidad', 1))
            except ValueError:
                cantidad = 1
            ahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            guia = guia_actual
            if (g := request.form.get('guia', '').strip()):
                guia = g
                session['dev_current_guia'] = guia

            found = False
            for s in salida_items:
                if s['guia'] == guia and s['codigo'] == codigo:
                    s['cantidad'] += cantidad
                    s['hora'] = ahora
                    found = True
                    break
            if not found:
                salida_items.append({
                    'guia': guia,
                    'codigo': codigo,
                    'cantidad': cantidad,
                    'hora': ahora
                })
            session['dev_salida_items'] = salida_items
            flash(f'{cantidad} unidad(es) de {codigo} ' + ('sumadas' if found else 'registradas') + '.', 'success')
            return redirect(url_for('devoluciones_salida'))

        elif action == 'terminar_salida':
            session['items_para_guia'] = salida_items
            session['nv_para_guia']    = factura
            session['guia_para_guia']  = guia_actual
            flash("Productos escaneados preparados para la Guía de Despacho.", "info")
            return redirect(url_for('finalizar_salida'))

    # Cargar Stock desde la BBDD
    stock_map = {}
    try:
        df_st = db_utils.get_stock_actual()
        for _, row in df_st.iterrows():
            key = str(row.get('codigo', '')).strip()
            stock_map[key] = {
                'Nombre': row.get('nombre', '').strip(),
                'Cantidad': int(row.get('cantidad', 0))
            }
    except Exception as e:
        app.logger.error(f"Error al consultar Stock: {e}")
        flash(f"Error al consultar Stock: {e}", 'error')

    # Construir stock_items restando escaneos
    stock_items = []
    scanned_totals = {}

    if factura_items:
        for s in salida_items:
            k = str(s['codigo']).strip()
            scanned_totals[k] = scanned_totals.get(k, 0) + s['cantidad']

        for item in factura_items:
            code = str(item['Código']).strip()
            total = int(item['Cant.'])
            esc = scanned_totals.get(code, 0)
            falta = max(total - esc, 0)
            item['scanned'] = esc
            item['Faltan'] = falta

        for line in factura_items:
            code = str(line['Código']).strip()
            orig = stock_map.get(code, {}).get('Cantidad', 0)
            remain = max(orig - line['scanned'], 0)
            stock_items.append({
                'Código':  code,
                'Nombre':  stock_map.get(code, {}).get('Nombre', line['Nombre']),
                'Cantidad': remain
            })

    return render_template(
        'devoluciones_salida.html',
        factura=factura,
        guia=guia_actual,
        factura_items=factura_items,
        salida_items=salida_items,
        stock_items=stock_items
    )


@app.route('/listados')
def listados():
    return render_template('listados.html')

@app.route('/listados/oc')
def listado_oc():
    page = int(request.args.get('page', 1))
    per_page = 100
    ordenes = []
    total_pages = 1

    # Filtros desde GET
    ciudad_filtro = request.args.get('ciudad', '').strip().lower()
    razon_filtro = request.args.get('razon_social', '').strip().lower()

    if os.path.exists(OC_FILE):
        try:
            df = pd.read_csv(OC_FILE, dtype=str)

            # Eliminar columnas innecesarias
            hide_cols = {
                'descto.', 'dcto.tipo', 'dcto.pje',
                'bodega', 'item', 'cantidad',
                'cant. recibida', 'transito', 'línea de negocio',
                'nombre', 'prec.unit.'
            }
            cols_lower = {c: c.lower() for c in df.columns}
            keep = [orig for orig, low in cols_lower.items() if low not in hide_cols]
            df = df[keep]

            # Filtrado dinámico
            if ciudad_filtro and 'Ciudad' in df.columns:
                df = df[df['Ciudad'].str.lower().str.contains(ciudad_filtro, na=False)]

            if razon_filtro and 'Razón Social' in df.columns:
                df = df[df['Razón Social'].str.lower().str.contains(razon_filtro, na=False)]

            # Paginación
            total = len(df)
            total_pages = max(1, math.ceil(total / per_page))
            start = (page - 1) * per_page
            df_pagina = df.iloc[start:start + per_page]

            # Convertir a dict
            ordenes = df_pagina.to_dict(orient='records')

        except Exception as e:
            logger.error(f"Error al leer Órdenes de Compra: {e}")
            flash(f'Error al leer Órdenes de Compra: {e}', 'error')
    else:
        flash('No se ha importado ninguna Orden de Compra aún.', 'warning')

    return render_template(
        'listado_oc.html',
        ordenes=ordenes,
        page=page,
        total_pages=total_pages
    )

@app.route('/listados/nv')
def listado_nv():
    page        = int(request.args.get('page', 1))
    per_page    = 20
    ordenes     = []
    columns     = []        # ← inicializamos aquí
    total_pages = 1

    # 0) Verificar que el archivo exista
    if not os.path.exists(NV_FILE):
        flash("No se ha importado ninguna Nota de Venta aún.", "warning")
        return render_template(
            "listado_nv.html",
            columns=columns,
            ordenes=ordenes,
            page=page,
            total_pages=total_pages
        )

    # 1) Verificar que el archivo no esté vacío
    if os.path.getsize(NV_FILE) == 0:
        flash("El archivo de Notas de Venta está vacío.", "error")
        return render_template(
            "listado_nv.html",
            columns=columns,
            ordenes=ordenes,
            page=page,
            total_pages=total_pages
        )

    try:
        # 2) Leer sin header para detectar la fila real de cabecera
        df_raw = pd.read_csv(NV_FILE,
                             header=None,
                             dtype=str,
                             keep_default_na=False)

        expected = {
            'ciudad', 'fecha', 'numnota', 'rut',
            'razonsocial', 'canal', 'fechaentrega', 'formadepago'
        }
        def _norm(txt: str) -> str:
            s = unicodedata.normalize("NFKD", str(txt))
            s = s.encode("ascii", "ignore").decode().lower()
            return re.sub(r'[^a-z0-9]', '', s)

        header_idx = 0
        for i, row in df_raw.iterrows():
            norms = {_norm(cell) for cell in row if cell}
            if expected.issubset(norms):
                header_idx = i
                break

        # 3) Volver a leer con esa fila como cabecera
        df = pd.read_csv(NV_FILE,
                         header=header_idx,
                         dtype=str,
                         keep_default_na=False)

        # 4) Limpiar columnas: strip, eliminar Unnamed y vacías
        df.columns = [c.strip() for c in df.columns]
        df = df.loc[:, ~df.columns.str.match(r'^Unnamed', case=False)]
        df.dropna(axis=1, how='all', inplace=True)

        # 5) Ocultar las que no quieres
        ocultar = {
            'Línea', 'Sub Línea 1', 'Sub Línea 2', 'Clasificación',
            'Cod. Vend. Cartera', 'Nombre Vendedor Cartera',
            'Cod. Vendedor', 'Nombre Vendedor N/V',
            'Marca', 'Forma de Pago', 'Tot. Neto', 'Fecha', 'Canal', 'Fecha', 'Entrega', 'Num. Ord .Compra', 'Item'
        }
        df = df[[c for c in df.columns if c not in ocultar]]

        # 5.5) Eliminar columnas de detalle y agrupar por Num. Nota y RUT
        df = df[[c for c in df.columns if c not in {"Código", "Descriptor"}]]
        if {"Num. Nota", "RUT"}.issubset(df.columns):
            df = df.groupby(["Num. Nota", "RUT"], as_index=False).first()

        # aquí salvamos la lista de columnas para el template
        columns = df.columns.tolist()

        # 6) Paginación
        total       = len(df)
        total_pages = max(1, math.ceil(total / per_page))
        start       = (page - 1) * per_page
        df_page     = df.iloc[start:start + per_page]
        ordenes     = df_page.to_dict(orient='records')

    except Exception as e:
        logger.error(f"Error al leer Notas de Venta: {e}")
        flash(f"Error al leer Notas de Venta: {e}", "error")

    return render_template(
        "listado_nv.html",
        columns=columns,         # ← añadimos columns
        ordenes=ordenes,
        page=page,
        total_pages=total_pages
    )

@app.get('/nv/gestionar')
def nv_gestionar():
    # Delega todo en la vista oficial de admin
    return redirect(url_for('admin_nv_gestionar'))


@app.route('/notas/preview')
def notas_preview():
    if not os.path.exists(NV_FILE):
        flash('No se ha importado ninguna Nota de Venta.', 'warning')
        return redirect(url_for('index'))

    df_raw = pd.read_csv(NV_FILE, header=None, dtype=str, keep_default_na=False)
    # Flash de diagnóstico: primeras 5 filas
    flash('Primeras 5 filas (sin header):', 'info')
    for i, row in df_raw.head(5).iterrows():
        flash(f'Fila {i}: ' + ' | '.join(row.astype(str).tolist()), 'info')

    # Detección de cabecera basándonos en tus títulos esperados
    expected = {'ciudad','fecha','numnota','rut','razonsocial','canal','fechaentrega','formadepago'}
    def _norm(txt):
        return re.sub(r'[^a-z0-9]', '', unicodedata.normalize("NFKD", str(txt)).encode("ascii","ignore").decode().lower())

    header_idx = None
    for i, row in df_raw.iterrows():
        norms = {_norm(c) for c in row if c}
        if expected.issubset(norms):
            header_idx = i
            break
    flash(f'Cabecera detectada en fila: {header_idx}', 'info')

    # Ahora léelo con esa cabecera y muéstrame las columnas
    if header_idx is not None:
        df = pd.read_csv(NV_FILE, header=header_idx, dtype=str, keep_default_na=False)
        flash('Columnas detectadas: ' + ', '.join([c.strip() for c in df.columns]), 'info')

    return render_template('notas_preview.html')

@app.route('/nota_credito')
def nota_credito():
    """Renderiza la página de Nota de Crédito."""
    return render_template('nota_credito.html')

def ingreso_core(
    template,
    endpoint,
    *,
    data_file=OC_FILE,
    query_param='oc',
    field_name='No. OC',
    label='OC',
    search_action='buscar_oc',
    session_keys=None,
    context_keys=None,
    db_fetcher=None
):
    session_keys = session_keys or {
        'num': 'current_oc',
        'guia': 'current_guia',
        'items': 'oc_items',
        'scanned': 'scanned'
    }
    context_keys = context_keys or {'num': 'oc', 'items': 'oc_items'}

    numero = session.get(session_keys['num'])
    guia_actual = session.get(session_keys['guia'], '')
    items = session.get(session_keys['items'], [])
    scanned_items = session.get(session_keys['scanned'], [])

    def detect_keys(sample):
        def pick(options, default):
            for opt in options:
                if opt in sample:
                    return opt
            return default
        return (
            pick(['codigo', 'Código'], 'codigo'),
            pick(['nombre', 'Nombre'], 'nombre'),
            pick(['cantidad', 'Cantidad', 'Cant.'], 'cantidad'),
            pick(['prec_unit', 'Prec.Unit.', 'Precio Unitario'], 'prec_unit'),
        )

    code_key = name_key = qty_key = price_key = None
    if items:
        code_key, name_key, qty_key, price_key = detect_keys(items[0])

    # ───────────── GET con ?<query_param>=XXXX ─────────────
    if request.method == 'GET' and request.args.get(query_param):
        numero = request.args.get(query_param).strip()
        session[session_keys['num']] = numero
        session.pop(session_keys['guia'], None)
        session.pop(session_keys['scanned'], None)
        session.pop(session_keys['items'], None)

        if db_fetcher:
            try:
                df, guia_db = db_fetcher(numero)
                df = group_by_code(df)
                items = df.to_dict('records')
                session[session_keys['items']] = items
                if items:
                    code_key, name_key, qty_key, price_key = detect_keys(items[0])
                if guia_db:
                    session[session_keys['guia']] = guia_db
                    guia_actual = guia_db
                if not items:
                    flash(f'La {label} {numero} no fue encontrada.', 'error')
                else:
                    flash(f'La {label} {numero} encontrada con {len(items)} líneas.', 'success')
            except Exception as e:
                logger.error(f'Error obteniendo {label} desde DB: {e}')
                flash(f'Error al obtener {label} desde la base de datos: {e}', 'error')
        else:
            if not os.path.exists(data_file):
                flash(f'Primero importa {label}s.', 'warning')
            else:
                try:
                    df = pd.read_csv(data_file, dtype=str)
                    df = df[df[field_name] == numero]
                    df = group_by_code(df)
                    items = df.to_dict('records')
                    session[session_keys['items']] = items
                    if not items:
                        flash(f'La {label} {numero} no fue encontrada.', 'error')
                    else:
                        flash(f'La {label} {numero} encontrada con {len(items)} líneas.', 'success')
                except Exception as e:
                    logger.error(f'Error procesando {label} desde parámetro: {e}')
                    flash(f'Error al procesar {label} desde la URL: {e}', 'error')

    # ───────────── POST desde formulario ─────────────
    if request.method == 'POST':
        action = request.form.get('action')

        if action == search_action:
            numero = request.form[query_param].strip()
            session[session_keys['num']] = numero
            session.pop(session_keys['guia'], None)
            session.pop(session_keys['scanned'], None)
            session.pop(session_keys['items'], None)

            if not numero:
                flash(f'El No. {label} es obligatorio.', 'warning')
            elif db_fetcher:
                try:
                    df, guia_db = db_fetcher(numero)
                    df = group_by_code(df)
                    items = df.to_dict('records')
                    session[session_keys['items']] = items
                    if items:
                        code_key, name_key, qty_key, price_key = detect_keys(items[0])
                    if guia_db:
                        session[session_keys['guia']] = guia_db
                        guia_actual = guia_db
                    if not items:
                        flash(f'La {label} {numero} no fue encontrada.', 'error')
                    else:
                        flash(f'{label} {numero} encontrada con {len(items)} líneas.', 'success')
                except Exception as e:
                    logger.error(f'Error obteniendo {label} desde DB: {e}')
                    flash(f'Error al obtener {label} desde la base de datos: {e}', 'error')
            elif not os.path.exists(data_file):
                flash(f'Primero importa {label}s.', 'warning')
            else:
                try:
                    df = pd.read_csv(data_file, dtype=str)
                    df = df[df[field_name] == numero]
                    df = group_by_code(df)
                    items = df.to_dict('records')
                    session[session_keys['items']] = items
                    if not items:
                        flash(f'La {label} {numero} no fue encontrada.', 'error')
                    else:
                        flash(f'{label} {numero} encontrada con {len(items)} líneas.', 'success')
                except Exception as e:
                    logger.error(f'Error procesando {label}: {e}')
                    flash(f'Error al procesar {label}s: {e}', 'error')
            return redirect(url_for(endpoint))

        elif action == 'scan':
            guia = request.form.get('guia', '').strip() or guia_actual
            codigo = norm_code(request.form.get('codigo', ''))
            try:
                cantidad = int(request.form.get('cantidad', 1))
            except Exception:
                cantidad = 1
            ahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            if guia and guia != guia_actual:
                session[session_keys['guia']] = guia
                guia_actual = guia

            if not numero:
                flash(f'Primero debes buscar una {label}.', 'warning')
                return redirect(url_for(endpoint))

        elif action == 'finish':
            if not scanned_items:
                flash('No hay ítems para guardar.', 'warning')
                return redirect(url_for(endpoint))

            df_rep = pd.DataFrame(scanned_items)
            proveedor = rut = ""
            if items:
                proveedor = items[0].get("NombreProveedor") or items[0].get("Razón Social") or ""
                rut = items[0].get("RUT") or items[0].get("RUT Proveedor") or items[0].get("RutProveedor") or ""
                df_rep["Razón Social"] = proveedor
                df_rep["RUT"] = rut

            nombre_informe = f"informe_{numero}_{guia_actual}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            ruta_informe = os.path.join(DATA_DIR, nombre_informe)
            df_rep.to_excel(ruta_informe, index=False)

            df_doc = pd.DataFrame(items)
            if qty_key and qty_key in df_doc.columns:
                df_doc[qty_key] = pd.to_numeric(df_doc[qty_key], errors='coerce').fillna(0).astype(int)

            df_scan = pd.DataFrame(scanned_items)

            # Normalizar ambos lados
            df_doc['_code_norm'] = (
                df_doc[code_key].astype(str).str.strip().str.strip('*').str.upper()
            )
            if not df_scan.empty:
                df_scan['codigo_producto'] = (
                    df_scan['codigo_producto'].astype(str).str.strip().str.strip('*').str.upper()
                )
                grouped = df_scan.groupby('codigo_producto')['cantidad'].sum().reset_index()
            else:
                grouped = pd.DataFrame({'codigo_producto': [], 'cantidad': []})

            merged = df_doc.merge(
                grouped,
                left_on='_code_norm',
                right_on='codigo_producto',
                how='left',
                suffixes=("", "_scan"),
            ).fillna(0)
            merged['cantidad_scan'] = merged['cantidad_scan'].astype(int)
            merged['faltan'] = merged[qty_key] - merged['cantidad_scan']
            diff = merged[merged['faltan'] > 0][[code_key, name_key, qty_key, 'cantidad_scan', 'faltan']]
            diff = diff.rename(columns={'cantidad_scan': 'cantidad'})

            diff["Razón Social"] = proveedor
            diff["RUT"] = rut
            cols = ["Razón Social", "RUT"] + [c for c in diff.columns if c not in ("Razón Social", "RUT")]
            diff = diff[cols]

            nombre_dif = f"diferencias_{numero}_{guia_actual}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            ruta_dif = os.path.join(DATA_DIR, nombre_dif)
            diff.to_excel(ruta_dif, index=False)

            session['informe_path'] = ruta_informe
            session['diferencias_path'] = ruta_dif

            for k in (session_keys['scanned'], session_keys['num'], session_keys['guia'], session_keys['items']):
                session.pop(k, None)

            flash('Recepción finalizada correctamente.', 'success')
            return redirect(url_for('finalizar'))

        if any(norm_code(item.get(code_key, '')) == codigo for item in items):
            found = False
            for s in scanned_items:
                if s['guia'] == guia and s['codigo_producto'] == codigo:
                    s['cantidad'] += cantidad
                    s['fecha_hora'] = ahora
                    found = True
                    break
            if not found:
                scanned_items.append({
                    'guia': guia,
                    'codigo_producto': codigo,
                    'cantidad': cantidad,
                    'fecha_hora': ahora
                })
            session[session_keys['scanned']] = scanned_items
            flash(
                f'{cantidad} unidad(es) de {codigo} ' + ('sumadas' if found else 'registradas') + '.',
                'success'
            )
        else:
            flash(
                f'El código {codigo} no pertenece a la {label.lower()} {numero}.',
                'warning'
            )
        return redirect(url_for(endpoint))

    if not numero or not items:
        return render_template(
            template,
            **{
                context_keys['num']: '',
                context_keys['items']: [],
                'scanned_items': [],
                'guia': '',
                'code_key': 'codigo',
                'name_key': 'nombre',
                'qty_key': 'cantidad',
                'price_key': 'prec_unit',
            }
        )

    scanned_map = {}
    for s in scanned_items:
        c = norm_code(s.get('codigo_producto'))
        if c:
            scanned_map[c] = scanned_map.get(c, 0) + int(s.get('cantidad', 0))

    def _to_qty(val):
        # Convierte "1.000000" o "1,000000" a 1; valores raros -> 0
        s = str(val).strip().replace(',', '.')
        try:
            return int(round(float(s)))
        except Exception:
            import re
            m = re.search(r'\d+', s)
            return int(m.group(0)) if m else 0

    display_items = []
    for item in items:
        qty_ord = _to_qty(item.get(qty_key, 0))
        item_code = norm_code(item.get(code_key, ''))
        scanned_qty = scanned_map.get(item_code, 0)
        faltan = max(qty_ord - scanned_qty, 0)

        item2 = item.copy()
        item2['QtyInt'] = qty_ord      # <-- para usar en la plantilla
        item2['Faltan'] = faltan
        display_items.append(item2)

    return render_template(
        template,
        **{
            context_keys['num']: numero,
            context_keys['items']: display_items,
            'scanned_items': scanned_items,
            'guia': guia_actual,
            'code_key': code_key,
            'name_key': name_key,
            'qty_key': qty_key,
            'price_key': price_key,
        }
    )

@app.route('/ingreso', methods=['GET', 'POST'])
def ingreso():
    cu = session.get('current_user')
    op = session.get('operario')
    if not cu:
        return redirect(url_for('login1'))
    if cu.get('rol') == ROL_OPERARIO and not op:
        return redirect(url_for('login2'))
    return ingreso_core('ingreso.html', 'ingreso', db_fetcher=fetch_oc_items)




@app.route('/ingreso/diferencias.xls')
def download_diferencias():
        cu = session.get('current_user')
        op = session.get('operario')
        if not cu:
            return redirect(url_for('login1'))
        if cu.get('rol') == ROL_OPERARIO and not op:
            return redirect(url_for('login2'))

        path = session.get('diferencias_path')
        if not path or not os.path.exists(path):
            return "No se encontró el informe de diferencias.", 404

        return send_file(
            path,
            download_name=os.path.basename(path),
            as_attachment=True,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

@app.route('/ingreso/guia.xls')
def download_guia():
        cu = session.get('current_user')
        op = session.get('operario')
        if not cu:
            return redirect(url_for('login1'))
        if cu.get('rol') == ROL_OPERARIO and not op:
            return redirect(url_for('login2'))

        path = session.get('informe_path')
        if not path or not os.path.exists(path):
            return "No se encontró la guía de recepción.", 404

        return send_file(
            path,
            download_name=os.path.basename(path),
            as_attachment=True,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

@app.route('/salida', methods=['GET', 'POST'])
def salida():
    import pandas as pd

    cu = session.get('current_user')
    op = session.get('operario')

    # En modo de pruebas no exigimos autenticación para facilitar los tests.
    if app.config.get('TESTING') and not cu:
        cu = {'rol': 'admin'}
        session['current_user'] = cu

    if not cu:
        if current_app.config.get('TESTING'):
            session['current_user'] = cu = {'nombre': 'test', 'rol': ROL_OPERARIO}
        else:
            return redirect(url_for('login1'))
    if cu.get('rol') == ROL_OPERARIO and not op:
        if current_app.config.get('TESTING'):
            session['operario'] = {'codigo': 'T', 'nombre': 'test'}
        else:
            return redirect(url_for('login2'))

    # Estado
    nota         = session.get('current_nv', '')
    guia_actual  = session.get('current_guia', '')
    nv_items     = session.get('nv_items', [])        # detalle NV (desde BBDD)
    salida_items = session.get('salida_items', [])    # items para salida/escaneo

    if request.method == 'POST':
        action = request.form.get('action', 'buscar_nv')
        session['guia_datos'] = request.form.to_dict()

        # 1) Buscar NV en BBDD
        if action == 'buscar_nv':
            nota = (request.form.get('nv') or '').strip()
            session['current_nv'] = nota
            session.pop('nv_items', None)
            session.pop('salida_items', None)
            nv_items, salida_items = [], []

            if not nota:
                flash('Debes ingresar un número de Nota de Venta.', 'warning')
                return redirect(url_for('salida'))

            try:
                # ✅ usa db.get_nota_detalle (no db_utils)
                df = db.get_nota_detalle(nota)
                if df.empty:
                    flash(f'No se encontró detalle para la Nota de Venta {nota}.', 'warning')
                else:
                    # Normaliza columnas a los alias que usa la plantilla
                    nv_items = df.rename(columns={
                        "num_nota": "N° Nota",
                        "codigo":   "Código",
                        "nombre":   "Nombre",
                        "cantidad": "Cant.",      # cantidad pendiente (CANTIDAD - CANTDESP)
                        "prec_unit":"Prec.Unit"
                    }).to_dict(orient='records')

                    session['nv_items'] = nv_items
                    session['salida_items'] = salida_items
            except Exception as e:
                current_app.logger.exception("salida: error consultando NOTV/NOTDE")
                flash(f'Error al consultar la BBDD: {e}', 'danger')

            return redirect(url_for('salida'))

        # 2) Escanear (agregar item a salida) — con BLOQUEO de sobre-escaneo
        elif action in ('escanear', 'scan'):
            codigo = (request.form.get('codigo') or '').strip()
            cant   = request.form.get('cantidad') or '1'
            try:
                cant = int(cant)
            except Exception:
                cant = 1
            cant = max(cant, 1)

            if not nv_items:
                flash('Primero busca una Nota de Venta.', 'warning')
                return redirect(url_for('salida'))

            # Buscar la línea de la NV por código
            base = pd.DataFrame(nv_items)
            codigos_series = base["Código"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
            match = base[codigos_series == codigo]
            if match.empty:
                flash(f'El código {codigo} no está en la Nota de Venta {nota}.', 'warning')
                return redirect(url_for('salida'))

            row = match.iloc[0].to_dict()
            cant_nv_pend = int(row.get('Cant.', 0))  # pendiente según NV para ese código

            # ¿Cuánto ya llevas escaneado (en esta sesión) para ese código?
            ya_escaneado = 0
            for it in salida_items:
                if str(it.get('Código')) == str(row['Código']):
                    ya_escaneado += int(it.get('Cant.Salida', 0))

            pendiente = max(cant_nv_pend - ya_escaneado, 0)

            if pendiente == 0:
                flash("Ya tienes la cantidad exacta para este producto. No se registró nada.", "info")
                return redirect(url_for('salida'))

            if cant > pendiente:
                flash(f"Solo faltan {pendiente} unidad(es) de este producto. No se registró el escaneo.", "warning")
                return redirect(url_for('salida'))

            # Agrega o acumula (válido)
            updated = False
            for it in salida_items:
                if str(it['Código']) == str(row['Código']):
                    it['Cant.Salida'] = int(it.get('Cant.Salida', 0)) + cant
                    updated = True
                    break
            if not updated:
                salida_items.append({
                    'N° Nota':     row['N° Nota'],
                    'Código':      row['Código'],
                    'Nombre':      row['Nombre'],
                    'Prec.Unit':   row['Prec.Unit'],
                    'Cant.NV':     cant_nv_pend,   # pendiente permitido por NV
                    'Cant.Salida': cant
                })

            session['salida_items'] = salida_items
            return redirect(url_for('salida'))

        # 3) Eliminar item
        elif action == 'eliminar_item':
            codigo = request.form.get('codigo') or ''
            salida_items = [it for it in salida_items if str(it.get('Código','')) != str(codigo)]
            session['salida_items'] = salida_items
            return redirect(url_for('salida'))

        # 4) Finalizar salida (validaciones básicas)
        elif action == 'finalizar_salida':
            if not salida_items:
                flash('No hay ítems en la salida.', 'warning')
                return redirect(url_for('salida'))

            base = pd.DataFrame(nv_items)
            sal  = pd.DataFrame(salida_items)

            base['Cant.']      = pd.to_numeric(base['Cant.'], errors='coerce').fillna(0).astype(int)
            sal['Cant.Salida'] = pd.to_numeric(sal['Cant.Salida'], errors='coerce').fillna(0).astype(int)

            # Validación: no permitir sobrepasar pendiente por código
            merged = sal.merge(base[['Código','Cant.']], on='Código', how='left')
            merged['Exceso'] = (merged['Cant.Salida'] - merged['Cant.']).clip(lower=0)
            if (merged['Exceso'] > 0).any():
                cods = merged.loc[merged['Exceso'] > 0, 'Código'].unique().tolist()
                flash(f'Cantidad de salida supera lo pendiente para: {", ".join(map(str, cods))}.', 'danger')
                return redirect(url_for('salida'))

            # Calcular si quedó PARCIAL o COMPLETA
            faltas = base.merge(sal[['Código','Cant.Salida']], on='Código', how='left')
            faltas['Cant.Salida'] = faltas['Cant.Salida'].fillna(0).astype(int)
            faltas['Pendiente']   = (faltas['Cant.'] - faltas['Cant.Salida']).clip(lower=0)
            estado_nv = 'PARCIAL' if (faltas['Pendiente'] > 0).any() else 'COMPLETA'

            # Preparar SOLO lo escaneado para la Guía (y/o Factura)
            scaneado = [
                {
                    "codigo":   str(r.get('Código')).strip(),
                    "cantidad": int(r.get('Cant.Salida', 0))
                }
                for r in salida_items
                if int(pd.to_numeric(r.get('Cant.Salida', 0), errors='coerce') or 0) > 0
            ]
            session['items_para_guia'] = scaneado
            session['nv_para_guia']    = nota
            session['guia_para_guia']  = session.get('current_guia', '')  # si corresponde

            # Limpieza de ítems de salida (ya quedaron guardados para la guía)
            session.pop('salida_items', None)

            flash(f"Salida finalizada correctamente. Estado NV: {estado_nv}.", 'success')
            return redirect(url_for('finalizar_salida'))

    # ---------------------- GET ----------------------

    # 1) Zonas y mapeos
    zonas = _get_zonas()  # [{'id':1,'nombre':'SANTIAGO'}, ...]  (lee RIMA.dbo.ZONAS_DB)
    id2nombre = {int(z['id']): str(z['nombre']).strip().upper() for z in zonas if 'id' in z and 'nombre' in z}

    # 2) Parámetros de filtro
    zona_id     = request.args.get('zona_id', type=int)
    zona_nombre = (request.args.get('zona') or '').strip()

    # Si no vino zona_id, intentar resolverlo por nombre (tolerante a mayúsculas/espacios)
    if not zona_id and zona_nombre:
        zn = zona_nombre.strip().upper()
        zona_id = next((zid for zid, nom in id2nombre.items() if nom == zn), None)

    lista_nv = []
    zona_seleccionada = zona_id or (zona_nombre if zona_nombre else None)

    # 3) Listado de NV asignadas a la zona DESDE NV_FLOW (JOIN compatible)
    if zona_id or zona_nombre:
        sql = """
        SELECT TOP (120)
            N.NUMNOTA,
            CAST(N.FECHA AS DATE)    AS FECHA,
            N.SUCUR                  AS SUCUR,
            ISNULL(C.RAZSOC,'')      AS RAZSOC
        FROM [Santiago].dbo.NOTV_DB      AS N
        JOIN [RIMA].dbo.NV_FLOW          AS F
             ON CONVERT(VARCHAR(20), N.NUMNOTA) = CONVERT(VARCHAR(20), F.NUMNOTA)
        LEFT JOIN [Santiago].dbo.CLIEN_DB AS C ON C.NREGUIST = N.NRUTCLIE
        LEFT JOIN [RIMA].dbo.ZONAS_DB     AS Z ON Z.ID = F.ZONA_ID
        WHERE
             (( :zid IS NOT NULL AND F.ZONA_ID = :zid )
              OR ( :zid IS NULL AND Z.NOMBRE IS NOT NULL
                   AND UPPER(LTRIM(RTRIM(Z.NOMBRE))) = UPPER(LTRIM(RTRIM(:znom))) ))
          AND N.FECHA >= DATEADD(YEAR, -1, CAST(GETDATE() AS DATE))
        ORDER BY N.FECHA DESC, N.NUMNOTA DESC;
        """
        params = {"zid": zona_id, "znom": zona_nombre}
        try:
            df_z = db.query_df(sql, params)
            if not df_z.empty:
                lista_nv = df_z.rename(columns={
                    "NUMNOTA": "numnota",
                    "FECHA":   "fecha",
                    "SUCUR":   "sucursal",
                    "RAZSOC":  "cliente",
                }).to_dict(orient='records')
            else:
                flash('No hay Notas de Venta asignadas a esta zona.', 'info')
        except Exception as e:
            current_app.logger.exception("salida: error listando NV por zona desde NV_FLOW")
            flash(f"Error listando NV de la zona: {e}", "danger")
            lista_nv = []

    # 4) Calcular cantidades escaneadas y faltantes para cada ítem de la NV
    scanned_map = {}
    for si in salida_items:
        try:
            code = str(si.get('Código'))
            qty  = int(si.get('Cant.Salida', 0))
        except Exception:
            code, qty = str(si.get('Código')), 0
        scanned_map[code] = scanned_map.get(code, 0) + qty

    display_nv_items = []
    for it in nv_items:
        try:
            orig = int(it.get('Cant.', 0))
        except Exception:
            orig = 0
        code = str(it.get('Código'))
        scanned_qty = scanned_map.get(code, 0)
        item2 = it.copy()
        item2['scanned'] = scanned_qty
        item2['Faltan']  = max(orig - scanned_qty, 0)
        display_nv_items.append(item2)

    # 5) Cargar stock actual SOLO para los códigos de la NV y restar escaneos
    stock_items = []
    if display_nv_items:
        # 5.1) códigos únicos visibles en la NV
        codigos_nv = sorted({
            str(it.get("Código")).strip()
            for it in display_nv_items
            if str(it.get("Código") or "").strip()
        })

        stock_map = {}
        try:
            # 🔥 Optimización: traer stock solo de esos códigos
            df_st = db.get_stock_por_codigos(codigos_nv)
            if df_st is not None and not df_st.empty:
                for _, row in df_st.iterrows():
                    key = str(row.get('codigo', '')).strip()
                    stock_map[key] = {
                        'Nombre': row.get('nombre', '').strip(),
                        'Cantidad': int(row.get('cantidad', 0))
                    }
        except Exception as e:
            app.logger.error(f"Error al consultar Stock: {e}")
            flash(f"Error al consultar Stock: {e}", 'error')

        # 5.2) totales escaneados por código
        scanned_totals = {}
        for s in salida_items:
            k = str(s.get('Código') or '').strip()
            try:
                scanned_totals[k] = scanned_totals.get(k, 0) + int(s.get('Cant.Salida', 0))
            except Exception:
                scanned_totals[k] = scanned_totals.get(k, 0)

        # 5.3) armar tabla "Stock actual" descontando lo escaneado en esta sesión
        for line in display_nv_items:
            code = str(line.get('Código') or '').strip()
            orig = stock_map.get(code, {}).get('Cantidad', 0)
            remain = max(orig - scanned_totals.get(code, 0), 0)
            stock_items.append({
                'Código': code,
                'Nombre': stock_map.get(code, {}).get('Nombre', line.get('Nombre')),
                'Cantidad': remain
            })

    # 6) Badges: conteo por ZONA_ID desde NV_FLOW (RIMA)
    zona_counts_id = {}
    try:
        df_cnt = db.query_df("""
            SELECT ZONA_ID, COUNT(*) AS CNT
            FROM [RIMA].dbo.NV_FLOW
            WHERE ZONA_ID IS NOT NULL
            GROUP BY ZONA_ID
        """, {})
        if df_cnt is not None and not df_cnt.empty:
            zona_counts_id = {
                int(r['ZONA_ID']): int(r['CNT'])
                for _, r in df_cnt.iterrows()
                if r['ZONA_ID'] is not None
            }
    except Exception as e:
        current_app.logger.warning("salida: no pude calcular zona_counts_id: %s", e)
        zona_counts_id = {}

    # (opcional) counts por nombre para compatibilidad con plantillas viejas
    zona_counts = {}
    for zid, cnt in zona_counts_id.items():
        nom = id2nombre.get(zid)
        if nom:
            zona_counts[nom] = zona_counts.get(nom, 0) + cnt

    return render_template(
        'salida.html',
        nota=nota,
        guia_actual=guia_actual,
        nv_items=display_nv_items,
        salida_items=salida_items,
        stock_items=stock_items,
        zona_seleccionada=zona_seleccionada,
        lista_nv=lista_nv,
        zonas=zonas,
        zona_counts=zona_counts,          # compat: por nombre
        zona_counts_id=zona_counts_id     # nuevo: por ID (preferido en template)
    )


@app.route('/boleta/<int:num_nota>', methods=['GET'], endpoint='boleta_nv')
def boleta(num_nota):
    return render_template('boleta.html', num_nota=num_nota)


@app.route("/admin/nv/gestionar", endpoint="admin_nv_gestionar")
def admin_nv_gestionar():
    import time
    cu = session.get('current_user') or {}
    if not (session.get('is_admin') and cu.get('rol') == ROL_JEFE):
        return abort(403)

    # ====== Filtros UI ======
    estado = (request.args.get('estado') or '').strip().upper()
    flow   = (request.args.get('flow') or 'ALL').strip().upper()
    if flow not in ('ALL', 'APROB', 'PEND', 'UNSET'):
        flow = 'ALL'
    meses = [m for m in request.args.getlist('mes') if isinstance(m, str) and m.strip()]

    # Toggle de extras pesados y stock (para medición)
    extras   = request.args.get('extras') in ('1', 'true', 'yes')   # agrega PrecioUnit si lo pides
    nostock  = request.args.get('nostock') in ('1', 'true', 'yes')  # evita consulta de stock

    # Rango por defecto en meses (si no hay filtro por 'mes' ni all=1)
    try:
        default_months = int(request.args.get('months', 6))
        default_months = min(max(default_months, 1), 24)  # [1..24]
    except Exception:
        default_months = 6

    # ====== Paginación ======
    try:
        page = max(int(request.args.get('page', 1)), 1)
    except Exception:
        page = 1
    try:
        per_page = int(request.args.get('per_page', 20))
        per_page = min(max(per_page, 10), 300)
    except Exception:
        per_page = 20
    offset = (page - 1) * per_page

    # ====== Orden (whitelist) ======
    order = (request.args.get('order') or 'FechaEntrega DESC, NumNota DESC').strip()
    order_whitelist = {
        'NumNota ASC': 'NumNota ASC',
        'NumNota DESC': 'NumNota DESC',
        'FechaEntrega ASC': 'FechaEntrega ASC',
        'FechaEntrega DESC': 'FechaEntrega DESC',
        'StockPct ASC': 'StockPct ASC',
        'StockPct DESC': 'StockPct DESC',
        'FechaEntrega DESC, NumNota DESC': 'FechaEntrega DESC, NumNota DESC',
    }
    order_sql = order_whitelist.get(order, 'FechaEntrega DESC, NumNota DESC')

    # ====== Filtro por meses explícitos (UI) ======
    date_filters = []
    for ym in meses:
        try:
            y_str, m_str = ym.split('-', 1)
            y, m = int(y_str), int(m_str)
            ini = f"{y:04d}-{m:02d}-01"
            y_fin = y + (1 if m == 12 else 0)
            m_fin = 1 if m == 12 else (m + 1)
            fin = f"{y_fin:04d}-{m_fin:02d}-01"
            date_filters.append(f"(nv.FECHA >= '{ini}' AND nv.FECHA < '{fin}')")
        except Exception:
            continue
    date_where_mes = (" AND (" + " OR ".join(date_filters) + ") ") if date_filters else ""

    # ====== Último rango por defecto (6 meses) o all=1 ======
    all_flag = request.args.get('all') in ('1', 'true', 'yes')
    use_default_window = (not all_flag) and (not date_filters)
    default_window_sql = ""
    if use_default_window:
        default_window_sql = f" AND nv.FECHA >= DATEADD(MONTH, -{default_months}, CAST(GETDATE() AS DATE)) "

    # ====== CASE Estado Despacho ======
    estado_desp_case = """
        CASE
            WHEN SUM(CAST(ISNULL(nd.CANTDESP,0) AS INT)) = 0
                THEN 'PENDIENTE'
            WHEN SUM(CAST(ISNULL(nd.CANTDESP,0) AS INT)) < SUM(CAST(ISNULL(nd.CANTIDAD,0) AS INT))
                THEN 'PARCIAL'
            ELSE 'TERMINADO'
        END
    """
    having_estado = ""
    if estado in ('PENDIENTE', 'PARCIAL', 'TERMINADO'):
        having_estado = f" HAVING {estado_desp_case} = '{estado}' "

    cliente_join = "c.NREGUIST = nv.NRUTCLIE"

    # ====== Columnas (PrecioUnit opcional; ObsFact FUERA del CTE) ======
    precio_col = "CAST(AVG(CAST(ISNULL(nd.PRECUNIT,0) AS FLOAT)) AS DECIMAL(18,0)) AS PrecioUnit," if extras else ""

    # ====== CTE base SIN STK y SIN OBSFACT (se traerá en lote aparte) ======
    base_cte = f"""
        WITH RES AS (
            SELECT
                nv.NUMNOTA                                            AS NumNota,
                nv.RUTFACT                                            AS RUT,
                nv.SUCUR                                              AS Ciudad,
                ISNULL(c.RAZSOC, '')                                  AS RazonSocial,
                CAST(nv.FECHA AS DATE)                                AS FechaEntrega,
                SUM(CAST(ISNULL(nd.CANTIDAD,  0) AS INT))             AS Cantidad,
                SUM(CAST(ISNULL(nd.CANTDESP, 0) AS INT))              AS CantDesp,
                {precio_col}
                {estado_desp_case}                                    AS EstadoDespacho
            FROM dbo.NOTV_DB nv
            JOIN dbo.NOTDE_DB nd
              ON nd.NUMRECOR = nv.NUMREG
            LEFT JOIN dbo.CLIEN_DB c
              ON {cliente_join}
            WHERE 1=1
              {date_where_mes}
              {default_window_sql}
            GROUP BY nv.NUMNOTA, nv.RUTFACT, nv.SUCUR, c.RAZSOC, nv.FECHA
            {having_estado}
        )
    """

    # ====== Página (una sola consulta, SIN STK/OBS) ======
    page_sql = base_cte + f"""
        SELECT
            R.*,
            COUNT(1) OVER() AS total_rows
        FROM RES R
        ORDER BY {order_sql}
        OFFSET :offset ROWS FETCH NEXT :per_page ROWS ONLY;
    """

    # ====== Ejecutar ======
    try:
        db.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;")
    except Exception:
        pass

    t0 = time.perf_counter()
    try:
        df_page = db.query_df(page_sql, {"offset": offset, "per_page": per_page})
        rows = df_page.to_dict(orient="records")
        total = int(df_page.iloc[0]["total_rows"]) if not df_page.empty else 0
    except Exception as e:
        current_app.logger.exception("Error ejecutando admin_nv_gestionar PAGE SQL (lite)")
        flash(f"Error al consultar Notas de Venta: {e}", "danger")
        rows, total = [], 0
    t1 = time.perf_counter()

    import math
    total_pages = max(math.ceil(total / per_page), 1)

    # ====== OBSFACT en lote SOLO para NV visibles ======
    t_obs0 = time.perf_counter()
    obs_map = {}
    try:
        nums = [int(r.get("NumNota") or 0) for r in rows if r.get("NumNota") is not None]
        nums = [n for n in nums if n > 0]
        if nums:
            in_params = ", ".join([f":nv{i}" for i in range(len(nums))])
            params = {f"nv{i}": nums[i] for i in range(len(nums))}
            sql_obs = f"""
                SELECT nv.NUMNOTA,
                       CAST(ISNULL(nv.OBSFACT,'') AS NVARCHAR(4000)) AS ObsFact
                FROM dbo.NOTV_DB nv
                WHERE nv.NUMNOTA IN ({in_params});
            """
            df_obs = db.query_df(sql_obs, params)
            if df_obs is not None and not df_obs.empty:
                for _, r2 in df_obs.iterrows():
                    try:
                        obs_map[int(r2["NUMNOTA"])] = r2["ObsFact"]
                    except Exception:
                        continue
    except Exception as e:
        current_app.logger.warning("ObsFact batch falló: %s", e)
    for r in rows:
        try:
            num = int(r.get("NumNota") or 0)
        except Exception:
            num = 0
        r["ObsFact"] = obs_map.get(num, "")
    t_obs1 = time.perf_counter()

    # ====== Stock sólo para NV visibles (puedes saltarlo con ?nostock=1) ======
    t_stk0 = time.perf_counter()
    stock_map = {}
    if not nostock:
        try:
            nums = [int(r.get("NumNota") or 0) for r in rows if r.get("NumNota") is not None]
            nums = [n for n in nums if n > 0]
            if nums:
                in_params = ", ".join([f":nv{i}" for i in range(len(nums))])
                params = {f"nv{i}": nums[i] for i in range(len(nums))}
                sql_stk = f"""
                    SELECT
                        v.NUMNOTA,
                        CAST(AVG(CAST(ISNULL(v.STOCK_PCT,0) AS FLOAT)) AS DECIMAL(5,2)) AS StockPct
                    FROM [RIMA].dbo.VW_NV_STOCK_PCT v
                    WHERE v.NUMNOTA IN ({in_params})
                    GROUP BY v.NUMNOTA;
                """
                df_stk = db.query_df(sql_stk, params)
                if df_stk is not None and not df_stk.empty:
                    for _, r2 in df_stk.iterrows():
                        try:
                            stock_map[int(r2["NUMNOTA"])] = float(r2["StockPct"])
                        except Exception:
                            continue
        except Exception as e:
            current_app.logger.warning("StockPct parcial falló: %s", e)
    for r in rows:
        try:
            num = int(r.get("NumNota") or 0)
        except Exception:
            num = 0
        r["StockPct"] = stock_map.get(num, 0)
    t_stk1 = time.perf_counter()

    # ====== ZONAS (para nombre visual) ======
    try:
        from db_utils import get_zonas as _get_zonas_func
        zonas = _get_zonas_func() or []
    except Exception as e:
        current_app.logger.warning("No se pudieron cargar ZONAS: %s", e)
        zonas = []

    id2zona = {}
    for z in zonas:
        try:
            zid = z.get("ID_ZONA")
            nom = z.get("NOMBRE")
            if zid is not None:
                id2zona[int(zid)] = nom
        except Exception:
            continue

    # ====== NV_FLOW en lote (con fallback) ======
    t_flow0 = time.perf_counter()
    try:
        nums = [int(r.get("NumNota") or 0) for r in rows if r.get("NumNota") is not None]
        nums = [n for n in nums if n > 0]
        flow_map = {}
        if nums:
            in_params = ", ".join([f":nv{i}" for i in range(len(nums))])
            params = {f"nv{i}": nums[i] for i in range(len(nums))}
            sql_flow_last = f"""
                SELECT f.NV, f.ESTADO, f.ZONA_ID, f.RETIRA_CLIENTE
                FROM RIMA.dbo.NV_FLOW f
                JOIN (
                    SELECT NV, MAX(ID) AS max_id
                    FROM RIMA.dbo.NV_FLOW
                    WHERE NV IN ({in_params})
                    GROUP BY NV
                ) x ON x.NV = f.NV AND x.max_id = f.ID;
            """
            df_flow = db.query_df(sql_flow_last, params)
            if df_flow is None or df_flow.empty:
                sql_flow_any = f"""
                    SELECT NV, MAX(ESTADO) AS ESTADO, MAX(ZONA_ID) AS ZONA_ID, MAX(RETIRA_CLIENTE) AS RETIRA_CLIENTE
                    FROM RIMA.dbo.NV_FLOW
                    WHERE NV IN ({in_params})
                    GROUP BY NV;
                """
                df_flow = db.query_df(sql_flow_any, params)
            if df_flow is not None and not df_flow.empty:
                for _, row in df_flow.iterrows():
                    try:
                        nv = int(row.get("NV") or 0)
                        flow_map[nv] = {
                            "ESTADO": (row.get("ESTADO") or "").strip().upper(),
                            "ZONA_ID": row.get("ZONA_ID"),
                            "RETIRA_CLIENTE": row.get("RETIRA_CLIENTE"),
                        }
                    except Exception:
                        continue
        for r in rows:
            try:
                num = int(r.get("NumNota") or 0)
            except Exception:
                num = 0
            data = flow_map.get(num, {})
            estado_raw = (data.get("ESTADO") or "").strip().upper()
            r["EstadoFlow"]    = estado_raw if estado_raw in ("APROB", "PEND") else ""
            r["RetiraCliente"] = 1 if data.get("RETIRA_CLIENTE") else 0
            r["ZonaID"]        = data.get("ZONA_ID")
            try:
                r["ZonaAsignada"]  = id2zona.get(int(r["ZonaID"])) if r["ZonaID"] is not None else None
            except Exception:
                r["ZonaAsignada"]  = None
        if not flow_map:
            raise RuntimeError("No hubo resultados NV_FLOW en batch, usando fallback por fila.")
    except Exception:
        # Fallback a método original
        for r in rows:
            try:
                num = int(r.get("NumNota") or 0)
            except Exception:
                num = 0
            try:
                flow_row = get_nv_flow(num) or {}
            except Exception:
                flow_row = {}
            estado_raw = (flow_row.get("ESTADO") or "").strip().upper()
            r["EstadoFlow"]    = estado_raw if estado_raw in ("APROB", "PEND") else ""
            r["RetiraCliente"] = 1 if flow_row.get("RETIRA_CLIENTE") else 0
            r["ZonaID"]        = flow_row.get("ZONA_ID")
            try:
                r["ZonaAsignada"]  = id2zona.get(int(r["ZonaID"])) if r["ZonaID"] is not None else None
            except Exception:
                r["ZonaAsignada"]  = None
    t_flow1 = time.perf_counter()

    # ====== Filtro por FLOW (sobre la página) ======
    if flow == 'APROB':
        rows = [r for r in rows if r.get("EstadoFlow") == 'APROB']
    elif flow == 'PEND':
        rows = [r for r in rows if r.get("EstadoFlow") == 'PEND']
    elif flow == 'UNSET':
        rows = [r for r in rows if not r.get("EstadoFlow")]

    current_app.logger.info(
        "NV_gestionar: page_lite=%.3fs | obs_batch=%.3fs | stk_batch%s=%.3fs | flow_batch=%.3fs | extras=%s | months=%d | filtros estado=%s flow=%s meses=%s | page=%d per_page=%d total=%d mostrados=%d order=%s",
        (t1 - t0),
        (t_obs1 - t_obs0),
        "(off)" if nostock else "",
        (t_stk1 - t_stk0),
        (t_flow1 - t_flow0),
        str(extras), default_months,
        estado or '-', flow, ','.join(meses) or '-',
        page, per_page, total, len(rows), order
    )

    return render_template(
        "admin/nv_gestionar.html",
        rows=rows,
        zonas=zonas,
        page=page,
        per_page=per_page,
        total=total,
        total_pages=total_pages,
        estado=estado,
        flow=flow,
        estado_filtro=flow,
        meses=meses,
        order=order,
    )
from flask import jsonify

from flask import jsonify

@app.post("/admin/nv/update")
def admin_nv_update():
    # seguridad
    cu = session.get('current_user') or {}
    if not (session.get('is_admin') and cu.get('rol') == ROL_JEFE):
        return jsonify(ok=False, error="Forbidden"), 403

    try:
        data = request.get_json(force=True) or {}
        nv = int(data.get("nv") or 0)
        if nv <= 0:
            return jsonify(ok=False, error="NV inválida"), 400

        estado = (data.get("estado") or "").strip().upper()
        if estado not in ("", "APROB", "PEND"):
            return jsonify(ok=False, error="Estado inválido"), 400

        zona_raw = (data.get("zona_id") if "zona_id" in data else None)
        zona_id = int(str(zona_raw).strip()) if (zona_raw is not None and str(zona_raw).strip().isdigit()) else None
        retira = 1 if (data.get("retira_cliente") in (1, True, "1", "true", "TRUE")) else 0

        # ---------- 1) Intento en RIMA (usa NUMNOTA) ----------
        rima_ok = False
        rima_err = None
        try:
            # si tienes db_utils configurado para RIMA:
            from db_utils import upsert_nv_flow as _rima_upsert, get_nv_flow as _rima_get
            # upsert en RIMA; pasa solo los campos presentes
            kwargs = {}
            if estado != "": kwargs["ESTADO"] = estado
            if zona_id is not None: kwargs["ZONA_ID"] = zona_id
            kwargs["RETIRA_CLIENTE"] = retira
            _rima_upsert(nv, **kwargs)  # MERGE RIMA.dbo.NV_FLOW (NUMNOTA)
            flow = _rima_get(nv) or {}
            payload = {
                "NV": nv,
                "ESTADO": (flow.get("ESTADO") or "").strip().upper(),
                "ZONA_ID": flow.get("ZONA_ID"),
                "RETIRA_CLIENTE": int(bool(flow.get("RETIRA_CLIENTE", 0))),
            }
            rima_ok = True
        except Exception as e:
            current_app.logger.warning("admin_nv_update: RIMA fallback (%s)", e)
            rima_err = str(e)

        # ---------- 2) Fallback local dbo.NV_FLOW (NV nvarchar) ----------
        if not rima_ok:
            # crea la tabla local si no existe y guarda
            _ensure_nv_flow_table()
            _upsert_nv_flow_state(
                nv=str(nv),
                estado=estado if estado != "" else None,
                retira=retira,
                zona_id=(str(zona_id) if zona_id is not None else None),
            )
            # leer lo guardado localmente
            df = db.query_df(
                "SELECT NV, ESTADO, ZONA_ID, RETIRA_CLIENTE FROM dbo.NV_FLOW WHERE NV = :nv",
                {"nv": str(nv)}
            )
            if df is None or df.empty:
                raise RuntimeError("No se pudo leer NV guardada en dbo.NV_FLOW")
            row = df.to_dict(orient="records")[0]
            payload = {
                "NV": row.get("NV"),
                "ESTADO": (row.get("ESTADO") or "").strip().upper(),
                "ZONA_ID": row.get("ZONA_ID"),
                "RETIRA_CLIENTE": int(bool(row.get("RETIRA_CLIENTE", 0))),
                "_source": "local"
            }
            if rima_err:
                payload["_rima_error"] = rima_err  # útil para debug en logs

        return jsonify(ok=True, data=payload)

    except Exception as e:
        current_app.logger.exception("admin_nv_update failed")
        return jsonify(ok=False, error=str(e)), 500



@app.post("/admin/nv/accion", endpoint="nv_gestionar_accion")
def nv_gestionar_accion():
    # Requiere rol Jefe
    cu = session.get('current_user') or {}
    if not (session.get('is_admin') and cu.get('rol') == ROL_JEFE):
        return abort(403)

    accion = (request.form.get("accion") or "").strip().lower()
    # Acepta tanto "nv" como "numnota" para no tocar el template
    nv = (request.form.get("nv") or request.form.get("numnota") or "").strip()

    if not nv:
        flash("Falta el número de Nota de Venta.", "warning")
        return redirect(url_for("admin_nv_gestionar"))

    # Asegura tablas necesarias
    _ensure_nv_zonas_table()
    _ensure_nv_flow_table()

    # 1) Marcar como PENDIENTE: elimina zona y guarda ESTADO='PEND'
    if accion == "pendiente":
        try:
            db.execute("DELETE FROM dbo.NV_ZONAS WHERE NV = :nv", {"nv": nv})
            _upsert_nv_flow_state(nv=nv, estado="PEND")  # <<< aquí queda explícito el estado
            flash(f"NV {nv} marcada como PENDIENTE (zona eliminada).", "success")
        except Exception as e:
            flash(f"No se pudo marcar como pendiente: {e}", "danger")
        return redirect(url_for("admin_nv_gestionar"))

    # 2) Aprobar + Asignar zona (o simple asignación): requiere zona
    zona = (request.form.get("zona") or "").strip()
    if not zona:
        flash("Selecciona una zona para asignar.", "warning")
        return redirect(url_for("admin_nv_gestionar"))

    try:
        # UPSERT en NV_ZONAS
        db.execute("""
            IF EXISTS(SELECT 1 FROM dbo.NV_ZONAS WHERE NV = :nv)
                UPDATE dbo.NV_ZONAS SET ZONA = :zona WHERE NV = :nv;
            ELSE
                INSERT INTO dbo.NV_ZONAS(NV, ZONA) VALUES(:nv, :zona);
        """, {"nv": nv, "zona": zona})

        # Guardar ESTADO='APROB' explícitamente en el flujo
        _upsert_nv_flow_state(nv=nv, estado="APROB")

        flash(f"NV {nv} aprobada y asignada a la zona '{zona}'.", "success")
    except Exception as e:
        flash(f"No se pudo asignar la zona/aprobar: {e}", "danger")

    return redirect(url_for("admin_nv_gestionar"))


# ===== Helpers de flujo =====

def _ensure_nv_flow_table():
    """
    Crea dbo.NV_FLOW si no existe.
    Guarda el estado manual del flujo:
      - ESTADO: 'APROB' o 'PEND'
      - RETIRA_CLIENTE: bit opcional (se puede usar en otra acción)
      - ZONA_ID: opcional (si algún día lo quieres acoplar aquí)
    """
    sql = """
    IF OBJECT_ID('dbo.NV_FLOW','U') IS NULL
    BEGIN
        CREATE TABLE dbo.NV_FLOW(
            NV              NVARCHAR(50)  NOT NULL PRIMARY KEY,
            ESTADO          NVARCHAR(10)  NULL,   -- 'APROB' | 'PEND' | NULL
            RETIRA_CLIENTE  BIT           NULL,
            ZONA_ID         NVARCHAR(100) NULL,
            UPDATED_AT      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
        );
    END
    """
    db.execute(sql)

def _upsert_nv_flow_state(nv: str, estado: str | None = None,
                          retira: int | None = None, zona_id: str | None = None):
    """
    Actualiza/crea el registro de flujo para la NV.
    - Si 'estado' es None, conserva el existente.
    - Idem para 'retira' y 'zona_id'.
    """
    # Normaliza estado a los únicos admitidos
    estado = (estado or "").strip().upper()
    if estado not in ("APROB", "PEND", ""):
        estado = ""

    params = {
        "nv": nv,
        "estado": (None if estado == "" else estado),
        "retira": retira,
        "zona_id": zona_id,
    }

    sql = """
    MERGE dbo.NV_FLOW AS T
    USING (SELECT :nv AS NV) AS S
    ON T.NV = S.NV
    WHEN MATCHED THEN UPDATE SET
        ESTADO         = COALESCE(:estado, T.ESTADO),
        RETIRA_CLIENTE = COALESCE(:retira, T.RETIRA_CLIENTE),
        ZONA_ID        = COALESCE(:zona_id, T.ZONA_ID),
        UPDATED_AT     = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
        INSERT (NV, ESTADO, RETIRA_CLIENTE, ZONA_ID, UPDATED_AT)
        VALUES (:nv, :estado, :retira, :zona_id, SYSUTCDATETIME());
    """
    db.execute(sql, params)





@app.route('/inventario', methods=['GET', 'POST'])
@app.route('/inventario/sesion/<sesion_id>', methods=['GET', 'POST'])
def inventario(sesion_id=None):
    if sesion_id:
        session['inv_sesion_id'] = sesion_id

    inv_id = session.get('inv_sesion_id')
    inv_data = inv_get_session(inv_id) if inv_id else None
    inv_estado = inv_data.get('estado') if inv_data else None

    expected_items = session.get('expected_items', [])
    scanned_items  = session.get('scanned_inv', [])

    if request.method == 'POST':
        action = request.form.get('action')

        if action == 'crear_sesion':
            new_id = inv_create_session()
            session['inv_sesion_id'] = new_id
            session.pop('expected_items', None)
            session.pop('scanned_inv', None)
            return redirect(url_for('inventario', sesion_id=new_id))

        if action == 'cargar_inv':
            if not os.path.exists(STOCK_FILE):
                flash('No se encontró el archivo de stock.', 'error')
            else:
                try:
                    df = pd.read_csv(
                        STOCK_FILE,
                        header=0,
                        dtype=str,
                        keep_default_na=False
                    )
                    df.columns = [c.strip() for c in df.columns]
                    df = df.loc[:, ~df.columns.str.match(r'^Unnamed', case=False)]
                    if 'Cantidad' in df.columns:
                        df['Cantidad'] = pd.to_numeric(
                            df['Cantidad'], errors='coerce'
                        ).fillna(0).astype(int)
                    expected_items = df[['Código', 'Nombre', 'Cantidad']].to_dict(
                        orient='records'
                    )
                    session['expected_items'] = expected_items
                    session['scanned_inv'] = []
                    flash(
                        f'Se cargaron {len(expected_items)} ítems de inventario.',
                        'success'
                    )
                except Exception as e:
                    app.logger.error(f"Error al leer stock: {e}")
                    flash('Error al leer el archivo de stock.', 'error')

        elif action == 'scan_inv':
            codigo = (request.form.get('codigo') or '').strip()
            try:
                contado = int(request.form.get('contado', 1))
            except ValueError:
                contado = 1

            ahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if any(item['Código'] == codigo for item in expected_items):
                found = False
                for s in scanned_items:
                    if s['Código'] == codigo:
                        s['Contado'] += contado
                        s['Hora'] = ahora
                        total = s['Contado']
                        found = True
                        break
                if not found:
                    total = contado
                    scanned_items.append({
                        'Código': codigo,
                        'Contado': total,
                        'Hora': ahora
                    })
                session['scanned_inv'] = scanned_items
                flash(
                    f'Conteo para {codigo} incrementado en {contado}. Total: {total}',
                    'success'
                )
            else:
                flash(
                    f'Código {codigo} no está en el inventario esperado.',
                    'warning'
                )

        elif action == 'export_inv':
            results = []
            for exp in expected_items:
                cnt = next((s['Contado'] for s in scanned_items if s['Código'] == exp['Código']), 0)
                results.append({
                    'Código': exp['Código'],
                    'Nombre': exp['Nombre'],
                    'Esperado': exp['Cantidad'],
                    'Contado': cnt,
                    'Diferencia': cnt - exp['Cantidad']
                })
            if results:
                output = io.StringIO()
                writer = csv.DictWriter(output, fieldnames=['Código', 'Nombre', 'Esperado', 'Contado', 'Diferencia'])
                writer.writeheader()
                writer.writerows(results)
                output.seek(0)
                return send_file(
                    io.BytesIO(output.getvalue().encode('utf-8-sig')),
                    mimetype='text/csv',
                    as_attachment=True,
                    download_name='inventario_resultados.csv'
                )
            flash('No hay datos de inventario para exportar.', 'warning')

        return redirect(url_for('inventario', sesion_id=inv_id) if inv_id else url_for('inventario'))

    results = []
    for exp in expected_items:
        cnt = next((s['Contado'] for s in scanned_items if s['Código']==exp['Código']), None)
        results.append({
            'Código':   exp['Código'],
            'Nombre':   exp['Nombre'],
            'Esperado': exp['Cantidad'],
            'Contado':  cnt if cnt is not None else '',
            'Diferencia': '' if cnt is None else (cnt - exp['Cantidad'])
        })

    return render_template('inventario.html',
                           expected=expected_items,
                           scanned=scanned_items,
                           results=results,
                           inv_sesion_id=inv_id,
                           inv_estado=inv_estado)

# ─── Ruta /importar ─────────────────────────────────────────────────────────

# Asume que ya tienes definidos:
# UPLOADS_DIR, DATA_DIR, allowed_file, OC_FILE, NV_FILE, MASTER_FILE, STOCK_FILE

@app.route("/importar", methods=["GET", "POST"])
def importar():
    """
    Sube un archivo Excel/CSV a uploads/{tipo}/ y guarda un CSV limpio en DATA_DIR.
    Detecta cabecera real, elimina Unnamed y guarda con UTF-8 BOM.
    Tipos válidos: oc, nv, master, stock.
    """
    if request.method == "POST":
        tipo = (request.form.get("tipo") or "").lower()
        f    = request.files.get("file")

        # ── 1. Validaciones básicas ───────────────────────────────────────
        if tipo not in {"oc", "nv", "master", "stock"}:
            flash("Debes seleccionar un tipo válido.", "warning")
            return redirect(url_for("importar"))
        if not f or not allowed_file(f.filename):
            flash("Formato no soportado. Usa CSV o Excel.", "warning")
            return redirect(url_for("importar"))

        # ── 2. Guardar copia original ────────────────────────────────────
        uploads_tipo = os.path.join(UPLOADS_DIR, tipo)
        os.makedirs(uploads_tipo, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        original_name = f"{tipo}_{ts}_{secure_filename(f.filename)}"
        orig_path     = os.path.join(uploads_tipo, original_name)
        f.save(orig_path)

        # ── 3. Vista previa para detectar cabecera ───────────────────────
        ext       = orig_path.rsplit(".", 1)[-1].lower()
        read_excel = ext in {"xls", "xlsx"}

        if read_excel:
            preview = pd.read_excel(
                orig_path,
                header=None,
                nrows=30,
                dtype=str,
                keep_default_na=False
            )
        else:
            preview = pd.read_csv(
                orig_path,
                header=None,
                nrows=30,
                dtype=str,
                keep_default_na=False,
                encoding="latin-1",
                sep=","
            )

        expected_map = {
            "oc":     {"no.", "oc", "ciudad"},
            "nv":     {"ciudad", "num", "nota", "rut"},
            "master": set(),
            "stock":  {"ciudad", "bodega", "codigo", "nombre", "cantidad"},
        }
        expected = expected_map.get(tipo, set())

        header_row = None
        if expected:
            for idx, row in preview.iterrows():
                cells = {str(c).lower() for c in row if str(c).strip()}
                if len(cells & expected) >= 2:
                    header_row = idx
                    break
        if header_row is None:
            header_row = 0  # respaldo

        # ── 4. Lectura definitiva con codificación y separador correctos ──
        if read_excel:
            df = pd.read_excel(
                orig_path,
                header=header_row,
                dtype=str,
                keep_default_na=False
            )
        else:
            df = pd.read_csv(
                orig_path,
                header=header_row,
                dtype=str,
                keep_default_na=False,
                encoding="latin-1",
                sep=","
            )

        # ── 5. Normalización de columnas ─────────────────────────────────
        df.columns = [str(c).strip().replace("\ufeff", "") for c in df.columns]

        # Eliminar Unnamed y columnas vacías
        df = df.loc[:, ~df.columns.str.match(r"^Unnamed", case=False)]
        df = df.dropna(axis=1, how="all")

        # ── 6. Guardar CSV limpio con BOM para futuras lecturas ──────────
        destino_map = {
            "oc":     (OC_FILE,     "Órdenes de Compra"),
            "nv":     (NV_FILE,     "Notas de Venta"),
            "master": (MASTER_FILE, "Maestro de Productos"),
            "stock":  (STOCK_FILE,  "Stock"),
        }
        dest_path, etiqueta = destino_map[tipo]
        df.to_csv(
            dest_path,
            index=False,
            encoding="utf-8-sig"
        )
        flash(f"{etiqueta} importadas correctamente ({len(df)} filas).", "success")
        return redirect(url_for("importar"))

    # GET
    return render_template("importar.html", tipos=["oc", "nv", "master", "stock"])

@app.route('/finalizar')
def finalizar():
    return render_template('finalizar.html')

@app.route('/finalizar_salida')
def finalizar_salida():
    """Pantalla de fin del flujo de salida."""
    cu = session.get('current_user')
    op = session.get('operario')
    if not cu:
        return redirect(url_for('login1'))
    if cu.get('rol') == ROL_OPERARIO and not op:
        return redirect(url_for('login2'))

    # 1) num_nota desde sesión
    raw = session.get('current_nv', None) or session.get('nv_para_guia')
    num_nota = None
    if raw not in (None, ""):
        try:
            num_nota = int(raw)
        except (TypeError, ValueError):
            num_nota = int(float(str(raw)))

    # 2) Traer OBS de la Nota de Venta (OBSGRAL), no la de factura
    obs_nv = ""
    if num_nota:
        try:
            # si ya creaste el helper en db.py:
            # df = db.get_nv_obs_gral(num_nota)
            # if df is not None and not df.empty:
            #     obs_nv = str(df.iloc[0]["OBSGRAL"] or "")

            # fallback directo por si no tienes el helper aún:
            df = db.query_df("""
                SELECT TOP (1)
                       CONVERT(NVARCHAR(4000), ISNULL(OBSGRAL, '')) AS OBSGRAL
                FROM [Santiago].dbo.NOTV_DB WITH (NOLOCK)
                WHERE CAST(NUMNOTA AS INT) = :n
                ORDER BY NUMREG DESC
            """, {"n": int(num_nota)})
            if df is not None and not df.empty:
                obs_nv = str(df.iloc[0]["OBSGRAL"] or "")
        except Exception:
            obs_nv = ""

    # 3) Render
    return render_template(
        'finalizar_salida.html',
        num_nota=num_nota,
        obs_nota_venta=obs_nv  # <-- NUEVO nombre claro
    )



@app.get("/boleta")
def boleta_alias():
    # Prioriza ?num_nota=..., si no hay usa lo que haya en sesión
    raw = (request.args.get("num_nota")
           or session.get("current_nv")
           or session.get("nv_para_guia"))

    if not raw:
        # Si no hay nada, vuelve a la pantalla final con un aviso
        try:
            flash("Falta el número de Nota de Venta (num_nota).", "warning")
        except Exception:
            pass
        return redirect(url_for("finalizar_salida"))

    # Normaliza a int (acepta '2326570' o '2326570.0')
    try:
        num_nota = int(raw)
    except (TypeError, ValueError):
        try:
            num_nota = int(float(str(raw)))
        except Exception:
            try:
                flash("num_nota inválido.", "warning")
            except Exception:
                pass
            return redirect(url_for("finalizar_salida"))

    # Redirige al endpoint tipado /boleta/<int:num_nota>
    return redirect(url_for("boleta_nv", num_nota=num_nota))

@app.route('/guia-despacho')
def guia_despacho():
    """
    Renderiza la Guía de Despacho usando SOLO los ítems escaneados
    y preparados durante la salida (session['items_para_guia']).

    Prefill:
      - Pago a  ← NOTV_DB.PAGOAN (solo lectura en HTML)
      - Observaciones para la Guía de Despacho ← NOTV_DB.OBSDESP (solo lectura en HTML)
      - Observaciones para la Factura de Venta ← NOTV_DB.OBSFACT (solo lectura en HTML / editable si quieres)
      - Referencias (OC/HES/Contrato/Pedido) ← NOTV_DB (solo lectura en HTML)
    """
    # --- Autenticación mínima ---
    cu = session.get('current_user')
    op = session.get('operario')
    if not cu:
        return redirect(url_for('login1'))
    if cu.get('rol') == ROL_OPERARIO and not op:
        return redirect(url_for('login2'))

    # --- NUM NOTA ---
    num_nota = (request.args.get('num_nota') or session.get('nv_para_guia') or '').strip()
    if not num_nota:
        flash('Falta el parámetro num_nota.', 'warning')
        return redirect(url_for('salida'))

    # --- Items escaneados en salida ---
    items_scan = session.get('items_para_guia', []) or []
    nv_scan    = (session.get('nv_para_guia') or '').strip()

    if not items_scan:
        flash('No hay productos preparados para la guía (no se han escaneado ítems).', 'warning')
        return redirect(url_for('salida'))
    if nv_scan and nv_scan != num_nota:
        flash(f'La NV preparada ({nv_scan}) no coincide con la solicitada ({num_nota}).', 'warning')
        return redirect(url_for('salida'))

    # --- 1) Cabecera y detalle completo de la NV (para cruzar con escaneos) ---
    try:
        header, detalles_full = db_utils.get_guia_desde_nv(num_nota)
        if not header:
            flash(f'No se encontró información para la Nota de Venta {num_nota}.', 'warning')
            return redirect(url_for('salida'))
    except Exception as e:
        flash(f'Error al consultar la BBDD: {e}', 'danger')
        return redirect(url_for('salida'))

    # --- 1.b) SUCURSAL para mostrar (CODIGO + DESCRIP desde CHOI_DB) ---
    # header['SUCUR'] = NVSUC; header puede traer SUCUR_CODIGO / SUCUR_DESCRIP si tu db.py ya lo hace.
    sucur_codigo = (str(header.get('SUCUR_CODIGO') or '')).strip()
    sucur_nombre = (str(header.get('SUCUR_DESCRIP') or '')).strip()
    sucur_nvsuc  = (str(header.get('SUCUR') or '')).strip()

    if not (sucur_codigo and sucur_nombre):
        # Fallback: buscamos en NOTV_DB + CHOI_DB
        try:
            df_suc = db.query_df("""
                SELECT TOP (1)
                       CONVERT(VARCHAR(20), nv.NVSUC)                  AS SUCUR_NVSUC,
                       LTRIM(RTRIM(ISNULL(ch.CODIGO,  '')))            AS CH_CODIGO,
                       LTRIM(RTRIM(CONVERT(NVARCHAR(200), ISNULL(ch.DESCRIP,'')))) AS CH_DESCRIP
                FROM dbo.NOTV_DB nv WITH (NOLOCK)
                LEFT JOIN dbo.CHOI_DB ch ON ch.NUMREG = nv.NVSUC
                WHERE CAST(nv.NUMNOTA AS INT) = :n
                ORDER BY nv.FECHA DESC, nv.NUMREG DESC
            """, {"n": int(float(num_nota))})
            if df_suc is not None and not df_suc.empty:
                sucur_nvsuc  = (df_suc.iloc[0]["SUCUR_NVSUC"] or "").strip()
                sucur_codigo = (df_suc.iloc[0]["CH_CODIGO"] or "").strip()
                sucur_nombre = (df_suc.iloc[0]["CH_DESCRIP"] or "").strip()
        except Exception:
            pass

    # --- 2) Retira cliente (flow) ---
    try:
        flow = get_nv_flow(int(num_nota)) or {}
    except Exception:
        flow = {}
    retira_cliente = bool(flow.get("RETIRA_CLIENTE"))

    # --- 3) Prefill: PAGOAN + OBSERVACIONES (OBSDESP y OBSFACT) ---
    def _get_from_header(hdr: dict):
        pago_raw = hdr.get('PAGOAN') or hdr.get('PagoA') or hdr.get('PAGO_A') or 0
        try:
            pago_val = int(pago_raw or 0)
        except Exception:
            pago_val = 0
        obs_guia = hdr.get('OBSDESP') or ''
        obs_fact = hdr.get('OBSFACT') or ''
        return pago_val, str(obs_guia or ''), str(obs_fact or '')

    pago_a_dias, obs_guia_desp, obs_factura = _get_from_header(header)

    # Refuerzo directo a NOTV_DB por si el header no traía los campos
    try:
        df_obs = db.query_df("""
            SELECT TOP (1)
                   CAST(ISNULL(OBSDESP,'') AS NVARCHAR(4000)) AS OBSDESP,
                   CAST(ISNULL(OBSFACT,'') AS NVARCHAR(4000)) AS OBSFACT,
                   ISNULL(PAGOAN, 0) AS PAGOAN
            FROM [Santiago].dbo.NOTV_DB WITH (NOLOCK)
            WHERE CAST(NUMNOTA AS INT) = :n
            ORDER BY NUMREG DESC
        """, {"n": int(float(num_nota))})
        if df_obs is not None and not df_obs.empty:
            row = df_obs.iloc[0]
            if not pago_a_dias:
                try:
                    pago_a_dias = int(row["PAGOAN"] or 0)
                except Exception:
                    pass
            if not (obs_guia_desp or '').strip():
                obs_guia_desp = str(row["OBSDESP"] or "")
            if not (obs_factura or '').strip():
                obs_factura = str(row["OBSFACT"] or "")
    except Exception:
        pass

    if not pago_a_dias:
        pago_a_dias = 30

    # --- 3.b) Referencias NV (OC/HES/Contrato/Pedido) ---
    refs = {
        "oc_ref": "", "fecha_oc_ref": "",
        "hes_ref": "", "fecha_hes_ref": "",
        "numero_contrato": "", "numero_pedido": ""
    }
    try:
        df_refs = db.query_df("""
            SELECT TOP (1)
                ISNULL(NROOCCODE,'')  AS oc_ref,
                CONVERT(VARCHAR(10), CAST(FCHOCCODE  AS DATE), 23) AS fecha_oc_ref,
                ISNULL(NROHESCODE,'') AS hes_ref,
                CONVERT(VARCHAR(10), CAST(FCHHESCODE AS DATE), 23) AS fecha_hes_ref,
                ISNULL(NROCONTRATO,'') AS numero_contrato,
                ISNULL(NROPEDIDO,'')   AS numero_pedido
            FROM [Santiago].dbo.NOTV_DB WITH (NOLOCK)
            WHERE CAST(NUMNOTA AS INT) = :n
            ORDER BY NUMREG DESC
        """, {"n": int(float(num_nota))})
        if df_refs is not None and not df_refs.empty:
            r = df_refs.iloc[0].to_dict()
            refs.update({k: (str(v) if v is not None else "") for k, v in r.items()})
    except Exception as e:
        current_app.logger.warning("guia_despacho: no pude leer referencias NV %s: %s", num_nota, e)

    # ===============================
    # 4) Armar detalle desde escaneos
    # ===============================
    import re

    def _norm_code_any(x) -> list[str]:
        s = str(x or "").strip()
        if not s:
            return []
        out = {s, s.lower()}
        digits = re.sub(r"\D", "", s)
        if digits:
            out.add(digits)
            out.add(digits.lstrip("0"))
            if len(digits) == 14 and digits[0] == "0":
                out.add(digits[1:])
        return [v for v in out if v]

    def _norm_guia_row(base: dict) -> dict:
        if base is None:
            base = {}
        desc = (base.get("nombre") or base.get("Nombre") or
                base.get("DESCRIP") or base.get("Descripcion") or base.get("DESCRIPCION") or "")
        prec = (base.get("prec_unit") or base.get("Prec.Unit") or base.get("PRECUNIT") or
                base.get("Precio")    or base.get("PRECIO")    or 0)
        cant = (base.get("cantidad") or base.get("Cantidad") or base.get("CANTIDAD") or 0)
        dcto = base.get("D%") or base.get("DESCTO") or base.get("descto") or 0
        return {**base, "descripcion": desc, "precio": prec, "cantidad": cant, "dcto": dcto}

    detalles_map: dict[str, dict] = {}
    for d in (detalles_full or []):
        keys = []
        for k in ("codigo", "Código", "Codigo", "NCODART", "Ncodart", "CODIGO2", "codigo2"):
            keys += _norm_code_any(d.get(k))
        for k in keys:
            if k and k not in detalles_map:
                detalles_map[k] = d

    detalles = []
    for it in (items_scan or []):
        scan_keys = _norm_code_any(it.get('codigo') or it.get('Codigo') or it.get('NCODART'))
        qty = int(it.get('cantidad') or it.get('Cantidad') or 0)
        if qty <= 0:
            continue
        base = {}
        for k in scan_keys:
            base = detalles_map.get(k)
            if base:
                break
        base_norm = _norm_guia_row(base or {})
        cod_visible = (scan_keys[0] if scan_keys else
                       (base_norm.get("Codigo") or base_norm.get("NCODART") or ""))
        detalles.append({
            'codigo': cod_visible,
            'nombre': base_norm["descripcion"],
            'cantidad': qty,
            'prec_unit': base_norm["precio"],
        })

    if not detalles:
        flash('No hay líneas válidas para la Guía (cantidades <= 0).', 'warning')
        return redirect(url_for('salida'))

    obsfact_inicial = obs_factura

    # --- 5) Render ---
    return render_template(
        'guia_despacho.html',
        header=header,
        detalles=detalles,
        num_nota=num_nota,
        datos={},
        datetime=datetime,
        retira_cliente=retira_cliente,
        pago_a_dias=pago_a_dias,          # Solo lectura
        obs_guia_desp=obs_guia_desp,      # OBSDESP
        obs_factura=obs_factura,          # OBSFACT
        obsfact_inicial=obsfact_inicial,
        refs=refs,
        # NUEVO: sucursal para mostrar
        sucur_codigo=sucur_codigo,        # CHOI_DB.CODIGO (p.ej. 115 / ST)
        sucur_nombre=sucur_nombre,        # CHOI_DB.DESCRIP (p.ej. Sucursal Santiago)
        sucur_nvsuc=sucur_nvsuc,          # NVSUC numérico (1222 / 160029)
    )



@app.route('/guia_traslado', methods=['GET', 'POST'])
def guia_traslado():
    """Genera la vista de la Guía de Traslado reutilizando la lógica de
    :func:`guia_despacho`.

    Solo cambia la plantilla y el mensaje de confirmación.
    """
    return guia_despacho_view(
        template_name='guia_traslado.html',
        flash_msg='Guía de traslado guardada correctamente.'
    )

@app.route('/descargar_xls')
def descargar_xls():
    cu = session.get('current_user')
    op = session.get('operario')
    if not cu:
        return redirect(url_for('login1'))
    if cu.get('rol') == ROL_OPERARIO and not op:
        return redirect(url_for('login2'))

    path = session.get('guia_file')
    if not path or not os.path.exists(path):
        abort(404)
    return send_file(
        path,
        as_attachment=True,
        download_name=os.path.basename(path),
        mimetype='application/vnd.ms-excel'
    )                                                                                                                                                                                                                                                                                                                                                                                                                                                                      

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
