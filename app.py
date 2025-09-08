import os
import csv
import math
import io
import logging
from datetime import datetime
from flask import (
    Flask, render_template, request, redirect,
    url_for, flash, session, send_file, current_app, abort
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

@app.post("/admin/nv/<int:num_nota>/asignar")
def admin_nv_asignar(num_nota):
    if not _es_admin():
        flash("No autorizado.", "error")
        return _back_to_gestionar()

    try:
        zona_id = int(request.form.get("zona_id") or 0)
    except ValueError:
        zona_id = 0
    if not zona_id:
        flash("Debes seleccionar una zona.", "warning")
        return _back_to_gestionar()

    # 1) Guarda en el flow (ID de zona, asignado_por, fechas, etc.)
    set_nv_zona(
        num_nota,
        zona_id,
        asignado_por=(session.get('current_user') or {}).get('nombre')
    )

    # 2) --- SYNC con dbo.NV_ZONAS (lo que usa /salida) --------------------
    #    a) obtén el nombre de la zona desde RIMA.dbo.ZONAS_DB
    try:
        df = db.query_df_rima(
            "SELECT NOMBRE FROM dbo.ZONAS_DB WHERE ID = :i",
            {"i": zona_id}
        )
        zona_nombre = (df.iloc[0]["NOMBRE"] if not df.empty else None)
    except Exception:
        zona_nombre = None

    if zona_nombre:
        #    b) crea la tabla NV_ZONAS si no existe
        _ensure_nv_zonas_table()  # ya la tienes definida

        #    c) upsert en dbo.NV_ZONAS (en la BD principal que lee /salida)
        db.execute("""
            IF EXISTS (SELECT 1 FROM dbo.NV_ZONAS WHERE NV = :nv)
                UPDATE dbo.NV_ZONAS SET ZONA = :zona WHERE NV = :nv;
            ELSE
                INSERT INTO dbo.NV_ZONAS (NV, ZONA) VALUES (:nv, :zona);
        """, {"nv": str(num_nota), "zona": zona_nombre})

    flash("Zona asignada.", "success")
    return _back_to_gestionar()

# app.py (imports)
from flask import request, session, g, redirect, url_for

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

    # Disponible en templates como g.is_rf si quieres
    g.is_rf = bool(session.get('is_rf', False))

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

    # 6) Ir a la guía (pasando la NV si la tenemos)
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

@app.route("/factura", methods=["GET"], endpoint="factura_nv")
def factura_nv():
    cu = session.get('current_user'); op = session.get('operario')
    if not cu: return redirect(url_for('login1'))
    if cu.get('rol') == ROL_OPERARIO and not op: return redirect(url_for('login2'))

    num_nota = (request.args.get("num_nota") or "").strip()
    if not num_nota:
        flash("Falta el número de Nota de Venta (num_nota).", "warning")
        return redirect(url_for("listado_nv"))

    # 1) Solo cabecera desde la NV
    try:
        header, detalles_full = db_utils.get_guia_desde_nv(num_nota)
        if not header:
            flash(f"No se encontró la NV {num_nota}.", "warning")
            return redirect(url_for("listado_nv"))
    except Exception as e:
        flash(f"Error al consultar la BBDD: {e}", "danger")
        return redirect(url_for("listado_nv"))

    # 2) Armar detalle SOLO con lo escaneado (igual que guía)
    items_scan = session.get('items_para_guia', []) or []
    if not items_scan:
        flash("No hay productos preparados (no se han escaneado ítems).", "warning")
        return redirect(url_for("listado_nv"))

    def _norm(x): return str(x).strip().strip('*').upper()
    base_map = {}
    for d in (detalles_full or []):
        cod = d.get('codigo') or d.get('Código') or d.get('NCODART') or d.get('cod') or ''
        base_map[_norm(cod)] = d

    detalles = []
    for it in items_scan:
        cod = _norm(it.get('codigo', ''))
        qty = int(it.get('cantidad', 0))
        if qty <= 0: continue
        base = base_map.get(cod, {})
        detalles.append({
            'codigo': cod,
            'nombre': base.get('nombre') or base.get('Nombre') or base.get('DESCRIP') or '',
            'cantidad': qty,
            'prec_unit': base.get('prec_unit') or base.get('Prec.Unit') or base.get('PRECUNIT') or 0
        })

    header = dict(header or {})
    header.setdefault("FAV_A", header.get("cliente") or header.get("razon_social") or "")
    header.setdefault("VEND_CODIGO", header.get("vendedor_codigo") or "")

    return render_template("factura_nv.html", header=header, detalles=detalles, num_nota=num_nota)



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
                df = db_utils.get_nota_detalle(nota)
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
                flash(f'Error al consultar la BBDD: {e}', 'danger')

            return redirect(url_for('salida'))

        # 2) Escanear (agregar item a salida)
        elif action in ('escanear', 'scan'):
            codigo = (request.form.get('codigo') or '').strip()
            cant   = request.form.get('cantidad') or '1'
            try:
                cant = int(cant)
            except Exception:
                cant = 1

            if not nv_items:
                flash('Primero busca una Nota de Venta.', 'warning')
                return redirect(url_for('salida'))

            base = pd.DataFrame(nv_items)
            codigos = (
                base["Código"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
            )
            match = base[codigos == codigo]
            if match.empty:
                flash(f'El código {codigo} no está en la Nota de Venta {nota}.', 'warning')
                return redirect(url_for('salida'))

            row = match.iloc[0].to_dict()

            # Agrega o acumula
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
                    'Cant.NV':     int(row.get('Cant.', 0)),   # pendiente permitido por NV
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

            # Guardar estado en NV_ZONAS. Si no existe la columna ESTADO, se crea.
            try:
                db.execute("""
                    IF OBJECT_ID('dbo.NV_ZONAS','U') IS NULL
                    BEGIN
                        CREATE TABLE dbo.NV_ZONAS(
                            NV   NVARCHAR(50)  NOT NULL PRIMARY KEY,
                            ZONA NVARCHAR(100) NOT NULL
                        );
                    END;

                    IF COL_LENGTH('dbo.NV_ZONAS','ESTADO') IS NULL
                        ALTER TABLE dbo.NV_ZONAS ADD ESTADO NVARCHAR(20) NULL;

                    IF EXISTS(SELECT 1 FROM dbo.NV_ZONAS WHERE NV = :nv)
                        UPDATE dbo.NV_ZONAS SET ESTADO = :estado WHERE NV = :nv;
                    ELSE
                        INSERT INTO dbo.NV_ZONAS(NV, ZONA, ESTADO) VALUES(:nv, N'(SIN ZONA)', :estado);
                """, {"nv": nota, "estado": estado_nv})
            except Exception as e:
                app.logger.error(f"No se pudo actualizar estado NV {nota}: {e}")

            # Preparar SOLO lo escaneado para la Guía (y/o Factura)
            # Esto es lo que leerá guia_despacho_view desde session['items_para_guia'].
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
            # (opcional) Si ya manejas un número de guía en /salida, lo guardas aquí:
            session['guia_para_guia']  = session.get('current_guia', '')

            # Limpieza de ítems de salida (ya quedaron guardados para la guía)
            session.pop('salida_items', None)

            flash(f"Salida finalizada correctamente. Estado NV: {estado_nv}.", 'success')
            return redirect(url_for('finalizar_salida'))



    # GET
    # Consultas auxiliares para hubs o zonas
    zona = request.args.get('zona')
    hub_id = request.args.get('hub_id', type=int)  # reservado por si se usa luego
    lista_nv = None
    zona_seleccionada = None
    if zona:
        zona_seleccionada = zona
        sql = """
        SELECT
            N.NUMNOTA,
            N.FECHA,
            N.SUCUR,
            ISNULL(C.RAZSOC, '') AS RAZSOC
        FROM dbo.NOTV_DB AS N
        JOIN dbo.NV_ZONAS AS Z      ON Z.NV = N.NUMNOTA      -- si tu tabla está en esta misma BD
        LEFT JOIN dbo.CLIEN_DB AS C ON C.NREGUIST = N.NRUTCLIE
        WHERE Z.ZONA = :zona
        ORDER BY N.NUMNOTA DESC
        """

        df_z = db.query_df(sql, {"zona": zona})
        if not df_z.empty:
            lista_nv = df_z.rename(columns={
                "NUMNOTA": "numnota",
                "FECHA": "fecha",
                "SUCUR": "sucursal",
                "RAZSOC": "cliente",
            }).to_dict(orient="records")
        else:
            flash('No hay Notas de Venta asignadas', 'info')

    # Calcular cantidades escaneadas y faltantes para cada ítem de la NV
    scanned_map = {}
    for si in salida_items:
        try:
            code = str(si.get('Código'))
            qty = int(si.get('Cant.Salida', 0))
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
        item2['Faltan'] = max(orig - scanned_qty, 0)
        display_nv_items.append(item2)

    # Cargar stock actual desde la BBDD y restar escaneos
    stock_items = []
    if display_nv_items:
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

        scanned_totals = {}
        for s in salida_items:
            k = str(s.get('Código')).strip()
            scanned_totals[k] = scanned_totals.get(k, 0) + int(s.get('Cant.Salida', 0))

        for line in display_nv_items:
            code = str(line.get('Código')).strip()
            orig = stock_map.get(code, {}).get('Cantidad', 0)
            remain = max(orig - scanned_totals.get(code, 0), 0)
            stock_items.append({
                'Código': code,
                'Nombre': stock_map.get(code, {}).get('Nombre', line.get('Nombre')),
                'Cantidad': remain
            })

    # NUEVO: pasar la lista de zonas al template para que /salida las muestre dinámicamente
    zonas = _get_zonas()

    return render_template(
        'salida.html',
        nota=nota,
        guia_actual=guia_actual,
        nv_items=display_nv_items,
        salida_items=salida_items,
        stock_items=stock_items,
        zona_seleccionada=zona_seleccionada,
        lista_nv=lista_nv,
        zonas=zonas                 # ← NUEVO
    )



@app.route("/admin/nv/gestionar", endpoint="admin_nv_gestionar")
def admin_nv_gestionar():
    cu = session.get('current_user') or {}
    if not (session.get('is_admin') and cu.get('rol') == ROL_JEFE):
        return abort(403)

    # ===== Filtros UI =====
    # 'estado' filtra por el estado CALCULADO de despacho (PENDIENTE/PARCIAL/TERMINADO)
    estado = (request.args.get('estado') or '').upper()
    meses  = request.args.getlist('mes')                  # ['2025-07','2025-08'] (YYYY-MM)
    page   = max(int(request.args.get('page', 1)), 1)
    per_page = min(max(int(request.args.get('per_page', 30)), 10), 300)

    # Orden permitido (whitelist)
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

    # ===== Filtro por mes(es) -> OR de rangos [ini, fin) =====
    date_filters = []
    for ym in meses:
        try:
            y, m = [int(x) for x in ym.split('-')]
            ini = f"{y:04d}-{m:02d}-01"
            fin = f"{(y + (m==12)) :04d}-{(1 if m==12 else m+1):02d}-01"
            date_filters.append(f"(nv.FECHA >= '{ini}' AND nv.FECHA < '{fin}')")
        except Exception:
            pass
    date_where = (" AND (" + " OR ".join(date_filters) + ") ") if date_filters else ""

    # ===== Estado de DESPACHO (calculado por cantidades) =====
    # OJO: esto NO es el flujo manual (APROB/PEND). Solo para filtros/indicadores.
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
    if estado in ('PENDIENTE','PARCIAL','TERMINADO'):
        having_estado = f" HAVING {estado_desp_case} = '{estado}' "

    offset = (page - 1) * per_page

    # En tu esquema real la relación cliente suele ser c.NREGUIST = nv.NRUTCLIE
    cliente_join = "c.NREGUIST = nv.NRUTCLIE"

    # ===== SQL principal con CTE y paginación =====
    main_sql = f"""
    WITH RES AS (
        SELECT
            nv.NUMNOTA                                        AS NumNota,
            nv.NRUTCLIE                                       AS RUT,
            nv.SUCUR                                          AS Ciudad,
            ISNULL(c.RAZSOC, '')                              AS RazonSocial,
            CAST(nv.FECHA AS DATE)                            AS FechaEntrega,
            SUM(CAST(ISNULL(nd.CANTIDAD,  0) AS INT))         AS Cantidad,
            SUM(CAST(ISNULL(nd.CANTDESP, 0) AS INT))          AS CantDesp,
            CAST(AVG(CAST(ISNULL(nd.PRECUNIT,0) AS FLOAT)) AS DECIMAL(18,0)) AS PrecioUnit,
            {estado_desp_case}                                AS EstadoDespacho
        FROM dbo.NOTV_DB nv
        JOIN dbo.NOTDE_DB nd ON nd.NUMRECOR = nv.NUMREG
        LEFT JOIN dbo.CLIEN_DB c ON {cliente_join}
        WHERE 1=1 {date_where}
        GROUP BY nv.NUMNOTA, nv.NRUTCLIE, nv.SUCUR, c.RAZSOC, nv.FECHA
        {having_estado}
    ),
    STK AS (
        SELECT v.NUMNOTA,
               CAST(AVG(CAST(ISNULL(v.STOCK_PCT,0) AS FLOAT)) AS DECIMAL(5,2)) AS StockPct
        FROM [RIMA].dbo.VW_NV_STOCK_PCT v
        GROUP BY v.NUMNOTA
    )
    SELECT R.*,
           ISNULL(S.StockPct, 0) AS StockPct
    FROM RES R
    LEFT JOIN STK S ON S.NUMNOTA = R.NumNota
    ORDER BY {order_sql}
    OFFSET {offset} ROWS FETCH NEXT {per_page} ROWS ONLY;
    """

    # Total para paginación (mismo filtro)
    cnt_sql = f"""
    SELECT COUNT(*) AS total
    FROM (
        SELECT nv.NUMNOTA
        FROM dbo.NOTV_DB nv
        JOIN dbo.NOTDE_DB nd ON nd.NUMRECOR = nv.NUMREG
        WHERE 1=1 {date_where}
        GROUP BY nv.NUMNOTA, nv.NRUTCLIE, nv.SUCUR, nv.FECHA
        {"HAVING " + estado_desp_case + " = '" + estado + "'" if having_estado else ""}
    ) X;
    """

    # Ejecuta y arma filas
    rows = db.query_df(main_sql).to_dict(orient="records")
    total_df = db.query_df(cnt_sql)
    total = int(total_df.iloc[0, 0]) if not total_df.empty else 0

    # Zonas y flow
    zonas = _get_zonas() if callable(globals().get("_get_zonas")) else []
    id2zona = {z.get("ID"): z.get("NOMBRE") for z in zonas if isinstance(z, dict)}

    for r in rows:
        num = int(r.get("NumNota") or 0)
        try:
            flow = get_nv_flow(num) or {}
        except Exception:
            flow = {}

        # === AQUÍ EL CAMBIO IMPORTANTE ===
        # Solo aceptar estados explícitos del FLUJO (manual). Nada por defecto.
        estado_raw = (flow.get("ESTADO") or "").strip().upper()
        r["EstadoFlow"] = estado_raw if estado_raw in ("APROB", "PEND") else ""

        r["RetiraCliente"] = 1 if flow.get("RETIRA_CLIENTE") else 0
        r["ZonaID"]        = flow.get("ZONA_ID")
        r["ZonaAsignada"]  = id2zona.get(r["ZonaID"])

    return render_template("admin/nv_gestionar.html",
                           rows=rows, zonas=zonas,
                           page=page, per_page=per_page, total=total,
                           estado=estado, meses=meses, order=order)



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



@app.post("/admin/nv/asignar")
def admin_nv_asignar_form():
    cu = session.get('current_user') or {}
    if not (session.get('is_admin') and cu.get('rol') == ROL_JEFE):
        return abort(403)
    nv = (request.form.get("nv") or "").strip()
    zona = (request.form.get("zona") or "").strip()
    if not nv or not zona:
        flash("Faltan datos para asignar zona.", "warning")
        return redirect(url_for("admin_nv_gestionar"))
    try:
        _ensure_nv_zonas_table()
        db.execute("""
            IF EXISTS(SELECT 1 FROM dbo.NV_ZONAS WHERE NV = :nv)
                UPDATE dbo.NV_ZONAS SET ZONA = :zona WHERE NV = :nv;
            ELSE
                INSERT INTO dbo.NV_ZONAS(NV, ZONA) VALUES(:nv, :zona);
        """, {"nv": nv, "zona": zona})
        flash(f"NV {nv} asignada a zona '{zona}'.", "success")
    except Exception as e:
        flash(f"No se pudo asignar la NV {nv}: {e}", "danger")
    return redirect(url_for("admin_nv_gestionar"))

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
    """Pantalla de fin del flujo de salida.

    Se aprovecha de esta vista para ofrecer la generación de la guía de despacho
    prellenada con los datos de la Nota de Venta trabajada. Para ello se toma el
    ``num_nota`` almacenado en la sesión antes de limpiar el resto del estado y
    se pasa como parámetro a la plantilla.
    """
    cu = session.get('current_user')
    op = session.get('operario')
    if not cu:
        return redirect(url_for('login1'))
    if cu.get('rol') == ROL_OPERARIO and not op:
        return redirect(url_for('login2'))

    num_nota = session.get('current_nv', '')

    # Limpiar el estado de la sesión para comenzar de cero si es necesario
    session.pop('nv_items', None)
    session.pop('salida_items', None)
    session.pop('current_nv', None)
    session.pop('current_guia', None)

    # Renderiza la plantilla final pasando el número de nota
    return render_template('finalizar_salida.html', num_nota=num_nota)

EXPORT_DIR = os.path.join(os.getcwd(), 'exports')
os.makedirs(EXPORT_DIR, exist_ok=True)

# Alias para que exista un endpoint 'guia_despacho' que invoque la misma lógica que 'salida'
def guia_despacho_view(template_name: str = 'guia_despacho.html',
                       flash_msg: str = 'Guía de despacho guardada correctamente.'):
    """Genera la vista de la Guía de Despacho.

    Lee los datos desde ``nv.csv`` y los traspasa a la guía. Se hace un
    esfuerzo por normalizar los nombres de columnas del archivo de notas de
    venta para que coincidan con los campos esperados en la plantilla, ya que
    los archivos provenientes de distintos orígenes suelen variar en tildes o
    abreviaciones.
    """
    cu = session.get('current_user')
    op = session.get('operario')
    if not cu:
        return redirect(url_for('login1'))
    if cu.get('rol') == ROL_OPERARIO and not op:
        return redirect(url_for('login2'))

    # 1. Cargar sesión o querystring
    nota     = session.get('nv_para_guia') or request.args.get('nv', '').strip()
    guia     = session.get('guia_para_guia') or request.args.get('guia', '').strip()
    scaneado = session.get('items_para_guia', [])  # ← solo los escaneados

    # 2. Cargar archivo NV para traer nombre, descripción, precio
    datos_nv = {}
    lineas: list[dict] = []
    if nota and os.path.exists(NV_FILE):
        try:
            df = pd.read_csv(NV_FILE, header=0, dtype=str, keep_default_na=False)
            df.columns = [c.strip() for c in df.columns]

            # Normalizar nombres de columnas al formato usado en la guía
            def _norm(txt: str) -> str:
                s = unicodedata.normalize("NFKD", str(txt))
                s = s.encode("ascii", "ignore").decode().lower()
                return re.sub(r"[^a-z0-9]", "", s)

            wanted = {
                'ciudad': 'Ciudad',
                'fecha': 'Fecha',
                'numnota': 'Num. Nota',
                'rut': 'RUT',
                'razonsocial': 'Razón Social',
                'canal': 'Canal',
                'fechaentrega': 'Fecha Entrega',
                'formadepago': 'Forma de Pago',
                'numordcompra': 'Num. Ord .Compra',
                'codigo': 'Código',
                'codigoproducto': 'Código',
                'descriptor': 'Descriptor',
                'descripcion': 'Descriptor',
                'cantidad': 'Cantidad',
                'cant': 'Cantidad',
                'preciounitario': 'Precio Unitario',
                'precio': 'Precio Unitario',
            }

            renames = {}
            norm_cols = {_norm(c): c for c in df.columns}
            for key, canonical in wanted.items():
                if key in norm_cols:
                    renames[norm_cols[key]] = canonical
            if renames:
                df.rename(columns=renames, inplace=True)

            df = df.loc[:, ~df.columns.str.match(r'^Unnamed', case=False)]

            if 'Num. Nota' in df.columns:
                df = df[df['Num. Nota'].astype(str).str.strip() == str(nota)]
            else:
                df = pd.DataFrame()

            df['Cantidad'] = pd.to_numeric(df.get('Cantidad', '0'), errors='coerce').fillna(0).astype(int)
            df['Precio Unitario'] = pd.to_numeric(df.get('Precio Unitario', '0'), errors='coerce').fillna(0).astype(int)

            # 2a. Datos generales de cabecera (cliente, dirección, etc.)
            datos_nv = df.iloc[0].to_dict() if not df.empty else {}

            # 2b. Armar líneas para la guía
            if scaneado:
                # Usar sólo los productos escaneados
                map_nv = {row.get('Código'): row for _, row in df.iterrows()}
                for s in scaneado:
                    codigo = s['codigo']
                    cantidad = s['cantidad']
                    info = map_nv.get(codigo, {})
                    linea = {
                        'codigo': codigo,
                        'descripcion': info.get('Descriptor', ''),
                        'cantidad': cantidad,
                        'precio': info.get('Precio Unitario', ''),
                        'descuento': '0%'  # puedes ajustar si hay descuento
                    }
                    lineas.append(linea)
           

        except Exception as e:
            flash(f'Error leyendo NV para la guía: {e}', 'error')

    # 3. Si POST → guardar
    if request.method == 'POST':
        datos = request.form.to_dict()
        session['guia_datos'] = datos
        session['guia_lineas'] = lineas

        if request.form.get('action') == 'guardar':
            df = pd.DataFrame(lineas)
            gd = datos.get('gr_numero', 'GD')
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            fname = f"guia_{gd}_{ts}.xlsx"
            fpath = os.path.join(EXPORT_DIR, secure_filename(fname))
            df.to_excel(fpath, index=False)
            session['guia_file'] = fpath
            flash(flash_msg, "success")

        elif request.form.get('action') == 'export':
            return redirect(url_for('descargar_xls'))

    # 4. Render
    descarga_url = None
    guia_file = session.get('guia_file')
    if guia_file and os.path.exists(guia_file):
        descarga_url = url_for('descargar_xls')

    return render_template(
        template_name,
        datos=datos_nv,
        lineas=lineas,
        descarga_url=descarga_url,
        datetime=datetime
    )

@app.route('/guia-despacho')
def guia_despacho():
    """
    Renderiza la Guía de Despacho usando SOLO los ítems escaneados
    y preparados durante la salida (session['items_para_guia']).
    - Toma num_nota desde ?num_nota=... o, si no viene, desde session['nv_para_guia'].
    - Trae del flow el flag RETIRA_CLIENTE para preconfigurar la guía.
    """
    cu = session.get('current_user')
    op = session.get('operario')
    if not cu:
        return redirect(url_for('login1'))
    if cu.get('rol') == ROL_OPERARIO and not op:
        return redirect(url_for('login2'))

    # num_nota puede venir por querystring o haber quedado en sesión al preparar la guía
    num_nota = (request.args.get('num_nota') or session.get('nv_para_guia') or '').strip()
    if not num_nota:
        flash('Falta el parámetro num_nota.', 'warning')
        return redirect(url_for('salida'))

    # Debe existir un "paquete" listo desde la salida
    items_scan = session.get('items_para_guia', []) or []
    nv_scan    = (session.get('nv_para_guia') or '').strip()

    # Validaciones de consistencia
    if not items_scan:
        flash('No hay productos preparados para la guía (no se han escaneado ítems).', 'warning')
        return redirect(url_for('salida'))
    if nv_scan and nv_scan != num_nota:
        flash(f'La NV preparada ({nv_scan}) no coincide con la solicitada ({num_nota}).', 'warning')
        return redirect(url_for('salida'))

    # Trae solo la CABECERA/metadata desde la NV (para datos del cliente, vendedor, etc.)
    # El detalle lo armamos desde items_scan para garantizar "solo escaneado".
    try:
        header, detalles_full = db_utils.get_guia_desde_nv(num_nota)
        if not header:
            flash(f'No se encontró información para la Nota de Venta {num_nota}.', 'warning')
            return redirect(url_for('salida'))
    except Exception as e:
        flash(f'Error al consultar la BBDD: {e}', 'danger')
        return redirect(url_for('salida'))

    # 👉 NUEVO: trae el flow para saber si es "retira cliente"
    try:
        flow = get_nv_flow(int(num_nota)) or {}
    except Exception:
        flow = {}
    retira_cliente = bool(flow.get("RETIRA_CLIENTE"))

    # Indexa detalle original por código para recuperar nombre/precio si está disponible
    def _norm_code(x):  # mismo criterio que usas en salida
        return str(x).strip().strip('*').upper()

    detalles_map = {}
    for d in (detalles_full or []):
        # aceptar variantes de nombres de columnas
        cod = d.get('codigo') or d.get('Código') or d.get('NCODART') or d.get('cod') or ''
        detalles_map[_norm_code(cod)] = d

    # Construye el detalle FINAL solo con lo escaneado
    detalles = []
    for it in items_scan:
        cod = _norm_code(it.get('codigo', ''))
        qty = int(it.get('cantidad', 0))
        if qty <= 0:
            continue

        base = detalles_map.get(cod, {})
        nombre = base.get('nombre') or base.get('Nombre') or base.get('DESCRIP') or ''
        prec   = base.get('prec_unit') or base.get('Prec.Unit') or base.get('PRECUNIT') or 0

        detalles.append({
            'codigo': cod,
            'nombre': nombre,
            'cantidad': qty,         # ya validado contra pendiente en /salida
            'prec_unit': prec
        })

    if not detalles:
        flash('No hay líneas válidas para la Guía (cantidades <= 0).', 'warning')
        return redirect(url_for('salida'))

    # Render: solo escaneado + flag de "retira cliente" para bloquear la opción en el template
    return render_template(
        'guia_despacho.html',
        header=header,
        detalles=detalles,
        num_nota=num_nota,
        datos={},
        datetime=datetime,
        retira_cliente=retira_cliente  # <-- úsalo en el template para marcar/inhabilitar
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
