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
try:
    import sqlalchemy  # noqa: F401
except ModuleNotFoundError as exc:
    raise ModuleNotFoundError(
        "Missing dependency 'sqlalchemy'. Install it with 'pip install SQLAlchemy'."
    ) from exc

try:
    import db
except ModuleNotFoundError as exc:
    raise ModuleNotFoundError(
        "Could not import required module 'db'. Ensure 'db.py' exists."
    ) from exc


def is_admin_or_cargar():
    return session.get('role') in ('admin', 'cargar')

# --- Configuración de logging ---
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = 'CAMBIAR_POR_CLAVE_SECRETA'  # necesario para session


@app.route('/_debug/role/<rol>')
def _debug_set_role(rol):
    session['role'] = rol
    flash(f'Rol seteado a: {rol}', 'info')
    return redirect(url_for('index'))

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


def _nv_rows_for_gestion():
    """
    Lee NV desde NV_FILE detectando la fila de cabecera y devuelve
    las columnas pedidas para gestión:
    Num. Nota, RUT, Ciudad, Razón Social, Fecha Entrega,
    Cantidad, Cant. Desp., Precio Unitario, Pendiente, Terminado
    """
    if not os.path.exists(NV_FILE) or os.path.getsize(NV_FILE) == 0:
        return []

    # 1) Detectar cabecera
    df_raw = pd.read_csv(NV_FILE, header=None, dtype=str, keep_default_na=False)

    expected = {'ciudad','fecha','numnota','rut','razonsocial','canal','fechaentrega','formadepago'}

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

    # 2) Leer con cabecera detectada
    df = pd.read_csv(NV_FILE, header=header_idx, dtype=str, keep_default_na=False)
    df.columns = [c.strip() for c in df.columns]

    # 3) Selector robusto de columna
    def pick(*names):
        # Primero por coincidencia exacta
        for n in names:
            if n in df.columns:
                return n
        # Luego por normalización
        def nz(s):
            return re.sub(r'[^a-z0-9]', '', str(s).lower())
        inv = {nz(c): c for c in df.columns}
        for n in names:
            key = nz(n)
            if key in inv:
                return inv[key]
        return None

    c_num = pick('Num. Nota', 'NUMNOTA', 'N° Nota', 'NumNota')
    c_rut = pick('RUT', 'Rut')
    c_ciud = pick('Ciudad', 'CIUDAD')
    c_raz = pick('Razón Social', 'Razon Social', 'RAZON SOCIAL', 'Cliente')
    c_fent = pick('Fecha Entrega', 'Entrega', 'Fecha', 'FechaEntrega')
    c_qty = pick('Cantidad', 'Cant.')
    c_dsp = pick('Cant. Desp.', 'Cant. Despachada', 'Cant desp', 'Cant Desp', 'CANTDESP')
    c_pu = pick('Precio Unitario', 'Prec. Unit.', 'Prec Unit', 'Prec.Unit.')

    out = pd.DataFrame()
    out['Num. Nota'] = df[c_num] if c_num else ''
    out['RUT'] = df[c_rut] if c_rut else ''
    out['Ciudad'] = df[c_ciud] if c_ciud else ''
    out['Razón Social'] = df[c_raz] if c_raz else ''
    out['Fecha Entrega'] = df[c_fent] if c_fent else df.get('Fecha', '')

    def to_num_series(s):
        return pd.to_numeric(
            s.astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False),
            errors='coerce'
        ).fillna(0)

    out['Cantidad'] = to_num_series(df[c_qty]) if c_qty else 0
    out['Cant. Desp.'] = to_num_series(df[c_dsp]) if c_dsp else 0
    out['Precio Unitario'] = to_num_series(df[c_pu]) if c_pu else 0

    out['Pendiente'] = (out['Cantidad'] - out['Cant. Desp.']).astype(int)
    out['Terminado'] = out['Pendiente'].apply(lambda x: 'SI' if x == 0 else 'NO')

    # Tipos básicos para mostrar bonito
    out['Cantidad'] = out['Cantidad'].astype(int)
    out['Cant. Desp.'] = out['Cant. Desp.'].astype(int)
    return out.to_dict(orient='records')

# --- Integración con la base de datos ---
def fetch_oc_items(num_oc):
    """Obtiene detalle de una OC usando db.get_oc_detalle.

    Retorna un DataFrame con columnas estandarizadas y el número de guía
    si está disponible.
    """
    rows = db.get_oc_detalle(num_oc)
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

# --- Rutas ---
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/devoluciones')
def devoluciones():
    return render_template('devoluciones.html')


@app.route('/devoluciones/ingreso', methods=['GET', 'POST'])
def devolucion_ingreso():
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
        df_st = db.get_stock_actual()
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





@app.route('/nv/gestionar')
def nv_gestionar():
    if not is_admin_or_cargar():
        flash('No tienes permisos para gestionar.', 'warning')
        return redirect(url_for('listado_nv'))

    rows = _nv_rows_for_gestion()
    hubs = db.query_df("SELECT ID, NOMBRE FROM WMS.HUBS ORDER BY NOMBRE", {})
    hubs_list = hubs.to_dict(orient='records') if not hubs.empty else []
    return render_template('nv_gestionar.html', rows=rows, hubs=hubs_list)


@app.route('/nv/gestionar/accion', methods=['POST'])
def nv_gestionar_accion():
    if not is_admin_or_cargar():
        flash('No tienes permisos para gestionar.', 'warning')
        return redirect(url_for('listado_nv'))

    numnota = request.form.get('numnota')
    accion = request.form.get('accion')
    hub_id = request.form.get('hub_id')

    if not numnota or not accion:
        flash('Faltan datos de la acción.', 'warning')
        return redirect(url_for('nv_gestionar'))

    if accion == 'aprobar':
        if not hub_id:
            flash('Selecciona un hub para aprobar y asignar.', 'warning')
            return redirect(url_for('nv_gestionar'))

        sql_upsert = """
        MERGE WMS.NV_REVIEW AS T
        USING (SELECT CAST(:numnota AS INT) AS NUMNOTA) AS S
          ON (T.NUMNOTA = S.NUMNOTA)
        WHEN MATCHED THEN
          UPDATE SET ESTADO='asignada', HUB_ID=:hub, FECHA_ASIGNACION=:f
        WHEN NOT MATCHED THEN
          INSERT (NUMNOTA, ESTADO, HUB_ID, FECHA_ASIGNACION)
          VALUES (:numnota, 'asignada', :hub, :f);
        """
        db.execute(sql_upsert, {"numnota": numnota, "hub": hub_id, "f": datetime.now()})
        flash(f'NV {numnota} aprobada y asignada.', 'success')

    elif accion == 'pendiente':
        sql_upsert = """
        MERGE WMS.NV_REVIEW AS T
        USING (SELECT CAST(:numnota AS INT) AS NUMNOTA) AS S
          ON (T.NUMNOTA = S.NUMNOTA)
        WHEN MATCHED THEN
          UPDATE SET ESTADO='pendiente', HUB_ID=NULL, FECHA_ASIGNACION=NULL
        WHEN NOT MATCHED THEN
          INSERT (NUMNOTA, ESTADO) VALUES (:numnota, 'pendiente');
        """
        db.execute(sql_upsert, {"numnota": numnota})
        flash(f'NV {numnota} marcada como pendiente.', 'info')

    else:
        flash('Acción no reconocida.', 'warning')

    return redirect(url_for('nv_gestionar'))


# --- Admin: gestionar zonas -------------------------------------------------

@app.route('/admin/zonas', methods=['GET'])
def zonas_admin():
    if not is_admin_or_cargar():
        flash('No tienes permisos para gestionar zonas.', 'warning')
        return redirect(url_for('index'))
    df = db.query_df("SELECT ID, NOMBRE FROM WMS.HUBS ORDER BY NOMBRE", {})
    rows = df.to_dict(orient='records') if not df.empty else []
    return render_template('zonas_admin.html', zonas=rows)


@app.route('/admin/zonas/add', methods=['POST'])
def zonas_admin_add():
    if not is_admin_or_cargar():
        flash('No tienes permisos para gestionar zonas.', 'warning')
        return redirect(url_for('index'))

    nombre = (request.form.get('nombre') or '').strip()
    if not nombre:
        flash('Debes ingresar un nombre de zona.', 'warning')
        return redirect(url_for('zonas_admin'))

    exist = db.query_df(
        "SELECT 1 AS X FROM WMS.HUBS WHERE UPPER(NOMBRE)=UPPER(:n)",
        {"n": nombre}
    )
    if not exist.empty:
        flash(f'La zona "{nombre}" ya existe.', 'info')
        return redirect(url_for('zonas_admin'))

    db.execute("INSERT INTO WMS.HUBS(NOMBRE) VALUES (:n)", {"n": nombre})
    flash(f'Zona "{nombre}" agregada correctamente.', 'success')
    return redirect(url_for('zonas_admin'))


@app.route('/admin/zonas/delete', methods=['POST'])
def zonas_admin_delete():
    if not is_admin_or_cargar():
        flash('No tienes permisos para gestionar zonas.', 'warning')
        return redirect(url_for('index'))

    hub_id = request.form.get('hub_id')
    if not hub_id:
        flash('Falta el ID de la zona a eliminar.', 'warning')
        return redirect(url_for('zonas_admin'))

    in_use = db.query_df(
        "SELECT TOP 1 1 AS X FROM WMS.NV_REVIEW WHERE HUB_ID = :id",
        {"id": hub_id}
    )
    if not in_use.empty:
        flash('No puedes eliminar esta zona porque tiene NV asignadas.', 'warning')
        return redirect(url_for('zonas_admin'))

    db.execute("DELETE FROM WMS.HUBS WHERE ID=:id", {"id": hub_id})
    flash('Zona eliminada.', 'success')
    return redirect(url_for('zonas_admin'))


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


@app.route('/factura_nv')
def factura_nv():
    """Prellena la Factura de Venta con datos de una Nota de Venta."""
    num_nota = (request.args.get('num_nota') or '').strip()
    header = {}
    if num_nota:
        try:
            header = db.get_factura_desde_nv(num_nota)
            if not header:
                flash(f'No se encontró información para la Nota de Venta {num_nota}.', 'warning')
        except Exception as e:
            flash(f'Error al consultar la BBDD: {e}', 'danger')
    return render_template('factura_nv.html', header=header, num_nota=num_nota, datetime=datetime)



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
    return ingreso_core('ingreso.html', 'ingreso', db_fetcher=fetch_oc_items)




@app.route('/ingreso/diferencias.xls')
def download_diferencias():
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
    # Estado
    nota         = session.get('current_nv', '')
    guia_actual  = session.get('current_guia', '')
    nv_items     = session.get('nv_items', [])        # detalle NV (desde BBDD)
    salida_items = session.get('salida_items', [])    # items para salida/escaneo

    hubs_df = db.query_df("SELECT ID, NOMBRE FROM WMS.HUBS WHERE 1=1 ORDER BY NOMBRE", {})
    hubs = hubs_df.to_dict(orient='records') if not hubs_df.empty else []

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
                flash(f'Error al consultar la BBDD: {e}', 'danger')

            return redirect(url_for('salida'))

        # 2) Escanear (agregar item a salida)
        elif action in ('escanear', 'scan'):
            codigo = (request.form.get('codigo') or '').strip()
            cant   = request.form.get('cantidad') or '1'
            try:
                cant = int(cant)
            except:
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
            base['Cant.']        = pd.to_numeric(base['Cant.'], errors='coerce').fillna(0).astype(int)
            sal['Cant.Salida']   = pd.to_numeric(sal['Cant.Salida'], errors='coerce').fillna(0).astype(int)

            # Validación: no permitir sobrepasar pendiente
            merged = sal.merge(base[['Código','Cant.']], on='Código', how='left')
            merged['Exceso'] = (merged['Cant.Salida'] - merged['Cant.']).clip(lower=0)
            if (merged['Exceso'] > 0).any():
                cods = merged.loc[merged['Exceso'] > 0, 'Código'].unique().tolist()
                flash(f'Cantidad de salida supera lo pendiente para: {", ".join(map(str, cods))}.', 'danger')
                return redirect(url_for('salida'))

            # Si todo OK: (aquí podrías insertar movimiento, generar GD, etc.)
            session.pop('salida_items', None)
            flash('Salida finalizada correctamente.', 'success')
            return redirect(url_for('salida'))

    # GET
    # Calcular cantidades escaneadas y faltantes para cada ítem de la NV
    scanned_map = {}
    for si in salida_items:
        try:
            code = str(si.get('Código'))
            qty = int(si.get('Cant.Salida', 0))
        except Exception:
            # Valores inesperados se tratan como 0
            code, qty = str(si.get('Código')), 0
        scanned_map[code] = scanned_map.get(code, 0) + qty

    display_nv_items = []
    for it in nv_items:
        orig = 0
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
            df_st = db.get_stock_actual()
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

    return render_template(
        'salida.html',
        nota=nota,
        guia_actual=guia_actual,
        nv_items=display_nv_items,
        salida_items=salida_items,
        stock_items=stock_items,
        hubs=hubs
    )




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
        df.columns = [
            str(c).strip().replace("\ufeff", "")
            for c in df.columns
        ]
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
            else:
                # Si no se escaneó nada, incluir todas las líneas de la NV
                for _, row in df.iterrows():
                    lineas.append({
                        'codigo': row.get('Código', ''),
                        'descripcion': row.get('Descriptor', ''),
                        'cantidad': row.get('Cantidad', 0),
                        'precio': row.get('Precio Unitario', ''),
                        'descuento': '0%'
                    })

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
    Prellena la Guía de Despacho con datos de la Nota de Venta (num_nota).
    Parámetro: ?num_nota=xxxxx
    """
    num_nota = (request.args.get('num_nota') or '').strip()
    if not num_nota:
        flash('Falta el parámetro num_nota.', 'warning')
        return redirect(url_for('salida'))

    try:
        header, detalles = db.get_guia_desde_nv(num_nota)
        if not header:
            flash(f'No se encontró información para la Nota de Venta {num_nota}.', 'warning')
            return redirect(url_for('salida'))
    except Exception as e:
        flash(f'Error al consultar la BBDD: {e}', 'danger')
        return redirect(url_for('salida'))

    return render_template('guia_despacho.html', header=header, detalles=detalles, num_nota=num_nota, datos={}, datetime=datetime)


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

