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

# --- Configuración de logging ---
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = 'CAMBIAR_POR_CLAVE_SECRETA'  # necesario para session

# --- Directorios y rutas de archivos ---
BASE_DIR     = os.path.dirname(__file__)
DATA_DIR     = os.path.join(BASE_DIR, 'data')
os.makedirs(DATA_DIR, exist_ok=True)

GUIDE_FOLDER = os.path.join(DATA_DIR, 'guides')
os.makedirs(GUIDE_FOLDER, exist_ok=True)

UPLOADS_DIR  = os.path.join(DATA_DIR, 'uploads')
os.makedirs(UPLOADS_DIR, exist_ok=True)

STOCK_FILE   = os.path.join(DATA_DIR, 'stock.csv')
OC_FILE      = os.path.join(DATA_DIR, 'oc_pendientes.csv')
NV_FILE      = os.path.join(DATA_DIR, 'nv.csv')      # Notas de venta
MASTER_FILE  = os.path.join(DATA_DIR, 'productos_maestra.csv')

ALLOWED_EXT  = {'csv', 'xls', 'xlsx'}
FIELDNAMES   = ['codigo_producto', 'cantidad', 'ultima_actualizacion']

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

# --- Rutas ---
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/devoluciones')
def devoluciones():
    return render_template('devoluciones.html')


@app.route('/devoluciones/ingreso')
def devolucion_ingreso():
    return redirect(url_for('ingreso'))


from flask import render_template, request, redirect, url_for, flash, session

@app.route('/devolucion_salida', methods=['GET', 'POST'])
def devolucion_salida():
    # Si guardas datos en sesión, recupéralos aquí:
    devoluciones = session.get('devolucion_items', [])

    if request.method == 'POST':
        # 1) Extrae los campos del formulario/escáner
        guia   = request.form.get('guia')    # o como lo llames
        codigo = request.form.get('codigo')
        cantidad = request.form.get('cantidad')
        timestamp = datetime.now().isoformat()

        # 2) Guarda tu lógica (puede ser CSV, BD, session…)
        append_guide_entry(guia, codigo, cantidad, timestamp)

        # 3) Si usas sesión para mostrar, actualiza session['devolucion_items']…

        flash('Devolución registrada con éxito.', 'success')
        return redirect(url_for('devolucion_salida'))

    # GET: renderiza el formulario + lista de devoluciones
    return render_template('devolucion_salida.html',
                           devoluciones=devoluciones)


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
    per_page    = 100
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



@app.route('/ingreso', methods=['GET', 'POST'])
def ingreso():
    oc = session.get('current_oc')
    guia_actual = session.get('current_guia', '')
    oc_items = session.get('oc_items', [])
    scanned_items = session.get('scanned', [])

    # ───────────── GET con ?oc=XXXX ─────────────
    if request.method == 'GET' and request.args.get('oc'):
        oc = request.args.get('oc').strip()
        session['current_oc'] = oc
        session.pop('current_guia', None)
        session.pop('scanned', None)
        session.pop('oc_items', None)

        if not os.path.exists(OC_FILE):
            flash('Primero importa Órdenes de Compra.', 'warning')
        else:
            try:
                df = pd.read_csv(OC_FILE, dtype=str)
                oc_items = df[df['No. OC'] == oc].to_dict('records')
                session['oc_items'] = oc_items
                if not oc_items:
                    flash(f'La Orden de Compra {oc} no fue encontrada.', 'error')
                else:
                    flash(f'La Orden de Compra {oc} encontrada con {len(oc_items)} líneas.', 'success')
            except Exception as e:
                logger.error(f'Error procesando OC desde parámetro: {e}')
                flash(f'Error al procesar OC desde la URL: {e}', 'error')

    # ───────────── POST desde formulario ─────────────
    if request.method == 'POST':
        action = request.form.get('action')

        if action == 'buscar_oc':
            oc = request.form['oc'].strip()
            session['current_oc'] = oc
            session.pop('current_guia', None)
            session.pop('scanned', None)
            session.pop('oc_items', None)

            if not oc:
                flash('El No. OC es obligatorio.', 'warning')
            elif not os.path.exists(OC_FILE):
                flash('Primero importa Órdenes de Compra.', 'warning')
            else:
                try:
                    df = pd.read_csv(OC_FILE, dtype=str)
                    oc_items = df[df['No. OC'] == oc].to_dict('records')
                    session['oc_items'] = oc_items
                    if not oc_items:
                        flash(f'La OC {oc} no fue encontrada.', 'error')
                    else:
                        flash(f'OC {oc} encontrada con {len(oc_items)} líneas.', 'success')
                except Exception as e:
                    logger.error(f'Error procesando OC: {e}')
                    flash(f'Error al procesar Órdenes de Compra: {e}', 'error')
            return redirect(url_for('ingreso'))

        elif action == 'scan':
            guia = request.form.get('guia', '').strip() or guia_actual
            codigo = request.form.get('codigo', '').strip()
            try:
                cantidad = int(request.form.get('cantidad', 1))
            except:
                cantidad = 1
            ahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            if guia and guia != guia_actual:
                session['current_guia'] = guia
                guia_actual = guia

            if not oc:
                flash('Primero debes buscar una OC.', 'warning')
                return redirect(url_for('ingreso'))

        elif action == 'finish':
            if not scanned_items:
                flash('No hay ítems para guardar.', 'warning')
                return redirect(url_for('ingreso'))

            df_rep = pd.DataFrame(scanned_items)
            proveedor = rut = ""
            if oc_items:
                proveedor = oc_items[0].get("NombreProveedor") or oc_items[0].get("Razón Social") or ""
                rut = oc_items[0].get("RUT") or oc_items[0].get("RUT Proveedor") or oc_items[0].get("RutProveedor") or ""
                df_rep["Razón Social"] = proveedor
                df_rep["RUT"] = rut

            nombre_informe = f"informe_{oc}_{guia_actual}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            ruta_informe = os.path.join(DATA_DIR, nombre_informe)
            df_rep.to_excel(ruta_informe, index=False)

            df_oc = pd.DataFrame(oc_items)
            df_oc['Cantidad'] = pd.to_numeric(df_oc['Cantidad'], errors='coerce').fillna(0).astype(int)
            df_scan = pd.DataFrame(scanned_items)
            grouped = df_scan.groupby('codigo_producto')['cantidad'].sum().reset_index()

            merged = df_oc.merge(grouped, left_on='Código', right_on='codigo_producto', how='left').fillna(0)
            merged['cantidad'] = merged['cantidad'].astype(int)
            merged['faltan'] = merged['Cantidad'] - merged['cantidad']
            diff = merged[merged['faltan'] > 0][['Código', 'Nombre', 'Cantidad', 'cantidad', 'faltan']]

            diff["Razón Social"] = proveedor
            diff["RUT"] = rut
            cols = ["Razón Social", "RUT"] + [c for c in diff.columns if c not in ("Razón Social", "RUT")]
            diff = diff[cols]

            nombre_dif = f"diferencias_{oc}_{guia_actual}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            ruta_dif = os.path.join(DATA_DIR, nombre_dif)
            diff.to_excel(ruta_dif, index=False)

            session['informe_path'] = ruta_informe
            session['diferencias_path'] = ruta_dif

            for k in ('scanned', 'current_oc', 'current_guia', 'oc_items'):
                session.pop(k, None)

            flash('Recepción finalizada correctamente.', 'success')
            return redirect(url_for('finalizar'))

        if any(item.get('Código', '') == codigo for item in oc_items):
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
            session['scanned'] = scanned_items
            flash(f'{cantidad} unidad(es) de {codigo} ' +
                  ('sumadas' if found else 'registradas') + '.', 'success')
        else:
            flash(f'El código {codigo} no pertenece a la órden {oc}.', 'warning')
        return redirect(url_for('ingreso'))

    if not oc or not oc_items:
        return render_template('ingreso.html', oc='', oc_items=[], scanned_items=[], guia='')

    scanned_map = {}
    for s in scanned_items:
        codigo = s.get('codigo_producto')
        if codigo:
            scanned_map[codigo] = scanned_map.get(codigo, 0) + s.get('cantidad', 0)

    oc_display = []
    for item in oc_items:
        try:
            qty_ord = int(item.get('Cantidad', '0'))
        except ValueError:
            qty_ord = 0
        scanned_qty = scanned_map.get(item.get('Código', ''), 0)
        faltan = qty_ord - scanned_qty
        item2 = item.copy()
        item2['Faltan'] = max(faltan, 0)
        oc_display.append(item2)

    return render_template(
        'ingreso.html',
        oc=oc,
        oc_items=oc_display,
        scanned_items=scanned_items,
        guia=guia_actual
    )




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
    # 1) Recuperar sesión
    nota         = session.get('current_nv', '')
    guia_actual  = session.get('current_guia', '')
    nv_items     = session.get('nv_items', [])
    salida_items = session.get('salida_items', [])

    # 2) POST: buscar nota, escanear o terminar salida
    if request.method == 'POST':
        session['guia_datos'] = request.form.to_dict()
        action = request.form.get('action', 'buscar_nv')

        if action == 'buscar_nv':
            nota = (request.form.get('nv') or '').strip()
            session['current_nv'] = nota
            session.pop('nv_items', None)
            session.pop('salida_items', None)

            if not nota:
                flash('Debes ingresar un número de Nota de Venta.', 'warning')
                return redirect(url_for('salida'))
            if not os.path.exists(NV_FILE):
                flash('No se ha importado ninguna Nota de Venta.', 'warning')
                return redirect(url_for('salida'))

            try:
                df_nv = pd.read_csv(NV_FILE, header=0, dtype=str, keep_default_na=False)
                df_nv.columns = [c.strip() for c in df_nv.columns]
                df_nv = df_nv.loc[:, ~df_nv.columns.str.match(r'^Unnamed', case=False)]

                if 'Num. Nota' not in df_nv.columns:
                    flash("La columna 'Num. Nota' no está en el archivo de NV.", 'error')
                    return redirect(url_for('salida'))

                df_nv = df_nv[df_nv['Num. Nota'] == nota]
                if df_nv.empty:
                    flash(f'No se encontró la Nota de Venta {nota}.', 'error')
                    return redirect(url_for('salida'))

                df_nv['Cantidad'] = pd.to_numeric(df_nv.get('Cantidad','0'), errors='coerce').fillna(0).astype(int)
                df_nv['Precio Unitario'] = pd.to_numeric(df_nv.get('Precio Unitario','0'), errors='coerce').fillna(0).astype(int)

                df_show = df_nv[['Código','Descriptor','Cantidad','Precio Unitario']].copy()
                df_show.columns = ['Código','Nombre','Cant.','Prec.Unit.']
                df_show['Faltan'] = df_show['Cant.']

                nv_items = df_show.to_dict(orient='records')
                session['nv_items'] = nv_items
                session['salida_items'] = []
                flash(f'Nota {nota} cargada con {len(nv_items)} líneas.', 'success')
            except Exception as e:
                app.logger.error(f"Error al leer NV en salida: {e}")
                flash(f"Error al leer Nota de Venta: {e}", 'error')
                return redirect(url_for('salida'))

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
                session['current_guia'] = guia

            found = False
            for s in salida_items:
                if s['guia'] == guia and s['codigo'] == codigo:
                    s['cantidad'] += cantidad
                    s['hora'] = ahora
                    found = True
                    break
            if not found:
                salida_items.append({
                    'guia': guia, 'codigo': codigo,
                    'cantidad': cantidad, 'hora': ahora
                })
            session['salida_items'] = salida_items
            flash(f'{cantidad} unidad(es) de {codigo} ' + ('sumadas' if found else 'registradas') + '.', 'success')
            return redirect(url_for('salida'))

        elif action == 'terminar_salida':
            # ─── Guardar datos escaneados para usar en Guía ────────
            session['items_para_guia'] = salida_items
            session['nv_para_guia']    = nota
            session['guia_para_guia']  = guia_actual
            flash("Productos escaneados preparados para la Guía de Despacho.", "info")
            return redirect(url_for('finalizar_salida'))

    # 3) Cargar Stock
    stock_map = {}
    if os.path.exists(STOCK_FILE):
        try:
            df_st = pd.read_csv(STOCK_FILE, header=0, dtype=str, keep_default_na=False, encoding='utf-8-sig', sep=',')
            df_st.columns = [c.strip().replace("\ufeff", "") for c in df_st.columns]
            df_st = df_st.rename(columns={
                'CÃ³digo':          'Código',
                'Codigo':           'Código',
                'codigo_producto':  'Código',
                'Cantidad':         'Cantidad',
                'cantidad':         'Cantidad',
                'Nombre':           'Nombre',
                'nombre':           'Nombre'
            })

            if all(col in df_st.columns for col in ('Código','Nombre','Cantidad')):
                df_st['Cantidad'] = pd.to_numeric(df_st['Cantidad'], errors='coerce').fillna(0).astype(int)
                for _, row in df_st.iterrows():
                    key = str(row['Código']).strip()
                    stock_map[key] = {
                        'Nombre':   row['Nombre'].strip(),
                        'Cantidad': row['Cantidad']
                    }
        except Exception as e:
            app.logger.error(f"Error al leer Stock: {e}")
            flash(f"Error al leer Stock: {e}", 'error')
    else:
        flash(f"DEBUG: no existe STOCK_FILE en '{STOCK_FILE}'", 'warning')

    # 4) Construir stock_items restando escaneos:
    stock_items = []
    scanned_totals = {}

    if nv_items:
        for s in salida_items:
            k = str(s['codigo']).strip()
            scanned_totals[k] = scanned_totals.get(k, 0) + s['cantidad']

        for item in nv_items:
            code = str(item['Código']).strip()
            total = int(item['Cant.'])
            esc = scanned_totals.get(code, 0)
            falta = max(total - esc, 0)
            item['scanned'] = esc
            item['Faltan'] = falta

        for line in nv_items:
            code = str(line['Código']).strip()
            orig = stock_map.get(code, {}).get('Cantidad', 0)
            remain = max(orig - line['scanned'], 0)
            stock_items.append({
                'Código':  code,
                'Nombre':  stock_map.get(code, {}).get('Nombre', line['Nombre']),
                'Cantidad': remain
            })

    # 5) Renderizar
    return render_template(
        'salida.html',
        nota=nota,
        guia=guia_actual,
        nv_items=nv_items,
        salida_items=salida_items,
        stock_items=stock_items
    )





from pandas.errors import EmptyDataError

@app.route('/inventario', methods=['GET', 'POST'])
def inventario():
    # Variables en sesión
    expected_items = session.get('expected_items', [])
    scanned_items  = session.get('scanned_inv', [])

    if request.method == 'POST':
        action = request.form.get('action')

        # ── 1) Cargar Inventario (esperado) ──────────────────────────────
        if action == 'cargar_inv':
            # Leemos stock con header automático
            df = pd.read_csv(
                STOCK_FILE,
                header=0,
                dtype=str,
                keep_default_na=False
            )
            # Normalizamos nombres y eliminamos Unnamed
            df.columns = [c.strip() for c in df.columns]
            df = df.loc[:, ~df.columns.str.match(r'^Unnamed', case=False)]
            # Convertimos Cantidad a int
            if 'Cantidad' in df.columns:
                df['Cantidad'] = pd.to_numeric(df['Cantidad'],
                                               errors='coerce').fillna(0).astype(int)
            # Preparamos lista de dicts
            expected_items = df[['Código','Nombre','Cantidad']].to_dict(orient='records')
            session['expected_items'] = expected_items
            session['scanned_inv']    = []
            flash(f'Se cargaron {len(expected_items)} ítems de inventario.', 'success')

        # ── 2) Registrar Conteo ───────────────────────────────────────────
        elif action == 'scan_inv':
            codigo   = (request.form.get('codigo') or '').strip()
            try:
                contado = int(request.form.get('contado', 0))
            except ValueError:
                contado = 0

            ahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            # Busco en expected
            if any(item['Código']==codigo for item in expected_items):
                # Acumulo en scanned_items
                found = False
                for s in scanned_items:
                    if s['Código']==codigo:
                        s['Contado'] = contado
                        s['Hora']    = ahora
                        found = True
                        break
                if not found:
                    scanned_items.append({
                        'Código': codigo,
                        'Contado': contado,
                        'Hora': ahora
                    })
                session['scanned_inv'] = scanned_items
                flash(f'Conteo para {codigo} registrado: {contado}', 'success')
            else:
                flash(f'Código {codigo} no está en el inventario esperado.', 'warning')

        return redirect(url_for('inventario'))

    # ── Render ────────────────────────────────────────────────────────────
    # Junto a la lista esperada, computamos diferencias:
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
                           results=results)


# ─── Ruta /importar ─────────────────────────────────────────────────────────
from flask import (
    Flask, request, redirect, url_for,
    flash, render_template, session
)
import os
import pandas as pd
from datetime import datetime
from werkzeug.utils import secure_filename

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



@app.route('/finalizar.html')
def finalizar():
        return render_template('finalizar.html')
from flask import session, render_template, redirect, url_for

@app.route('/finalizar_salida')
def finalizar_salida():
    # (Opcional) Limpiar sesión de salida para comenzar de cero si es necesario
    session.pop('nv_items', None)
    session.pop('salida_items', None)
    session.pop('current_nv', None)
    session.pop('current_guia', None)
    # Renderiza una plantilla nueva (puedes clonar tu 'finalizar.html')
    return render_template('finalizar_salida.html')


EXPORT_DIR = os.path.join(os.getcwd(), 'exports')
os.makedirs(EXPORT_DIR, exist_ok=True)

# Alias para que exista un endpoint 'guia_despacho' que invoque la misma lógica que 'salida'
@app.route('/guia_despacho', methods=['GET', 'POST'])
def guia_despacho():
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
            flash("Guía de despacho guardada correctamente.", "success")

        elif request.form.get('action') == 'export':
            return redirect(url_for('descargar_xls'))

    # 4. Render
    descarga_url = None
    guia_file = session.get('guia_file')
    if guia_file and os.path.exists(guia_file):
        descarga_url = url_for('descargar_xls')

    return render_template(
        'guia_despacho.html',
        datos=datos_nv,
        lineas=lineas,
        descarga_url=descarga_url,
        datetime=datetime
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

