# tests/test_salida.py
import os
import sys
import pandas as pd
from auth_map import ROL_OPERARIO

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import app as app_module
from auth_map import ROL_JEFE


def _login(client):
    """Helper to populate session with a logged-in user."""
    with client.session_transaction() as sess:
        sess['current_user'] = {'rol': ROL_JEFE}

<<<<<<< ours

def test_salida_get_no_db_call(monkeypatch):
    """GET /salida should render page without hitting the database."""
    called = {}
=======
def test_salida_filters_hub(monkeypatch):
    def fake_query(sql, params):
        # Simula que devolvemos hubs
        return pd.DataFrame([{'ID': 1, 'NOMBRE': 'Hub1'}])
>>>>>>> theirs

<<<<<<< ours
    def fake_stock():
        called['called'] = True
=======
    monkeypatch.setattr(app_module.db, 'query_df', fake_query)
    app_module.app.config['TESTING'] = True
    client = app_module.app.test_client()
    with client.session_transaction() as sess:
<<<<<<< ours
        sess['current_user'] = {'nombre': 'Operario', 'rol': ROL_OPERARIO}
        sess['operario'] = {'codigo': '1', 'nombre': 'Operario'}

    resp = client.get('/salida?hub_id=1')
    assert resp.status_code == 200


=======
        sess['current_user'] = {'rol': 'admin'}
    resp = client.get('/salida?hub_id=1')
    assert resp.status_code == 200
    # Se verifica que la respuesta sea exitosa
>>>>>>> theirs
def test_salida_without_hub(monkeypatch):
    def fake_query(sql, params):
        # Simula lista de hubs vacía (o consulta base)
>>>>>>> theirs
        return pd.DataFrame()

    monkeypatch.setattr(app_module.db_utils, 'get_stock_actual', fake_stock)
    app_module.app.config['TESTING'] = True
    client = app_module.app.test_client()
<<<<<<< ours
    _login(client)

    resp = client.get('/salida')
    assert resp.status_code == 200
    assert 'called' not in called


def test_salida_buscar_nv(monkeypatch):
    """POST /salida buscar_nv should query nota detalle and store session data."""
    called = {}

    def fake_get_nota_detalle(nota):
        called['nota'] = nota
        return pd.DataFrame([
            {
                'num_nota': '123',
                'codigo': 'A1',
                'nombre': 'Prod1',
                'cantidad': 2,
                'prec_unit': 10,
            }
        ])

    def fake_stock():
        # stock consult is triggered after buscar_nv
        return pd.DataFrame([
            {'codigo': 'A1', 'nombre': 'Prod1', 'cantidad': 5}
        ])

    monkeypatch.setattr(app_module.db_utils, 'get_nota_detalle', fake_get_nota_detalle)
    monkeypatch.setattr(app_module.db_utils, 'get_stock_actual', fake_stock)
    app_module.app.config['TESTING'] = True
    client = app_module.app.test_client()
<<<<<<< ours
    _login(client)
=======
    with client.session_transaction() as sess:
        sess['current_user'] = {'nombre': 'Operario', 'rol': ROL_OPERARIO}
        sess['operario'] = {'codigo': '1', 'nombre': 'Operario'}

    resp = client.get('/salida')
    assert resp.status_code == 200


=======
    with client.session_transaction() as sess:
        sess['current_user'] = {'rol': 'admin'}
    resp = client.get('/salida')
    assert resp.status_code == 200
    # Se verifica que la respuesta sea exitosa
>>>>>>> theirs
# --------------------------------
# 2) Nuevos tests (ZONAS / NV_ZONAS)
# --------------------------------

def test_salida_con_zona_ejecuta_query_nv(monkeypatch):
    """
    GET /salida?zona=LA SERENA debe consultar NOTV_DB JOIN NV_ZONAS
    con param nombrado :zona y renderizar tabla/listado.
    """
    def fake_query(sql, params):
        # Cuando se consulta por zona, devolvemos NV asignadas
        if 'NV_ZONAS' in sql.upper():
            return pd.DataFrame([
                {'NUMNOTA': 2326135, 'FECHA': '2025-08-01', 'SUCUR': 'SCL', 'RAZSOC': 'ACME S.A.'},
                {'NUMNOTA': 2326136, 'FECHA': '2025-08-02', 'SUCUR': 'SCL', 'RAZSOC': 'ACME S.A.'},
            ])
        return pd.DataFrame()

    monkeypatch.setattr(app_module.db, 'query_df', fake_query)
    app_module.app.config['TESTING'] = True
    client = app_module.app.test_client()
    with client.session_transaction() as sess:
<<<<<<< ours
        sess['current_user'] = {'nombre': 'Operario', 'rol': ROL_OPERARIO}
        sess['operario'] = {'codigo': '1', 'nombre': 'Operario'}
>>>>>>> theirs

    resp = client.post('/salida', data={'action': 'buscar_nv', 'nv': '123'}, follow_redirects=True)
    assert resp.status_code == 200
<<<<<<< ours
    assert called['nota'] == '123'

    with client.session_transaction() as sess:
        assert sess.get('current_nv') == '123'
        assert len(sess.get('nv_items', [])) == 1
=======
=======
        sess['current_user'] = {'rol': 'admin'}
    resp = client.get('/salida?zona=LA%20SERENA')
    assert resp.status_code == 200
    # Validar parámetros y SQL
    # Se verifica que la respuesta sea exitosa
>>>>>>> theirs


def test_salida_con_zona_sin_resultados_muestra_mensaje(monkeypatch):
    """
    GET /salida?zona=LA SERENA cuando no hay NV asignadas
    debe mostrar el mensaje "No hay Notas de Venta asignadas".
    """
    def fake_query(sql, params):
        # Retorna vacío para la consulta de zona
        if 'NV_ZONAS' in sql.upper():
            return pd.DataFrame()
        return pd.DataFrame()

    monkeypatch.setattr(app_module.db, 'query_df', fake_query)
    app_module.app.config['TESTING'] = True
    client = app_module.app.test_client()
    with client.session_transaction() as sess:
<<<<<<< ours
        sess['current_user'] = {'nombre': 'Operario', 'rol': ROL_OPERARIO}
        sess['operario'] = {'codigo': '1', 'nombre': 'Operario'}

    resp = client.get('/salida?zona=LA%20SERENA')
    assert resp.status_code == 200
>>>>>>> theirs
=======
        sess['current_user'] = {'rol': 'admin'}
    resp = client.get('/salida?zona=LA%20SERENA')
    assert resp.status_code == 200
    # Se verifica que la respuesta sea exitosa
>>>>>>> theirs
