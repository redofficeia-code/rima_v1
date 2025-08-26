# tests/test_salida.py
import os
import sys
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import app as app_module
from auth_map import ROL_JEFE


def _login(client):
    """Helper to populate session with a logged-in user."""
    with client.session_transaction() as sess:
        sess['current_user'] = {'rol': ROL_JEFE}


def test_salida_get_no_db_call(monkeypatch):
    """GET /salida should render page without hitting the database."""
    called = {}

    def fake_stock():
        called['called'] = True
        return pd.DataFrame()

    monkeypatch.setattr(app_module.db_utils, 'get_stock_actual', fake_stock)
    app_module.app.config['TESTING'] = True
    client = app_module.app.test_client()
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
    _login(client)

    resp = client.post('/salida', data={'action': 'buscar_nv', 'nv': '123'}, follow_redirects=True)
    assert resp.status_code == 200
    assert called['nota'] == '123'

    with client.session_transaction() as sess:
        assert sess.get('current_nv') == '123'
        assert len(sess.get('nv_items', [])) == 1
