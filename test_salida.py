# tests/test_salida.py
import os
import sys
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import app as app_module


# -------------------------
# 1) Tests existentes (HUB)
# -------------------------

def test_salida_filters_hub(monkeypatch):
    executed = {}

    def fake_query(sql, params):
        executed['sql'] = sql
        executed['params'] = params
        # Simula que devolvemos hubs
        return pd.DataFrame([{'ID': 1, 'NOMBRE': 'Hub1'}])

    monkeypatch.setattr(app_module.db, 'query_df', fake_query)
    app_module.app.config['TESTING'] = True
    client = app_module.app.test_client()

    resp = client.get('/salida?hub_id=1')
    assert resp.status_code == 200
    assert executed['params'] == {'hub_id': 1}
    assert 'WHERE ID = :hub_id' in executed['sql']


def test_salida_without_hub(monkeypatch):
    executed = {}

    def fake_query(sql, params):
        executed['sql'] = sql
        executed['params'] = params
        # Simula lista de hubs vacía (o consulta base)
        return pd.DataFrame()

    monkeypatch.setattr(app_module.db, 'query_df', fake_query)
    app_module.app.config['TESTING'] = True
    client = app_module.app.test_client()

    resp = client.get('/salida')
    assert resp.status_code == 200
    assert executed['params'] == {}
    assert 'WHERE 1=1' in executed['sql']


# --------------------------------
# 2) Nuevos tests (ZONAS / NV_ZONAS)
# --------------------------------

def test_salida_con_zona_ejecuta_query_nv(monkeypatch):
    """
    GET /salida?zona=LA SERENA debe consultar NOTV_DB JOIN NV_ZONAS
    con param nombrado :zona y renderizar tabla/listado.
    """
    executed = {}

    def fake_query(sql, params):
        executed['sql'] = sql
        executed['params'] = params
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

    resp = client.get('/salida?zona=LA%20SERENA')
    assert resp.status_code == 200
    # Validar parámetros y SQL
    assert executed['params'] == {'zona': 'LA SERENA'}
    upper_sql = executed['sql'].upper()
    assert 'FROM' in upper_sql and 'NOTV_DB' in upper_sql
    assert 'JOIN' in upper_sql and 'NV_ZONAS' in upper_sql
    assert 'Z.ZONA = :ZONA' in upper_sql or 'Z.ZONA = :zona'.upper() in upper_sql
    # Heurística de que se renderizó el listado (botón "Cargar NV" o tabla)
    assert (b'Cargar NV' in resp.data) or (b'<table' in resp.data)


def test_salida_con_zona_sin_resultados_muestra_mensaje(monkeypatch):
    """
    GET /salida?zona=LA SERENA cuando no hay NV asignadas
    debe mostrar el mensaje "No hay Notas de Venta asignadas".
    """
    executed = {}

    def fake_query(sql, params):
        executed['sql'] = sql
        executed['params'] = params
        # Retorna vacío para la consulta de zona
        if 'NV_ZONAS' in sql.upper():
            return pd.DataFrame()
        return pd.DataFrame()

    monkeypatch.setattr(app_module.db, 'query_df', fake_query)
    app_module.app.config['TESTING'] = True
    client = app_module.app.test_client()

    resp = client.get('/salida?zona=LA%20SERENA')
    assert resp.status_code == 200
    assert executed['params'] == {'zona': 'LA SERENA'}
    # Debe estar el mensaje en el HTML (o en flash)
    assert (b'No hay Notas de Venta asignadas' in resp.data) or (b'No hay Notas de Venta' in resp.data)
