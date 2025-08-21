import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import app as app_module


def test_salida_filters_hub(monkeypatch):
    executed = {}

    def fake_query(sql, params):
        executed['sql'] = sql
        executed['params'] = params
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
        return pd.DataFrame()

    monkeypatch.setattr(app_module.db, 'query_df', fake_query)
    app_module.app.config['TESTING'] = True
    client = app_module.app.test_client()
    resp = client.get('/salida')
    assert resp.status_code == 200
    assert executed['params'] == {}
    assert 'WHERE 1=1' in executed['sql']