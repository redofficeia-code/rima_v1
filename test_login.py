import os
import sys
import pytest

sys.path.append(os.path.dirname(__file__))
import app as app_module


def test_login_jefe_redirects_admin(monkeypatch):
    def fake_login(usuario, clave):
        return {'nombre': 'Bodega', 'rol': app_module.ROL_JEFE, 'is_admin': True}
    monkeypatch.setattr(app_module, 'login_nivel1', fake_login)
    app_module.app.config['TESTING'] = True
    client = app_module.app.test_client()

    resp = client.post('/login1', data={'usuario': 'BODEGA', 'clave': 'x'}, follow_redirects=True)
    assert resp.status_code == 200
    assert b'Panel de Administraci' in resp.data