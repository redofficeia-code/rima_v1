import pytest
from app import app
from auth_map import ROL_JEFE, ROL_OPERARIO


@pytest.fixture
def client():
    app.config["TESTING"] = True
    app.secret_key = "test"
    with app.test_client() as c:
        yield c


def test_admin_requires_proper_role(client):
    # Sin sesión debe retornar 403
    resp = client.get("/admin")
    assert resp.status_code == 403

    # Con rol no administrador también 403
    with client.session_transaction() as sess:
        sess["current_user"] = {"nombre": "Operario", "rol": ROL_OPERARIO}
    resp = client.get("/admin")
    assert resp.status_code == 403

    # Con rol de jefe permite el acceso
    with client.session_transaction() as sess:
        sess["current_user"] = {"nombre": "Jefe", "rol": ROL_JEFE}
    resp = client.get("/admin")
    assert resp.status_code == 200

