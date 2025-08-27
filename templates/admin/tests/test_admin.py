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

<<<<<<< ours
=======

def test_admin_login_ok(client):
    resp_get = client.get("/admin/login")
    assert resp_get.status_code == 200
    resp_post = client.post(
        "/admin/login",
        data={"password": os.environ.get("ADMIN_KEY", "admin123")},
        follow_redirects=True,
    )
    assert resp_post.status_code == 200
    assert "MENÚ ADMIN".encode("utf-8") in resp_post.data
>>>>>>> theirs
