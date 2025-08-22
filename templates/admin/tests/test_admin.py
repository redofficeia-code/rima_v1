import os
import pytest
from app import app


@pytest.fixture
def client():
    app.config["TESTING"] = True
    app.secret_key = "test"
    with app.test_client() as c:
        yield c


def test_admin_forbidden_without_login(client):
    resp = client.get("/admin")
    assert resp.status_code == 403


def test_admin_with_key_param(client):
    resp = client.get("/admin?key=" + os.environ.get("ADMIN_KEY", "admin123"))
    assert resp.status_code == 200


def test_admin_login_ok(client):
    resp_get = client.get("/admin/login")
    assert resp_get.status_code == 200
    resp_post = client.post(
        "/admin/login",
        data={"password": os.environ.get("ADMIN_KEY", "admin123")},
        follow_redirects=True,
    )
    assert resp_post.status_code == 200
    assert b"Panel de Administraci" in resp_post.data
