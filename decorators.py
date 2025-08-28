from functools import wraps
from flask import session, abort, redirect, url_for
from auth_map import ROL_JEFE, ROL_ALIASES

def _norm(s: str) -> str:
    return (s or "").strip().casefold()

def _is_jefe(cu: dict) -> bool:
    """
    True si el usuario en sesión es Jefe de Bodega.
    Acepta:
      - igualdad normalizada contra ROL_JEFE
      - alias declarados en ROL_ALIASES
      - flag is_admin en sesión (por compatibilidad)
    """
    if not cu:
        return False
    rol_sesion = _norm(cu.get("rol"))
    rol_jefe   = _norm(ROL_JEFE)

    if rol_sesion == rol_jefe:
        return True

    # Aceptar aliases (ADMIN, BODEGA, etc.)
    try:
        for alias, canon in (ROL_ALIASES or {}).items():
            if _norm(alias) == rol_sesion and _norm(canon) == rol_jefe:
                return True
    except Exception:
        pass

    # Fallback: si marcaste is_admin al loguear
    from flask import session as _s
    if _s.get("is_admin") is True:
        return True

    return False


def admin_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        cu = session.get('current_user')
        if not cu:
            return redirect(url_for('login1'))
        if not _is_jefe(cu):
            abort(403)
        return f(*args, **kwargs)
    return wrapper
