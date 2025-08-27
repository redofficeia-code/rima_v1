from functools import wraps
from flask import session, abort
from auth_map import ROL_JEFE


def admin_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        cu = session.get('current_user')
        if not cu or cu.get('rol') != ROL_JEFE:
            abort(403)
        return f(*args, **kwargs)
    return wrapper