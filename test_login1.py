# test_login1.py
from auth_service import login_usuario

def probar(usuario, clave):
    u = login_usuario(usuario, clave)
    if not u:
        print(f"❌ Falló login para {usuario} con clave '{clave}'")
    else:
        print(f"✅ Login OK: {u}")

if __name__ == "__main__":
    # Prueba con BB1
    probar("BB1", "123.70.228.339.496.260.360.420.432.321")  # reemplaza con clave real si difiere
    
    # Prueba con SPT
    probar("SPT", "123.86.228.345.456.585.714.728.432.531.321")  # idem
