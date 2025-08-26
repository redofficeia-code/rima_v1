# legacy_passwords.py
import re

def looks_dotcode(s: str) -> bool:
    """Detecta formato tipo '123.xxx.xxx...321'"""
    return bool(re.fullmatch(r"\d+(?:\.\d+)+", (s or "").strip()))

def legacy_preprocess(pwd: str) -> str:
    """
    Pre-normalización que usa el legado antes de codificar:
    - capitalize(): primera letra mayúscula, resto minúsculas.
    Ajusta aquí si encuentras otra regla en el sistema viejo.
    """
    return (pwd or "").strip().capitalize()

def codificar_clave(clave: str) -> str:
    """
    Algoritmo legado de codificación de claves:
    123 . [ord(c)*(i+1) + (i*3) + 3]... . 321
    """
    resultado = ["123"]
    for i, c in enumerate(clave):
        valor = ord(c) * (i + 1) + (i * 3) + 3
        resultado.append(str(valor))
    resultado.append("321")
    return ".".join(resultado)
