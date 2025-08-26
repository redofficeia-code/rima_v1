import re

def looks_dotcode(s: str) -> bool:
    """Detecta formato tipo '123.70.228.339...321'"""
    return bool(re.fullmatch(r"\d+(?:\.\d+)+", (s or "").strip()))

def codificar_clave(clave: str) -> str:
    """
    Algoritmo legado de codificación de claves.
    Genera cadenas tipo: 123.x.x.x...321
    """
    resultado = ["123"]
    for i, c in enumerate(clave):
        valor = ord(c) * (i + 1) + (i * 3) + 3
        resultado.append(str(valor))
    resultado.append("321")
    return ".".join(resultado)
