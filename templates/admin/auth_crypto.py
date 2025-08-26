import hmac
import unicodedata


def _norm(s: str) -> str:
    s = (s or "").strip()
    return unicodedata.normalize("NFC", s)


def codificar_clave(clave: str) -> str:
    clave = _norm(clave)
    partes = ["123"]
    for i, c in enumerate(clave):
        valor = ord(c) * (i + 1) + (i * 3) + 3
        partes.append(str(valor))
    partes.append("321")
    return ".".join(partes)


def comparar_constante(a: str, b: str) -> bool:
    return hmac.compare_digest(str(a or ""), str(b or ""))
