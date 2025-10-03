# services/folios.py
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError, OperationalError
from typing import Optional
from db import get_engine

def _reservar_en_rima() -> Optional[int]:
    """
    Intenta pedir el siguiente NUMFACT en RIMA usando el SP corporativo.
    Si no hay permisos u otro error, devuelve None para que el caller haga fallback.
    """
    eng = get_engine("rima")
    try:
        with eng.begin() as cx:
            # Patrón compatible con SQL Server: parámetro OUTPUT + SELECT
            row = cx.execute(
                text("DECLARE @nf INT; EXEC RIMA.dbo.sp_wms_next_numfact_gd @numfact=@nf OUTPUT; SELECT @nf AS nf;")
            ).first()
            if row and row[0] is not None:
                return int(row[0])
            return None
    except (ProgrammingError, OperationalError):
        # Sin permisos u otra falla → que el caller use fallback
        return None
    except Exception:
        # Cualquier otra excepción: también forzamos fallback
        return None

def _fallback_siguiente_en_santiago() -> int:
    """
    Calcula un folio siguiente en Santiago con bloqueo para evitar carreras:
    MAX(NUMFACT numérico, TIPODOC=2) + 1 con UPDLOCK+HOLDLOCK.
    """
    eng = get_engine("santiago")
    with eng.begin() as cx:
        row = cx.execute(text("""
            SET NOCOUNT ON;
            DECLARE @nf INT;
            SELECT @nf = ISNULL(
                MAX(CASE WHEN ISNUMERIC(NUMFACT)=1 THEN CONVERT(INT, NUMFACT) ELSE 0 END), 0
            ) + 1
            FROM dbo.DOCU_DB WITH (UPDLOCK, HOLDLOCK)
            WHERE TIPODOC = 2;
            SELECT @nf AS nf;
        """)).first()
        return int(row[0])

def _existe_en_santiago(numfact: int) -> bool:
    """
    Verifica en Santiago si el NUMFACT ya fue usado por una guía (TIPODOC=2).
    """
    eng = get_engine("santiago")
    with eng.begin() as cx:
        row = cx.execute(
            text("SELECT 1 FROM dbo.DOCU_DB WHERE TIPODOC=2 AND NUMFACT=:nf"),
            {"nf": numfact},
        ).first()
        return row is not None

def next_numfact_unico(max_retries: int = 5, prefer_sucursal: Optional[int] = None) -> int:
    """
    Devuelve un NUMFACT garantizado libre en Santiago.
    Estrategia:
      1) Intentar reservar en RIMA (SP). Si funciona, usar ese folio.
      2) Si no hay permisos o falla, fallback: calcular en Santiago (MAX+1 con bloqueo).
      3) Validar contra Santiago; si está ocupado, reintentar hasta max_retries.

    Nota: 'prefer_sucursal' reservado para futura lógica de folios por sucursal.
    """
    # Primer intento: RIMA
    nf = _reservar_en_rima()
    attempt = 0

    while attempt < max_retries:
        attempt += 1

        # Si no pudimos en RIMA, usar fallback local (o si el reservado ya está ocupado)
        if nf is None:
            nf = _fallback_siguiente_en_santiago()

        # Confirmar que no esté usado en Santiago
        if not _existe_en_santiago(nf):
            return nf

        # Si está ocupado, intentamos de nuevo:
        # - Preferimos volver a RIMA (por si ya quedó liberado un nuevo folio).
        # - Si vuelve a fallar, usamos de nuevo el fallback (trae MAX+1 actualizado).
        reservado = _reservar_en_rima()
        nf = reservado if reservado is not None else _fallback_siguiente_en_santiago()

    raise RuntimeError("No pude obtener NUMFACT libre en Santiago (intentos agotados)")
