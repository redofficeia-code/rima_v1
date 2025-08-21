from typing import List
from models import db, Zona, AsignacionNV


def upsert_asignacion(num_nota: str, zona_id: int, assigned_by: str) -> AsignacionNV:
    num_nota = (num_nota or "").strip()
    a = AsignacionNV.query.filter_by(num_nota=num_nota).first()
    if a:
        a.zona_id = zona_id
        a.assigned_by = assigned_by
    else:
        a = AsignacionNV(num_nota=num_nota, zona_id=zona_id, assigned_by=assigned_by)
        db.session.add(a)
    db.session.commit()
    return a


def nv_asignadas_por_zona(zona_id: int, estados: List[str] = None) -> list[str]:
    q = AsignacionNV.query.filter_by(zona_id=zona_id)
    if estados:
        q = q.filter(AsignacionNV.estado.in_(estados))
    return [a.num_nota for a in q.order_by(AsignacionNV.assigned_at.desc()).all()]


def marcar_asignacion_completada(num_nota: str):
    a = AsignacionNV.query.filter_by(num_nota=num_nota).first()
    if a and a.estado != "completada":
        a.estado = "completada"
        db.session.commit()