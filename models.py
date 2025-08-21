from datetime import datetime
from flask_sqlalchemy import SQLAlchemy


db = SQLAlchemy()


class Zona(db.Model):
    __tablename__ = "zonas"
    id = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(120), unique=True, index=True, nullable=False)


class AsignacionNV(db.Model):
    __tablename__ = "asignaciones_nv"
    id = db.Column(db.Integer, primary_key=True)
    num_nota = db.Column(db.String(32), unique=True, index=True, nullable=False)
    zona_id = db.Column(db.Integer, db.ForeignKey("zonas.id"), nullable=False)
    estado = db.Column(db.String(20), default="pendiente", nullable=False)
    assigned_by = db.Column(db.String(120), nullable=False)
    assigned_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    zona = db.relationship("Zona", lazy="joined")