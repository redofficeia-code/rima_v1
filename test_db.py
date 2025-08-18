import os
import urllib.parse
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DRIVER = os.getenv("DB_DRIVER")
SERVER = os.getenv("DB_SERVER")
DATABASE = os.getenv("DB_DATABASE")
AUTH = os.getenv("DB_AUTH", "sql").lower()

def _env_bool(value: str) -> bool:
    value = value.strip().lower()
    if value in {"yes", "true", "1"}:
        return True
    if value in {"no", "false", "0"}:
        return False
    return True

trust_cert = _env_bool(os.getenv("DB_TRUST_CERT", "yes"))

if AUTH == "windows":
    odbc = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"Trusted_Connection=yes;"
        f"Encrypt=yes;TrustServerCertificate={'yes' if trust_cert else 'no'};"
    )
else:
    USER = os.getenv("DB_USER")
    PWD = os.getenv("DB_PASSWORD")
    odbc = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={USER};PWD={PWD};"
        f"Encrypt=yes;TrustServerCertificate={'yes' if trust_cert else 'no'};"
    )

params = urllib.parse.quote_plus(odbc)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

try:
    with engine.connect() as conn:
        print("✅ Conexión exitosa a SQL Server")
        result = conn.execute(text("SELECT TOP 5 NOMBRE FROM ART_DB"))
        for row in result:
            print("Producto:", row[0])
except Exception as e:
    print("❌ Error de conexión:", e)
