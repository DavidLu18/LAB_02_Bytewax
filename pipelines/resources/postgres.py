# Database Setup
import os
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool


_POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
_POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
_POSTGRES_USER = os.environ.get("POSTGRES_USER", "david")
_POSTGRES_PASSWORD = quote_plus(os.environ.get("POSTGRES_PASSWORD", "Omen123456789"))
_POSTGRES_DB = os.environ.get("POSTGRES_DB", "DavidDB")


_DATABASE_URL = f"postgresql+psycopg2://{_POSTGRES_USER}:{_POSTGRES_PASSWORD}@{_POSTGRES_HOST}:{_POSTGRES_PORT}/{_POSTGRES_DB}"
# print(DATABASE_URL)

engine = create_engine(
    _DATABASE_URL,
    poolclass=QueuePool,
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=1800,
    pool_pre_ping=True,
    echo=False,
)
DbSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    db = DbSessionLocal()
    try:
        yield db
    finally:
        db.close()
