from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", 5432)
DB_NAME = os.getenv("DB_NAME", "mydb")
DB_USER = os.getenv("DB_USER", "user")
DB_PASS = os.getenv("DB_PASS", "password")
DB_SCHEMA = os.getenv("DB_SCHEMA", "compact_brake")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(
    DATABASE_URL,
    connect_args={
        "options": f"-c search_path={DB_SCHEMA}"
    }
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
schema = DB_SCHEMA

def db_connection():
    try:
        with engine.connect() as conn:
            version = conn.execute(text("SELECT version();")).scalar()
            return {"db_version": version}
    except SQLAlchemyError as e:
        raise RuntimeError(f"Database connection failed: {str(e)}")
    
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


