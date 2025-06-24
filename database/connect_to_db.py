from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError

DB_HOST = "127.0.0.1"
DB_PORT = 15432
DB_NAME = "mydb"
DB_USER = "user"
DB_PASS = "password"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def test_db_connection():
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


