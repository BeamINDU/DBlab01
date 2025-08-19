from fastapi import APIRouter, HTTPException
from database import db_connection

router = APIRouter(tags=["General"])

@router.get("/")
def read_root():
    return {"message": "API is working"}

@router.get("/test-db")
def test_db():
    try:
        return db_connection()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
