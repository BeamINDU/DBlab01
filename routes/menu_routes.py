from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from database import SessionLocal, MenuDB

router = APIRouter(tags=["Menu"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/menus")
def get_menus():
    try:
        return MenuDB().get_menu()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
  
        