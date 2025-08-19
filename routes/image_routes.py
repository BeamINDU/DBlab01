from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime

from database import SessionLocal, ImagesService

router = APIRouter(tags=["Image"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/result-image")
def result_image(filename: str, defecttime: datetime):
    try:
        return ImagesService.result_image(filename, defecttime)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
  
        