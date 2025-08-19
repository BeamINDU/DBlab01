from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from database import SessionLocal, PermissionDB
from database.schemas import ChangePasswordRequest

router = APIRouter(tags=["Permission"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/user-permissions")
def user_permission(userid: str, db: Session = Depends(get_db)):
    try:
        return PermissionDB().user_permission(userid, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/login")
def login(username: str, password: str):
  try:
      return PermissionDB().login(username, password)
  except Exception as e:
      raise HTTPException(status_code=500, detail=str(e))

@router.get("/user-info")
def user_info(userid: str = Query(...)):
    try:
        return PermissionDB().user_info(userid)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/change-password")
def change_password(payload: ChangePasswordRequest, db: Session = Depends(get_db)):
    try:
        return PermissionDB().change_password( payload.userid, payload.current_password, payload.new_password, db )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
        